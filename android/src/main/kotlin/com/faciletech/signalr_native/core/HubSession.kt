package com.faciletech.signalr_native.core

import com.microsoft.signalr.Action
import com.microsoft.signalr.Action1
import com.microsoft.signalr.Action2
import com.microsoft.signalr.Action3
import com.microsoft.signalr.Action4
import com.microsoft.signalr.Action5
import com.microsoft.signalr.Action6
import com.microsoft.signalr.Action7
import com.microsoft.signalr.Action8
import com.microsoft.signalr.GsonHubProtocol
import com.microsoft.signalr.HubConnection
import com.microsoft.signalr.HubConnectionBuilder
import com.microsoft.signalr.HubConnectionState
import com.microsoft.signalr.HubException
import com.microsoft.signalr.TransportEnum
import com.microsoft.signalr.messagepack.MessagePackHubProtocol
import io.reactivex.rxjava3.core.Single
import io.reactivex.rxjava3.disposables.Disposable
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

/** Plain-Kotlin connection settings, mapped from the Pigeon message by the plugin. */
internal class SessionConfig(
    val url: String,
    val messagePack: Boolean,
    val envelope: Boolean,
    val headers: Map<String, String>,
    val skipNegotiation: Boolean,
    val hasAccessTokenProvider: Boolean,
    val serverTimeoutMs: Long,
    val keepAliveIntervalMs: Long,
    val handshakeTimeoutMs: Long,
    val reconnect: ReconnectPolicy,
    val serverMethods: List<String>,
    val serverMethodArgCounts: List<Int>,
    val logLevel: Int,
)

/** What a session needs from its host (the Flutter plugin, or a test). */
internal interface SessionHost {
    /** Runs [task] on the platform thread. */
    fun postToMain(task: Runnable)

    fun schedule(delayMs: Long, task: Runnable): Cancellable

    /** Sends one data-plane batch to Dart. Platform thread only. */
    fun sendFrame(frame: ByteBuffer)

    fun onState(kind: StateKind, connectionId: String?, attempt: Int, error: String?)

    /** Level uses SignalRLogLevel indices: 0 debug, 1 info, 2 warning, 3 error. */
    fun onLog(level: Int, message: String, traceId: String?)

    /** Asks Dart for a token; [callback] may run on any thread. */
    fun requestAccessToken(callback: (String?, Throwable?) -> Unit)
}

/** Result of an outbound call, mapped to CallResultMsg by the plugin. */
internal class CallResult(
    val result: ByteArray?,
    val nativeStartMicros: Long,
    val nativeEndMicros: Long,
    val errorKind: String? = null,
    val errorMessage: String? = null,
)

/**
 * One SignalR connection: the Java HubConnection, its handlers, the reconnect controller
 * and the data plane. Public methods run on the platform thread.
 */
internal class HubSession(
    val handle: Long,
    private val config: SessionConfig,
    private val host: SessionHost,
) {
    private val codec = PayloadCodec.of(config.messagePack, config.envelope)
    private val dataPlane = DataPlane(handle, host::postToMain, host::sendFrame)
    private val streams = ConcurrentHashMap<Long, Disposable>()

    private val connection: HubConnection = buildConnection()

    private val controller = ReconnectController(
        connection = object : ManagedConnection {
            override fun start(onResult: (Throwable?) -> Unit) {
                connection.start().subscribe(
                    { host.postToMain { onResult(null) } },
                    { e -> host.postToMain { onResult(e) } },
                )
            }

            override fun stop(onDone: () -> Unit) {
                connection.stop().subscribe(
                    { host.postToMain { onDone() } },
                    { host.postToMain { onDone() } },
                )
            }

            override fun connectionId(): String? = connection.connectionId
        },
        policy = config.reconnect,
        scheduler = object : Scheduler {
            override fun schedule(delayMs: Long, task: () -> Unit): Cancellable = host.schedule(delayMs) { task() }
            override fun nowMs(): Long = Clock.nowMicros() / 1000
        },
        sink = { kind, id, attempt, error ->
            log(if (kind == StateKind.DISCONNECTED && error != null) WARNING else INFO,
                "State $kind${if (attempt > 0) " #$attempt" else ""}${error?.let { ": $it" } ?: ""}", null)
            host.onState(kind, id, attempt, error)
        },
    )

    init {
        connection.onClosed { e -> host.postToMain { controller.onConnectionClosed(e) } }
        registerServerMethods()
    }

    fun start(callback: (Throwable?) -> Unit) = controller.start(callback)

    fun stop(callback: () -> Unit) = controller.stop(callback)

    fun dispose(callback: () -> Unit) {
        streams.values.forEach { it.dispose() }
        streams.clear()
        controller.stop {
            try {
                connection.close()
            } catch (_: Exception) {
            }
            callback()
        }
    }

    fun send(method: String, args: List<ByteArray>, traceId: String, callback: (CallResult) -> Unit) {
        val start = Clock.nowMicros()
        log(DEBUG, "send $method (${args.size} args)", traceId)
        val result = try {
            connection.send(method, *decodeArgs(args))
            CallResult(null, start, Clock.nowMicros())
        } catch (e: Exception) {
            failure(e, start, traceId)
        }
        callback(result)
    }

    fun invoke(method: String, args: List<ByteArray>, traceId: String, callback: (CallResult) -> Unit) {
        val start = Clock.nowMicros()
        log(DEBUG, "invoke $method (${args.size} args)", traceId)
        val single: Single<Any> = try {
            connection.invoke<Any>(codec.type, method, *decodeArgs(args))
        } catch (e: Exception) {
            return callback(failure(e, start, traceId))
        }
        single.subscribe(
            { value ->
                val end = Clock.nowMicros()
                val bytes = codec.resultBytes(value)
                host.postToMain { callback(CallResult(bytes, start, end)) }
            },
            { e ->
                val r = failure(e, start, traceId)
                host.postToMain { callback(r) }
            },
        )
    }

    fun streamStart(
        streamId: Long,
        method: String,
        args: List<ByteArray>,
        traceId: String,
        callback: (CallResult) -> Unit,
    ) {
        val start = Clock.nowMicros()
        log(DEBUG, "stream $method #$streamId", traceId)
        val disposable = try {
            connection.stream<Any>(codec.type, method, *decodeArgs(args)).subscribe(
                { item ->
                    if (item !== codec.nullResult) {
                        dataPlane.add(DataEntry.streamItem(streamId, Clock.nowMicros(), codec.toBytes(item)))
                    }
                },
                { e ->
                    streams.remove(streamId)
                    dataPlane.add(DataEntry.streamEnd(streamId, Clock.nowMicros(), describe(e)))
                },
                {
                    streams.remove(streamId)
                    dataPlane.add(DataEntry.streamEnd(streamId, Clock.nowMicros(), null))
                },
            )
        } catch (e: Exception) {
            return callback(failure(e, start, traceId))
        }
        streams[streamId] = disposable
        callback(CallResult(null, start, Clock.nowMicros()))
    }

    fun streamCancel(streamId: Long) {
        streams.remove(streamId)?.dispose()
    }

    // ------------------------------------------------------------------------------------

    private fun buildConnection(): HubConnection {
        val builder = HubConnectionBuilder.create(config.url)
            .withTransport(TransportEnum.WEBSOCKETS)
            .shouldSkipNegotiate(config.skipNegotiation)
            .withHeaders(config.headers)
            .withServerTimeout(config.serverTimeoutMs)
            .withKeepAliveInterval(config.keepAliveIntervalMs)
            .withHandshakeResponseTimeout(config.handshakeTimeoutMs)
            .withHubProtocol(
                NullResultProtocol(
                    if (config.messagePack) MessagePackHubProtocol() else GsonHubProtocol(),
                    codec.nullResult,
                ),
            )
        if (config.hasAccessTokenProvider) {
            // Deferred: evaluated on every start(), so each reconnect fetches a fresh token.
            builder.withAccessTokenProvider(
                Single.defer {
                    Single.create<String> { emitter ->
                        host.requestAccessToken { token, error ->
                            if (error != null) emitter.tryOnError(error) else emitter.onSuccess(token ?: "")
                        }
                    }
                },
            )
        }
        return builder.build()
    }

    /**
     * Registers every declared server method once, for the connection's lifetime. Handlers
     * run on the client's network thread: they stamp the receive time, convert arguments to
     * bytes and hand them to the data plane — nothing else.
     */
    private fun registerServerMethods() {
        val t = codec.type
        config.serverMethods.forEachIndexed { index, name ->
            val arity = if (config.envelope) 1 else config.serverMethodArgCounts[index]
            fun emit(vararg values: Any?) {
                val received = Clock.nowMicros()
                val args = Array(values.size) { codec.toBytes(values[it]) }
                dataPlane.add(DataEntry.invocation(index, received, args))
            }
            when (arity) {
                0 -> connection.on(name, Action { emit() })
                1 -> connection.on(name, Action1<Any?> { a -> emit(a) }, t)
                2 -> connection.on(name, Action2<Any?, Any?> { a, b -> emit(a, b) }, t, t)
                3 -> connection.on(name, Action3<Any?, Any?, Any?> { a, b, c -> emit(a, b, c) }, t, t, t)
                4 -> connection.on(name, Action4<Any?, Any?, Any?, Any?> { a, b, c, d -> emit(a, b, c, d) }, t, t, t, t)
                5 -> connection.on(
                    name,
                    Action5<Any?, Any?, Any?, Any?, Any?> { a, b, c, d, e -> emit(a, b, c, d, e) },
                    t, t, t, t, t,
                )
                6 -> connection.on(
                    name,
                    Action6<Any?, Any?, Any?, Any?, Any?, Any?> { a, b, c, d, e, f -> emit(a, b, c, d, e, f) },
                    t, t, t, t, t, t,
                )
                7 -> connection.on(
                    name,
                    Action7<Any?, Any?, Any?, Any?, Any?, Any?, Any?> { a, b, c, d, e, f, g -> emit(a, b, c, d, e, f, g) },
                    t, t, t, t, t, t, t,
                )
                8 -> connection.on(
                    name,
                    Action8<Any?, Any?, Any?, Any?, Any?, Any?, Any?, Any?> { a, b, c, d, e, f, g, h ->
                        emit(a, b, c, d, e, f, g, h)
                    },
                    t, t, t, t, t, t, t, t,
                )
                else -> throw IllegalArgumentException("$name: $arity arguments; the Java client binds at most 8")
            }
        }
    }

    private fun decodeArgs(args: List<ByteArray>): Array<Any?> = Array(args.size) { codec.fromBytes(args[it]) }

    private fun failure(e: Throwable, start: Long, traceId: String): CallResult {
        val kind = when {
            e is HubException -> "invocation"
            connection.connectionState != HubConnectionState.CONNECTED -> "notConnected"
            else -> "native"
        }
        log(if (kind == "invocation") INFO else WARNING, "call failed ($kind): ${describe(e)}", traceId)
        return CallResult(null, start, Clock.nowMicros(), kind, describe(e))
    }

    private fun describe(e: Throwable): String = e.message ?: e.javaClass.simpleName

    private fun log(level: Int, message: String, traceId: String?) {
        if (level < config.logLevel) return
        host.postToMain { host.onLog(level, message, traceId) }
    }

    private companion object {
        const val DEBUG = 0
        const val INFO = 1
        const val WARNING = 2
    }
}
