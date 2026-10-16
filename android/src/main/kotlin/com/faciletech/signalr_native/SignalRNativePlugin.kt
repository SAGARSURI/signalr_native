package com.faciletech.signalr_native

import android.os.Handler
import android.os.Looper
import com.faciletech.signalr_native.core.CallResult
import com.faciletech.signalr_native.core.Cancellable
import com.faciletech.signalr_native.core.HubSession
import com.faciletech.signalr_native.core.ReconnectPolicy
import com.faciletech.signalr_native.core.SessionConfig
import com.faciletech.signalr_native.core.SessionHost
import com.faciletech.signalr_native.core.StateKind
import io.flutter.embedding.engine.plugins.FlutterPlugin
import io.flutter.plugin.common.BinaryMessenger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.resume

/**
 * Flutter glue: Pigeon host API → [HubSession]s, session events → Pigeon Flutter API,
 * data batches → the raw "signalr_native/data" channel.
 *
 * Pigeon 29 generates `suspend` functions for @async methods. Each one hops to the
 * platform thread before touching a session, because sessions (and their reconnect
 * controllers) are confined to it.
 */
class SignalRNativePlugin : FlutterPlugin, SignalRHostApi {
    private var messenger: BinaryMessenger? = null
    private var flutterApi: SignalRFlutterApi? = null
    private val main = Handler(Looper.getMainLooper())
    private var scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private val sessions = HashMap<Long, HubSession>()
    private val nextHandle = AtomicLong(1)

    override fun onAttachedToEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        messenger = binding.binaryMessenger
        scope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
        flutterApi = SignalRFlutterApi(binding.binaryMessenger)
        SignalRHostApi.setUp(binding.binaryMessenger, this)
    }

    override fun onDetachedFromEngine(binding: FlutterPlugin.FlutterPluginBinding) {
        SignalRHostApi.setUp(binding.binaryMessenger, null)
        sessions.values.forEach { it.dispose {} }
        sessions.clear()
        scope.cancel()
        flutterApi = null
        messenger = null
    }

    // ---- SignalRHostApi ------------------------------------------------------------------

    override fun create(config: ConnectConfigMsg): Long {
        val handle = nextHandle.getAndIncrement()
        val r = config.reconnect
        val sessionConfig = SessionConfig(
            url = config.url,
            messagePack = config.hubProtocol == ProtocolMsg.MESSAGE_PACK,
            envelope = config.payloadMode == PayloadModeMsg.ENVELOPE,
            headers = config.headers,
            skipNegotiation = config.skipNegotiation,
            hasAccessTokenProvider = config.hasAccessTokenProvider,
            serverTimeoutMs = config.serverTimeoutMs,
            keepAliveIntervalMs = config.keepAliveIntervalMs,
            handshakeTimeoutMs = config.handshakeTimeoutMs,
            reconnect = ReconnectPolicy(
                delaysMs = r.delaysMs,
                exponential = r.exponential,
                initialMs = r.initialMs,
                maxMs = r.maxMs,
                multiplier = r.multiplier,
                jitter = r.jitter,
                maxAttempts = r.maxAttempts,
                maxElapsedMs = r.maxElapsedMs,
            ),
            serverMethods = config.serverMethods,
            serverMethodArgCounts = config.serverMethodArgCounts.map { it.toInt() },
            logLevel = config.logLevel.toInt(),
        )
        sessions[handle] = HubSession(handle, sessionConfig, Host(handle))
        return handle
    }

    override suspend fun start(handle: Long) = onMain<Unit> {
        val session = sessions[handle] ?: throw unknown(handle)
        val error = suspendCancellableCoroutine<Throwable?> { cont -> session.start { cont.resume(it) } }
        if (error != null) throw FlutterError("connect", error.message ?: error.javaClass.simpleName, null)
    }

    override suspend fun stop(handle: Long) = onMain<Unit> {
        val session = sessions[handle] ?: return@onMain
        suspendCancellableCoroutine<Unit> { cont -> session.stop { cont.resume(Unit) } }
    }

    override suspend fun send(handle: Long, method: String, args: List<ByteArray>, traceId: String): CallResultMsg =
        call(handle) { s, done -> s.send(method, args, traceId, done) }

    override suspend fun invoke(handle: Long, method: String, args: List<ByteArray>, traceId: String): CallResultMsg =
        call(handle) { s, done -> s.invoke(method, args, traceId, done) }

    override suspend fun streamStart(
        handle: Long,
        streamId: Long,
        method: String,
        args: List<ByteArray>,
        traceId: String,
    ): CallResultMsg = call(handle) { s, done -> s.streamStart(streamId, method, args, traceId, done) }

    override fun streamCancel(handle: Long, streamId: Long) {
        sessions[handle]?.streamCancel(streamId)
    }

    override suspend fun dispose(handle: Long) = onMain<Unit> {
        val session = sessions.remove(handle) ?: return@onMain
        suspendCancellableCoroutine<Unit> { cont -> session.dispose { cont.resume(Unit) } }
    }

    private suspend fun <T> onMain(block: suspend () -> T): T = withContext(Dispatchers.Main.immediate) { block() }

    private suspend fun call(handle: Long, op: (HubSession, (CallResult) -> Unit) -> Unit): CallResultMsg = onMain {
        val session = sessions[handle] ?: throw unknown(handle)
        suspendCancellableCoroutine<CallResult> { cont -> op(session) { cont.resume(it) } }.toMsg()
    }

    // ---- Session host ----------------------------------------------------------------------

    private inner class Host(private val handle: Long) : SessionHost {
        override fun postToMain(task: Runnable) {
            if (Looper.myLooper() === Looper.getMainLooper()) task.run() else main.post(task)
        }

        override fun schedule(delayMs: Long, task: Runnable): Cancellable {
            main.postDelayed(task, delayMs)
            return object : Cancellable {
                override fun cancel() = main.removeCallbacks(task)
            }
        }

        override fun sendFrame(frame: ByteBuffer) {
            messenger?.send(DATA_CHANNEL, frame)
        }

        override fun onState(kind: StateKind, connectionId: String?, attempt: Int, error: String?) {
            val api = flutterApi ?: return
            val event = StateEventMsg(
                    kind = when (kind) {
                        StateKind.DISCONNECTED -> StateKindMsg.DISCONNECTED
                        StateKind.CONNECTING -> StateKindMsg.CONNECTING
                        StateKind.CONNECTED -> StateKindMsg.CONNECTED
                        StateKind.RECONNECTING -> StateKindMsg.RECONNECTING
                    },
                    connectionId = connectionId,
                    attempt = attempt.toLong(),
                    error = error,
            )
            scope.launch { runCatching { api.onState(handle, event) } }
        }

        override fun onLog(level: Int, message: String, traceId: String?) {
            val api = flutterApi ?: return
            val event = LogEventMsg(
                level = level.toLong(),
                message = message,
                timeMicros = com.faciletech.signalr_native.core.Clock.nowMicros(),
                traceId = traceId,
            )
            scope.launch { runCatching { api.onLog(handle, event) } }
        }

        override fun requestAccessToken(callback: (String?, Throwable?) -> Unit) {
            val api = flutterApi ?: return callback(null, IllegalStateException("Engine detached"))
            scope.launch {
                val result = runCatching { api.accessToken(handle) }
                callback(result.getOrNull(), result.exceptionOrNull())
            }
        }
    }

    private fun CallResult.toMsg() = CallResultMsg(
        result = result,
        nativeStartMicros = nativeStartMicros,
        nativeEndMicros = nativeEndMicros,
        errorKind = errorKind,
        errorMessage = errorMessage,
    )

    private fun unknown(handle: Long) =
        FlutterError("unknown_handle", "No SignalR connection with handle $handle", null)

    private companion object {
        /** Must match PigeonHubPlatform.dataChannelName. */
        const val DATA_CHANNEL = "signalr_native/data"
    }
}
