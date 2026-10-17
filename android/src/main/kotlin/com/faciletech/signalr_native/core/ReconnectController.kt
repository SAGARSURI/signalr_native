package com.faciletech.signalr_native.core

/** What the controller drives. Callbacks may arrive on any thread; the owner re-posts them. */
internal interface ManagedConnection {
    fun start(onResult: (Throwable?) -> Unit)
    fun stop(onDone: () -> Unit)
    fun connectionId(): String?
}

internal interface Cancellable {
    fun cancel()
}

internal interface Scheduler {
    fun schedule(delayMs: Long, task: () -> Unit): Cancellable
    fun nowMs(): Long
}

internal enum class StateKind { DISCONNECTED, CONNECTING, CONNECTED, RECONNECTING }

internal fun interface StateSink {
    fun onState(kind: StateKind, connectionId: String?, attempt: Int, error: String?)
}

/**
 * Connection lifecycle + automatic reconnect, owned by the plugin so Android and iOS behave
 * identically (the Java client has no automatic reconnect; the Swift client's waits after
 * each failed attempt instead of before).
 *
 * Not thread-safe by design: every method, including the callbacks passed to
 * [ManagedConnection], must run on one thread (the platform thread).
 */
internal class ReconnectController(
    private val connection: ManagedConnection,
    private val policy: ReconnectPolicy,
    private val scheduler: Scheduler,
    private val sink: StateSink,
) {
    private enum class Phase { STOPPED, STARTING, CONNECTED, RECONNECTING, STOPPING }

    private var phase = Phase.STOPPED
    private var retryCount = 0
    private var cycleStartMs = 0L
    private var pendingAttempt: Cancellable? = null
    private var attemptInFlight = false

    val isConnected: Boolean get() = phase == Phase.CONNECTED

    fun start(callback: (Throwable?) -> Unit) {
        when (phase) {
            Phase.CONNECTED -> return callback(null)
            Phase.STOPPED -> Unit
            else -> return callback(IllegalStateException("Cannot start while ${phase.name.lowercase()}"))
        }
        phase = Phase.STARTING
        sink.onState(StateKind.CONNECTING, null, 0, null)
        connection.start { error ->
            if (phase != Phase.STARTING) {
                // stop() was called while connecting. If its stop already finished and this
                // start still succeeded, nothing else would close the socket.
                if (error == null && phase == Phase.STOPPED) connection.stop {}
                callback(error ?: IllegalStateException("Stopped while connecting"))
                return@start
            }
            if (error == null) {
                phase = Phase.CONNECTED
                sink.onState(StateKind.CONNECTED, connection.connectionId(), 0, null)
            } else {
                phase = Phase.STOPPED
                sink.onState(StateKind.DISCONNECTED, null, 0, describe(error))
            }
            callback(error)
        }
    }

    fun stop(callback: () -> Unit) {
        when (phase) {
            Phase.STOPPED -> callback()
            Phase.STOPPING -> callback()
            Phase.RECONNECTING -> {
                pendingAttempt?.cancel()
                pendingAttempt = null
                phase = Phase.STOPPED
                sink.onState(StateKind.DISCONNECTED, null, 0, null)
                if (attemptInFlight) connection.stop {}
                callback()
            }
            Phase.STARTING, Phase.CONNECTED -> {
                phase = Phase.STOPPING
                connection.stop {
                    if (phase == Phase.STOPPING) {
                        phase = Phase.STOPPED
                        sink.onState(StateKind.DISCONNECTED, null, 0, null)
                    }
                    callback()
                }
            }
        }
    }

    /** Wire to the client's onClosed. */
    fun onConnectionClosed(error: Throwable?) {
        // STARTING / RECONNECTING: the in-flight start's callback owns the outcome.
        // STOPPING / STOPPED: a requested stop.
        if (phase != Phase.CONNECTED) return
        retryCount = 0
        cycleStartMs = scheduler.nowMs()
        scheduleNext(error)
    }

    private fun scheduleNext(reason: Throwable?) {
        val delay = policy.nextDelayMs(retryCount, scheduler.nowMs() - cycleStartMs)
        if (delay == null) {
            phase = Phase.STOPPED
            sink.onState(StateKind.DISCONNECTED, null, 0, describe(reason) ?: "Reconnect policy exhausted")
            return
        }
        phase = Phase.RECONNECTING
        sink.onState(StateKind.RECONNECTING, null, retryCount + 1, describe(reason))
        pendingAttempt = scheduler.schedule(delay) { attempt() }
    }

    private fun attempt() {
        pendingAttempt = null
        if (phase != Phase.RECONNECTING) return
        attemptInFlight = true
        connection.start { error ->
            attemptInFlight = false
            if (phase != Phase.RECONNECTING) {
                // stop() during the attempt: same late-success case as in start().
                if (error == null && phase == Phase.STOPPED) connection.stop {}
                return@start
            }
            if (error == null) {
                phase = Phase.CONNECTED
                sink.onState(StateKind.CONNECTED, connection.connectionId(), 0, null)
            } else {
                retryCount++
                scheduleNext(error)
            }
        }
    }

    private fun describe(error: Throwable?): String? =
        error?.let { it.message ?: it.javaClass.simpleName }
}
