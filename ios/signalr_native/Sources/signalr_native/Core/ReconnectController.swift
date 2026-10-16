import Foundation
import SignalRClient

/// What the controller drives. Callbacks may arrive on any thread; the owner re-posts them.
protocol ManagedConnection: AnyObject {
    func start(onResult: @escaping (Error?) -> Void)
    func stop(onDone: @escaping () -> Void)
    func connectionId() -> String?
}

protocol Cancellable {
    func cancel()
}

protocol Scheduler {
    func schedule(delayMs: Int64, task: @escaping () -> Void) -> Cancellable
    func nowMs() -> Int64
}

enum StateKind: String {
    case disconnected = "DISCONNECTED"
    case connecting = "CONNECTING"
    case connected = "CONNECTED"
    case reconnecting = "RECONNECTING"
}

typealias StateSink = (_ kind: StateKind, _ connectionId: String?, _ attempt: Int, _ error: String?) -> Void

/// Connection lifecycle + automatic reconnect, owned by the plugin so Android and iOS behave
/// identically. Port of ReconnectController.kt: the Swift client's own automatic reconnect
/// attempts first and sleeps after a failure, so the same delays would fire at different times.
///
/// Not thread-safe by design: every method, including the callbacks passed to
/// `ManagedConnection`, must run on one thread (the main thread).
final class ReconnectController {
    private enum Phase { case stopped, starting, connected, reconnecting, stopping }

    private let connection: ManagedConnection
    private let policy: ReconnectPolicy
    private let scheduler: Scheduler
    private let sink: StateSink

    private var phase = Phase.stopped
    private var retryCount = 0
    private var cycleStartMs: Int64 = 0
    private var pendingAttempt: Cancellable?
    private var attemptInFlight = false

    init(connection: ManagedConnection, policy: ReconnectPolicy, scheduler: Scheduler, sink: @escaping StateSink) {
        self.connection = connection
        self.policy = policy
        self.scheduler = scheduler
        self.sink = sink
    }

    var isConnected: Bool { phase == .connected }

    func start(_ callback: @escaping (Error?) -> Void) {
        switch phase {
        case .connected: return callback(nil)
        case .stopped: break
        default: return callback(ReconnectError.invalidState("Cannot start while \(phase)"))
        }
        phase = .starting
        sink(.connecting, nil, 0, nil)
        connection.start { [self] error in
            if phase != .starting {
                // stop() was called while connecting. If its stop already finished and this
                // start still succeeded, nothing else would close the socket.
                if error == nil && phase == .stopped { connection.stop {} }
                callback(error ?? ReconnectError.invalidState("Stopped while connecting"))
                return
            }
            if let error {
                phase = .stopped
                sink(.disconnected, nil, 0, describe(error))
            } else {
                phase = .connected
                sink(.connected, connection.connectionId(), 0, nil)
            }
            callback(error)
        }
    }

    func stop(_ callback: @escaping () -> Void) {
        switch phase {
        case .stopped, .stopping:
            callback()
        case .reconnecting:
            pendingAttempt?.cancel()
            pendingAttempt = nil
            phase = .stopped
            sink(.disconnected, nil, 0, nil)
            if attemptInFlight { connection.stop {} }
            callback()
        case .starting, .connected:
            phase = .stopping
            connection.stop { [self] in
                if phase == .stopping {
                    phase = .stopped
                    sink(.disconnected, nil, 0, nil)
                }
                callback()
            }
        }
    }

    /// Wire to the client's onClosed.
    func onConnectionClosed(_ error: Error?) {
        // starting / reconnecting: the in-flight start's callback owns the outcome.
        // stopping / stopped: a requested stop.
        guard phase == .connected else { return }
        retryCount = 0
        cycleStartMs = scheduler.nowMs()
        scheduleNext(error)
    }

    private func scheduleNext(_ reason: Error?) {
        guard let delay = policy.nextDelayMs(retryCount: retryCount, elapsedMs: scheduler.nowMs() - cycleStartMs) else {
            phase = .stopped
            sink(.disconnected, nil, 0, describe(reason) ?? "Reconnect policy exhausted")
            return
        }
        phase = .reconnecting
        sink(.reconnecting, nil, retryCount + 1, describe(reason))
        pendingAttempt = scheduler.schedule(delayMs: delay) { [weak self] in self?.attempt() }
    }

    private func attempt() {
        pendingAttempt = nil
        guard phase == .reconnecting else { return }
        attemptInFlight = true
        connection.start { [self] error in
            attemptInFlight = false
            guard phase == .reconnecting else {
                // stop() during the attempt: same late-success case as in start().
                if error == nil && phase == .stopped { connection.stop {} }
                return
            }
            if let error {
                retryCount += 1
                scheduleNext(error)
            } else {
                phase = .connected
                sink(.connected, connection.connectionId(), 0, nil)
            }
        }
    }

    private func describe(_ error: Error?) -> String? {
        error.map(describeError)
    }
}

enum ReconnectError: LocalizedError, CustomStringConvertible {
    case invalidState(String)

    var description: String {
        switch self {
        case .invalidState(let message): return message
        }
    }

    var errorDescription: String? { description }
}

/// Human-readable message for any error. SignalRError only implements CustomStringConvertible,
/// which every Error satisfies through NSError bridging, so it is matched by type; the
/// plugin's own errors are LocalizedErrors.
func describeError(_ error: Error) -> String {
    if let e = error as? SignalRError { return e.description }
    if let e = error as? LocalizedError, let d = e.errorDescription { return d }
    return error.localizedDescription
}
