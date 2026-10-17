import Foundation
import SignalRClient

/// The part of the Swift client's HubConnection that ClientConnection drives; tests fake it.
protocol StartableConnection: AnyObject {
    func start() async throws
    func stop() async
}

extension HubConnection: StartableConnection {}

/// Whether a connection's messages may reach Dart. On from the start (a server may send in
/// the same frame as the handshake reply, before start() returns), off once its attempt is
/// aborted or stopped: an aborted attempt can still finish connecting before its stop lands
/// and must not deliver (or duplicate the next attempt's messages) in that window. Read on
/// the client's actor, so lock-guarded.
final class LiveFlag: @unchecked Sendable {
    private let lock = NSLock()
    private var on = true

    var isOn: Bool {
        lock.lock()
        defer { lock.unlock() }
        return on
    }

    func set(_ value: Bool) {
        lock.lock()
        on = value
        lock.unlock()
    }
}

/// Adapts the Swift client to the controller. Results are re-posted to the main thread.
///
/// Each start builds a new connection (see HubSession), so starts never wait for each other.
/// The client ignores stop() until a start has reached it, and only rejects the handshake
/// wait (not a token fetch, negotiate or connect) when stopped mid-start. So each start is a
/// `StartAttempt` that stop() or the start timeout can abort: the abort reports at once and
/// closes the attempt's connection in the background, and a start that still succeeds
/// afterwards closes itself.
final class ClientConnection<C: StartableConnection>: ManagedConnection {
    private let build: (LiveFlag) async -> C
    private let postToMain: (@escaping () -> Void) -> Void
    private let schedule: (Int64, @escaping () -> Void) -> Cancellable
    private let timeoutMs: Int64
    /// Receives closes of the live connection. Main thread.
    var onClosed: ((Error?) -> Void)?
    /// The latest start; its connection is the one stop() closes. Main thread.
    private var latest: StartAttempt<C>?
    /// The connection of the last successful start. Main thread.
    private(set) var current: C?

    init(build: @escaping (LiveFlag) async -> C,
         postToMain: @escaping (@escaping () -> Void) -> Void,
         schedule: @escaping (Int64, @escaping () -> Void) -> Cancellable,
         timeoutMs: Int64) {
        self.build = build
        self.postToMain = postToMain
        self.schedule = schedule
        self.timeoutMs = timeoutMs
    }

    func start(onResult: @escaping (Error?) -> Void) {
        let attempt = StartAttempt<C>(onResult: onResult)
        latest = attempt
        let build = self.build
        let postToMain = self.postToMain
        if timeoutMs > 0 {
            let seconds = Double(timeoutMs) / 1000
            let timer = schedule(timeoutMs) { [weak self] in
                self?.abort(attempt, ReconnectError.invalidState("Start timed out after \(seconds) s"))
            }
            attempt.onFinish = { timer.cancel() }
        }
        Task { [weak self] in
            let connection = await build(attempt.live)
            // Aborted before the connection existed: already reported, never started.
            guard attempt.bind(connection) else { return }
            do {
                try await connection.start()
                if attempt.finish() {
                    postToMain { self?.connected(attempt, connection) }
                } else {
                    // Aborted (and reported) while starting, and the abort's stop came too early.
                    await connection.stop()
                }
            } catch {
                if attempt.finish() { postToMain { attempt.report(error) } }
            }
        }
    }

    func stop(onDone: @escaping () -> Void) {
        guard let attempt = latest else { return onDone() }
        // A start in progress: it reports now, and its connection closes in the background
        // (the client's stop waits for a pending token fetch, negotiate or connect).
        if abort(attempt, ReconnectError.invalidState("Stopped while connecting")) { return onDone() }
        attempt.live.set(false)
        guard let connection = attempt.connection else { return onDone() }
        let postToMain = self.postToMain
        Task {
            await connection.stop()
            postToMain(onDone)
        }
    }

    /// A connection's onClosed. Main thread.
    func connectionClosed(_ connection: C, _ error: Error?) {
        if connection === current {
            onClosed?(error)
        } else if let attempt = latest, attempt.connection === connection {
            // The client fires onClosed as soon as its start succeeded, so a server that closes
            // right after the handshake can beat the success post; replay it after that.
            attempt.earlyClose = EarlyClose(error: error)
        }
    }

    /// The Swift client does not expose the connection id.
    func connectionId() -> String? { nil }

    /// Main thread.
    private func connected(_ attempt: StartAttempt<C>, _ connection: C) {
        current = connection
        attempt.report(nil)
        if let early = attempt.earlyClose { onClosed?(early.error) }
    }

    /// Aborts a start that has not settled: reports `error` and closes its connection, if it
    /// has one yet, in the background. False when the attempt already settled. Main thread.
    @discardableResult
    private func abort(_ attempt: StartAttempt<C>, _ error: Error) -> Bool {
        guard attempt.abort() else { return false }
        attempt.live.set(false)
        attempt.report(error)
        if let connection = attempt.connection { Task { await connection.stop() } }
        return true
    }
}

struct EarlyClose {
    let error: Error?
}

/// One start of the client: it either finishes or is aborted first, never both.
final class StartAttempt<C: AnyObject>: @unchecked Sendable {
    let live = LiveFlag()
    private let lock = NSLock()
    private var finished = false
    private var aborted = false
    private var boundConnection: C?
    /// Set on the main thread before the start runs; called once when the attempt settles.
    var onFinish: (() -> Void)?
    /// Main thread only.
    private var onResult: ((Error?) -> Void)?
    /// Main thread only. A close of this attempt's connection seen before its success.
    var earlyClose: EarlyClose?

    init(onResult: @escaping (Error?) -> Void) {
        self.onResult = onResult
    }

    /// Delivers the attempt's result once; later reports are dropped. Main thread.
    func report(_ error: Error?) {
        let onResult = self.onResult
        self.onResult = nil
        onResult?(error)
    }

    var connection: C? {
        lock.lock()
        defer { lock.unlock() }
        return boundConnection
    }

    /// Records the connection this attempt starts; false when it was aborted first, in which
    /// case the connection is never started.
    func bind(_ connection: C) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !aborted else { return false }
        boundConnection = connection
        return true
    }

    /// Returns false when the attempt was aborted first.
    func finish() -> Bool {
        settle { finished = true }
    }

    /// Returns false when the attempt already finished or was aborted.
    func abort() -> Bool {
        settle { aborted = true }
    }

    private func settle(_ mark: () -> Void) -> Bool {
        lock.lock()
        let settled = finished || aborted
        if !settled { mark() }
        let done = settled ? nil : onFinish
        lock.unlock()
        done?()
        return !settled
    }
}

enum TimeoutError: LocalizedError {
    case timedOut(String)

    var errorDescription: String? {
        switch self {
        case .timedOut(let what): return what
        }
    }
}

/// Runs `operation` but gives up after `ms`, even when the operation can't be cancelled (a
/// call into Dart that never answers): the caller resumes and the operation finishes alone.
func withTimeout<T>(ms: Int64, _ what: String, _ operation: @escaping () async throws -> T) async throws -> T {
    let once = ResumeOnce<T>()
    return try await withCheckedThrowingContinuation { continuation in
        once.continuation = continuation
        Task {
            do { once.resume(.success(try await operation())) } catch { once.resume(.failure(error)) }
        }
        Task {
            try? await Task.sleep(nanoseconds: UInt64(max(ms, 0)) * 1_000_000)
            once.resume(.failure(TimeoutError.timedOut(what)))
        }
    }
}

private final class ResumeOnce<T>: @unchecked Sendable {
    private let lock = NSLock()
    var continuation: CheckedContinuation<T, Error>?

    func resume(_ result: Result<T, Error>) {
        lock.lock()
        let c = continuation
        continuation = nil
        lock.unlock()
        c?.resume(with: result)
    }
}
