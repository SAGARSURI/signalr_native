import Foundation
import SignalRClient

/// Plain-Swift connection settings, mapped from the Pigeon message by the plugin.
struct SessionConfig {
    let url: String
    let messagePack: Bool
    let envelope: Bool
    let headers: [String: String]
    let skipNegotiation: Bool
    let hasAccessTokenProvider: Bool
    let serverTimeoutMs: Int64
    let keepAliveIntervalMs: Int64
    /// The Swift client has no handshake timeout, so the adapter applies this to the whole
    /// start (token, WebSocket connect and handshake) and aborts the start when it expires.
    let handshakeTimeoutMs: Int64
    let reconnect: ReconnectPolicy
    let serverMethods: [String]
    let serverMethodArgCounts: [Int]
    /// SignalRLogLevel index: 0 debug, 1 info, 2 warning, 3 error, 4 none.
    let logLevel: Int
}

/// What a session needs from its host (the Flutter plugin, or a test).
protocol SessionHost: AnyObject, Sendable {
    /// Runs `task` on the main thread.
    func postToMain(_ task: @escaping () -> Void)

    func schedule(delayMs: Int64, task: @escaping () -> Void) -> Cancellable

    /// Sends one data-plane batch to Dart. Main thread only.
    func sendFrame(_ frame: Data)

    func onState(kind: StateKind, connectionId: String?, attempt: Int, error: String?)

    /// Level uses SignalRLogLevel indices: 0 debug, 1 info, 2 warning, 3 error.
    func onLog(level: Int, message: String, traceId: String?)

    /// Asks Dart for a token. Called by the client before every connect.
    func requestAccessToken() async throws -> String?
}

/// Result of an outbound call, mapped to CallResultMsg by the plugin.
struct CallResult {
    var result: Data?
    var nativeStartMicros: Int64
    var nativeEndMicros: Int64
    var errorKind: String?
    var errorMessage: String?
}

/// One SignalR connection: the Swift HubConnection, its handlers, the reconnect controller
/// and the data plane. Mirrors HubSession.kt. Public methods run on the main thread.
final class HubSession {
    static let debug = 0
    static let info = 1
    static let warning = 2

    let handle: Int64
    private let config: SessionConfig
    private let host: SessionHost
    private let codec: PayloadCodec
    private let dataPlane: DataPlane
    private let connection: HubConnection
    private let client: ClientConnection
    private var controller: ReconnectController!
    private let outbound = OutboundQueue()
    private var streams: [Int64: ActiveStream] = [:]
    /// Invokes waiting for their completion. The Swift client never fails these when the
    /// connection closes, so the session does it when the controller leaves Connected.
    private var pendingInvokes: [Int: (start: Int64, callback: (CallResult) -> Void)] = [:]
    private var nextInvokeId = 0

    init(handle: Int64, config: SessionConfig, host: SessionHost) {
        self.handle = handle
        self.config = config
        self.host = host
        let codec = PayloadCodec.of(messagePack: config.messagePack, envelope: config.envelope)
        self.codec = codec
        let dataPlane = DataPlane(handle: handle, postToMain: { [weak host] task in host?.postToMain(task) },
                                  deliver: { [weak host] frame in host?.sendFrame(frame) })
        self.dataPlane = dataPlane
        let connection = HubSession.buildConnection(config: config, host: host)
        self.connection = connection

        // Every declared server method is registered once, for the connection's lifetime,
        // before the first start (the Swift client drops a whole JSON frame if any message in
        // it targets an unregistered method).
        let methods = config.serverMethods
        let arities = config.serverMethodArgCounts
        let envelope = config.envelope
        let logLevel = config.logLevel
        let closed = ClosedForwarder()
        // Built outside the Task so it does not hold the host strongly.
        let onClosed: (Error?) async -> Void = { [weak host] error in
            host?.postToMain { closed.controller?.onConnectionClosed(error) }
        }
        let log: @Sendable (String) -> Void = { [weak host] message in
            guard HubSession.warning >= logLevel, let host else { return }
            host.postToMain { host.onLog(level: HubSession.warning, message: message, traceId: nil) }
        }
        let registration = Task {
            await connection.onClosed(handler: onClosed)
            switch codec {
            case .jsonArgs, .msgpackArgs:
                await HubSession.register(PassthroughValue.self, on: connection, methods: methods, arities: arities,
                                          envelope: envelope, codec: codec, plane: dataPlane, log: log)
            case .jsonEnvelope:
                await HubSession.register(String.self, on: connection, methods: methods, arities: arities,
                                          envelope: envelope, codec: codec, plane: dataPlane, log: log)
            case .msgpackEnvelope:
                await HubSession.register(Data.self, on: connection, methods: methods, arities: arities,
                                          envelope: envelope, codec: codec, plane: dataPlane, log: log)
            }
        }
        client = ClientConnection(connection: connection, ready: registration, host: host,
                                  timeoutMs: config.handshakeTimeoutMs)

        controller = ReconnectController(
            connection: client,
            policy: config.reconnect,
            scheduler: HostScheduler(host: host),
            sink: { [weak self] kind, id, attempt, error in
                guard let self else { return }
                let level = kind == .disconnected && error != nil ? HubSession.warning : HubSession.info
                self.log(level, "State \(kind.rawValue)\(attempt > 0 ? " #\(attempt)" : "")\(error.map { ": \($0)" } ?? "")", nil)
                if kind == .reconnecting || kind == .disconnected { self.failInFlight(error) }
                self.host.onState(kind: kind, connectionId: id, attempt: attempt, error: error)
            }
        )
        closed.controller = controller
    }

    func start(_ callback: @escaping (Error?) -> Void) { controller.start(callback) }

    func stop(_ callback: @escaping () -> Void) { controller.stop(callback) }

    func dispose(_ callback: @escaping () -> Void) {
        failInFlight(nil)
        controller.stop(callback)
    }

    func send(method: String, args: [Data], traceId: String, callback: @escaping (CallResult) -> Void) {
        let start = Clock.nowMicros()
        log(HubSession.debug, "send \(method) (\(args.count) args)", traceId)
        guard let decoded = prepare(args, start: start, traceId: traceId, callback: callback) else { return }
        let connection = self.connection
        // Awaited in the queue: a send completes once written, so the next call goes out after it.
        outbound.enqueue { [self] in
            do {
                try await VariadicCalls.send(connection, method, decoded)
                let end = Clock.nowMicros()
                host.postToMain { callback(CallResult(result: nil, nativeStartMicros: start, nativeEndMicros: end)) }
            } catch {
                host.postToMain { [self] in callback(failure(error, start: start, traceId: traceId)) }
            }
        }
    }

    func invoke(method: String, args: [Data], traceId: String, callback: @escaping (CallResult) -> Void) {
        let start = Clock.nowMicros()
        log(HubSession.debug, "invoke \(method) (\(args.count) args)", traceId)
        guard let decoded = prepare(args, start: start, traceId: traceId, callback: callback) else { return }
        let connection = self.connection
        let codec = self.codec
        let id = nextInvokeId
        nextInvokeId += 1
        pendingInvokes[id] = (start, callback)
        // Started in order but not awaited by the queue: an invoke only returns with its result.
        outbound.enqueue { [self] in
            Task {
                do {
                    // Optional return types: the client's typed invoke throws for a nil result
                    // unless the requested type itself is Optional.
                    let bytes: Data?
                    switch codec {
                    case .jsonArgs, .msgpackArgs:
                        let r: PassthroughValue? = try await VariadicCalls.invoke(connection, method, decoded)
                        bytes = try r.map { try codec.toBytes($0) }
                    case .jsonEnvelope:
                        let r: String? = try await VariadicCalls.invoke(connection, method, decoded)
                        bytes = r.map { Data($0.utf8) }
                    case .msgpackEnvelope:
                        let r: Data? = try await VariadicCalls.invoke(connection, method, decoded)
                        bytes = r
                    }
                    let end = Clock.nowMicros()
                    host.postToMain { [self] in
                        pendingInvokes.removeValue(forKey: id)?.callback(
                            CallResult(result: bytes, nativeStartMicros: start, nativeEndMicros: end))
                    }
                } catch {
                    host.postToMain { [self] in
                        // Gone when the connection was lost first: the caller already has its error.
                        guard let pending = pendingInvokes.removeValue(forKey: id) else { return }
                        pending.callback(failure(error, start: start, traceId: traceId))
                    }
                }
            }
        }
    }

    func streamStart(streamId: Int64, method: String, args: [Data], traceId: String,
                     callback: @escaping (CallResult) -> Void) {
        let start = Clock.nowMicros()
        log(HubSession.debug, "stream \(method) #\(streamId)", traceId)
        guard let decoded = prepare(args, start: start, traceId: traceId, callback: callback) else { return }
        switch codec {
        case .jsonArgs, .msgpackArgs:
            runStream(PassthroughValue.self, streamId: streamId, method: method, args: decoded, start: start,
                      traceId: traceId, callback: callback)
        case .jsonEnvelope:
            runStream(String.self, streamId: streamId, method: method, args: decoded, start: start,
                      traceId: traceId, callback: callback)
        case .msgpackEnvelope:
            runStream(Data.self, streamId: streamId, method: method, args: decoded, start: start,
                      traceId: traceId, callback: callback)
        }
    }

    func streamCancel(streamId: Int64) {
        // Missing when the stream already ended or the connection was lost.
        streams.removeValue(forKey: streamId)?.cancel()
    }

    /// The connection is gone (reconnecting, stopped or disposed): completes every pending
    /// invoke with notConnected and ends every native stream. Dart fails its own stream
    /// subscriptions on the same state change. Main thread.
    private func failInFlight(_ reason: String?) {
        let message = "Connection lost" + (reason.map { ": \($0)" } ?? "")
        let invokes = pendingInvokes.sorted { $0.key < $1.key }
        pendingInvokes.removeAll()
        let end = Clock.nowMicros()
        for (_, pending) in invokes {
            pending.callback(CallResult(result: nil, nativeStartMicros: pending.start, nativeEndMicros: end,
                                        errorKind: "notConnected", errorMessage: message))
        }
        let active = streams
        streams.removeAll()
        for (_, s) in active { s.cancel() }
    }

    // ------------------------------------------------------------------------------------

    private static func buildConnection(config: SessionConfig, host: SessionHost) -> HubConnection {
        let level = config.logLevel <= 3 ? LogLevel(rawValue: config.logLevel) : nil
        let logHandler = BridgeLogHandler(minLevel: config.logLevel) { [weak host] level, message in
            guard let host else { return }
            host.postToMain { host.onLog(level: level, message: message, traceId: nil) }
        }

        var options = HttpConnectionOptions()
        options.transport = .webSockets
        options.skipNegotiation = config.skipNegotiation
        options.headers = config.headers.isEmpty ? nil : config.headers
        options.logHandler = logHandler
        if config.hasAccessTokenProvider {
            // Called on every start, so each reconnect fetches a fresh token.
            options.accessTokenFactory = { [weak host] in try await host?.requestAccessToken() }
        }

        var builder = HubConnectionBuilder()
            .withUrl(url: config.url, options: options)
            .withHubProtocol(hubProtocol: config.messagePack ? .messagePack : .json)
            .withServerTimeout(serverTimeout: TimeInterval(config.serverTimeoutMs) / 1000)
            .withKeepAliveInterval(keepAliveInterval: TimeInterval(config.keepAliveIntervalMs) / 1000)
            .withLogHandler(logHandler: logHandler)
        // After withUrl(options:), which replaces the connection options. No level = no logs.
        if let level { builder = builder.withLogLevel(logLevel: level) }
        // No withAutomaticReconnect: the plugin's ReconnectController owns reconnects.
        return builder.build()
    }

    /// Registers handlers that run on the client's actor: they stamp the receive time,
    /// convert arguments to bytes and hand them to the data plane, nothing else.
    private static func register<T>(_ type: T.Type, on c: HubConnection, methods: [String], arities: [Int],
                                    envelope: Bool, codec: PayloadCodec, plane: DataPlane,
                                    log: @escaping @Sendable (String) -> Void) async {
        for (index, name) in methods.enumerated() {
            let arity = envelope ? 1 : arities[index]
            let emit: ([Any?]) -> Void = { values in
                let received = Clock.nowMicros()
                do {
                    let args = try values.map { try codec.toBytes($0) }
                    plane.add(.invocation(methodIndex: index, receivedAtMicros: received, args: args))
                } catch {
                    log("\(name): dropped a message whose arguments could not be converted: \(describeError(error))")
                }
            }
            switch arity {
            case 0: await c.on(name) { emit([]) }
            case 1: await c.on(name) { (a: T) in emit([a]) }
            case 2: await c.on(name) { (a: T, b: T) in emit([a, b]) }
            case 3: await c.on(name) { (a: T, b: T, d: T) in emit([a, b, d]) }
            case 4: await c.on(name) { (a: T, b: T, d: T, e: T) in emit([a, b, d, e]) }
            case 5: await c.on(name) { (a: T, b: T, d: T, e: T, f: T) in emit([a, b, d, e, f]) }
            case 6: await c.on(name) { (a: T, b: T, d: T, e: T, f: T, g: T) in emit([a, b, d, e, f, g]) }
            case 7: await c.on(name) { (a: T, b: T, d: T, e: T, f: T, g: T, h: T) in emit([a, b, d, e, f, g, h]) }
            case 8:
                await c.on(name) { (a: T, b: T, d: T, e: T, f: T, g: T, h: T, i: T) in
                    emit([a, b, d, e, f, g, h, i])
                }
            default:
                log("\(name): \(arity) arguments; the adapter binds at most 8. Not registered.")
            }
        }
    }

    private func runStream<E>(_ type: E.Type, streamId: Int64, method: String, args: [Any], start: Int64,
                              traceId: String, callback: @escaping (CallResult) -> Void) {
        let connection = self.connection
        let codec = self.codec
        let plane = self.dataPlane
        // Registered before anything runs, so a cancel or a lost connection always finds it.
        let active = ActiveStream()
        streams[streamId] = active
        let unregister = { [self] in
            if streams[streamId] === active { streams.removeValue(forKey: streamId) }
        }
        // Awaited in the queue: stream() returns once the StreamInvocation is written.
        outbound.enqueue { [self] in
            let result: any StreamResult<E>
            do {
                result = try await VariadicCalls.stream(connection, method, args)
            } catch {
                host.postToMain { [self] in
                    unregister()
                    callback(failure(error, start: start, traceId: traceId))
                }
                return
            }
            active.attach(result)
            Task {
                var end: String?
                do {
                    for try await item in result.stream {
                        if active.isCancelled { return }
                        plane.add(.streamItem(streamId: streamId, receivedAtMicros: Clock.nowMicros(),
                                              item: try codec.toBytes(item)))
                    }
                } catch {
                    end = describeError(error)
                }
                if active.isCancelled { return }
                plane.add(.streamEnd(streamId: streamId, receivedAtMicros: Clock.nowMicros(), error: end))
                self.host.postToMain(unregister)
            }
            let sent = Clock.nowMicros()
            host.postToMain { callback(CallResult(result: nil, nativeStartMicros: start, nativeEndMicros: sent)) }
        }
    }

    /// Fails fast while not connected (no queueing) and decodes Dart's argument bytes.
    private func prepare(_ args: [Data], start: Int64, traceId: String,
                         callback: (CallResult) -> Void) -> [Any]? {
        guard controller.isConnected else {
            callback(CallResult(result: nil, nativeStartMicros: start, nativeEndMicros: Clock.nowMicros(),
                                errorKind: "notConnected", errorMessage: "Not connected"))
            return nil
        }
        do {
            return try args.map { try codec.fromBytes($0) }
        } catch {
            log(HubSession.warning, "could not decode arguments: \(describeError(error))", traceId)
            callback(CallResult(result: nil, nativeStartMicros: start, nativeEndMicros: Clock.nowMicros(),
                                errorKind: "native", errorMessage: "Could not decode arguments: \(describeError(error))"))
            return nil
        }
    }

    /// Main thread.
    private func failure(_ error: Error, start: Int64, traceId: String) -> CallResult {
        let kind: String
        var message = describeError(error)
        if case SignalRError.invocationError(let serverMessage) = error {
            kind = "invocation"
            message = serverMessage
        } else if !controller.isConnected {
            kind = "notConnected"
        } else {
            kind = "native"
            if codec == .jsonArgs, case SignalRError.invalidOperation = error {
                message += " (the Swift client cannot pass scalar results or items in JSON args mode;"
                    + " return an object or array, or use envelope mode)"
            }
        }
        log(kind == "invocation" ? HubSession.info : HubSession.warning, "call failed (\(kind)): \(message)", traceId)
        return CallResult(result: nil, nativeStartMicros: start, nativeEndMicros: Clock.nowMicros(),
                          errorKind: kind, errorMessage: message)
    }

    private func log(_ level: Int, _ message: String, _ traceId: String?) {
        guard level >= config.logLevel else { return }
        host.postToMain { [host] in host.onLog(level: level, message: message, traceId: traceId) }
    }
}

/// Adapts the Swift client to the controller. Results are re-posted to the main thread.
///
/// The client ignores stop() until a start has reached it, and start and stop run as separate
/// Tasks, so a stop can overtake the start it should cancel. Each start therefore gets a
/// `StartAttempt`: stop() (or the start timeout) aborts it, and a start that still succeeds
/// after being aborted closes itself and reports the abort. A start waits for every earlier
/// start and stop; a stop runs at once so it can abort a start that is in progress.
private final class ClientConnection: ManagedConnection {
    private let connection: HubConnection
    private let host: SessionHost
    private let timeoutMs: Int64
    /// The last start or stop; the next start runs after it. Main thread.
    private var lastOp: Task<Void, Never>
    private var current: StartAttempt?

    init(connection: HubConnection, ready: Task<Void, Never>, host: SessionHost, timeoutMs: Int64) {
        self.connection = connection
        self.host = host
        self.timeoutMs = timeoutMs
        lastOp = ready
    }

    func start(onResult: @escaping (Error?) -> Void) {
        let attempt = StartAttempt()
        current = attempt
        let previous = lastOp
        let connection = self.connection
        let host = self.host
        if timeoutMs > 0 {
            let seconds = Double(timeoutMs) / 1000
            let timer = host.schedule(delayMs: timeoutMs) { [weak self] in
                // The client has no handshake timeout; stopping it rejects the pending handshake.
                if attempt.abort(ReconnectError.invalidState("Start timed out after \(seconds) s")) {
                    self?.stopClient(then: nil)
                }
            }
            attempt.onFinish = { timer.cancel() }
        }
        lastOp = Task {
            await previous.value
            if let aborted = attempt.abortedError {
                attempt.finish()
                host.postToMain { onResult(aborted) }
                return
            }
            do {
                try await connection.start()
                if attempt.finish() {
                    host.postToMain { onResult(nil) }
                } else {
                    // Aborted while the client was starting and the abort's stop came too early.
                    await connection.stop()
                    host.postToMain { onResult(attempt.abortedError) }
                }
            } catch {
                attempt.finish()
                let reported = attempt.abortedError ?? error
                host.postToMain { onResult(reported) }
            }
        }
    }

    func stop(onDone: @escaping () -> Void) {
        current?.abort(ReconnectError.invalidState("Stopped while connecting"))
        current = nil
        stopClient(then: onDone)
    }

    private func stopClient(then onDone: (() -> Void)?) {
        let stop = Task { [connection, host] in
            await connection.stop()
            if let onDone { host.postToMain(onDone) }
        }
        let previous = lastOp
        lastOp = Task {
            await previous.value
            await stop.value
        }
    }

    /// The Swift client does not expose the connection id.
    func connectionId() -> String? { nil }
}

/// One start of the client: either it finishes or it is aborted first, never both.
private final class StartAttempt: @unchecked Sendable {
    private let lock = NSLock()
    private var finished = false
    private var aborted: Error?
    /// Set on the main thread before the start runs; called once when the attempt settles.
    var onFinish: (() -> Void)?

    var abortedError: Error? {
        lock.lock()
        defer { lock.unlock() }
        return aborted
    }

    /// Returns false when the attempt was aborted first.
    @discardableResult
    func finish() -> Bool {
        lock.lock()
        let settled = finished || aborted != nil
        finished = true
        let done = settled ? nil : onFinish
        lock.unlock()
        done?()
        return !settled
    }

    /// Returns false when the attempt already finished or was aborted.
    @discardableResult
    func abort(_ error: Error) -> Bool {
        lock.lock()
        let settled = finished || aborted != nil
        if !settled { aborted = error }
        let done = settled ? nil : onFinish
        lock.unlock()
        done?()
        return !settled
    }
}

/// Lets the onClosed handler, registered before the controller exists, reach it. Main thread.
private final class ClosedForwarder: @unchecked Sendable {
    weak var controller: ReconnectController?
}

private struct HostScheduler: Scheduler {
    let host: SessionHost

    func schedule(delayMs: Int64, task: @escaping () -> Void) -> Cancellable {
        host.schedule(delayMs: delayMs, task: task)
    }

    func nowMs() -> Int64 { Clock.nowMicros() / 1000 }
}

/// A server-to-client stream. Registered before the client call starts; `attach` hands it
/// the client's stream, and `cancel` (before or after that) stops delivery and tells the server.
private final class ActiveStream: @unchecked Sendable {
    private let lock = NSLock()
    private var cancelled = false
    private var cancelResult: (() async -> Void)?

    var isCancelled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return cancelled
    }

    func attach<E>(_ result: any StreamResult<E>) {
        let cancelResult: () async -> Void = { await result.cancel() }
        lock.lock()
        let alreadyCancelled = cancelled
        if !alreadyCancelled { self.cancelResult = cancelResult }
        lock.unlock()
        if alreadyCancelled { Task { await cancelResult() } }
    }

    /// Also ends the client's stream: its cancel finishes the iterator with an error.
    func cancel() {
        lock.lock()
        let first = !cancelled
        cancelled = true
        let cancelResult = self.cancelResult
        self.cancelResult = nil
        lock.unlock()
        guard first, let cancelResult else { return }
        Task { await cancelResult() }
    }
}

/// Forwards the client's logs (hub and HTTP connection) to Dart, filtered by level.
private final class BridgeLogHandler: LogHandler {
    private let minLevel: Int
    private let sink: @Sendable (Int, String) -> Void

    init(minLevel: Int, sink: @escaping @Sendable (Int, String) -> Void) {
        self.minLevel = minLevel
        self.sink = sink
    }

    func log(logLevel: LogLevel, message: LogMessage, file: String, function: String, line: UInt) {
        guard logLevel.rawValue >= minLevel else { return }
        sink(logLevel.rawValue, "SignalRClient: \(message.description)")
    }
}
