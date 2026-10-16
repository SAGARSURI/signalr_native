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
    /// Not configurable on the Swift client (it has no handshake timeout); kept for parity.
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
    private var streams: [Int64: ActiveStream] = [:]
    private var cancelledBeforeStart: Set<Int64> = []

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
        let registration = Task {
            await connection.onClosed { [weak host] error in
                host?.postToMain { closed.controller?.onConnectionClosed(error) }
            }
            let log: @Sendable (String) -> Void = { [weak host] message in
                guard HubSession.warning >= logLevel, let host else { return }
                host.postToMain { host.onLog(level: HubSession.warning, message: message, traceId: nil) }
            }
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
        client = ClientConnection(connection: connection, ready: registration, host: host)

        controller = ReconnectController(
            connection: client,
            policy: config.reconnect,
            scheduler: HostScheduler(host: host),
            sink: { [weak self] kind, id, attempt, error in
                guard let self else { return }
                let level = kind == .disconnected && error != nil ? HubSession.warning : HubSession.info
                self.log(level, "State \(kind.rawValue)\(attempt > 0 ? " #\(attempt)" : "")\(error.map { ": \($0)" } ?? "")", nil)
                self.host.onState(kind: kind, connectionId: id, attempt: attempt, error: error)
            }
        )
        closed.controller = controller
    }

    func start(_ callback: @escaping (Error?) -> Void) { controller.start(callback) }

    func stop(_ callback: @escaping () -> Void) { controller.stop(callback) }

    func dispose(_ callback: @escaping () -> Void) {
        for (_, s) in streams { s.cancel() }
        streams.removeAll()
        controller.stop(callback)
    }

    func send(method: String, args: [Data], traceId: String, callback: @escaping (CallResult) -> Void) {
        let start = Clock.nowMicros()
        log(HubSession.debug, "send \(method) (\(args.count) args)", traceId)
        guard let decoded = prepare(args, start: start, traceId: traceId, callback: callback) else { return }
        let connection = self.connection
        Task {
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
                host.postToMain { callback(CallResult(result: bytes, nativeStartMicros: start, nativeEndMicros: end)) }
            } catch {
                host.postToMain { [self] in callback(failure(error, start: start, traceId: traceId)) }
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
        if let s = streams.removeValue(forKey: streamId) {
            s.cancel()
        } else {
            // The cancel can overtake the stream's own registration (see runStream).
            cancelledBeforeStart.insert(streamId)
        }
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
        Task {
            let result: any StreamResult<E>
            do {
                result = try await VariadicCalls.stream(connection, method, args)
            } catch {
                host.postToMain { [self] in callback(failure(error, start: start, traceId: traceId)) }
                return
            }
            let active = ActiveStream(result: result)
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
                self.host.postToMain { [weak self] in self?.streams.removeValue(forKey: streamId) }
            }
            let sent = Clock.nowMicros()
            host.postToMain { [self] in
                if cancelledBeforeStart.remove(streamId) != nil {
                    active.cancel()
                } else {
                    streams[streamId] = active
                }
                callback(CallResult(result: nil, nativeStartMicros: start, nativeEndMicros: sent))
            }
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
private final class ClientConnection: ManagedConnection {
    private let connection: HubConnection
    private let ready: Task<Void, Never>
    private let host: SessionHost

    init(connection: HubConnection, ready: Task<Void, Never>, host: SessionHost) {
        self.connection = connection
        self.ready = ready
        self.host = host
    }

    func start(onResult: @escaping (Error?) -> Void) {
        Task { [connection, ready, host] in
            await ready.value
            do {
                try await connection.start()
                host.postToMain { onResult(nil) }
            } catch {
                host.postToMain { onResult(error) }
            }
        }
    }

    func stop(onDone: @escaping () -> Void) {
        Task { [connection, host] in
            await connection.stop()
            host.postToMain(onDone)
        }
    }

    /// The Swift client does not expose the connection id.
    func connectionId() -> String? { nil }
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

/// A running server-to-client stream; `cancel` stops delivery and tells the server.
private final class ActiveStream: @unchecked Sendable {
    private let lock = NSLock()
    private var cancelled = false
    private let cancelResult: () async -> Void

    init<E>(result: any StreamResult<E>) {
        cancelResult = { await result.cancel() }
    }

    var isCancelled: Bool {
        lock.lock()
        defer { lock.unlock() }
        return cancelled
    }

    func cancel() {
        lock.lock()
        let first = !cancelled
        cancelled = true
        lock.unlock()
        guard first else { return }
        let cancelResult = self.cancelResult
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
