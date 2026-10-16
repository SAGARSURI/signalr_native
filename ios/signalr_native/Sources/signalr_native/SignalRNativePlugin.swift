import Flutter
import Foundation

/// Flutter glue: Pigeon host API → HubSessions, session events → Pigeon Flutter API,
/// data batches → the raw "signalr_native/data" channel. Mirrors SignalRNativePlugin.kt.
///
/// Pigeon 29 generates async host methods and calls them from a main-actor Task. Sessions
/// (and their reconnect controllers) are confined to the main thread, so every method that
/// touches one is either synchronous (Flutter calls those on the main thread) or @MainActor.
public final class SignalRNativePlugin: NSObject, FlutterPlugin, SignalRHostApi {
    /// Must match PigeonHubPlatform.dataChannelName.
    static let dataChannel = "signalr_native/data"

    fileprivate let messenger: FlutterBinaryMessenger
    fileprivate let flutterApi: SignalRFlutterApi
    private var sessions: [Int64: HubSession] = [:]
    private var nextHandle: Int64 = 1

    init(messenger: FlutterBinaryMessenger) {
        self.messenger = messenger
        self.flutterApi = SignalRFlutterApi(binaryMessenger: messenger)
    }

    public static func register(with registrar: FlutterPluginRegistrar) {
        let instance = SignalRNativePlugin(messenger: registrar.messenger())
        SignalRHostApiSetup.setUp(binaryMessenger: registrar.messenger(), api: instance)
        registrar.publish(instance)
    }

    public func detachFromEngine(for registrar: FlutterPluginRegistrar) {
        SignalRHostApiSetup.setUp(binaryMessenger: registrar.messenger(), api: nil)
        for session in sessions.values { session.dispose {} }
        sessions.removeAll()
    }

    // MARK: SignalRHostApi

    func create(config: ConnectConfigMsg) throws -> Int64 {
        let handle = nextHandle
        nextHandle += 1
        let r = config.reconnect
        let sessionConfig = SessionConfig(
            url: config.url,
            messagePack: config.hubProtocol == .messagePack,
            envelope: config.payloadMode == .envelope,
            headers: config.headers,
            skipNegotiation: config.skipNegotiation,
            hasAccessTokenProvider: config.hasAccessTokenProvider,
            serverTimeoutMs: config.serverTimeoutMs,
            keepAliveIntervalMs: config.keepAliveIntervalMs,
            handshakeTimeoutMs: config.handshakeTimeoutMs,
            reconnect: ReconnectPolicy(
                delaysMs: r.delaysMs,
                exponential: r.exponential,
                initialMs: r.initialMs,
                maxMs: r.maxMs,
                multiplier: r.multiplier,
                jitter: r.jitter,
                maxAttempts: r.maxAttempts,
                maxElapsedMs: r.maxElapsedMs
            ),
            serverMethods: config.serverMethods,
            serverMethodArgCounts: config.serverMethodArgCounts.map { Int($0) },
            logLevel: Int(config.logLevel)
        )
        sessions[handle] = HubSession(handle: handle, config: sessionConfig, host: Host(plugin: self, handle: handle))
        return handle
    }

    @MainActor func start(handle: Int64) async throws {
        let session = try session(handle)
        let error: Error? = await withCheckedContinuation { c in session.start { c.resume(returning: $0) } }
        if let error {
            throw PigeonError(code: "connect", message: describeError(error), details: nil)
        }
    }

    @MainActor func stop(handle: Int64) async throws {
        guard let session = sessions[handle] else { return }
        await withCheckedContinuation { c in session.stop { c.resume() } }
    }

    @MainActor func send(handle: Int64, method: String, args: [FlutterStandardTypedData], traceId: String) async throws -> CallResultMsg {
        try await call(handle) { s, done in s.send(method: method, args: args.map(\.data), traceId: traceId, callback: done) }
    }

    @MainActor func invoke(handle: Int64, method: String, args: [FlutterStandardTypedData], traceId: String) async throws -> CallResultMsg {
        try await call(handle) { s, done in s.invoke(method: method, args: args.map(\.data), traceId: traceId, callback: done) }
    }

    @MainActor func streamStart(handle: Int64, streamId: Int64, method: String, args: [FlutterStandardTypedData],
                                traceId: String) async throws -> CallResultMsg {
        try await call(handle) { s, done in
            s.streamStart(streamId: streamId, method: method, args: args.map(\.data), traceId: traceId, callback: done)
        }
    }

    func streamCancel(handle: Int64, streamId: Int64) throws {
        sessions[handle]?.streamCancel(streamId: streamId)
    }

    @MainActor func dispose(handle: Int64) async throws {
        guard let session = sessions.removeValue(forKey: handle) else { return }
        await withCheckedContinuation { c in session.dispose { c.resume() } }
    }

    // MARK: Helpers

    private func session(_ handle: Int64) throws -> HubSession {
        guard let session = sessions[handle] else {
            throw PigeonError(code: "unknown_handle", message: "No SignalR connection with handle \(handle)", details: nil)
        }
        return session
    }

    @MainActor private func call(_ handle: Int64,
                                 _ op: (HubSession, @escaping (CallResult) -> Void) -> Void) async throws -> CallResultMsg {
        let session = try session(handle)
        let r: CallResult = await withCheckedContinuation { c in op(session) { c.resume(returning: $0) } }
        return CallResultMsg(
            result: r.result.map { FlutterStandardTypedData(bytes: $0) },
            nativeStartMicros: r.nativeStartMicros,
            nativeEndMicros: r.nativeEndMicros,
            errorKind: r.errorKind,
            errorMessage: r.errorMessage
        )
    }
}

/// Per-connection bridge from a HubSession back to Flutter.
private final class Host: SessionHost, @unchecked Sendable {
    private weak var plugin: SignalRNativePlugin?
    private let handle: Int64

    init(plugin: SignalRNativePlugin, handle: Int64) {
        self.plugin = plugin
        self.handle = handle
    }

    func postToMain(_ task: @escaping () -> Void) {
        if Thread.isMainThread { task() } else { DispatchQueue.main.async(execute: task) }
    }

    func schedule(delayMs: Int64, task: @escaping () -> Void) -> Cancellable {
        let item = DispatchWorkItem(block: task)
        DispatchQueue.main.asyncAfter(deadline: .now() + .milliseconds(Int(delayMs)), execute: item)
        return WorkItemCancellable(item: item)
    }

    func sendFrame(_ frame: Data) {
        plugin?.messenger.send(onChannel: SignalRNativePlugin.dataChannel, message: frame)
    }

    func onState(kind: StateKind, connectionId: String?, attempt: Int, error: String?) {
        guard let api = plugin?.flutterApi else { return }
        let event = StateEventMsg(
            kind: {
                switch kind {
                case .disconnected: return .disconnected
                case .connecting: return .connecting
                case .connected: return .connected
                case .reconnecting: return .reconnecting
                }
            }(),
            connectionId: connectionId,
            attempt: Int64(attempt),
            error: error
        )
        let handle = self.handle
        Task { @MainActor in try? await api.onState(handle: handle, event: event) }
    }

    func onLog(level: Int, message: String, traceId: String?) {
        guard let api = plugin?.flutterApi else { return }
        let event = LogEventMsg(level: Int64(level), message: message, timeMicros: Clock.nowMicros(), traceId: traceId)
        let handle = self.handle
        Task { @MainActor in try? await api.onLog(handle: handle, event: event) }
    }

    @MainActor func requestAccessToken() async throws -> String? {
        guard let api = plugin?.flutterApi else {
            throw PigeonError(code: "detached", message: "Engine detached", details: nil)
        }
        return try await api.accessToken(handle: handle)
    }
}

private struct WorkItemCancellable: Cancellable {
    let item: DispatchWorkItem

    func cancel() { item.cancel() }
}
