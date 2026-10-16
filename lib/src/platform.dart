import 'dart:typed_data';

/// Internal seam between [SignalRHub] and the native side, so hub logic is testable
/// without a device. [PigeonHubPlatform] is the real implementation.
abstract class HubPlatform {
  Future<int> create(PlatformConfig config);
  Future<void> start(int handle);
  Future<void> stop(int handle);
  Future<NativeCallResult> send(int handle, String method, List<Uint8List> args, String traceId);
  Future<NativeCallResult> invoke(int handle, String method, List<Uint8List> args, String traceId);
  Future<NativeCallResult> streamStart(
      int handle, int streamId, String method, List<Uint8List> args, String traceId);
  Future<void> streamCancel(int handle, int streamId);
  Future<void> dispose(int handle);

  /// Routes native events for [handle] to [listener] until [unbind].
  void bind(int handle, PlatformListener listener);
  void unbind(int handle);
}

abstract class PlatformListener {
  /// One data-plane batch (see FrameCodec), already routed by handle.
  void onBatch(ByteData data);
  void onState(NativeState state);
  void onLog(NativeLog log);
  Future<String?> accessToken();
}

final class PlatformConfig {
  const PlatformConfig({
    required this.url,
    required this.messagePack,
    required this.headers,
    required this.skipNegotiation,
    required this.hasAccessTokenProvider,
    required this.serverTimeoutMs,
    required this.keepAliveIntervalMs,
    required this.handshakeTimeoutMs,
    required this.reconnect,
    required this.envelope,
    required this.serverMethods,
    required this.serverMethodArgCounts,
    required this.logLevel,
  });

  final String url;
  final bool messagePack;
  final Map<String, String> headers;
  final bool skipNegotiation;
  final bool hasAccessTokenProvider;
  final int serverTimeoutMs;
  final int keepAliveIntervalMs;
  final int handshakeTimeoutMs;
  final PlatformReconnect reconnect;

  final bool envelope;

  /// Index in this list = method id in data frames.
  final List<String> serverMethods;

  /// Parallel to [serverMethods].
  final List<int> serverMethodArgCounts;
  final int logLevel;
}

final class PlatformReconnect {
  const PlatformReconnect({
    required this.delaysMs,
    required this.exponential,
    this.initialMs = 0,
    this.maxMs = 0,
    this.multiplier = 1,
    this.jitter = 0,
    this.maxAttempts,
    this.maxElapsedMs,
  });

  final List<int> delaysMs;
  final bool exponential;
  final int initialMs;
  final int maxMs;
  final double multiplier;
  final double jitter;
  final int? maxAttempts;
  final int? maxElapsedMs;
}

enum NativeErrorKind { notConnected, invocation, native }

final class NativeCallResult {
  const NativeCallResult({
    required this.nativeStartMicros,
    required this.nativeEndMicros,
    this.result,
    this.errorKind,
    this.errorMessage,
  });

  final Uint8List? result;
  final int nativeStartMicros;
  final int nativeEndMicros;
  final NativeErrorKind? errorKind;
  final String? errorMessage;
}

enum NativeStateKind { disconnected, connecting, connected, reconnecting }

final class NativeState {
  const NativeState(this.kind, {this.connectionId, this.attempt = 0, this.error});
  final NativeStateKind kind;
  final String? connectionId;
  final int attempt;
  final String? error;
}

final class NativeLog {
  const NativeLog(this.level, this.message, this.timeMicros, {this.traceId});

  /// SignalRLogLevel.index
  final int level;
  final String message;
  final int timeMicros;
  final String? traceId;
}
