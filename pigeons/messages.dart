// Control plane between Dart and the native adapters. Generate with:
//   dart run pigeon --input pigeons/messages.dart
//
// High-volume server messages do NOT go through here: they use the batched binary
// data channel described in lib/src/frame_codec.dart.
import 'package:pigeon/pigeon.dart';

@ConfigurePigeon(PigeonOptions(
  dartOut: 'lib/src/generated/messages.g.dart',
  dartPackageName: 'signalr_native',
  kotlinOut: 'android/src/main/kotlin/com/faciletech/signalr_native/Messages.g.kt',
  kotlinOptions: KotlinOptions(package: 'com.faciletech.signalr_native'),
  swiftOut: 'ios/signalr_native/Sources/signalr_native/Messages.g.swift',
))
enum ProtocolMsg { json, messagePack }

enum PayloadModeMsg { envelope, args }

enum StateKindMsg { disconnected, connecting, connected, reconnecting }

class ReconnectMsg {
  ReconnectMsg({
    required this.delaysMs,
    required this.exponential,
    required this.initialMs,
    required this.maxMs,
    required this.multiplier,
    required this.jitter,
    this.maxAttempts,
    this.maxElapsedMs,
  });

  /// Used when [exponential] is false. Empty = never reconnect.
  List<int> delaysMs;
  bool exponential;
  int initialMs;
  int maxMs;
  double multiplier;
  double jitter;
  int? maxAttempts;
  int? maxElapsedMs;
}

class ConnectConfigMsg {
  ConnectConfigMsg({
    required this.url,
    required this.hubProtocol,
    required this.headers,
    required this.skipNegotiation,
    required this.hasAccessTokenProvider,
    required this.serverTimeoutMs,
    required this.keepAliveIntervalMs,
    required this.handshakeTimeoutMs,
    required this.reconnect,
    required this.payloadMode,
    required this.serverMethods,
    required this.serverMethodArgCounts,
    required this.logLevel,
  });

  String url;
  /// Not `protocol`: Pigeon emits field names verbatim and that is a Swift keyword.
  ProtocolMsg hubProtocol;
  Map<String, String> headers;
  bool skipNegotiation;
  bool hasAccessTokenProvider;
  int serverTimeoutMs;
  int keepAliveIntervalMs;
  int handshakeTimeoutMs;
  ReconnectMsg reconnect;
  /// Applies to every method in both directions.
  PayloadModeMsg payloadMode;

  /// Registered natively at create time. Position in this list is the method's id in
  /// data frames.
  List<String> serverMethods;

  /// Parallel to [serverMethods]; always 1 for envelope.
  List<int> serverMethodArgCounts;

  /// SignalRLogLevel.index; native drops anything below it before crossing the bridge.
  int logLevel;
}

class CallResultMsg {
  CallResultMsg({
    required this.nativeStartMicros,
    required this.nativeEndMicros,
    this.result,
    this.errorKind,
    this.errorMessage,
  });

  /// Return value bytes (invoke only); null for void/null results and for send.
  Uint8List? result;
  int nativeStartMicros;
  int nativeEndMicros;

  /// null on success; otherwise 'notConnected', 'invocation' or 'native'.
  String? errorKind;
  String? errorMessage;
}

class StateEventMsg {
  StateEventMsg({required this.kind, required this.attempt, this.connectionId, this.error});

  StateKindMsg kind;
  String? connectionId;

  /// Reconnect attempt (1-based) for [StateKindMsg.reconnecting]; 0 otherwise.
  int attempt;
  String? error;
}

class LogEventMsg {
  LogEventMsg({required this.level, required this.message, required this.timeMicros, this.traceId});

  int level;
  String message;
  int timeMicros;
  String? traceId;
}

@HostApi()
abstract class SignalRHostApi {
  /// Builds a native HubConnection and registers every server method. Returns a handle.
  int create(ConnectConfigMsg config);

  @async
  void start(int handle);

  @async
  void stop(int handle);

  @async
  CallResultMsg send(int handle, String method, List<Uint8List> args, String traceId);

  @async
  CallResultMsg invoke(int handle, String method, List<Uint8List> args, String traceId);

  /// Items and completion arrive on the data channel tagged with [streamId].
  @async
  CallResultMsg streamStart(int handle, int streamId, String method, List<Uint8List> args, String traceId);

  void streamCancel(int handle, int streamId);

  /// Stops if needed and releases the native connection.
  @async
  void dispose(int handle);
}

@FlutterApi()
abstract class SignalRFlutterApi {
  @async
  String? accessToken(int handle);

  void onState(int handle, StateEventMsg event);

  void onLog(int handle, LogEventMsg event);
}
