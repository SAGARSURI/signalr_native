import 'dart:convert';
import 'dart:typed_data';

import 'observer.dart';

/// Wire protocol between the native client and the hub.
enum HubProtocol {
  /// SignalR JSON hub protocol. Supported by both native clients.
  json,

  /// SignalR MessagePack hub protocol. Android adds `com.microsoft.signalr.messagepack`.
  messagePack,
}

/// How hub method payloads travel, for every method in both directions.
///
/// Listeners don't see the difference: [HubMessage.payload] holds the same bytes in
/// both modes when the server serializes the payload the same way, so switching is a
/// one-line config change.
enum PayloadMode {
  /// Compatibility path, works with any server. The native client decodes each argument
  /// into a generic tree and re-encodes it for Dart. Slower on iOS.
  args,

  /// Fast path. Every hub method takes and returns exactly one already-serialized
  /// argument: a JSON string with [HubProtocol.json], a `byte[]` with
  /// [HubProtocol.messagePack]. The native client forwards the bytes untouched.
  envelope,
}

/// When and how often to reconnect after an unexpected disconnect.
///
/// Applied identically on both platforms: mapped to a `RetryPolicy` on iOS and run by the
/// plugin's own reconnect loop on Android (the Java client has no automatic reconnect).
sealed class ReconnectPolicy {
  const ReconnectPolicy._();

  /// Never reconnect automatically.
  static const ReconnectPolicy none = DelaysReconnectPolicy._([]);

  /// The SignalR default: wait 0, 2, 10 and 30 seconds, then give up.
  static const ReconnectPolicy standard = DelaysReconnectPolicy._([
    Duration.zero,
    Duration(seconds: 2),
    Duration(seconds: 10),
    Duration(seconds: 30),
  ]);

  /// One attempt per delay, then give up.
  const factory ReconnectPolicy.delays(List<Duration> delays) = DelaysReconnectPolicy._;

  /// `initial * multiplier^attempt`, capped at [max], with ±[jitter] randomisation.
  /// Stops after [maxAttempts] attempts or [maxElapsed] total time, whichever comes first;
  /// both null means retry forever.
  const factory ReconnectPolicy.exponential({
    Duration initial,
    Duration max,
    double multiplier,
    double jitter,
    int? maxAttempts,
    Duration? maxElapsed,
  }) = ExponentialReconnectPolicy._;
}

/// See [ReconnectPolicy.delays].
final class DelaysReconnectPolicy extends ReconnectPolicy {
  const DelaysReconnectPolicy._(this.delays) : super._();

  final List<Duration> delays;
}

/// See [ReconnectPolicy.exponential].
final class ExponentialReconnectPolicy extends ReconnectPolicy {
  const ExponentialReconnectPolicy._({
    this.initial = const Duration(milliseconds: 500),
    this.max = const Duration(seconds: 30),
    this.multiplier = 2.0,
    this.jitter = 0.2,
    this.maxAttempts,
    this.maxElapsed,
  })  : assert(multiplier >= 1.0),
        assert(jitter >= 0.0 && jitter < 1.0),
        super._();

  final Duration initial;
  final Duration max;
  final double multiplier;
  final double jitter;
  final int? maxAttempts;
  final Duration? maxElapsed;
}

/// Minimum level of native client logs forwarded to [SignalRObserver.onLog].
enum SignalRLogLevel { debug, info, warning, error, none }

/// Encodes outbound arguments ([SignalRHub.send], [SignalRHub.invoke]) to wire bytes.
///
/// The bytes must match [SignalRConfig.protocol]: JSON text for [HubProtocol.json],
/// MessagePack for [HubProtocol.messagePack]. They are what an envelope method receives
/// verbatim, and what an args method's native client decodes into a generic value.
abstract interface class HubArgsEncoder {
  Uint8List encode(Object? value);
}

/// Default [HubArgsEncoder] for [HubProtocol.json]: `jsonEncode` → UTF-8.
/// Values that are already [Uint8List] are passed through as pre-encoded JSON.
final class JsonArgsEncoder implements HubArgsEncoder {
  const JsonArgsEncoder();

  @override
  Uint8List encode(Object? value) {
    if (value is Uint8List) return value;
    return utf8.encode(jsonEncode(value));
  }
}

/// Connection settings. Immutable; create a new [SignalRHub] to change them.
final class SignalRConfig {
  SignalRConfig({
    required this.url,
    required Set<String> serverMethods,
    this.payloadMode = PayloadMode.args,
    Map<String, int> argCounts = const {},
    this.protocol = HubProtocol.json,
    HubArgsEncoder? argsEncoder,
    this.accessTokenProvider,
    this.headers = const {},
    this.skipNegotiation = false,
    this.serverTimeout = const Duration(seconds: 30),
    this.keepAliveInterval = const Duration(seconds: 15),
    this.handshakeTimeout = const Duration(seconds: 15),
    this.reconnect = ReconnectPolicy.standard,
    this.invokeTimeout,
    this.logLevel = SignalRLogLevel.info,
    this.observer,
  })  : serverMethods = Set.unmodifiable(serverMethods),
        argCounts = Map.unmodifiable(argCounts),
        argsEncoder = argsEncoder ??
            (protocol == HubProtocol.json
                ? const JsonArgsEncoder()
                : throw ArgumentError.value(
                    null, 'argsEncoder', 'A MessagePack HubArgsEncoder is required for HubProtocol.messagePack')) {
    if (!url.hasScheme || !url.hasAuthority) {
      throw ArgumentError.value(url, 'url', 'Must be an absolute http(s) URL');
    }
    if (serverMethods.isEmpty) {
      throw ArgumentError.value(serverMethods, 'serverMethods', 'Declare every method the server calls');
    }
    for (final MapEntry(:key, :value) in argCounts.entries) {
      if (!serverMethods.contains(key)) {
        throw ArgumentError.value(key, 'argCounts', 'Not in serverMethods');
      }
      if (value < 0 || value > 8) {
        throw ArgumentError.value(value, 'argCounts[$key]', 'The native clients bind 0 to 8 arguments');
      }
      if (payloadMode == PayloadMode.envelope && value != 1) {
        throw ArgumentError.value(value, 'argCounts[$key]', 'Envelope methods take exactly one argument');
      }
    }
    if (keepAliveInterval * 2 > serverTimeout) {
      throw ArgumentError('serverTimeout should be at least double keepAliveInterval');
    }
  }

  final Uri url;

  /// Every method the server invokes on this client.
  ///
  /// All of them are registered natively at connect and never unregistered: the Swift
  /// client drops a whole JSON frame if any message in it targets an unregistered method.
  /// Listening and cancelling via [SignalRHub.on] only gates delivery in Dart.
  final Set<String> serverMethods;

  /// Applies to every method, server-to-client and client-to-server.
  final PayloadMode payloadMode;

  /// Argument count per server method for [PayloadMode.args]; methods not listed take
  /// one. The native clients reject messages whose count differs. Must be empty or all
  /// 1 for [PayloadMode.envelope].
  final Map<String, int> argCounts;

  int argCountOf(String method) => argCounts[method] ?? 1;

  final HubProtocol protocol;
  final HubArgsEncoder argsEncoder;

  /// Called before every connect and reconnect attempt. Return null for no token.
  final Future<String?> Function()? accessTokenProvider;

  final Map<String, String> headers;

  /// Connect straight over WebSockets without the negotiate request.
  /// Not supported by Azure SignalR Service.
  final bool skipNegotiation;

  final Duration serverTimeout;
  final Duration keepAliveInterval;
  final Duration handshakeTimeout;
  final ReconnectPolicy reconnect;

  /// Default timeout for [SignalRHub.invoke]; null waits indefinitely.
  final Duration? invokeTimeout;

  final SignalRLogLevel logLevel;
  final SignalRObserver? observer;
}
