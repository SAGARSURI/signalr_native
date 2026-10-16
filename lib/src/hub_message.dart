import 'dart:typed_data';

import 'config.dart';

/// A server-to-client invocation, or one item of a server stream.
///
/// [args] are wire bytes per argument, encoded per [protocol]: UTF-8 JSON text for
/// [HubProtocol.json], MessagePack for [HubProtocol.messagePack]. They are views into
/// the batch buffer received from the platform — copy them if you keep them long-term.
final class HubMessage {
  const HubMessage({
    required this.target,
    required this.args,
    required this.protocol,
    required this.nativeReceivedAtMicros,
  });

  /// Hub method name (or the stream's hub method for stream items).
  final String target;

  final List<Uint8List> args;
  final HubProtocol protocol;

  /// Wall-clock time (microseconds since epoch) at which the native client handed this
  /// message to the plugin. `DateTime.now().microsecondsSinceEpoch - this` is the time
  /// spent crossing the platform bridge plus queueing in Dart.
  final int nativeReceivedAtMicros;

  /// The single argument. Same bytes in [PayloadMode.envelope] and
  /// [PayloadMode.args] (for single-argument methods).
  Uint8List get payload {
    if (args.length != 1) {
      throw StateError('$target has ${args.length} arguments; use args[i]');
    }
    return args[0];
  }

  @override
  String toString() => 'HubMessage($target, ${args.length} args, '
      '${args.fold<int>(0, (n, a) => n + a.length)} bytes)';
}

/// Result of [SignalRHub.invoke].
final class HubResult {
  const HubResult({
    required this.payload,
    required this.protocol,
    required this.trace,
  });

  /// Wire bytes of the return value, or null for a void hub method / null result.
  final Uint8List? payload;
  final HubProtocol protocol;

  /// Timing of this call; also delivered to [SignalRObserver.onOutbound].
  final OutboundTiming trace;
}

/// Timing for one outbound call, split at the platform bridge.
final class OutboundTiming {
  const OutboundTiming({
    required this.traceId,
    required this.dartStartMicros,
    required this.nativeStartMicros,
    required this.nativeEndMicros,
    required this.dartEndMicros,
  });

  /// Unique per call; also attached to native log lines for this call.
  final String traceId;

  /// Wall-clock micros since epoch at each boundary. Native values are 0 when the call
  /// failed before reaching the native side.
  final int dartStartMicros;
  final int nativeStartMicros;
  final int nativeEndMicros;
  final int dartEndMicros;

  /// Native client time: serialize + send, plus the server round trip for invoke.
  Duration get native => Duration(microseconds: nativeEndMicros - nativeStartMicros);

  /// Total time as seen by the caller.
  Duration get total => Duration(microseconds: dartEndMicros - dartStartMicros);

  /// Time spent crossing the platform bridge, both directions combined.
  Duration get bridge => total - native;
}
