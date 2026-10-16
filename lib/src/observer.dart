import 'config.dart';
import 'hub_message.dart';
import 'hub_state.dart';

/// Hooks for logging, tracing and metrics (Sentry, New Relic, …). All methods are
/// optional and run synchronously on the Dart side — keep them cheap; hand off to your
/// own queue for anything heavy.
///
/// Inbound traffic is reported per batch, not per message, so observing it costs
/// nothing proportional to the tick rate.
abstract class SignalRObserver {
  const SignalRObserver();

  void onStateChanged(HubState previous, HubState next) {}

  /// Logs from the native client (SLF4J on Android, `LogHandler` on iOS) and from the
  /// plugin, filtered by [SignalRConfig.logLevel].
  void onLog(SignalRLogRecord record) {}

  /// One per completed or failed send / invoke / stream start.
  void onOutbound(OutboundTrace trace) {}

  /// One per batch of server messages delivered to Dart.
  void onInboundBatch(InboundBatchTrace trace) {}
}

enum LogSource { native, plugin }

final class SignalRLogRecord {
  const SignalRLogRecord({
    required this.level,
    required this.message,
    required this.source,
    required this.timeMicros,
    this.traceId,
  });

  final SignalRLogLevel level;
  final String message;
  final LogSource source;
  final int timeMicros;
  final String? traceId;

  @override
  String toString() => '[${level.name}] ${source.name}: $message';
}

enum OutboundKind { send, invoke, stream }

final class OutboundTrace {
  const OutboundTrace({
    required this.kind,
    required this.method,
    required this.argBytes,
    required this.timing,
    this.error,
  });

  final OutboundKind kind;
  final String method;

  /// Total encoded size of the arguments.
  final int argBytes;
  final OutboundTiming timing;
  final Object? error;
}

final class InboundBatchTrace {
  const InboundBatchTrace({
    required this.messages,
    required this.bytes,
    required this.oldestBridgeLatency,
    required this.decodeMicros,
  });

  /// Messages in the batch (invocations + stream items).
  final int messages;

  /// Batch size on the bridge.
  final int bytes;

  /// Native receive → Dart decode for the oldest message in the batch: the worst
  /// queueing delay this batch saw.
  final Duration oldestBridgeLatency;

  /// Time spent decoding the batch frame in Dart.
  final int decodeMicros;
}
