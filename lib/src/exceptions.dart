/// Base type for errors thrown by this package.
sealed class SignalRException implements Exception {
  const SignalRException(this.message, {this.traceId});
  final String message;

  /// The outbound call's trace id, when the error belongs to one.
  final String? traceId;

  @override
  String toString() => '$runtimeType: $message${traceId == null ? '' : ' [trace $traceId]'}';
}

/// [SignalRHub.send], [SignalRHub.invoke] or [SignalRHub.stream] was called while not
/// connected. Calls are not queued during reconnects: stale trading intents are worse
/// than failed ones.
final class HubNotConnectedException extends SignalRException {
  const HubNotConnectedException(super.message, {super.traceId});
}

/// The hub method threw, or the server returned an error completion.
final class HubInvocationException extends SignalRException {
  const HubInvocationException(super.message, {super.traceId});
}

/// [SignalRConfig.invokeTimeout] (or the per-call timeout) elapsed. The server may still
/// execute the call.
final class HubTimeoutException extends SignalRException {
  const HubTimeoutException(super.message, {super.traceId});
}

/// Connecting failed: negotiate, transport, handshake or token provider error.
final class HubConnectException extends SignalRException {
  const HubConnectException(super.message);
}

/// The plugin and the native side disagree (unexpected frame, unknown handle).
/// Indicates a bug — please report it.
final class HubProtocolException extends SignalRException {
  const HubProtocolException(super.message);
}
