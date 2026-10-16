/// Connection state, identical on Android and iOS.
sealed class HubState {
  const HubState();
}

/// Not connected. [error] is set when the connection was lost rather than stopped,
/// including after the reconnect policy gave up.
final class HubDisconnected extends HubState {
  const HubDisconnected([this.error]);
  final Object? error;

  @override
  String toString() => error == null ? 'Disconnected' : 'Disconnected($error)';
}

final class HubConnecting extends HubState {
  const HubConnecting();

  @override
  String toString() => 'Connecting';
}

/// [connectionId] is null on iOS: the Swift client does not expose it.
final class HubConnected extends HubState {
  const HubConnected({this.connectionId, this.isReconnect = false});
  final String? connectionId;

  /// True when this connection was restored by the reconnect policy. Server-side group
  /// membership does not survive a reconnect — re-join groups when this is true.
  final bool isReconnect;

  @override
  String toString() => 'Connected(${connectionId ?? '-'}${isReconnect ? ', reconnect' : ''})';
}

/// Waiting for or making reconnect attempt [attempt] (1-based) after [error].
final class HubReconnecting extends HubState {
  const HubReconnecting({required this.attempt, this.error});
  final int attempt;
  final Object? error;

  @override
  String toString() => 'Reconnecting(#$attempt, $error)';
}
