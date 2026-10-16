/// Flutter SignalR client backed by the official Java (Android) and Swift (iOS) clients.
///
/// ```dart
/// final hub = SignalRHub(SignalRConfig(
///   url: Uri.parse('https://example.com/hubs/trading'),
///   serverMethods: {'Quotes', 'OrderUpdate'},
///   payloadMode: PayloadMode.args, // or .envelope once the server sends pre-serialized payloads
/// ));
/// hub.on('Quotes').listen((m) => quotes.add(decodeQuote(m.payload)));
/// await hub.start();
/// ```
///
/// Switching between [PayloadMode.args] and [PayloadMode.envelope] is a config change
/// only: [HubMessage.payload] carries the same bytes in both modes as long as the server
/// serializes the payload the same way.
library;

export 'src/config.dart';
export 'src/exceptions.dart';
export 'src/hub.dart' show SignalRHub;
export 'src/hub_message.dart';
export 'src/hub_state.dart';
export 'src/observer.dart';
