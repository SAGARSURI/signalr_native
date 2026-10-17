# signalr_native

Flutter SignalR client backed by the **official** ASP.NET Core SignalR clients: Java (`com.microsoft.signalr`) on Android, Swift (`dotnet/signalr-client-swift`) on iOS. Built for high-throughput trading streams: payloads cross the platform bridge as raw bytes in batched binary frames, and every call is traceable.

> **Status: work in progress.** The Dart API and the Android adapter are done and tested. iOS is next. Nothing has been verified against a live hub yet. See [docs/CONTEXT.md](docs/CONTEXT.md) for decisions, measurements and the roadmap.

## Usage

```dart
final hub = SignalRHub(SignalRConfig(
  url: Uri.parse('https://example.com/hubs/trading'),
  serverMethods: {'Quotes', 'OrderUpdate'},
  payloadMode: PayloadMode.args, // or .envelope once the server sends pre-serialized payloads
  reconnect: ReconnectPolicy.standard,
  observer: MyObserver(),        // state, logs, per-call timing, per-batch inbound metrics
));

hub.on('Quotes').listen((m) => decodeQuote(m.payload)); // raw bytes; decode in Dart
await hub.start();
final result = await hub.invoke('PlaceOrder', [order]);
```

- **Payload modes.** `args` works with any server. `envelope` expects each hub method to take and return one already-serialized argument: a JSON string, or `byte[]` with MessagePack. That's much faster on iOS. Listener code is the same in both modes.
- **Reconnect** is run by the plugin on both platforms, with the same timing. The delay is waited before each attempt, matching the .NET and JS clients.
- **Calls while disconnected** fail fast. Nothing is queued.

## Development

```bash
dart run pigeon --input pigeons/messages.dart   # generated code is not committed
bash tool/verify.sh                             # pigeon + analyze + Dart tests
bash tool/verify_android.sh                     # + example scaffold, debug APK, Kotlin unit tests
git config core.hooksPath .githooks             # once per clone: format, analyze, Dart tests before each push
```

CI (`.github/workflows/ci.yml`) runs `tool/ci.sh dart`, `android` and `ios` on every pull request. Format with `dart format --page-width=120 lib test pigeons example/lib`.

Requirements: Flutter 3.47+, Android Studio (its JBR is used for Gradle). iOS 14+ once the iOS adapter lands.

## Layout

| Path | What |
|---|---|
| `lib/` | Dart API, data-frame decoder, Pigeon platform |
| `pigeons/messages.dart` | Control-plane schema (Dart, Kotlin, Swift) |
| `android/src/main/kotlin/…/core/` | `HubSession`, payload codecs, reconnect controller, data plane |
| `android/src/test/` | Kotlin unit tests (frame layout, batching, reconnect) |
| `test/` | Dart tests (fake platform, frame codec) |
| `example/` | Bench app: connect, per-method counts, invoke, bridge latency |
| `docs/CONTEXT.md` | Decisions, spike results, server contract, roadmap |

## License

[MIT](LICENSE)
