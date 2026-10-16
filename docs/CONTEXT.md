# signalr_native — project context

Flutter plugin that wraps the **official** ASP.NET Core SignalR clients — Java on Android, Swift on iOS — for the trading app, where throughput, latency and tracing matter. This file is the hand-off for anyone (human or Claude) picking the work up: what was decided, why, what was measured, and where it stands.

Status as of 2026-10-16: **M1 (Dart layer), M2 (Android adapter) and M3 (iOS adapter) done and green** — analyze clean, 31 Dart + 16 Kotlin + 26 Swift tests pass, debug APK and iOS simulator app build · nothing has talked to a real hub yet (M4).

---

## 1. Architecture

```
Flutter app (BLoC / repositories)
        │  SignalRHub API (Dart)
        ▼
Dart package  ── SignalRHub · SignalRObserver (tracing) · FrameCodec (decoder)
        │
        ├── Control plane: Pigeon (create/start/stop/send/invoke/stream, state, logs, token)
        └── Data plane:    raw BinaryMessenger channel "signalr_native/data", batched binary frames
        ▼
Android: Kotlin HubSession ── official Java client (com.microsoft.signalr 10.0.13)
iOS:     Swift adapter     ── official Swift client (dotnet/signalr-client-swift, commit 3938376)
        ▼
ASP.NET Core hub (WebSockets; JSON now, MessagePack later)
```

## 2. Decisions (all agreed with Sagar)

| # | Decision | Why |
|---|---|---|
| D1 | **Reconnect is owned by the plugin on both platforms**; policy defined once in Dart (`none`, `standard` = 0/2/10/30 s, `delays`, `exponential` with jitter / maxAttempts / maxElapsed). The delay is waited **before** each attempt. | Java client has no auto-reconnect. Swift's built-in one attempts first and sleeps *after* failures, so the same delays fire at different times. A plugin loop gives identical semantics. |
| D2 | **Raw-bytes payload contract.** Dart receives wire bytes per argument; decoding to models happens in Dart (isolates, existing codecs). | Native type mapping is the expensive part, especially on iOS. |
| D3 | **One global `payloadMode`: `args` (default) or `envelope`.** All methods or none — no per-method switch. | Rollout will be all-or-nothing; per-method config was rejected as extra complexity. |
| D4 | **Envelope** = every hub method takes/returns exactly one pre-serialized argument: a JSON *string* with the JSON protocol, a `byte[]` with MessagePack. Listener code and `HubMessage.payload` bytes are identical in both modes, so switching is a config change. | Spike (a): 9–33× faster on iOS JSON, ~2,200× on iOS MessagePack snapshots. |
| D5 | **JSON now, MessagePack later**, via `protocol:` config only. | Server currently JSON. |
| D6 | **Every server method is declared in `serverMethods` and registered natively once at create**; native `off()` is never called; `hub.on()` only gates delivery in Dart. | The Swift client drops a whole JSON frame if any message in it targets an unregistered method. |
| D7 | **No queueing while disconnected/reconnecting** — `send`/`invoke`/`stream` fail fast with `HubNotConnectedException`. | Stale trading intents are worse than failed ones. |
| D8 | **v1 scope excludes** stateful reconnect, client→server streaming, client results. | Scope control (all are supported by one or both clients; deferral is a choice). |
| D9 | **Data plane batching**: native appends messages to a pending batch; the first message after a flush posts one flush to the platform thread; whatever arrives meanwhile joins it. No timer. | Zero added latency when idle, natural coalescing under tick bursts; one platform-channel hop per batch. |
| D10 | **Tracing built in**: per-call trace id + `OutboundTiming` (Dart→native→native end→Dart), per-batch `InboundBatchTrace` (count, bytes, oldest bridge latency, decode µs), native + plugin logs via `SignalRObserver`. Wall-clock µs timestamps on both sides. | Sentry / New Relic integration via a single observer. |
| D11 | **Void / null invoke results** on Android go through `NullResultProtocol` (wraps the client's HubProtocol and substitutes a sentinel for result-less completions). | The Java client's typed `invoke` never completes for void/null results (it only completes on a value). |
| D12 | Android binding types per (protocol, mode): JSON args `JsonElement`, JSON envelope `String`, MessagePack args `Object`, MessagePack envelope `byte[]`. | Proven in spike (a); `JsonNode` fails on scalar args; `JsonElement` keeps original number text. |

## 3. Server contract (for the backend team)

Required now (args mode):
- Keep hub method argument counts stable; declare them in the app (`argCounts`, default 1).
- **One object argument per method** — iOS JSON args mode cannot pass scalar arguments through (they bypass Decodable).
- **Exact prices as strings or scaled integers** — iOS JSON parses numbers to `Double` (`123456789.123456789123` → `123456789.12345679`).
- Invoke results in args mode on iOS JSON must be objects/arrays or null (scalar results can't be captured).
- **No `null` arguments** in iOS JSON args mode: the Swift client hands the handler `NSNull`, the cast fails and the message is dropped.
- iOS JSON args mode re-encodes payloads: number text and object key order can change (`100.0` → `100`); Android keeps the original text. Envelope mode is byte-exact on both.
- iOS MessagePack: **map keys must be strings** (integer keys throw in the client and drop the whole frame; a dropped completion leaves its invoke waiting for `invokeTimeout`).
- Envelope mode: every result/argument must really be a string (JSON) or `bin` (MessagePack); anything else drops the frame on iOS.

When switching to envelope:
- Every method sends/returns **one** argument: `JsonSerializer.Serialize(payload)` (JSON protocol) or `byte[]` (MessagePack).
- The inner serialization must equal what args mode produced, so app decoders keep working unchanged.

With MessagePack:
- **No `DateTime` / extension types** — the Java client rejects MessagePack extension types. Send epoch milliseconds.

## 4. Verified client facts (official sources + source code)

| | Java 10.0.13 | Swift (commit 3938376) |
|---|---|---|
| Auto reconnect | ✗ (only `onClosed`) | ✓ but attempt-then-sleep (see D1) |
| Transports | WebSockets, LongPolling (no SSE/fallback) | WebSockets, SSE, LongPolling |
| MessagePack | ✓ separate artifact `signalr-messagepack` | ✓ built in (`withHubProtocol(.messagePack)`) |
| Headers / token / skip negotiate | `withHeaders`, `withAccessTokenProvider(Single)`, `shouldSkipNegotiate` | `HttpConnectionOptions.headers / accessTokenFactory / skipNegotiation` |
| Timeouts | `withServerTimeout`, `withKeepAliveInterval`, `withHandshakeResponseTimeout` (ms) | `withServerTimeout`, `withKeepAliveInterval` (s) |
| Connection id | `getConnectionId()` | not exposed |
| Logging | SLF4J 1.7 | `withLogLevel` + `withLogHandler` (debug logs every payload — keep ≥ info in prod) |
| Min platform | Android API 21 (OkHttp 4) | iOS 14, Swift 5.10 |
| Client streaming / client results | ✓ / ✓ | ✓ / ✓ |
| Stateful reconnect | ✗ | ✓ |

Behavioural gotchas found in the spike (all have tests):
- iOS JSON: a message for an **unregistered target throws for the whole frame** (co-batched messages lost). Android drops only that message.
- iOS MessagePack: extra args silently ignored; missing args drop the frame.
- iOS JSON: scalar args arrive as `NSString`/`NSNumber`, not the registered Decodable type.
- Java MessagePack: extension types (timestamps) → binding failure.
- Java typed `invoke`: hangs on void/null results (fixed by D11).
- iOS typed `invoke`: throws on nil results (to handle in M3 by mapping that specific error to a null payload).

## 5. Spike (a) results — macOS 27, M-series, release builds (directional; on-device numbers pending spike b)

Per message, ns unless noted:

| Path | Android (JVM) | iOS |
|---|---|---|
| JSON typed model (baseline) | 693 | 6,134 |
| JSON args passthrough (`JsonElement` / `PassthroughValue`) | 711 | 28,551 |
| JSON envelope (String → UTF-8) | **505** | **3,285** |
| MessagePack typed model | 621 | 3,982 |
| MessagePack args passthrough | 797 | 38,406 |
| MessagePack envelope (bin → bytes) | **410** | **2,274** |
| 1,000-quote snapshot, JSON args | 0.35 ms | 22.5 ms |
| 1,000-quote snapshot, JSON envelope | **0.08 ms** | **0.69 ms** |
| 1,000-quote snapshot, MessagePack args | 0.33 ms | 40.9 ms |
| 1,000-quote snapshot, MessagePack envelope | **0.002 ms** | **0.018 ms** |

iOS cost is in the **parse** into a generic Decodable (≈26 µs of the 29 µs), not the re-encode: the Codable API has no "peek", so a schemaless type throws ~5–6 errors per field. Encode-only: JSONEncoder 1.5 µs, MsgpackWriter 0.26 µs.

## 6. Public Dart API (M1, implemented)

```dart
final hub = SignalRHub(SignalRConfig(
  url: Uri.parse('https://…/hubs/trading'),
  serverMethods: {'Quotes', 'OrderUpdate'},
  payloadMode: PayloadMode.args,            // or .envelope
  protocol: HubProtocol.json,               // .messagePack needs argsEncoder
  argCounts: {'Pair': 2},                   // args mode only; default 1
  accessTokenProvider: () async => token,   // called before every connect + reconnect
  headers: {...}, skipNegotiation: false,
  serverTimeout: Duration(seconds: 30), keepAliveInterval: Duration(seconds: 15),
  handshakeTimeout: Duration(seconds: 15), invokeTimeout: Duration(seconds: 10),
  reconnect: ReconnectPolicy.standard,      // .none / .delays([...]) / .exponential(...)
  logLevel: SignalRLogLevel.info,
  observer: myObserver,                     // onStateChanged / onLog / onOutbound / onInboundBatch
));

hub.on('Quotes').listen((m) => decode(m.payload));     // sync broadcast; m.args for multi-arg
await hub.start();
await hub.send('Subscribe', [{'symbols': ['EURUSD']}]);
final r = await hub.invoke('PlaceOrder', [order]);      // r.payload (nullable bytes), r.trace
hub.stream('Candles', ['EURUSD']).listen(...);          // cancel → server cancel
hub.states.listen(...);  // Disconnected / Connecting / Connected(isReconnect) / Reconnecting(attempt)
await hub.dispose();
```

Exceptions: `HubNotConnectedException`, `HubInvocationException`, `HubTimeoutException`, `HubConnectException`, `HubProtocolException` (all carry `traceId` where relevant).

## 7. Data frame layout (native → Dart, little-endian)

```
header   u8 'S' | u8 'R' | u8 version(1) | u8 0 | u32 handle | u32 messageCount
message  u8 kind (0 invocation, 1 stream item, 2 stream end) | u8 argCount | u16 0
         u32 id (method index in serverMethods, or stream id) | i64 receivedAtMicros
         kind 0/1: argCount × (u32 len, bytes)    kind 2: u32 errLen (0 = ok), UTF-8 error
```
Dart reads argument bytes as views into the received buffer (no copy). Golden-byte tests exist on both sides.

## 8. Repo layout

```
signalr_native/
  lib/signalr_native.dart            exports
  lib/src/config.dart                SignalRConfig, PayloadMode, ReconnectPolicy, HubArgsEncoder
  lib/src/hub.dart                   SignalRHub (state, routing, tracing, streams)
  lib/src/frame_codec.dart           data-plane decoder + reference FrameWriter
  lib/src/platform.dart              internal HubPlatform seam (fake in tests)
  lib/src/pigeon_platform.dart       real platform: Pigeon + raw data channel
  lib/src/{hub_message,hub_state,observer,exceptions}.dart
  lib/src/generated/messages.g.dart  (Pigeon output — generated by tool/verify*.sh, not committed)
  pigeons/messages.dart              Pigeon schema → Dart, Kotlin, Swift
  android/signalr-deps.gradle        deps applied onto the flutter-create plugin build file
  android/src/main/kotlin/.../SignalRNativePlugin.kt   Pigeon host impl, session host
  android/src/main/kotlin/.../core/  HubSession, PayloadCodec, NullResultProtocol,
                                     ReconnectController, ReconnectPolicy, DataPlane,
                                     FrameWriter, Clock
  android/src/test/kotlin/.../core/  FrameWriterTest, DataPlaneTest, ReconnectControllerTest
  example/                           bench app (connect, counts, invoke Echo, latency)
  test/                              31 Dart tests (fake platform, frame codec)
  tool/verify.sh                     M1: pigeon + analyze + Dart tests
  tool/verify_android.sh             M2: + plugin gradle from template, example scaffold,
                                     debug APK build, Kotlin unit tests
```

## 9. Toolchain (Sagar's Mac)

macOS 27 · Flutter 3.47.6 / Dart 3.13.5 · pigeon 29.0.10 · flutter_lints 6.0.0 · Swift 6.4 · JDK 25 on PATH (Gradle needs ≥ 9.1 for JDK 25; Android builds use Android Studio's JBR) · project lives at `~/Documents/Flutter/signalr_native` (spike archive in `~/Documents/Flutter/signalr_native_spike/`).

Build loop from a Claude cloud session (no shell on the Mac): Claude writes changed files into `~/Documents/Flutter/signalr_native-<tag>.zip`; Sagar runs `bash ~/Documents/Flutter/signalr-native-update.sh <tag>` (unzips over the project, runs `tool/verify_android.sh`); Claude reads `~/Documents/Flutter/signalr-native-<tag>.txt`. Staging from the Mac is limited to 7 folder levels below the connected folder.

Transfer gotcha: when files are pushed to the Mac from a Claude cloud session, overwriting an existing file can silently keep the old content — push under a **new file name** each time and verify checksums.

## 10. Milestones

- [x] Spike (a): passthrough types + envelope benchmarks (both platforms, real clients).
- [x] **M1** Dart API, Pigeon schema, data-plane codec, 31 tests — analyze clean, all green.
- [x] **M2** Android adapter — `HubSession`, codecs, `NullResultProtocol`, reconnect loop, data plane, plugin glue (Pigeon 29 generates Kotlin `suspend` host APIs; the plugin hops to `Dispatchers.Main`). Debug APK builds, 16 Kotlin tests green. Plugin build file comes from `flutter create --template=plugin` (AGP 9.1, Kotlin 2.4, minSdk 24) + `signalr-deps.gradle`.
- [x] **M3** iOS adapter — `HubSession` on the Swift client (pinned to commit 3938376 for the MessagePack UTF-8 fix), `PassthroughValue` + own `MsgpackWriter`/`MsgpackReader` (the client's are internal), `String`/`Data` for envelope, port of `ReconnectController`, `FrameWriter.swift` + `DataPlane`, log bridge, invoke asks for Optional result types so void/null results don't throw. **SPM only** (the client has no CocoaPods spec; Flutter 3.47 defaults to SPM); iOS 15+. `tool/verify_ios.sh` unit-tests the Flutter-free `Core/` from a throwaway package (26 tests), then builds the example for the simulator. Pigeon field `protocol` renamed `hubProtocol` (Swift keyword).
- iOS gaps vs Android: no connection id (not exposed); `handshakeTimeout` bounds the whole start (token + connect + handshake) because the client has no handshake timeout; JSON args mode can't carry scalar or null args/results/stream items; a null stream item crashes the client (JSON args and envelope); an invoke is not ordered against calls made right after it (sends and stream starts are); stream items can reach Dart after invocations that arrived later; a server close reason is not passed on.
- PR #1 review fixes (iOS): in-flight invokes fail with notConnected and native streams end when the connection drops (the client never fails them); start/stop race closed (a start aborted by stop or timeout closes itself, starts wait for earlier stops); outbound sends/stream starts go through one ordered queue; one shared JSON coder; **a new HubConnection per start/reconnect attempt** (the Swift client keeps an Azure SignalR redirect token for every later start of the same connection and stops calling the token provider, so reconnects to Azure failed). Azure SignalR Service needs `skipNegotiation: false`. Sagar's hubs run on Azure SignalR Service; the app uses SPM. Both controllers close a start that succeeds after stop finished.
- [ ] **M4** Example + local ASP.NET Core bench hub; spike (b) on-device latency vs the current Dart SignalR client; spike (c) chaos: airplane mode, server restart, token expiry mid-retry.

Open items:
- Android SLF4J → observer log bridge (currently plugin-level logs only; the Java client's internal logs need an SLF4J binding).
- Android cleartext HTTP for local bench testing (network security config in the example).
- iOS JSON args mode: scalar args, results and stream items are unsupported by the Swift client — document in the README / server contract.
