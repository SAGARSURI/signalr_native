import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:signalr_native/signalr_native.dart';
import 'package:signalr_native/src/frame_codec.dart';
import 'package:signalr_native/src/platform.dart';

import 'fake_platform.dart';

Uint8List b(String s) => utf8.encode(s);

class RecordingObserver extends SignalRObserver {
  final List<(HubState, HubState)> states = [];
  final List<OutboundTrace> outbound = [];
  final List<InboundBatchTrace> inbound = [];
  final List<SignalRLogRecord> logs = [];

  @override
  void onStateChanged(HubState previous, HubState next) => states.add((previous, next));
  @override
  void onOutbound(OutboundTrace trace) => outbound.add(trace);
  @override
  void onInboundBatch(InboundBatchTrace trace) => inbound.add(trace);
  @override
  void onLog(SignalRLogRecord record) => logs.add(record);
}

SignalRConfig config({
  PayloadMode payloadMode = PayloadMode.args,
  SignalRObserver? observer,
  ReconnectPolicy reconnect = ReconnectPolicy.standard,
  Duration? invokeTimeout,
  Future<String?> Function()? token,
}) =>
    SignalRConfig(
      url: Uri.parse('https://example.com/hubs/trading'),
      serverMethods: {'Quotes', 'OrderUpdate', if (payloadMode == PayloadMode.args) 'Pair'},
      payloadMode: payloadMode,
      argCounts: {if (payloadMode == PayloadMode.args) 'Pair': 2},
      reconnect: reconnect,
      invokeTimeout: invokeTimeout,
      accessTokenProvider: token,
      observer: observer,
    );

void main() {
  late FakePlatform platform;
  late RecordingObserver observer;
  late SignalRHub hub;

  setUp(() {
    platform = FakePlatform();
    observer = RecordingObserver();
    hub = SignalRHub.withPlatform(config(observer: observer), platform);
  });

  group('config', () {
    test('rejects relative URLs, empty manifests and MessagePack without an encoder', () {
      expect(() => SignalRConfig(url: Uri.parse('/hub'), serverMethods: {'A'}), throwsArgumentError);
      expect(() => SignalRConfig(url: Uri.parse('https://x.io/h'), serverMethods: {}), throwsArgumentError);
      expect(
          () =>
              SignalRConfig(url: Uri.parse('https://x.io/h'), serverMethods: {'A'}, protocol: HubProtocol.messagePack),
          throwsArgumentError);
    });

    test('argCounts must name declared methods, and envelope methods take one argument', () {
      final url = Uri.parse('https://x.io/h');
      expect(() => SignalRConfig(url: url, serverMethods: {'A'}, argCounts: {'B': 2}), throwsArgumentError);
      expect(() => SignalRConfig(url: url, serverMethods: {'A'}, argCounts: {'A': 9}), throwsArgumentError);
      expect(
          () => SignalRConfig(url: url, serverMethods: {'A'}, payloadMode: PayloadMode.envelope, argCounts: {'A': 2}),
          throwsArgumentError);
      expect(SignalRConfig(url: url, serverMethods: {'A'}, argCounts: {'A': 2}).argCountOf('A'), 2);
    });

    test('on() only accepts declared server methods', () {
      expect(hub.on('Quotes'), isA<Stream<HubMessage>>());
      expect(() => hub.on('Nope'), throwsArgumentError);
    });

    test('maps payload mode, method order, arg counts and reconnect policy to the platform', () async {
      final h = SignalRHub.withPlatform(
        config(
          reconnect: const ReconnectPolicy.exponential(
            initial: Duration(milliseconds: 250),
            max: Duration(seconds: 20),
            maxAttempts: 12,
          ),
        ),
        platform,
      );
      await h.start();

      final c = platform.config!;
      expect(c.envelope, isFalse);
      expect(c.serverMethods, ['Quotes', 'OrderUpdate', 'Pair']);
      expect(c.serverMethodArgCounts, [1, 1, 2]);
      expect(c.messagePack, isFalse);
      expect(c.reconnect.exponential, isTrue);
      expect(c.reconnect.initialMs, 250);
      expect(c.reconnect.maxMs, 20000);
      expect(c.reconnect.maxAttempts, 12);
    });

    test('envelope mode reaches the platform as one flag', () async {
      final p = FakePlatform();
      await SignalRHub.withPlatform(config(payloadMode: PayloadMode.envelope), p).start();
      expect(p.config!.envelope, isTrue);
      expect(p.config!.serverMethodArgCounts, [1, 1]);
    });

    test('ReconnectPolicy.standard is 0/2/10/30 s', () async {
      await hub.start();
      expect(platform.config!.reconnect.delaysMs, [0, 2000, 10000, 30000]);
      expect(platform.config!.reconnect.exponential, isFalse);
    });
  });

  group('inbound', () {
    test('listener code and payload bytes are identical in args and envelope mode', () async {
      final same = b('{"s":"EURUSD","b":1.08451}');
      for (final mode in PayloadMode.values) {
        final p = FakePlatform();
        final h = SignalRHub.withPlatform(config(payloadMode: mode), p);
        await h.start();
        final got = <HubMessage>[];
        h.on('Quotes').listen(got.add);
        p.batch((FrameWriter(p.handle)..invocation(0, 100, [same])).build());
        expect(got.single.payload, same, reason: '$mode');
        expect(got.single.target, 'Quotes', reason: '$mode');
        expect(got.single.nativeReceivedAtMicros, 100, reason: '$mode');
      }
    });

    test('delivers synchronously, in order, and drops methods nobody listens to', () async {
      await hub.start();
      final seen = <String>[];
      hub.on('Quotes').listen((m) => seen.add(utf8.decode(m.payload)));

      platform.batch((FrameWriter(platform.handle)
            ..invocation(0, 1, [b('1')])
            ..invocation(1, 2, [b('ignored')])
            ..invocation(0, 3, [b('2')]))
          .build());

      expect(seen, ['1', '2']); // no await needed: sync delivery
      expect(observer.inbound.single.messages, 3);
    });

    test('payload throws for multi-argument methods; args works', () async {
      await hub.start();
      final got = <HubMessage>[];
      hub.on('Pair').listen(got.add);
      platform.batch((FrameWriter(platform.handle)..invocation(2, 1, [b('"A"'), b('2')])).build());
      expect(() => got.single.payload, throwsStateError);
      expect(got.single.args.map(utf8.decode), ['"A"', '2']);
    });

    test('a corrupt frame is logged and dropped without breaking later frames', () async {
      await hub.start();
      final got = <HubMessage>[];
      hub.on('Quotes').listen(got.add);
      platform.batch(ByteData(4));
      platform.batch((FrameWriter(platform.handle)..invocation(0, 1, [b('1')])).build());
      expect(got, hasLength(1));
      expect(observer.logs.single.level, SignalRLogLevel.error);
    });
  });

  group('outbound', () {
    test('send/invoke fail fast when not connected, and still report a trace', () async {
      await expectLater(hub.send('Ping'), throwsA(isA<HubNotConnectedException>()));
      expect(platform.calls, isNot(contains('send:Ping')));
      expect(observer.outbound.single.error, isA<HubNotConnectedException>());
      expect(observer.outbound.single.timing.traceId, isNotEmpty);
    });

    test('args are JSON-encoded per argument and results come back as bytes', () async {
      await hub.start();
      platform.onCall = (method, args) => NativeCallResult(
            nativeStartMicros: 1,
            nativeEndMicros: 5,
            result: b('{"orderId":"A1"}'),
          );
      final r = await hub.invoke('PlaceOrder', [
        {'symbol': 'EURUSD', 'qty': 1.5},
        'GTC',
      ]);

      expect(platform.sentArgs.single.map(utf8.decode), ['{"symbol":"EURUSD","qty":1.5}', '"GTC"']);
      expect(utf8.decode(r.payload!), '{"orderId":"A1"}');
      expect(r.trace.native, const Duration(microseconds: 4));
      final t = observer.outbound.single;
      expect(t.kind, OutboundKind.invoke);
      expect(t.method, 'PlaceOrder');
      expect(t.error, isNull);
      expect(t.timing.traceId, r.trace.traceId);
    });

    test('pre-encoded Uint8List args are passed through untouched', () async {
      await hub.start();
      final raw = b('{"already":"encoded"}');
      await hub.send('PlaceOrder', [raw]);
      expect(identical(platform.sentArgs.single.single, raw), isTrue);
    });

    test('native error kinds map to typed exceptions', () async {
      await hub.start();
      platform.onCall = (m, a) => const NativeCallResult(
          nativeStartMicros: 1, nativeEndMicros: 2, errorKind: NativeErrorKind.invocation, errorMessage: 'rejected');
      await expectLater(
          hub.invoke('PlaceOrder'),
          throwsA(isA<HubInvocationException>()
              .having((e) => e.message, 'message', 'rejected')
              .having((e) => e.traceId, 'traceId', isNotNull)));

      platform.onCall = (m, a) =>
          const NativeCallResult(nativeStartMicros: 1, nativeEndMicros: 2, errorKind: NativeErrorKind.notConnected);
      await expectLater(hub.send('X'), throwsA(isA<HubNotConnectedException>()));
    });

    test('invoke timeout', () async {
      final h = SignalRHub.withPlatform(config(invokeTimeout: const Duration(milliseconds: 20)), platform);
      await h.start();
      platform.pendingInvoke = Completer();
      await expectLater(h.invoke('Slow'), throwsA(isA<HubTimeoutException>()));
    });

    test('trace ids are unique per call', () async {
      await hub.start();
      await hub.send('A');
      await hub.send('B');
      final ids = observer.outbound.map((t) => t.timing.traceId).toSet();
      expect(ids, hasLength(2));
    });
  });

  group('streams', () {
    test('starts on listen, delivers items, completes on stream end', () async {
      await hub.start();
      final items = <String>[];
      final done = Completer<void>();
      hub.stream('Candles', ['EURUSD']).listen((m) => items.add(utf8.decode(m.payload)), onDone: done.complete);
      await pumpEventQueue();
      expect(platform.calls, contains('streamStart:1:Candles'));

      platform.batch((FrameWriter(platform.handle)
            ..streamItem(1, 1, b('1'))
            ..streamItem(1, 2, b('2'))
            ..streamEnd(1, 3))
          .build());
      await done.future;
      expect(items, ['1', '2']);
    });

    test('server error ends the stream with HubInvocationException', () async {
      await hub.start();
      final errors = <Object>[];
      hub.stream('Candles').listen((_) {}, onError: errors.add);
      await pumpEventQueue();
      platform.batch((FrameWriter(platform.handle)..streamEnd(1, 1, error: 'no such symbol')).build());
      await pumpEventQueue();
      expect(errors.single, isA<HubInvocationException>());
    });

    test('cancelling the subscription cancels the native stream', () async {
      await hub.start();
      final sub = hub.stream('Candles').listen((_) {});
      await pumpEventQueue();
      await sub.cancel();
      expect(platform.cancelledStreams, [1]);
    });

    test('connection loss errors active streams', () async {
      await hub.start();
      final errors = <Object>[];
      hub.stream('Candles').listen((_) {}, onError: errors.add);
      await pumpEventQueue();
      platform.state(const NativeState(NativeStateKind.reconnecting, attempt: 1, error: 'socket closed'));
      await pumpEventQueue();
      expect(errors.single, isA<HubNotConnectedException>());
    });
  });

  group('state', () {
    test('reconnect cycle is reported identically to the observer and the stream', () async {
      final seen = <HubState>[];
      hub.states.listen(seen.add);
      await hub.start();
      platform
        ..state(const NativeState(NativeStateKind.reconnecting, attempt: 1, error: 'socket closed'))
        ..state(const NativeState(NativeStateKind.reconnecting, attempt: 2, error: 'timeout'))
        ..state(const NativeState(NativeStateKind.connected, connectionId: 'c2'))
        ..state(const NativeState(NativeStateKind.disconnected, error: 'gave up'));

      expect(seen.map((s) => s.toString()), [
        'Connecting',
        'Connected(c1)',
        'Reconnecting(#1, socket closed)',
        'Reconnecting(#2, timeout)',
        'Connected(c2, reconnect)',
        'Disconnected(gave up)',
      ]);
      expect(observer.states.map((s) => s.$2), seen);
      expect((seen[4] as HubConnected).isReconnect, isTrue);
    });

    test('a fresh start after stop is not reported as a reconnect', () async {
      await hub.start();
      await hub.stop();
      await hub.start();
      expect((hub.state as HubConnected).isReconnect, isFalse);
    });

    test('start is a no-op when connected; create happens once', () async {
      await hub.start();
      await hub.start();
      expect(platform.calls.where((c) => c == 'create'), hasLength(1));
      expect(platform.calls.where((c) => c == 'start'), hasLength(1));
    });
  });

  group('lifecycle', () {
    test('access token requests go to the provider', () async {
      final h = SignalRHub.withPlatform(config(token: () async => 'jwt-1'), platform);
      await h.start();
      expect(platform.config!.hasAccessTokenProvider, isTrue);
      expect(await platform.listener!.accessToken(), 'jwt-1');
    });

    test('native logs below logLevel are filtered', () async {
      await hub.start();
      platform.listener!
        ..onLog(const NativeLog(0, 'debug noise', 1))
        ..onLog(const NativeLog(2, 'warn', 2, traceId: 't-1'));
      expect(observer.logs.single.message, 'warn');
      expect(observer.logs.single.traceId, 't-1');
      expect(observer.logs.single.source, LogSource.native);
    });

    test('dispose releases the native connection and blocks further use', () async {
      await hub.start();
      await hub.dispose();
      expect(platform.calls.last, 'dispose');
      expect(platform.listener, isNull);
      await expectLater(hub.send('X'), throwsStateError);
    });
  });
}
