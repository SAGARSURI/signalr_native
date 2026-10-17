import 'dart:async';
import 'dart:math';

import 'package:flutter/foundation.dart';
import 'package:flutter/services.dart' show PlatformException;

import 'config.dart';
import 'exceptions.dart';
import 'frame_codec.dart';
import 'hub_message.dart';
import 'hub_state.dart';
import 'observer.dart';
import 'pigeon_platform.dart';
import 'platform.dart';

/// One SignalR connection, backed by the official native client of the platform.
///
/// Every method in [SignalRConfig.serverMethods] is registered natively when the
/// connection is created and stays registered for its lifetime; [on] only controls
/// delivery in Dart.
final class SignalRHub {
  SignalRHub(SignalRConfig config) : this._(config, PigeonHubPlatform.instance);

  @visibleForTesting
  SignalRHub.withPlatform(SignalRConfig config, HubPlatform platform) : this._(config, platform);

  SignalRHub._(this.config, this._platform)
      : _methodNames = config.serverMethods.toList(growable: false),
        _tracePrefix = Random().nextInt(0xFFFFFF).toRadixString(36) {
    _listener = _Listener(this);
    _methodControllers = List.generate(
      _methodNames.length,
      (_) => StreamController<HubMessage>.broadcast(sync: true),
      growable: false,
    );
  }

  final SignalRConfig config;
  final HubPlatform _platform;
  final List<String> _methodNames;
  late final List<StreamController<HubMessage>> _methodControllers;
  late final _Listener _listener;
  final StreamController<HubState> _states = StreamController<HubState>.broadcast(sync: true);
  final Map<int, _ActiveStream> _streams = {};
  final String _tracePrefix;

  Future<int>? _handle;
  int? _handleValue;
  HubState _state = const HubDisconnected();
  bool _connectedOnce = false;
  bool _disposed = false;
  int _traceCounter = 0;
  int _streamCounter = 0;

  HubState get state => _state;

  /// State changes. Broadcast; read [state] for the current value.
  Stream<HubState> get states => _states.stream;

  bool get isConnected => _state is HubConnected;

  /// Messages the server sends to [method]. Broadcast and synchronous: listeners run in
  /// the same event-loop turn the batch arrives in, so keep them cheap.
  ///
  /// Throws [ArgumentError] if [method] is not declared in [SignalRConfig.serverMethods].
  Stream<HubMessage> on(String method) {
    final index = _methodNames.indexOf(method);
    if (index < 0) {
      throw ArgumentError.value(method, 'method', 'Not declared in SignalRConfig.serverMethods');
    }
    return _methodControllers[index].stream;
  }

  /// Connects. Completes when connected; throws [HubConnectException] on failure.
  /// No-op when already connected. Reconnects after that are automatic, per
  /// [SignalRConfig.reconnect].
  Future<void> start() async {
    _checkNotDisposed();
    if (_state is HubConnected) return;
    final handle = await _ensureHandle();
    try {
      await _platform.start(handle);
    } on PlatformException catch (e) {
      throw HubConnectException(e.message ?? e.code);
    }
  }

  /// Disconnects without reconnecting. Pending invokes fail.
  Future<void> stop() async {
    final handle = _handleValue;
    if (handle == null || _disposed) return;
    await _platform.stop(handle);
  }

  /// Fire-and-forget call to hub [method]. Completes once the message is handed to the
  /// native transport, not when the server processes it.
  Future<void> send(String method, [List<Object?> args = const []]) async {
    await _call(OutboundKind.send, method, args, null, (h, m, a, t) => _platform.send(h, m, a, t));
  }

  /// Calls hub [method] and waits for its result.
  ///
  /// Throws [HubNotConnectedException], [HubInvocationException] or
  /// [HubTimeoutException]. A timed-out call may still run on the server.
  Future<HubResult> invoke(String method, [List<Object?> args = const [], Duration? timeout]) {
    return _call(OutboundKind.invoke, method, args, timeout ?? config.invokeTimeout,
        (h, m, a, t) => _platform.invoke(h, m, a, t));
  }

  /// Starts server-to-client streaming hub [method] when listened to; cancelling the
  /// subscription cancels the server stream. Items have [HubMessage.target] == [method].
  Stream<HubMessage> stream(String method, [List<Object?> args = const []]) {
    late final StreamController<HubMessage> controller;
    int? streamId;
    controller = StreamController<HubMessage>(
      sync: true,
      onListen: () {
        final id = streamId = ++_streamCounter;
        _streams[id] = _ActiveStream(method, controller);
        unawaited(_call(OutboundKind.stream, method, args, null, (h, m, a, t) => _platform.streamStart(h, id, m, a, t))
            .then<void>((_) {}, onError: (Object e, StackTrace s) {
          if (_streams.remove(id) == null) return;
          controller.addError(e, s);
          unawaited(controller.close());
        }));
      },
      onCancel: () {
        final id = streamId;
        final handle = _handleValue;
        if (id != null && _streams.remove(id) != null && handle != null) {
          unawaited(_platform.streamCancel(handle, id));
        }
      },
    );
    return controller.stream;
  }

  /// Stops and releases the native connection. The hub cannot be used afterwards.
  Future<void> dispose() async {
    if (_disposed) return;
    _disposed = true;
    final handle = _handleValue;
    if (handle != null) {
      await _platform.dispose(handle);
      _platform.unbind(handle);
    }
    for (final c in _methodControllers) {
      await c.close();
    }
    for (final s in _streams.values) {
      await s.controller.close();
    }
    _streams.clear();
    await _states.close();
  }

  // ---------------------------------------------------------------------------------
  // Outbound

  Future<HubResult> _call(
    OutboundKind kind,
    String method,
    List<Object?> args,
    Duration? timeout,
    Future<NativeCallResult> Function(int handle, String method, List<Uint8List> args, String traceId) native,
  ) async {
    _checkNotDisposed();
    final traceId = '$_tracePrefix-${(++_traceCounter).toRadixString(36)}';
    final dartStart = _nowMicros();
    final encoded = [for (final a in args) config.argsEncoder.encode(a)];
    final argBytes = encoded.fold<int>(0, (n, a) => n + a.length);

    NativeCallResult? result;
    Object? error;
    try {
      final handle = _handleValue;
      if (handle == null || _state is! HubConnected) {
        throw HubNotConnectedException('Cannot ${kind.name} "$method" while ${_state.runtimeType}', traceId: traceId);
      }
      var call = native(handle, method, encoded, traceId);
      if (timeout != null) {
        call = call.timeout(timeout,
            onTimeout: () => throw HubTimeoutException('"$method" did not complete within ${timeout.inMilliseconds} ms',
                traceId: traceId));
      }
      result = await call;
      error = switch (result.errorKind) {
        null => null,
        NativeErrorKind.notConnected => HubNotConnectedException(result.errorMessage ?? method, traceId: traceId),
        NativeErrorKind.invocation => HubInvocationException(result.errorMessage ?? method, traceId: traceId),
        NativeErrorKind.native => HubProtocolException(result.errorMessage ?? method),
      };
    } on SignalRException catch (e) {
      error = e;
    } on PlatformException catch (e) {
      error = HubProtocolException('${e.code}: ${e.message}');
    }

    final timing = OutboundTiming(
      traceId: traceId,
      dartStartMicros: dartStart,
      nativeStartMicros: result?.nativeStartMicros ?? 0,
      nativeEndMicros: result?.nativeEndMicros ?? 0,
      dartEndMicros: _nowMicros(),
    );
    config.observer?.onOutbound(OutboundTrace(
      kind: kind,
      method: method,
      argBytes: argBytes,
      timing: timing,
      error: error,
    ));
    if (error != null) throw error;
    return HubResult(payload: result!.result, protocol: config.protocol, trace: timing);
  }

  // ---------------------------------------------------------------------------------
  // Inbound, via _Listener so these stay out of the public API.

  void _onBatch(ByteData data) {
    final sw = Stopwatch()..start();
    final DecodedBatch batch;
    try {
      batch = FrameCodec.decode(data);
    } on HubProtocolException catch (e) {
      _log(SignalRLogLevel.error, 'Dropped data frame: ${e.message}');
      return;
    }
    final decodeMicros = sw.elapsedMicroseconds;
    var oldest = 0;
    final protocol = config.protocol;

    for (final m in batch.messages) {
      if (oldest == 0 || m.receivedAtMicros < oldest) oldest = m.receivedAtMicros;
      switch (m.kind) {
        case FrameCodec.kindInvocation:
          if (m.id >= _methodControllers.length) {
            _log(SignalRLogLevel.error, 'Unknown method id ${m.id}');
            continue;
          }
          final controller = _methodControllers[m.id];
          if (!controller.hasListener) continue;
          controller.add(HubMessage(
            target: _methodNames[m.id],
            args: m.args,
            protocol: protocol,
            nativeReceivedAtMicros: m.receivedAtMicros,
          ));
        case FrameCodec.kindStreamItem:
          final s = _streams[m.id];
          s?.controller.add(HubMessage(
            target: s.method,
            args: m.args,
            protocol: protocol,
            nativeReceivedAtMicros: m.receivedAtMicros,
          ));
        case FrameCodec.kindStreamEnd:
          final s = _streams.remove(m.id);
          if (s == null) continue;
          if (m.error != null) s.controller.addError(HubInvocationException(m.error!));
          unawaited(s.controller.close());
      }
    }

    final observer = config.observer;
    if (observer != null && batch.messages.isNotEmpty) {
      observer.onInboundBatch(InboundBatchTrace(
        messages: batch.messages.length,
        bytes: data.lengthInBytes,
        oldestBridgeLatency: Duration(microseconds: _nowMicros() - oldest),
        decodeMicros: decodeMicros,
      ));
    }
  }

  void _onState(NativeState s) {
    final next = switch (s.kind) {
      NativeStateKind.disconnected => HubDisconnected(s.error),
      NativeStateKind.connecting => const HubConnecting(),
      NativeStateKind.connected => HubConnected(connectionId: s.connectionId, isReconnect: _connectedOnce),
      NativeStateKind.reconnecting => HubReconnecting(attempt: s.attempt, error: s.error),
    };
    if (next is HubConnected) _connectedOnce = true;
    if (next is HubDisconnected) {
      _connectedOnce = false;
      _failStreams(HubNotConnectedException('Connection closed${s.error == null ? '' : ': ${s.error}'}'));
    }
    if (next is HubReconnecting) {
      _failStreams(HubNotConnectedException('Connection lost${s.error == null ? '' : ': ${s.error}'}'));
    }
    final previous = _state;
    _state = next;
    config.observer?.onStateChanged(previous, next);
    if (!_states.isClosed) _states.add(next);
  }

  void _onLog(NativeLog log) {
    final level = SignalRLogLevel.values[log.level.clamp(0, SignalRLogLevel.values.length - 1)];
    if (level.index < config.logLevel.index) return;
    config.observer?.onLog(SignalRLogRecord(
      level: level,
      message: log.message,
      source: LogSource.native,
      timeMicros: log.timeMicros,
      traceId: log.traceId,
    ));
  }

  Future<String?> _accessToken() async => config.accessTokenProvider?.call();

  // ---------------------------------------------------------------------------------

  Future<int> _ensureHandle() => _handle ??= () async {
        final handle = await _platform.create(_platformConfig());
        _handleValue = handle;
        _platform.bind(handle, _listener);
        return handle;
      }();

  PlatformConfig _platformConfig() {
    final r = config.reconnect;
    return PlatformConfig(
      url: config.url.toString(),
      messagePack: config.protocol == HubProtocol.messagePack,
      headers: config.headers,
      skipNegotiation: config.skipNegotiation,
      hasAccessTokenProvider: config.accessTokenProvider != null,
      serverTimeoutMs: config.serverTimeout.inMilliseconds,
      keepAliveIntervalMs: config.keepAliveInterval.inMilliseconds,
      handshakeTimeoutMs: config.handshakeTimeout.inMilliseconds,
      reconnect: switch (r) {
        DelaysReconnectPolicy(:final delays) => PlatformReconnect(
            delaysMs: [for (final d in delays) d.inMilliseconds],
            exponential: false,
          ),
        ExponentialReconnectPolicy() => PlatformReconnect(
            delaysMs: const [],
            exponential: true,
            initialMs: r.initial.inMilliseconds,
            maxMs: r.max.inMilliseconds,
            multiplier: r.multiplier,
            jitter: r.jitter,
            maxAttempts: r.maxAttempts,
            maxElapsedMs: r.maxElapsed?.inMilliseconds,
          ),
      },
      envelope: config.payloadMode == PayloadMode.envelope,
      serverMethods: _methodNames,
      serverMethodArgCounts: [for (final m in _methodNames) config.argCountOf(m)],
      logLevel: config.logLevel.index,
    );
  }

  void _failStreams(Object error) {
    if (_streams.isEmpty) return;
    final active = _streams.values.toList();
    _streams.clear();
    for (final s in active) {
      s.controller.addError(error);
      unawaited(s.controller.close());
    }
  }

  void _log(SignalRLogLevel level, String message, {String? traceId}) {
    if (level.index < config.logLevel.index) return;
    config.observer?.onLog(SignalRLogRecord(
      level: level,
      message: message,
      source: LogSource.plugin,
      timeMicros: _nowMicros(),
      traceId: traceId,
    ));
  }

  void _checkNotDisposed() {
    if (_disposed) throw StateError('SignalRHub has been disposed');
  }

  static int _nowMicros() => DateTime.now().microsecondsSinceEpoch;
}

final class _ActiveStream {
  _ActiveStream(this.method, this.controller);
  final String method;
  final StreamController<HubMessage> controller;
}

final class _Listener implements PlatformListener {
  _Listener(this._hub);
  final SignalRHub _hub;

  @override
  void onBatch(ByteData data) => _hub._onBatch(data);

  @override
  void onState(NativeState state) => _hub._onState(state);

  @override
  void onLog(NativeLog log) => _hub._onLog(log);

  @override
  Future<String?> accessToken() => _hub._accessToken();
}
