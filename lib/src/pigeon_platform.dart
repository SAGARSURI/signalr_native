import 'dart:typed_data';

import 'package:flutter/services.dart';

import 'exceptions.dart';
import 'frame_codec.dart';
import 'generated/messages.g.dart';
import 'platform.dart';

/// Real [HubPlatform]: Pigeon for control calls, one binary channel for data.
final class PigeonHubPlatform extends HubPlatform implements SignalRFlutterApi {
  PigeonHubPlatform({BinaryMessenger? messenger})
      : _host = SignalRHostApi(binaryMessenger: messenger) {
    SignalRFlutterApi.setUp(this, binaryMessenger: messenger);
    // Raw messenger handler: frames are already binary, so no codec and no reply.
    (messenger ?? ServicesBinding.instance.defaultBinaryMessenger)
        .setMessageHandler(dataChannelName, _onData);
  }

  /// Must match the native adapters.
  static const String dataChannelName = 'signalr_native/data';

  static final PigeonHubPlatform instance = PigeonHubPlatform();

  final SignalRHostApi _host;
  final Map<int, PlatformListener> _listeners = {};

  Future<ByteData?> _onData(ByteData? message) async {
    if (message == null || message.lengthInBytes < FrameCodec.headerSize) return null;
    final handle = message.getUint32(4, Endian.little);
    _listeners[handle]?.onBatch(message);
    return null;
  }

  @override
  void bind(int handle, PlatformListener listener) => _listeners[handle] = listener;

  @override
  void unbind(int handle) => _listeners.remove(handle);

  @override
  Future<int> create(PlatformConfig c) => _host.create(ConnectConfigMsg(
        url: c.url,
        hubProtocol: c.messagePack ? ProtocolMsg.messagePack : ProtocolMsg.json,
        headers: c.headers,
        skipNegotiation: c.skipNegotiation,
        hasAccessTokenProvider: c.hasAccessTokenProvider,
        serverTimeoutMs: c.serverTimeoutMs,
        keepAliveIntervalMs: c.keepAliveIntervalMs,
        handshakeTimeoutMs: c.handshakeTimeoutMs,
        reconnect: ReconnectMsg(
          delaysMs: c.reconnect.delaysMs,
          exponential: c.reconnect.exponential,
          initialMs: c.reconnect.initialMs,
          maxMs: c.reconnect.maxMs,
          multiplier: c.reconnect.multiplier,
          jitter: c.reconnect.jitter,
          maxAttempts: c.reconnect.maxAttempts,
          maxElapsedMs: c.reconnect.maxElapsedMs,
        ),
        payloadMode: c.envelope ? PayloadModeMsg.envelope : PayloadModeMsg.args,
        serverMethods: c.serverMethods,
        serverMethodArgCounts: c.serverMethodArgCounts,
        logLevel: c.logLevel,
      ));

  @override
  Future<void> start(int handle) => _host.start(handle);

  @override
  Future<void> stop(int handle) => _host.stop(handle);

  @override
  Future<NativeCallResult> send(int handle, String method, List<Uint8List> args, String traceId) async =>
      _result(await _host.send(handle, method, args, traceId));

  @override
  Future<NativeCallResult> invoke(int handle, String method, List<Uint8List> args, String traceId) async =>
      _result(await _host.invoke(handle, method, args, traceId));

  @override
  Future<NativeCallResult> streamStart(
          int handle, int streamId, String method, List<Uint8List> args, String traceId) async =>
      _result(await _host.streamStart(handle, streamId, method, args, traceId));

  @override
  Future<void> streamCancel(int handle, int streamId) => _host.streamCancel(handle, streamId);

  @override
  Future<void> dispose(int handle) => _host.dispose(handle);

  static NativeCallResult _result(CallResultMsg r) => NativeCallResult(
        result: r.result,
        nativeStartMicros: r.nativeStartMicros,
        nativeEndMicros: r.nativeEndMicros,
        errorKind: switch (r.errorKind) {
          null => null,
          'notConnected' => NativeErrorKind.notConnected,
          'invocation' => NativeErrorKind.invocation,
          _ => NativeErrorKind.native,
        },
        errorMessage: r.errorMessage,
      );

  // SignalRFlutterApi — called by native, routed by handle.

  @override
  Future<String?> accessToken(int handle) {
    final listener = _listeners[handle];
    if (listener == null) {
      throw HubProtocolException('Token requested for unknown handle $handle');
    }
    return listener.accessToken();
  }

  @override
  void onState(int handle, StateEventMsg event) => _listeners[handle]?.onState(NativeState(
        switch (event.kind) {
          StateKindMsg.disconnected => NativeStateKind.disconnected,
          StateKindMsg.connecting => NativeStateKind.connecting,
          StateKindMsg.connected => NativeStateKind.connected,
          StateKindMsg.reconnecting => NativeStateKind.reconnecting,
        },
        connectionId: event.connectionId,
        attempt: event.attempt,
        error: event.error,
      ));

  @override
  void onLog(int handle, LogEventMsg event) => _listeners[handle]
      ?.onLog(NativeLog(event.level, event.message, event.timeMicros, traceId: event.traceId));
}
