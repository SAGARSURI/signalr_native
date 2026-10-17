import 'dart:async';
import 'dart:typed_data';

import 'package:signalr_native/src/platform.dart';

/// Scriptable [HubPlatform] that records every call.
class FakePlatform extends HubPlatform {
  final List<String> calls = [];
  PlatformConfig? config;
  PlatformListener? listener;
  int handle = 7;

  /// Result for the next send/invoke/streamStart; defaults to success.
  NativeCallResult Function(String method, List<Uint8List> args)? onCall;
  Completer<NativeCallResult>? pendingInvoke;

  final List<List<Uint8List>> sentArgs = [];
  final List<int> cancelledStreams = [];

  void state(NativeState s) => listener!.onState(s);
  void batch(ByteData data) => listener!.onBatch(data);

  @override
  Future<int> create(PlatformConfig c) async {
    calls.add('create');
    config = c;
    return handle;
  }

  @override
  Future<void> start(int h) async {
    calls.add('start');
    state(const NativeState(NativeStateKind.connecting));
    state(const NativeState(NativeStateKind.connected, connectionId: 'c1'));
  }

  @override
  Future<void> stop(int h) async {
    calls.add('stop');
    state(const NativeState(NativeStateKind.disconnected));
  }

  NativeCallResult _respond(String method, List<Uint8List> args) {
    sentArgs.add(args);
    return onCall?.call(method, args) ?? const NativeCallResult(nativeStartMicros: 10, nativeEndMicros: 20);
  }

  @override
  Future<NativeCallResult> send(int h, String method, List<Uint8List> args, String traceId) async {
    calls.add('send:$method');
    return _respond(method, args);
  }

  @override
  Future<NativeCallResult> invoke(int h, String method, List<Uint8List> args, String traceId) {
    calls.add('invoke:$method');
    final pending = pendingInvoke;
    if (pending != null) {
      sentArgs.add(args);
      return pending.future;
    }
    return Future.value(_respond(method, args));
  }

  @override
  Future<NativeCallResult> streamStart(int h, int streamId, String method, List<Uint8List> args, String traceId) async {
    calls.add('streamStart:$streamId:$method');
    return _respond(method, args);
  }

  @override
  Future<void> streamCancel(int h, int streamId) async {
    calls.add('streamCancel:$streamId');
    cancelledStreams.add(streamId);
  }

  @override
  Future<void> dispose(int h) async => calls.add('dispose');

  @override
  void bind(int h, PlatformListener l) => listener = l;

  @override
  void unbind(int h) => listener = null;
}
