import 'dart:convert';
import 'dart:typed_data';

import 'exceptions.dart';

/// Data plane: native → Dart batches of server messages over one BasicMessageChannel
/// with [BinaryCodec]. The native side appends messages to a pending batch and posts it
/// to the platform thread; whatever arrives while a post is in flight joins the next
/// batch, so batches grow under load and add no latency when idle.
///
/// Layout (little-endian):
/// ```
/// header   u8 'S' | u8 'R' | u8 version (1) | u8 0
///          u32 handle
///          u32 messageCount
/// message  u8 kind          0 = invocation, 1 = stream item, 2 = stream end
///          u8 argCount      0 for kind 2
///          u16 0
///          u32 id           kind 0: index into SignalRConfig.serverMethods
///                           kind 1, 2: stream id
///          i64 receivedAtMicros (wall clock, µs since epoch)
///          kind 0, 1: argCount × (u32 length, bytes)
///          kind 2: u32 errorLength (0 = completed normally), UTF-8 error
/// ```
/// The same layout is produced by `FrameWriter.kt` and `FrameWriter.swift`.
abstract final class FrameCodec {
  static const int version = 1;
  static const int headerSize = 12;
  static const int messageHeaderSize = 16;

  static const int kindInvocation = 0;
  static const int kindStreamItem = 1;
  static const int kindStreamEnd = 2;

  /// Parses a batch. Argument bytes are views into [data]'s buffer (no copies).
  static DecodedBatch decode(ByteData data) {
    if (data.lengthInBytes < headerSize ||
        data.getUint8(0) != 0x53 ||
        data.getUint8(1) != 0x52) {
      throw const HubProtocolException('Not a signalr_native data frame');
    }
    final frameVersion = data.getUint8(2);
    if (frameVersion != version) {
      throw HubProtocolException('Unsupported data frame version $frameVersion');
    }
    final handle = data.getUint32(4, Endian.little);
    final count = data.getUint32(8, Endian.little);
    final buffer = data.buffer;
    final base = data.offsetInBytes;
    final messages = List<DecodedMessage>.filled(count, DecodedMessage._empty, growable: false);

    var o = headerSize;
    for (var i = 0; i < count; i++) {
      _need(data, o, messageHeaderSize);
      final kind = data.getUint8(o);
      final argCount = data.getUint8(o + 1);
      final id = data.getUint32(o + 4, Endian.little);
      final receivedAt = data.getInt64(o + 8, Endian.little);
      o += messageHeaderSize;

      switch (kind) {
        case kindInvocation || kindStreamItem:
          final args = List<Uint8List>.filled(argCount, _emptyBytes, growable: false);
          for (var a = 0; a < argCount; a++) {
            _need(data, o, 4);
            final len = data.getUint32(o, Endian.little);
            o += 4;
            _need(data, o, len);
            args[a] = buffer.asUint8List(base + o, len);
            o += len;
          }
          messages[i] = DecodedMessage(kind, id, receivedAt, args, null);
        case kindStreamEnd:
          _need(data, o, 4);
          final len = data.getUint32(o, Endian.little);
          o += 4;
          _need(data, o, len);
          final error = len == 0 ? null : utf8.decode(buffer.asUint8List(base + o, len));
          o += len;
          messages[i] = DecodedMessage(kind, id, receivedAt, const [], error);
        default:
          throw HubProtocolException('Unknown message kind $kind');
      }
    }
    if (o != data.lengthInBytes) {
      throw HubProtocolException('Trailing ${data.lengthInBytes - o} bytes in data frame');
    }
    return DecodedBatch(handle, messages);
  }

  static void _need(ByteData data, int offset, int length) {
    if (offset + length > data.lengthInBytes) {
      throw const HubProtocolException('Truncated data frame');
    }
  }

  static final Uint8List _emptyBytes = Uint8List(0);
}

final class DecodedBatch {
  const DecodedBatch(this.handle, this.messages);
  final int handle;
  final List<DecodedMessage> messages;
}

final class DecodedMessage {
  const DecodedMessage(this.kind, this.id, this.receivedAtMicros, this.args, this.error);
  final int kind;
  final int id;
  final int receivedAtMicros;
  final List<Uint8List> args;
  final String? error;

  static const DecodedMessage _empty = DecodedMessage(-1, 0, 0, [], null);
}

/// Reference encoder, mirroring the native writers. Used by tests and the fake platform.
final class FrameWriter {
  FrameWriter(this.handle);

  final int handle;
  final BytesBuilder _body = BytesBuilder(copy: false);
  int _count = 0;

  int get messageCount => _count;

  void invocation(int methodIndex, int receivedAtMicros, List<Uint8List> args) =>
      _item(FrameCodec.kindInvocation, methodIndex, receivedAtMicros, args);

  void streamItem(int streamId, int receivedAtMicros, Uint8List item) =>
      _item(FrameCodec.kindStreamItem, streamId, receivedAtMicros, [item]);

  void streamEnd(int streamId, int receivedAtMicros, {String? error}) {
    final err = error == null ? Uint8List(0) : utf8.encode(error);
    _body
      ..add(_messageHeader(FrameCodec.kindStreamEnd, 0, streamId, receivedAtMicros))
      ..add(_u32(err.length))
      ..add(err);
    _count++;
  }

  void _item(int kind, int id, int receivedAtMicros, List<Uint8List> args) {
    if (args.length > 255) throw ArgumentError('Too many arguments');
    _body.add(_messageHeader(kind, args.length, id, receivedAtMicros));
    for (final a in args) {
      _body
        ..add(_u32(a.length))
        ..add(a);
    }
    _count++;
  }

  ByteData build() {
    final body = _body.takeBytes();
    final out = Uint8List(FrameCodec.headerSize + body.length);
    final bd = ByteData.sublistView(out)
      ..setUint8(0, 0x53)
      ..setUint8(1, 0x52)
      ..setUint8(2, FrameCodec.version)
      ..setUint32(4, handle, Endian.little)
      ..setUint32(8, _count, Endian.little);
    out.setRange(FrameCodec.headerSize, out.length, body);
    _count = 0;
    return bd;
  }

  static Uint8List _messageHeader(int kind, int argCount, int id, int receivedAtMicros) {
    final b = ByteData(FrameCodec.messageHeaderSize)
      ..setUint8(0, kind)
      ..setUint8(1, argCount)
      ..setUint32(4, id, Endian.little)
      ..setInt64(8, receivedAtMicros, Endian.little);
    return b.buffer.asUint8List();
  }

  static Uint8List _u32(int v) => (ByteData(4)..setUint32(0, v, Endian.little)).buffer.asUint8List();
}
