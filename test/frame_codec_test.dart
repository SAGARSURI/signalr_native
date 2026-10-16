import 'dart:convert';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:signalr_native/signalr_native.dart';
import 'package:signalr_native/src/frame_codec.dart';

Uint8List b(String s) => utf8.encode(s);

void main() {
  group('FrameCodec', () {
    test('round-trips invocations, stream items and stream ends', () {
      final w = FrameWriter(42)
        ..invocation(3, 1000, [b('{"s":"EURUSD"}'), b('7')])
        ..invocation(0, 1001, [])
        ..streamItem(9, 1002, b('[1,2]'))
        ..streamEnd(9, 1003)
        ..streamEnd(10, 1004, error: 'boom ✗');
      final batch = FrameCodec.decode(w.build());

      expect(batch.handle, 42);
      expect(batch.messages, hasLength(5));

      final inv = batch.messages[0];
      expect(inv.kind, FrameCodec.kindInvocation);
      expect(inv.id, 3);
      expect(inv.receivedAtMicros, 1000);
      expect(inv.args.map(utf8.decode), ['{"s":"EURUSD"}', '7']);

      expect(batch.messages[1].args, isEmpty);

      final item = batch.messages[2];
      expect(item.kind, FrameCodec.kindStreamItem);
      expect(item.id, 9);
      expect(utf8.decode(item.args.single), '[1,2]');

      expect(batch.messages[3].kind, FrameCodec.kindStreamEnd);
      expect(batch.messages[3].error, isNull);
      expect(batch.messages[4].error, 'boom ✗');
    });

    test('argument bytes are views into the frame buffer, not copies', () {
      final data = (FrameWriter(1)..invocation(0, 1, [b('abc')])).build();
      final arg = FrameCodec.decode(data).messages.single.args.single;
      // Header (12) + message header (16) + length prefix (4) = first argument byte.
      const firstArgByte = FrameCodec.headerSize + FrameCodec.messageHeaderSize + 4;
      data.setUint8(firstArgByte, 0x7A); // 'z'
      expect(utf8.decode(arg), 'zbc', reason: 'a write to the frame must be visible through the payload');
    });

    test('decodes a frame that sits at an offset inside a larger buffer', () {
      final frame = (FrameWriter(5)..invocation(1, 2, [b('xyz')])).build();
      final padded = Uint8List(frame.lengthInBytes + 9)
        ..setRange(9, 9 + frame.lengthInBytes, frame.buffer.asUint8List());
      final view = ByteData.sublistView(padded, 9);
      final m = FrameCodec.decode(view).messages.single;
      expect(utf8.decode(m.args.single), 'xyz');
    });

    test('rejects bad magic, unknown version, truncation and trailing bytes', () {
      final good = (FrameWriter(1)..invocation(0, 1, [b('abc')])).build().buffer.asUint8List();

      final badMagic = Uint8List.fromList(good)..[0] = 0;
      expect(() => FrameCodec.decode(ByteData.sublistView(badMagic)), throwsA(isA<HubProtocolException>()));

      final badVersion = Uint8List.fromList(good)..[2] = 99;
      expect(() => FrameCodec.decode(ByteData.sublistView(badVersion)), throwsA(isA<HubProtocolException>()));

      final truncated = Uint8List.sublistView(good, 0, good.length - 1);
      expect(() => FrameCodec.decode(ByteData.sublistView(truncated)), throwsA(isA<HubProtocolException>()));

      final trailing = Uint8List(good.length + 1)..setRange(0, good.length, good);
      expect(() => FrameCodec.decode(ByteData.sublistView(trailing)), throwsA(isA<HubProtocolException>()));
    });

    test('empty batch', () {
      expect(FrameCodec.decode(FrameWriter(3).build()).messages, isEmpty);
    });
  });
}
