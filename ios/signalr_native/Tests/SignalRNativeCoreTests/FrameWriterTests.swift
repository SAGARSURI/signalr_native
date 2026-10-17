import XCTest
@testable import SignalRNativeCore

final class FrameWriterTests: XCTestCase {
    /// Same golden bytes as FrameWriterTest.kt, hand-assembled from the layout in
    /// lib/src/frame_codec.dart, so Swift, Kotlin and Dart cannot drift apart silently.
    func testMatchesDartLayout() {
        let frame = FrameWriter.encode(handle: 7, entries: [
            .invocation(methodIndex: 3, receivedAtMicros: 0x0102030405060708, args: [Data("ab".utf8), Data()]),
            .streamEnd(streamId: 9, receivedAtMicros: 1, error: "x"),
        ])
        let expected: [UInt8] = [
            // header: 'S' 'R' v1 0 | handle=7 | count=2
            0x53, 0x52, 1, 0, 7, 0, 0, 0, 2, 0, 0, 0,
            // invocation: kind 0, argc 2, pad | id 3 | receivedAt LE
            0, 2, 0, 0, 3, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1,
            // arg "ab", arg ""
            2, 0, 0, 0, 0x61, 0x62, 0, 0, 0, 0,
            // stream end: kind 2, argc 0, pad | id 9 | receivedAt 1
            2, 0, 0, 0, 9, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            // error "x"
            1, 0, 0, 0, 0x78,
        ]
        XCTAssertEqual([UInt8](frame), expected)
    }

    func testStreamEndWithoutErrorHasZeroLength() {
        let frame = [UInt8](FrameWriter.encode(handle: 1, entries: [.streamEnd(streamId: 4, receivedAtMicros: 0, error: nil)]))
        XCTAssertEqual(frame.count, FrameWriter.headerSize + FrameWriter.messageHeaderSize + 4)
        XCTAssertEqual(Array(frame.suffix(4)), [0, 0, 0, 0])
    }

    func testEmptyBatch() {
        XCTAssertEqual([UInt8](FrameWriter.encode(handle: 2, entries: [])), [0x53, 0x52, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0])
    }

    func testStreamItemAndNegativeTimestamp() {
        let frame = [UInt8](FrameWriter.encode(handle: 1, entries: [
            .streamItem(streamId: 5, receivedAtMicros: -2, item: Data([0xff])),
        ]))
        XCTAssertEqual(Array(frame[12..<28]), [1, 1, 0, 0, 5, 0, 0, 0, 0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff])
        XCTAssertEqual(Array(frame[28...]), [1, 0, 0, 0, 0xff])
    }
}
