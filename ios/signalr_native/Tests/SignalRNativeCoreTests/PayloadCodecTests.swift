import XCTest
@testable import SignalRNativeCore

final class PayloadCodecTests: XCTestCase {
    private let quote: PassthroughValue = .map([
        "s": .string("EURUSD"), "b": .double(1.08451), "a": .double(1.08453), "t": .int(1729050000123),
    ])

    // MessagePack writer / reader ----------------------------------------------------------

    func testMsgpackRoundTripsEveryShape() throws {
        let values: [PassthroughValue] = [
            .null, .bool(true), .bool(false),
            .int(0), .int(127), .int(128), .int(255), .int(256), .int(65535), .int(65536), .int(Int64(Int32.max) + 1),
            .int(-1), .int(-32), .int(-33), .int(-128), .int(-129), .int(-32768), .int(-32769), .int(Int64.min),
            .uint(UInt64.max), .double(1.5), .double(-0.1),
            .string(""), .string("€uro ✓"), .string(String(repeating: "x", count: 31)),
            .string(String(repeating: "x", count: 255)), .string(String(repeating: "y", count: 70_000)),
            .binary(Data()), .binary(Data(repeating: 7, count: 300)),
            .ext(-1, Data([0x66, 0x0F, 0x5A, 0x00])), .ext(5, Data([1, 2, 3])),
            .array([]), .array((0..<20).map { .int($0) }),
            .map([:]), .map(Dictionary(uniqueKeysWithValues: (0..<20).map { ("k\($0)", PassthroughValue.int($0)) })),
            quote, .array([quote, .null, .map(["nested": .array([.string("deep")])])]),
        ]
        for v in values {
            var w = MsgpackWriter()
            w.write(v)
            XCTAssertEqual(try MsgpackReader.read(w.data), v, "\(v)")
        }
    }

    func testMsgpackUsesCompactEncodings() {
        func bytes(_ v: PassthroughValue) -> [UInt8] { var w = MsgpackWriter(); w.write(v); return [UInt8](w.data) }
        XCTAssertEqual(bytes(.int(5)), [0x05])
        XCTAssertEqual(bytes(.int(-5)), [0xfb])
        XCTAssertEqual(bytes(.int(200)), [0xcc, 200])
        XCTAssertEqual(bytes(.int(-100)), [0xd0, 0x9c])
        XCTAssertEqual(bytes(.string("é")), [0xa2, 0xc3, 0xa9], "length is the UTF-8 byte count")
        XCTAssertEqual(bytes(.binary(Data([1]))), [0xc4, 1, 1])
        XCTAssertEqual(bytes(.ext(-1, Data([0, 0, 0, 1]))), [0xd6, 0xff, 0, 0, 0, 1])
    }

    func testMsgpackReaderReadsFloat32AndRejectsBadInput() throws {
        XCTAssertEqual(try MsgpackReader.read(Data([0xca, 0x3f, 0xc0, 0x00, 0x00])), .double(1.5))
        XCTAssertThrowsError(try MsgpackReader.read(Data([0xa3, 0x61])), "truncated string")
        XCTAssertThrowsError(try MsgpackReader.read(Data([0xc1])), "never-used byte")
        XCTAssertThrowsError(try MsgpackReader.read(Data([0x01, 0x02])), "trailing bytes")
        XCTAssertThrowsError(try MsgpackReader.read(Data()), "empty")
    }

    // Codecs ---------------------------------------------------------------------------------

    func testJsonArgsRoundTrip() throws {
        let codec = PayloadCodec.jsonArgs
        let fromDart = Data(#"{"s":"EURUSD","b":1.08451,"a":1.08453,"t":1729050000123}"#.utf8)
        let arg = try codec.fromBytes(fromDart)
        XCTAssertEqual(arg as? PassthroughValue, quote)
        let back = try codec.toBytes(arg)
        XCTAssertEqual(try JSONDecoder().decode(PassthroughValue.self, from: back), quote)
        XCTAssertEqual(try codec.fromBytes(Data("42".utf8)) as? PassthroughValue, .int(42), "top-level scalars from Dart")
    }

    func testJsonArgsNullAndFoundationScalars() throws {
        let codec = PayloadCodec.jsonArgs
        XCTAssertEqual(String(decoding: try codec.toBytes(nil), as: UTF8.self), "null")
        XCTAssertEqual(String(decoding: try codec.toBytes(NSNull()), as: UTF8.self), "null")
        XCTAssertEqual(String(decoding: try codec.toBytes("x" as NSString), as: UTF8.self), #""x""#)
        XCTAssertEqual(String(decoding: try codec.toBytes(NSNumber(value: 3)), as: UTF8.self), "3")
    }

    func testJsonEnvelopeIsAStraightCopy() throws {
        let codec = PayloadCodec.jsonEnvelope
        let payload = #"{"s":"EURUSD","q":123456789.123456789123}"#
        XCTAssertEqual(try codec.fromBytes(Data(payload.utf8)) as? String, payload)
        XCTAssertEqual(try codec.toBytes(payload), Data(payload.utf8), "number text is preserved")
        XCTAssertEqual(try codec.toBytes(payload as NSString), Data(payload.utf8), "bridged NSString")
        XCTAssertThrowsError(try codec.toBytes(PassthroughValue.int(1)))
    }

    func testMsgpackArgsRoundTrip() throws {
        let codec = PayloadCodec.msgpackArgs
        var w = MsgpackWriter()
        w.write(quote)
        let arg = try codec.fromBytes(w.data)
        XCTAssertEqual(arg as? PassthroughValue, quote)
        XCTAssertEqual(try MsgpackReader.read(try codec.toBytes(arg)), quote)
        XCTAssertEqual(try codec.toBytes(nil), Data([0xc0]))
    }

    func testMsgpackEnvelopeIsAStraightCopy() throws {
        let codec = PayloadCodec.msgpackEnvelope
        let bytes = Data([0x81, 0xa1, 0x73, 0xa6, 0x45, 0x55, 0x52, 0x55, 0x53, 0x44])
        XCTAssertEqual(try codec.fromBytes(bytes) as? Data, bytes)
        XCTAssertEqual(try codec.toBytes(bytes), bytes)
        XCTAssertEqual(try codec.toBytes(nil), Data())
    }

    // Client behaviour the session relies on ------------------------------------------------

    /// HubConnection.invoke<TReturn> ends with `(try await tcs.task()) as? TReturn` on an `Any?`.
    /// The session asks for Optional result types so a nil (void / null) result casts instead of
    /// throwing. This reproduces that cast in a generic context, as the client does it.
    func testOptionalReturnTypeAcceptsNilResults() {
        func clientCast<TReturn>(_ value: Any?, as _: TReturn.Type) -> TReturn? { value as? TReturn }

        let none: Any? = nil
        XCTAssertNil(clientCast(none, as: PassthroughValue.self), "non-optional type: nil fails, the client throws")
        XCTAssertNotNil(clientCast(none, as: PassthroughValue?.self), "optional type: nil casts")
        XCTAssertNotNil(clientCast(none, as: String?.self))
        XCTAssertNotNil(clientCast(none, as: Data?.self))

        // JSON protocol: the client decodes objects with the registered (Optional) type.
        let decoded: Any = try! JSONDecoder().decode(PassthroughValue?.self, from: Data(#"{"a":1}"#.utf8)) as Any
        XCTAssertEqual(clientCast(decoded, as: PassthroughValue?.self), .some(.some(.map(["a": .int(1)]))))
        // JSON protocol: string scalars come back as Foundation strings.
        XCTAssertEqual(clientCast("r" as NSString, as: String?.self), .some(.some("r")))
        // JSON args mode limitation: number scalars cannot become PassthroughValue.
        XCTAssertNil(clientCast(NSNumber(value: 1), as: PassthroughValue?.self))
    }
}
