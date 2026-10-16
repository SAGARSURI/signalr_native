import Foundation

/// MessagePack writer for the bridge (args mode with the MessagePack protocol).
/// SignalRClient's own encoder is internal, so the plugin needs its own.
struct MsgpackWriter {
    private(set) var data = Data()

    mutating func write(_ v: PassthroughValue) {
        switch v {
        case .null: byte(0xc0)
        case .bool(let b): byte(b ? 0xc3 : 0xc2)
        case .int(let i):
            if i >= 0 { uint(UInt64(i)) } else { int(i) }
        case .uint(let u): uint(u)
        case .double(let d): byte(0xcb); be64(d.bitPattern)
        case .string(let s):
            let b = Data(s.utf8)
            switch b.count {
            case ..<32: byte(0xa0 | UInt8(b.count))
            case ..<0x100: byte(0xd9); byte(UInt8(b.count))
            case ..<0x10000: byte(0xda); be16(UInt16(b.count))
            default: byte(0xdb); be32(UInt32(b.count))
            }
            data.append(b)
        case .binary(let b):
            switch b.count {
            case ..<0x100: byte(0xc4); byte(UInt8(b.count))
            case ..<0x10000: byte(0xc5); be16(UInt16(b.count))
            default: byte(0xc6); be32(UInt32(b.count))
            }
            data.append(b)
        case .ext(let t, let b):
            switch b.count {
            case 1: byte(0xd4)
            case 2: byte(0xd5)
            case 4: byte(0xd6)
            case 8: byte(0xd7)
            case 16: byte(0xd8)
            case ..<0x100: byte(0xc7); byte(UInt8(b.count))
            case ..<0x10000: byte(0xc8); be16(UInt16(b.count))
            default: byte(0xc9); be32(UInt32(b.count))
            }
            byte(UInt8(bitPattern: t))
            data.append(b)
        case .array(let a):
            arrayHeader(a.count)
            for x in a { write(x) }
        case .map(let m):
            switch m.count {
            case ..<16: byte(0x80 | UInt8(m.count))
            case ..<0x10000: byte(0xde); be16(UInt16(m.count))
            default: byte(0xdf); be32(UInt32(m.count))
            }
            for (k, x) in m { write(.string(k)); write(x) }
        }
    }

    mutating func arrayHeader(_ n: Int) {
        switch n {
        case ..<16: byte(0x90 | UInt8(n))
        case ..<0x10000: byte(0xdc); be16(UInt16(n))
        default: byte(0xdd); be32(UInt32(n))
        }
    }

    private mutating func uint(_ u: UInt64) {
        switch u {
        case ..<0x80: byte(UInt8(u))
        case ..<0x100: byte(0xcc); byte(UInt8(u))
        case ..<0x10000: byte(0xcd); be16(UInt16(u))
        case ..<0x1_0000_0000: byte(0xce); be32(UInt32(u))
        default: byte(0xcf); be64(u)
        }
    }

    private mutating func int(_ i: Int64) {
        switch i {
        case -32 ..< 0: byte(UInt8(bitPattern: Int8(i)))
        case Int64(Int8.min) ..< 0: byte(0xd0); byte(UInt8(bitPattern: Int8(i)))
        case Int64(Int16.min) ..< 0: byte(0xd1); be16(UInt16(bitPattern: Int16(i)))
        case Int64(Int32.min) ..< 0: byte(0xd2); be32(UInt32(bitPattern: Int32(i)))
        default: byte(0xd3); be64(UInt64(bitPattern: i))
        }
    }

    private mutating func byte(_ b: UInt8) { data.append(b) }
    private mutating func be16(_ v: UInt16) { withUnsafeBytes(of: v.bigEndian) { data.append(contentsOf: $0) } }
    private mutating func be32(_ v: UInt32) { withUnsafeBytes(of: v.bigEndian) { data.append(contentsOf: $0) } }
    private mutating func be64(_ v: UInt64) { withUnsafeBytes(of: v.bigEndian) { data.append(contentsOf: $0) } }
}

enum MsgpackReadError: LocalizedError, CustomStringConvertible {
    case truncated(at: Int)
    case invalidByte(UInt8, at: Int)
    case trailingBytes(Int)
    case invalidUtf8(at: Int)

    var description: String {
        switch self {
        case .truncated(let o): return "MessagePack value truncated at byte \(o)"
        case .invalidByte(let b, let o): return "Invalid MessagePack byte 0x\(String(b, radix: 16)) at \(o)"
        case .trailingBytes(let n): return "\(n) unexpected bytes after the MessagePack value"
        case .invalidUtf8(let o): return "Invalid UTF-8 string at byte \(o)"
        }
    }

    var errorDescription: String? { description }
}

/// MessagePack reader for outbound arguments: Dart sends one MessagePack value per argument,
/// which the client must re-encode inside the hub message, so it needs a generic value.
struct MsgpackReader {
    private let bytes: [UInt8]
    private var o = 0

    /// Reads exactly one value; trailing bytes are an error.
    static func read(_ data: Data) throws -> PassthroughValue {
        var r = MsgpackReader(bytes: [UInt8](data))
        let v = try r.value()
        guard r.o == r.bytes.count else { throw MsgpackReadError.trailingBytes(r.bytes.count - r.o) }
        return v
    }

    private init(bytes: [UInt8]) { self.bytes = bytes }

    private mutating func value() throws -> PassthroughValue {
        let start = o
        let b = try u8()
        switch b {
        case 0x00...0x7f: return .int(Int64(b))
        case 0x80...0x8f: return try map(Int(b & 0x0f))
        case 0x90...0x9f: return try array(Int(b & 0x0f))
        case 0xa0...0xbf: return try string(Int(b & 0x1f))
        case 0xc0: return .null
        case 0xc2: return .bool(false)
        case 0xc3: return .bool(true)
        case 0xc4: return .binary(try take(Int(try u8())))
        case 0xc5: return .binary(try take(Int(try be16())))
        case 0xc6: return .binary(try take(Int(try be32())))
        case 0xc7: return try ext(Int(try u8()))
        case 0xc8: return try ext(Int(try be16()))
        case 0xc9: return try ext(Int(try be32()))
        case 0xca: return .double(Double(Float(bitPattern: try be32())))
        case 0xcb: return .double(Double(bitPattern: try be64()))
        case 0xcc: return .int(Int64(try u8()))
        case 0xcd: return .int(Int64(try be16()))
        case 0xce: return .int(Int64(try be32()))
        case 0xcf:
            let u = try be64()
            return u <= UInt64(Int64.max) ? .int(Int64(u)) : .uint(u)
        case 0xd0: return .int(Int64(Int8(bitPattern: try u8())))
        case 0xd1: return .int(Int64(Int16(bitPattern: try be16())))
        case 0xd2: return .int(Int64(Int32(bitPattern: try be32())))
        case 0xd3: return .int(Int64(bitPattern: try be64()))
        case 0xd4: return try ext(1)
        case 0xd5: return try ext(2)
        case 0xd6: return try ext(4)
        case 0xd7: return try ext(8)
        case 0xd8: return try ext(16)
        case 0xd9: return try string(Int(try u8()))
        case 0xda: return try string(Int(try be16()))
        case 0xdb: return try string(Int(try be32()))
        case 0xdc: return try array(Int(try be16()))
        case 0xdd: return try array(Int(try be32()))
        case 0xde: return try map(Int(try be16()))
        case 0xdf: return try map(Int(try be32()))
        case 0xe0...0xff: return .int(Int64(Int8(bitPattern: b)))
        default: throw MsgpackReadError.invalidByte(b, at: start)
        }
    }

    private mutating func map(_ n: Int) throws -> PassthroughValue {
        var m: [String: PassthroughValue] = [:]
        m.reserveCapacity(n)
        for _ in 0..<n {
            let key: String
            switch try value() {
            case .string(let s): key = s
            case .int(let i): key = String(i)
            case .uint(let u): key = String(u)
            case let other: key = String(describing: other)
            }
            m[key] = try value()
        }
        return .map(m)
    }

    private mutating func array(_ n: Int) throws -> PassthroughValue {
        var a: [PassthroughValue] = []
        a.reserveCapacity(n)
        for _ in 0..<n { a.append(try value()) }
        return .array(a)
    }

    private mutating func string(_ n: Int) throws -> PassthroughValue {
        let at = o
        let d = try take(n)
        guard let s = String(data: d, encoding: .utf8) else { throw MsgpackReadError.invalidUtf8(at: at) }
        return .string(s)
    }

    private mutating func ext(_ n: Int) throws -> PassthroughValue {
        let type = Int8(bitPattern: try u8())
        return .ext(type, try take(n))
    }

    private mutating func take(_ n: Int) throws -> Data {
        guard n >= 0, o + n <= bytes.count else { throw MsgpackReadError.truncated(at: o) }
        defer { o += n }
        return Data(bytes[o..<(o + n)])
    }

    private mutating func u8() throws -> UInt8 {
        guard o < bytes.count else { throw MsgpackReadError.truncated(at: o) }
        defer { o += 1 }
        return bytes[o]
    }

    private mutating func be16() throws -> UInt16 {
        var v: UInt16 = 0
        for _ in 0..<2 { v = v << 8 | UInt16(try u8()) }
        return v
    }

    private mutating func be32() throws -> UInt32 {
        var v: UInt32 = 0
        for _ in 0..<4 { v = v << 8 | UInt32(try u8()) }
        return v
    }

    private mutating func be64() throws -> UInt64 {
        var v: UInt64 = 0
        for _ in 0..<8 { v = v << 8 | UInt64(try u8()) }
        return v
    }
}
