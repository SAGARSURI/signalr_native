import Foundation
import SignalRClient

/// Captures any hub argument as a generic tree, so the adapter never maps to app models.
/// Registered as the parameter / return / stream item type in args mode (from spike (a)).
///
/// Known limits of the Swift client, both in JSON args mode only:
/// - scalar arguments (string, number, bool) bypass Decodable and arrive as Foundation
///   values, so they never reach this type; servers must send objects or arrays.
/// - numbers are parsed by JSONSerialization first, so their original text is lost
///   (`123456789.123456789123` arrives as `123456789.12345679`).
enum PassthroughValue: Codable, Equatable {
    case null
    case bool(Bool)
    case int(Int64)
    case uint(UInt64)
    case double(Double)
    case string(String)
    case binary(Data)
    case ext(Int8, Data)   // MessagePack extension, e.g. timestamp (-1) for .NET DateTime
    case array([PassthroughValue])
    case map([String: PassthroughValue])

    private struct AnyKey: CodingKey {
        var stringValue: String
        var intValue: Int?
        init?(stringValue: String) { self.stringValue = stringValue; self.intValue = nil }
        init?(intValue: Int) { self.stringValue = String(intValue); self.intValue = intValue }
    }

    init(from decoder: Decoder) throws {
        // Public extension API of SignalRClient's MessagePack decoder.
        if decoder.isMsgpackDecoder(), let type = try? decoder.getMsgpackExtType() {
            self = .ext(type, try decoder.getMsgpackExtData())
            return
        }
        if let keyed = try? decoder.container(keyedBy: AnyKey.self) {
            var m: [String: PassthroughValue] = [:]
            for k in keyed.allKeys {
                m[k.stringValue] = try keyed.decode(PassthroughValue.self, forKey: k)
            }
            self = .map(m)
            return
        }
        if var unkeyed = try? decoder.unkeyedContainer() {
            var a: [PassthroughValue] = []
            while !unkeyed.isAtEnd {
                a.append(try unkeyed.decode(PassthroughValue.self))
            }
            self = .array(a)
            return
        }
        let s = try decoder.singleValueContainer()
        if s.decodeNil() { self = .null; return }
        // Order matters: Bool before numbers, integers before Double, String before Data
        // (JSONDecoder would otherwise read a base64-looking string as Data).
        if let v = try? s.decode(Bool.self) { self = .bool(v); return }
        if let v = try? s.decode(Int64.self) { self = .int(v); return }
        if let v = try? s.decode(UInt64.self) { self = .uint(v); return }
        if let v = try? s.decode(Double.self) { self = .double(v); return }
        if let v = try? s.decode(String.self) { self = .string(v); return }
        if let v = try? s.decode(Data.self) { self = .binary(v); return }
        throw DecodingError.dataCorruptedError(in: s, debugDescription: "Unsupported hub argument value")
    }

    func encode(to encoder: Encoder) throws {
        switch self {
        case .map(let m):
            var c = encoder.container(keyedBy: AnyKey.self)
            for (k, v) in m { try c.encode(v, forKey: AnyKey(stringValue: k)!) }
        case .array(let a):
            var c = encoder.unkeyedContainer()
            for v in a { try c.encode(v) }
        case .ext(let type, let data) where encoder.isMsgpackEncoder():
            // Keeps the extension type when the client writes MessagePack (outbound args).
            try encoder.encodeMsgpackExt(extType: type, extData: data)
        default:
            var c = encoder.singleValueContainer()
            switch self {
            case .null: try c.encodeNil()
            case .bool(let v): try c.encode(v)
            case .int(let v): try c.encode(v)
            case .uint(let v): try c.encode(v)
            case .double(let v): try c.encode(v)
            case .string(let v): try c.encode(v)
            case .binary(let v): try c.encode(v)
            case .ext(_, let d): try c.encode(d)
            case .map, .array: break
            }
        }
    }
}
