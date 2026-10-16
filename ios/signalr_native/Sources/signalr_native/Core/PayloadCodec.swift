import Foundation

/// How one argument / return value is bound by the Swift client and turned into the bytes
/// Dart receives (and back for outbound calls). Chosen once per connection from
/// (protocol, payloadMode); mirrors PayloadCodec.kt. Envelope modes are straight copies;
/// args modes go through PassthroughValue.
enum PayloadCodec {
    /// JSON hub protocol, args mode: PassthroughValue, re-encoded as JSON text.
    case jsonArgs
    /// JSON hub protocol, envelope mode: one JSON *string* argument carrying the payload.
    case jsonEnvelope
    /// MessagePack hub protocol, args mode: PassthroughValue, re-encoded as MessagePack.
    case msgpackArgs
    /// MessagePack hub protocol, envelope mode: one `bin` argument (Data).
    case msgpackEnvelope

    static func of(messagePack: Bool, envelope: Bool) -> PayloadCodec {
        switch (messagePack, envelope) {
        case (true, true): return .msgpackEnvelope
        case (true, false): return .msgpackArgs
        case (false, true): return .jsonEnvelope
        case (false, false): return .jsonArgs
        }
    }

    /// Bytes for Dart from a value the client produced (an argument, stream item or result).
    func toBytes(_ value: Any?) throws -> Data {
        switch self {
        case .jsonArgs:
            guard let value, !(value is NSNull) else { return Data("null".utf8) }
            if let v = value as? PassthroughValue { return try JSONEncoder().encode(v) }
            // Scalars the client did not run through Decodable.
            return try JSONSerialization.data(withJSONObject: value, options: [.fragmentsAllowed])
        case .jsonEnvelope:
            guard let value else { return Data() }
            if let s = value as? String { return Data(s.utf8) }
            throw PayloadError.unexpectedType(expected: "String", got: value)
        case .msgpackArgs:
            var w = MsgpackWriter()
            if let value {
                guard let v = value as? PassthroughValue else {
                    throw PayloadError.unexpectedType(expected: "PassthroughValue", got: value)
                }
                w.write(v)
            } else {
                w.write(.null)
            }
            return w.data
        case .msgpackEnvelope:
            guard let value else { return Data() }
            if let d = value as? Data { return d }
            throw PayloadError.unexpectedType(expected: "Data", got: value)
        }
    }

    /// Client argument from the bytes Dart sent for one outbound argument.
    func fromBytes(_ data: Data) throws -> Any {
        switch self {
        case .jsonArgs: return try JSONDecoder().decode(PassthroughValue.self, from: data)
        case .jsonEnvelope: return String(decoding: data, as: UTF8.self)
        case .msgpackArgs: return try MsgpackReader.read(data)
        case .msgpackEnvelope: return data
        }
    }
}

enum PayloadError: LocalizedError, CustomStringConvertible {
    case unexpectedType(expected: String, got: Any)
    case tooManyArguments(Int, max: Int)

    var description: String {
        switch self {
        case .unexpectedType(let expected, let got): return "Expected \(expected), got \(type(of: got))"
        case .tooManyArguments(let n, let max): return "\(n) arguments; the Swift client adapter passes at most \(max)"
        }
    }

    var errorDescription: String? { description }
}
