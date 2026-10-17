import Foundation
import SignalRClient

/// The Swift client's send / invoke / stream take `arguments: Any...`, and Swift cannot
/// splat an array into a variadic parameter, so each arity is spelled out.
enum VariadicCalls {
    static let maxArguments = 10

    static func send(_ c: HubConnection, _ m: String, _ a: [Any]) async throws {
        switch a.count {
        case 0: try await c.send(method: m)
        case 1: try await c.send(method: m, arguments: a[0])
        case 2: try await c.send(method: m, arguments: a[0], a[1])
        case 3: try await c.send(method: m, arguments: a[0], a[1], a[2])
        case 4: try await c.send(method: m, arguments: a[0], a[1], a[2], a[3])
        case 5: try await c.send(method: m, arguments: a[0], a[1], a[2], a[3], a[4])
        case 6: try await c.send(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5])
        case 7: try await c.send(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6])
        case 8: try await c.send(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7])
        case 9: try await c.send(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8])
        case 10: try await c.send(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9])
        default: throw tooMany(a.count)
        }
    }

    static func invoke<R>(_ c: HubConnection, _ m: String, _ a: [Any]) async throws -> R {
        switch a.count {
        case 0: return try await c.invoke(method: m)
        case 1: return try await c.invoke(method: m, arguments: a[0])
        case 2: return try await c.invoke(method: m, arguments: a[0], a[1])
        case 3: return try await c.invoke(method: m, arguments: a[0], a[1], a[2])
        case 4: return try await c.invoke(method: m, arguments: a[0], a[1], a[2], a[3])
        case 5: return try await c.invoke(method: m, arguments: a[0], a[1], a[2], a[3], a[4])
        case 6: return try await c.invoke(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5])
        case 7: return try await c.invoke(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6])
        case 8: return try await c.invoke(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7])
        case 9: return try await c.invoke(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8])
        case 10: return try await c.invoke(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9])
        default: throw tooMany(a.count)
        }
    }

    static func stream<E>(_ c: HubConnection, _ m: String, _ a: [Any]) async throws -> any StreamResult<E> {
        switch a.count {
        case 0: return try await c.stream(method: m)
        case 1: return try await c.stream(method: m, arguments: a[0])
        case 2: return try await c.stream(method: m, arguments: a[0], a[1])
        case 3: return try await c.stream(method: m, arguments: a[0], a[1], a[2])
        case 4: return try await c.stream(method: m, arguments: a[0], a[1], a[2], a[3])
        case 5: return try await c.stream(method: m, arguments: a[0], a[1], a[2], a[3], a[4])
        case 6: return try await c.stream(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5])
        case 7: return try await c.stream(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6])
        case 8: return try await c.stream(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7])
        case 9: return try await c.stream(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8])
        case 10: return try await c.stream(method: m, arguments: a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9])
        default: throw tooMany(a.count)
        }
    }

    private static func tooMany(_ n: Int) -> Error {
        PayloadError.tooManyArguments(n, max: maxArguments)
    }
}
