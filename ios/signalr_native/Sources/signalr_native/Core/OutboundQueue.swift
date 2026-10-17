import Foundation

/// Starts outbound calls in the order Dart made them. Each step begins after the previous
/// one returned, so a step that awaits its write (send, stream start) keeps the wire order;
/// separate Tasks per call could reach the client's actor in any order.
/// `enqueue` is main-thread only.
final class OutboundQueue {
    private var tail: Task<Void, Never>?

    func enqueue(_ step: @escaping () async -> Void) {
        let previous = tail
        tail = Task {
            await previous?.value
            await step()
        }
    }
}
