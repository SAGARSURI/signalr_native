import Foundation

/// Coalesces server messages into data-plane batches. Mirrors DataPlane.kt.
///
/// `add` is called from the client's actor (any thread). The first message after a flush
/// posts one flush to the main thread; everything that arrives before that flush runs joins
/// the same batch. Idle: one message per batch, no added delay. Under load: batches grow to
/// whatever accumulated during one main-thread hop.
final class DataPlane: @unchecked Sendable {
    private let handle: Int64
    private let postToMain: (@escaping () -> Void) -> Void
    private let deliver: (Data) -> Void
    private let lock = NSLock()
    private var pending: [DataEntry] = []
    private var flushScheduled = false

    init(handle: Int64, postToMain: @escaping (@escaping () -> Void) -> Void, deliver: @escaping (Data) -> Void) {
        self.handle = handle
        self.postToMain = postToMain
        self.deliver = deliver
        pending.reserveCapacity(64)
    }

    func add(_ entry: DataEntry) {
        lock.lock()
        pending.append(entry)
        let schedule = !flushScheduled
        flushScheduled = true
        lock.unlock()
        if schedule { postToMain { [weak self] in self?.flush() } }
    }

    /// Must run on the main thread.
    func flush() {
        lock.lock()
        let batch = pending
        pending = []
        pending.reserveCapacity(max(64, batch.count))
        flushScheduled = false
        lock.unlock()
        if !batch.isEmpty { deliver(FrameWriter.encode(handle: handle, entries: batch)) }
    }
}
