import XCTest
@testable import SignalRNativeCore

final class DataPlaneTests: XCTestCase {
    private let lock = NSLock()
    private var posted: [() -> Void] = []
    private var delivered: [Data] = []
    private lazy var plane = DataPlane(
        handle: 5,
        postToMain: { [unowned self] task in self.lock.lock(); self.posted.append(task); self.lock.unlock() },
        deliver: { [unowned self] frame in self.delivered.append(frame) }
    )

    private func count(_ frame: Data) -> UInt32 {
        frame.withUnsafeBytes { UInt32(littleEndian: $0.loadUnaligned(fromByteOffset: 8, as: UInt32.self)) }
    }

    private func runPosted() {
        lock.lock()
        let tasks = posted
        posted = []
        lock.unlock()
        tasks.forEach { $0() }
    }

    func testMessagesArrivingBeforeTheFlushRunsShareOneBatch() {
        for i in 0..<3 { plane.add(.invocation(methodIndex: 0, receivedAtMicros: Int64(i), args: [Data([1])])) }
        XCTAssertEqual(posted.count, 1, "only the first add schedules a flush")
        runPosted()
        XCTAssertEqual(delivered.map(count), [3])
    }

    func testIdleMessagesFlushIndividually() {
        plane.add(.invocation(methodIndex: 0, receivedAtMicros: 1, args: [Data([1])]))
        runPosted()
        plane.add(.invocation(methodIndex: 0, receivedAtMicros: 2, args: [Data([2])]))
        XCTAssertEqual(posted.count, 1, "a new flush is scheduled after the previous one ran")
        runPosted()
        XCTAssertEqual(delivered.map(count), [1, 1])
    }

    func testFlushWithNothingPendingSendsNothing() {
        plane.flush()
        XCTAssertTrue(delivered.isEmpty)
    }

    func testConcurrentProducersLoseNothing() {
        let plane = self.plane
        DispatchQueue.concurrentPerform(iterations: 8) { t in
            for i in 0..<1000 {
                plane.add(.invocation(methodIndex: t, receivedAtMicros: Int64(i), args: [Data([0])]))
            }
        }
        runPosted()
        plane.flush()
        XCTAssertEqual(delivered.map(count).reduce(0, +), 8000)
    }
}
