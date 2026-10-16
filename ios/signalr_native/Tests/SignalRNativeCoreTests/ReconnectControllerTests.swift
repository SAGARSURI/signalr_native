import XCTest
@testable import SignalRNativeCore

/// Port of ReconnectControllerTest.kt: both platforms must reconnect identically.
final class ReconnectControllerTests: XCTestCase {
    private struct TestError: LocalizedError, CustomStringConvertible {
        let description: String
        init(_ d: String) { description = d }
        var errorDescription: String? { description }
    }

    /// Manual time + task queue; start() results are scripted per call.
    private final class Harness: ManagedConnection, Scheduler {
        var now: Int64 = 0
        private var tasks: [(id: Int, at: Int64, run: () -> Void)] = []
        private var nextId = 0
        var startResults: [Error?] = []
        var pendingStart: ((Error?) -> Void)?
        var startCalls = 0
        var stopCalls = 0
        var states: [String] = []
        var controller: ReconnectController!

        init(_ policy: ReconnectPolicy) {
            controller = ReconnectController(connection: self, policy: policy, scheduler: self) { [unowned self] kind, id, attempt, error in
                self.states.append([kind.rawValue, id, attempt > 0 ? String(attempt) : nil, error].compactMap { $0 }.joined(separator: " "))
            }
        }

        func start(onResult: @escaping (Error?) -> Void) {
            startCalls += 1
            if startResults.isEmpty { pendingStart = onResult } else { onResult(startResults.removeFirst()) }
        }

        func stop(onDone: @escaping () -> Void) {
            stopCalls += 1
            onDone()
        }

        func connectionId() -> String? { "c\(startCalls)" }

        private final class Handle: Cancellable {
            let cancelFn: () -> Void
            init(_ f: @escaping () -> Void) { cancelFn = f }
            func cancel() { cancelFn() }
        }

        func schedule(delayMs: Int64, task: @escaping () -> Void) -> Cancellable {
            nextId += 1
            let id = nextId
            tasks.append((id, now + delayMs, task))
            return Handle { [unowned self] in self.tasks.removeAll { $0.id == id } }
        }

        func nowMs() -> Int64 { now }

        func advance(_ ms: Int64) {
            now += ms
            while let due = tasks.filter({ $0.at <= now }).min(by: { $0.at < $1.at }) {
                tasks.removeAll { $0.id == due.id }
                due.run()
            }
        }

        var scheduled: Int { tasks.count }
    }

    private func policy(_ delays: [Int64]) -> ReconnectPolicy {
        ReconnectPolicy(delaysMs: delays, exponential: false, initialMs: 0, maxMs: 0, multiplier: 1, jitter: 0,
                        maxAttempts: nil, maxElapsedMs: nil)
    }

    private lazy var standard = policy([0, 2000, 10000, 30000])

    func testStartAndUnexpectedCloseReconnectAfterEachDelay() {
        let h = Harness(standard)
        h.startResults.append(nil)
        var result: Error? = TestError("not called")
        h.controller.start { result = $0 }
        XCTAssertNil(result)
        XCTAssertTrue(h.controller.isConnected)

        h.startResults.append(contentsOf: [TestError("refused"), TestError("refused"), nil])
        h.controller.onConnectionClosed(TestError("socket closed"))
        h.advance(0) // attempt 1 after 0 ms: fails
        h.advance(1999)
        XCTAssertEqual(h.startCalls, 2, "attempt 2 waits the full 2 s")
        h.advance(1) // attempt 2: fails
        h.advance(10000) // attempt 3: succeeds

        XCTAssertEqual(h.states, [
            "CONNECTING",
            "CONNECTED c1",
            "RECONNECTING 1 socket closed",
            "RECONNECTING 2 refused",
            "RECONNECTING 3 refused",
            "CONNECTED c4",
        ])
    }

    func testGivesUpWhenPolicyIsExhausted() {
        let h = Harness(policy([0, 100]))
        h.startResults.append(nil)
        h.controller.start { _ in }
        h.startResults.append(contentsOf: [TestError("a"), TestError("b")])
        h.controller.onConnectionClosed(TestError("lost"))
        h.advance(0)
        h.advance(100)
        XCTAssertEqual(h.states.last, "DISCONNECTED b")
        XCTAssertEqual(h.scheduled, 0)
    }

    func testNoneMeansNoReconnect() {
        let h = Harness(policy([]))
        h.startResults.append(nil)
        h.controller.start { _ in }
        h.controller.onConnectionClosed(TestError("lost"))
        XCTAssertEqual(h.states, ["CONNECTING", "CONNECTED c1", "DISCONNECTED lost"])
    }

    func testUserStopIsNotReconnected() {
        let h = Harness(standard)
        h.startResults.append(nil)
        h.controller.start { _ in }
        var stopped = false
        h.controller.stop { stopped = true }
        h.controller.onConnectionClosed(nil) // the client reports its own close
        h.advance(60000)
        XCTAssertTrue(stopped)
        XCTAssertEqual(h.states, ["CONNECTING", "CONNECTED c1", "DISCONNECTED"])
        XCTAssertEqual(h.startCalls, 1)
    }

    func testStopDuringReconnectDelayCancelsTheAttempt() {
        let h = Harness(standard)
        h.startResults.append(nil)
        h.controller.start { _ in }
        h.startResults.append(TestError("refused"))
        h.controller.onConnectionClosed(TestError("lost"))
        h.advance(0) // attempt 1 fails, attempt 2 scheduled in 2 s
        h.controller.stop {}
        h.advance(60000)
        XCTAssertEqual(h.startCalls, 2)
        XCTAssertEqual(h.states.last, "DISCONNECTED")
        XCTAssertEqual(h.scheduled, 0)
    }

    func testStopWhileAnAttemptIsInFlightIgnoresItsResult() {
        let h = Harness(standard)
        h.startResults.append(nil)
        h.controller.start { _ in }
        h.controller.onConnectionClosed(TestError("lost"))
        h.advance(0) // attempt 1 pending (no scripted result)
        h.controller.stop {}
        XCTAssertEqual(h.stopCalls, 1)
        h.pendingStart!(nil) // late success must not resurrect the connection
        XCTAssertEqual(h.states.last, "DISCONNECTED")
        XCTAssertFalse(h.controller.isConnected)
    }

    func testFailedInitialStartDoesNotEnterReconnect() {
        let h = Harness(standard)
        h.startResults.append(TestError("401"))
        var error: Error?
        h.controller.start { error = $0 }
        XCTAssertEqual((error as? TestError)?.description, "401")
        XCTAssertEqual(h.states, ["CONNECTING", "DISCONNECTED 401"])
        XCTAssertEqual(h.scheduled, 0)
    }

    func testExponentialPolicyGrowsCapsAndStops() {
        let p = ReconnectPolicy(delaysMs: [], exponential: true, initialMs: 500, maxMs: 4000, multiplier: 2, jitter: 0,
                                maxAttempts: 6, maxElapsedMs: nil)
        XCTAssertEqual((0..<6).map { p.nextDelayMs(retryCount: $0, elapsedMs: 0) }, [500, 1000, 2000, 4000, 4000, 4000])
        XCTAssertNil(p.nextDelayMs(retryCount: 6, elapsedMs: 0))

        let elapsed = ReconnectPolicy(delaysMs: [], exponential: true, initialMs: 500, maxMs: 4000, multiplier: 2,
                                      jitter: 0, maxAttempts: nil, maxElapsedMs: 10000)
        XCTAssertNil(elapsed.nextDelayMs(retryCount: 1, elapsedMs: 10000))
    }

    func testJitterStaysWithinBounds() {
        let p = ReconnectPolicy(delaysMs: [], exponential: true, initialMs: 1000, maxMs: 1000, multiplier: 1, jitter: 0.2,
                                maxAttempts: nil, maxElapsedMs: nil)
        for _ in 0..<1000 {
            let d = p.nextDelayMs(retryCount: 0, elapsedMs: 0)!
            XCTAssertTrue((800...1200).contains(d), "\(d)")
        }
    }
}
