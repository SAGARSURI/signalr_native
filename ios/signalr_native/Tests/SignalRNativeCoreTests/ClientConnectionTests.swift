import XCTest
@testable import SignalRNativeCore

/// The start/stop/timeout/close races of ClientConnection, against a fake client whose
/// start() finishes only when the test says so.
@MainActor
final class ClientConnectionTests: XCTestCase {
    private struct TestError: LocalizedError {
        let errorDescription: String?
        init(_ d: String) { errorDescription = d }
    }

    private final class FakeConnection: StartableConnection, @unchecked Sendable {
        private let lock = NSLock()
        private var pending: CheckedContinuation<Void, Error>?
        private var stops = 0
        /// Runs inside start(), just before it returns successfully.
        var beforeStartReturns: (() -> Void)?

        var stopCount: Int { lock.lock(); defer { lock.unlock() }; return stops }
        var isStarting: Bool { lock.lock(); defer { lock.unlock() }; return pending != nil }

        func start() async throws {
            try await withCheckedThrowingContinuation { c in
                lock.lock()
                pending = c
                lock.unlock()
            }
            beforeStartReturns?()
        }

        func stop() async {
            lock.withLock { stops += 1 }
        }

        func completeStart(_ error: Error? = nil) {
            lock.lock()
            let c = pending
            pending = nil
            lock.unlock()
            if let error { c?.resume(throwing: error) } else { c?.resume() }
        }
    }

    /// Production code only posts from the main thread or the start Task; the box keeps the
    /// test's GCD hop free of Sendable warnings.
    private struct MainTask: @unchecked Sendable {
        let run: () -> Void
    }

    private final class Timer: Cancellable {
        let fire: () -> Void
        var cancelled = false
        init(_ fire: @escaping () -> Void) { self.fire = fire }
        func cancel() { cancelled = true }
    }

    private var built: [FakeConnection] = []
    private var flags: [LiveFlag] = []
    private var timers: [Timer] = []
    private var closes: [String?] = []

    private func makeClient(timeoutMs: Int64 = 0) -> ClientConnection<FakeConnection> {
        let client = ClientConnection<FakeConnection>(
            build: { [unowned self] live in
                await MainActor.run {
                    let c = FakeConnection()
                    self.built.append(c)
                    self.flags.append(live)
                    return c
                }
            },
            postToMain: { task in
                let task = MainTask(run: task)
                DispatchQueue.main.async { task.run() }
            },
            schedule: { [unowned self] _, task in
                let t = Timer(task)
                self.timers.append(t)
                return t
            },
            timeoutMs: timeoutMs
        )
        client.onClosed = { [unowned self] error in self.closes.append(error?.localizedDescription) }
        return client
    }

    /// Lets queued main-thread work and the start Task run until `condition` holds.
    private func waitUntil(_ condition: () -> Bool, file: StaticString = #filePath, line: UInt = #line) async {
        for _ in 0..<500 where !condition() {
            try? await Task.sleep(nanoseconds: 2_000_000)
        }
        XCTAssertTrue(condition(), "timed out waiting", file: file, line: line)
    }

    func testSuccessfulStartReportsOnceAndBecomesCurrent() async {
        let client = makeClient()
        var results: [String?] = []
        client.start { results.append($0?.localizedDescription) }
        await waitUntil { self.built.first?.isStarting == true }
        built[0].completeStart()
        await waitUntil { !results.isEmpty }
        XCTAssertEqual(results, [nil])
        XCTAssertTrue(client.current === built[0])
        XCTAssertTrue(flags[0].isOn)
    }

    func testStopWhileStartingReportsAtOnceAndClosesALateSuccess() async {
        let client = makeClient()
        var results: [String?] = []
        client.start { results.append($0?.localizedDescription) }
        await waitUntil { self.built.first?.isStarting == true }
        var stopped = false
        client.stop { stopped = true }
        XCTAssertEqual(results, ["Stopped while connecting"], "reported without waiting for the client")
        XCTAssertTrue(stopped, "stop does not wait for the client's start to unwind")
        XCTAssertFalse(flags[0].isOn, "an aborted attempt delivers nothing")
        await waitUntil { self.built[0].stopCount == 1 }

        built[0].completeStart() // the client connected anyway
        await waitUntil { self.built[0].stopCount == 2 }
        try? await Task.sleep(nanoseconds: 20_000_000)
        XCTAssertEqual(results.count, 1, "the late success is not reported")
        XCTAssertNil(client.current)
    }

    func testTimeoutReportsWhileTheClientStillHangs() async {
        let client = makeClient(timeoutMs: 15000)
        var results: [String?] = []
        client.start { results.append($0?.localizedDescription) }
        await waitUntil { self.built.first?.isStarting == true }
        timers[0].fire()
        XCTAssertEqual(results, ["Start timed out after 15.0 s"])
        await waitUntil { self.built[0].stopCount == 1 }

        built[0].completeStart(TestError("aborted")) // the client's own failure arrives later
        try? await Task.sleep(nanoseconds: 20_000_000)
        XCTAssertEqual(results.count, 1)
    }

    func testFinishedStartCancelsItsTimer() async {
        let client = makeClient(timeoutMs: 15000)
        client.start { _ in }
        await waitUntil { self.built.first?.isStarting == true }
        built[0].completeStart()
        await waitUntil { client.current != nil }
        XCTAssertTrue(timers[0].cancelled)
    }

    func testCloseThatBeatsTheSuccessIsReplayedAfterIt() async {
        let client = makeClient()
        var events: [String] = []
        client.onClosed = { events.append("closed: \($0?.localizedDescription ?? "nil")") }
        client.start { events.append("started: \($0?.localizedDescription ?? "ok")") }
        await waitUntil { self.built.first?.isStarting == true }
        let connection = built[0]
        // The client fires onClosed while start() is still returning, so its post lands first.
        connection.beforeStartReturns = {
            DispatchQueue.main.async { client.connectionClosed(connection, TestError("server closed")) }
        }
        connection.completeStart()
        await waitUntil { events.count == 2 }
        XCTAssertEqual(events, ["started: ok", "closed: server closed"])
    }

    func testClosesOfOtherConnectionsAreIgnored() async {
        let client = makeClient()
        client.start { _ in }
        await waitUntil { self.built.first?.isStarting == true }
        built[0].completeStart()
        await waitUntil { client.current != nil }
        client.connectionClosed(FakeConnection(), TestError("stale"))
        XCTAssertEqual(closes, [])
        client.connectionClosed(built[0], TestError("lost"))
        XCTAssertEqual(closes, ["lost"])
    }

    func testEachStartBuildsANewConnection() async {
        let client = makeClient()
        client.start { _ in }
        await waitUntil { self.built.first?.isStarting == true }
        built[0].completeStart()
        await waitUntil { client.current != nil }
        client.start { _ in }
        await waitUntil { self.built.count == 2 && self.built[1].isStarting }
        XCTAssertFalse(built[0] === built[1])
    }

    func testWithTimeoutGivesUpOnAnOperationThatNeverAnswers() async {
        do {
            _ = try await withTimeout(ms: 10, "token") { () async throws -> String in
                try await Task.sleep(nanoseconds: 5_000_000_000)
                return "late"
            }
            XCTFail("expected a timeout")
        } catch {
            XCTAssertEqual(error.localizedDescription, "token")
        }
        let value = try? await withTimeout(ms: 5000, "token") { "fast" }
        XCTAssertEqual(value, "fast")
    }
}
