import XCTest
@testable import SignalRNativeCore

final class OutboundQueueTests: XCTestCase {
    private actor Log {
        var order: [Int] = []
        func append(_ i: Int) { order.append(i) }
    }

    func testStepsRunInEnqueueOrderEvenWhenEarlierOnesSuspend() async {
        let queue = OutboundQueue()
        let log = Log()
        let done = expectation(description: "all steps ran")
        done.expectedFulfillmentCount = 20
        for i in 0..<20 {
            queue.enqueue {
                // Earlier steps suspend longer, so separate Tasks would finish in reverse.
                try? await Task.sleep(nanoseconds: UInt64(20 - i) * 1_000_000)
                await log.append(i)
                done.fulfill()
            }
        }
        await fulfillment(of: [done], timeout: 5)
        let order = await log.order
        XCTAssertEqual(order, Array(0..<20))
    }
}
