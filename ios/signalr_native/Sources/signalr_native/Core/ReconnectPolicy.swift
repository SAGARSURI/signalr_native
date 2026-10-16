import Foundation

/// Same semantics as the Dart ReconnectPolicy and ReconnectPolicy.kt: the delay is waited
/// BEFORE each attempt (the .NET/JS client behaviour). nil means give up.
struct ReconnectPolicy {
    let delaysMs: [Int64]
    let exponential: Bool
    let initialMs: Int64
    let maxMs: Int64
    let multiplier: Double
    let jitter: Double
    let maxAttempts: Int64?
    let maxElapsedMs: Int64?
    /// Uniform in [0, 1). Injectable for tests.
    var random: () -> Double = { Double.random(in: 0..<1) }

    /// - Parameter retryCount: attempts already made in this reconnect cycle (0 for the first).
    func nextDelayMs(retryCount: Int, elapsedMs: Int64) -> Int64? {
        if !exponential {
            return retryCount < delaysMs.count ? delaysMs[retryCount] : nil
        }
        if let maxAttempts, Int64(retryCount) >= maxAttempts { return nil }
        if let maxElapsedMs, elapsedMs >= maxElapsedMs { return nil }
        let base = min(Double(maxMs), Double(initialMs) * pow(multiplier, Double(retryCount)))
        let factor = jitter == 0 ? 1.0 : 1.0 + jitter * (random() * 2.0 - 1.0)
        return max(0, Int64(base * factor))
    }
}
