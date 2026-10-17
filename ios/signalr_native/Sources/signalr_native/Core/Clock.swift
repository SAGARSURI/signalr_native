import Foundation

/// Wall-clock microseconds since epoch, matching Dart's DateTime.now().microsecondsSinceEpoch
/// closely enough to measure bridge latency. Uses Date rather than an uptime clock so the
/// plugin calls no required-reason API (no privacy manifest needed).
enum Clock {
    static func nowMicros() -> Int64 {
        Int64((Date().timeIntervalSince1970 * 1_000_000).rounded())
    }
}
