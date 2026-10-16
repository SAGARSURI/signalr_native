package com.faciletech.signalr_native.core

/**
 * Wall-clock microseconds since epoch, with nanoTime resolution and monotonic within a
 * process. Matches Dart's DateTime.now().microsecondsSinceEpoch closely enough to measure
 * bridge latency. (Instant.now() would need API 26.)
 */
internal object Clock {
    private val baseMicros: Long = System.currentTimeMillis() * 1000L - System.nanoTime() / 1000L

    fun nowMicros(): Long = baseMicros + System.nanoTime() / 1000L
}
