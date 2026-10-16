package com.faciletech.signalr_native.core

import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.random.Random

/**
 * Same semantics as the Dart ReconnectPolicy and ReconnectPolicy.swift: the delay is
 * waited BEFORE each attempt (the .NET/JS client behaviour). null means give up.
 */
internal class ReconnectPolicy(
    private val delaysMs: List<Long>,
    private val exponential: Boolean,
    private val initialMs: Long,
    private val maxMs: Long,
    private val multiplier: Double,
    private val jitter: Double,
    private val maxAttempts: Long?,
    private val maxElapsedMs: Long?,
    private val random: Random = Random.Default,
) {
    /** @param retryCount attempts already made in this reconnect cycle (0 for the first). */
    fun nextDelayMs(retryCount: Int, elapsedMs: Long): Long? {
        if (!exponential) return delaysMs.getOrNull(retryCount)
        if (maxAttempts != null && retryCount >= maxAttempts) return null
        if (maxElapsedMs != null && elapsedMs >= maxElapsedMs) return null
        val base = min(maxMs.toDouble(), initialMs * multiplier.pow(retryCount))
        val factor = if (jitter == 0.0) 1.0 else 1.0 + jitter * (random.nextDouble() * 2.0 - 1.0)
        return max(0L, (base * factor).toLong())
    }
}
