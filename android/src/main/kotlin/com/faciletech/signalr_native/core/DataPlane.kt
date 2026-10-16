package com.faciletech.signalr_native.core

import java.nio.ByteBuffer

/**
 * Coalesces server messages into data-plane batches.
 *
 * [add] is called on the client's network thread. The first message after a flush posts
 * one flush to the platform thread; everything that arrives before that flush runs joins
 * the same batch. Idle: one message per batch, no added delay. Under load: batches grow
 * to whatever accumulated during one main-thread hop.
 */
internal class DataPlane(
    private val handle: Long,
    private val postToMain: (Runnable) -> Unit,
    private val deliver: (ByteBuffer) -> Unit,
) {
    private val lock = Any()
    private var pending = ArrayList<DataEntry>(64)
    private var flushScheduled = false
    private val flushTask = Runnable { flush() }

    fun add(entry: DataEntry) {
        val schedule: Boolean
        synchronized(lock) {
            pending.add(entry)
            schedule = !flushScheduled
            flushScheduled = true
        }
        if (schedule) postToMain(flushTask)
    }

    /** Must run on the platform thread. */
    fun flush() {
        val batch: ArrayList<DataEntry>
        synchronized(lock) {
            batch = pending
            pending = ArrayList(maxOf(64, batch.size))
            flushScheduled = false
        }
        if (batch.isNotEmpty()) deliver(FrameWriter.encode(handle, batch))
    }
}
