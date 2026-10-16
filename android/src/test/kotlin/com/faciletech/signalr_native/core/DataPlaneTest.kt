package com.faciletech.signalr_native.core

import org.junit.Assert.assertEquals
import org.junit.Test
import java.nio.ByteBuffer
import java.nio.ByteOrder

class DataPlaneTest {
    private val posted = ArrayList<Runnable>()
    private val delivered = ArrayList<ByteBuffer>()
    private val plane = DataPlane(5, { posted.add(it) }, { delivered.add(it) })

    private fun count(frame: ByteBuffer) = frame.duplicate().order(ByteOrder.LITTLE_ENDIAN).getInt(8)

    @Test
    fun messagesArrivingBeforeTheFlushRunsShareOneBatch() {
        repeat(3) { plane.add(DataEntry.invocation(0, it.toLong(), arrayOf(byteArrayOf(1)))) }
        assertEquals("only the first add schedules a flush", 1, posted.size)

        posted.removeAt(0).run()
        assertEquals(1, delivered.size)
        assertEquals(3, count(delivered[0]))
    }

    @Test
    fun idleMessagesFlushIndividually() {
        plane.add(DataEntry.invocation(0, 1, arrayOf(byteArrayOf(1))))
        posted.removeAt(0).run()
        plane.add(DataEntry.invocation(0, 2, arrayOf(byteArrayOf(2))))
        assertEquals("a new flush is scheduled after the previous one ran", 1, posted.size)
        posted.removeAt(0).run()
        assertEquals(listOf(1, 1), delivered.map { count(it) })
    }

    @Test
    fun flushWithNothingPendingSendsNothing() {
        plane.flush()
        assertEquals(0, delivered.size)
    }

    @Test
    fun concurrentProducersLoseNothing() {
        val threads = (0 until 8).map { t ->
            Thread { repeat(1000) { plane.add(DataEntry.invocation(t, it.toLong(), arrayOf(byteArrayOf(0)))) } }
        }
        threads.forEach(Thread::start)
        threads.forEach(Thread::join)
        synchronized(posted) { posted.toList() }.forEach(Runnable::run)
        plane.flush()
        assertEquals(8000, delivered.sumOf { count(it) })
    }
}
