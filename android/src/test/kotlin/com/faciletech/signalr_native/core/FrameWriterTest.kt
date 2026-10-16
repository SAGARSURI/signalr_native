package com.faciletech.signalr_native.core

import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Test

class FrameWriterTest {
    private fun bytes(buf: java.nio.ByteBuffer): ByteArray = ByteArray(buf.remaining()).also { buf.get(it) }

    /**
     * Golden bytes, hand-assembled from the layout in lib/src/frame_codec.dart, so Kotlin
     * and Dart cannot drift apart silently.
     */
    @Test
    fun matchesDartLayout() {
        val frame = FrameWriter.encode(
            7,
            listOf(
                DataEntry.invocation(3, 0x0102030405060708, arrayOf("ab".toByteArray(), ByteArray(0))),
                DataEntry.streamEnd(9, 1, "x"),
            ),
        )
        val expected = byteArrayOf(
            // header: 'S' 'R' v1 0 | handle=7 | count=2
            0x53, 0x52, 1, 0, 7, 0, 0, 0, 2, 0, 0, 0,
            // invocation: kind 0, argc 2, pad | id 3 | receivedAt LE
            0, 2, 0, 0, 3, 0, 0, 0, 8, 7, 6, 5, 4, 3, 2, 1,
            // arg "ab", arg ""
            2, 0, 0, 0, 'a'.code.toByte(), 'b'.code.toByte(), 0, 0, 0, 0,
            // stream end: kind 2, argc 0, pad | id 9 | receivedAt 1
            2, 0, 0, 0, 9, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0,
            // error "x"
            1, 0, 0, 0, 'x'.code.toByte(),
        )
        assertTrue("BinaryMessenger needs a direct buffer", frame.isDirect)
        assertEquals(0, frame.position())
        assertArrayEquals(expected, bytes(frame))
    }

    @Test
    fun streamEndWithoutErrorHasZeroLength() {
        val frame = bytes(FrameWriter.encode(1, listOf(DataEntry.streamEnd(4, 0, null))))
        assertEquals(FrameWriter.HEADER_SIZE + FrameWriter.MESSAGE_HEADER_SIZE + 4, frame.size)
        assertArrayEquals(byteArrayOf(0, 0, 0, 0), frame.copyOfRange(frame.size - 4, frame.size))
    }

    @Test
    fun emptyBatch() {
        val frame = bytes(FrameWriter.encode(2, emptyList()))
        assertArrayEquals(byteArrayOf(0x53, 0x52, 1, 0, 2, 0, 0, 0, 0, 0, 0, 0), frame)
    }
}
