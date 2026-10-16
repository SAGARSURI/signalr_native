package com.faciletech.signalr_native.core

import java.nio.ByteBuffer
import java.nio.ByteOrder

/** One message in a data-plane batch. See lib/src/frame_codec.dart for the layout. */
internal class DataEntry private constructor(
    val kind: Int,
    val id: Long,
    val receivedAtMicros: Long,
    val args: Array<ByteArray>,
    val error: ByteArray?,
) {
    companion object {
        const val KIND_INVOCATION = 0
        const val KIND_STREAM_ITEM = 1
        const val KIND_STREAM_END = 2

        fun invocation(methodIndex: Int, receivedAtMicros: Long, args: Array<ByteArray>) =
            DataEntry(KIND_INVOCATION, methodIndex.toLong(), receivedAtMicros, args, null)

        fun streamItem(streamId: Long, receivedAtMicros: Long, item: ByteArray) =
            DataEntry(KIND_STREAM_ITEM, streamId, receivedAtMicros, arrayOf(item), null)

        fun streamEnd(streamId: Long, receivedAtMicros: Long, error: String?) =
            DataEntry(KIND_STREAM_END, streamId, receivedAtMicros, emptyArray(), error?.toByteArray(Charsets.UTF_8))
    }
}

/** Encodes batches in the layout Dart's FrameCodec reads. Mirrors FrameWriter.swift. */
internal object FrameWriter {
    const val VERSION = 1
    const val HEADER_SIZE = 12
    const val MESSAGE_HEADER_SIZE = 16

    /** Returns a direct, little-endian buffer positioned at 0, as BinaryMessenger expects. */
    fun encode(handle: Long, entries: List<DataEntry>): ByteBuffer {
        var size = HEADER_SIZE
        for (e in entries) {
            size += MESSAGE_HEADER_SIZE
            if (e.kind == DataEntry.KIND_STREAM_END) {
                size += 4 + (e.error?.size ?: 0)
            } else {
                for (a in e.args) size += 4 + a.size
            }
        }

        val buf = ByteBuffer.allocateDirect(size).order(ByteOrder.LITTLE_ENDIAN)
        buf.put(0x53).put(0x52).put(VERSION.toByte()).put(0)
        buf.putInt(handle.toInt())
        buf.putInt(entries.size)
        for (e in entries) {
            buf.put(e.kind.toByte())
            buf.put(if (e.kind == DataEntry.KIND_STREAM_END) 0 else e.args.size.toByte())
            buf.putShort(0)
            buf.putInt(e.id.toInt())
            buf.putLong(e.receivedAtMicros)
            if (e.kind == DataEntry.KIND_STREAM_END) {
                val err = e.error
                buf.putInt(err?.size ?: 0)
                if (err != null) buf.put(err)
            } else {
                for (a in e.args) {
                    buf.putInt(a.size)
                    buf.put(a)
                }
            }
        }
        buf.flip()
        return buf
    }
}
