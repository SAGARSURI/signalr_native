package com.faciletech.signalr_native.core

import com.microsoft.signalr.CompletionMessage
import com.microsoft.signalr.HubMessage
import com.microsoft.signalr.HubProtocol
import com.microsoft.signalr.InvocationBinder
import java.nio.ByteBuffer

/**
 * Wraps the client's protocol to fix one behaviour: the Java client's typed invoke only
 * completes when the result is non-null, so a void hub method (or a null result) leaves
 * the call pending forever. Successful completions without a result get [nullResult]
 * instead; the session maps it back to "no payload".
 *
 * Stream completions pass through here too, so stream subscribers must drop [nullResult].
 */
internal class NullResultProtocol(
    private val inner: HubProtocol,
    private val nullResult: Any,
) : HubProtocol {
    override fun getName(): String = inner.name

    override fun getVersion(): Int = inner.version

    override fun parseMessages(message: ByteBuffer, binder: InvocationBinder): List<HubMessage>? {
        val messages = inner.parseMessages(message, binder) ?: return null
        var out: MutableList<HubMessage>? = null
        for (i in messages.indices) {
            val m = messages[i]
            if (m is CompletionMessage && m.error == null && m.result == null) {
                if (out == null) out = messages.toMutableList()
                out[i] = CompletionMessage(m.headers, m.invocationId, nullResult, null)
            }
        }
        return out ?: messages
    }

    override fun writeMessage(message: HubMessage): ByteBuffer = inner.writeMessage(message)
}
