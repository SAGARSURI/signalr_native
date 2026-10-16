package com.faciletech.signalr_native.core

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.gson.JsonElement
import com.google.gson.JsonNull
import com.google.gson.JsonParser
import com.google.gson.JsonPrimitive
import org.msgpack.jackson.dataformat.MessagePackFactory
import java.lang.reflect.Type

/**
 * How one argument / return value is bound by the Java client and turned into the bytes
 * Dart receives (and back for outbound calls). Chosen once per connection from
 * (protocol, payloadMode). Spike results: JsonElement and Object keep the args path cheap;
 * String / byte[] make the envelope path a straight copy.
 */
internal sealed class PayloadCodec {
    /** Parameter/return type registered with the Java client. */
    abstract val type: Type

    /**
     * Marker the NullResultProtocol puts in place of a null/void invocation result, so the
     * typed invoke completes instead of hanging. Must be an instance of [type], compared
     * by identity.
     */
    abstract val nullResult: Any

    abstract fun toBytes(value: Any?): ByteArray

    abstract fun fromBytes(bytes: ByteArray): Any?

    /** Result bytes for Dart; null for void/null results. */
    fun resultBytes(value: Any?): ByteArray? =
        if (value == null || value === nullResult) null else toBytes(value)

    /** JSON hub protocol, args mode: each argument is a Gson tree; number text is preserved. */
    object JsonArgs : PayloadCodec() {
        override val type: Type = JsonElement::class.java
        override val nullResult: Any = JsonPrimitive(String(CharArray(0)))

        override fun toBytes(value: Any?): ByteArray =
            ((value as JsonElement?) ?: JsonNull.INSTANCE).toString().toByteArray(Charsets.UTF_8)

        override fun fromBytes(bytes: ByteArray): Any = JsonParser.parseString(String(bytes, Charsets.UTF_8))
    }

    /** JSON hub protocol, envelope mode: one JSON string argument carrying the payload. */
    object JsonEnvelope : PayloadCodec() {
        override val type: Type = String::class.java
        override val nullResult: Any = String(CharArray(0))

        override fun toBytes(value: Any?): ByteArray = (value as String?)?.toByteArray(Charsets.UTF_8) ?: ByteArray(0)

        override fun fromBytes(bytes: ByteArray): Any = String(bytes, Charsets.UTF_8)
    }

    /**
     * MessagePack hub protocol, args mode: Object, not JsonNode (JsonNode fails on scalar
     * arguments), re-packed with the same Jackson setup the client uses.
     * Limitation of the Java client: MessagePack extension types (e.g. .NET DateTime) are
     * rejected — send timestamps as integers.
     */
    object MsgpackArgs : PayloadCodec() {
        private val mapper = ObjectMapper(MessagePackFactory())
        override val type: Type = Any::class.java
        override val nullResult: Any = Any()

        override fun toBytes(value: Any?): ByteArray = mapper.writeValueAsBytes(value)

        override fun fromBytes(bytes: ByteArray): Any? = mapper.readValue(bytes, Any::class.java)
    }

    /** MessagePack hub protocol, envelope mode: one `bin` (byte[]) argument. */
    object MsgpackEnvelope : PayloadCodec() {
        override val type: Type = ByteArray::class.java
        override val nullResult: Any = ByteArray(0)

        override fun toBytes(value: Any?): ByteArray = (value as ByteArray?) ?: ByteArray(0)

        override fun fromBytes(bytes: ByteArray): Any = bytes
    }

    companion object {
        fun of(messagePack: Boolean, envelope: Boolean): PayloadCodec = when {
            messagePack && envelope -> MsgpackEnvelope
            messagePack -> MsgpackArgs
            envelope -> JsonEnvelope
            else -> JsonArgs
        }
    }
}
