package com.faciletech.signalr_native.core

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Test

class ReconnectControllerTest {
    /** Manual time + task queue; start() results are scripted per call. */
    private class Harness(policy: ReconnectPolicy) {
        var now = 0L
        private val tasks = ArrayList<Pair<Long, () -> Unit>>()
        val startResults = ArrayDeque<Throwable?>()
        var pendingStart: ((Throwable?) -> Unit)? = null
        var startCalls = 0
        var stopCalls = 0
        val states = ArrayList<String>()

        val connection = object : ManagedConnection {
            override fun start(onResult: (Throwable?) -> Unit) {
                startCalls++
                if (startResults.isEmpty()) pendingStart = onResult else onResult(startResults.removeFirst())
            }

            override fun stop(onDone: () -> Unit) {
                stopCalls++
                onDone()
            }

            override fun connectionId() = "c$startCalls"
        }

        val controller = ReconnectController(
            connection,
            policy,
            object : Scheduler {
                override fun schedule(delayMs: Long, task: () -> Unit): Cancellable {
                    val entry = (now + delayMs) to task
                    tasks.add(entry)
                    return object : Cancellable {
                        override fun cancel() {
                            tasks.remove(entry)
                        }
                    }
                }

                override fun nowMs() = now
            },
        ) { kind, id, attempt, error ->
            states.add(listOfNotNull(kind.name, id, attempt.takeIf { it > 0 }?.toString(), error).joinToString(" "))
        }

        fun advance(ms: Long) {
            now += ms
            while (true) {
                val due = tasks.filter { it.first <= now }.minByOrNull { it.first } ?: break
                tasks.remove(due)
                due.second()
            }
        }

        val scheduled get() = tasks.size
    }

    private val standard = ReconnectPolicy(listOf(0, 2000, 10000, 30000), false, 0, 0, 1.0, 0.0, null, null)

    @Test
    fun startAndUnexpectedCloseReconnectAfterEachDelay() {
        val h = Harness(standard)
        h.startResults.add(null)
        var result: Throwable? = Exception("not called")
        h.controller.start { result = it }
        assertNull(result)
        assertTrue(h.controller.isConnected)

        h.startResults.addAll(listOf(Exception("refused"), Exception("refused"), null))
        h.controller.onConnectionClosed(Exception("socket closed"))
        h.advance(0) // attempt 1 after 0 ms: fails
        h.advance(1999)
        assertEquals("attempt 2 waits the full 2 s", 2, h.startCalls)
        h.advance(1) // attempt 2: fails
        h.advance(10000) // attempt 3: succeeds

        assertEquals(
            listOf(
                "CONNECTING",
                "CONNECTED c1",
                "RECONNECTING 1 socket closed",
                "RECONNECTING 2 refused",
                "RECONNECTING 3 refused",
                "CONNECTED c4",
            ),
            h.states,
        )
    }

    @Test
    fun givesUpWhenPolicyIsExhausted() {
        val h = Harness(ReconnectPolicy(listOf(0, 100), false, 0, 0, 1.0, 0.0, null, null))
        h.startResults.add(null)
        h.controller.start {}
        h.startResults.addAll(listOf(Exception("a"), Exception("b")))
        h.controller.onConnectionClosed(Exception("lost"))
        h.advance(0)
        h.advance(100)
        assertEquals("DISCONNECTED b", h.states.last())
        assertEquals(0, h.scheduled)
    }

    @Test
    fun noneMeansNoReconnect() {
        val h = Harness(ReconnectPolicy(emptyList(), false, 0, 0, 1.0, 0.0, null, null))
        h.startResults.add(null)
        h.controller.start {}
        h.controller.onConnectionClosed(Exception("lost"))
        assertEquals(listOf("CONNECTING", "CONNECTED c1", "DISCONNECTED lost"), h.states)
    }

    @Test
    fun userStopIsNotReconnected() {
        val h = Harness(standard)
        h.startResults.add(null)
        h.controller.start {}
        var stopped = false
        h.controller.stop { stopped = true }
        h.controller.onConnectionClosed(null) // the client reports its own close
        h.advance(60000)
        assertTrue(stopped)
        assertEquals(listOf("CONNECTING", "CONNECTED c1", "DISCONNECTED"), h.states)
        assertEquals(1, h.startCalls)
    }

    @Test
    fun stopDuringReconnectDelayCancelsTheAttempt() {
        val h = Harness(standard)
        h.startResults.add(null)
        h.controller.start {}
        h.startResults.add(Exception("refused"))
        h.controller.onConnectionClosed(Exception("lost"))
        h.advance(0) // attempt 1 fails, attempt 2 scheduled in 2 s
        h.controller.stop {}
        h.advance(60000)
        assertEquals(2, h.startCalls)
        assertEquals("DISCONNECTED", h.states.last())
        assertEquals(0, h.scheduled)
    }

    @Test
    fun stopWhileAnAttemptIsInFlightIgnoresItsResult() {
        val h = Harness(standard)
        h.startResults.add(null)
        h.controller.start {}
        h.controller.onConnectionClosed(Exception("lost"))
        h.advance(0) // attempt 1 pending (no scripted result)
        h.controller.stop {}
        assertEquals(1, h.stopCalls)
        h.pendingStart!!(null) // late success must not resurrect the connection
        assertEquals("DISCONNECTED", h.states.last())
        assertTrue(!h.controller.isConnected)
        assertEquals("the late success is closed again", 2, h.stopCalls)
    }

    @Test
    fun startThatSucceedsAfterStopFinishedIsClosed() {
        val h = Harness(standard)
        var error: Throwable? = null
        h.controller.start { error = it }
        var stopped = false
        h.controller.stop { stopped = true }
        assertTrue(stopped)
        assertEquals(1, h.stopCalls)
        h.pendingStart!!(null) // the start won the race inside the client
        assertEquals("the late success is closed again", 2, h.stopCalls)
        assertTrue(error != null)
        assertEquals(listOf("CONNECTING", "DISCONNECTED"), h.states)
        assertTrue(!h.controller.isConnected)
    }

    @Test
    fun failedInitialStartDoesNotEnterReconnect() {
        val h = Harness(standard)
        h.startResults.add(Exception("401"))
        var error: Throwable? = null
        h.controller.start { error = it }
        assertEquals("401", error?.message)
        assertEquals(listOf("CONNECTING", "DISCONNECTED 401"), h.states)
        assertEquals(0, h.scheduled)
    }

    @Test
    fun exponentialPolicyGrowsCapsAndStops() {
        val p = ReconnectPolicy(emptyList(), true, 500, 4000, 2.0, 0.0, 6, null)
        assertEquals(listOf(500L, 1000L, 2000L, 4000L, 4000L, 4000L), (0 until 6).map { p.nextDelayMs(it, 0) })
        assertNull(p.nextDelayMs(6, 0))

        val elapsed = ReconnectPolicy(emptyList(), true, 500, 4000, 2.0, 0.0, null, 10000)
        assertNull(elapsed.nextDelayMs(1, 10000))
    }

    @Test
    fun jitterStaysWithinBounds() {
        val p = ReconnectPolicy(emptyList(), true, 1000, 1000, 1.0, 0.2, null, null)
        repeat(1000) {
            val d = p.nextDelayMs(0, 0)!!
            assertTrue("$d", d in 800..1200)
        }
    }
}
