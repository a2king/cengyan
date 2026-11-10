package io.cengyan.compute;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class AdmissionQueueTest {
    @Test void 超时和取消后不会迟到开始且不延长等待时间() {
        AtomicLong time = new AtomicLong();
        var queue = new AdmissionQueue<String>(1, Duration.ofSeconds(2), time::get);
        var first = queue.submit("q1", "MATCH", Duration.ofSeconds(10));
        assertEquals("BUSY", queue.submit("q2", "MATCH", Duration.ofSeconds(10)).completion().join().code());
        assertSame(first, queue.poll());
        time.addAndGet(Duration.ofSeconds(1).toNanos());
        assertTrue(queue.retryReservation(first));
        assertSame(first, queue.poll());
        time.addAndGet(Duration.ofSeconds(1).toNanos());
        assertFalse(queue.start(first));
        assertEquals("QUEUE_TIMEOUT", first.completion().join().code());
        assertNull(queue.poll());
        var second = queue.submit("q3", "MATCH", Duration.ofSeconds(10));
        assertSame(second, queue.poll());
        assertTrue(queue.cancel(second));
        assertFalse(queue.start(second));
    }

    @Test void 开始后只检查执行截止时间并支持排空() {
        AtomicLong time = new AtomicLong();
        var queue = new AdmissionQueue<String>(1, Duration.ofSeconds(2), time::get);
        var ticket = queue.submit("q1", "MATCH", Duration.ofSeconds(10));
        queue.poll();
        assertTrue(queue.start(ticket));
        queue.beginDrain();
        assertEquals("BUSY", queue.submit("q2", "MATCH", Duration.ofSeconds(1)).completion().join().code());
        time.addAndGet(Duration.ofSeconds(3).toNanos());
        queue.sweep();
        assertEquals(AdmissionQueue.State.RUNNING, ticket.state());
        assertFalse(queue.drained());
        time.addAndGet(Duration.ofSeconds(7).toNanos());
        queue.sweep();
        assertEquals("QUERY_TIMEOUT", ticket.completion().join().code());
        assertThrows(IllegalArgumentException.class, () -> queue.submit("q1", "MATCH", Duration.ofSeconds(1)));
        assertFalse(queue.finish(ticket, new AdmissionQueue.Outcome("OK", "")));
        assertFalse(queue.drained());
        assertTrue(queue.cleanupComplete(ticket));
        assertTrue(queue.drained());
    }

    @Test void 外部完成通知不能修改队列内部状态() {
        var queue = new AdmissionQueue<String>(1, Duration.ofSeconds(2));
        var ticket = queue.submit("q", "MATCH", Duration.ofSeconds(10));
        ticket.completion().complete(new AdmissionQueue.Outcome("OK", "forged"));
        assertEquals(AdmissionQueue.State.QUEUED, ticket.state());
        assertFalse(ticket.completion().isDone());
        queue.poll();
        assertTrue(queue.start(ticket));
        assertTrue(queue.finish(ticket, new AdmissionQueue.Outcome("OK", "")));
        assertEquals("OK", ticket.completion().join().code());
    }
}
