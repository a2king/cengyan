package io.cengyan.compute;

import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import static org.junit.jupiter.api.Assertions.*;

class ExecutionSlotsTest {
    @Test void 预留过期重复消费和清理释放() {
        AtomicLong now = new AtomicLong();
        var slots = new ExecutionSlots(2, 100, 200, Duration.ofSeconds(10), now::get);
        var first = slots.reserve("r1", 60, 100);
        assertEquals(first, slots.reserve("r1", 60, 100));
        assertNull(slots.reserve("r2", 50, 0));
        assertThrows(IllegalArgumentException.class, () -> slots.reserve("r1", 1, 1));
        assertTrue(slots.start(first));
        assertFalse(slots.start(first));
        assertFalse(slots.cancelReservation(first));
        var second = slots.reserve("r2", 40, 100);
        assertEquals(0, slots.load().available());
        now.addAndGet(Duration.ofSeconds(11).toNanos());
        assertFalse(slots.start(second));
        assertEquals(1, slots.load().active());
        assertEquals(1, slots.load().available());
        assertTrue(slots.finish(first));
        assertFalse(slots.finish(first));
        assertEquals(0, slots.load().memoryReserved());
        assertEquals(0, slots.load().diskReserved());
    }

    @Test void 并发预留不会超过槽位与节点资源() throws Exception {
        var slots = new ExecutionSlots(8, 80, 160, Duration.ofSeconds(10));
        try (var threads = Executors.newFixedThreadPool(16)) {
            CountDownLatch gate = new CountDownLatch(1);
            var attempts = new ArrayList<Future<ExecutionSlots.Reservation>>();
            for (int i = 0; i < 100; i++) {
                String id = "r" + i;
                attempts.add(threads.submit(() -> { gate.await(); return slots.reserve(id, 10, 20); }));
            }
            gate.countDown();
            int accepted = 0;
            for (var attempt : attempts) if (attempt.get(5, TimeUnit.SECONDS) != null) accepted++;
            assertEquals(8, accepted);
            assertEquals(80, slots.load().memoryReserved());
            assertEquals(160, slots.load().diskReserved());
        }
    }

    @Test void 排空拒绝新任务但允许已预留请求开始() {
        var slots = new ExecutionSlots(2, 100, 100, Duration.ofSeconds(10));
        var reservation = slots.reserve("old", 10, 10);
        slots.beginDrain();
        assertNull(slots.reserve("new", 10, 10));
        assertFalse(slots.drained());
        assertTrue(slots.start(reservation));
        assertEquals(0, slots.load().available());
        slots.finish(reservation);
        assertTrue(slots.drained());
    }
}
