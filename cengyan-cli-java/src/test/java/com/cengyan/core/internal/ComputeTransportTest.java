package com.cengyan.core.internal;

import com.cengyan.core.Address;
import com.cengyan.core.exception.ConnectionException;
import com.cengyan.core.exception.ProtocolException;
import com.cengyan.core.exception.ReadTimeoutException;
import com.cengyan.core.exception.RequestTooLargeException;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ComputeTransportTest {
    private static final Address A = new Address("a", 1);
    private static final Address B = new Address("b", 2);
    private static final Address C = new Address("c", 3);

    private static ComputeTransport transport(FakeConnections fake, Address... addresses) {
        return transport(fake, 30_000, addresses);
    }

    private static ComputeTransport transport(FakeConnections fake, long timeout, Address... addresses) {
        return new ComputeTransport(Arrays.asList(addresses), timeout, new ConnectionPool(fake, 8, 60_000), new Random(1));
    }

    private static Map<String, Object> payload(String op) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("op", op);
        map.put("token", "user-token");
        return map;
    }

    private static ConnectionException lost() {
        return new ConnectionException("lost", "CONNECTION_ERROR", null);
    }

    @Test
    void writeRetriesSameRequestAtOtherNodes() {
        AtomicInteger calls = new AtomicInteger();
        FakeConnections fake = new FakeConnections((address, p) -> calls.incrementAndGet() <= 2 ? lost() : FakeConnections.response("OK"));
        ComputeTransport transport = transport(fake, A, B, C);
        Map<String, Object> original = payload("write");
        try {
            assertEquals("OK", transport.request(original, null, true, null).get("code"));
            assertEquals(3, fake.sent.size());
            Set<Object> ids = new HashSet<Object>();
            for (FakeConnections.Sent sent : fake.sent) ids.add(sent.payload.get("requestId"));
            assertEquals(1, ids.size());
            assertNotEquals(fake.sent.get(0).address, fake.sent.get(1).address);
            assertFalse(original.containsKey("requestId"), "调用方的 payload 不能被修改");
            for (FakeConnections.Sent sent : fake.sent) assertEquals("user-token", sent.payload.get("token"));
        } finally {
            transport.close();
        }
    }

    @Test
    void fourFailedSendsReturnFailedWithOriginalRequestId() {
        FakeConnections fake = new FakeConnections((address, p) -> lost());
        ComputeTransport transport = transport(fake, A, B);
        Map<String, Object> request = payload("write");
        request.put("requestId", "original");
        try {
            Map<String, Object> response = transport.request(request, null, true, null);
            assertEquals("FAILED", response.get("code"));
            assertTrue(String.valueOf(response.get("message")).contains("3"));
            assertEquals("original", response.get("requestId"));
            assertEquals(4, fake.sent.size());
            for (FakeConnections.Sent sent : fake.sent) assertEquals("original", sent.payload.get("requestId"));
        } finally {
            transport.close();
        }
    }

    @Test
    void readDisconnectIsNotRetriedButMovesDefaultEntry() {
        AtomicInteger calls = new AtomicInteger();
        FakeConnections fake = new FakeConnections((address, p) -> calls.incrementAndGet() == 1 ? lost() : FakeConnections.response("OK"));
        ComputeTransport transport = transport(fake, A, B, C);
        try {
            Address failedAt = transport.address();
            assertThrows(ConnectionException.class, () -> transport.request(payload("read"), null, false, null));
            assertEquals(1, fake.sent.size());
            assertNotEquals(failedAt, transport.address());
            transport.request(payload("read"), null, false, null);
            assertNotEquals(failedAt, fake.sent.get(1).address);
        } finally {
            transport.close();
        }
    }

    @Test
    void businessErrorsAndProtocolErrorsAreNotRetried() {
        FakeConnections conflict = new FakeConnections((address, p) -> FakeConnections.response("WRITE_CONFLICT"));
        ComputeTransport first = transport(conflict, A);
        assertEquals("WRITE_CONFLICT", first.request(payload("write"), null, true, null).get("code"));
        assertEquals(1, conflict.sent.size());
        first.close();

        FakeConnections broken = new FakeConnections((address, p) -> new ProtocolException("invalid", "PROTOCOL_ERROR", null));
        ComputeTransport second = transport(broken, A, B);
        assertThrows(ProtocolException.class, () -> second.request(payload("write"), null, true, null));
        assertEquals(1, broken.sent.size());
        second.close();
    }

    @Test
    void oversizedRequestIsNotRetriedAndConnectionIsKept() {
        FakeConnections fake = new FakeConnections((address, p) -> new RequestTooLargeException("too large", "REQUEST_TOO_LARGE", null));
        ComputeTransport transport = transport(fake, A, B);
        try {
            assertThrows(RequestTooLargeException.class, () -> transport.request(payload("write"), null, true, null));
            assertEquals(1, fake.sent.size());
            assertEquals(1, transport.pool().idleCount());
        } finally {
            transport.close();
        }
    }

    @Test
    void addressValidation() {
        FakeConnections fake = FakeConnections.ok();
        assertThrows(IllegalArgumentException.class, () -> transport(fake));
        assertThrows(IllegalArgumentException.class, () -> transport(fake, A, A));
        assertThrows(IllegalArgumentException.class, () -> transport(fake, 0, A));
        assertThrows(IllegalArgumentException.class, () -> new Address("a", 0));
        assertThrows(IllegalArgumentException.class, () -> new Address(" ", 1));
        assertEquals(new Address("::1", 19669), Address.parse("[::1]:19669"));
        assertEquals(new Address("graph.internal", 19669), Address.parse("graph.internal:19669"));
        assertThrows(IllegalArgumentException.class, () -> Address.parse("graph.internal"));
    }

    @Test
    void addressDiscoveryKeepsCurrentEntryWhenPossible() {
        FakeConnections fake = FakeConnections.ok();
        ComputeTransport transport = transport(fake, A);
        try {
            transport.request(payload("ping"), null, false, null);
            transport.updateAddresses(Arrays.asList(B, A, A));
            assertEquals(A, transport.address());
            assertEquals(0, fake.closed.size());
            transport.updateAddresses(Arrays.asList(B));
            assertEquals(B, transport.address());
            assertEquals(Arrays.asList(A), fake.closed);
        } finally {
            transport.close();
        }
    }

    @Test
    void retryBudgetDoesNotRestartOriginalDeadline() throws Exception {
        FakeConnections fake = new FakeConnections((address, p) -> {
            try {
                Thread.sleep(120);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return lost();
        });
        ComputeTransport transport = transport(fake, 200, A, B);
        try {
            Map<String, Object> request = payload("write");
            request.put("requestId", "fixed");
            Map<String, Object> response = transport.request(request, null, true, null);
            assertEquals("FAILED", response.get("code"));
            assertTrue(String.valueOf(response.get("message")).contains("截止时间"));
            assertEquals(2, fake.sent.size());
            assertTrue(((Long) fake.sent.get(0).payload.get("remainingTimeoutMs")) <= 200);
            assertTrue(((Long) fake.sent.get(1).payload.get("remainingTimeoutMs")) < 100);
        } finally {
            transport.close();
        }
    }

    @Test
    void nonPositiveTimeoutIsRejected() {
        FakeConnections fake = FakeConnections.ok();
        ComputeTransport transport = transport(fake, A);
        assertThrows(IllegalArgumentException.class, () -> transport.request(payload("read"), 0L, false, null));
        transport.close();
    }

    @Test
    void alternatingPreferredAddressesReuseConnections() {
        FakeConnections fake = FakeConnections.ok();
        ComputeTransport transport = transport(fake, A, B);
        try {
            for (int i = 0; i < 5; i++) {
                transport.request(payload("bulkInsert"), null, true, A);
                transport.request(payload("bulkInsert"), null, true, B);
            }
            assertEquals(2, fake.opened.size());
        } finally {
            transport.close();
        }
        assertEquals(2, fake.closed.size());
    }

    @Test
    void failedPreferredAddressDoesNotMoveDefaultEntry() {
        Address x = new Address("x", 9);
        FakeConnections fake = new FakeConnections((address, p) -> address.equals(x) ? lost() : FakeConnections.response("OK"));
        ComputeTransport transport = transport(fake, A, B, C);
        try {
            Address entry = transport.address();
            assertEquals("OK", transport.request(payload("bulkInsert"), null, true, x).get("code"));
            assertEquals(entry, fake.sent.get(1).address, "指定地址断线后改走默认入口重试");
            assertEquals(entry, transport.address());
        } finally {
            transport.close();
        }
    }

    @Test
    void concurrentRequestsUseSeparateConnectionsAndPoolKeepsAtMostMaxIdle() throws Exception {
        CountDownLatch inside = new CountDownLatch(12);
        FakeConnections fake = new FakeConnections((address, p) -> {
            inside.countDown();
            try {
                inside.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
            return FakeConnections.response("OK");
        });
        ComputeTransport transport = transport(fake, A);
        Thread[] threads = new Thread[12];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> transport.request(payload("ping"), null, false, null));
            threads[i].start();
        }
        for (Thread thread : threads) thread.join();
        assertEquals(12, fake.opened.size());
        assertEquals(8, transport.pool().idleCount());
        assertEquals(4, fake.closed.size());
        assertEquals(0, transport.pool().borrowedCount());
        transport.close();
        assertEquals(12, fake.closed.size());
        assertThrows(ConnectionException.class, () -> transport.request(payload("ping"), null, false, null));
    }

    @Test
    void writeDuringCloseIsReportedAsUncertain() {
        ComputeTransport[] holder = new ComputeTransport[1];
        FakeConnections fake = new FakeConnections((address, p) -> {
            holder[0].close();
            return lost();
        });
        holder[0] = transport(fake, A, B);
        Map<String, Object> request = payload("bulkInsert");
        request.put("requestId", "r1");
        Map<String, Object> response = holder[0].request(request, null, true, null);
        assertEquals("FAILED", response.get("code"));
        assertEquals("r1", response.get("requestId"));
    }

    @Test
    void idleConnectionsExpire() throws Exception {
        FakeConnections fake = FakeConnections.ok();
        ComputeTransport transport = new ComputeTransport(Arrays.asList(A), 30_000, new ConnectionPool(fake, 8, 50), new Random(1));
        try {
            transport.request(payload("ping"), null, false, null);
            Thread.sleep(80);
            transport.request(payload("ping"), null, false, null);
            assertEquals(2, fake.opened.size());
            assertEquals(1, fake.closed.size());
        } finally {
            transport.close();
        }
    }

    @Test
    void readTimeoutAlsoTriggersWriteRetry() {
        AtomicInteger calls = new AtomicInteger();
        FakeConnections fake = new FakeConnections((address, p) -> calls.incrementAndGet() == 1
                ? new ReadTimeoutException("slow", "READ_TIMEOUT", null) : FakeConnections.response("OK"));
        ComputeTransport transport = transport(fake, A, B);
        try {
            assertEquals("OK", transport.request(payload("write"), null, true, null).get("code"));
            List<FakeConnections.Sent> sent = fake.sent;
            assertEquals(sent.get(0).payload.get("requestId"), sent.get(1).payload.get("requestId"));
        } finally {
            transport.close();
        }
    }
}
