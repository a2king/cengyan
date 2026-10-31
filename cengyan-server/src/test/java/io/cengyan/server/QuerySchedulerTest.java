package io.cengyan.server;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QuerySchedulerTest {
    @Test
    void 重查询和普通查询占满时登录仍能执行() throws Exception {
        QueryScheduler scheduler = open("""
                query_threads=2
                interactive_reserved=1
                heavy_query_threads=1
                query_queue_ms=300
                """);
        CountDownLatch heavyStarted = new CountDownLatch(1);
        CountDownLatch generalStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            scheduler.submit(QueryScheduler.Lane.HEAVY, "heavy", 1000, () -> hold(heavyStarted, release),
                    message -> { throw new AssertionError(message); });
            scheduler.submit(QueryScheduler.Lane.GENERAL, "scan", 1000, () -> hold(generalStarted, release),
                    message -> { throw new AssertionError(message); });
            assertTrue(heavyStarted.await(2, TimeUnit.SECONDS));
            assertTrue(generalStarted.await(2, TimeUnit.SECONDS));

            CountDownLatch loginDone = new CountDownLatch(1);
            AtomicReference<String> loginBusy = new AtomicReference<>();
            scheduler.submit(QueryScheduler.Lane.RESERVED, "login", 1000, loginDone::countDown, loginBusy::set);
            assertTrue(loginDone.await(2, TimeUnit.SECONDS), () -> "登录被拒绝：" + loginBusy.get());

            CountDownLatch rejected = new CountDownLatch(1);
            AtomicReference<String> busy = new AtomicReference<>();
            scheduler.submit(QueryScheduler.Lane.HEAVY, "another-heavy", 1000, () -> { }, message -> {
                busy.set(message);
                rejected.countDown();
            });
            assertTrue(rejected.await(2, TimeUnit.SECONDS));
            assertEquals("查询线程已满", busy.get());
        } finally {
            release.countDown();
            scheduler.close();
        }
    }

    @Test
    void 同一会话排队时不占用别的会话的线程() throws Exception {
        QueryScheduler scheduler = open("""
                query_threads=2
                interactive_reserved=0
                heavy_query_threads=1
                query_queue_ms=2000
                """);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            scheduler.submit(QueryScheduler.Lane.GENERAL, "s1", 1000, () -> hold(started, release),
                    message -> { throw new AssertionError(message); });
            assertTrue(started.await(2, TimeUnit.SECONDS));
            scheduler.submit(QueryScheduler.Lane.GENERAL, "s1", 5000, () -> { }, message -> { });
            Thread.sleep(100);

            CountDownLatch otherDone = new CountDownLatch(1);
            AtomicReference<String> busy = new AtomicReference<>();
            scheduler.submit(QueryScheduler.Lane.GENERAL, "s2", 1000, otherDone::countDown, busy::set);
            assertTrue(otherDone.await(800, TimeUnit.MILLISECONDS), () -> "其他会话被拒绝：" + busy.get());
        } finally {
            release.countDown();
            scheduler.close();
        }
    }

    @Test
    void 语句和重查询会分到不同的池() {
        assertEquals(QueryScheduler.Lane.RESERVED, QueryScheduler.laneForQuery(" \n-- note\nSHOW GRAPHS"));
        assertEquals(QueryScheduler.Lane.RESERVED, QueryScheduler.laneForQuery("use demo"));
        assertEquals(QueryScheduler.Lane.GENERAL, QueryScheduler.laneForQuery("MATCH (a) RETURN a"));
        assertEquals(QueryScheduler.Lane.HEAVY, QueryScheduler.laneForAdmin("stats"));
        assertEquals(QueryScheduler.Lane.HEAVY, QueryScheduler.laneForAdmin("rebuildIndex"));
        assertEquals(QueryScheduler.Lane.GENERAL, QueryScheduler.laneForAdmin("showMembers"));
        assertEquals(120_000L, QueryScheduler.capTimeout(0, 120_000L));
        assertEquals(1000L, QueryScheduler.capTimeout(1000, 120_000L));
        assertEquals(50L, QueryScheduler.capTimeout(50, 0));
    }

    private static void hold(CountDownLatch started, CountDownLatch release) {
        started.countDown();
        try {
            release.await();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }

    private static QueryScheduler open(String properties) throws Exception {
        Path file = Files.createTempFile("cengyan-sched-", ".properties");
        try {
            Files.writeString(file, properties);
            return QueryScheduler.open(ServerConfig.load(file));
        } finally {
            Files.deleteIfExists(file);
        }
    }
}
