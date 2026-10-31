package io.cengyan.query;

import io.cengyan.catalog.StatsRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatsCacheTest {
    @Test
    void 三十分钟内数量和大小都不再扫描() {
        AtomicLong now = new AtomicLong(1_000_000L);
        StatsCache cache = new StatsCache(StatsCache.FRESH_MILLIS, now::get);
        AtomicInteger countLoads = new AtomicInteger();
        AtomicInteger sizeLoads = new AtomicInteger();

        StatsCache.Snapshot<List<StatsRow>> first = cache.counts(1, () -> {
            countLoads.incrementAndGet();
            return List.of(new StatsRow("NODE", "Company", 3));
        });
        StatsCache.Snapshot<List<StatsRow>> second = cache.counts(1, () -> {
            countLoads.incrementAndGet();
            return List.of(new StatsRow("NODE", "Company", 9));
        });
        assertEquals(first, second);
        assertEquals(1, countLoads.get());
        assertEquals(List.of(), cache.counts(2, () -> {
            countLoads.incrementAndGet();
            return List.of();
        }).value());
        assertEquals(2, countLoads.get());

        assertEquals(100L, cache.storage(() -> {
            sizeLoads.incrementAndGet();
            return 100L;
        }).value());
        now.addAndGet(StatsCache.FRESH_MILLIS - 1);
        assertEquals(100L, cache.storage(() -> {
            sizeLoads.incrementAndGet();
            return 200L;
        }).value());
        assertEquals(1, sizeLoads.get());

        now.addAndGet(1);
        assertEquals(9L, cache.counts(1, () -> {
            countLoads.incrementAndGet();
            return List.of(new StatsRow("NODE", "Company", 9));
        }).value().get(0).count());
        assertEquals(200L, cache.storage(() -> {
            sizeLoads.incrementAndGet();
            return 200L;
        }).value());
        assertEquals(3, countLoads.get());
        assertEquals(2, sizeLoads.get());
    }

    @Test
    void 读不到大小时不进入生鲜期() {
        AtomicInteger loads = new AtomicInteger();
        StatsCache cache = new StatsCache(StatsCache.FRESH_MILLIS, () -> 1L);
        assertEquals(-1L, cache.storage(() -> {
            loads.incrementAndGet();
            return -1L;
        }).value());
        assertEquals(8L, cache.storage(() -> {
            loads.incrementAndGet();
            return 8L;
        }).value());
        assertEquals(2, loads.get());
    }
}
