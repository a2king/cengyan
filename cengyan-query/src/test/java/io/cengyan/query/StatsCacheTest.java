package io.cengyan.query;

import io.cengyan.catalog.StatsRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StatsCacheTest {
    @Test
    void 数量只在手动刷新时重新扫描() {
        AtomicLong now = new AtomicLong(1_000_000L);
        StatsCache cache = new StatsCache(StatsCache.FRESH_MILLIS, now::get);
        AtomicInteger loads = new AtomicInteger();
        assertEquals(null, cache.cachedCounts(1));

        StatsCache.Snapshot<List<StatsRow>> first = cache.refreshCounts(1, () -> {
            loads.incrementAndGet();
            return List.of(new StatsRow("NODE", "Company", 3));
        });
        now.addAndGet(StatsCache.FRESH_MILLIS + 1);
        assertEquals(first, cache.cachedCounts(1));
        assertEquals(1, loads.get());

        assertEquals(9L, cache.refreshCounts(1, () -> {
            loads.incrementAndGet();
            return List.of(new StatsRow("NODE", "Company", 9));
        }).value().get(0).count());
        assertEquals(2, loads.get());
        assertEquals(9L, cache.cachedCounts(1).value().get(0).count());
        assertEquals(List.of(), cache.refreshCounts(2, () -> {
            loads.incrementAndGet();
            return List.of();
        }).value());
        assertEquals(null, cache.cachedCounts(3));
    }

    @Test
    void 三十分钟内大小不再扫描() {
        AtomicLong now = new AtomicLong(1_000_000L);
        StatsCache cache = new StatsCache(StatsCache.FRESH_MILLIS, now::get);
        AtomicInteger sizeLoads = new AtomicInteger();

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
        assertEquals(200L, cache.storage(() -> {
            sizeLoads.incrementAndGet();
            return 200L;
        }).value());
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
