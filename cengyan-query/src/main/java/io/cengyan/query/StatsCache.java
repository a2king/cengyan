package io.cengyan.query;

import io.cengyan.catalog.StatsRow;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** 数量统计一直保留上次结果，直到手动刷新。存储占用仍按生鲜期复用。 */
final class StatsCache {
    static final long FRESH_MILLIS = 30L * 60 * 1000;

    private final long freshMillis;
    private final LongSupplier clock;
    private final ConcurrentHashMap<Integer, Snapshot<List<StatsRow>>> counts = new ConcurrentHashMap<>();
    private final Object countGate = new Object();
    private Snapshot<Long> storage;
    private final Object storageGate = new Object();

    StatsCache() {
        this(FRESH_MILLIS, System::currentTimeMillis);
    }

    StatsCache(long freshMillis, LongSupplier clock) {
        if (freshMillis <= 0) {
            throw new IllegalArgumentException("生鲜期必须为正");
        }
        if (clock == null) {
            throw new IllegalArgumentException("时钟不能为空");
        }
        this.freshMillis = freshMillis;
        this.clock = clock;
    }

    Snapshot<List<StatsRow>> cachedCounts(int graphId) {
        return counts.get(graphId);
    }

    Snapshot<List<StatsRow>> refreshCounts(int graphId, Supplier<List<StatsRow>> load) {
        if (load == null) {
            throw new IllegalArgumentException("加载不能为空");
        }
        synchronized (countGate) {
            Snapshot<List<StatsRow>> loaded = new Snapshot<>(List.copyOf(load.get()), clock.getAsLong());
            counts.put(graphId, loaded);
            return loaded;
        }
    }

    Snapshot<Long> storage(LongSupplier load) {
        Snapshot<Long> found = fresh(storage);
        if (found != null) {
            return found;
        }
        synchronized (storageGate) {
            found = fresh(storage);
            if (found != null) {
                return found;
            }
            long bytes = load.getAsLong();
            long collectedAt = clock.getAsLong();
            Snapshot<Long> loaded = new Snapshot<>(bytes, collectedAt);
            if (bytes >= 0) {
                storage = loaded;
            }
            return loaded;
        }
    }

    private <T> Snapshot<T> fresh(Snapshot<T> sample) {
        if (sample == null) {
            return null;
        }
        if (clock.getAsLong() - sample.collectedAt() >= freshMillis) {
            return null;
        }
        return sample;
    }

    record Snapshot<T>(T value, long collectedAt) {
    }
}
