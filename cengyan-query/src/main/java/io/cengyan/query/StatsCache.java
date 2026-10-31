package io.cengyan.query;

import io.cengyan.catalog.StatsRow;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/** 数量和存储占用的生鲜结果。过期前重复读取不再扫描。 */
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

    Snapshot<List<StatsRow>> counts(int graphId, Supplier<List<StatsRow>> load) {
        Snapshot<List<StatsRow>> found = fresh(counts.get(graphId));
        if (found != null) {
            return found;
        }
        synchronized (countGate) {
            found = fresh(counts.get(graphId));
            if (found != null) {
                return found;
            }
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
