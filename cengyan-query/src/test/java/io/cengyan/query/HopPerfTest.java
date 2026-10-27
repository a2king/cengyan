package io.cengyan.query;

import io.cengyan.store.Direction;
import io.cengyan.store.PerfReport;
import io.cengyan.store.ReadSession;
import io.cengyan.store.StoreOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("perf")
class HopPerfTest {
    private static final long BLOCK_CACHE = 256L * 1024 * 1024;
    private static final long LIMIT_NANOS = 10_000_000L;

    @Test
    void 每跳40个点的10跳p99低于10毫秒() throws Exception {
        StoreOptions options = new StoreOptions(BLOCK_CACHE, 2, 64L * 1024 * 1024);
        try (HopLoopTest.Db db = new HopLoopTest.Db(options)) {
            HopLoopTest.writeLayers(db.store, 10, 40);
            try (ReadSession session = db.store.read()) {
                for (int i = 0; i < 30; i++) {
                    assertEquals(40, expand(session).size());
                }
                long[] nanos = new long[80];
                for (int i = 0; i < nanos.length; i++) {
                    long start = System.nanoTime();
                    Set<Long> frontier = expand(session);
                    nanos[i] = System.nanoTime() - start;
                    assertEquals(40, frontier.size());
                }
                long p50 = PerfReport.percentile(nanos, 0.50);
                long p99 = PerfReport.percentile(nanos, 0.99);
                PerfReport.append(
                        "10 跳，每跳 40 个新点",
                        "1 个起点，10 层，每层 40 点",
                        true,
                        BLOCK_CACHE,
                        p50,
                        p99);
                assertTrue(p99 < LIMIT_NANOS, "10 跳 p99=" + p99 / 1_000_000.0 + "ms");
            }
        }
    }

    private static Set<Long> expand(ReadSession session) {
        return HopLoop.expand(session, 1, 1, Direction.OUT, 0L, 10, 10, 64, 64, null);
    }
}
