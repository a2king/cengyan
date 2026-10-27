package io.cengyan.store;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("perf")
class EdgePerfTest {
    private static final int VERTICES = 10_000;
    private static final int DEGREE = 40;
    private static final long BLOCK_CACHE = 256L * 1024 * 1024;
    private static final long LIMIT_NANOS = 10_000_000L;

    @Test
    void 热缓存读取40条出边和入边的p99低于10毫秒() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-perf-");
        StoreOptions options = new StoreOptions(BLOCK_CACHE, 2, 64L * 1024 * 1024);
        try (GraphStore store = GraphStore.open(dir, options)) {
            StoreBatch batch = new StoreBatch();
            int pending = 0;
            for (int src = 0; src < VERTICES; src++) {
                for (int step = 1; step <= DEGREE; step++) {
                    long dst = (src + step) % VERTICES;
                    batch.put(Cf.EDGE, KeyCodec.edgeKey(1, 0, src, 1, Direction.OUT.code(), 0L, dst), new byte[0]);
                    batch.put(Cf.EDGE, KeyCodec.edgeKey(1, 0, dst, 1, Direction.IN.code(), 0L, src), new byte[0]);
                    pending++;
                    if (pending == 2000) {
                        store.commitEdges(batch);
                        batch = new StoreBatch();
                        pending = 0;
                    }
                }
            }
            if (!batch.isEmpty()) {
                store.commitEdges(batch);
            }
            try (ReadSession session = store.read()) {
                long[] outNanos = sample(session, Direction.OUT);
                long[] inNanos = sample(session, Direction.IN);
                long outP50 = PerfReport.percentile(outNanos, 0.50);
                long outP99 = PerfReport.percentile(outNanos, 0.99);
                long inP50 = PerfReport.percentile(inNanos, 0.50);
                long inP99 = PerfReport.percentile(inNanos, 0.99);
                String volume = VERTICES + " 点，每点 " + DEGREE + " 条边";
                PerfReport.append("读取 40 条出边", volume, true, BLOCK_CACHE, outP50, outP99);
                PerfReport.append("读取 40 条入边", volume, true, BLOCK_CACHE, inP50, inP99);
                assertTrue(outP99 < LIMIT_NANOS, "出边 p99=" + outP99 / 1_000_000.0 + "ms");
                assertTrue(inP99 < LIMIT_NANOS, "入边 p99=" + inP99 / 1_000_000.0 + "ms");
            }
        } finally {
            delete(dir);
        }
    }

    private static long[] sample(ReadSession session, Direction direction) {
        for (int i = 0; i < 100; i++) {
            int count = countEdges(session, direction);
            assertEquals(DEGREE, count);
        }
        long[] nanos = new long[300];
        for (int i = 0; i < nanos.length; i++) {
            long start = System.nanoTime();
            int count = countEdges(session, direction);
            nanos[i] = System.nanoTime() - start;
            assertEquals(DEGREE, count);
        }
        return nanos;
    }

    private static int countEdges(ReadSession session, Direction direction) {
        int count = 0;
        try (EdgeCursor cursor = session.scanEdges(1, 0L, 1, direction, DEGREE, 64, null)) {
            while (cursor.hasNext()) {
                cursor.next();
                count++;
            }
        }
        return count;
    }

    private static void delete(Path root) throws Exception {
        if (!Files.exists(root)) {
            return;
        }
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                try (Stream<Path> walk = Files.walk(root)) {
                    for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(path);
                    }
                }
                return;
            } catch (Exception ex) {
                System.gc();
                Thread.sleep(50L * (attempt + 1));
            }
        }
    }
}
