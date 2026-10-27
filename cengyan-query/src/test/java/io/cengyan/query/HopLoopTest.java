package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import io.cengyan.store.StoreBatch;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HopLoopTest {
    @Test
    void 每跳40个新点走完10跳() throws Exception {
        try (Db db = new Db()) {
            writeLayers(db.store, 10, 40);
            try (ReadSession session = db.store.read()) {
                Set<Long> frontier = HopLoop.expand(
                        session, 1, 1, Direction.OUT, 0L, 10, 10, 64, 64, null);
                assertEquals(40, frontier.size());
                assertTrue(frontier.contains(10_000L));
                assertTrue(frontier.contains(10_000L + 39));
            }
        }
    }

    @Test
    void 下一跳超过点数上限时在第一跳失败() throws Exception {
        try (Db db = new Db()) {
            writeWide(db.store, 70, 70);
            try (ReadSession session = db.store.read()) {
                long start = System.nanoTime();
                CengyanException ex = assertThrows(CengyanException.class, () -> HopLoop.expand(
                        session, 1, 1, Direction.OUT, 0L, 10, 10, 200, 64, null));
                long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
                assertEquals(ErrorCode.FRONTIER_EXCEEDED, ex.code());
                assertTrue(ex.getMessage().contains("第 1 跳"), ex.getMessage());
                assertTrue(elapsedMs < 1000, "耗时 " + elapsedMs + "ms");
            }
        }
    }

    @Test
    void 单点边数超过上限时不会把图扫完() throws Exception {
        try (Db db = new Db()) {
            writeStar(db.store, 200);
            try (ReadSession session = db.store.read()) {
                CengyanException ex = assertThrows(CengyanException.class, () -> HopLoop.expand(
                        session, 1, 1, Direction.OUT, 0L, 10, 10, 64, 64, null));
                assertEquals(ErrorCode.TRUNCATED, ex.code());
                assertTrue(ex.getMessage().contains("第 1 跳"), ex.getMessage());
                assertTrue(ex.getMessage().contains("65"), ex.getMessage());
            }
        }
    }

    @Test
    void 跳数超过上限直接拒绝() throws Exception {
        try (Db db = new Db()) {
            db.store.insertEdge(1, 1, 0L, 1L, 2L);
            try (ReadSession session = db.store.read()) {
                CengyanException ex = assertThrows(CengyanException.class, () -> HopLoop.expand(
                        session, 1, 1, Direction.OUT, 1L, 11, 10, 64, 64, null));
                assertEquals(ErrorCode.SYNTAX, ex.code());
            }
        }
    }

    /** 第 layer 层第 index 个点。层号从 1 开始，起点单独用 0。 */
    static long layerId(int layer, int index) {
        return layer * 1000L + index;
    }

    static void writeLayers(GraphStore store, int hops, int width) {
        StoreBatch batch = new StoreBatch();
        for (int index = 0; index < width; index++) {
            put(batch, 0L, layerId(1, index));
        }
        for (int layer = 1; layer < hops; layer++) {
            for (int index = 0; index < width; index++) {
                put(batch, layerId(layer, index), layerId(layer + 1, index));
            }
        }
        store.commitEdges(batch);
    }

    static void writeWide(GraphStore store, int fanout, int secondFanout) {
        StoreBatch batch = new StoreBatch();
        int pending = 0;
        for (int i = 0; i < fanout; i++) {
            long mid = layerId(1, i);
            put(batch, 0L, mid);
            pending++;
            for (int j = 0; j < secondFanout; j++) {
                put(batch, mid, 100_000L + (long) i * secondFanout + j);
                pending++;
                if (pending >= 2000) {
                    store.commitEdges(batch);
                    batch = new StoreBatch();
                    pending = 0;
                }
            }
        }
        if (!batch.isEmpty()) {
            store.commitEdges(batch);
        }
    }

    static void writeStar(GraphStore store, int degree) {
        StoreBatch batch = new StoreBatch();
        for (int i = 0; i < degree; i++) {
            put(batch, 0L, 1000L + i);
        }
        store.commitEdges(batch);
    }

    private static void put(StoreBatch batch, long src, long dst) {
        batch.put(Cf.EDGE, KeyCodec.edgeKey(1, 0, src, 1, Direction.OUT.code(), 0L, dst), new byte[0]);
        batch.put(Cf.EDGE, KeyCodec.edgeKey(1, 0, dst, 1, Direction.IN.code(), 0L, src), new byte[0]);
    }

    static final class Db implements AutoCloseable {
        final Path dir;
        final GraphStore store;

        Db() throws Exception {
            this(io.cengyan.store.StoreOptions.development());
        }

        Db(io.cengyan.store.StoreOptions options) throws Exception {
            dir = Files.createTempDirectory("cengyan-hop-");
            store = GraphStore.open(dir, options);
        }

        @Override
        public void close() throws Exception {
            store.close();
            if (!Files.exists(dir)) {
                return;
            }
            for (int attempt = 0; attempt < 10; attempt++) {
                try {
                    try (Stream<Path> walk = Files.walk(dir)) {
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
}
