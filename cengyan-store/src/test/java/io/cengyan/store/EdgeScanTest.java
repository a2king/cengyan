package io.cengyan.store;

import io.cengyan.CancelToken;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EdgeScanTest {
    @Test
    void 出边和入边都能按前缀扫到() throws Exception {
        try (Db db = new Db()) {
            db.store.insertEdge(1, 1, 0L, 10L, 20L);
            db.store.insertEdge(1, 1, 0L, 30L, 10L);
            try (ReadSession session = db.store.read()) {
                List<EdgeRef> outs = drain(session.scanEdges(1, 10L, 1, Direction.OUT, -1, 64, null));
                List<EdgeRef> ins = drain(session.scanEdges(1, 10L, 1, Direction.IN, -1, 64, null));
                assertEquals(List.of(new EdgeRef(1, 0L, 10L, 20L)), outs);
                assertEquals(List.of(new EdgeRef(1, 0L, 30L, 10L)), ins);
            }
        }
    }

    @Test
    void 只写出边或只写入边都失败() throws Exception {
        try (Db db = new Db()) {
            StoreBatch outOnly = new StoreBatch();
            outOnly.put(Cf.EDGE, KeyCodec.edgeKey(1, 0, 1L, 1, Direction.OUT.code(), 0L, 2L), new byte[0]);
            CengyanException out = assertThrows(CengyanException.class, () -> db.store.commitEdges(outOnly));
            assertEquals(ErrorCode.INTERNAL, out.code());

            StoreBatch inOnly = new StoreBatch();
            inOnly.put(Cf.EDGE, KeyCodec.edgeKey(1, 0, 2L, 1, Direction.IN.code(), 0L, 1L), new byte[0]);
            CengyanException in = assertThrows(CengyanException.class, () -> db.store.commitEdges(inOnly));
            assertEquals(ErrorCode.INTERNAL, in.code());

            try (ReadSession session = db.store.read()) {
                assertTrue(drain(session.scanEdges(1, 1L, 1, Direction.OUT, -1, 64, null)).isEmpty());
                assertTrue(drain(session.scanEdges(1, 2L, 1, Direction.IN, -1, 64, null)).isEmpty());
            }
        }
    }

    @Test
    void 自环两条键都在且属性列族为空() throws Exception {
        try (Db db = new Db()) {
            db.store.insertEdge(1, 1, 3L, 5L, 5L);
            try (ReadSession session = db.store.read()) {
                byte[] outKey = KeyCodec.edgeKey(1, 0, 5L, 1, Direction.OUT.code(), 3L, 5L);
                byte[] inKey = KeyCodec.edgeKey(1, 0, 5L, 1, Direction.IN.code(), 3L, 5L);
                assertEquals(0, session.get(Cf.EDGE, outKey).length);
                assertEquals(0, session.get(Cf.EDGE, inKey).length);
                assertNull(session.get(Cf.PROP, KeyCodec.propKey(1, 1, 3L, 5L, 5L)));
                assertEquals(1, drain(session.scanEdges(1, 5L, 1, Direction.OUT, -1, 64, null)).size());
                assertEquals(1, drain(session.scanEdges(1, 5L, 1, Direction.IN, -1, 64, null)).size());
            }
        }
    }

    @Test
    void 达到语句上限就停止且超过每点上限失败() throws Exception {
        try (Db db = new Db()) {
            insertStar(db.store, 1L, 100);
            try (ReadSession session = db.store.read();
                 EdgeCursor cursor = session.scanEdges(1, 1L, 1, Direction.OUT, 10, 64, null)) {
                assertEquals(10, drain(cursor).size());
                assertEquals(10, cursor.examined());
            }
        }
    }

    @Test
    void 截断前已经返回的边不超过上限() throws Exception {
        try (Db db = new Db()) {
            insertStar(db.store, 1L, 100);
            try (ReadSession session = db.store.read();
                 EdgeCursor cursor = session.scanEdges(1, 1L, 1, Direction.OUT, -1, 64, null)) {
                List<EdgeRef> got = new ArrayList<>();
                CengyanException ex = assertThrows(CengyanException.class, () -> {
                    while (cursor.hasNext()) {
                        got.add(cursor.next());
                    }
                });
                assertEquals(ErrorCode.TRUNCATED, ex.code());
                assertEquals(64, got.size());
            }
        }
    }

    @Test
    void 扫描中途取消后在64条内结束() throws Exception {
        try (Db db = new Db()) {
            insertStar(db.store, 1L, 200);
            FlagCancel cancel = new FlagCancel();
            try (ReadSession session = db.store.read();
                 EdgeCursor cursor = session.scanEdges(1, 1L, 1, Direction.OUT, -1, 1000, cancel)) {
                List<EdgeRef> got = new ArrayList<>();
                got.add(cursor.next());
                cancel.fire();
                CengyanException ex = assertThrows(CengyanException.class, () -> {
                    while (cursor.hasNext()) {
                        got.add(cursor.next());
                    }
                });
                assertEquals(ErrorCode.QUERY_CANCELED, ex.code());
                assertTrue(got.size() <= 64, "实际返回 " + got.size());
                assertTrue(got.size() < 200);
            }
        }
    }

    @Test
    void 快照看不见之后提交的边且重启后两边都在() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-reopen-");
        GraphStore store = GraphStore.open(dir);
        try {
            store.insertEdge(1, 1, 0L, 1L, 2L);
            try (ReadSession session = store.read()) {
                store.insertEdge(1, 1, 0L, 1L, 3L);
                List<EdgeRef> edges = drain(session.scanEdges(1, 1L, 1, Direction.OUT, -1, 64, null));
                assertEquals(List.of(new EdgeRef(1, 0L, 1L, 2L)), edges);
            }
        } finally {
            store.close();
        }
        try (GraphStore reopened = GraphStore.open(dir);
             ReadSession session = reopened.read()) {
            List<EdgeRef> edges = drain(session.scanEdges(1, 1L, 1, Direction.OUT, -1, 64, null));
            assertEquals(2, edges.size());
            assertNotNull(session.get(Cf.EDGE, KeyCodec.edgeKey(1, 0, 3L, 1, Direction.IN.code(), 0L, 1L)));
        } finally {
            delete(dir);
        }
    }

    @Test
    void 同一对点的rank按大端有序() throws Exception {
        try (Db db = new Db()) {
            db.store.insertEdge(1, 1, 5L, 1L, 2L);
            db.store.insertEdge(1, 1, 0L, 1L, 2L);
            try (ReadSession session = db.store.read()) {
                List<EdgeRef> edges = drain(session.scanEdges(1, 1L, 1, Direction.OUT, -1, 64, null));
                assertEquals(0L, edges.get(0).rank());
                assertEquals(5L, edges.get(1).rank());
            }
        }
    }

    private static void insertStar(GraphStore store, long center, int degree) {
        StoreBatch batch = new StoreBatch();
        for (int i = 0; i < degree; i++) {
            long dst = 1000L + i;
            batch.put(Cf.EDGE, KeyCodec.edgeKey(1, 0, center, 1, Direction.OUT.code(), 0L, dst), new byte[0]);
            batch.put(Cf.EDGE, KeyCodec.edgeKey(1, 0, dst, 1, Direction.IN.code(), 0L, center), new byte[0]);
        }
        store.commitEdges(batch);
    }

    private static List<EdgeRef> drain(EdgeCursor cursor) {
        List<EdgeRef> edges = new ArrayList<>();
        while (cursor.hasNext()) {
            edges.add(cursor.next());
        }
        return edges;
    }

    private static final class FlagCancel implements CancelToken {
        private volatile boolean flag;

        @Override
        public boolean cancelled() {
            return flag;
        }

        @Override
        public void onCancel(Runnable hook) {
            // 不登记关闭钩子，用来卡住「只靠每 64 条检查」这条路径。
        }

        void fire() {
            flag = true;
        }
    }

    private static final class Db implements AutoCloseable {
        final Path dir;
        final GraphStore store;

        Db() throws IOException {
            dir = Files.createTempDirectory("cengyan-edge-");
            store = GraphStore.open(dir);
        }

        @Override
        public void close() throws IOException {
            store.close();
            delete(dir);
        }
    }

    private static void delete(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        IOException last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                if (!Files.exists(root)) {
                    return;
                }
                try (Stream<Path> walk = Files.walk(root)) {
                    for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(path);
                    }
                }
                return;
            } catch (IOException ex) {
                last = ex;
                System.gc();
                try {
                    Thread.sleep(50L * (attempt + 1));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw ex;
                }
            }
        }
        if (last != null) {
            throw last;
        }
    }
}
