package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.EdgeRef;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogTest {
    @Test
    void 重启后类型和数据都还在() throws Exception {
        try (Db db = new Db()) {
            int graphId = db.catalog.createGraph("company");
            db.catalog.createNodeType(graphId, "Company", List.of(new PropDef("name", ValueType.STRING, false)));
            RelType holder = db.catalog.createRelType(graphId, "holder",
                    List.of(new PropDef("ratio", ValueType.DOUBLE, false)));
            assertTrue(db.catalog.insertVertex(graphId, "Company", 1L, Map.of("name", "root"), false));
            assertTrue(db.catalog.insertRel(graphId, "holder", 0L,
                    VertexWrite.of("Company", 1L, Map.of("name", "root")),
                    VertexWrite.of("Company", 2L, Map.of("name", "child")),
                    Map.of("ratio", 0.5d), false));
            db.reopen();
            assertEquals("company", db.catalog.graph("company").canonical());
            assertEquals(1, db.catalog.nodeType(graphId, "Company").schemaVersion());
            assertEquals("child", db.catalog.readVertex(graphId, "Company", 2L).get("name"));
            assertEquals(0.5d, db.catalog.readRel(graphId, "holder", 0L, 1L, 2L).get("ratio"));
            try (ReadSession session = db.store.read()) {
                assertEquals(0, session.get(Cf.EDGE, edgeKey(graphId, holder.id(), Direction.OUT, 1L, 2L)).length);
                assertEquals(0, session.get(Cf.EDGE, edgeKey(graphId, holder.id(), Direction.IN, 2L, 1L)).length);
                assertTrue(session.get(Cf.PROP, KeyCodec.propKey(graphId, holder.id(), 0L, 1L, 2L)).length > 0);
            }
        }
    }

    @Test
    void 扫拓扑时不读属性() throws Exception {
        try (Db db = new Db()) {
            int graphId = db.catalog.createGraph("company");
            db.catalog.createNodeType(graphId, "Company", List.of(new PropDef("name", ValueType.STRING, true)));
            RelType holder = db.catalog.createRelType(graphId, "holder",
                    List.of(new PropDef("ratio", ValueType.DOUBLE, false)));
            for (int i = 0; i < 40; i++) {
                long dst = 100L + i;
                db.catalog.insertRel(graphId, "holder", 0L,
                        VertexWrite.onlyId(1L), VertexWrite.onlyId(dst), Map.of("ratio", 0.01d), false);
            }
            assertEquals(0L, db.store.propReads());
            try (ReadSession session = db.store.read();
                 EdgeCursor cursor = session.scanEdges(graphId, 1L, holder.id(), Direction.OUT, -1, 64, null)) {
                List<EdgeRef> edges = new ArrayList<>();
                while (cursor.hasNext()) {
                    edges.add(cursor.next());
                }
                assertEquals(40, edges.size());
            }
            assertEquals(0L, db.store.propReads());
            List<EdgeRef> refs = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                refs.add(new EdgeRef(holder.id(), 0L, 1L, 100L + i));
            }
            List<Map<String, Object>> props = db.catalog.multiGetProps(graphId, "holder", refs);
            assertEquals(40, props.size());
            assertEquals(0.01d, props.get(0).get("ratio"));
            assertEquals(40L, db.store.propReads());
        }
    }

    @Test
    void 提交前失败则三条键都不可见() throws Exception {
        try (Db db = new Db()) {
            int graphId = db.catalog.createGraph("company");
            db.catalog.createNodeType(graphId, "Company", List.of(new PropDef("name", ValueType.STRING, false)));
            RelType holder = db.catalog.createRelType(graphId, "holder",
                    List.of(new PropDef("ratio", ValueType.DOUBLE, false)));
            db.store.injectBeforeWrite(() -> {
                throw new CengyanException(ErrorCode.INTERNAL, "提交前失败");
            });
            CengyanException failed = assertThrows(CengyanException.class, () -> db.catalog.insertRel(
                    graphId, "holder", 0L,
                    VertexWrite.of("Company", 1L, Map.of("name", "root")),
                    VertexWrite.of("Company", 2L, Map.of("name", "child")),
                    Map.of("ratio", 0.25d), false));
            assertEquals(ErrorCode.INTERNAL, failed.code());
            assertTrue(failed.getMessage().contains("提交前失败"));
            try (ReadSession session = db.store.read()) {
                assertNull(session.get(Cf.EDGE, edgeKey(graphId, holder.id(), Direction.OUT, 1L, 2L)));
                assertNull(session.get(Cf.EDGE, edgeKey(graphId, holder.id(), Direction.IN, 2L, 1L)));
                assertNull(session.get(Cf.PROP, KeyCodec.propKey(graphId, holder.id(), 0L, 1L, 2L)));
                assertNull(session.get(Cf.VERTEX, KeyCodec.vertexKey(graphId, 0, 1L, 1)));
            }
            assertTrue(db.catalog.insertRel(graphId, "holder", 0L,
                    VertexWrite.onlyId(1L), VertexWrite.onlyId(2L), Map.of("ratio", 0.25d), false));
            db.reopen();
            try (ReadSession session = db.store.read()) {
                assertNotMissing(session.get(Cf.EDGE, edgeKey(graphId, holder.id(), Direction.OUT, 1L, 2L)));
                assertNotMissing(session.get(Cf.EDGE, edgeKey(graphId, holder.id(), Direction.IN, 2L, 1L)));
                assertNotMissing(session.get(Cf.PROP, KeyCodec.propKey(graphId, holder.id(), 0L, 1L, 2L)));
            }
            assertEquals(0.25d, db.catalog.readRel(graphId, "holder", 0L, 1L, 2L).get("ratio"));
        }
    }

    @Test
    void 别名指向图且不能当成图删除() throws Exception {
        try (Db db = new Db()) {
            int graphId = db.catalog.createGraph("company");
            db.catalog.createRelType(graphId, "holder", List.of(new PropDef("ratio", ValueType.DOUBLE, false)));
            db.catalog.insertRel(graphId, "holder", 0L,
                    VertexWrite.onlyId(1L), VertexWrite.onlyId(2L), Map.of("ratio", 0.4d), false);
            db.catalog.createAlias("corp", "company");
            GraphName alias = db.catalog.graph("corp");
            assertEquals(graphId, alias.graphId());
            assertEquals("company", alias.canonical());
            assertTrue(alias.alias());
            assertEquals(List.of(new AliasRow("corp", "company", graphId)), db.catalog.aliases());

            CengyanException dropByAlias = assertThrows(CengyanException.class, () -> db.catalog.dropGraph("corp"));
            assertEquals(ErrorCode.DROP_ALIAS_AS_GRAPH, dropByAlias.code());
            CengyanException reuse = assertThrows(CengyanException.class, () -> db.catalog.createGraph("corp"));
            assertEquals(ErrorCode.NAME_CONFLICT, reuse.code());
            assertEquals(0.4d, db.catalog.readRel(graphId, "holder", 0L, 1L, 2L).get("ratio"));

            CengyanException chained = assertThrows(CengyanException.class,
                    () -> db.catalog.createAlias("again", "corp"));
            assertEquals(ErrorCode.GRAPH_NOT_FOUND, chained.code());
            assertTrue(chained.getMessage().contains("别名"));

            db.catalog.dropAlias("corp");
            assertTrue(db.catalog.aliases().isEmpty());
            CengyanException gone = assertThrows(CengyanException.class, () -> db.catalog.graph("corp"));
            assertEquals(ErrorCode.GRAPH_NOT_FOUND, gone.code());
            assertEquals(0.4d, db.catalog.readRel(graphId, "holder", 0L, 1L, 2L).get("ratio"));

            CengyanException dropGraphAsAlias = assertThrows(CengyanException.class,
                    () -> db.catalog.dropAlias("company"));
            assertEquals(ErrorCode.NAME_CONFLICT, dropGraphAsAlias.code());

            db.catalog.dropGraph("company");
            assertTrue(db.catalog.graphs().isEmpty());
            CengyanException missing = assertThrows(CengyanException.class, () -> db.catalog.graph("company"));
            assertEquals(ErrorCode.GRAPH_NOT_FOUND, missing.code());
            try (ReadSession session = db.store.read()) {
                assertNull(session.get(Cf.EDGE, edgeKey(graphId, 1, Direction.OUT, 1L, 2L)));
                assertNull(session.get(Cf.PROP, KeyCodec.propKey(graphId, 1, 0L, 1L, 2L)));
                assertNull(session.get(Cf.CATALOG, KeyCodec.nameKey("company")));
                assertNull(session.get(Cf.CATALOG, KeyCodec.graphRecordKey(graphId)));
            }
        }
    }

    @Test
    void 两个会话抢同一个名字只有一个成功() throws Exception {
        try (Db db = new Db()) {
            ExecutorService pool = Executors.newFixedThreadPool(2);
            CountDownLatch start = new CountDownLatch(1);
            AtomicInteger created = new AtomicInteger();
            AtomicInteger conflict = new AtomicInteger();
            AtomicReference<Throwable> unexpected = new AtomicReference<>();
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                tasks.add(pool.submit(() -> {
                    start.await();
                    try {
                        db.catalog.createGraph("Race");
                        created.incrementAndGet();
                    } catch (CengyanException ex) {
                        if (ex.code() == ErrorCode.NAME_CONFLICT) {
                            conflict.incrementAndGet();
                        } else {
                            unexpected.set(ex);
                        }
                    } catch (Throwable ex) {
                        unexpected.set(ex);
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
            pool.shutdown();
            assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
            assertNull(unexpected.get());
            assertEquals(1, created.get());
            assertEquals(1, conflict.get());
            assertEquals(List.of("Race"), db.catalog.graphs());
        }
    }

    @Test
    void 只能在末尾增加可空列并且旧行读出空值() throws Exception {
        try (Db db = new Db()) {
            int graphId = db.catalog.createGraph("company");
            db.catalog.createNodeType(graphId, "Company", List.of(new PropDef("capital", ValueType.INT64, false)));
            db.catalog.insertVertex(graphId, "Company", 7L, Map.of("capital", 10L), false);
            CengyanException required = assertThrows(CengyanException.class, () -> db.catalog.addNodeProperty(
                    graphId, "Company", new PropDef("note", ValueType.STRING, false)));
            assertEquals(ErrorCode.TYPE_MISMATCH, required.code());
            assertTrue(required.getMessage().contains("可空"));
            NodeType updated = db.catalog.addNodeProperty(
                    graphId, "Company", new PropDef("note", ValueType.STRING, true));
            assertEquals(2, updated.schemaVersion());
            Map<String, Object> row = db.catalog.readVertex(graphId, "Company", 7L);
            assertEquals(10L, row.get("capital"));
            assertNull(row.get("note"));
            db.reopen();
            Map<String, Object> again = db.catalog.readVertex(graphId, "Company", 7L);
            assertEquals(10L, again.get("capital"));
            assertNull(again.get("note"));
            assertEquals(2, db.catalog.nodeType(graphId, "Company").schemaVersion());
        }
    }

    @Test
    void 名字规则和默认覆盖() throws Exception {
        try (Db db = new Db()) {
            CengyanException chinese = assertThrows(CengyanException.class, () -> db.catalog.createGraph("股东"));
            assertEquals(ErrorCode.SYNTAX, chinese.code());
            assertEquals(ErrorCode.SYNTAX, assertThrows(CengyanException.class,
                    () -> db.catalog.createGraph("1company")).code());
            assertEquals(ErrorCode.SYNTAX, assertThrows(CengyanException.class,
                    () -> db.catalog.createGraph("_company")).code());
            assertEquals(ErrorCode.SYNTAX, assertThrows(CengyanException.class,
                    () -> db.catalog.createGraph("a".repeat(129))).code());

            int lower = db.catalog.createGraph("graph");
            int upper = db.catalog.createGraph("Graph");
            assertTrue(lower != upper);
            int graphId = db.catalog.createGraph("company");
            CengyanException shared = assertThrows(CengyanException.class, () -> {
                db.catalog.createNodeType(graphId, "holder", List.of());
                db.catalog.createRelType(graphId, "holder", List.of());
            });
            assertEquals(ErrorCode.NAME_CONFLICT, shared.code());
            db.catalog.createRelType(graphId, "owns", List.of(new PropDef("ratio", ValueType.DOUBLE, false)));
            db.catalog.insertRel(graphId, "owns", 0L,
                    VertexWrite.onlyId(1L), VertexWrite.onlyId(2L), Map.of("ratio", 0.2d), false);
            assertFalse(db.catalog.insertRel(graphId, "owns", 0L,
                    VertexWrite.onlyId(1L), VertexWrite.onlyId(2L), Map.of("ratio", 0.9d), true));
            assertEquals(0.2d, db.catalog.readRel(graphId, "owns", 0L, 1L, 2L).get("ratio"));
            assertTrue(db.catalog.insertRel(graphId, "owns", 0L,
                    VertexWrite.onlyId(1L), VertexWrite.onlyId(2L), Map.of("ratio", 0.9d), false));
            assertEquals(0.9d, db.catalog.readRel(graphId, "owns", 0L, 1L, 2L).get("ratio"));
        }
    }

    private static byte[] edgeKey(int graphId, int edgeTypeId, Direction direction, long src, long dst) {
        long rank = 0L;
        if (direction == Direction.OUT) {
            return KeyCodec.edgeKey(graphId, 0, src, edgeTypeId, direction.code(), rank, dst);
        }
        return KeyCodec.edgeKey(graphId, 0, src, edgeTypeId, direction.code(), rank, dst);
    }

    private static void assertNotMissing(byte[] value) {
        assertTrue(value != null);
    }

    private static final class Db implements AutoCloseable {
        private final Path dir;
        private GraphStore store;
        private Catalog catalog;

        private Db() throws IOException {
            dir = Files.createTempDirectory("cengyan-catalog-");
            store = GraphStore.open(dir);
            catalog = Catalog.load(store);
        }

        private void reopen() {
            store.close();
            store = GraphStore.open(dir);
            catalog = Catalog.load(store);
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
