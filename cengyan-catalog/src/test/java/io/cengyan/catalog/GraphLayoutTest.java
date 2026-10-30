package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Direction;
import io.cengyan.store.EdgeCursor;
import io.cengyan.store.GraphPaths;
import io.cengyan.store.GraphStore;
import io.cengyan.store.ReadSession;
import io.cengyan.store.ShardedGraphStore;
import io.cengyan.store.StoreOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphLayoutTest {
    @Test
    void 不同类型的写入锁互不阻塞() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-stripes");
        try (ShardedGraphStore store = ShardedGraphStore.open(dir, StoreOptions.development())) {
            CountDownLatch entered = new CountDownLatch(2);
            CountDownLatch release = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> person = pool.submit(() -> hold(store, "node:1:Person", entered, release));
                Future<?> company = pool.submit(() -> hold(store, "node:1:Company", entered, release));
                assertTrue(entered.await(5, TimeUnit.SECONDS), "不同类型应能同时进入写入");
                release.countDown();
                person.get(5, TimeUnit.SECONDS);
                company.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                pool.shutdownNow();
            }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void 图数据按类型和分片分开放() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-layout");
        final int graphId;
        try {
        try (ShardedGraphStore store = ShardedGraphStore.open(dir, StoreOptions.development())) {
            Catalog catalog = Catalog.load(store);
            graphId = catalog.createGraph("company", 2, null);
            catalog.createNodeType(graphId, "Person", List.of(new PropDef("name", ValueType.STRING, false)));
            catalog.createNodeType(graphId, "Company", List.of(new PropDef("name", ValueType.STRING, false)));
            catalog.createRelType(graphId, "knows", List.of(new PropDef("since", ValueType.INT64, true)));
            catalog.createIndex(graphId, false, "person_name", true, "Person", List.of("name"));
            CengyanException reserved = assertThrows(CengyanException.class, () -> catalog.createGraph("cache"));
            assertEquals(ErrorCode.SYNTAX, reserved.code());

            ExecutorService pool = Executors.newFixedThreadPool(2);
            try {
                Future<?> person = pool.submit(() -> catalog.bulkInsertVertices(
                        graphId, "Person", List.of(new Catalog.BulkVertex(2L, Map.of("name", "ada"))), false));
                Future<?> company = pool.submit(() -> catalog.bulkInsertVertices(
                        graphId, "Company", List.of(new Catalog.BulkVertex(2L, Map.of("name", "acme"))), false));
                person.get(30, TimeUnit.SECONDS);
                company.get(30, TimeUnit.SECONDS);
            } finally {
                pool.shutdownNow();
            }
            catalog.insertRel(
                    graphId,
                    "knows",
                    0L,
                    new VertexWrite("Person", 2L, Map.of("name", "ada")),
                    new VertexWrite("Company", 2L, Map.of("name", "acme")),
                    Map.of("since", 1L),
                    false);

            assertDb(GraphPaths.node(dir, "company", "Person", 0));
            assertDb(GraphPaths.node(dir, "company", "Company", 0));
            assertDb(GraphPaths.edge(dir, "company", "knows", 0));
            assertDb(GraphPaths.index(dir, "company", "Person-name", 0));
            assertDb(GraphPaths.meta(dir, "company"));
            assertDb(GraphPaths.system(dir));
            assertFalse(Files.exists(dir.resolve("CURRENT")));
            assertFalse(Files.exists(GraphPaths.index(dir, "company", "person_name", 0)));
            assertWal(GraphPaths.node(dir, "company", "Person", 0));
            assertEquals("ada", catalog.readVertex(graphId, "Person", 2L).get("name"));
            assertEquals("acme", catalog.readVertex(graphId, "Company", 2L).get("name"));
            catalog.rebuildIndex(graphId, "person_name", null);
            store.flush();
            assertEquals(1L, catalog.readRel(graphId, "knows", 0L, 2L, 2L).get("since"));
            assertFilesStayInOwnDb(dir);
            try (ReadSession session = store.read();
                 EdgeCursor cursor = session.scanEdges(
                         graphId, 2L, catalog.relType(graphId, "knows").id(), Direction.OUT, -1, 16, null)) {
                assertTrue(cursor.hasNext());
            }
        }
        try (ShardedGraphStore store = ShardedGraphStore.open(dir, StoreOptions.development())) {
            Catalog catalog = Catalog.load(store);
            assertEquals("ada", catalog.readVertex(graphId, "Person", 2L).get("name"));
            assertEquals("acme", catalog.readVertex(graphId, "Company", 2L).get("name"));
            assertEquals(1L, catalog.readRel(graphId, "knows", 0L, 2L, 2L).get("since"));
        }
        } finally {
            deleteTree(dir);
        }
    }

    @Test
    void 旧库启动时拆到图目录() throws Exception {
        Path root = Files.createTempDirectory("cengyan-migrate");
        Path legacy = root.resolve("data");
        int graphId;
        try (GraphStore store = GraphStore.open(legacy, StoreOptions.development())) {
            Catalog catalog = Catalog.load(store);
            graphId = catalog.createGraph("company");
            catalog.createNodeType(graphId, "Company", List.of(new PropDef("name", ValueType.STRING, false)));
            assertTrue(catalog.insertVertex(graphId, "Company", 7L, Map.of("name", "acme"), false));
        }
        try (ShardedGraphStore store = ShardedGraphStore.open(root, StoreOptions.development())) {
            LayoutMigration.migrate(root, store);
            Catalog catalog = Catalog.load(store);
            assertEquals("acme", catalog.readVertex(graphId, "Company", 7L).get("name"));
            assertDb(GraphPaths.node(root, "company", "Company", 0));
            assertDb(GraphPaths.meta(root, "company"));
            assertDb(GraphPaths.system(root));
            assertFalse(Files.exists(legacy.resolve("CURRENT")));
        } finally {
            deleteTree(root);
        }
    }

    private static Object hold(ShardedGraphStore store, String stripe, CountDownLatch entered, CountDownLatch release) {
        return store.withStripes(List.of(stripe), () -> {
            entered.countDown();
            try {
                if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("等待释放超时");
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(ex);
            }
            return null;
        });
    }

    private static void assertDb(Path dir) {
        assertTrue(Files.isRegularFile(dir.resolve("CURRENT")), () -> "缺少数据库：" + dir);
    }

    private static void assertFilesStayInOwnDb(Path root) throws IOException {
        try (Stream<Path> files = Files.walk(root)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                assertTrue(Files.isRegularFile(file.getParent().resolve("CURRENT")),
                        () -> "文件跑到了所属库外面：" + file);
            }
        }
    }

    private static void assertWal(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            assertTrue(files.anyMatch(file -> file.getFileName().toString().endsWith(".log")),
                    () -> "缺少未刷盘日志：" + dir);
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
