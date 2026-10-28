package io.cengyan.catalog;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.ReadSession;
import io.cengyan.store.StoreOptions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase9Test {
    @Test
    void 同步期间拒绝写入提升后新节点能读到同一条边() throws Exception {
        try (Db db = new Db()) {
            int graphId = db.prepare();
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            db.membership.afterReadonly(() -> {
                entered.countDown();
                await(release);
            });
            AtomicReference<Throwable> failed = new AtomicReference<>();
            Thread copier = new Thread(() -> {
                try {
                    db.membership.addReplica("g", 3);
                } catch (Throwable ex) {
                    failed.set(ex);
                }
            }, "cengyan-replica");
            copier.start();
            assertTrue(entered.await(30, TimeUnit.SECONDS));
            CengyanException denied = assertThrows(CengyanException.class,
                    () -> db.insert(graphId, 3, 4));
            assertEquals(ErrorCode.SPACE_READONLY, denied.code());
            assertEquals(0.5d, db.catalog.readRel(graphId, "holder", 0, 1, 2).get("ratio"));
            release.countDown();
            copier.join(60_000);
            assertNull(failed.get());
            assertEquals(3, db.membership.voterCount("g"));
            byte[] edge = outKey(graphId, db.holder, 1, 2);
            try (ReadSession second = db.membership.store(2).read();
                 ReadSession third = db.membership.store(3).read()) {
                assertNotNull(second.get(Cf.EDGE, edge));
                assertNotNull(third.get(Cf.EDGE, edge));
            }
            db.membership.store(3).close();
            db.insert(graphId, 5, 6);
            try (ReadSession second = db.membership.store(2).read()) {
                assertNotNull(second.get(Cf.EDGE, outKey(graphId, db.holder, 5, 6)));
            }
        }
    }

    @Test
    void 提升前退出投票副本数仍是一() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-phase9-");
        GraphStore writing = GraphStore.open(dir);
        int graphId;
        try {
            Catalog catalog = Catalog.load(writing);
            Membership membership = Membership.open(catalog, writing, "127.0.0.1:1", StoreOptions.development());
            graphId = catalog.createGraph("g");
            catalog.createRelType(graphId, "holder", java.util.List.of(new PropDef("ratio", ValueType.DOUBLE, true)));
            membership.ensureVotes(graphId);
            catalog.insertRel(graphId, "holder", 0, VertexWrite.onlyId(1), VertexWrite.onlyId(2),
                    Map.of("ratio", 0.5d), false);
            membership.crashBeforePromote();
            CengyanException exited = assertThrows(CengyanException.class, () -> membership.addReplica("g", 3));
            assertTrue(exited.getMessage().contains("提升前"));
            membership.close();
        } finally {
            writing.close();
        }
        GraphStore reading = GraphStore.open(dir);
        try {
            Catalog catalog = Catalog.load(reading);
            Membership membership = Membership.open(catalog, reading, "127.0.0.1:1", StoreOptions.development());
            assertEquals(1, membership.voterCount("g"));
            assertEquals(0.5d, catalog.readRel(graphId, "holder", 0, 1, 2).get("ratio"));
            membership.close();
        } finally {
            reading.close();
            delete(dir);
        }
    }

    @Test
    void 搬一个分片时另一个分片可以插入() throws Exception {
        try (Db db = new Db()) {
            int graphId = db.prepare();
            db.catalog.setPartCount(graphId, 2);
            db.membership.ensureVotes(graphId);
            AtomicReference<CengyanException> denied = new AtomicReference<>();
            db.membership.duringMove(() -> {
                db.insert(graphId, 1, 3);
                try {
                    db.insert(graphId, 0, 2);
                } catch (CengyanException ex) {
                    denied.set(ex);
                }
            });
            db.membership.addStorage("127.0.0.1:9");
            assertEquals(ErrorCode.PART_READONLY, denied.get().code());
            assertEquals(0.5d, db.catalog.readRel(graphId, "holder", 0, 1, 3).get("ratio"));
        }
    }

    @Test
    void 唯一投票成员不能下线() throws Exception {
        try (Db db = new Db()) {
            int graphId = db.prepare();
            db.membership.ensureVotes(graphId);
            CengyanException denied = assertThrows(CengyanException.class, () -> db.membership.drainStorage(1));
            assertEquals(ErrorCode.NODE_NOT_DRAINED, denied.code());
        }
    }

    @Test
    void 新会话落到查询更少的计算节点() throws Exception {
        try (Db db = new Db();
             ServerSocket busy = new ServerSocket(0);
             ServerSocket idle = new ServerSocket(0)) {
            int loaded = db.membership.addCompute("127.0.0.1:" + busy.getLocalPort());
            int free = db.membership.addCompute("127.0.0.1:" + idle.getLocalPort());
            db.membership.report(loaded, 2, 0, 0.2d);
            db.membership.report(free, 0, 0, 0.9d);
            int first = db.membership.choose(0);
            db.membership.notePin(first);
            assertEquals(free, first);
            db.membership.report(free, 8, 0, 0);
            assertEquals(free, db.membership.choose(first));
        }
    }

    @Test
    void 第二步之前退出重启后入边被补上() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-phase9-");
        GraphStore writing = GraphStore.open(dir);
        int graphId;
        int holder;
        try {
            Catalog catalog = Catalog.load(writing);
            graphId = catalog.createGraph("g");
            holder = catalog.createRelType(
                    graphId, "holder", java.util.List.of(new PropDef("ratio", ValueType.DOUBLE, true))).id();
            catalog.setPartCount(graphId, 2);
            writing.injectBeforeWrite(() -> writing.injectBeforeWrite(() -> {
                throw new ProcessExit();
            }));
            assertThrows(ProcessExit.class, () -> catalog.insertRel(
                    graphId, "holder", 0, VertexWrite.onlyId(1), VertexWrite.onlyId(2),
                    Map.of("ratio", 0.5d), false));
        } finally {
            writing.close();
        }
        GraphStore reading = GraphStore.open(dir);
        try {
            Catalog catalog = Catalog.load(reading);
            byte[] inKey = KeyCodec.edgeKey(
                    graphId, KeyCodec.partOf(2, 2), 2, holder, Direction.IN.code(), 0, 1);
            byte[] txnKey = KeyCodec.txnKey(graphId, KeyCodec.partOf(1, 2), holder, 0, 1, 2);
            try (ReadSession session = reading.read()) {
                assertNotNull(session.get(Cf.EDGE, inKey));
                assertNull(session.get(Cf.TXN, txnKey));
            }
            assertEquals(0.5d, catalog.readRel(graphId, "holder", 0, 1, 2).get("ratio"));
        } finally {
            reading.close();
            delete(dir);
        }
    }

    @Test
    void 第二步失败后两侧都没有() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-phase9-");
        GraphStore writing = GraphStore.open(dir);
        int graphId;
        int holder;
        try {
            Catalog catalog = Catalog.load(writing);
            graphId = catalog.createGraph("g");
            holder = catalog.createRelType(
                    graphId, "holder", java.util.List.of(new PropDef("ratio", ValueType.DOUBLE, true))).id();
            catalog.setPartCount(graphId, 2);
            writing.injectBeforeWrite(() -> writing.injectBeforeWrite(() -> {
                throw new RuntimeException("入边失败");
            }));
            CengyanException failed = assertThrows(CengyanException.class, () -> catalog.insertRel(
                    graphId, "holder", 0, VertexWrite.onlyId(1), VertexWrite.onlyId(2),
                    Map.of("ratio", 0.5d), false));
            assertTrue(failed.getMessage().contains("入边写入失败"));
        } finally {
            writing.close();
        }
        GraphStore reading = GraphStore.open(dir);
        try {
            Catalog catalog = Catalog.load(reading);
            byte[] outKey = KeyCodec.edgeKey(
                    graphId, KeyCodec.partOf(1, 2), 1, holder, Direction.OUT.code(), 0, 2);
            byte[] inKey = KeyCodec.edgeKey(
                    graphId, KeyCodec.partOf(2, 2), 2, holder, Direction.IN.code(), 0, 1);
            byte[] txnKey = KeyCodec.txnKey(graphId, KeyCodec.partOf(1, 2), holder, 0, 1, 2);
            try (ReadSession session = reading.read()) {
                assertNull(session.get(Cf.EDGE, outKey));
                assertNull(session.get(Cf.EDGE, inKey));
                assertNull(session.get(Cf.PROP, KeyCodec.propKey(graphId, holder, 0, 1, 2)));
                assertNull(session.get(Cf.TXN, txnKey));
            }
            assertNull(catalog.readRel(graphId, "holder", 0, 1, 2));
        } finally {
            reading.close();
            delete(dir);
        }
    }

    private static byte[] outKey(int graphId, int edgeTypeId, long src, long dst) {
        return KeyCodec.edgeKey(graphId, 0, src, edgeTypeId, Direction.OUT.code(), 0, dst);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                throw new IllegalStateException("等待超时");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
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

    private static final class Db implements AutoCloseable {
        private final Path dir;
        private final GraphStore store;
        private final Catalog catalog;
        private final Membership membership;
        private int holder;

        private Db() throws IOException {
            dir = Files.createTempDirectory("cengyan-phase9-");
            store = GraphStore.open(dir);
            catalog = Catalog.load(store);
            membership = Membership.open(catalog, store, "127.0.0.1:1", StoreOptions.development());
        }

        private int prepare() {
            int graphId = catalog.createGraph("g");
            holder = catalog.createRelType(
                    graphId, "holder", java.util.List.of(new PropDef("ratio", ValueType.DOUBLE, true))).id();
            insert(graphId, 1, 2);
            return graphId;
        }

        private void insert(int graphId, long src, long dst) {
            catalog.insertRel(graphId, "holder", 0, VertexWrite.onlyId(src), VertexWrite.onlyId(dst),
                    Map.of("ratio", 0.5d), false);
        }

        @Override
        public void close() throws IOException {
            membership.close();
            store.close();
            delete(dir);
        }
    }
}
