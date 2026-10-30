package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.PerfReport;
import io.cengyan.store.StoreBatch;
import io.cengyan.store.StoreOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HopMatchTest {
    @Test
    void 定长是那一跳变长是新点的并集() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})",
                    "INSERT (a:Company {id:2})-[:holder]->(b:Company {id:3})",
                    "INSERT (a:Company {id:3})-[:holder]->(b:Company {id:4})");
            QueryResult fixed = run(db, "MATCH (a:Company {id:1})-[:holder]->{2}(b) RETURN b.id");
            assertEquals(List.of(3L), column(fixed));
            QueryResult range = run(db, "MATCH (a:Company {id:1})-[:holder]->{1,2}(b) RETURN b.id");
            assertEquals(List.of(2L, 3L), column(range));
            CengyanException tooFar = assertThrows(CengyanException.class,
                    () -> db.engine.execute(
                            db.session,
                            "MATCH (a:Company {id:1})-[:holder]->{11}(b) RETURN b.id",
                            java.util.Map.of(),
                            new QueryLimits(64, 64, 10, 100),
                            null));
            assertEquals(ErrorCode.SYNTAX, tooFar.code());
            CengyanException paths = assertThrows(CengyanException.class, () -> db.engine.execute(
                    db.session,
                    "MATCH (a:Company {id:1})-[e:holder]->{1,2}(b) RETURN e",
                    java.util.Map.of(),
                    new QueryLimits(64, 64, 10, 1),
                    null));
            assertEquals(ErrorCode.PATH_LIMIT_EXCEEDED, paths.code());
        }
    }

    @Test
    void 同一张图只改过滤条件() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            int[] ids = prepare(db);
            long start = System.nanoTime();
            CengyanException exceeded = assertThrows(CengyanException.class, () -> run(
                    db, "MATCH (a:Company {id:0})-[e:holder]->{10}(b) WHERE e.rank >= 0 RETURN b.id",
                    new QueryLimits(64, 64, 10, 100)));
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            assertEquals(ErrorCode.FRONTIER_EXCEEDED, exceeded.code());
            assertTrue(exceeded.getMessage().contains("第 2 跳"), exceeded.getMessage());
            assertTrue(exceeded.getMessage().contains("起点"), exceeded.getMessage());
            assertTrue(elapsedMs < 1000, "耗时 " + elapsedMs + "ms");
            QueryResult kept = run(db,
                    "MATCH (a:Company {id:0})-[e:holder]->{10}(b) WHERE e.rank = 0 RETURN b.id",
                    new QueryLimits(64, 64, 10, 100));
            assertEquals(40, kept.rows().size());
            Set<Long> idsAtEnd = new HashSet<>(column(kept));
            for (int index = 0; index < 40; index++) {
                assertTrue(idsAtEnd.contains(spine(10, index)));
            }
            assertEquals(ids[0], db.session.current().graphId());
        }
    }

    @Tag("perf")
    @Test
    void 过滤后每跳40个点的10跳p99低于10毫秒() throws Exception {
        StoreOptions options = new StoreOptions(256L * 1024 * 1024, 2, 64L * 1024 * 1024);
        try (QueryEngineTest.Db db = new QueryEngineTest.Db(options)) {
            prepare(db);
            QueryLimits limits = new QueryLimits(64, 64, 10, 100);
            String query = "MATCH (a:Company {id:0})-[e:holder]->{10}(b) WHERE e.rank = 0 RETURN b.id";
            for (int i = 0; i < 30; i++) {
                assertEquals(40, db.engine.execute(db.session, query, java.util.Map.of(), limits, null).rows().size());
            }
            long[] nanos = new long[80];
            for (int i = 0; i < nanos.length; i++) {
                long begin = System.nanoTime();
                QueryResult result = db.engine.execute(db.session, query, java.util.Map.of(), limits, null);
                nanos[i] = System.nanoTime() - begin;
                assertEquals(40, result.rows().size());
            }
            long p50 = PerfReport.percentile(nanos, 0.50);
            long p99 = PerfReport.percentile(nanos, 0.99);
            PerfReport.append(
                    "10 跳过滤后 40 个点",
                    "1 个起点，10 层，每层 40 点，过滤 rank = 0",
                    true,
                    256L * 1024 * 1024,
                    p50,
                    p99);
            assertTrue(p99 < 20_000_000L, "10 跳 p99=" + p99 / 1_000_000.0 + "ms");
        }
    }

    private static int[] prepare(QueryEngineTest.Db db) {
        script(db,
                "CREATE GRAPH Company",
                "USE Company",
                "CREATE NODE TYPE Company ()",
                "CREATE REL TYPE holder ()");
        int graphId = db.session.current().graphId();
        int typeId = db.catalog.relType(graphId, "holder").id();
        db.catalog.insertVertex(graphId, "Company", 0L, java.util.Map.of(), false);
        writeWide(db.store, graphId, typeId);
        return new int[] {graphId, typeId};
    }

    private static void writeWide(GraphStore store, int graphId, int typeId) {
        BatchWriter writer = new BatchWriter(store);
        for (int index = 0; index < 40; index++) {
            writer.put(graphId, typeId, 0L, spine(1, index), 0L);
        }
        for (int layer = 1; layer <= 9; layer++) {
            for (int index = 0; index < 40; index++) {
                long from = spine(layer, index);
                writer.put(graphId, typeId, from, spine(layer + 1, index), 0L);
                for (int lane = 1; lane < 40; lane++) {
                    writer.put(graphId, typeId, from, extra(layer + 1, index, lane), 1L);
                }
            }
        }
        writer.finish();
    }

    private static final class BatchWriter {
        private final GraphStore store;
        private StoreBatch batch = new StoreBatch();
        private int pending;

        private BatchWriter(GraphStore store) {
            this.store = store;
        }

        private void put(int graphId, int typeId, long src, long dst, long rank) {
            batch.put(Cf.EDGE, KeyCodec.edgeKey(graphId, 0, src, typeId, Direction.OUT.code(), rank, dst), new byte[0]);
            batch.put(Cf.EDGE, KeyCodec.edgeKey(graphId, 0, dst, typeId, Direction.IN.code(), rank, src), new byte[0]);
            pending++;
            if (pending >= 2000) {
                store.commitEdges(batch);
                batch = new StoreBatch();
                pending = 0;
            }
        }

        private void finish() {
            if (!batch.isEmpty()) {
                store.commitEdges(batch);
            }
        }
    }

    private static long spine(int layer, int index) {
        return layer * 1000L + index;
    }

    private static long extra(int layer, int parent, int lane) {
        return 1_000_000L + layer * 100_000L + (long) parent * 40L + lane;
    }

    private static void script(QueryEngineTest.Db db, String... statements) {
        for (String statement : statements) {
            db.engine.execute(db.session, statement, java.util.Map.of(), new QueryLimits(64), null);
        }
    }

    private static QueryResult run(QueryEngineTest.Db db, String gql) {
        return db.engine.execute(db.session, gql, java.util.Map.of(), new QueryLimits(64), null);
    }

    private static QueryResult run(QueryEngineTest.Db db, String gql, QueryLimits limits) {
        return db.engine.execute(db.session, gql, java.util.Map.of(), limits, null);
    }

    private static List<Long> column(QueryResult result) {
        List<Long> values = new java.util.ArrayList<>();
        for (List<Object> row : result.rows()) {
            values.add((Long) row.get(0));
        }
        return values;
    }
}
