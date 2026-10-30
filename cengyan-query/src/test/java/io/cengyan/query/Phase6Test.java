package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.store.Cf;
import io.cengyan.store.Direction;
import io.cengyan.store.KeyCodec;
import io.cengyan.store.StoreBatch;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase6Test {
    @Test
    void 内存里的穿透持股按比例累加并且环不乘第二次() {
        long source = 1L;
        long alpha = 2L;
        long beta = 3L;
        long gamma = 4L;
        Map<Long, List<OwnershipExecutor.Edge>> graph = new HashMap<>();
        graph.put(source, List.of(new OwnershipExecutor.Edge(alpha, 0.5d), new OwnershipExecutor.Edge(beta, 0.5d)));
        graph.put(alpha, List.of(new OwnershipExecutor.Edge(gamma, 0.4d)));
        graph.put(beta, List.of(new OwnershipExecutor.Edge(gamma, 0.4d)));
        OwnershipExecutor.Result result = OwnershipExecutor.largest(graph, source, 10, null);
        assertEquals(0.5d, result.best().get(alpha), 1e-9);
        assertEquals(0.5d, result.best().get(beta), 1e-9);
        assertEquals(0.4d, result.best().get(gamma), 1e-9);
        assertEquals(alpha, result.vid());
        assertFalse(result.best().containsKey(source));

        Map<Long, List<OwnershipExecutor.Edge>> cycle = new HashMap<>();
        cycle.put(source, List.of(new OwnershipExecutor.Edge(alpha, 0.5d), new OwnershipExecutor.Edge(beta, 0.5d)));
        cycle.put(alpha, List.of(new OwnershipExecutor.Edge(alpha, 0.4d)));
        cycle.put(beta, List.of(new OwnershipExecutor.Edge(gamma, 0.4d)));
        OwnershipExecutor.Result cycled = OwnershipExecutor.largest(cycle, source, 10, null);
        assertEquals(0.5d, cycled.best().get(alpha), 1e-9);
        assertEquals(0.2d, cycled.best().get(gamma), 1e-9);
        assertEquals(alpha, cycled.vid());

    }

    @Test
    void 穿透持股走扫描器时和内存结果一致() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.5}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.5}]->(b:Company {id:3})",
                    "INSERT (a:Company {id:2})-[:holder {ratio:0.4}]->(b:Company {id:4})",
                    "INSERT (a:Company {id:3})-[:holder {ratio:0.4}]->(b:Company {id:4})");
            QueryEngine engine = zeroThresholds(db);
            QueryResult result = engine.execute(
                    db.session,
                    "CALL ownership.largest_holder({start: 1, rel: \"holder\", ratioProp: \"ratio\", hops: 10}) YIELD vid, ratio",
                    Map.of(),
                    new QueryLimits(64),
                    null);
            assertEquals(List.of("vid", "ratio"), result.columns());
            assertEquals(2L, result.rows().get(0).get(0));
            assertEquals(0.5d, ((Number) result.rows().get(0).get(1)).doubleValue(), 1e-9);
        }
    }

    @Test
    void 指向自己的环不会把比例再乘一遍() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.5}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.5}]->(b:Company {id:3})",
                    "INSERT (a:Company {id:2})-[:holder {ratio:0.4}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:3})-[:holder {ratio:0.4}]->(b:Company {id:4})");
            QueryResult result = zeroThresholds(db).execute(
                    db.session,
                    "CALL ownership.largest_holder({start: 1, rel: \"holder\", ratioProp: \"ratio\", hops: 10}) YIELD vid, ratio",
                    Map.of(),
                    new QueryLimits(64),
                    null);
            assertEquals(2L, result.rows().get(0).get(0));
            assertEquals(0.5d, ((Number) result.rows().get(0).get(1)).doubleValue(), 1e-9);
        }
    }

    @Test
    void 最短路从两端扩展超限就失败找不到就空() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})",
                    "INSERT (a:Company {id:2})-[:holder]->(b:Company {id:3})",
                    "INSERT (a:Company {id:3})-[:holder]->(b:Company {id:4})",
                    "INSERT (a:Company {id:4})-[:holder]->(b:Company {id:5})");
            QueryResult tooShort = run(db,
                    "CALL path.shortest({start: 1, end: 5, rel: \"holder\", hops: 1}) YIELD length, vids");
            assertEquals(List.of("length", "vids"), tooShort.columns());
            assertTrue(tooShort.rows().isEmpty());
            QueryResult path = db.engine.execute(
                    db.session,
                    "CALL path.shortest({start: $s, end: 5, rel: \"holder\", hops: 4}) YIELD vids, length",
                    Map.of("s", 1L),
                    new QueryLimits(64),
                    null);
            assertEquals(List.of(1L, 2L, 3L, 4L, 5L), path.rows().get(0).get(0));
            assertEquals(4L, path.rows().get(0).get(1));
            script(db, "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:5})");
            QueryResult direct = run(db,
                    "CALL path.shortest({start: 1, end: 5, rel: \"holder\", hops: 1}) YIELD length, vids");
            assertEquals(1L, direct.rows().get(0).get(0));
            assertEquals(List.of(1L, 5L), direct.rows().get(0).get(1));
            QueryResult same = run(db,
                    "CALL path.shortest({start: 1, end: 1, rel: \"holder\", hops: 3}) YIELD length, vids");
            assertEquals(0L, same.rows().get(0).get(0));
            assertEquals(List.of(1L), same.rows().get(0).get(1));
            CengyanException unknown = assertThrows(CengyanException.class, () -> run(db,
                    "CALL path.all({start: 1, end: 5, rel: \"holder\", hops: 4}) YIELD length, vids"));
            assertEquals(ErrorCode.SYNTAX, unknown.code());
            assertTrue(unknown.getMessage().contains("未知过程"));
        }
    }

    @Test
    void 最短路碰到每点和每跳上限就失败() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()");
            for (int id = 2; id <= 66; id++) {
                script(db, "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:" + id + "})");
            }
            CengyanException frontier = assertThrows(CengyanException.class, () -> db.engine.execute(
                    db.session,
                    "CALL path.shortest({start: 1, end: 1000, rel: \"holder\", hops: 4}) YIELD length, vids",
                    Map.of(),
                    new QueryLimits(200, 64, 10, 100),
                    null));
            assertEquals(ErrorCode.FRONTIER_EXCEEDED, frontier.code());
            assertTrue(frontier.getMessage().contains("上限"));
            CengyanException degree = assertThrows(CengyanException.class, () -> db.engine.execute(
                    db.session,
                    "CALL path.shortest({start: 1, end: 1000, rel: \"holder\", hops: 4}) YIELD length, vids",
                    Map.of(),
                    new QueryLimits(64),
                    null));
            assertEquals(ErrorCode.TRUNCATED, degree.code());
            assertTrue(degree.getMessage().contains("第 1 跳"));
        }
    }

    @Test
    void 时限到了扫描就停() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()");
            int graphId = db.session.current().graphId();
            int typeId = db.catalog.relType(graphId, "holder").id();
            db.catalog.insertVertex(graphId, "Company", 1L, Map.of(), false);
            writeStar(db, graphId, typeId, 2_000_000);
            QueryHandle handle = QueryHandle.start(50);
            long started = System.nanoTime();
            CengyanException canceled = assertThrows(CengyanException.class, () -> db.engine.execute(
                    db.session,
                    "MATCH (a:Company {id:1})-[:holder]->(b) RETURN b.id",
                    Map.of(),
                    new QueryLimits(Integer.MAX_VALUE, 64, 10, 100),
                    handle));
            long elapsedMs = (System.nanoTime() - started) / 1_000_000L;
            assertEquals(ErrorCode.QUERY_CANCELED, canceled.code());
            assertEquals("超时", handle.reason());
            assertTrue(elapsedMs < 100, "耗时 " + elapsedMs + "ms");
            long seen = db.store.edgesExamined();
            assertTrue(seen > 1_000, "扫描条数 " + seen);
            Thread.sleep(200);
            assertEquals(seen, db.store.edgesExamined());
        }
    }

    private static void writeStar(QueryEngineTest.Db db, int graphId, int typeId, int edges) {
        StoreBatch batch = new StoreBatch();
        int pending = 0;
        for (int index = 0; index < edges; index++) {
            long dst = index + 2L;
            batch.put(Cf.EDGE, KeyCodec.edgeKey(graphId, 0, 1L, typeId, Direction.OUT.code(), 0L, dst), new byte[0]);
            batch.put(Cf.EDGE, KeyCodec.edgeKey(graphId, 0, dst, typeId, Direction.IN.code(), 0L, 1L), new byte[0]);
            pending++;
            if (pending >= 4_000) {
                db.store.commitEdges(batch);
                batch = new StoreBatch();
                pending = 0;
            }
        }
        if (!batch.isEmpty()) {
            db.store.commitEdges(batch);
        }
    }

    private static QueryEngine zeroThresholds(QueryEngineTest.Db db) {
        return new QueryEngine(db.catalog, db.store);
    }

    private static QueryResult run(QueryEngineTest.Db db, String gql) {
        return db.engine.execute(db.session, gql, Map.of(), new QueryLimits(64), null);
    }

    private static void script(QueryEngineTest.Db db, String... statements) {
        for (String statement : statements) {
            db.engine.execute(db.session, statement, Map.of(), new QueryLimits(64), null);
        }
    }
}
