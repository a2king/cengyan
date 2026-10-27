package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.VertexWrite;
import io.cengyan.store.GraphStore;
import io.cengyan.store.PerfReport;
import io.cengyan.store.StoreOptions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryEngineTest {
    @Test
    void 两个方向和无向匹配() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company (name STRING NULL)",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:1, name:'root'})-[:holder {ratio:0.5}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})<-[:holder]-(b:Company {id:3})");
            QueryResult out = run(db, "MATCH (a:Company {id:1})-[:holder]->(b) RETURN b.id");
            assertEquals(List.of(2L), column(out, 0));
            QueryResult in = run(db, "MATCH (a:Company {id:1})<-[:holder]-(b) RETURN b.id");
            assertEquals(List.of(3L), column(in, 0));
            QueryResult any = run(db, "MATCH (a:Company {id:1})-[:holder]-(b) RETURN b.id");
            assertEquals(List.of(2L, 3L), column(any, 0));
            db.engine.execute(db.session,
                    "INSERT (a:Company {id:5})-[:holder]->(b:Company {id:5})", Map.of(), limits(), null);
            QueryResult loop = run(db, "MATCH (a:Company {id:5})-[:holder]-(b) RETURN b.id");
            assertEquals(List.of(5L), column(loop, 0));
        }
    }

    @Test
    void 参数不会拼进语句() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:3})");
            QueryResult result = db.engine.execute(
                    db.session,
                    "MATCH (a:Company {id:1})-[e:holder]->(b) WHERE b.id = $id RETURN b.id",
                    Map.of("id", 2, "note", "1 OR 1 = 1"),
                    limits(),
                    null);
            assertEquals(List.of(2L), column(result, 0));
            QueryResult ordered = db.engine.execute(
                    db.session,
                    "MATCH (a:Company {id:1})-[:holder]->(b) RETURN b.id AS id ORDER BY id DESC",
                    Map.of(),
                    limits(),
                    null);
            assertEquals(List.of(3L, 2L), column(ordered, 0));
        }
    }

    @Test
    void 别名和日期时间() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "CREATE ALIAS corp FOR GRAPH Company",
                    "USE corp",
                    "CREATE NODE TYPE Company (founded DATETIME NULL)",
                    "CREATE REL TYPE holder (since DATETIME NULL)",
                    "INSERT (a:Company {id:1, founded: DATETIME '2024-01-31T12:00:00Z'})"
                            + "-[:holder {since: DATETIME '2024-01-31 12:00:00'}]->(b:Company {id:2})");
            db.engine.execute(db.session, "USE Company", Map.of(), limits(), null);
            QueryResult result = run(db, "MATCH (a:Company {id:1})-[e:holder]->(b) RETURN a.founded, e.since");
            assertEquals("2024-01-31T12:00:00Z", result.rows().get(0).get(0));
            assertEquals("2024-01-31T12:00:00Z", result.rows().get(0).get(1));
            QueryResult graphs = run(db, "SHOW GRAPHS");
            assertEquals(List.of("Company"), column(graphs, 0));
        }
    }

    @Test
    void 只返回终点编号时不读属性() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company (name STRING NULL)",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:1, name:'root'})-[:holder {ratio:0.5}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.2}]->(b:Company {id:3})",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.1}]->(b:Company {id:4})");
            long before = db.store.propReads();
            QueryResult ids = run(db, "MATCH (a:Company {id:1})-[e:holder]->(b) RETURN b.id LIMIT 2");
            assertEquals(2, ids.rows().size());
            assertEquals(0, db.store.propReads() - before);
            long afterIds = db.store.propReads();
            QueryResult ratios = run(db, "MATCH (a:Company {id:1})-[e:holder]->(b) RETURN e.ratio LIMIT 2");
            assertEquals(2, ratios.rows().size());
            assertEquals(2, db.store.propReads() - afterIds);
        }
    }

    @Test
    void 度数超过上限时语句上限仍然先停住() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()");
            int graphId = db.session.current().graphId();
            for (int i = 1; i <= 1000; i++) {
                db.catalog.insertRel(graphId, "holder", 0L,
                        VertexWrite.of("Company", 1L, Map.of()), VertexWrite.onlyId(i), Map.of(), false);
            }
            long before = db.store.edgesExamined();
            QueryResult result = run(db, "MATCH (a:Company {id:1})-[:holder]->(b) RETURN b.id LIMIT 10");
            assertEquals(10, result.rows().size());
            assertEquals(10, db.store.edgesExamined() - before);
            assertEquals(1L, result.rows().get(0).get(0));
            assertEquals(10L, result.rows().get(9).get(0));
            CengyanException truncated = assertThrows(CengyanException.class, () -> run(
                    db, "MATCH (a:Company {id:1})-[:holder]->(b) RETURN b.id LIMIT 100", new QueryLimits(64)));
            assertEquals(ErrorCode.TRUNCATED, truncated.code());
        }
    }

    @Test
    void 没有起点编号时按类型扫描() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})",
                    "INSERT (a:Company {id:3})-[:holder]->(b:Company {id:4})",
                    "INSERT (a:Company {id:5})-[:holder]->(b:Company {id:6})");
            QueryResult result = run(db, "MATCH (a:Company)-[:holder]->(b) RETURN a.id");
            assertEquals(List.of(1L, 3L, 5L), column(result, 0));
            CengyanException truncated = assertThrows(CengyanException.class,
                    () -> run(db, "MATCH (a:Company)-[:holder]->(b) RETURN a.id", new QueryLimits(2)));
            assertEquals(ErrorCode.TRUNCATED, truncated.code());
            CengyanException syntax = assertThrows(CengyanException.class,
                    () -> run(db, "MATCH (a)-[:holder]->(b) RETURN b.id"));
            assertEquals(ErrorCode.SYNTAX, syntax.code());
        }
    }

    @Test
    void 插入无向边和除以零会失败() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})");
            CengyanException undirected = assertThrows(CengyanException.class,
                    () -> run(db, "INSERT (a:Company {id:3})-[:holder]-(b:Company {id:4})"));
            assertEquals(ErrorCode.SYNTAX, undirected.code());
            assertTrue(undirected.getMessage().contains("插入边必须有方向"));
            CengyanException divide = assertThrows(CengyanException.class,
                    () -> run(db, "MATCH (a:Company {id:1})-[:holder]->(b) RETURN 1 / 0"));
            assertEquals(ErrorCode.DIVIDE_BY_ZERO, divide.code());
            assertFalse(db.catalog.graphs().isEmpty());
        }
    }

    @Tag("perf")
    @Test
    void 一跳一百条边的p99低于10毫秒() throws Exception {
        StoreOptions options = new StoreOptions(256L * 1024 * 1024, 2, 64L * 1024 * 1024);
        try (Db db = new Db(options)) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()");
            int graphId = db.session.current().graphId();
            for (int i = 1; i <= 100; i++) {
                db.catalog.insertRel(graphId, "holder", 0L,
                        VertexWrite.of("Company", 1L, Map.of()), VertexWrite.onlyId(i), Map.of(), false);
            }
            QueryLimits limits = new QueryLimits(128);
            String query = "MATCH (a:Company {id:1})-[:holder]->(b) RETURN b.id LIMIT 100";
            for (int i = 0; i < 30; i++) {
                assertEquals(100, db.engine.execute(db.session, query, Map.of(), limits, null).rows().size());
            }
            long[] nanos = new long[80];
            for (int i = 0; i < nanos.length; i++) {
                long start = System.nanoTime();
                QueryResult result = db.engine.execute(db.session, query, Map.of(), limits, null);
                nanos[i] = System.nanoTime() - start;
                assertEquals(100, result.rows().size());
            }
            long p50 = PerfReport.percentile(nanos, 0.50);
            long p99 = PerfReport.percentile(nanos, 0.99);
            PerfReport.append("一跳 LIMIT 100", "1 个起点，100 条边", true, 256L * 1024 * 1024, p50, p99);
            assertTrue(p99 < 10_000_000L, "一跳 p99=" + p99 / 1_000_000.0 + "ms");
        }
    }

    private static QueryResult run(Db db, String gql) {
        return run(db, gql, limits());
    }

    private static QueryResult run(Db db, String gql, QueryLimits limits) {
        return db.engine.execute(db.session, gql, Map.of(), limits, null);
    }

    private static void script(Db db, String... statements) {
        for (String statement : statements) {
            db.engine.execute(db.session, statement, Map.of(), limits(), null);
        }
    }

    private static QueryLimits limits() {
        return new QueryLimits(64);
    }

    private static List<Object> column(QueryResult result, int index) {
        List<Object> values = new ArrayList<>();
        for (List<Object> row : result.rows()) {
            values.add(row.get(index));
        }
        return values;
    }

    private static final class Db implements AutoCloseable {
        private final Path dir;
        private final GraphStore store;
        private final Catalog catalog;
        private final QueryEngine engine;
        private final SessionState session = new SessionState();

        private Db() throws Exception {
            this(StoreOptions.development());
        }

        private Db(StoreOptions options) throws Exception {
            dir = Files.createTempDirectory("cengyan-gql-");
            store = GraphStore.open(dir, options);
            catalog = Catalog.load(store);
            engine = new QueryEngine(catalog, store);
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
