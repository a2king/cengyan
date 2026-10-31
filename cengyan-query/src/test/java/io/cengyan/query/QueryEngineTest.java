package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Catalog;
import io.cengyan.catalog.VertexWrite;
import io.cengyan.store.GraphStore;
import io.cengyan.store.KeyCodec;
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
    void 批量节点和边一次提交并可查询() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Bulk",
                    "USE Bulk",
                    "CREATE NODE TYPE Person (name STRING NULL)",
                    "CREATE REL TYPE knows (score DOUBLE NULL)");
            QueryResult nodes = db.engine.bulkInsert(
                    db.session,
                    "node",
                    "Person",
                    List.of(
                            Map.of("id", 1L, "name", "one"),
                            Map.of("id", 2L, "name", "two"),
                            Map.of("id", 3L, "name", "three")),
                    false,
                    false,
                    null);
            assertEquals(3, nodes.rowsAffected());
            QueryResult edges = db.engine.bulkInsert(
                    db.session,
                    "edge",
                    "knows",
                    List.of(
                            Map.of("src", 1L, "dst", 2L, "rank", 0L, "score", 0.5d),
                            Map.of("src", 1L, "dst", 3L, "rank", 0L, "score", 0.25d)),
                    false,
                    false,
                    null);
            assertEquals(2, edges.rowsAffected());
            QueryResult result = run(db, "MATCH (a:Person {id:1})-[e:knows]->(b) RETURN b.id, e.score");
            assertEquals(2, result.rows().size());
        }
    }

    @Test
    void 批量节点校验失败时整批不提交() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH BulkAtomic",
                    "USE BulkAtomic",
                    "CREATE NODE TYPE Person (name STRING)");
            assertThrows(CengyanException.class, () -> db.engine.bulkInsert(
                    db.session,
                    "node",
                    "Person",
                    List.of(Map.of("id", 1L, "name", "ok"), Map.of("id", 2L)),
                    false,
                    false,
                    null));
            assertEquals(0, run(db, "MATCH (a:Person) RETURN a.id").rows().size());
        }
    }

    @Test
    void 未注册端点默认失败开启后跳过() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Org",
                    "USE Org",
                    "CREATE NODE TYPE org ID STRING(64) ()",
                    "CREATE REL TYPE inv ()");
            QueryResult nodes = db.engine.bulkInsert(
                    db.session,
                    "node",
                    "org",
                    List.of(Map.of("id", "e_known"), Map.of("id", "e_other")),
                    false,
                    false,
                    null);
            assertEquals(2, nodes.rowsAffected());
            CengyanException missing = assertThrows(CengyanException.class, () -> db.engine.bulkInsert(
                    db.session,
                    "edge",
                    "inv",
                    List.of(Map.of("src", "e_known", "dst", "e_missing")),
                    false,
                    false,
                    null));
            assertEquals(ErrorCode.TYPE_MISMATCH, missing.code());
            assertTrue(missing.getMessage().contains("尚未注册"));
            assertEquals(0, run(db, "MATCH (a:org)-[:inv]->(b) RETURN b.id").rows().size());
            QueryResult edges = db.engine.bulkInsert(
                    db.session,
                    "edge",
                    "inv",
                    List.of(
                            Map.of("src", "e_known", "dst", "e_missing"),
                            Map.of("src", "e_known", "dst", "e_other")),
                    false,
                    true,
                    null);
            assertEquals(1, edges.rowsAffected());
            assertEquals(List.of("e_other"), column(run(db, "MATCH (a:org)-[:inv]->(b) RETURN b.id"), 0));
        }
    }

    @Test
    void 批量边可以跨分片写入() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Multi PARTS 2",
                    "USE Multi",
                    "CREATE NODE TYPE Person ()",
                    "CREATE REL TYPE knows (score DOUBLE NULL)");
            assertEquals(2, db.catalog.partCount(db.session.current().graphId()));
            assertTrue(KeyCodec.partOf(1L, 2) != KeyCodec.partOf(2L, 2));
            assertEquals(KeyCodec.partOf(1L, 2), KeyCodec.partOf(3L, 2));
            db.engine.bulkInsert(
                    db.session,
                    "node",
                    "Person",
                    List.of(Map.of("id", 1L), Map.of("id", 2L), Map.of("id", 3L)),
                    false,
                    false,
                    null);
            QueryResult edges = db.engine.bulkInsert(
                    db.session,
                    "edge",
                    "knows",
                    List.of(
                            Map.of("src", 1L, "dst", 2L, "score", 0.5d),
                            Map.of("src", 1L, "dst", 3L, "score", 0.25d)),
                    false,
                    false,
                    null);
            assertEquals(2, edges.rowsAffected());
            List<Object> targets = new ArrayList<>(column(
                    run(db, "MATCH (a:Person {id:1})-[e:knows]->(b) RETURN b.id"), 0));
            targets.sort(Comparator.comparing(value -> ((Number) value).longValue()));
            assertEquals(List.of(2L, 3L), targets);
            assertEquals(List.of(0.5d), column(
                    run(db, "MATCH (a:Person {id:1})-[e:knows]->(b:Person {id:2}) RETURN e.score"), 0));
            assertEquals(List.of(1L), column(
                    run(db, "MATCH (a:Person {id:2})<-[:knows]-(b) RETURN b.id"), 0));
            assertEquals(List.of(1L), column(
                    run(db, "MATCH (a:Person {id:3})<-[:knows]-(b) RETURN b.id"), 0));
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

    @Test
    void 修改和删除一次提交() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company (name STRING NULL)",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:1, name:'root'})-[:holder {ratio:0.5}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.2}]->(b:Company {id:3})");
            int graphId = db.session.current().graphId();
            QueryResult updated = run(db,
                    "MATCH (a:Company {id:1})-[e:holder]->(b {id:2}) SET e.ratio = 0.25, a.name = 'renamed'");
            assertEquals(2, updated.rowsAffected());
            assertEquals(0.25d, db.catalog.readRel(graphId, "holder", 0L, 1L, 2L).get("ratio"));
            assertEquals("renamed", db.catalog.readVertex(graphId, "Company", 1L).get("name"));
            assertEquals(0.2d, db.catalog.readRel(graphId, "holder", 0L, 1L, 3L).get("ratio"));
            QueryResult missed = run(db,
                    "MATCH (a:Company {id:1})-[e:holder]->(b) WHERE b.id = 9 SET e.ratio = 0.9");
            assertEquals(0, missed.rowsAffected());
            assertEquals(0.25d, db.catalog.readRel(graphId, "holder", 0L, 1L, 2L).get("ratio"));
            db.store.injectBeforeWrite(() -> {
                throw new CengyanException(ErrorCode.INTERNAL, "提交前失败");
            });
            CengyanException crashed = assertThrows(CengyanException.class, () -> run(db,
                    "MATCH (a:Company {id:1})-[e:holder]->(b {id:2}) SET e.ratio = 0.8"));
            assertTrue(crashed.getMessage().contains("提交前失败"));
            assertEquals(0.25d, db.catalog.readRel(graphId, "holder", 0L, 1L, 2L).get("ratio"));
            QueryResult deleted = run(db, "MATCH (a:Company {id:1})-[e:holder]->(b {id:2}) DELETE e");
            assertEquals(1, deleted.rowsAffected());
            assertEquals(null, db.catalog.readRel(graphId, "holder", 0L, 1L, 2L));
            assertEquals(0.2d, db.catalog.readRel(graphId, "holder", 0L, 1L, 3L).get("ratio"));
            QueryResult removed = run(db, "MATCH (a:Company {id:1}) DELETE a");
            assertEquals(1, removed.rowsAffected());
            assertEquals(null, db.catalog.readVertex(graphId, "Company", 1L));
            assertEquals(null, db.catalog.readRel(graphId, "holder", 0L, 1L, 3L));
            CengyanException multi = assertThrows(CengyanException.class, () -> run(db,
                    "MATCH (a:Company {id:1})-[:holder]->{1,2}(b) DELETE b"));
            assertEquals(ErrorCode.SYNTAX, multi.code());
            assertTrue(multi.getMessage().contains("一跳"));
        }
    }

    @Test
    void 点的边数超过上限时不删除() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()");
            int graphId = db.session.current().graphId();
            for (int i = 2; i <= 66; i++) {
                db.catalog.insertRel(graphId, "holder", 0L,
                        VertexWrite.of("Company", 1L, Map.of()), VertexWrite.onlyId(i), Map.of(), false);
            }
            CengyanException exceeded = assertThrows(CengyanException.class,
                    () -> run(db, "MATCH (a:Company {id:1}) DELETE a"));
            assertEquals(ErrorCode.VERTEX_DEGREE_EXCEEDED, exceeded.code());
            assertEquals(Map.of(), db.catalog.readRel(graphId, "holder", 0L, 1L, 2L));
            assertEquals(Map.of(), db.catalog.readVertex(graphId, "Company", 1L));
        }
    }

    @Test
    void 聚合和去重在已经收住的行上计算() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.5}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})-[:holder@1 {ratio:0.5}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.25}]->(b:Company {id:3})");
            QueryResult distinct = run(db,
                    "MATCH (a:Company {id:1})-[:holder]->(b) RETURN DISTINCT b.id AS id ORDER BY id");
            assertEquals(List.of(2L, 3L), column(distinct, 0));
            QueryResult totals = run(db,
                    "MATCH (a:Company {id:1})-[e:holder]->(b) RETURN count(*) AS n, sum(e.ratio) AS total");
            assertEquals(List.of("n", "total"), totals.columns());
            assertEquals(3L, totals.rows().get(0).get(0));
            assertEquals(1.25d, ((Number) totals.rows().get(0).get(1)).doubleValue(), 1e-9);
            QueryResult empty = run(db,
                    "MATCH (a:Company {id:1})-[e:holder]->(b) WHERE b.id = 9 RETURN count(*) AS n, sum(e.ratio) AS total");
            assertEquals(0L, empty.rows().get(0).get(0));
            assertEquals(null, empty.rows().get(0).get(1));
            CengyanException mixed = assertThrows(CengyanException.class, () -> run(db,
                    "MATCH (a:Company {id:1})-[e:holder]->(b) RETURN count(*), b.id"));
            assertEquals(ErrorCode.SYNTAX, mixed.code());
            assertTrue(mixed.getMessage().contains("聚合"));
        }
    }

    @Test
    void 多种边和只有点() throws Exception {
        try (Db db = new Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "CREATE REL TYPE works (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:1})-[:holder {ratio:0.5}]->(b:Company {id:2})",
                    "INSERT (a:Company {id:1})-[:works {ratio:0.2}]->(b:Company {id:3})",
                    "INSERT (a:Company {id:2})-[:holder {ratio:0.1}]->(b:Company {id:4})");
            assertEquals(List.of(2L, 3L), column(run(db, "MATCH (a {id:1})-[e]->(b) RETURN b.id"), 0));
            List<Object> types = new ArrayList<>();
            for (Object cell : column(run(db, "MATCH (a {id:1})-[e]->(b) RETURN e"), 0)) {
                Map<?, ?> edge = (Map<?, ?>) cell;
                types.add(edge.get("type"));
                assertEquals("Company", edge.get("srcType"));
                assertEquals("Company", edge.get("dstType"));
            }
            assertEquals(List.of("holder", "works"), types);
            assertEquals(List.of(2L, 3L), column(run(db, "MATCH (a {id:1})-[e:holder|:works]->(b) RETURN b.id"), 0));
            assertEquals(List.of(3L), column(run(db, "MATCH (a {id:1})-[:works]->(b) RETURN b.id"), 0));
            assertEquals(List.of(2L), column(run(db, "MATCH (a {id:1})-[e]->(b) RETURN b.id LIMIT 1"), 0));
            QueryResult hops = run(db, "MATCH (a {id:1})-[:holder|:works]->{1,2}(b) RETURN b.id");
            assertEquals(List.of(2L, 3L, 4L), column(hops, 0));
            Map<?, ?> node = (Map<?, ?>) run(db, "MATCH (a:Company {id:1}) RETURN a").rows().get(0).get(0);
            assertEquals(1L, node.get("id"));
            assertEquals("Company", node.get("type"));
            assertEquals(List.of(1L), column(run(db, "MATCH (a:Company) WHERE a.id = 1 RETURN a.id"), 0));
            CengyanException missing = assertThrows(CengyanException.class,
                    () -> run(db, "MATCH (a {id:1})-[:holder|:missing]->(b) RETURN b.id"));
            assertEquals(ErrorCode.TYPE_NOT_FOUND, missing.code());
            CengyanException bare = assertThrows(CengyanException.class,
                    () -> run(db, "INSERT (a {id:1})-[e]->(b {id:9})"));
            assertEquals(ErrorCode.SYNTAX, bare.code());
            assertTrue(bare.getMessage().contains("一个类型"));
            CengyanException many = assertThrows(CengyanException.class,
                    () -> run(db, "INSERT (a {id:1})-[:holder|:works]->(b {id:9})"));
            assertEquals(ErrorCode.SYNTAX, many.code());
            CengyanException truncated = assertThrows(CengyanException.class,
                    () -> run(db, "MATCH (a {id:1})-[e]->(b) RETURN b.id", new QueryLimits(1)));
            assertEquals(ErrorCode.TRUNCATED, truncated.code());
            run(db, "MATCH (a {id:1})-[e:holder|:works]->(b) SET e.ratio = 0.9");
            assertEquals(List.of(0.9d, 0.9d), column(run(db, "MATCH (a {id:1})-[e]->(b) RETURN e.ratio"), 0));
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

    static final class Db implements AutoCloseable {
        final Path dir;
        final GraphStore store;
        final Catalog catalog;
        final QueryEngine engine;
        final SessionState session = new SessionState();

        Db() throws Exception {
            this(StoreOptions.development());
        }

        Db(StoreOptions options) throws Exception {
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
