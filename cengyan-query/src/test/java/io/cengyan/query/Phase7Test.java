package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase7Test {
    @Test
    void 建完前走扫描重建后等值和范围走索引() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp",
                    "USE corp",
                    "CREATE NODE TYPE Company (name STRING NULL, capital INT64 NULL)",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:1, name:'acme', capital:-2})",
                    "INSERT (a:Company {id:2, name:'beta', capital:0})",
                    "INSERT (a:Company {id:3, capital:4})",
                    "INSERT (a {id:1})-[:holder {ratio:0.5}]->(b {id:2})",
                    "INSERT (a {id:2})-[:holder {ratio:-0.4}]->(b {id:3})",
                    "INSERT (a {id:3})-[:holder {ratio:0.2}]->(b {id:1})");
            script(db,
                    "CREATE INDEX by_name ON NODE TYPE Company (name)",
                    "CREATE INDEX by_capital ON NODE TYPE Company (capital)");
            long reads = db.store.indexReads();
            long vertices = db.store.verticesExamined();
            assertEquals(List.of(1L), ids(run(db, "MATCH (a:Company)-[:holder]->(b) WHERE a.name = 'acme' RETURN a.id")));
            assertTrue(db.store.verticesExamined() > vertices);
            assertEquals(reads, db.store.indexReads());

            assertEquals(2, db.engine.admin(db.session, "rebuildIndex", "by_name", null).rowsAffected());
            assertEquals(3, db.engine.admin(db.session, "rebuildIndex", "by_capital", null).rowsAffected());
            QueryResult listed = run(db, "SHOW INDEXES");
            assertEquals(List.of("name", "target", "type", "properties", "unique", "ready"), listed.columns());
            assertEquals(2, listed.rows().size());
            assertEquals(true, listed.rows().get(0).get(5));

            reads = db.store.indexReads();
            vertices = db.store.verticesExamined();
            assertEquals(List.of(1L), ids(run(db, "MATCH (a:Company)-[:holder]->(b) WHERE a.name = 'acme' RETURN a.id")));
            assertTrue(db.store.indexReads() > reads);
            assertEquals(vertices, db.store.verticesExamined());

            reads = db.store.indexReads();
            vertices = db.store.verticesExamined();
            assertEquals(List.of(2L, 3L), ids(run(db,
                    "MATCH (a:Company)-[:holder]->(b) WHERE a.capital >= 0 RETURN a.id")));
            assertEquals(List.of(1L), ids(run(db, "MATCH (a:Company)-[:holder]->(b) WHERE a.capital < 0 RETURN a.id")));
            assertTrue(db.store.indexReads() > reads);
            assertEquals(vertices, db.store.verticesExamined());

            reads = db.store.indexReads();
            vertices = db.store.verticesExamined();
            List<Long> scanned = ids(run(db, "MATCH (a:Company)-[:holder]->(b) WHERE a.name > 'a' RETURN a.id"));
            assertTrue(scanned.contains(1L));
            assertTrue(scanned.contains(2L));
            assertTrue(db.store.verticesExamined() > vertices);
            assertEquals(reads, db.store.indexReads());

            reads = db.store.indexReads();
            vertices = db.store.verticesExamined();
            assertEquals(List.of(), ids(run(db, "MATCH (a:Company)-[:holder]->(b) WHERE a.name = 'missing' RETURN a.id")));
            assertEquals(vertices, db.store.verticesExamined());

            long edges = db.store.edgesExamined();
            reads = db.store.indexReads();
            assertEquals(List.of(2L), ids(run(db, "MATCH (a:Company)-[e:holder]->(b) WHERE e.ratio = 0.5 RETURN b.id")));
            assertTrue(db.store.edgesExamined() > edges);
            script(db, "CREATE INDEX by_ratio ON REL TYPE holder (ratio)");
            assertEquals(3, db.engine.admin(db.session, "rebuildIndex", "by_ratio", null).rowsAffected());
            edges = db.store.edgesExamined();
            reads = db.store.indexReads();
            vertices = db.store.verticesExamined();
            assertEquals(List.of(2L), ids(run(db, "MATCH (a:Company)-[e:holder]->(b) WHERE e.ratio = 0.5 RETURN b.id")));
            assertEquals(List.of(3L), ids(run(db, "MATCH (a:Company)-[e:holder]->(b) WHERE e.ratio < 0 RETURN b.id")));
            assertEquals(edges, db.store.edgesExamined());
            assertEquals(vertices, db.store.verticesExamined());
            assertTrue(db.store.indexReads() > reads);

            script(db, "INSERT (a {id:1})-[:holder@1 {ratio:0.1}]->(b {id:1})");
            QueryResult stats = db.engine.admin(db.session, "stats", "", null);
            assertEquals(List.of("target", "name", "count"), stats.columns());
            assertEquals(List.of("NODE", "Company", 3L), stats.rows().get(0));
            assertEquals(List.of("REL", "holder", 4L), stats.rows().get(1));
            assertNotNull(stats.collectedAt());
            assertEquals(stats.collectedAt(), db.engine.admin(db.session, "stats", "", null).collectedAt());

            run(db, "MATCH (a:Company {id:1}) SET a.name = 'gamma'");
            vertices = db.store.verticesExamined();
            assertEquals(List.of(), ids(run(db, "MATCH (a:Company)-[:holder]->(b) WHERE a.name = 'acme' RETURN a.id")));
            assertEquals(List.of(1L, 1L), ids(run(db, "MATCH (a:Company)-[:holder]->(b) WHERE a.name = 'gamma' RETURN a.id")));
            assertEquals(vertices, db.store.verticesExamined());

            run(db, "MATCH (a:Company {id:2}) DELETE a");
            assertNull(db.catalog.readVertex(db.session.current().graphId(), "Company", 2L));
            vertices = db.store.verticesExamined();
            assertEquals(List.of(3L), ids(run(db, "MATCH (a:Company)-[:holder]->(b) WHERE a.capital >= 0 RETURN a.id")));
            assertEquals(vertices, db.store.verticesExamined());
        }
    }

    @Test
    void 字符串节点ID可以写入查询并连接边() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp",
                    "USE corp",
                    "CREATE NODE TYPE Company ID STRING(32) (name STRING NULL)",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:'e_001', name:'root'})",
                    "INSERT (a:Company {id:'e_002', name:'child'})",
                    "INSERT (a {id:'e_001'})-[:holder {ratio:0.5}]->(b {id:'e_002'})");
            QueryResult listed = run(db, "SHOW NODE TYPES");
            assertEquals(List.of("name", "id", "schemaVersion", "idType", "idLength", "comment"), listed.columns());
            assertEquals(java.util.Arrays.asList("Company", 1, 1, "STRING", 32, null), listed.rows().get(0));
            QueryResult result = run(db, "MATCH (a:Company {id:'e_001'}) RETURN a.id, a.name");
            assertEquals(List.of(List.of("e_001", "root")), result.rows());
            assertEquals(List.of(List.of("e_002")),
                    run(db, "MATCH (a:Company {id:'e_001'})-[:holder]->(b) RETURN b.id").rows());
            assertEquals(List.of(List.of("e_002")),
                    run(db, "MATCH (a:Company {id:'e_001'})-[:holder]->(b {id:'e_002'}) RETURN b.id").rows());
            assertEquals(List.of(),
                    run(db, "MATCH (a:Company {id:'e_001'})-[:holder]->(b {id:'missing'}) RETURN b.id").rows());
            Map<?, ?> edge = (Map<?, ?>) run(db,
                    "MATCH (a:Company {id:'e_001'})-[e:holder]->(b) RETURN e").rows().get(0).get(0);
            assertEquals("e_001", edge.get("src"));
            assertEquals("e_002", edge.get("dst"));
        }
    }

    @Test
    void 查询和修改不存在的字符串ID不会创建映射() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp",
                    "USE corp",
                    "CREATE NODE TYPE Company ID STRING(32) (name STRING NULL)",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:'e_001', name:'root'})");
            assertEquals(List.of(), run(db, "MATCH (a:Company {id:'missing'}) RETURN a.id").rows());
            script(db, "MATCH (a:Company {id:'missing'}) SET a.name = 'ghost'");
            assertEquals(List.of(), run(db, "MATCH (a {id:'missing'}) RETURN a.id").rows());
            CengyanException missing = assertThrows(CengyanException.class,
                    () -> run(db, "INSERT (a {id:'missing'})-[:holder]->(b {id:'e_001'})"));
            assertEquals(ErrorCode.TYPE_MISMATCH, missing.code());
        }
    }

    @Test
    void 过程调用支持字符串节点ID并返回外部ID() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp",
                    "USE corp",
                    "CREATE NODE TYPE Company ID STRING(32) (name STRING NULL)",
                    "CREATE REL TYPE holder (ratio DOUBLE NULL)",
                    "INSERT (a:Company {id:'e_001', name:'root'})",
                    "INSERT (a:Company {id:'e_002', name:'mid'})",
                    "INSERT (a:Company {id:'e_003', name:'leaf'})",
                    "INSERT (a {id:'e_001'})-[:holder {ratio:0.8}]->(b {id:'e_002'})",
                    "INSERT (a {id:'e_002'})-[:holder {ratio:0.7}]->(b {id:'e_003'})");
            QueryResult shortest = run(db,
                    "CALL path.shortest({start: 'e_001', end: 'e_003', rel: 'holder', hops: 3}) YIELD length, vids");
            assertEquals(List.of(2L, List.of("e_001", "e_002", "e_003")), shortest.rows().get(0));
            QueryResult subgraph = run(db,
                    "CALL subgraph.bounded({start: 'e_001', rel: 'holder', hops: 1}) YIELD vid");
            assertEquals(List.of(List.of("e_001"), List.of("e_002")), subgraph.rows());
            QueryResult ownership = run(db,
                    "CALL ownership.largest_holder({start: 'e_001', rel: 'holder', ratioProp: 'ratio', hops: 3}) YIELD vid, ratio");
            assertEquals("e_002", ownership.rows().get(0).get(0));
        }
    }

    @Test
    void 唯一索引拒绝重复并允许多个空值() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp",
                    "USE corp",
                    "CREATE NODE TYPE Company (name STRING NULL)",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:20, name:'dup'})",
                    "INSERT (a:Company {id:21, name:'dup'})",
                    "CREATE UNIQUE INDEX by_name ON NODE TYPE Company (name)");
            CengyanException duplicated = assertThrows(CengyanException.class,
                    () -> db.engine.admin(db.session, "rebuildIndex", "by_name", null));
            assertEquals(ErrorCode.UNIQUE_VIOLATION, duplicated.code());
            assertTrue(duplicated.getMessage().contains("唯一索引冲突"));
            assertEquals(false, run(db, "SHOW INDEXES").rows().get(0).get(5));
            assertEquals("dup", db.catalog.readVertex(db.session.current().graphId(), "Company", 20L).get("name"));
            assertEquals("dup", db.catalog.readVertex(db.session.current().graphId(), "Company", 21L).get("name"));

            script(db, "DROP INDEX by_name", "CREATE UNIQUE INDEX by_name ON NODE TYPE Company (name)");
            run(db, "MATCH (a:Company {id:20}) DELETE a");
            run(db, "MATCH (a:Company {id:21}) DELETE a");
            assertEquals(0, db.engine.admin(db.session, "rebuildIndex", "by_name", null).rowsAffected());
            script(db,
                    "INSERT (a:Company {id:1, name:'solo'})",
                    "INSERT (a:Company {id:2})",
                    "INSERT (a:Company {id:3})");
            CengyanException conflict = assertThrows(CengyanException.class,
                    () -> run(db, "INSERT (a:Company {id:4, name:'solo'})"));
            assertEquals(ErrorCode.UNIQUE_VIOLATION, conflict.code());
            assertEquals("solo", db.catalog.readVertex(db.session.current().graphId(), "Company", 1L).get("name"));
            assertNull(db.catalog.readVertex(db.session.current().graphId(), "Company", 4L));

            CengyanException sameBatch = assertThrows(CengyanException.class, () -> run(db,
                    "INSERT (a:Company {id:40, name:'twin'})-[:holder]->(b:Company {id:41, name:'twin'})"));
            assertEquals(ErrorCode.UNIQUE_VIOLATION, sameBatch.code());
            assertNull(db.catalog.readVertex(db.session.current().graphId(), "Company", 40L));
            assertNull(db.catalog.readVertex(db.session.current().graphId(), "Company", 41L));

            run(db, "MATCH (a:Company {id:1}) DELETE a");
            script(db, "INSERT (a:Company {id:5, name:'solo'})");
            assertEquals("solo", db.catalog.readVertex(db.session.current().graphId(), "Company", 5L).get("name"));

            db.store.injectBeforeWrite(() -> {
                throw new CengyanException(ErrorCode.INTERNAL, "写入前失败");
            });
            CengyanException aborted = assertThrows(CengyanException.class,
                    () -> run(db, "INSERT (a:Company {id:6, name:'kept'})"));
            assertEquals(ErrorCode.INTERNAL, aborted.code());
            assertNull(db.catalog.readVertex(db.session.current().graphId(), "Company", 6L));
            assertEquals("solo", db.catalog.readVertex(db.session.current().graphId(), "Company", 5L).get("name"));
        }
    }

    @Test
    void 重建被取消时索引保持未就绪() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp",
                    "USE corp",
                    "CREATE NODE TYPE Company (name STRING NULL)",
                    "INSERT (a:Company {id:1, name:'acme'})",
                    "CREATE INDEX by_name ON NODE TYPE Company (name)");
            QueryHandle handle = QueryHandle.start(60_000);
            handle.cancel("测试");
            CengyanException canceled = assertThrows(CengyanException.class,
                    () -> db.engine.admin(db.session, "rebuildIndex", "by_name", handle));
            assertEquals(ErrorCode.QUERY_CANCELED, canceled.code());
            assertEquals(false, run(db, "SHOW INDEXES").rows().get(0).get(5));
            CengyanException unknown = assertThrows(CengyanException.class,
                    () -> db.engine.admin(db.session, "vacuum", "", null));
            assertEquals(ErrorCode.SYNTAX, unknown.code());
            CengyanException missing = assertThrows(CengyanException.class,
                    () -> run(db, "CREATE INDEX bad ON NODE TYPE Company (missing)"));
            assertEquals(ErrorCode.TYPE_MISMATCH, missing.code());
            assertTrue(missing.getMessage().contains("未知属性"));
        }
    }

    private static void script(QueryEngineTest.Db db, String... statements) {
        for (String statement : statements) {
            run(db, statement);
        }
    }

    private static QueryResult run(QueryEngineTest.Db db, String gql) {
        return db.engine.execute(db.session, gql, Map.of(), new QueryLimits(64), null);
    }

    private static List<Long> ids(QueryResult result) {
        List<Long> values = new ArrayList<>();
        for (List<Object> row : result.rows()) {
            values.add((Long) row.get(0));
        }
        return values;
    }
}
