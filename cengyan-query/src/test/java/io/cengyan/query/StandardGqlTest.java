package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardGqlTest {
    @Test
    void 新语法覆盖多段可选集合表达式和写入() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp IF NOT EXISTS",
                    "USE corp",
                    "CREATE NODE TYPE org (name STRING, comment STRING NULL, regcap DOUBLE NULL)",
                    "CREATE REL TYPE inv (ratio DOUBLE NULL)",
                    "INSERT (a:org {id:1, name:'root', regcap:1e8}), (b:org {id:2, name:'mid', regcap:10.0}), (a)-[:inv {ratio:0.6}]->(b)",
                    "INSERT (a:org {id:3, name:'end'})",
                    "INSERT (a {id:2})-[:inv {ratio:0.5}]->(b {id:3})");
            QueryResult chain = run(db, "MATCH (a:org {id:1})-[:inv]->(b)-[:inv]->(c) RETURN c.id");
            assertEquals(List.of(3L), column(chain));
            QueryResult optional = run(db, "MATCH (a:org {id:3}) OPTIONAL MATCH (a)-[:inv]->(b) RETURN a.id, b.id");
            assertEquals(1, optional.rows().size());
            assertEquals(3L, optional.rows().get(0).get(0));
            assertEquals(null, optional.rows().get(0).get(1));
            QueryResult filtered = run(db, "MATCH (a:org {id:1})-[e:inv]->{1,2}(b) WHERE e.ratio > 0.55 RETURN b.id");
            assertEquals(List.of(2L), column(filtered));
            QueryResult labels = run(db, "MATCH (a:org {id:1}) RETURN labels(a)");
            assertEquals(List.of("org"), labels.rows().get(0).get(0));
            QueryResult chosen = run(db, "MATCH (a:org {id:1}) RETURN CASE WHEN a.regcap > 1 THEN 'big' ELSE 'small' END");
            assertEquals("big", chosen.rows().get(0).get(0));
            QueryResult united = run(db,
                    "MATCH (a:org {id:1}) RETURN a.id AS id UNION MATCH (a:org {id:3}) RETURN a.id AS id");
            assertEquals(2, united.rows().size());
            run(db, "MATCH (a:org {id:1}) SET a.comment = 'note'");
            assertEquals("note", run(db, "MATCH (a:org {id:1}) RETURN a.comment").rows().get(0).get(0));
            CengyanException blocked = assertThrows(CengyanException.class,
                    () -> run(db, "MATCH (a:org {id:2}) DELETE a"));
            assertEquals(ErrorCode.CONSTRAINT, blocked.code());
            run(db, "MATCH (a:org {id:2}) DETACH DELETE a");
            run(db, "DROP REL TYPE inv");
            run(db, "DROP NODE TYPE org");
            assertEquals(List.of(), run(db, "SHOW NODE TYPES").rows());
        }
    }

    @Test
    void 反引号和转义() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp",
                    "USE corp",
                    "CREATE NODE TYPE org (`持股比例` DOUBLE NULL)",
                    "INSERT (a:org {id:1, `持股比例`:0.2})");
            QueryResult row = run(db, "MATCH (a:org {id:1}) RETURN a.`持股比例`");
            assertEquals(0.2d, ((Number) row.rows().get(0).get(0)).doubleValue(), 1e-9);
            QueryResult text = run(db, "MATCH (a:org {id:1}) RETURN 'a\\nb'");
            assertEquals("a\nb", text.rows().get(0).get(0));
            assertTrue(run(db, "MATCH (a:org|%) RETURN a.id").rows().size() >= 1);
        }
    }

    private static void script(QueryEngineTest.Db db, String... statements) {
        for (String statement : statements) {
            db.engine.execute(db.session, statement, Map.of(), new QueryLimits(64), null);
        }
    }

    private static QueryResult run(QueryEngineTest.Db db, String gql) {
        return db.engine.execute(db.session, gql, Map.of(), new QueryLimits(64), null);
    }

    private static List<Object> column(QueryResult result) {
        return result.rows().stream().map(row -> row.get(0)).toList();
    }
}
