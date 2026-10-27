package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.catalog.Auth;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Phase8Test {
    @Test
    void 有界子图包含起点并沿两个方向扩展() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})",
                    "INSERT (a:Company {id:2})-[:holder]->(b:Company {id:3})");
            QueryResult around = run(db,
                    "CALL subgraph.bounded({start: 2, rel: 'holder', hops: 1}) YIELD vid");
            assertEquals(List.of("vid"), around.columns());
            assertEquals(List.of(2L, 3L, 1L), column(around));
            QueryResult downstream = run(db,
                    "CALL subgraph.bounded({start: 1, rel: 'holder', hops: 2}) YIELD vid");
            assertEquals(List.of(1L, 2L, 3L), column(downstream));
            QueryResult alone = run(db,
                    "CALL subgraph.bounded({start: 1, rel: 'holder', hops: 0}) YIELD vid");
            assertEquals(List.of(1L), column(alone));
            CengyanException extra = assertThrows(CengyanException.class, () -> run(db,
                    "CALL subgraph.bounded({start: 1, rel: 'holder', hops: 1, sample: 1}) YIELD vid"));
            assertEquals(ErrorCode.SYNTAX, extra.code());
            assertTrue(extra.getMessage().contains("未知参数"));
        }
    }

    @Test
    void 子图碰到每跳点数和每点边数就失败() throws Exception {
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
                    "CALL subgraph.bounded({start: 1, rel: \"holder\", hops: 1}) YIELD vid",
                    Map.of(),
                    new QueryLimits(200, 64, 10, 100),
                    null));
            assertEquals(ErrorCode.FRONTIER_EXCEEDED, frontier.code());
            CengyanException degree = assertThrows(CengyanException.class, () -> db.engine.execute(
                    db.session,
                    "CALL subgraph.bounded({start: 1, rel: \"holder\", hops: 1}) YIELD vid",
                    Map.of(),
                    new QueryLimits(64),
                    null));
            assertEquals(ErrorCode.TRUNCATED, degree.code());
            assertTrue(degree.getMessage().contains("第 1 跳"));
        }
    }

    @Test
    void 只读会话不能改数据但可以查询() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})");
            db.session.bind("reader", Auth.ROLE_READ);
            CengyanException insert = assertThrows(CengyanException.class, () -> script(db,
                    "INSERT (a:Company {id:2})-[:holder]->(b:Company {id:3})"));
            assertEquals(ErrorCode.FORBIDDEN, insert.code());
            assertTrue(insert.getMessage().contains("只读"));
            CengyanException create = assertThrows(CengyanException.class, () -> script(db, "CREATE GRAPH Other"));
            assertEquals(ErrorCode.FORBIDDEN, create.code());
            QueryResult rows = run(db, "MATCH (a:Company {id:1})-[:holder]->(b) RETURN b.id");
            assertEquals(List.of(2L), column(rows));
            CengyanException rebuild = assertThrows(CengyanException.class,
                    () -> db.engine.admin(db.session, "flush", "", null));
            assertEquals(ErrorCode.FORBIDDEN, rebuild.code());
        }
    }

    private static QueryResult run(QueryEngineTest.Db db, String gql) {
        return db.engine.execute(db.session, gql, Map.of(), new QueryLimits(64), null);
    }

    private static void script(QueryEngineTest.Db db, String... statements) {
        for (String statement : statements) {
            db.engine.execute(db.session, statement, Map.of(), new QueryLimits(64), null);
        }
    }

    private static List<Object> column(QueryResult result) {
        List<Object> values = new ArrayList<>();
        for (List<Object> row : result.rows()) {
            values.add(row.get(0));
        }
        return values;
    }
}
