package io.cengyan.query;

import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PathAllTest {
    @Test
    void 从一个点向外返回全部简单路径并且环不再往下走() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})",
                    "INSERT (a:Company {id:2})-[:holder]->(b:Company {id:3})",
                    "INSERT (a:Company {id:3})-[:holder]->(b:Company {id:1})",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:4})",
                    "INSERT (a:Company {id:2})-[:holder]->(b:Company {id:4})");
            QueryResult paths = run(db, """
                    CALL path.all({start: 1, rel: "holder", min: 1, max: 3, direction: "out", limit: 20})
                    YIELD path
                    """);
            assertEquals(Set.of(
                    List.of(1L, 2L),
                    List.of(1L, 2L, 3L),
                    List.of(1L, 2L, 4L),
                    List.of(1L, 4L)), nodeIds(paths));
            for (List<Object> ids : nodeIds(paths)) {
                assertEquals(ids.size(), new HashSet<>(ids).size());
            }
            for (List<Object> row : paths.rows()) {
                assertAligned(row.get(0));
            }
            QueryResult back = run(db, """
                    CALL path.all({start: 1, end: 1, rel: "holder", max: 3, limit: 20})
                    YIELD path
                    """);
            assertEquals(List.of(), back.rows());
            QueryResult once = run(db, "MATCH (a:Company {id:1})-[:holder]->{1,3}(b) RETURN b.id");
            assertEquals(Set.of(List.of(2L), List.of(3L), List.of(4L)), new HashSet<>(once.rows()));
        }
    }

    @Test
    void 两点之间返回全部路径() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})",
                    "INSERT (a:Company {id:2})-[:holder]->(b:Company {id:4})",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:3})",
                    "INSERT (a:Company {id:3})-[:holder]->(b:Company {id:4})",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:4})");
            QueryResult paths = run(db, """
                    CALL path.all({start: 1, end: 4, rel: "holder", min: 1, max: 3, limit: 20})
                    YIELD length, path
                    """);
            assertEquals(Set.of(
                    path(1, List.of(node(1L), node(4L)), List.of(edge("holder", 0, 1L, 4L))),
                    path(2, List.of(node(1L), node(2L), node(4L)),
                            List.of(edge("holder", 0, 1L, 2L), edge("holder", 0, 2L, 4L))),
                    path(2, List.of(node(1L), node(3L), node(4L)),
                            List.of(edge("holder", 0, 1L, 3L), edge("holder", 0, 3L, 4L)))),
                    bodies(paths, 1));
            for (List<Object> row : paths.rows()) {
                assertEquals(row.get(0), ((Map<?, ?>) row.get(1)).get("length"));
            }
            CengyanException exceeded = assertThrows(CengyanException.class, () -> run(db, """
                    CALL path.all({start: 1, end: 4, rel: "holder", max: 3, limit: 1})
                    YIELD path
                    """));
            assertEquals(ErrorCode.PATH_LIMIT_EXCEEDED, exceeded.code());
            assertTrue(exceeded.getMessage().contains("超过上限 1"));
        }
    }

    @Test
    void 字符串节点ID按外部ID返回() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH corp",
                    "USE corp",
                    "CREATE NODE TYPE Company ID STRING(32) ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:'e_001'})-[:holder]->(b:Company {id:'e_002'})",
                    "INSERT (a:Company {id:'e_002'})-[:holder]->(b:Company {id:'e_003'})",
                    "INSERT (a:Company {id:'e_001'})-[:holder]->(b:Company {id:'e_003'})");
            QueryResult paths = run(db, """
                    CALL path.all({start: "e_001", end: "e_003", rel: "holder", max: 2, limit: 10})
                    YIELD path
                    """);
            assertEquals(Set.of(
                    path(1, List.of(node("e_001"), node("e_003")),
                            List.of(edge("holder", 0, "e_001", "e_003"))),
                    path(2, List.of(node("e_001"), node("e_002"), node("e_003")),
                            List.of(edge("holder", 0, "e_001", "e_002"), edge("holder", 0, "e_002", "e_003")))),
                    bodies(paths, 0));
        }
    }

    @Test
    void 路径变量返回同样的对象并且终点写在模式里() throws Exception {
        try (QueryEngineTest.Db db = new QueryEngineTest.Db()) {
            script(db,
                    "CREATE GRAPH Company",
                    "USE Company",
                    "CREATE NODE TYPE Company ()",
                    "CREATE REL TYPE holder ()",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:2})",
                    "INSERT (a:Company {id:2})-[:holder]->(b:Company {id:4})",
                    "INSERT (a:Company {id:1})-[:holder]->(b:Company {id:4})");
            QueryResult outward = run(db, """
                    MATCH p = (a:Company {id:1})-[:holder]->{1,3}(b) RETURN p LIMIT 20
                    """);
            assertEquals(Set.of(
                    List.of(1L, 2L),
                    List.of(1L, 2L, 4L),
                    List.of(1L, 4L)), nodeIds(outward));
            QueryResult between = run(db, """
                    MATCH p = (a:Company {id:1})-[:holder]->{1,3}(b:Company {id:4}) RETURN p LIMIT 20
                    """);
            assertEquals(Set.of(List.of(1L, 4L), List.of(1L, 2L, 4L)), nodeIds(between));
            QueryResult backward = run(db, """
                    MATCH p = (a:Company {id:2})<-[:holder]-{1}(b) RETURN p LIMIT 5
                    """);
            assertEquals(Set.of(path(1, List.of(node(2L), node(1L)), List.of(edge("holder", 0, 1L, 2L)))),
                    bodies(backward, 0));
            QueryResult oneHop = run(db, """
                    MATCH p = (a:Company {id:1})-[:holder]->(b) RETURN p
                    """);
            assertEquals(Set.of(
                    path(1, List.of(node(1L), node(2L)), List.of(edge("holder", 0, 1L, 2L))),
                    path(1, List.of(node(1L), node(4L)), List.of(edge("holder", 0, 1L, 4L)))),
                    bodies(oneHop, 0));
            CengyanException missing = assertThrows(CengyanException.class, () -> run(db, """
                    MATCH p = (a:Company {id:1})-[:holder]->{1,3}(b) RETURN p
                    """));
            assertEquals(ErrorCode.SYNTAX, missing.code());
            assertTrue(missing.getMessage().contains("需要写 LIMIT"));
            CengyanException tooMany = assertThrows(CengyanException.class, () -> run(db, """
                    MATCH p = (a:Company {id:1})-[:holder]->{1,3}(b) RETURN p LIMIT 1
                    """));
            assertEquals(ErrorCode.PATH_LIMIT_EXCEEDED, tooMany.code());
            assertTrue(tooMany.getMessage().contains("超过上限 1"));
        }
    }

    @SuppressWarnings("unchecked")
    private static Set<List<Object>> nodeIds(QueryResult result) {
        Set<List<Object>> ids = new HashSet<>();
        for (List<Object> row : result.rows()) {
            Map<String, Object> path = (Map<String, Object>) row.get(0);
            List<Object> nodes = new ArrayList<>();
            for (Object node : (List<Object>) path.get("nodes")) {
                nodes.add(((Map<String, Object>) node).get("id"));
            }
            ids.add(nodes);
        }
        return ids;
    }

    @SuppressWarnings("unchecked")
    private static void assertAligned(Object value) {
        Map<String, Object> path = (Map<String, Object>) value;
        List<Object> nodes = (List<Object>) path.get("nodes");
        List<Object> edges = (List<Object>) path.get("edges");
        assertEquals(edges.size(), ((Number) path.get("length")).intValue());
        assertEquals(nodes.size(), edges.size() + 1);
    }

    @SuppressWarnings("unchecked")
    private static Set<Map<String, Object>> bodies(QueryResult result, int column) {
        Set<Map<String, Object>> paths = new HashSet<>();
        for (List<Object> row : result.rows()) {
            paths.add((Map<String, Object>) row.get(column));
        }
        return paths;
    }

    private static Map<String, Object> path(int length, List<Map<String, Object>> nodes, List<Map<String, Object>> edges) {
        Map<String, Object> path = new LinkedHashMap<>();
        path.put("length", (long) length);
        path.put("nodes", nodes);
        path.put("edges", edges);
        return path;
    }

    private static Map<String, Object> node(Object id) {
        Map<String, Object> node = new LinkedHashMap<>();
        node.put("id", id);
        return node;
    }

    private static Map<String, Object> edge(String type, long rank, Object src, Object dst) {
        Map<String, Object> edge = new LinkedHashMap<>();
        edge.put("type", type);
        edge.put("rank", rank);
        edge.put("src", src);
        edge.put("dst", dst);
        return edge;
    }

    private static void script(QueryEngineTest.Db db, String... statements) {
        for (String statement : statements) {
            db.engine.execute(db.session, statement, Map.of(), new QueryLimits(64), null);
        }
    }

    private static QueryResult run(QueryEngineTest.Db db, String gql) {
        return db.engine.execute(db.session, gql, Map.of(), new QueryLimits(64), null);
    }
}
