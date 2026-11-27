package io.cengyan.query;

import io.cengyan.distributed.EntityCodec;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.distributed.TransactionEngine;
import io.cengyan.parser.Expr;
import io.cengyan.parser.Gql;
import io.cengyan.parser.Stmt;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.GraphDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** 路径查询的起点下推、最短路和读缓存。存储换成内存图，统计每类读取的次数。 */
class RemoteTraversalTest {
    @TempDir Path work;
    static final GraphDefinition GRAPH = GraphDefinition.newBuilder().setId(7).setName("G").setPartitions(4).build();

    /** 内存图：读点、读邻接边都计数，扫描直接失败。 */
    static final class Graph implements RemoteQueryEngine.Reads {
        final Map<Value, Entity> nodes = new LinkedHashMap<>();
        final List<Entity> edges = new ArrayList<>();
        final Map<String, Integer> neighborReads = new HashMap<>();
        int nodeReads;

        Graph node(String id, String type) {
            nodes.put(EntityCodec.value(id), Entity.newBuilder().setNode(true).setType(type).setId(EntityCodec.value(id)).build());
            return this;
        }
        Graph inv(String from, String to, double conprop) {
            edges.add(Entity.newBuilder().setNode(false).setType("inv").setId(EntityCodec.value(from + ">" + to))
                    .setSource(EntityCodec.value(from)).setTarget(EntityCodec.value(to)).putProperties("conprop", EntityCodec.value(conprop)).build());
            return this;
        }
        @Override public Entity node(GraphDefinition graph, String type, Value id, TransactionEngine.QueryScope scope) {
            nodeReads++;
            Entity found = nodes.get(id);
            return found == null || (!type.isEmpty() && !type.equals(found.getType())) ? null : found;
        }
        @Override public List<Entity> neighbors(GraphDefinition graph, Value vertex, boolean out, String edgeType, TransactionEngine.QueryScope scope) {
            neighborReads.merge(EntityCodec.object(vertex) + (out ? ">" : "<"), 1, Integer::sum);
            return edges.stream().filter(e -> (out ? e.getSource() : e.getTarget()).equals(vertex)).toList();
        }
        @Override public List<Entity> scan(GraphDefinition graph, String type, Map<String, Value> filters, TransactionEngine.QueryScope scope) {
            throw new AssertionError("WHERE 里给了起点 ID，不应该扫描整张图");
        }
    }

    private RemoteQueryEngine engine(Graph graph) {
        return engine(graph, "1GB");
    }

    private RemoteQueryEngine engine(Graph graph, String disk) {
        RemoteQueryEngine engine = new RemoteQueryEngine(
                new StorageGateway("test", List.of(new InetSocketAddress("127.0.0.1", 1))), work, "64MB", disk, 1);
        engine.reads = graph;
        return engine;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Entity>> clauses(RemoteQueryEngine engine, Stmt.Select select) throws Exception {
        Method method = RemoteQueryEngine.class.getDeclaredMethod("clauses", GraphDefinition.class, List.class, Map.class, BooleanSupplier.class);
        method.setAccessible(true);
        try (var memory = QueryMemory.open(64L << 20)) {
            return (List<Map<String, Entity>>) method.invoke(engine, GRAPH, select.clauses(), Map.of(), (BooleanSupplier) () -> false);
        } catch (InvocationTargetException ex) {
            if (ex.getCause() instanceof Exception cause) throw cause;
            throw ex;
        }
    }

    private static QueryResult run(RemoteQueryEngine engine, String gql) throws Exception {
        Stmt.Select select = (Stmt.Select) Gql.parse(gql);
        List<Map<String, Entity>> rows = clauses(engine, select);
        Method project = RemoteQueryEngine.class.getDeclaredMethod("project", List.class, List.class, List.class,
                boolean.class, Expr.class, Expr.class, Expr.class, ExprEval.class, BooleanSupplier.class);
        project.setAccessible(true);
        Stmt.Tail t = select.tail();
        assertTrue(RemoteQueryEngine.groupsMatch(t.items(), t.groupBy()), "GROUP BY 校验");
        try (var memory = QueryMemory.open(64L << 20)) {
            return (QueryResult) project.invoke(engine, rows, t.items(), t.orderBy(), t.distinct(), t.postFilter(), t.skip(), t.limit(),
                    new ExprEval(Map.of(), false), (BooleanSupplier) () -> false);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<String> nodeIds(Object path) {
        Map<String, Object> body = (Map<String, Object>) path;
        return ((List<Map<String, Object>>) body.get("nodes")).stream().map(n -> String.valueOf(n.get("id"))).toList();
    }

    @Test
    void 最短路按层搜索并且只从WHERE给的起点出发() throws Exception {
        Graph g = new Graph().node("p1", "person");
        for (String id : List.of("a", "b", "c", "d", "e", "q1", "q2", "q3", "x")) g.node(id, "org");
        // p1 到 q1 有两条 2 跳最短路和一条 3 跳的路；q2 要 4 跳；q3 不连通；x 是一个多跳内到处都连的大点。
        g.inv("a", "p1", 0.5).inv("a", "q1", 0.5).inv("p1", "b", 0.5).inv("b", "q1", 0.5)
                .inv("p1", "d", 0.5).inv("d", "e", 0.5).inv("e", "q1", 0.5)
                .inv("q1", "c", 0.5).inv("c", "q2", 0.5);
        for (String id : List.of("a", "b", "d", "e")) g.inv(id, "x", 0.1);
        QueryResult result = run(engine(g), "MATCH p = ALL SHORTEST TRAIL (s)-[:inv]-{1,5}(t) "
                + "WHERE s.id IN ['p1'] AND t.id IN ['q1', 'q2', 'q3'] RETURN p");
        List<List<String>> paths = result.rows().stream().map(row -> nodeIds(row.getFirst())).toList();
        assertEquals(Set.of(List.of("p1", "a", "q1"), List.of("p1", "b", "q1"), List.of("p1", "a", "q1", "c", "q2"),
                List.of("p1", "b", "q1", "c", "q2")), new HashSet<>(paths), paths.toString());
        for (var entry : g.neighborReads.entrySet()) assertEquals(1, entry.getValue(), "每个点每个方向只读一次邻接边：" + entry.getKey());
    }

    @Test
    void 路径展开经过同一个点时不重复读存储() throws Exception {
        Graph g = new Graph().node("p1", "person");
        for (String id : List.of("a", "b", "m", "z1", "z2", "z3")) g.node(id, "org");
        // 菱形之后再扇出：经过 m 的路径有两条，m 的邻接边只应读一次。
        g.inv("a", "p1", 0.6).inv("b", "p1", 0.4).inv("m", "a", 1.0).inv("m", "b", 1.0)
                .inv("z1", "m", 0.5).inv("z2", "m", 0.5).inv("z3", "m", 0.5);
        QueryResult result = run(engine(g), "MATCH p = (start:person)<-[e:inv]-{1,4}(investee:org) WHERE id(start) IN ('p1') "
                + "LET path_conprop = REDUCE(acc = 1.0, rel IN relationships(p) | acc * rel.conprop) "
                + "RETURN id(investee) AS end_vid, collect_list(nodes(p)) AS node_list, sum(path_conprop) AS total_conprop "
                + "FILTER total_conprop >= 0.5 ORDER BY total_conprop DESC, end_vid ASC");
        assertEquals(List.of("end_vid", "node_list", "total_conprop"), result.columns());
        List<String> ends = result.rows().stream().map(row -> String.valueOf(row.getFirst())).toList();
        assertEquals(List.of("m", "a", "z1", "z2", "z3"), ends, "m=0.6+0.4，a=0.6，z=0.5；b=0.4 被过滤");
        assertEquals(1.0, ((Number) result.rows().getFirst().get(2)).doubleValue(), 1e-9);
        assertEquals(2, ((List<?>) result.rows().getFirst().get(1)).size(), "m 有两条路径");
        for (var entry : g.neighborReads.entrySet()) assertEquals(1, entry.getValue(), "重复读了邻接边：" + entry.getKey());
    }

    @Test
    void 受益所有人查询返回起点终点并按终点分组() throws Exception {
        Graph g = new Graph().node("q", "org").node("h", "org").node("u1", "person").node("u2", "person");
        g.inv("q", "h", 0.8).inv("h", "u1", 0.5).inv("q", "u1", 0.1).inv("h", "u2", 0.2);
        QueryResult result = run(engine(g), "MATCH p = (start:org {id: 'q'})-[e:inv]->{1,10}(shareholder:person) "
                + "LET path_conprop = REDUCE(acc = 1.0, rel IN relationships(p) | acc * rel.conprop) "
                + "RETURN start as start_node, shareholder as end_node, collect_list(nodes(p)) AS node_list, "
                + "collect_list(relationships(p)) AS path_list, sum(path_conprop) AS total_conprop "
                + "GROUP BY shareholder FILTER total_conprop >= 0.25 ORDER BY total_conprop DESC");
        assertEquals(1, result.rows().size(), result.rows().toString());
        List<Object> row = result.rows().getFirst();
        assertEquals("u1", String.valueOf(((Map<?, ?>) row.get(1)).get("id")));
        assertEquals("q", String.valueOf(((Map<?, ?>) row.get(0)).get("id")));
        assertEquals(0.5, ((Number) row.get(4)).doubleValue(), 1e-9, "0.8*0.5 + 0.1");
        assertEquals(2, ((List<?>) row.get(3)).size());
    }

    @Test
    void 按层计数与落盘路径聚合保持结果并遵守临时盘预算() throws Exception {
        var graph = new Graph().node("s", "org").node("a", "org").node("b", "org").node("t", "org")
                .inv("a", "s", 0.6).inv("b", "s", 0.4).inv("t", "a", 1).inv("t", "b", 1);
        String match = "MATCH p = (s:org)<-[e:inv]-{1,7}(t:org) WHERE id(s) = 's' ";
        var count = (Stmt.Select) Gql.parse(match + "RETURN count(*) AS n");
        String gql = match + "LET w = REDUCE(acc = 1.0, rel IN relationships(p) | acc * coalesce(rel.conprop, 0.0)) "
                + "RETURN s AS start_node, t AS end_node, collect_list(nodes(p)) AS node_list, "
                + "collect_list(relationships(p)) AS path_list, sum(w) AS total GROUP BY start_node, end_node "
                + "FILTER total >= 0.5 ORDER BY total DESC SKIP 0 LIMIT 1000";
        var grouped = (Stmt.Select) Gql.parse(gql);
        assertNotNull(SpillPathQuery.plan(count));
        assertNotNull(SpillPathQuery.plan(grouped));
        var reference = run(engine(graph), gql);
        try (var memory = QueryMemory.open(16L << 20)) {
            assertEquals(List.of(List.of(4L)), engine(graph).run(GRAPH, count, Map.of(), () -> false).rows());
            var result = engine(graph).run(GRAPH, grouped, Map.of(), () -> false);
            assertEquals(reference, result);
            var limit = engine(graph, "128B");
            assertEquals("QUERY_TEMP_LIMIT_EXCEEDED", assertThrows(io.cengyan.distributed.ClusterException.class,
                    () -> limit.run(GRAPH, grouped, Map.of(), () -> false)).code());
        }
        try (var files = java.nio.file.Files.list(work)) { assertEquals(0, files.count()); }
    }

    @Test
    void 起点ID候选从节点模式和WHERE里取交集() {
        ExprEval eval = new ExprEval(Map.of("ids", List.of("a", "b")), false);
        Stmt.Clause.Match match = (Stmt.Clause.Match) ((Stmt.Select) Gql.parse(
                "MATCH p = (s)-[:inv]->(t) WHERE id(s) = 'a' AND t.id IN $ids AND s.name = 'x' RETURN s")).clauses().getFirst();
        Stmt.NodePat s = ((Stmt.Element.Node) match.patterns().getFirst().elements().getFirst()).node();
        Stmt.NodePat t = ((Stmt.Element.Node) match.patterns().getFirst().elements().getLast()).node();
        assertEquals(List.of("a"), RemoteQueryEngine.idCandidates(s, match.where(), eval));
        assertEquals(List.of("a", "b"), RemoteQueryEngine.idCandidates(t, match.where(), eval));
        Stmt.Clause.Match or = (Stmt.Clause.Match) ((Stmt.Select) Gql.parse(
                "MATCH p = (s)-[:inv]->(t) WHERE id(s) = 'a' OR s.name = 'x' RETURN s")).clauses().getFirst();
        assertNull(RemoteQueryEngine.idCandidates(s, or.where(), eval), "OR 不能下推");
    }
}
