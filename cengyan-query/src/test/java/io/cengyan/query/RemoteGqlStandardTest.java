package io.cengyan.query;

import io.cengyan.distributed.EntityCodec;
import io.cengyan.distributed.StorageGateway;
import io.cengyan.distributed.TransactionEngine;
import io.cengyan.parser.Gql;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import io.cengyan.protocol.proto.ClusterProtocol.GraphDefinition;
import io.cengyan.protocol.proto.ClusterProtocol.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/** 标准 GQL 写法在 Compute 执行器上的结果。存储换成内存图。 */
class RemoteGqlStandardTest {
    @TempDir Path work;
    static final GraphDefinition GRAPH = GraphDefinition.newBuilder().setId(7).setName("G").setPartitions(4).build();

    static final class Graph implements RemoteQueryEngine.Reads {
        final Map<Value, Entity> nodes = new LinkedHashMap<>();
        final List<Entity> edges = new ArrayList<>();

        Graph node(String id, String type, Object... props) {
            var node = Entity.newBuilder().setNode(true).setType(type).setId(EntityCodec.value(id));
            for (int i = 0; i + 1 < props.length; i += 2) node.putProperties((String) props[i], EntityCodec.value(props[i + 1]));
            nodes.put(EntityCodec.value(id), node.build());
            return this;
        }
        Graph edge(String type, String from, String to, double w) {
            edges.add(Entity.newBuilder().setNode(false).setType(type).setId(EntityCodec.value(type + ":" + from + ">" + to))
                    .setSource(EntityCodec.value(from)).setTarget(EntityCodec.value(to)).putProperties("w", EntityCodec.value(w)).build());
            return this;
        }
        @Override public Entity node(GraphDefinition graph, String type, Value id, TransactionEngine.QueryScope scope) {
            Entity found = nodes.get(id);
            return found == null || (!type.isEmpty() && !type.equals(found.getType())) ? null : found;
        }
        @Override public List<Entity> neighbors(GraphDefinition graph, Value vertex, boolean out, String edgeType, TransactionEngine.QueryScope scope) {
            return edges.stream().filter(e -> (out ? e.getSource() : e.getTarget()).equals(vertex))
                    .filter(e -> edgeType == null || edgeType.equals(e.getType())).toList();
        }
        @Override public List<Entity> scan(GraphDefinition graph, String type, Map<String, Value> filters, TransactionEngine.QueryScope scope) {
            return nodes.values().stream().filter(n -> type == null || type.equals(n.getType()))
                    .filter(n -> filters.entrySet().stream().allMatch(f -> f.getValue().equals(n.getPropertiesMap().get(f.getKey())))).toList();
        }
    }

    /** a→b→c→d 一条链（R），a→c 一条近路（R），a→d 一条 S，a→b 一条 T；e 是孤立点。 */
    static Graph sample() {
        Graph g = new Graph();
        for (String id : List.of("a", "b", "c", "d")) g.node(id, "N", "name", id.toUpperCase(Locale.ROOT));
        g.node("e", "M", "name", "E");
        return g.edge("R", "a", "b", 0.9).edge("R", "b", "c", 0.2).edge("R", "c", "d", 0.8).edge("R", "a", "c", 0.5)
                .edge("S", "a", "d", 0.1).edge("T", "a", "b", 0.7);
    }

    private RemoteQueryEngine engine(Graph graph) {
        RemoteQueryEngine engine = new RemoteQueryEngine(
                new StorageGateway("test", List.of(new InetSocketAddress("127.0.0.1", 1))), work, "64MB", "1GB", 1);
        engine.reads = graph;
        return engine;
    }

    private QueryResult run(Graph graph, String gql) {
        try (var memory = QueryMemory.open(64L << 20)) {
            return engine(graph).run(GRAPH, Gql.parse(gql), Map.of(), () -> false);
        }
    }

    private static List<Object> column(QueryResult result, int index) {
        return result.rows().stream().map(row -> row.get(index)).toList();
    }

    @Test
    void EXISTS和VALUE子查询在当前行上执行() {
        Graph g = sample();
        assertEquals(List.of("a", "b", "c"), column(run(g, "MATCH (n:N) WHERE EXISTS { (n)-[:R]->(m) } RETURN n.id ORDER BY n.id"), 0));
        assertEquals(List.of("d"), column(run(g, "MATCH (n:N) WHERE NOT EXISTS { MATCH (n)-[:R]->(m) } RETURN n.id"), 0));
        QueryResult counted = run(g, "MATCH (n:N) RETURN n.id AS id, VALUE { MATCH (n)-[:R]->(m) RETURN count(*) } AS out ORDER BY id");
        assertEquals(List.of(2L, 1L, 1L, 0L), column(counted, 1));
    }

    @Test
    void 内联CALL和OPTIONAL块() {
        Graph g = sample();
        QueryResult inline = run(g, "MATCH (n:N) CALL (n) { MATCH (n)-[:R]->(m) RETURN count(*) AS c } RETURN n.id, c ORDER BY n.id");
        assertEquals(List.of(2L, 1L, 1L, 0L), column(inline, 1));
        QueryResult optional = run(g, "MATCH (n:N {id: 'c'}) OPTIONAL { MATCH (n)-[:R]->(m) MATCH (m)-[:R]->(k) } RETURN n.id, k");
        assertEquals(1, optional.rows().size());
        assertNull(optional.rows().get(0).get(1), "c→d 之后没有边，整块对不上，k 为空");
        QueryResult optionalCall = run(g, "MATCH (n:N {id: 'd'}) OPTIONAL CALL (n) { MATCH (n)-[:R]->(m) RETURN m.id AS next } RETURN n.id, next");
        assertEquals(Arrays.asList("d", null), optionalCall.rows().get(0));
    }

    @Test
    void 边上的标签表达式真的按类型过滤() {
        Graph g = sample();
        assertEquals(List.of("b", "c", "d"), column(run(g, "MATCH (a:N {id: 'a'})-[e:R|S]->(b) RETURN b.id ORDER BY b.id"), 0),
                "T 类型的 a→b 不能算进来");
        assertEquals(List.of("b", "d"), column(run(g, "MATCH (a:N {id: 'a'})-[e:!R]->(b) RETURN b.id ORDER BY b.id"), 0));
        assertEquals(List.of("b"), column(run(g, "MATCH (a:N {id: 'a'})-[e IS R WHERE e.w > 0.6]->(b) RETURN b.id"), 0));
        assertEquals(List.of("b", "c", "d"), column(run(g, "MATCH (a:N {id: 'a'})->(b) RETURN DISTINCT b.id ORDER BY b.id"), 0));
        assertEquals(List.of("a", "b", "c", "d"), column(run(g, "MATCH (n) WHERE n:N RETURN n.id ORDER BY n.id"), 0));
        assertEquals(List.of("e"), column(run(g, "MATCH (n:!N) RETURN n.id"), 0));
    }

    @Test
    void 路径并集去重而多重集并集保留重复() {
        Graph g = sample();
        String union = "MATCH (a:N {id: 'a'})-[:R]->(b) | (a:N {id: 'a'})-[:T]->(b) RETURN b.id ORDER BY b.id";
        assertEquals(List.of("b", "b", "c"), column(run(g, union), 0), "a-R->b 和 a-T->b 是两条不同的路径（边不同），都保留");
        assertEquals(List.of("b", "c"), column(run(g, "MATCH (a:N {id: 'a'})-[:R]->(b) | (a:N {id: 'a'})-[:R]->(b) RETURN b.id ORDER BY b.id"), 0),
                "两个分支匹配到同一条路径，只留一份");
        assertEquals(List.of("b", "b", "c", "c"), column(run(g, "MATCH (a:N {id: 'a'})-[:R]->(b) |+| (a:N {id: 'a'})-[:R]->(b) RETURN b.id ORDER BY b.id"), 0));
    }

    @Test
    void 不同边模式排除重复的边() {
        Graph g = sample();
        String query = "MATCH (a:N {id: 'a'})-[e:R]->(b), (a)-[f:R]->(c) RETURN count(*)";
        assertEquals(4L, run(g, query).rows().get(0).get(0));
        assertEquals(2L, run(g, query.replace("MATCH ", "MATCH DIFFERENT EDGES ")).rows().get(0).get(0));
    }

    @Test
    void 量化路径模式每一跳都检查组内条件() {
        Graph g = sample();
        // 只走 w > 0.4 的 R 边：a→b(0.9)、a→c(0.5)、c→d(0.8)；b→c(0.2) 不能走。
        QueryResult result = run(g, "MATCH (s:N {id: 'a'}) ((x)-[e:R]->(y) WHERE e.w > 0.4){1,3} (t) RETURN t.id, size(e) AS hops ORDER BY t.id");
        assertEquals(List.of("b", "c", "d"), column(result, 0));
        assertEquals(List.of(1L, 1L, 2L), column(result, 1));
        // 组在开头，组内变量绑定为每一跳的列表。两跳且每跳 w > 0.4 的只有 a→c→d。
        QueryResult inner = run(g, "MATCH ((x:N)-[e:R]->(y:N) WHERE e.w > 0.4){2} RETURN x, y");
        assertEquals(1, inner.rows().size(), inner.rows().toString());
        assertEquals(List.of("a", "c"), ((List<?>) inner.rows().get(0).get(0)).stream().map(v -> ((Map<?, ?>) v).get("id")).toList());
        assertEquals(List.of("c", "d"), ((List<?>) inner.rows().get(0).get(1)).stream().map(v -> ((Map<?, ?>) v).get("id")).toList());
    }

    @Test
    void SHORTEST_k取前k条而不只是最短的一层() {
        Graph g = sample();
        QueryResult two = run(g, "MATCH p = SHORTEST 2 (s:N {id: 'a'})-[:R]->{1,3}(t:N {id: 'd'}) RETURN path_length(p) AS l ORDER BY l");
        assertEquals(List.of(2L, 3L), column(two, 0), "a→c→d 两跳、a→b→c→d 三跳");
        QueryResult any = run(g, "MATCH p = ANY (s:N {id: 'a'})-[:R]->{1,3}(t:N {id: 'd'}) RETURN path_length(p)");
        assertEquals(1, any.rows().size());
        QueryResult groups = run(g, "MATCH p = SHORTEST 1 GROUP (s:N {id: 'a'})-[:R]->{1,3}(t:N {id: 'd'}) RETURN path_length(p)");
        assertEquals(List.of(2L), column(groups, 0));
        QueryResult unbounded = run(g, "MATCH p = ANY SHORTEST (s:N {id: 'a'})-[:R]->*(t:N {id: 'd'}) RETURN path_length(p)");
        assertEquals(List.of(2L), column(unbounded, 0), "最短路不写上界也能停");
    }

    @Test
    void 管道中间的排序截取和FOR序号() {
        Graph g = sample();
        assertEquals(List.of("d", "c"), column(run(g, "MATCH (n:N) ORDER BY n.id DESC LIMIT 2 RETURN n.id"), 0));
        assertEquals(List.of("b"), column(run(g, "MATCH (n:N) ORDER BY n.id OFFSET 1 LIMIT 1 RETURN n.id"), 0));
        QueryResult ordinality = run(g, "FOR x IN ['p', 'q'] WITH ORDINALITY i RETURN x, i");
        assertEquals(List.of(1L, 2L), column(ordinality, 1));
        assertEquals(List.of(0L, 1L), column(run(g, "FOR x IN ['p', 'q'] WITH OFFSET i RETURN i"), 0));
        assertEquals(List.of(3L), column(run(g, "LET a = 1, b = a + 2 RETURN b"), 0));
    }

    @Test
    void 标准谓词和函数() {
        Graph g = sample();
        QueryResult result = run(g, "MATCH (a:N {id: 'a'})-[e:R]->(b:N {id: 'b'}) RETURN "
                + "SAME(a, a), ALL_DIFFERENT(a, b), PROPERTY_EXISTS(a, name), PROPERTY_EXISTS(a, nope), a = b, a <> b, "
                + "e IS DIRECTED, NULLIF(1, 1), LET x = 2, y = x * 3 IN y END, TRIM(BOTH 'x' FROM 'xxhixx'), "
                + "TRIM(LEADING FROM '  hi'), null IS UNKNOWN, (1 = 1) IS TRUE, a IS SOURCE OF e, LOG(10, 1000), CEILING(1.2), "
                + "CARDINALITY([1, 2, 3]), BYTE_LENGTH('层'), a:N, LIST[1, 2], RECORD {k: 1}");
        List<Object> row = result.rows().get(0);
        assertEquals(Arrays.asList(true, true, true, false, false, true, true, null, 6L, "hi", "hi", true, true, true), row.subList(0, 14));
        assertEquals(3.0, (Double) row.get(14), 1e-9);
        assertEquals(2.0, row.get(15));
        assertEquals(3L, row.get(16));
        assertEquals(3L, row.get(17));
        assertEquals(true, row.get(18));
        assertEquals(List.of(1L, 2L), row.get(19));
        assertEquals(Map.of("k", 1L), row.get(20));
        // a OR b XOR c 是 (a OR b) XOR c
        assertEquals(List.of(false), column(run(g, "RETURN true OR false XOR true"), 0));
    }

    @Test
    void 不读图的查询可以做集合运算和NEXT() throws Exception {
        RemoteQueryEngine engine = engine(new Graph());
        assertEquals(List.of(1L, 2L), column(engine.execute("q1", null, "RETURN 1 AS v UNION RETURN 2 AS v ORDER BY v", Map.of(), () -> false), 0));
        assertEquals(List.of(1L), column(engine(new Graph()).execute("q2", null, "RETURN 1 AS v OTHERWISE RETURN 2 AS v", Map.of(), () -> false), 0));
        assertEquals(List.of(2L), column(engine(new Graph()).execute("q3", null,
                "FOR v IN [] RETURN v OTHERWISE RETURN 2 AS v", Map.of(), () -> false), 0));
        QueryResult next = engine(new Graph()).execute("q4", null, "RETURN 1 AS a, 2 AS b NEXT YIELD b AS c RETURN c + 1 AS d", Map.of(), () -> false);
        assertEquals(List.of("d"), next.columns());
        assertEquals(List.of(3L), column(next, 0));
        assertEquals(List.of(4L), column(engine(new Graph()).execute("q5", null,
                "RETURN 2 AS x NEXT FILTER x > 1 LET y = x * 2 RETURN y", Map.of(), () -> false), 0));
        assertTrue(engine(new Graph()).execute("q6", null, "RETURN 1 AS x NEXT FINISH", Map.of(), () -> false).columns().isEmpty());
    }
}
