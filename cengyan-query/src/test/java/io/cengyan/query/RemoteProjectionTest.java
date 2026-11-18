package io.cengyan.query;

import io.cengyan.distributed.StorageGateway;
import io.cengyan.parser.Expr;
import io.cengyan.parser.Gql;
import io.cengyan.parser.Stmt;
import io.cengyan.protocol.proto.ClusterProtocol.Entity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** RETURN 投影：聚合识别、GROUP BY 校验、FILTER 与 LIMIT 的先后。不连存储，直接调用投影。 */
class RemoteProjectionTest {
    @TempDir Path work;

    private static Stmt.Tail tail(String gql) {
        return ((Stmt.Select) Gql.parse(gql)).tail();
    }

    @Test
    void collectList和统计函数都算聚合() {
        for (String name : List.of("count", "sum", "collect_list", "stddev_pop", "percentile_cont")) {
            assertTrue(RemoteQueryEngine.isAggregate(new Expr.Call(name, List.of(new Expr.Variable("x")))), name);
        }
        assertFalse(RemoteQueryEngine.isAggregate(new Expr.Call("id", List.of(new Expr.Variable("x")))));
    }

    @Test
    void groupBy可以写变量别名或返回项本身() {
        String head = "MATCH (s)-[:inv]->(t) RETURN id(s) AS sid, collect_list(t) AS ts, sum(t.v) AS total GROUP BY ";
        for (String ok : List.of("s", "sid", "id(s)")) {
            Stmt.Tail t = tail(head + ok);
            assertTrue(RemoteQueryEngine.groupsMatch(t.items(), t.groupBy()), ok);
        }
        for (String bad : List.of("t", "s, t", "total")) {
            Stmt.Tail t = tail(head + bad);
            assertFalse(RemoteQueryEngine.groupsMatch(t.items(), t.groupBy()), bad);
        }
        Stmt.Tail two = tail("MATCH (s)-[:inv]->(t) RETURN s AS start_node, t AS end_node, count(*) GROUP BY t");
        assertTrue(RemoteQueryEngine.groupsMatch(two.items(), two.groupBy()), "返回了但没写进 GROUP BY 的值也参与分组");
    }

    @Test
    void 分组后的FILTER在LIMIT之前生效并保留节点结构() throws Exception {
        Stmt.Tail t = tail("MATCH (s)-[:inv]->(t) RETURN k, collect_list(n) AS nodes, sum(v) AS total "
                + "GROUP BY k FILTER total >= 0.25 ORDER BY k LIMIT 2");
        RemoteQueryEngine engine = new RemoteQueryEngine(
                new StorageGateway("test", List.of(new InetSocketAddress("127.0.0.1", 1))), work, "64MB", "1GB", 1);
        Method scalar = RemoteQueryEngine.class.getDeclaredMethod("scalar", Object.class);
        scalar.setAccessible(true);
        Object[][] input = {{"a", 0.2, "n1"}, {"b", 0.1, "n2"}, {"a", 0.1, "n3"}, {"c", 0.5, "n4"}};
        List<Map<String, Entity>> bindings = new ArrayList<>();
        for (Object[] row : input) {
            Map<String, Entity> binding = new LinkedHashMap<>();
            binding.put("k", (Entity) scalar.invoke(engine, row[0]));
            binding.put("v", (Entity) scalar.invoke(engine, row[1]));
            binding.put("n", (Entity) scalar.invoke(engine, Map.of("id", row[2], "type", "org")));
            bindings.add(binding);
        }
        Method project = RemoteQueryEngine.class.getDeclaredMethod("project", List.class, List.class, List.class,
                boolean.class, Expr.class, Expr.class, Expr.class, ExprEval.class, BooleanSupplier.class);
        project.setAccessible(true);
        QueryResult result;
        try (var memory = QueryMemory.open(64L << 20)) {
            BooleanSupplier never = () -> false;
            result = (QueryResult) project.invoke(engine, bindings, t.items(), t.orderBy(), t.distinct(), t.postFilter(),
                    t.skip(), t.limit(), new ExprEval(Map.of(), false), never);
        }
        assertEquals(List.of("k", "nodes", "total"), result.columns());
        assertEquals(2, result.rows().size(), "b 被过滤后，LIMIT 2 应该拿到 a 和 c");
        assertEquals("a", result.rows().get(0).get(0));
        assertEquals(0.3, ((Number) result.rows().get(0).get(2)).doubleValue(), 1e-9);
        assertEquals(List.of(Map.of("id", "n1", "type", "org"), Map.of("id", "n3", "type", "org")), result.rows().get(0).get(1));
        assertEquals("c", result.rows().get(1).get(0));
    }
}
