package com.cengyan.core.smoke;

import com.cengyan.core.BulkOptions;
import com.cengyan.core.BulkProgress;
import com.cengyan.core.ClusterClient;
import com.cengyan.core.ClusterGraph;
import com.cengyan.core.GraphEdge;
import com.cengyan.core.GraphNode;
import com.cengyan.core.IdType;
import com.cengyan.core.Record;
import com.cengyan.core.Result;
import com.cengyan.core.exception.GqlSyntaxException;
import com.cengyan.core.mapping.CengyanId;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 对真实 Compute 的冒烟：建图、建类型、POJO/Map 批量写点（分片对齐）、写边、查询、POJO 映射、错误类型。
 * 不在 {@code mvn test} 里运行；由 smoke/run-smoke.sh 启动真实 Storage + Compute 后调用。
 *
 * <p>用法：java -cp &lt;sdk jar&gt;:&lt;test-classes&gt; com.cengyan.core.smoke.ClusterSmoke host:port password
 */
public final class ClusterSmoke {
    static final class Person {
        @CengyanId
        String key;
        String name;
        Long age;
        Instant born;
    }

    public static void main(String[] args) {
        try (ClusterClient client = ClusterClient.builder().addresses(args[0]).user("root").password(args[1]).build()) {
            client.createGraph("Smoke", 3, 1, IdType.STRING, IdType.STRING);
            ClusterGraph graph = client.graph("Smoke");
            graph.write("CREATE NODE TYPE N (name STRING NULL, age INT64 NULL, born DATETIME NULL)");
            graph.write("CREATE REL TYPE E (weight DOUBLE NULL)");

            List<Person> people = new ArrayList<Person>();
            for (int i = 0; i < 50; i++) {
                Person person = new Person();
                person.key = "p" + i;
                person.name = "名字" + i;
                person.age = (long) i;
                person.born = Instant.parse("2000-01-01T00:00:00Z").plusSeconds(i * 86400L);
                people.add(person);
            }
            BulkProgress nodes = graph.insertNodes("N", people,
                    BulkOptions.builder().batchSize(7).requestId((index, rows) -> "smoke-n-" + index).build());
            check(nodes.confirmed() == 50, "nodes " + nodes);

            List<Map<String, Object>> edges = new ArrayList<Map<String, Object>>();
            edges.add(edge("e1", "p0", "p1", 0.5));
            edges.add(edge("e2", "p1", "p2", 1.5));
            edges.add(edge("skip", "missing", "p2", 2.0));
            BulkProgress written = graph.insertEdges("E", edges, BulkOptions.builder().skipMissingEndpoints(true).build());
            check(written.confirmed() == 2 && written.skipped() == 1, "edges " + written);

            Map<String, Object> params = new HashMap<String, Object>();
            params.put("min", 45L);
            Result older = graph.read("MATCH (n:N) WHERE n.age >= $min RETURN n.name AS name, n.age AS age, n.born AS born ORDER BY n.age", params);
            check(older.size() == 5, "params query " + older.rows());
            Record first = older.first();
            check("名字45".equals(first.getString("name")) && first.getLong("age") == 45L, "first " + first);
            check(Instant.parse("2000-02-15T00:00:00Z").equals(first.getInstant("born")), "datetime " + first.get("born"));

            Person mapped = graph.read("MATCH (n:N {id: 'p3'}) RETURN n").one(Person.class);
            check("p3".equals(mapped.key) && "名字3".equals(mapped.name) && mapped.age == 3L, "pojo " + mapped.key + " " + mapped.name);
            GraphNode node = graph.read("MATCH (n:N {id: 'p3'}) RETURN n").one().getNode("n");
            check("N".equals(node.type()) && "p3".equals(node.id()), "node " + node);

            Record hop = graph.read("MATCH (a:N {id:'p0'})-[e:E]->(b:N) RETURN e, b.name AS name").one();
            GraphEdge edge = hop.getEdge("e");
            check("p0".equals(edge.src()) && "p1".equals(edge.dst()) && Double.valueOf(0.5).equals(edge.property("weight")), "edge " + edge);
            check("名字1".equals(hop.getString("name")), "hop " + hop);

            try {
                graph.read("MATCH (n:N RETURN n");
                check(false, "语法错误应抛 GqlSyntaxException");
            } catch (GqlSyntaxException expected) {
                // 期望
            }
            check(client.refreshNodes().size() == 1, "refreshNodes");
            check(graph.read("MATCH (n:N) RETURN count(n) AS c").one().getLong("c") == 50L, "count");
            System.out.println("{\"status\":\"OK\",\"nodes\":\"" + nodes + "\",\"edges\":\"" + written + "\",\"java\":\""
                    + System.getProperty("java.version") + "\"}");
        }
    }

    private static Map<String, Object> edge(String id, String src, String dst, double weight) {
        Map<String, Object> row = new HashMap<String, Object>();
        row.put("id", id);
        row.put("src", src);
        row.put("dst", dst);
        row.put("weight", weight);
        return row;
    }

    private static void check(boolean condition, String message) {
        if (!condition) throw new AssertionError("冒烟失败：" + message);
    }

    private ClusterSmoke() {
    }
}
