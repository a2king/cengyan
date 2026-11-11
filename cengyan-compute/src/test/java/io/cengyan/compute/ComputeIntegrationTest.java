package io.cengyan.compute;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.storage.*;
import io.cengyan.raft.RaftSettings;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.net.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class ComputeIntegrationTest {
    @TempDir Path home;
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test @Timeout(120)
    void 外部登录建图批量写入远程查询和权限隔离() throws Exception {
        List<ServerSocket> reserved = new ArrayList<>(); int[] ports = new int[5];
        try { for (int i = 0; i < ports.length; i++) { var socket = new ServerSocket(0); reserved.add(socket); ports[i] = socket.getLocalPort(); } }
        finally { for (var socket : reserved) socket.close(); }
        var storage = new StorageConfig("integration", "s1", address(ports[0]), address(ports[1]), home.resolve("storage"),
                List.of(RaftPeer.newBuilder().setId("s1").setAddress(address(ports[1])).setClientAddress(address(ports[0])).build()), RaftSettings.defaults(),
                16 * 1024 * 1024, 128, 4, 128);
        var compute = new ComputeConfig("integration", "c1", address(ports[2]), address(ports[3]), address(ports[4]), home.resolve("compute"),
                List.of(new ComputeConfig.Peer("c1", address(ports[3]), address(ports[4]), address(ports[2]))), List.of(address(ports[0])),
                2, 32, 10000, 60000, 64L * 1024 * 1024, 128L * 1024 * 1024, 1024L * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                10000, 1024 * 1024, "secret");
        java.nio.file.Files.createDirectories(storage.directory());
        try (var storageServer = new StorageServer(storage); var computeServer = new ComputeServer(compute); var socket = new Socket("127.0.0.1", ports[2])) {
            socket.setSoTimeout(60000);
            var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            JsonNode denied = call(writer, reader, JSON.createObjectNode().put("op", "execute").put("query", "SHOW GRAPHS"));
            assertEquals("UNAUTHENTICATED", denied.path("code").asText(), denied.toString());
            JsonNode login = call(writer, reader, JSON.createObjectNode().put("op", "login").put("user", "root").put("password", "secret"));
            assertEquals("OK", login.path("code").asText(), login.toString());
            String token = login.path("token").asText();
            assertFalse(token.isBlank());
            JsonNode nodes = call(writer, reader, request(token, "admin").put("command", "showNodes").put("graph", ""));
            ok(nodes); assertEquals(2, nodes.path("rows").size());
            assertEquals(List.of("nodeId", "role", "state", "address", "healthy"), JSON.convertValue(nodes.path("columns"), List.class));
            Set<String> roles = new HashSet<>();
            for (JsonNode row : nodes.path("rows")) { roles.add(row.get(1).asText()); assertEquals("ACTIVE", row.get(2).asText()); }
            assertEquals(Set.of("STORAGE", "COMPUTE"), roles);
            JsonNode badNode = call(writer, reader, request(token, "admin").put("command", "drainCompute").put("nodeId", "not-a-member"));
            assertEquals("NODE_NOT_FOUND", badNode.path("code").asText());
            ObjectNode create = request(token, "createGraph").put("name", "Company").put("partitions", 3).put("vertexIdType", "STRING").put("edgeIdType", "STRING");
            ok(call(writer, reader, create));
            ok(call(writer, reader, query(token, "CREATE NODE TYPE Company COMMENT '企业节点' ID STRING(36) (name STRING NULL COMMENT '名称')")));
            ok(call(writer, reader, query(token, "CREATE REL TYPE holder ()")));
            JsonNode nodeTypes = call(writer, reader, query(token, "SHOW NODE TYPES"));
            ok(nodeTypes);
            assertEquals(List.of("name", "id", "schemaVersion", "idType", "idLength", "comment"), JSON.convertValue(nodeTypes.path("columns"), List.class));
            assertEquals("STRING", nodeTypes.path("rows").get(0).get(3).asText());
            assertEquals(36, nodeTypes.path("rows").get(0).get(4).asInt());
            assertEquals("企业节点", nodeTypes.path("rows").get(0).get(5).asText());
            JsonNode description = call(writer, reader, query(token, "DESCRIBE NODE TYPE Company"));
            ok(description);
            assertEquals(List.of("property", "type", "nullable", "comment"), JSON.convertValue(description.path("columns"), List.class));
            assertEquals("name", description.path("rows").get(0).get(0).asText());
            assertEquals("名称", description.path("rows").get(0).get(3).asText());
            ObjectNode bulk = request(token, "bulkInsert").put("kind", "node").put("type", "Company");
            bulk.putArray("rows").addObject().put("id", "a").put("name", "Alice");
            bulk.withArray("rows").addObject().put("id", "b").put("name", "Bob");
            ok(call(writer, reader, bulk));
            ObjectNode edges = request(token, "bulkInsert").put("kind", "edge").put("type", "holder");
            edges.putArray("rows").addObject().put("id", "e1").put("src", "a").put("dst", "b");
            ok(call(writer, reader, edges));
            JsonNode stats = call(writer, reader, request(token, "admin").put("command", "stats").put("refresh", false));
            ok(stats);
            assertEquals("DATA_GROUP_COUNT_STORE", stats.path("statisticsSource").asText());
            assertEquals(2, stats.path("rows").get(0).get(2).asInt());
            assertEquals(1, stats.path("rows").get(1).get(2).asInt());
            JsonNode result = call(writer, reader, query(token, "MATCH (a:Company {id:'a'})-[e:holder]->(b:Company) RETURN b.id, b.name"));
            ok(result);
            assertEquals("b", result.path("rows").get(0).get(0).asText());
            assertEquals("Bob", result.path("rows").get(0).get(1).asText());
            JsonNode count = call(writer, reader, query(token, "MATCH (a:Company) RETURN count(a.id)"));
            ok(count); assertEquals(2, count.path("rows").get(0).get(0).asInt());
            ObjectNode increment = query(token, "MATCH (a:Company {id:'b'}) SET a.name='Changed'");
            ok(call(writer, reader, increment));
            ok(call(writer, reader, increment));
            ok(call(writer, reader, query(token, "CREATE INDEX by_name ON NODE TYPE Company (name)")));
            JsonNode indexed = call(writer, reader, query(token, "MATCH (a:Company {name:'Changed'}) RETURN a.id"));
            ok(indexed); assertEquals("b", indexed.path("rows").get(0).get(0).asText());
            ok(call(writer, reader, query(token, "DROP INDEX by_name")));
            ok(call(writer, reader, query(token, "CREATE INDEX by_name ON NODE TYPE Company (name)")));
            JsonNode rebuilt = call(writer, reader, query(token, "MATCH (a:Company {name:'Changed'}) RETURN a.id"));
            ok(rebuilt); assertEquals(1, rebuilt.path("rows").size());
            JsonNode ordered = call(writer, reader, query(token, "MATCH (a:Company) RETURN a.name, count(a.id) AS total ORDER BY total DESC, a.name"));
            ok(ordered); assertEquals(2, ordered.path("rows").size());
            JsonNode aliasOrder = call(writer, reader, query(token, "MATCH (a:Company) RETURN a.name AS name ORDER BY name DESC LIMIT 1"));
            ok(aliasOrder); assertEquals("Changed", aliasOrder.path("rows").get(0).get(0).asText());
            JsonNode discovery = call(writer, reader, request(token, "members"));
            ok(discovery); assertEquals("127.0.0.1", discovery.path("addresses").get(0).path("host").asText());
            JsonNode multiple = call(writer, reader, query(token, "MATCH (a:Company {id:'a'})-[:holder]->(b:Company), (b)-[:holder]-(c:Company) RETURN c.id"));
            ok(multiple); assertEquals("a", multiple.path("rows").get(0).get(0).asText());
            JsonNode scalar = call(writer, reader, request(token, "execute").put("graph", "").put("query", "RETURN 1+2 AS answer"));
            ok(scalar); assertEquals(3, scalar.path("rows").get(0).get(0).asInt());
            ObjectNode ordinary = query(token, "INSERT (a {id:'b'})-[:holder {id:'e2'}]->(b {id:'a'})");
            ok(call(writer, reader, ordinary));
            ok(call(writer, reader, ordinary));
            JsonNode repeated = call(writer, reader, query(token, "MATCH (a:Company {id:'a'})-[:holder]->{2}(b:Company) RETURN b.id"));
            ok(repeated); assertEquals("a", repeated.path("rows").get(0).get(0).asText());
            JsonNode shortest = call(writer, reader, query(token, "CALL path.shortest({start:'a', end:'b', rel:'holder', hops:3}) YIELD length, vids"));
            ok(shortest); assertEquals(1, shortest.path("rows").get(0).get(0).asInt());
            JsonNode bounded = call(writer, reader, query(token, "CALL subgraph.bounded({start:'a', rel:'holder', hops:3}) YIELD vid"));
            ok(bounded); assertEquals(2, bounded.path("rows").size());
            JsonNode path = call(writer, reader, query(token, "MATCH p = ANY SHORTEST (a:Company {id:'a'})-[:holder]->{1,3}(b:Company {id:'b'}) RETURN p"));
            ok(path); assertEquals(1, path.path("rows").get(0).get(0).path("length").asInt());
            JsonNode trail = call(writer, reader, query(token, "MATCH TRAIL (a:Company {id:'a'})-[:holder]->{2}(b:Company) RETURN b.id"));
            ok(trail); assertEquals("a", trail.path("rows").get(0).get(0).asText());
            // GQL regressions run through the real Compute/Storage path, not the removed local QueryEngine.
            JsonNode optional = call(writer, reader, query(token, "MATCH (a:Company {id:'a'}) OPTIONAL MATCH (a)-[:holder]->(b:Company {id:'missing'}) RETURN a.id, b.id"));
            ok(optional); assertEquals("a", optional.path("rows").get(0).get(0).asText());
            assertTrue(optional.path("rows").get(0).get(1).isNull());
            JsonNode union = call(writer, reader, query(token, "MATCH (a:Company {id:'a'}) RETURN a.id AS id UNION MATCH (a:Company {id:'b'}) RETURN a.id AS id"));
            ok(union); assertEquals(2, union.path("rows").size());
            JsonNode expression = call(writer, reader, query(token, "MATCH (a:Company {id:'a'}) RETURN CASE WHEN a.name = 'Alice' THEN 'yes' ELSE 'no' END AS answer"));
            ok(expression); assertEquals("yes", expression.path("rows").get(0).get(0).asText());
            ok(call(writer, reader, request(token, "admin").put("command", "createUser").put("user", "reader").put("password", "reader-secret").put("role", "read")));
            JsonNode readerLogin = call(writer, reader, JSON.createObjectNode().put("op", "login").put("user", "reader").put("password", "reader-secret"));
            ok(readerLogin);
            String readerToken = readerLogin.path("token").asText();
            ok(call(writer, reader, request(readerToken, "admin").put("command", "showNodes").put("graph", "")));
            assertEquals("FORBIDDEN", call(writer, reader, request(readerToken, "admin").put("command", "drainCompute").put("nodeId", "c1")).path("code").asText());
            JsonNode forbidden = call(writer, reader, query(readerToken, "INSERT (n:Company {id:'forbidden', name:'no'})"));
            assertEquals("FORBIDDEN", forbidden.path("code").asText(), forbidden.toString());
            ok(call(writer, reader, request(token, "admin").put("command", "setRole").put("user", "reader").put("role", "write")));
            assertEquals("UNAUTHENTICATED", call(writer, reader, query(readerToken, "SHOW GRAPHS")).path("code").asText());
            ok(call(writer, reader, request(token, "createGraph").put("name", "LargeBulk").put("partitions", 32)
                    .put("vertexIdType", "STRING").put("edgeIdType", "STRING")));
            ok(call(writer, reader, query(token, "CREATE NODE TYPE N ID STRING(64) (body STRING NULL)").put("graph", "LargeBulk")));
            ObjectNode large = request(token, "bulkInsert").put("graph", "LargeBulk").put("kind", "node").put("type", "N")
                    .put("requestId", "org-v2-" + "b".repeat(64)).put("timeoutMs", 60000);
            var largeRows = large.putArray("rows");
            for (int i = 0; i < 5000; i++) largeRows.addObject().put("id", "large-" + i).put("body", "x".repeat(740));
            assertTrue(JSON.writeValueAsBytes(large).length < 4 * 1024 * 1024);
            JsonNode largeWritten = call(writer, reader, large);
            ok(largeWritten); assertEquals(5000, largeWritten.path("rowsAffected").asInt());
            ok(call(writer, reader, large));
            try (var files = java.nio.file.Files.list(home.resolve("compute/requests"))) { assertFalse(files.findAny().isPresent()); }
        }
    }
    private static InetSocketAddress address(int port) { return new InetSocketAddress("127.0.0.1", port); }
    private static ObjectNode request(String token, String op) { return JSON.createObjectNode().put("op", op).put("token", token).put("graph", "Company").put("requestId", UUID.randomUUID().toString()); }
    private static ObjectNode query(String token, String gql) { return request(token, "execute").put("query", gql); }
    private static JsonNode call(BufferedWriter writer, BufferedReader reader, ObjectNode request) throws Exception {
        writer.write(request.toString()); writer.newLine(); writer.flush(); String line = reader.readLine(); assertNotNull(line); return JSON.readTree(line);
    }
    private static void ok(JsonNode response) { assertEquals("OK", response.path("code").asText(), response.toString()); }
}
