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

/** 标准写法经过真实 Storage：标准建图、默认可空、写入后 RETURN、REMOVE 属性、重试取回结果、USE 前缀。 */
class ComputeGqlStandardTest {
    @TempDir Path home;
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test @Timeout(180)
    void 标准写法端到端() throws Exception {
        List<ServerSocket> reserved = new ArrayList<>(); int[] ports = new int[5];
        try { for (int i = 0; i < ports.length; i++) { var socket = new ServerSocket(0); reserved.add(socket); ports[i] = socket.getLocalPort(); } }
        finally { for (var socket : reserved) socket.close(); }
        var storage = new StorageConfig("gqlstd", "s1", address(ports[0]), address(ports[1]), home.resolve("storage"),
                List.of(RaftPeer.newBuilder().setId("s1").setAddress(address(ports[1])).setClientAddress(address(ports[0])).build()), RaftSettings.defaults(),
                16 * 1024 * 1024, 128, 4, 128);
        var compute = new ComputeConfig("gqlstd", "c1", address(ports[2]), address(ports[3]), address(ports[4]), home.resolve("compute"),
                List.of(new ComputeConfig.Peer("c1", address(ports[3]), address(ports[4]), address(ports[2]))), List.of(address(ports[0])),
                2, 32, 10000, 60000, 64L * 1024 * 1024, 128L * 1024 * 1024, 1024L * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                10000, 1024 * 1024, "secret");
        java.nio.file.Files.createDirectories(storage.directory());
        try (var storageServer = new StorageServer(storage); var computeServer = new ComputeServer(compute); var socket = new Socket("127.0.0.1", ports[2])) {
            socket.setSoTimeout(60000);
            var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            JsonNode login = call(writer, reader, JSON.createObjectNode().put("op", "login").put("user", "root").put("password", "secret"));
            ok(login);
            String token = login.path("token").asText();

            ok(call(writer, reader, query(token, "", """
                    CREATE GRAPH 企业 {
                      (:公司 {名称 STRING NOT NULL, 备注 STRING, 注册资本 DOUBLE}),
                      (:公司)-[:持股 {比例 DOUBLE}]->(:公司)
                    } ID STRING(64)""")));
            JsonNode described = call(writer, reader, query(token, "企业", "DESCRIBE NODE TYPE 公司"));
            ok(described);
            Map<String, Boolean> nullable = new HashMap<>();
            for (JsonNode row : described.path("rows")) nullable.put(row.get(0).asText(), row.get(2).asBoolean());
            assertEquals(Map.of("名称", false, "备注", true, "注册资本", true), nullable, "默认可空，NOT NULL 必填");

            JsonNode missing = call(writer, reader, query(token, "企业", "INSERT (:公司 {id: 'x'})"));
            assertEquals("TYPE_MISMATCH", missing.path("code").asText(), missing.toString());

            JsonNode inserted = call(writer, reader, query(token, "企业",
                    "INSERT (a:公司 {id: 'a', 名称: '甲', 备注: 'n'})-[e:持股 {id: 'e1', 比例: 0.6}]->(b:公司 {id: 'b', 名称: '乙'}) RETURN a.名称 AS 甲方, e.比例 AS 比例"));
            ok(inserted);
            assertEquals("甲", inserted.path("rows").get(0).get(0).asText(), inserted.toString());
            assertEquals(0.6, inserted.path("rows").get(0).get(1).asDouble(), 1e-9);

            ObjectNode update = query(token, "企业", "MATCH (a:公司 {id: 'a'}) SET a.注册资本 = 100.0 REMOVE a.备注 RETURN a.注册资本 AS 资本, a.备注 AS 备注");
            JsonNode updated = call(writer, reader, update);
            ok(updated);
            assertEquals(100.0, updated.path("rows").get(0).get(0).asDouble(), 1e-9, updated.toString());
            assertTrue(updated.path("rows").get(0).get(1).isNull(), updated.toString());
            // 同一个 requestId 重试：不重做写入，返回第一次的结果。
            JsonNode retried = call(writer, reader, update);
            ok(retried);
            assertEquals(updated.path("rows"), retried.path("rows"), retried.toString());

            JsonNode read = call(writer, reader, query(token, "企业", "MATCH (a:公司 {id: 'a'}) RETURN a.备注 IS NULL AS 删掉了, a.注册资本 AS 资本"));
            ok(read);
            assertTrue(read.path("rows").get(0).get(0).asBoolean(), read.toString());

            // USE 前缀：请求本身不带图，也能查。
            JsonNode used = call(writer, reader, query(token, "", "USE 企业 MATCH (a:公司)-[:持股]->(b) RETURN b.名称"));
            ok(used);
            assertEquals("乙", used.path("rows").get(0).get(0).asText(), used.toString());

            JsonNode exists = call(writer, reader, query(token, "企业",
                    "MATCH (n:公司) WHERE NOT EXISTS { (n)-[:持股]->() } RETURN n.名称 -- 注释"));
            ok(exists);
            assertEquals("乙", exists.path("rows").get(0).get(0).asText(), exists.toString());

            JsonNode deleted = call(writer, reader, query(token, "企业", "MATCH (b:公司 {id: 'b'}) DETACH DELETE b FINISH"));
            ok(deleted);
            JsonNode left = call(writer, reader, query(token, "企业", "MATCH (n:公司) RETURN count(*) AS n"));
            ok(left);
            assertEquals(1, left.path("rows").get(0).get(0).asInt(), left.toString());
        }
    }

    private static InetSocketAddress address(int port) { return new InetSocketAddress("127.0.0.1", port); }
    private static ObjectNode query(String token, String graph, String gql) {
        return JSON.createObjectNode().put("op", "execute").put("token", token).put("graph", graph)
                .put("requestId", UUID.randomUUID().toString()).put("query", gql);
    }
    private static JsonNode call(BufferedWriter writer, BufferedReader reader, ObjectNode request) throws Exception {
        writer.write(request.toString()); writer.newLine(); writer.flush(); String line = reader.readLine(); assertNotNull(line); return JSON.readTree(line);
    }
    private static void ok(JsonNode response) { assertEquals("OK", response.path("code").asText(), response.toString()); }
}
