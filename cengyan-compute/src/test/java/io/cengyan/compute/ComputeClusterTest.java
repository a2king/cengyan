package io.cengyan.compute;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.raft.RaftSettings;
import io.cengyan.storage.*;
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

class ComputeClusterTest {
    @TempDir Path home;
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test @Timeout(120)
    void 相同令牌跨计算节点并在入口退出后访问其它节点() throws Exception {
        int[] ports = new int[11];
        List<ServerSocket> held = new ArrayList<>();
        try { for (int i = 0; i < ports.length; i++) { var socket = new ServerSocket(0); held.add(socket); ports[i] = socket.getLocalPort(); } }
        finally { for (var socket : held) socket.close(); }
        var storageConfig = new StorageConfig("compute-cluster", "s1", addr(ports[0]), addr(ports[1]), home.resolve("storage"),
                List.of(RaftPeer.newBuilder().setId("s1").setAddress(addr(ports[1])).setClientAddress(addr(ports[0])).build()),
                RaftSettings.defaults(), 8 * 1024 * 1024, 128, 4, 128);
        List<ComputeConfig.Peer> members = new ArrayList<>();
        for (int i = 0; i < 3; i++) members.add(new ComputeConfig.Peer("c" + i, addr(ports[3 + i * 3]), addr(ports[4 + i * 3]), addr(ports[2 + i * 3])));
        List<ComputeServer> nodes = new ArrayList<>();
        try (var storage = new StorageServer(storageConfig)) {
            for (int i = 0; i < 3; i++) {
                var peer = members.get(i);
                nodes.add(new ComputeServer(new ComputeConfig("compute-cluster", peer.id(), peer.client(), peer.rpc(), peer.raft(),
                        home.resolve(peer.id()), members, List.of(addr(ports[0])), 2, 32, 10000, 60000,
                        64L * 1024 * 1024, 128L * 1024 * 1024, 1024L * 1024 * 1024, 2L * 1024 * 1024 * 1024,
                        10000, 1024 * 1024, "secret")));
            }
            JsonNode login = call(ports[2], JSON.createObjectNode().put("op", "login").put("user", "root").put("password", "secret"));
            ok(login); String token = login.path("token").asText();
            JsonNode create = call(ports[2], request(token, "createGraph").put("name", "G").put("partitions", 2).put("vertexIdType", "STRING").put("edgeIdType", "STRING"));
            ok(create);
            ok(call(ports[5], request(token, "execute").put("graph", "G").put("query", "CREATE NODE TYPE N (name STRING NULL)")));
            ObjectNode bulk = request(token, "bulkInsert").put("graph", "G").put("kind", "node").put("type", "N");
            bulk.putArray("rows").addObject().put("id", "a").put("name", "alive");
            ok(call(ports[8], bulk));
            nodes.get(0).close(); nodes.set(0, null);
            JsonNode result = call(ports[5], request(token, "execute").put("graph", "G").put("query", "MATCH (n:N {id:'a'}) RETURN n.name"));
            ok(result); assertEquals("alive", result.path("rows").get(0).get(0).asText());
            JsonNode discovered = call(ports[8], request(token, "members"));
            ok(discovered); assertEquals(3, discovered.path("addresses").size());
        } finally { for (var node : nodes) if (node != null) node.close(); }
    }
    private static InetSocketAddress addr(int port) { return new InetSocketAddress("127.0.0.1", port); }
    private static ObjectNode request(String token, String op) {
        return JSON.createObjectNode().put("op", op).put("token", token).put("requestId", UUID.randomUUID().toString());
    }
    private static JsonNode call(int port, ObjectNode request) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(60000);
            var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer.write(request.toString()); writer.newLine(); writer.flush(); String line = reader.readLine(); assertNotNull(line); return JSON.readTree(line);
        }
    }
    private static void ok(JsonNode response) { assertEquals("OK", response.path("code").asText(), response.toString()); }
}
