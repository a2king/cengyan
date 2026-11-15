package io.cengyan.compute;

import com.fasterxml.jackson.databind.*;
import io.cengyan.raft.RaftSettings;
import io.cengyan.storage.*;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class ComputeJoinTest {
    @TempDir Path home;
    private static final ObjectMapper JSON = new ObjectMapper();
    @Test @Timeout(90)
    void 新计算节点从种子加入后成员列表包含新入口() throws Exception {
        int[] ports = new int[8]; List<ServerSocket> held = new ArrayList<>();
        try { for (int i = 0; i < ports.length; i++) { var socket = new ServerSocket(0); held.add(socket); ports[i] = socket.getLocalPort(); } }
        finally { for (var socket : held) socket.close(); }
        var storage = new StorageConfig("compute-join", "s1", addr(ports[0]), addr(ports[1]), home.resolve("storage"),
                List.of(RaftPeer.newBuilder().setId("s1").setAddress(addr(ports[1])).setClientAddress(addr(ports[0])).build()), RaftSettings.defaults(), 8*1024*1024, 128, 4, 128);
        var seed = new ComputeConfig.Peer("c1", addr(ports[3]), addr(ports[4]), addr(ports[2]));
        var next = new ComputeConfig.Peer("c2", addr(ports[6]), addr(ports[7]), addr(ports[5]));
        try (var s1 = new StorageServer(storage); var c1 = new ComputeServer(config(seed, List.of(seed), ports[0]));
             var c2 = new ComputeServer(config(next, List.of(seed), ports[0]))) {
            String token = call(ports[2], "{\"op\":\"login\",\"user\":\"root\",\"password\":\"secret\"}").path("token").asText();
            assertFalse(token.isBlank());
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            JsonNode members;
            do {
                members = call(ports[2], JSON.createObjectNode().put("op", "members").put("token", token).toString());
                if (members.path("addresses").size() == 2) break;
                Thread.sleep(50);
            } while (System.nanoTime() < deadline);
            assertEquals(2, members.path("addresses").size(), members.toString());
            JsonNode response = call(ports[5], JSON.createObjectNode().put("op", "execute").put("token", token).put("query", "RETURN 42 AS answer").toString());
            assertEquals("OK", response.path("code").asText(), response.toString());
            assertEquals(42, response.path("rows").get(0).get(0).asInt());
        }
    }
    private ComputeConfig config(ComputeConfig.Peer node, List<ComputeConfig.Peer> members, int storage) {
        return new ComputeConfig("compute-join", node.id(), node.client(), node.rpc(), node.raft(), home.resolve(node.id()), members, List.of(addr(storage)),
                2, 32, 10000, 60000, 64L*1024*1024, 128L*1024*1024, 1024L*1024*1024, 2L*1024*1024*1024, 10000, 1024*1024, "secret");
    }
    private static InetSocketAddress addr(int port) { return new InetSocketAddress("127.0.0.1", port); }
    private static JsonNode call(int port, String payload) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", port)) {
            socket.setSoTimeout(60000);
            var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
            var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
            writer.write(payload); writer.newLine(); writer.flush(); return JSON.readTree(reader.readLine());
        }
    }
}
