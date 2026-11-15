package io.cengyan.studio.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compute 客户端端口一行一个 JSON。如果请求被格式化成多行（例如用了缩进的 ObjectMapper），
 * Compute 只会读到第一个 "{"，回报 "Unexpected end-of-input"，节点永远无法通过健康检查。
 */
class ClusterOpsTest {
    @Test
    void computeRequestsAreSentAsOneLine() throws Exception {
        try (ServerSocket server = new ServerSocket(0)) {
            CompletableFuture<JsonNode> received = new CompletableFuture<>();
            CompletableFuture<String> secondLine = new CompletableFuture<>();
            Thread peer = new Thread(() -> {
                try (Socket socket = server.accept()) {
                    var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                    String line = reader.readLine();
                    received.complete(new ObjectMapper().readTree(line));
                    // Reply first: the client waits for an answer before it closes the socket.
                    var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                    writer.write("{\"code\":\"OK\",\"token\":\"t\"}\n");
                    writer.flush();
                    // Anything else the client wrote would arrive as another line and break the protocol.
                    socket.setSoTimeout(500);
                    try { secondLine.complete(reader.readLine() == null ? "" : "extra"); }
                    catch (java.net.SocketTimeoutException timeout) { secondLine.complete(""); }
                } catch (Exception ex) { received.completeExceptionally(ex); }
            }, "fake-studio-client");
            peer.setDaemon(true);
            peer.start();

            ClusterOps ops = new ClusterOps();
            ObjectNode request = ClusterStore.JSON.createObjectNode().put("op", "login").put("user", "root").put("password", "p");
            JsonNode reply = ops.computeCall("127.0.0.1", server.getLocalPort(), request, 5000);
            assertEquals("OK", reply.path("code").asText());
            JsonNode sent = received.get(5, TimeUnit.SECONDS);
            assertEquals("login", sent.path("op").asText(), "请求必须是完整的单行 JSON");
            assertEquals(5000, sent.path("timeoutMs").asInt());
            assertEquals("", secondLine.get(5, TimeUnit.SECONDS), "除请求外不能再写其他行");
        }
    }
}
