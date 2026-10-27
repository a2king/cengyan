package io.cengyan.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PingTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void ping返回OK() throws Exception {
        Path dir = Files.createTempDirectory("cengyan-ping-");
        ServerConfig config = ServerConfig.defaults().forTest(dir);
        try (GraphDbServer server = GraphDbServer.start(config)) {
            try (Socket socket = new Socket("127.0.0.1", server.port())) {
                socket.setSoTimeout(3000);
                BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                writer.write("{\"op\":\"ping\"}\n");
                writer.flush();
                JsonNode response = MAPPER.readTree(reader.readLine());
                assertEquals("OK", response.get("code").asText());

                writer.write("{不是 json\n");
                writer.flush();
                JsonNode bad = MAPPER.readTree(reader.readLine());
                assertEquals("SYNTAX", bad.get("code").asText());
            }
        } finally {
            deleteTree(dir);
        }
    }

    private static void deleteTree(Path root) throws Exception {
        Exception last = null;
        for (int attempt = 0; attempt < 10; attempt++) {
            try {
                if (!Files.exists(root)) {
                    return;
                }
                try (Stream<Path> walk = Files.walk(root)) {
                    for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
                        Files.deleteIfExists(path);
                    }
                }
                return;
            } catch (Exception ex) {
                last = ex;
                System.gc();
                Thread.sleep(50L * (attempt + 1));
            }
        }
        if (last != null) {
            throw last;
        }
    }
}
