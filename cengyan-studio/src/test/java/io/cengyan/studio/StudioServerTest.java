package io.cengyan.studio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StudioServerTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void 登录把图数据库的会话交回浏览器() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"OK","session":"s1","columns":["user","role"],"rows":[["root","write"]],"rowsAffected":0}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> response = post(studio.port(), "/api/login",
                        "{\"user\":\"root\",\"password\":\"cengyan\"}", "");
                assertEquals(200, response.statusCode());
                JsonNode body = MAPPER.readTree(response.body());
                assertEquals("OK", body.get("code").asText());
                assertEquals("s1", body.get("session").asText());
            }
            worker.join(3000);
            JsonNode sent = MAPPER.readTree(seen.get());
            assertEquals("login", sent.get("op").asText());
            assertEquals("root", sent.get("user").asText());
        }
    }

    @Test
    void 查询带上浏览器的会话() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"OK","session":"s1","columns":["name"],"rows":[["Company"]],"rowsAffected":0}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> response = post(studio.port(), "/api/query",
                        "{\"query\":\"SHOW GRAPHS\"}", "s1");
                assertEquals(200, response.statusCode());
                assertEquals("Company", MAPPER.readTree(response.body()).get("rows").get(0).get(0).asText());
            }
            worker.join(3000);
            JsonNode sent = MAPPER.readTree(seen.get());
            assertEquals("execute", sent.get("op").asText());
            assertEquals("s1", sent.get("session").asText());
            assertEquals("SHOW GRAPHS", sent.get("query").asText());
        }
    }

    @Test
    void 图数据库没起来时返回连不上() throws Exception {
        int closed;
        try (ServerSocket socket = new ServerSocket(0)) {
            closed = socket.getLocalPort();
        }
        try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", closed))) {
            HttpResponse<String> response = post(studio.port(), "/api/ping", "{}", "");
            assertEquals(502, response.statusCode());
            assertEquals("UNAVAILABLE", MAPPER.readTree(response.body()).get("code").asText());
        }
    }

    @Test
    void 首页是控制台页面() throws Exception {
        try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", 1))) {
            HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + studio.port() + "/"))
                    .GET()
                    .build();
            HttpResponse<String> response = HttpClient.newHttpClient()
                    .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("层岩控制台"));
            assertTrue(response.headers().firstValue("Content-Type").orElse("").startsWith("text/html"));
        }
    }

    private static void reply(ServerSocket db, AtomicReference<String> seen, String response) {
        try (var socket = db.accept();
             var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
             var writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8))) {
            seen.set(reader.readLine());
            writer.write(response.strip());
            writer.write('\n');
            writer.flush();
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    private static HttpResponse<String> post(int port, String path, String json, String session) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json));
        if (!session.isEmpty()) {
            builder.header("X-Cengyan-Session", session);
        }
        return HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
}
