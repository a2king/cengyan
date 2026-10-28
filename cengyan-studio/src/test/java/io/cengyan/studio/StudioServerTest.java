package io.cengyan.studio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    void 登录错误密码把图库错误交回() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"FORBIDDEN","message":"用户名或密码错误"}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> response = post(studio.port(), "/api/login",
                        "{\"user\":\"root\",\"password\":\"wrong\"}", "");
                assertEquals(200, response.statusCode());
                JsonNode body = MAPPER.readTree(response.body());
                assertEquals("FORBIDDEN", body.get("code").asText());
                assertTrue(body.get("message").asText().contains("密码"));
            }
            worker.join(3000);
            assertEquals("wrong", MAPPER.readTree(seen.get()).get("password").asText());
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
            assertEquals(StudioServer.QUERY_TIMEOUT_DEFAULT_MS, sent.get("timeoutMs").asInt());
        }
    }

    @Test
    void 查询超时被硬顶() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"OK","session":"s1","columns":["v"],"rows":[[1]],"rowsAffected":0}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> response = post(studio.port(), "/api/query",
                        "{\"query\":\"RETURN 1\",\"timeoutMs\":999999999}", "s1");
                assertEquals(200, response.statusCode());
            }
            worker.join(3000);
            assertEquals(StudioServer.QUERY_TIMEOUT_MAX_MS, MAPPER.readTree(seen.get()).get("timeoutMs").asInt());
        }
    }

    @Test
    void 非法超时被拒绝() throws Exception {
        try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", 1))) {
            HttpResponse<String> response = post(studio.port(), "/api/query",
                    "{\"query\":\"RETURN 1\",\"timeoutMs\":\"forever\"}", "s1");
            assertEquals(400, response.statusCode());
            assertEquals("SYNTAX", MAPPER.readTree(response.body()).get("code").asText());
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
    void admin只转发白名单() throws Exception {
        try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", 1))) {
            for (String command : new String[] {"createUser", "dropUser", "compact", "flush", "setRole"}) {
                HttpResponse<String> response = post(studio.port(), "/api/admin",
                        "{\"command\":\"" + command + "\"}", "s1");
                assertEquals(400, response.statusCode(), command);
                JsonNode body = MAPPER.readTree(response.body());
                assertEquals("SYNTAX", body.get("code").asText());
                assertTrue(body.get("message").asText().contains("stats"));
            }
        }
    }

    @Test
    void stats可以转发() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"OK","session":"s1","columns":["target","name","count"],"rows":[],"rowsAffected":0}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> response = post(studio.port(), "/api/admin",
                        "{\"command\":\"stats\",\"timeoutMs\":999999999}", "s1");
                assertEquals(200, response.statusCode());
            }
            worker.join(3000);
            JsonNode sent = MAPPER.readTree(seen.get());
            assertEquals("admin", sent.get("op").asText());
            assertEquals("stats", sent.get("command").asText());
            assertEquals("s1", sent.get("session").asText());
            assertEquals(StudioServer.STATS_TIMEOUT_MS, sent.get("timeoutMs").asInt());
        }
    }

    @Test
    void rebuildIndex可以转发() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"OK","session":"s1","columns":[],"rows":[],"rowsAffected":2}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> response = post(studio.port(), "/api/admin",
                        "{\"command\":\"rebuildIndex\",\"index\":\"by_name\"}", "s1");
                assertEquals(200, response.statusCode());
            }
            worker.join(3000);
            JsonNode sent = MAPPER.readTree(seen.get());
            assertEquals("rebuildIndex", sent.get("command").asText());
            assertEquals("by_name", sent.get("index").asText());
            assertEquals(StudioServer.REBUILD_TIMEOUT_MS, sent.get("timeoutMs").asInt());
        }
    }

    @Test
    void 副本管理可以转发() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"OK","session":"s1","columns":["nodeId"],"rows":[[2]],"rowsAffected":1}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> response = post(studio.port(), "/api/admin",
                        "{\"command\":\"addReplica\",\"graph\":\"demo\",\"replicas\":2}", "s1");
                assertEquals(200, response.statusCode());
            }
            worker.join(3000);
            JsonNode sent = MAPPER.readTree(seen.get());
            assertEquals("addReplica", sent.get("command").asText());
            assertEquals("demo", sent.get("graph").asText());
            assertEquals(2, sent.get("replicas").asInt());
            assertEquals(StudioServer.REBUILD_TIMEOUT_MS, sent.get("timeoutMs").asInt());
        }
    }

    @Test
    void 超时计算压到硬顶() {
        ObjectNode huge = MAPPER.createObjectNode().put("timeoutMs", 9_999_999);
        assertEquals(30_000, StudioServer.clampTimeout(huge, 10_000, 30_000));
        assertEquals(10_000, StudioServer.clampTimeout(MAPPER.createObjectNode(), 10_000, 30_000));
        ObjectNode low = MAPPER.createObjectNode().put("timeoutMs", 1500);
        assertEquals(1500, StudioServer.clampTimeout(low, 10_000, 30_000));
        assertEquals(-1, StudioServer.clampTimeout(MAPPER.createObjectNode().put("timeoutMs", 0), 10_000, 30_000));
    }

    @Test
    void 运行查询和终止会带上会话() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"OK","session":"s1","columns":["queryId","user","elapsedMs","query"],"rows":[],"rowsAffected":0}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> response = post(studio.port(), "/api/queries", "{}", "s1");
                assertEquals(200, response.statusCode());
                assertEquals("OK", MAPPER.readTree(response.body()).get("code").asText());
            }
            worker.join(3000);
            JsonNode sent = MAPPER.readTree(seen.get());
            assertEquals("queries", sent.get("op").asText());
            assertEquals("s1", sent.get("session").asText());
        }
    }

    @Test
    void 终止查询会转发queryId() throws Exception {
        try (ServerSocket db = new ServerSocket(0)) {
            AtomicReference<String> seen = new AtomicReference<>();
            Thread worker = new Thread(() -> reply(db, seen, """
                    {"code":"OK","session":"s1","queryId":"q1","message":"已终止"}
                    """));
            worker.start();
            try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", db.getLocalPort()))) {
                HttpResponse<String> missing = post(studio.port(), "/api/cancel", "{}", "s1");
                assertEquals(400, missing.statusCode());
                assertTrue(MAPPER.readTree(missing.body()).get("message").asText().contains("queryId"));
                HttpResponse<String> response = post(studio.port(), "/api/cancel", "{\"queryId\":\"q1\"}", "s1");
                assertEquals(200, response.statusCode());
            }
            worker.join(3000);
            JsonNode sent = MAPPER.readTree(seen.get());
            assertEquals("cancel", sent.get("op").asText());
            assertEquals("q1", sent.get("queryId").asText());
            assertEquals("s1", sent.get("session").asText());
        }
    }

    @Test
    void 默认转发到图库端口() {
        StudioConfig config = StudioConfig.defaults();
        assertEquals(19670, config.port());
        assertEquals("127.0.0.1", config.dbHost());
        assertEquals(19669, config.dbPort());
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

    @Test
    void 静态资源包含P0约束() throws Exception {
        try (StudioServer studio = StudioServer.start(new StudioConfig(0, "127.0.0.1", 1))) {
            String css = get(studio.port(), "/app.css");
            assertTrue(css.contains("#0B0F14"));
            assertTrue(css.contains("#2DD4BF"));
            String js = get(studio.port(), "/app.js");
            assertTrue(js.contains("MAX_TABLE_ROWS = 200"));
            assertTrue(js.contains("MAX_GRAPH_NODES = 200"));
            assertTrue(js.contains("MAX_GRAPH_EDGES = 500"));
            assertTrue(js.contains("function canWrite("));
            assertTrue(js.contains("用户权限"));
            assertTrue(js.contains("输入 ") && js.contains(" 确认"));
            assertFalse(js.contains("createUser"));
            assertFalse(js.contains("CREATE ALIAS"));
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

    private static String get(int port, String path) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                .GET()
                .build();
        HttpResponse<String> response = HttpClient.newHttpClient()
                .send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertEquals(200, response.statusCode(), path);
        return response.body();
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
