package io.cengyan.studio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 浏览器里的控制台。页面是静态文件，语句和图数据库的会话都经这里转发。
 */
public final class StudioServer implements AutoCloseable {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_BODY = 1024 * 1024;
    /** 查询默认超时。客户端可以调低，但不能超过 {@link #QUERY_TIMEOUT_MAX_MS}。 */
    static final int QUERY_TIMEOUT_DEFAULT_MS = 10_000;
    /** 查询超时硬顶，禁止浏览器传任意大 timeoutMs。 */
    static final int QUERY_TIMEOUT_MAX_MS = 30_000;
    static final int STATS_TIMEOUT_MS = 60_000;
    static final int REBUILD_TIMEOUT_MS = 120_000;

    private final DbClient db;
    private final HttpServer http;
    private final ExecutorService workers;
    private final int port;

    private StudioServer(DbClient db, HttpServer http, ExecutorService workers, int port) {
        this.db = db;
        this.http = http;
        this.workers = workers;
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        StudioConfig config = StudioConfig.parse(args);
        StudioServer server = start(config);
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.close();
            stopped.countDown();
        }, "cengyan-studio-shutdown"));
        System.out.println("层岩控制台已监听 http://" + config.host() + ":" + server.port()
                + " ，图数据库 " + config.dbHost() + ":" + config.dbPort());
        stopped.await();
    }

    public static StudioServer start(StudioConfig config) throws IOException {
        DbClient db = new DbClient(config.dbHost(), config.dbPort());
        HttpServer http = HttpServer.create(new InetSocketAddress(config.host(), config.port()), 0);
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        http.setExecutor(workers);
        StudioServer server = new StudioServer(db, http, workers, http.getAddress().getPort());
        http.createContext("/api/ping", server::ping);
        http.createContext("/api/login", server::login);
        http.createContext("/api/query", server::query);
        http.createContext("/api/queries", server::queries);
        http.createContext("/api/cancel", server::cancel);
        http.createContext("/api/admin", server::admin);
        http.createContext("/", server::page);
        http.start();
        return server;
    }

    public int port() {
        return port;
    }

    @Override
    public void close() {
        http.stop(0);
        workers.shutdownNow();
    }

    private void ping(HttpExchange exchange) {
        if (!"POST".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("SYNTAX", "只接受 POST"));
            return;
        }
        try {
            exchange.getRequestBody().readNBytes(MAX_BODY + 1);
        } catch (IOException ex) {
            send(exchange, 400, error("SYNTAX", "读请求失败"));
            return;
        }
        forward(exchange, MAPPER.createObjectNode().put("op", "ping"), 3000);
    }

    private void login(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "login");
        request.put("session", text(body, "session"));
        request.put("user", text(body, "user"));
        request.put("password", text(body, "password"));
        forward(exchange, request, 10000);
    }

    private void query(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        String query = text(body, "query");
        if (query.isBlank()) {
            send(exchange, 400, error("SYNTAX", "语句不能为空"));
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "execute");
        request.put("session", session(exchange, body));
        request.put("query", query);
        request.set("params", body.has("params") && body.get("params").isObject()
                ? body.get("params") : MAPPER.createObjectNode());
        int timeoutMs = clampTimeout(body, QUERY_TIMEOUT_DEFAULT_MS, QUERY_TIMEOUT_MAX_MS);
        if (timeoutMs <= 0) {
            send(exchange, 400, error("SYNTAX", "超时时间必须为正"));
            return;
        }
        request.put("timeoutMs", timeoutMs);
        forward(exchange, request, timeoutMs);
    }

    private void queries(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "queries");
        request.put("session", session(exchange, body));
        forward(exchange, request, 10000);
    }

    private void cancel(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        String queryId = text(body, "queryId");
        if (queryId.isBlank()) {
            send(exchange, 400, error("SYNTAX", "缺少 queryId"));
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "cancel");
        request.put("session", session(exchange, body));
        request.put("queryId", queryId);
        forward(exchange, request, 10000);
    }

    private void admin(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        String command = text(body, "command");
        if (!command.equals("stats") && !command.equals("storage") && !command.equals("rebuildIndex")
                && !command.equals("showMembers") && !command.equals("addReplica")) {
            send(exchange, 400, error("SYNTAX", "控制台只转发 stats、rebuildIndex、showMembers 和 addReplica"));
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "admin");
        request.put("session", session(exchange, body));
        request.put("command", command);
        request.put("index", text(body, "index"));
        request.put("graph", text(body, "graph"));
        JsonNode replicas = body.get("replicas");
        if (replicas != null && replicas.isNumber()) {
            request.put("replicas", replicas.asInt());
        }
        int cap = command.equals("rebuildIndex") || command.equals("addReplica")
                ? REBUILD_TIMEOUT_MS : STATS_TIMEOUT_MS;
        int timeoutMs = clampTimeout(body, cap, cap);
        if (timeoutMs <= 0) {
            send(exchange, 400, error("SYNTAX", "超时时间必须为正"));
            return;
        }
        request.put("timeoutMs", timeoutMs);
        forward(exchange, request, timeoutMs);
    }

    private void page(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("SYNTAX", "只接受 GET"));
            return;
        }
        String path = exchange.getRequestURI().getPath();
        String resource = switch (path) {
            case "/", "/index.html" -> "studio/index.html";
            case "/app.js" -> "studio/app.js";
            case "/app.css" -> "studio/app.css";
            default -> null;
        };
        if (resource == null && path.startsWith("/assets/") && !path.contains("..")) {
            resource = "studio" + path;
        }
        if (resource == null) {
            send(exchange, 404, error("SYNTAX", "没有这个页面"));
            return;
        }
        try (InputStream input = StudioServer.class.getClassLoader().getResourceAsStream(resource)) {
            if (input == null) {
                send(exchange, 500, error("INTERNAL", "控制台页面缺失"));
                return;
            }
            byte[] bytes = input.readAllBytes();
            exchange.getResponseHeaders().set("Content-Type", contentType(resource));
            exchange.getResponseHeaders().set("Cache-Control", path.startsWith("/assets/")
                    ? "public, max-age=31536000, immutable"
                    : "no-cache");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        }
    }

    private void forward(HttpExchange exchange, ObjectNode request, int timeoutMs) {
        try {
            String line = db.exchange(request, timeoutMs);
            JsonNode node = MAPPER.readTree(line);
            send(exchange, 200, node);
        } catch (IOException ex) {
            send(exchange, 502, error("UNAVAILABLE", "连不上图数据库：" + ex.getMessage()));
        }
    }

    private JsonNode read(HttpExchange exchange) {
        if (!"POST".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("SYNTAX", "只接受 POST"));
            return null;
        }
        try {
            byte[] body = exchange.getRequestBody().readNBytes(MAX_BODY + 1);
            if (body.length > MAX_BODY) {
                send(exchange, 413, error("SYNTAX", "请求过大"));
                return null;
            }
            if (body.length == 0) {
                return MAPPER.createObjectNode();
            }
            JsonNode node = MAPPER.readTree(body);
            if (!node.isObject()) {
                send(exchange, 400, error("SYNTAX", "请求不是 JSON 对象"));
                return null;
            }
            return node;
        } catch (IOException ex) {
            send(exchange, 400, error("SYNTAX", "请求不是合法的 JSON"));
            return null;
        }
    }

    private static String session(HttpExchange exchange, JsonNode body) {
        String header = exchange.getRequestHeaders().getFirst("X-Cengyan-Session");
        if (header != null && !header.isBlank()) {
            return header;
        }
        return text(body, "session");
    }

    /**
     * 读取客户端超时：缺省用 fallback，非法返回 -1，超过 max 时压到 max。
     */
    static int clampTimeout(JsonNode body, int fallback, int max) {
        JsonNode node = body.get("timeoutMs");
        if (node == null || node.isNull()) {
            return fallback;
        }
        if (!node.isNumber()) {
            return -1;
        }
        int value = node.asInt();
        if (value <= 0) {
            return -1;
        }
        return Math.min(value, max);
    }

    private static String text(JsonNode body, String field) {
        JsonNode node = body.get(field);
        return node == null || node.isNull() ? "" : node.asText("");
    }

    private static ObjectNode error(String code, String message) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("code", code);
        node.put("message", message);
        return node;
    }

    private static void send(HttpExchange exchange, int status, JsonNode body) {
        byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
        try {
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
            exchange.close();
        } catch (IOException ex) {
            exchange.close();
        }
    }

    private static String contentType(String resource) {
        if (resource.endsWith(".js")) {
            return "text/javascript; charset=utf-8";
        }
        if (resource.endsWith(".css")) {
            return "text/css; charset=utf-8";
        }
        if (resource.endsWith(".svg")) {
            return "image/svg+xml";
        }
        if (resource.endsWith(".woff2")) {
            return "font/woff2";
        }
        if (resource.endsWith(".ttf")) {
            return "font/ttf";
        }
        return "text/html; charset=utf-8";
    }
}
