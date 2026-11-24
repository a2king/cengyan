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
import java.util.Set;
import io.cengyan.studio.cluster.ClusterError;
import io.cengyan.studio.cluster.StudioAuth;
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
    private final int graphMaxNodes;
    private final int tableMaxNodes;
    private final ComputeOpsClient computeOps;
    private final io.cengyan.studio.cluster.ClusterManager cluster;
    private final StudioAuth auth;
    /** 数据库这样回答登录时说明数据库在正常工作，不走 root 本地兜底。 */
    private static final Set<String> DB_ANSWERED = Set.of("OK", "UNAUTHENTICATED", "FORBIDDEN", "INVALID_ARGUMENT", "SYNTAX");
    /** 控制台转发的管理命令：运维、节点维护、副本，以及用户管理（权限由 Compute 判断）。 */
    static final Set<String> ADMIN_COMMANDS = Set.of("stats", "storage", "showMembers", "addReplica", "replicaStatus", "cancelReplica", "showNodes",
            "drainStorage", "removeStorage", "drainCompute", "removeCompute",
            "listUsers", "createUser", "setRole", "setPassword", "dropUser");

    private StudioServer(DbClient db, HttpServer http, ExecutorService workers, int port, int graphMaxNodes, int tableMaxNodes,
                          ComputeOpsClient computeOps, io.cengyan.studio.cluster.ClusterManager cluster, StudioAuth auth) {
        this.db = db;
        this.http = http;
        this.workers = workers;
        this.port = port;
        this.graphMaxNodes = graphMaxNodes;
        this.tableMaxNodes = tableMaxNodes;
        this.computeOps = computeOps;
        this.cluster = cluster;
        this.auth = auth;
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
        io.cengyan.studio.cluster.ClusterManager cluster = config.home().isBlank() ? null
                : io.cengyan.studio.cluster.ClusterManager.open(java.nio.file.Path.of(config.home()));
        DbClient db = new DbClient(() -> {
            if (cluster != null) {
                var target = cluster.databaseTarget();
                if (target != null) return target;
            }
            return InetSocketAddress.createUnresolved(config.dbHost(), config.dbPort());
        });
        HttpServer http = HttpServer.create(new InetSocketAddress(config.host(), config.port()), 0);
        ExecutorService workers = Executors.newVirtualThreadPerTaskExecutor();
        http.setExecutor(workers);
        StudioAuth auth = new StudioAuth(cluster, config.home().isBlank() ? null : java.nio.file.Path.of(config.home()).resolve("data"),
                token -> MAPPER.readTree(db.exchange(MAPPER.createObjectNode().put("op", "session").put("session", token), 3000)));
        StudioServer server = new StudioServer(db, http, workers, http.getAddress().getPort(), config.graphMaxNodes(), config.tableMaxNodes(),
                new ComputeOpsClient(config.computeHost(), config.computePort(), config.computeUser(), config.computePassword()), cluster, auth);
        if (cluster != null) new io.cengyan.studio.cluster.ClusterApi(cluster, auth).register(http);
        http.createContext("/api/ping", server::ping);
        http.createContext("/api/session", server::session);
        http.createContext("/api/config", server::config);
        http.createContext("/api/login", server::login);
        http.createContext("/api/logout", server::logout);
        http.createContext("/api/password", server::password);
        http.createContext("/api/query", server::query);
        http.createContext("/api/queries", server::queries);
        http.createContext("/api/cancel", server::cancel);
        http.createContext("/api/admin", server::admin);
        http.createContext("/api/ops/overview", server::opsOverview);
        http.createContext("/api/ops/series", server::opsSeries);
        http.createContext("/api/ops/shards", server::opsShards);
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
        if (cluster != null) cluster.close();
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

    private void config(HttpExchange exchange) {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("SYNTAX", "只接受 GET"));
            return;
        }
        ObjectNode response = MAPPER.createObjectNode();
        response.put("code", "OK");
        response.put("graphMaxNodes", graphMaxNodes);
        response.put("tableMaxNodes", tableMaxNodes);
        response.put("computeOpsConfigured", computeOps.configured() || cluster != null);
        response.put("clusterManaged", cluster != null);
        send(exchange, 200, response);
    }

    private void session(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        String token = session(exchange, body);
        if (auth.isLocal(token)) {
            // root 本地会话由 Studio 自己回答；过期时与数据库一样返回 UNAUTHENTICATED，前端回到登录页。
            try {
                auth.resolve(token);
                send(exchange, 200, localSession(token));
            } catch (ClusterError ex) {
                send(exchange, 200, error("UNAUTHENTICATED", ex.getMessage()));
            }
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "session");
        request.put("session", token);
        try {
            JsonNode reply = MAPPER.readTree(db.exchange(request, 3000));
            if (reply instanceof ObjectNode node && "OK".equals(node.path("code").asText())) {
                node.put("role", StudioAuth.effectiveRole(node.path("user").asText(), node.path("role").asText()));
            }
            send(exchange, 200, reply);
        } catch (IOException ex) {
            send(exchange, 502, error("UNAVAILABLE", "连不上图数据库：" + ex.getMessage()));
        }
    }

    /** 登录成功的响应里 rows[0] = [user, role]：root 按 admin 返回（旧版 Compute 报 write，见 StudioAuth.effectiveRole）。 */
    private static void normalizeLoginRole(JsonNode reply) {
        JsonNode row = reply.path("rows").path(0);
        if (row instanceof com.fasterxml.jackson.databind.node.ArrayNode array && array.size() >= 2) {
            array.set(1, MAPPER.getNodeFactory().textNode(StudioAuth.effectiveRole(array.get(0).asText(), array.get(1).asText())));
        }
    }

    private static ObjectNode localSession(String token) {
        ObjectNode response = MAPPER.createObjectNode().put("code", "OK").put("session", token)
                .put("user", StudioAuth.ROOT).put("role", "admin").put("local", true)
                .put("message", "数据库不可用，当前为 root 本地登录，只能使用集群管理和运维监控");
        response.set("columns", MAPPER.valueToTree(java.util.List.of("user", "role")));
        response.set("rows", MAPPER.valueToTree(java.util.List.of(java.util.List.of(StudioAuth.ROOT, "admin"))));
        return response;
    }

    /** 运维监控：任何已登录的会话（含 root 本地会话）。失败时已写回响应，返回 false。 */
    private boolean requireSession(HttpExchange exchange) {
        try {
            auth.resolve(exchange.getRequestHeaders().getFirst("X-Cengyan-Session"));
            return true;
        } catch (ClusterError ex) {
            send(exchange, switch (ex.code()) { case "UNAUTHORIZED" -> 401; case "DB_UNAVAILABLE" -> 503; default -> 400; }, error(ex.code(), ex.getMessage()));
            return false;
        }
    }

    /** 本地会话不能访问数据库接口。失败时已写回响应，返回 true。 */
    private boolean rejectLocal(HttpExchange exchange, String token) {
        if (!auth.isLocal(token)) return false;
        send(exchange, 409, error("LOCAL_SESSION", "数据库不可用，当前为 root 本地登录，只能使用集群管理和运维监控；数据库恢复后请重新登录"));
        return true;
    }

    private void opsOverview(HttpExchange exchange) {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("SYNTAX", "只接受 GET"));
            return;
        }
        if (!requireSession(exchange)) return;
        try {
            JsonNode response = cluster != null ? cluster.opsOverview() : computeOps.overview(5000);
            int status = "OK".equals(response.path("code").asText()) ? 200 : 502;
            send(exchange, status, response);
        } catch (IOException ex) {
            send(exchange, 503, error("OPS_UNAVAILABLE", ex.getMessage()));
        }
    }

    /** 趋势曲线和分片拓扑只在集群托管模式下有：数据来自 Studio 本机的监控库。 */
    private void opsSeries(HttpExchange exchange) {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("SYNTAX", "只接受 GET"));
            return;
        }
        if (!requireSession(exchange)) return;
        if (cluster == null) {
            send(exchange, 404, error("OPS_UNAVAILABLE", "没有托管集群，不记录趋势"));
            return;
        }
        String query = exchange.getRequestURI().getRawQuery();
        String range = "1h";
        if (query != null) {
            for (String part : query.split("&")) if (part.startsWith("range=")) range = part.substring("range=".length());
        }
        try {
            send(exchange, 200, cluster.opsSeries(range));
        } catch (RuntimeException ex) {
            send(exchange, 503, error("OPS_UNAVAILABLE", ex.getMessage()));
        }
    }

    private void opsShards(HttpExchange exchange) {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("SYNTAX", "只接受 GET"));
            return;
        }
        if (!requireSession(exchange)) return;
        if (cluster == null) {
            send(exchange, 404, error("OPS_UNAVAILABLE", "没有托管集群"));
            return;
        }
        send(exchange, 200, cluster.opsShards());
    }

    private void login(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        String user = text(body, "user"), password = text(body, "password");
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "login");
        request.put("session", text(body, "session"));
        request.put("user", user);
        request.put("password", password);
        JsonNode reply = null;
        String failure = "";
        try {
            reply = MAPPER.readTree(db.exchange(request, 10000));
        } catch (IOException ex) {
            failure = ex.getMessage();
        }
        String code = reply == null ? "UNAVAILABLE" : reply.path("code").asText();
        if (reply != null && code.equals("OK")) {
            // root 刚被数据库验证：Studio 保存的 root 密码不同时同步（例如在命令行改过）。
            if (StudioAuth.ROOT.equals(user)) {
                try { auth.rootVerified(password); } catch (IOException | RuntimeException ignored) { /* 同步失败不影响登录 */ }
            }
            normalizeLoginRole(reply);
            send(exchange, 200, reply);
            return;
        }
        // 数据库能回答（包括密码错误）时直接返回，不走兜底，避免绕过数据库里已经改掉的密码。
        boolean dbDown = reply == null || !DB_ANSWERED.contains(code);
        if (dbDown && auth.managed() && auth.initialized() && StudioAuth.ROOT.equals(user)) {
            try {
                send(exchange, 200, localSession(auth.loginLocal(user, password)));
            } catch (ClusterError ex) {
                send(exchange, 200, error(ex.code().equals("UNAUTHORIZED") ? "UNAUTHENTICATED" : ex.code(), ex.getMessage()));
            }
            return;
        }
        if (reply == null) send(exchange, 502, error("UNAVAILABLE", "连不上图数据库：" + failure));
        else send(exchange, 200, reply);
    }

    private void logout(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        String token = session(exchange, body);
        auth.logout(token);
        if (!auth.isLocal(token) && !token.isBlank()) {
            try { db.exchange(MAPPER.createObjectNode().put("op", "logout").put("session", token), 3000); }
            catch (IOException ignored) { /* 令牌无状态，退出只是让浏览器丢弃它 */ }
        }
        send(exchange, 200, MAPPER.createObjectNode().put("code", "OK"));
    }

    /** 修改自己的密码。root 改成功后同步 Studio 保存的 root 密码。 */
    private void password(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        String token = session(exchange, body), current = text(body, "current"), next = text(body, "password");
        try {
            StudioAuth.Principal principal = auth.resolve(token);
            if (principal.local()) {
                auth.changeLocal(current, next);
                send(exchange, 200, MAPPER.createObjectNode().put("code", "OK"));
                return;
            }
            ObjectNode request = MAPPER.createObjectNode().put("op", "admin").put("session", token).put("command", "changePassword")
                    .put("current", current).put("password", next).put("timeoutMs", 15000);
            JsonNode reply = MAPPER.readTree(db.exchange(request, 15000));
            if ("OK".equals(reply.path("code").asText()) && StudioAuth.ROOT.equals(principal.user())) auth.rootVerified(next);
            send(exchange, 200, reply);
        } catch (ClusterError ex) {
            send(exchange, 200, error(ex.code(), ex.getMessage()));
        } catch (IOException ex) {
            send(exchange, 502, error("UNAVAILABLE", "连不上图数据库：" + ex.getMessage()));
        }
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
        String token = session(exchange, body);
        if (rejectLocal(exchange, token)) {
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "execute");
        request.put("session", token);
        request.put("query", query);
        request.put("graph", text(body, "graph"));
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
        String token = session(exchange, body);
        if (rejectLocal(exchange, token)) {
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "queries");
        request.put("session", token);
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
        String token = session(exchange, body);
        if (rejectLocal(exchange, token)) {
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "cancel");
        request.put("session", token);
        request.put("queryId", queryId);
        forward(exchange, request, 10000);
    }

    private void admin(HttpExchange exchange) {
        JsonNode body = read(exchange);
        if (body == null) {
            return;
        }
        String command = text(body, "command");
        if (!ADMIN_COMMANDS.contains(command)) {
            send(exchange, 400, error("SYNTAX", "控制台只转发 stats、storage、节点维护、副本管理和用户管理命令"));
            return;
        }
        String token = session(exchange, body);
        if (rejectLocal(exchange, token)) {
            return;
        }
        ObjectNode request = MAPPER.createObjectNode();
        request.put("op", "admin");
        request.put("session", token);
        request.put("command", command);
        request.put("index", text(body, "index"));
        request.put("graph", text(body, "graph"));
        request.put("address", text(body, "address"));
        for (String field : new String[] {"user", "password", "role"}) {
            if (body.hasNonNull(field)) request.put(field, text(body, field));
        }
        if (body.hasNonNull("nodeId")) request.put("nodeId", body.path("nodeId").asText());
        JsonNode replicas = body.get("replicas");
        if (replicas != null && replicas.isNumber()) {
            request.put("replicas", replicas.asInt());
        }
        if (body.path("refresh").asBoolean(false)) {
            request.put("refresh", true);
        }
        int cap = command.equals("rebuildIndex") || command.equals("addReplica")
                || command.equals("addStorage") || command.equals("removeStorage")
                || command.equals("addCompute") || command.equals("removeCompute")
                ? REBUILD_TIMEOUT_MS : STATS_TIMEOUT_MS;
        int timeoutMs = clampTimeout(body, cap, cap);
        if (timeoutMs <= 0) {
            send(exchange, 400, error("SYNTAX", "超时时间必须为正"));
            return;
        }
        request.put("timeoutMs", timeoutMs);
        if (command.equals("setPassword") && StudioAuth.ROOT.equals(text(body, "user"))) {
            // 只有 root 自己能改 root 的密码（Compute 判断）；成功后同步 Studio 保存的 root 密码。
            try {
                JsonNode reply = MAPPER.readTree(db.exchange(request, timeoutMs));
                if ("OK".equals(reply.path("code").asText())) auth.rootVerified(text(body, "password"));
                send(exchange, 200, reply);
            } catch (IOException ex) {
                send(exchange, 502, error("UNAVAILABLE", "连不上图数据库：" + ex.getMessage()));
            }
            return;
        }
        forward(exchange, request, timeoutMs);
    }

    private void page(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, error("SYNTAX", "只接受 GET"));
            return;
        }
        String path = exchange.getRequestURI().getPath();
        boolean appRoute = switch (path) {
            case "/", "/index.html", "/graph", "/gql", "/running", "/ops", "/cluster", "/users" -> true;
            default -> false;
        };
        String resource = appRoute ? "studio/index.html" : switch (path) {
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
