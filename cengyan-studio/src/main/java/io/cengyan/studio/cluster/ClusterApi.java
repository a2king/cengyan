package io.cengyan.studio.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.cengyan.studio.cluster.ClusterState.Role;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * {@code /api/cluster/*}：服务器、节点、配置、任务管理。除管理员初始化和登录外都需要请求头
 * {@code X-Cengyan-Admin} 携带管理员令牌。令牌放在请求头而不是 Cookie，跨站请求无法自动携带。
 */
public final class ClusterApi {
    private static final int MAX_BODY = 1024 * 1024;
    private static final Set<String> PUBLIC = Set.of("admin/status", "admin/setup", "admin/login");
    private final ClusterManager manager;
    private final AdminAuth auth;

    public ClusterApi(ClusterManager manager, AdminAuth auth) {
        this.manager = manager;
        this.auth = auth;
    }

    public void register(HttpServer http) { http.createContext("/api/cluster/", this::handle); }

    private void handle(HttpExchange exchange) {
        String path = exchange.getRequestURI().getPath().substring("/api/cluster/".length()).replaceAll("/+$", "");
        String method = exchange.getRequestMethod();
        try {
            if (!PUBLIC.contains(path) && !auth.valid(exchange.getRequestHeaders().getFirst("X-Cengyan-Admin")))
                throw new ClusterError("UNAUTHORIZED", "请先登录 Studio 管理员");
            JsonNode body = method.equals("POST") ? read(exchange) : ClusterStore.JSON.createObjectNode();
            Map<String, String> query = query(exchange);
            Object result = route(method, path, body, query, exchange);
            ObjectNode response = result instanceof ObjectNode node ? node : ClusterStore.JSON.createObjectNode();
            if (!(result instanceof ObjectNode) && result != null) response.set("data", ClusterStore.JSON.valueToTree(result));
            if (!response.has("code")) response.put("code", "OK");
            send(exchange, 200, response);
        } catch (ClusterError ex) {
            ObjectNode error = ClusterStore.JSON.createObjectNode().put("code", ex.code()).put("message", ex.getMessage());
            if (!ex.details().isEmpty()) error.set("details", ClusterStore.JSON.valueToTree(ex.details()));
            send(exchange, status(ex.code()), error);
        } catch (IllegalArgumentException ex) {
            send(exchange, 400, ClusterStore.JSON.createObjectNode().put("code", "INVALID_ARGUMENT").put("message", TaskRunner.message(ex)));
        } catch (IOException ex) {
            send(exchange, 502, ClusterStore.JSON.createObjectNode().put("code", "UNAVAILABLE").put("message", TaskRunner.message(ex)));
        } catch (RuntimeException ex) {
            java.util.logging.Logger.getLogger(ClusterApi.class.getName()).log(java.util.logging.Level.WARNING, "集群管理请求失败：" + path, ex);
            send(exchange, 500, ClusterStore.JSON.createObjectNode().put("code", "INTERNAL").put("message", TaskRunner.message(ex)));
        }
    }

    private Object route(String method, String path, JsonNode b, Map<String, String> q, HttpExchange exchange) throws IOException {
        String key = method + " " + path;
        return switch (key) {
            case "GET admin/status" -> ClusterStore.JSON.createObjectNode().put("initialized", auth.initialized());
            case "POST admin/setup" -> ClusterStore.JSON.createObjectNode().put("token", auth.setup(text(b, "password")));
            case "POST admin/login" -> ClusterStore.JSON.createObjectNode().put("token", auth.login(text(b, "password")));
            case "POST admin/logout" -> { auth.logout(exchange.getRequestHeaders().getFirst("X-Cengyan-Admin")); yield null; }
            case "POST admin/password" -> { auth.change(text(b, "current"), text(b, "password")); yield null; }
            case "GET overview" -> manager.overview();
            case "GET runtime" -> manager.runtime();
            case "GET catalog" -> Map.of("STORAGE", manager.catalog().entries(Role.STORAGE), "COMPUTE", manager.catalog().entries(Role.COMPUTE));
            case "POST servers" -> manager.addServer(text(b, "host"), b.path("sshPort").asInt(22), text(b, "password"), text(b, "note"));
            case "POST servers/remove" -> manager.removeServer(text(b, "id"));
            case "GET nodes/suggest" -> manager.suggest(role(q.get("role")), q.getOrDefault("server", ""));
            case "POST nodes" -> manager.addNode(role(text(b, "role")), text(b, "serverId"), text(b, "id"),
                    b.path("raftPort").asInt(), b.path("rpcPort").asInt(), b.path("clientPort").asInt(), text(b, "dataDir"));
            case "POST nodes/remove" -> {
                if (b.path("deleteData").asBoolean() && !text(b, "confirm").equals(text(b, "id")))
                    throw new ClusterError("INVALID_ARGUMENT", "删除数据目录需要输入节点 ID 确认");
                yield manager.removeNode(text(b, "id"), b.path("deleteData").asBoolean());
            }
            case "GET nodes/logs" -> Map.of("text", manager.logs(q.getOrDefault("id", ""), parse(q.get("lines"), 300)));
            case "POST restart" -> manager.restart(strings(b.path("nodes")));
            case "POST upgrade" -> manager.upgrade();
            case "GET config" -> config();
            case "POST config" -> manager.saveConfig(ClusterStore.JSON.convertValue(b.path("roleDefaults"), new TypeReference<Map<Role, Map<String, String>>>() { }),
                    ClusterStore.JSON.convertValue(b.path("nodeOverrides"), new TypeReference<Map<String, Map<String, String>>>() { }), text(b, "note"));
            case "GET config/history" -> manager.history();
            case "GET config/diff" -> manager.diff(parse(q.get("from"), 0), parse(q.get("to"), manager.state().config().version()));
            case "POST config/rollback" -> manager.rollback(b.path("version").asInt());
            case "POST root-password" -> { manager.setRootPassword(text(b, "password")); yield null; }
            case "GET tasks" -> manager.tasks().list(parse(q.get("limit"), 20));
            case "GET tasks/detail" -> manager.tasks().get(q.getOrDefault("id", ""));
            case "POST tasks/action" -> manager.act(text(b, "id"), text(b, "action"), text(b, "password"), b.path("exitMaintenance").asBoolean());
            case "POST maintenance/exit" -> { manager.exitMaintenance(); yield null; }
            default -> throw new ClusterError("NOT_FOUND", "没有这个接口：" + key);
        };
    }

    private ObjectNode config() {
        ClusterState state = manager.state();
        ObjectNode result = ClusterStore.JSON.createObjectNode();
        result.set("current", ClusterStore.JSON.valueToTree(state.config()));
        ObjectNode effective = result.putObject("effective");
        for (ClusterState.NodeSpec node : state.nodes())
            effective.set(node.id(), ClusterStore.JSON.valueToTree(manager.catalog().effective(node.role(), state.config(), node.id())));
        return result;
    }

    private static int status(String code) {
        return switch (code) {
            case "UNAUTHORIZED", "LOCKED" -> 401;
            case "NOT_FOUND" -> 404;
            case "BUSY", "CONFLICT", "INVALID_STATE", "ALREADY_INITIALIZED", "PORT_IN_USE", "NOT_EMPTY", "LAST_NODE", "NOTHING_TO_DO" -> 409;
            case "REMOTE_FAILED", "SSH_UNREACHABLE", "SSH_AUTH_FAILED", "HOST_KEY_CHANGED", "UNAVAILABLE", "GROUP_UNAVAILABLE" -> 502;
            default -> 400;
        };
    }

    private static Role role(String value) {
        try { return Role.valueOf(value == null ? "" : value.toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException ex) { throw new ClusterError("INVALID_ARGUMENT", "角色只能是 STORAGE 或 COMPUTE"); }
    }

    private static String text(JsonNode body, String field) {
        JsonNode node = body.get(field);
        return node == null || node.isNull() ? "" : node.asText("");
    }

    private static List<String> strings(JsonNode array) {
        List<String> result = new ArrayList<>();
        if (array.isArray()) array.forEach(n -> result.add(n.asText()));
        return result;
    }

    private static int parse(String value, int fallback) {
        try { return value == null || value.isBlank() ? fallback : Integer.parseInt(value); }
        catch (NumberFormatException ex) { return fallback; }
    }

    private static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> result = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null) return result;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) continue;
            result.put(java.net.URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                    java.net.URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
        }
        return result;
    }

    private static JsonNode read(HttpExchange exchange) throws IOException {
        byte[] body = exchange.getRequestBody().readNBytes(MAX_BODY + 1);
        if (body.length > MAX_BODY) throw new ClusterError("INVALID_ARGUMENT", "请求过大");
        if (body.length == 0) return ClusterStore.JSON.createObjectNode();
        JsonNode node;
        try { node = ClusterStore.JSON.readTree(body); }
        catch (IOException ex) { throw new ClusterError("INVALID_ARGUMENT", "请求不是合法的 JSON"); }
        if (!node.isObject()) throw new ClusterError("INVALID_ARGUMENT", "请求不是 JSON 对象");
        return node;
    }

    private static void send(HttpExchange exchange, int status, JsonNode body) {
        try {
            byte[] bytes = ClusterStore.JSON.writeValueAsBytes(body);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.getResponseHeaders().set("Cache-Control", "no-store");
            exchange.sendResponseHeaders(status, bytes.length);
            exchange.getResponseBody().write(bytes);
        } catch (IOException ignored) {
        } finally { exchange.close(); }
    }
}
