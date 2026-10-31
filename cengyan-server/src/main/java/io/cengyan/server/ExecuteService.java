package io.cengyan.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
import io.cengyan.query.AdminCall;
import io.cengyan.query.QueryEngine;
import io.cengyan.query.QueryHandle;
import io.cengyan.query.QueryLimits;
import io.cengyan.query.QueryResult;
import io.cengyan.query.SessionState;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** 网络线程只解析 JSON。排队和执行交给 {@link QueryScheduler}。 */
final class ExecuteService {
    private static final Logger log = LoggerFactory.getLogger(ExecuteService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final QueryEngine engine;
    private final ServerConfig config;
    private final QueryRegistry queries;
    private final QueryScheduler scheduler;
    private final Sessions sessions;
    private final ConcurrentMap<ChannelId, Set<String>> byChannel = new ConcurrentHashMap<>();

    ExecuteService(
            QueryEngine engine,
            ServerConfig config,
            QueryRegistry queries,
            QueryScheduler scheduler,
            Sessions sessions) {
        this.engine = engine;
        this.config = config;
        this.queries = queries;
        this.scheduler = scheduler;
        this.sessions = sessions;
    }

    void execute(ChannelHandlerContext ctx, JsonNode node) {
        String sessionId = node.path("session").asText("");
        if (sessionId.isBlank()) {
            sessionId = UUID.randomUUID().toString();
        }
        String session = sessionId;
        JsonNode paramsNode = node.get("params");
        if (paramsNode != null && !paramsNode.isNull() && !paramsNode.isObject()) {
            error(ctx, session, null, ErrorCode.SYNTAX, "参数必须是 JSON 对象");
            return;
        }
        String query = node.path("query").asText("");
        if (query.isBlank()) {
            error(ctx, session, null, ErrorCode.SYNTAX, "查询不能为空");
            return;
        }
        Map<String, Object> params = params(paramsNode);
        long timeoutMs = timeout(node);
        if (timeoutMs < 0) {
            error(ctx, session, null, ErrorCode.SYNTAX, "超时时间不能为负，0 表示不限");
            return;
        }
        dispatch(QueryScheduler.laneForQuery(query), session, timeoutMs,
                () -> run(ctx, session, query, params, timeoutMs), ctx);
    }

    void bulkInsert(ChannelHandlerContext ctx, JsonNode node) {
        String sessionId = node.path("session").asText("");
        if (sessionId.isBlank()) {
            sessionId = UUID.randomUUID().toString();
        }
        String session = sessionId;
        String kind = node.path("kind").asText("");
        String type = node.path("type").asText("");
        JsonNode rowsNode = node.get("rows");
        if (kind.isBlank() || type.isBlank() || rowsNode == null || !rowsNode.isArray()) {
            error(ctx, session, null, ErrorCode.SYNTAX, "bulkInsert 需要 kind、type 和 rows 数组");
            return;
        }
        if (rowsNode.size() == 0) {
            error(ctx, session, null, ErrorCode.SYNTAX, "bulkInsert 的 rows 不能为空");
            return;
        }
        if (rowsNode.size() > 10_000) {
            error(ctx, session, null, ErrorCode.SYNTAX, "单批最多 10000 行，请在客户端分块");
            return;
        }
        List<Map<String, Object>> rows = new ArrayList<>(rowsNode.size());
        for (JsonNode row : rowsNode) {
            if (!row.isObject()) {
                error(ctx, session, null, ErrorCode.SYNTAX, "bulkInsert 的每一行必须是 JSON 对象");
                return;
            }
            rows.add(object(row));
        }
        long timeoutMs = timeout(node);
        if (timeoutMs < 0) {
            error(ctx, session, null, ErrorCode.SYNTAX, "超时时间不能为负，0 表示不限");
            return;
        }
        boolean ignoreExisting = node.path("ignoreExisting").asBoolean(false);
        boolean skipMissing = node.path("skipMissing").asBoolean(false);
        long capped = QueryScheduler.capTimeout(timeoutMs, config.heavyMaxExecutionTimeMs());
        dispatch(QueryScheduler.Lane.HEAVY, session, capped,
                () -> runBulk(ctx, session, kind, type, rows, ignoreExisting, skipMissing, capped), ctx);
    }

    private void runBulk(
            ChannelHandlerContext ctx,
            String sessionId,
            String kind,
            String type,
            List<Map<String, Object>> rows,
            boolean ignoreExisting,
            boolean skipMissing,
            long timeoutMs) {
        QueryHandle handle = QueryHandle.start(timeoutMs);
        queries.register(handle);
        byChannel.computeIfAbsent(ctx.channel().id(), ignored -> ConcurrentHashMap.newKeySet()).add(handle.id());
        try {
            SessionState session = sessions.open(sessionId);
            synchronized (session) {
                if (!session.authenticated()) {
                    throw new CengyanException(ErrorCode.FORBIDDEN, "请先登录");
                }
                handle.describe(session.user(), "bulkInsert " + kind + " " + type + " rows=" + rows.size());
            }
            QueryResult result = engine.bulkInsert(session, kind, type, rows, ignoreExisting, skipMissing, handle);
            ObjectNode node = MAPPER.createObjectNode();
            node.put("code", ErrorCode.OK.name());
            node.put("session", sessionId);
            node.put("queryId", handle.id());
            putResult(node, result);
            write(ctx, node);
        } catch (CengyanException ex) {
            error(ctx, sessionId, handle.id(), ex.code(), ex.getMessage());
        } catch (RuntimeException ex) {
            log.warn("批量写入失败：{}", ex.toString());
            error(ctx, sessionId, handle.id(), ErrorCode.INTERNAL, "批量写入失败");
        } finally {
            finish(ctx, handle);
        }
    }

    void admin(ChannelHandlerContext ctx, JsonNode node) {
        String sessionId = node.path("session").asText("");
        if (sessionId.isBlank()) {
            sessionId = UUID.randomUUID().toString();
        }
        String session = sessionId;
        String command = node.path("command").asText("");
        if (command.isBlank()) {
            error(ctx, session, null, ErrorCode.SYNTAX, "缺少 command");
            return;
        }
        String index = node.path("index").asText("");
        long timeoutMs = timeout(node);
        if (timeoutMs < 0) {
            error(ctx, session, null, ErrorCode.SYNTAX, "超时时间不能为负，0 表示不限");
            return;
        }
        QueryScheduler.Lane lane = QueryScheduler.laneForAdmin(command);
        long effective = lane == QueryScheduler.Lane.HEAVY
                ? QueryScheduler.capTimeout(timeoutMs, config.heavyMaxExecutionTimeMs())
                : timeoutMs;
        AdminCall call = new AdminCall(
                command,
                index,
                node.path("user").asText(""),
                node.path("password").asText(""),
                node.path("role").asText(""),
                node.path("address").asText(""),
                node.path("nodeId").asInt(0),
                node.path("graph").asText(""),
                node.path("replicas").asInt(0),
                node.path("parts").asInt(0));
        dispatch(lane, session, effective, () -> runAdmin(ctx, session, call, effective), ctx);
    }

    void login(ChannelHandlerContext ctx, JsonNode node) {
        String sessionId = node.path("session").asText("");
        if (sessionId.isBlank()) {
            sessionId = UUID.randomUUID().toString();
        }
        String session = sessionId;
        String user = node.path("user").asText("");
        String password = node.path("password").asText("");
        long timeoutMs = timeout(node);
        if (timeoutMs < 0) {
            error(ctx, session, null, ErrorCode.SYNTAX, "超时时间不能为负，0 表示不限");
            return;
        }
        dispatch(QueryScheduler.Lane.RESERVED, session, timeoutMs,
                () -> runLogin(ctx, session, user, password, timeoutMs), ctx);
    }

    private void runLogin(
            ChannelHandlerContext ctx, String sessionId, String user, String password, long timeoutMs) {
        QueryHandle handle = QueryHandle.start(timeoutMs);
        queries.register(handle);
        byChannel.computeIfAbsent(ctx.channel().id(), ignored -> ConcurrentHashMap.newKeySet()).add(handle.id());
        try {
            SessionState session = sessions.open(sessionId);
            QueryResult result = engine.login(session, user, password);
            ObjectNode node = MAPPER.createObjectNode();
            node.put("code", ErrorCode.OK.name());
            node.put("session", sessionId);
            node.put("queryId", handle.id());
            putResult(node, result);
            write(ctx, node);
        } catch (CengyanException ex) {
            error(ctx, sessionId, handle.id(), ex.code(), ex.getMessage());
        } catch (RuntimeException ex) {
            log.warn("登录失败：{}", ex.toString());
            error(ctx, sessionId, handle.id(), ErrorCode.INTERNAL, "登录失败");
        } finally {
            finish(ctx, handle);
        }
    }

    private void runAdmin(
            ChannelHandlerContext ctx,
            String sessionId,
            AdminCall call,
            long timeoutMs) {
        QueryHandle handle = QueryHandle.start(timeoutMs);
        queries.register(handle);
        byChannel.computeIfAbsent(ctx.channel().id(), ignored -> ConcurrentHashMap.newKeySet()).add(handle.id());
        try {
            SessionState session = sessions.open(sessionId);
            if (!session.authenticated()) {
                throw new CengyanException(ErrorCode.FORBIDDEN, "请先登录");
            }
            QueryResult result = engine.admin(session, call, handle);
            ObjectNode node = MAPPER.createObjectNode();
            node.put("code", ErrorCode.OK.name());
            node.put("session", sessionId);
            node.put("queryId", handle.id());
            putResult(node, result);
            write(ctx, node);
        } catch (CengyanException ex) {
            error(ctx, sessionId, handle.id(), ex.code(), ex.getMessage());
        } catch (RuntimeException ex) {
            log.warn("执行管理命令失败：{}", ex.toString());
            error(ctx, sessionId, handle.id(), ErrorCode.INTERNAL, "执行管理命令失败");
        } finally {
            finish(ctx, handle);
        }
    }

    void queries(ChannelHandlerContext ctx, JsonNode node) {
        if (requireWrite(ctx, node) == null) {
            return;
        }
        ArrayNode columns = MAPPER.createArrayNode();
        columns.add("queryId");
        columns.add("user");
        columns.add("elapsedMs");
        columns.add("query");
        ArrayNode rows = MAPPER.createArrayNode();
        for (RunningQuery running : queries.list()) {
            ArrayNode row = rows.addArray();
            row.add(running.queryId());
            row.add(running.user());
            row.add(running.elapsedMs());
            row.add(running.statement());
        }
        ObjectNode response = MAPPER.createObjectNode();
        response.put("code", "OK");
        response.put("session", node.path("session").asText(""));
        response.set("columns", columns);
        response.set("rows", rows);
        response.put("rowsAffected", rows.size());
        write(ctx, response);
    }

    void cancel(ChannelHandlerContext ctx, JsonNode node) {
        if (requireWrite(ctx, node) == null) {
            return;
        }
        String queryId = node.path("queryId").asText("");
        String sessionId = node.path("session").asText("");
        if (queryId.isBlank()) {
            error(ctx, sessionId, null, ErrorCode.SYNTAX, "缺少 queryId");
            return;
        }
        if (queries.cancel(queryId, "被终止")) {
            ObjectNode response = MAPPER.createObjectNode();
            response.put("code", "OK");
            response.put("session", sessionId);
            response.put("queryId", queryId);
            response.put("message", "已终止");
            write(ctx, response);
            return;
        }
        error(ctx, sessionId, queryId, ErrorCode.QUERY_CANCELED, "没有找到该查询");
    }

    private SessionState requireWrite(ChannelHandlerContext ctx, JsonNode node) {
        String sessionId = node.path("session").asText("");
        if (sessionId.isBlank()) {
            error(ctx, "", null, ErrorCode.FORBIDDEN, "请先登录");
            return null;
        }
        SessionState session = sessions.open(sessionId);
        // 不占用会话锁。正在执行的语句会一直握着这把锁，管理请求要能在它运行期间列出并终止。
        if (!session.authenticated()) {
            error(ctx, sessionId, null, ErrorCode.FORBIDDEN, "请先登录");
            return null;
        }
        if (session.readOnly()) {
            error(ctx, sessionId, null, ErrorCode.FORBIDDEN, "只读用户不能管理正在执行的查询");
            return null;
        }
        return session;
    }

    void onInactive(ChannelHandlerContext ctx) {
        Set<String> ids = byChannel.remove(ctx.channel().id());
        if (ids != null) {
            for (String id : ids) {
                queries.cancel(id, "连接已断开");
            }
        }
        // 会话按会话号放在内存里，不随这条 TCP 一起删除。
        // Studio 每个 HTTP 请求都会新开一条连接，读完响应就关掉。
        // 如果在这里清会话，登录刚成功，紧接着的查询就会拿到一个未登录的新会话。
    }

    private void run(ChannelHandlerContext ctx, String sessionId, String query, Map<String, Object> params, long timeoutMs) {
        QueryHandle handle = QueryHandle.start(timeoutMs);
        boolean registered = false;
        try {
            SessionState session = sessions.open(sessionId);
            synchronized (session) {
                if (!session.authenticated()) {
                    throw new CengyanException(ErrorCode.FORBIDDEN, "请先登录");
                }
                handle.describe(session.user(), query);
            }
            queries.register(handle);
            registered = true;
            byChannel.computeIfAbsent(ctx.channel().id(), ignored -> ConcurrentHashMap.newKeySet()).add(handle.id());
            QueryResult result = engine.execute(
                    session, query, params, new QueryLimits(
                            config.maxEdgesPerVertex(),
                            config.maxFrontierPerHop(),
                            config.maxHops(),
                            config.maxPaths()),
                    handle);
            ObjectNode node = MAPPER.createObjectNode();
            node.put("code", ErrorCode.OK.name());
            node.put("session", sessionId);
            node.put("queryId", handle.id());
            putResult(node, result);
            write(ctx, node);
        } catch (CengyanException ex) {
            error(ctx, sessionId, handle.id(), ex.code(), ex.getMessage());
        } catch (RuntimeException ex) {
            log.warn("执行查询失败：{}", ex.toString());
            error(ctx, sessionId, handle.id(), ErrorCode.INTERNAL, "执行查询失败");
        } finally {
            if (registered) {
                finish(ctx, handle);
            }
        }
    }

    private void dispatch(
            QueryScheduler.Lane lane, String sessionId, long timeoutMs, Runnable task, ChannelHandlerContext ctx) {
        long sessionWaitMs = timeoutMs <= 0 ? config.queryQueueMs() : timeoutMs;
        scheduler.submit(lane, sessionId, sessionWaitMs, task,
                message -> error(ctx, sessionId, null, ErrorCode.BUSY, message));
    }

    private void finish(ChannelHandlerContext ctx, QueryHandle handle) {
        queries.unregister(handle.id());
        Set<String> ids = byChannel.get(ctx.channel().id());
        if (ids != null) {
            ids.remove(handle.id());
        }
    }

    private long timeout(JsonNode node) {
        JsonNode timeout = node.get("timeoutMs");
        if (timeout == null || timeout.isNull()) {
            return config.maxExecutionTimeMs();
        }
        if (!timeout.isNumber()) {
            return -1;
        }
        return timeout.asLong();
    }

    private static Map<String, Object> params(JsonNode node) {
        Map<String, Object> values = new LinkedHashMap<>();
        if (node == null || node.isNull()) {
            return values;
        }
        node.properties().forEach(entry -> values.put(entry.getKey(), value(entry.getValue())));
        return values;
    }

    private static Object value(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isIntegralNumber()) {
            return node.longValue();
        }
        if (node.isNumber()) {
            return node.doubleValue();
        }
        if (node.isTextual()) {
            return node.asText();
        }
        return node.toString();
    }

    void logout(ChannelHandlerContext ctx, JsonNode node) {
        String sessionId = node.path("session").asText("");
        if (sessionId.isBlank()) {
            error(ctx, "", null, ErrorCode.OK, "");
            return;
        }
        sessions.close(sessionId);
        ObjectNode response = MAPPER.createObjectNode();
        response.put("code", ErrorCode.OK.name());
        response.put("session", sessionId);
        write(ctx, response);
    }

    private static Map<String, Object> object(JsonNode node) {
        Map<String, Object> values = new LinkedHashMap<>();
        node.properties().forEach(entry -> values.put(entry.getKey(), valueDeep(entry.getValue())));
        return values;
    }

    private static Object valueDeep(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isObject()) {
            return object(node);
        }
        if (node.isArray()) {
            List<Object> values = new ArrayList<>(node.size());
            for (JsonNode child : node) {
                values.add(valueDeep(child));
            }
            return values;
        }
        return value(node);
    }

    private void error(ChannelHandlerContext ctx, String session, String queryId, ErrorCode code, String message) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("code", code.name());
        node.put("message", message == null ? "" : message);
        if (session != null) {
            node.put("session", session);
        }
        if (queryId != null) {
            node.put("queryId", queryId);
        }
        write(ctx, node);
    }

    private static void putResult(ObjectNode node, QueryResult result) {
        node.set("columns", MAPPER.valueToTree(result.columns()));
        node.set("rows", MAPPER.valueToTree(result.rows()));
        node.put("rowsAffected", result.rowsAffected());
        if (result.collectedAt() != null) {
            node.put("collectedAt", result.collectedAt());
        }
    }

    private void write(ChannelHandlerContext ctx, ObjectNode node) {
        try {
            ctx.writeAndFlush(MAPPER.writeValueAsString(node) + "\n");
        } catch (JsonProcessingException ex) {
            log.error("响应序列化失败", ex);
            ctx.close();
        }
    }
}
