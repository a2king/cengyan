package io.cengyan.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.CengyanException;
import io.cengyan.ErrorCode;
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
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/** 把 execute 放到查询线程。网络线程只解析 JSON 和占一个名额。 */
final class ExecuteService {
    private static final Logger log = LoggerFactory.getLogger(ExecuteService.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final QueryEngine engine;
    private final ServerConfig config;
    private final QueryRegistry queries;
    private final ExecutorService queryPool;
    private final Sessions sessions;
    private final Semaphore permits;
    private final ConcurrentMap<ChannelId, Set<String>> byChannel = new ConcurrentHashMap<>();

    ExecuteService(
            QueryEngine engine,
            ServerConfig config,
            QueryRegistry queries,
            ExecutorService queryPool,
            Sessions sessions) {
        this.engine = engine;
        this.config = config;
        this.queries = queries;
        this.queryPool = queryPool;
        this.sessions = sessions;
        this.permits = new Semaphore(config.queryThreads());
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
        if (timeoutMs <= 0) {
            error(ctx, session, null, ErrorCode.SYNTAX, "超时时间必须为正");
            return;
        }
        boolean acquired;
        try {
            acquired = permits.tryAcquire(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
            return;
        }
        if (!acquired) {
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
            return;
        }
        try {
            queryPool.execute(() -> run(ctx, session, query, params, timeoutMs));
        } catch (RejectedExecutionException ex) {
            permits.release();
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
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
        if (timeoutMs <= 0) {
            error(ctx, session, null, ErrorCode.SYNTAX, "超时时间必须为正");
            return;
        }
        boolean acquired;
        try {
            acquired = permits.tryAcquire(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
            return;
        }
        if (!acquired) {
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
            return;
        }
        try {
            queryPool.execute(() -> runAdmin(
                    ctx, session, command, index,
                    node.path("user").asText(""),
                    node.path("password").asText(""),
                    node.path("role").asText(""),
                    timeoutMs));
        } catch (RejectedExecutionException ex) {
            permits.release();
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
        }
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
        if (timeoutMs <= 0) {
            error(ctx, session, null, ErrorCode.SYNTAX, "超时时间必须为正");
            return;
        }
        if (!acquire(ctx, session)) {
            return;
        }
        try {
            queryPool.execute(() -> runLogin(ctx, session, user, password, timeoutMs));
        } catch (RejectedExecutionException ex) {
            permits.release();
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
        }
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
            node.set("columns", MAPPER.valueToTree(result.columns()));
            node.set("rows", MAPPER.valueToTree(result.rows()));
            node.put("rowsAffected", result.rowsAffected());
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
            String command,
            String index,
            String user,
            String password,
            String role,
            long timeoutMs) {
        QueryHandle handle = QueryHandle.start(timeoutMs);
        queries.register(handle);
        byChannel.computeIfAbsent(ctx.channel().id(), ignored -> ConcurrentHashMap.newKeySet()).add(handle.id());
        try {
            SessionState session = sessions.open(sessionId);
            if (!session.authenticated()) {
                throw new CengyanException(ErrorCode.FORBIDDEN, "请先登录");
            }
            QueryResult result = engine.admin(session, command, index, user, password, role, handle);
            ObjectNode node = MAPPER.createObjectNode();
            node.put("code", ErrorCode.OK.name());
            node.put("session", sessionId);
            node.put("queryId", handle.id());
            node.set("columns", MAPPER.valueToTree(result.columns()));
            node.set("rows", MAPPER.valueToTree(result.rows()));
            node.put("rowsAffected", result.rowsAffected());
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

    void onInactive(ChannelHandlerContext ctx) {
        Set<String> ids = byChannel.remove(ctx.channel().id());
        if (ids == null) {
            return;
        }
        for (String id : ids) {
            queries.cancel(id, "连接已断开");
        }
    }

    private void run(ChannelHandlerContext ctx, String sessionId, String query, Map<String, Object> params, long timeoutMs) {
        QueryHandle handle = QueryHandle.start(timeoutMs);
        queries.register(handle);
        byChannel.computeIfAbsent(ctx.channel().id(), ignored -> ConcurrentHashMap.newKeySet()).add(handle.id());
        try {
            SessionState session = sessions.open(sessionId);
            if (!session.authenticated()) {
                throw new CengyanException(ErrorCode.FORBIDDEN, "请先登录");
            }
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
            node.set("columns", MAPPER.valueToTree(result.columns()));
            node.set("rows", MAPPER.valueToTree(result.rows()));
            node.put("rowsAffected", result.rowsAffected());
            write(ctx, node);
        } catch (CengyanException ex) {
            error(ctx, sessionId, handle.id(), ex.code(), ex.getMessage());
        } catch (RuntimeException ex) {
            log.warn("执行查询失败：{}", ex.toString());
            error(ctx, sessionId, handle.id(), ErrorCode.INTERNAL, "执行查询失败");
        } finally {
            finish(ctx, handle);
        }
    }

    private boolean acquire(ChannelHandlerContext ctx, String session) {
        boolean acquired;
        try {
            acquired = permits.tryAcquire(50, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
            return false;
        }
        if (!acquired) {
            error(ctx, session, null, ErrorCode.BUSY, "查询线程已满");
            return false;
        }
        return true;
    }

    private void finish(ChannelHandlerContext ctx, QueryHandle handle) {
        queries.unregister(handle.id());
        Set<String> ids = byChannel.get(ctx.channel().id());
        if (ids != null) {
            ids.remove(handle.id());
        }
        permits.release();
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

    private void write(ChannelHandlerContext ctx, ObjectNode node) {
        try {
            ctx.writeAndFlush(MAPPER.writeValueAsString(node) + "\n");
        } catch (JsonProcessingException ex) {
            log.error("响应序列化失败", ex);
            ctx.close();
        }
    }
}
