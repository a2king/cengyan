package io.cengyan.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cengyan.ErrorCode;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一行一条 JSON。网络线程只解析和应答，不在这里扫 RocksDB。
 */
@ChannelHandler.Sharable
final class LineServer extends SimpleChannelInboundHandler<String> {
    private static final Logger log = LoggerFactory.getLogger(LineServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final QueryRegistry queries;

    LineServer(QueryRegistry queries) {
        this.queries = queries;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String msg) {
        if (msg == null || msg.isBlank()) {
            return;
        }
        try {
            handle(ctx, msg);
        } catch (JsonProcessingException ex) {
            error(ctx, ErrorCode.SYNTAX, "请求不是合法的 JSON");
        } catch (RuntimeException ex) {
            log.warn("处理请求失败：{}", ex.toString());
            error(ctx, ErrorCode.INTERNAL, "处理请求失败");
        }
    }

    private void handle(ChannelHandlerContext ctx, String msg) throws JsonProcessingException {
        JsonNode node = MAPPER.readTree(msg);
        if (!node.isObject()) {
            error(ctx, ErrorCode.SYNTAX, "请求必须是 JSON 对象");
            return;
        }
        String op = node.path("op").asText("");
        switch (op) {
            case "ping" -> ok(ctx);
            case "execute" -> error(ctx, ErrorCode.SYNTAX, "当前版本还不能执行查询");
            case "cancel" -> cancel(ctx, node);
            case "admin" -> error(ctx, ErrorCode.SYNTAX, "当前版本还不能执行管理命令");
            default -> error(ctx, ErrorCode.SYNTAX, "无法识别的操作");
        }
    }

    private void cancel(ChannelHandlerContext ctx, JsonNode node) {
        String queryId = node.path("queryId").asText("");
        if (queryId.isEmpty()) {
            error(ctx, ErrorCode.SYNTAX, "缺少 queryId");
            return;
        }
        if (queries.cancel(queryId, "被终止")) {
            ok(ctx);
        } else {
            error(ctx, ErrorCode.QUERY_CANCELED, "没有找到该查询");
        }
    }

    private void ok(ChannelHandlerContext ctx) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("code", ErrorCode.OK.name());
        write(ctx, node);
    }

    private void error(ChannelHandlerContext ctx, ErrorCode code, String message) {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("code", code.name());
        node.put("message", message);
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

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.warn("连接异常：{}", cause.toString());
        if (ctx.channel().isActive()) {
            error(ctx, ErrorCode.SYNTAX, "无法读取请求");
        }
        ctx.close();
    }
}
