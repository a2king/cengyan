package com.cengyan.core.exception;

import com.cengyan.core.BulkProgress;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * SDK 所有异常的基类。
 *
 * <p>分类约定：
 * <ul>
 *   <li>{@link TransportException}：连接、超时、协议层问题，请求是否送达不确定；</li>
 *   <li>{@link CommitUncertainException}：写请求提交状态无法确认，绝不能盲目重试；</li>
 *   <li>{@link QueryException}：服务端明确拒绝，请求没有生效；</li>
 *   <li>其余（认证、集群状态、未知码）直接是本类或其子类。</li>
 * </ul>
 */
public class CengyanException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final String code;
    private final Map<String, Object> response;
    private volatile BulkProgress bulkProgress;

    public CengyanException(String message, String code, Map<String, ?> response) {
        this(message, code, response, null);
    }

    public CengyanException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(format(code, message), cause);
        this.code = code == null || code.isEmpty() ? "UNKNOWN" : code;
        Map<String, Object> copy = new LinkedHashMap<String, Object>();
        if (response != null) copy.putAll(response);
        this.response = Collections.unmodifiableMap(copy);
    }

    private static String format(String code, String message) {
        String prefix = code == null || code.isEmpty() ? "UNKNOWN" : code;
        return message == null || message.isEmpty() ? prefix : prefix + " " + message;
    }

    /** 服务端错误码（如 {@code SYNTAX}），或 SDK 自己的码（如 {@code CONNECTION_ERROR}）。 */
    public String code() {
        return code;
    }

    /** 服务端原始响应（只读）；SDK 本地产生的异常为空表或只含 message。 */
    public Map<String, Object> response() {
        return response;
    }

    /** 服务端分配的查询 ID，没有时为 null。 */
    public String queryId() {
        Object value = response.get("queryId");
        return value == null ? null : String.valueOf(value);
    }

    /**
     * 与本次失败关联的 requestId：批量写入时为失败批次的 id，其余情况取自响应。
     * 写请求提交状态未知时，凭它在幂等窗口内确认或重放。
     */
    public String requestId() {
        BulkProgress progress = bulkProgress;
        if (progress != null && progress.requestId() != null) return progress.requestId();
        Object value = response.get("requestId");
        return value == null ? null : String.valueOf(value);
    }

    /** 批量写入失败时的全局进度；非批量路径为 null。 */
    public BulkProgress bulkProgress() {
        return bulkProgress;
    }

    /** SDK 内部使用：批量写入把失败批次的位置附到异常上。 */
    public void attachBulkProgress(BulkProgress progress) {
        this.bulkProgress = progress;
    }
}
