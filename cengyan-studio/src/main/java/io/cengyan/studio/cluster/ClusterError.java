package io.cengyan.studio.cluster;

import java.util.Map;

/** 带错误码的集群管理异常，HTTP 层原样返回 code、message 和 details。 */
public final class ClusterError extends RuntimeException {
    private final String code;
    private final Map<String, String> details;

    public ClusterError(String code, String message) {
        this(code, message, Map.of());
    }

    public ClusterError(String code, String message, Map<String, String> details) {
        super(message);
        this.code = code;
        this.details = Map.copyOf(details);
    }

    public ClusterError(String code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
        this.details = Map.of();
    }

    public String code() { return code; }

    /** 例如配置校验失败时：参数键 → 错误说明。 */
    public Map<String, String> details() { return details; }
}
