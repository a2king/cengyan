package io.cengyan.distributed;

/** 保留协议错误码，入口只能在明确的协议边界转换错误。 */
public final class ClusterException extends RuntimeException {
    private final String code;
    public ClusterException(String code, String message) { super(message); this.code = code; }
    public ClusterException(String code, String message, Throwable cause) { super(message, cause); this.code = code; }
    public String code() { return code; }
}
