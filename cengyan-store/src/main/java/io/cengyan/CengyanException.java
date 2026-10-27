package io.cengyan;

/**
 * 带错误码的失败。查询线程把它转成一行 JSON 响应，不附带半截结果。
 */
public final class CengyanException extends RuntimeException {
    private final ErrorCode code;

    public CengyanException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public CengyanException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() {
        return code;
    }
}
