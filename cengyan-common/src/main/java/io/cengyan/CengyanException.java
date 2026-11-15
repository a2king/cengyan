package io.cengyan;

/**
 * 公共查询异常，由 Compute 转成带错误码的响应，不附带半截结果。
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
