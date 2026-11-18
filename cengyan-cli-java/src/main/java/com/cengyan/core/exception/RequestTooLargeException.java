package com.cengyan.core.exception;

import java.util.Map;

/**
 * 请求或批次超过服务端大小上限，请求没有被执行。
 */
public class RequestTooLargeException extends QueryException {
    private static final long serialVersionUID = 1L;

    public RequestTooLargeException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public RequestTooLargeException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
