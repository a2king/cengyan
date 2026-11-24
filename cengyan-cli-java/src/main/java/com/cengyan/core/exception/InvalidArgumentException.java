package com.cengyan.core.exception;

import java.util.Map;

/**
 * 请求参数无效。
 */
public class InvalidArgumentException extends QueryException {
    private static final long serialVersionUID = 1L;

    public InvalidArgumentException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public InvalidArgumentException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
