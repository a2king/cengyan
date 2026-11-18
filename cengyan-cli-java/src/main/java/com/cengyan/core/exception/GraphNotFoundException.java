package com.cengyan.core.exception;

import java.util.Map;

/**
 * 图不存在。
 */
public class GraphNotFoundException extends QueryException {
    private static final long serialVersionUID = 1L;

    public GraphNotFoundException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public GraphNotFoundException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
