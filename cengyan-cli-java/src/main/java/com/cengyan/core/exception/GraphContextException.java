package com.cengyan.core.exception;

import java.util.Map;

/**
 * 图上下文使用不当（如把别名当作图删除）。
 */
public class GraphContextException extends QueryException {
    private static final long serialVersionUID = 1L;

    public GraphContextException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public GraphContextException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
