package com.cengyan.core.exception;

import java.util.Map;

/**
 * 查询被取消。
 */
public class QueryCanceledException extends QueryException {
    private static final long serialVersionUID = 1L;

    public QueryCanceledException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public QueryCanceledException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
