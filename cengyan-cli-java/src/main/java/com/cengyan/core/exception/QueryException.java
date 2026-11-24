package com.cengyan.core.exception;

import java.util.Map;

/**
 * 服务端明确拒绝了请求，请求没有生效。
 */
public class QueryException extends CengyanException {
    private static final long serialVersionUID = 1L;

    public QueryException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public QueryException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
