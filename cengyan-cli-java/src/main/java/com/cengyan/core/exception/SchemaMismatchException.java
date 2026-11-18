package com.cengyan.core.exception;

import java.util.Map;

/**
 * Schema 版本已变化或不匹配。
 */
public class SchemaMismatchException extends QueryException {
    private static final long serialVersionUID = 1L;

    public SchemaMismatchException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public SchemaMismatchException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
