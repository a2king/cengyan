package com.cengyan.core.exception;

import java.util.Map;

/**
 * 违反唯一索引。
 */
public class UniqueViolationException extends QueryException {
    private static final long serialVersionUID = 1L;

    public UniqueViolationException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public UniqueViolationException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
