package com.cengyan.core.exception;

import java.util.Map;

/**
 * 名字已被占用。
 */
public class NameConflictException extends QueryException {
    private static final long serialVersionUID = 1L;

    public NameConflictException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public NameConflictException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
