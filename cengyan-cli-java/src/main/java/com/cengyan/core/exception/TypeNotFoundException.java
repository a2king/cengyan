package com.cengyan.core.exception;

import java.util.Map;

/**
 * 点类型或边类型不存在。
 */
public class TypeNotFoundException extends QueryException {
    private static final long serialVersionUID = 1L;

    public TypeNotFoundException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public TypeNotFoundException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
