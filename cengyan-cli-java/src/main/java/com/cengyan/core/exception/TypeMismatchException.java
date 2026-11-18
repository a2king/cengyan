package com.cengyan.core.exception;

import java.util.Map;

/**
 * 值的类型与 Schema 或表达式不符。
 */
public class TypeMismatchException extends QueryException {
    private static final long serialVersionUID = 1L;

    public TypeMismatchException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public TypeMismatchException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
