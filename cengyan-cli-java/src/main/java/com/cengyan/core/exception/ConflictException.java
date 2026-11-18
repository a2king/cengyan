package com.cengyan.core.exception;

import java.util.Map;

/**
 * 并发写冲突，原样重试通常可以成功。
 */
public class ConflictException extends QueryException {
    private static final long serialVersionUID = 1L;

    public ConflictException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ConflictException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
