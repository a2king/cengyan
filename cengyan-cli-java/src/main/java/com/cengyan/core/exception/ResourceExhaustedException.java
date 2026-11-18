package com.cengyan.core.exception;

import java.util.Map;

/**
 * 查询内存或临时空间不足。
 */
public class ResourceExhaustedException extends QueryException {
    private static final long serialVersionUID = 1L;

    public ResourceExhaustedException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ResourceExhaustedException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
