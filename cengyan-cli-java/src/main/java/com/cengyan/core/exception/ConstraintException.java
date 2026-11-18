package com.cengyan.core.exception;

import java.util.Map;

/**
 * 违反约束（非空、ID 不可变更、类型非空等）。
 */
public class ConstraintException extends QueryException {
    private static final long serialVersionUID = 1L;

    public ConstraintException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ConstraintException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
