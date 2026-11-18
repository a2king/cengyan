package com.cengyan.core.exception;

import java.util.Map;

/**
 * 结果被截断，或触达度/前沿/路径/结果大小上限；{@link #response()} 里可能带部分数据。
 */
public class LimitExceededException extends QueryException {
    private static final long serialVersionUID = 1L;

    public LimitExceededException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public LimitExceededException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
