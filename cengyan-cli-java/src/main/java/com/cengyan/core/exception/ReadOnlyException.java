package com.cengyan.core.exception;

import java.util.Map;

/**
 * 图或分片处于只读状态。
 */
public class ReadOnlyException extends QueryException {
    private static final long serialVersionUID = 1L;

    public ReadOnlyException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ReadOnlyException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
