package com.cengyan.core.exception;

import java.util.Map;

/**
 * 服务端内部错误（含服务端兜底码 {@code FAILED} 用于只读请求时）。
 */
public class InternalException extends CengyanException {
    private static final long serialVersionUID = 1L;

    public InternalException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public InternalException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
