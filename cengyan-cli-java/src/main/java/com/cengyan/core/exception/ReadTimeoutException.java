package com.cengyan.core.exception;

import java.util.Map;

/**
 * 在截止时间内没有收到响应。
 */
public class ReadTimeoutException extends TransportException {
    private static final long serialVersionUID = 1L;

    public ReadTimeoutException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ReadTimeoutException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
