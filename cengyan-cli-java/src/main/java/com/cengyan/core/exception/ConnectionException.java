package com.cengyan.core.exception;

import java.util.Map;

/**
 * 无法建立连接，或连接在收到响应前断开。
 */
public class ConnectionException extends TransportException {
    private static final long serialVersionUID = 1L;

    public ConnectionException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ConnectionException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
