package com.cengyan.core.exception;

import java.util.Map;

/**
 * 服务端响应不是合法的协议 JSON。
 */
public class ProtocolException extends TransportException {
    private static final long serialVersionUID = 1L;

    public ProtocolException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ProtocolException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
