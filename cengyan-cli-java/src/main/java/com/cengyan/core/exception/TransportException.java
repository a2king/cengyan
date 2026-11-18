package com.cengyan.core.exception;

import java.util.Map;

/**
 * 连接、超时或协议层失败：请求是否送达服务端不确定。
 */
public class TransportException extends CengyanException {
    private static final long serialVersionUID = 1L;

    public TransportException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public TransportException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
