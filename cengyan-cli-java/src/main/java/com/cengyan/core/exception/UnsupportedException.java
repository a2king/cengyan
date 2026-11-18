package com.cengyan.core.exception;

import java.util.Map;

/**
 * 服务端不支持该操作。
 */
public class UnsupportedException extends CengyanException {
    private static final long serialVersionUID = 1L;

    public UnsupportedException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public UnsupportedException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
