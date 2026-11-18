package com.cengyan.core.exception;

import java.util.Map;

/**
 * 当前用户没有执行该操作的权限。
 */
public class AuthorizationException extends CengyanException {
    private static final long serialVersionUID = 1L;

    public AuthorizationException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public AuthorizationException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
