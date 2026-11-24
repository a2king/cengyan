package com.cengyan.core.exception;

import java.util.Map;

/**
 * 令牌缺失、过期或凭据被撤销；{@code ClusterClient} 会先自动重新登录一次。
 */
public class AuthenticationException extends CengyanException {
    private static final long serialVersionUID = 1L;

    public AuthenticationException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public AuthenticationException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
