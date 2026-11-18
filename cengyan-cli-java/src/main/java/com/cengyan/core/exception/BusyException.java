package com.cengyan.core.exception;

import java.util.Map;

/**
 * 服务端暂时没有执行名额，请求没有执行，稍后重试即可。
 */
public class BusyException extends QueryException {
    private static final long serialVersionUID = 1L;

    public BusyException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public BusyException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
