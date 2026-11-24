package com.cengyan.core.exception;

import java.util.Map;

/**
 * 同一 requestId 对应了不同的内容或目标；重试不会成功，必须换 id 或修正数据。
 */
public class RequestConflictException extends ConflictException {
    private static final long serialVersionUID = 1L;

    public RequestConflictException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public RequestConflictException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
