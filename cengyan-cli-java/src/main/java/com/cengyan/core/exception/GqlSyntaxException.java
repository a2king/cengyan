package com.cengyan.core.exception;

import java.util.Map;

/**
 * GQL 语法错误。
 */
public class GqlSyntaxException extends QueryException {
    private static final long serialVersionUID = 1L;

    public GqlSyntaxException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public GqlSyntaxException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
