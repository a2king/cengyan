package com.cengyan.core.exception;

import java.util.Map;

/**
 * 批量写入的数据在发送前被拒绝（缺 id、无法编码、单行过大等）。
 */
public class BulkInsertException extends QueryException {
    private static final long serialVersionUID = 1L;

    public BulkInsertException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public BulkInsertException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
