package com.cengyan.core.exception;

import java.util.Map;

/**
 * 写请求的提交状态无法确认：可能已提交，也可能未提交。
 *
 * <p>调用方不得盲目重试。应使用 {@link #requestId()} 重新确认，或在服务端幂等窗口内
 * 以同一 requestId 重放该请求（批量写入用 {@code startBatch(failedBatch)} 重放）。
 */
public class CommitUncertainException extends TransportException {
    private static final long serialVersionUID = 1L;

    public CommitUncertainException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public CommitUncertainException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
