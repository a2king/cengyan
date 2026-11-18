package com.cengyan.core.exception;

import java.util.Map;

/**
 * 查询或事务在截止时间前没有完成。读请求和已明确回滚的事务使用此类型；
 * 写请求超时且服务端无法确认结果时抛 {@link CommitUncertainException}。
 */
public class QueryTimeoutException extends QueryException {
    private static final long serialVersionUID = 1L;

    public QueryTimeoutException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public QueryTimeoutException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
