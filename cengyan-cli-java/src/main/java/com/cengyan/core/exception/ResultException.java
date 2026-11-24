package com.cengyan.core.exception;

import java.util.Map;

/**
 * 结果形状或类型不符合预期（如 {@code one()} 行数不为 1、取值类型不对、POJO 映射失败）。
 */
public class ResultException extends CengyanException {
    private static final long serialVersionUID = 1L;

    public ResultException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ResultException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
