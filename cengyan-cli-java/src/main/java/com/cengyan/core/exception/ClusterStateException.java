package com.cengyan.core.exception;

import java.util.Map;

/**
 * 集群拓扑、成员或维护状态不允许执行该请求。
 */
public class ClusterStateException extends CengyanException {
    private static final long serialVersionUID = 1L;

    public ClusterStateException(String message, String code, Map<String, ?> response) {
        super(message, code, response);
    }

    public ClusterStateException(String message, String code, Map<String, ?> response, Throwable cause) {
        super(message, code, response, cause);
    }
}
