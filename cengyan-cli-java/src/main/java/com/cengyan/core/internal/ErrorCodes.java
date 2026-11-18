package com.cengyan.core.internal;

import com.cengyan.core.exception.AuthenticationException;
import com.cengyan.core.exception.AuthorizationException;
import com.cengyan.core.exception.BusyException;
import com.cengyan.core.exception.CengyanException;
import com.cengyan.core.exception.ClusterStateException;
import com.cengyan.core.exception.CommitUncertainException;
import com.cengyan.core.exception.ConflictException;
import com.cengyan.core.exception.ConstraintException;
import com.cengyan.core.exception.GqlSyntaxException;
import com.cengyan.core.exception.GraphContextException;
import com.cengyan.core.exception.GraphNotFoundException;
import com.cengyan.core.exception.InternalException;
import com.cengyan.core.exception.InvalidArgumentException;
import com.cengyan.core.exception.LimitExceededException;
import com.cengyan.core.exception.NameConflictException;
import com.cengyan.core.exception.QueryCanceledException;
import com.cengyan.core.exception.QueryException;
import com.cengyan.core.exception.QueryTimeoutException;
import com.cengyan.core.exception.ReadOnlyException;
import com.cengyan.core.exception.RequestConflictException;
import com.cengyan.core.exception.RequestTooLargeException;
import com.cengyan.core.exception.ResourceExhaustedException;
import com.cengyan.core.exception.SchemaMismatchException;
import com.cengyan.core.exception.TypeMismatchException;
import com.cengyan.core.exception.TypeNotFoundException;
import com.cengyan.core.exception.UniqueViolationException;
import com.cengyan.core.exception.UnsupportedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 服务端错误码到异常类型的映射，与 pycengyan 的 {@code exceptions._ERRORS} 保持一致
 * （由 src/test/resources/contract/error-codes.json 契约测试校验）。
 */
public final class ErrorCodes {
    /** 构造某一类异常。 */
    public interface Factory {
        CengyanException create(String message, String code, Map<String, ?> response);
    }

    private static final Map<String, Factory> ERRORS;
    /**
     * 服务端对这些码不区分"执行前失败"和"提交途中失败"：写请求（或读写未知时）一律按提交状态未知处理，
     * 只读请求按普通错误处理。FAILED 也是服务端的兜底码，只读请求遇到它不代表有写入发生。
     */
    private static final Map<String, Factory> UNCERTAIN_FOR_WRITES;

    static {
        Map<String, Factory> map = new HashMap<String, Factory>();
        map.put("FORBIDDEN", AuthorizationException::new);
        map.put("UNAUTHENTICATED", AuthenticationException::new);
        map.put("SYNTAX", GqlSyntaxException::new);
        map.put("TYPE_MISMATCH", TypeMismatchException::new);
        map.put("DIVIDE_BY_ZERO", QueryException::new);
        map.put("TYPE_NOT_FOUND", TypeNotFoundException::new);
        map.put("GRAPH_NOT_FOUND", GraphNotFoundException::new);
        map.put("DROP_ALIAS_AS_GRAPH", GraphContextException::new);
        map.put("NAME_CONFLICT", NameConflictException::new);
        map.put("SCHEMA_MISMATCH", SchemaMismatchException::new);
        map.put("SCHEMA_VERSION_MISMATCH", SchemaMismatchException::new);
        map.put("CONSTRAINT", ConstraintException::new);
        map.put("TYPE_NOT_EMPTY", ConstraintException::new);
        map.put("ID_CONFLICT", ConstraintException::new);
        map.put("UNIQUE_VIOLATION", UniqueViolationException::new);
        map.put("INVALID_ARGUMENT", InvalidArgumentException::new);
        map.put("PROTOCOL_ERROR", InvalidArgumentException::new);
        map.put("BUSY", BusyException::new);
        map.put("RESERVATION_EXPIRED", BusyException::new);
        map.put("QUERY_CANCELED", QueryCanceledException::new);
        map.put("QUERY_TIMEOUT", QueryTimeoutException::new);
        // 事务协调器已判定回滚，确定没有生效。
        map.put("TRANSACTION_TIMEOUT", QueryTimeoutException::new);
        map.put("TRANSACTION_UNKNOWN", CommitUncertainException::new);
        map.put("QUERY_MEMORY_EXCEEDED", ResourceExhaustedException::new);
        map.put("VERTEX_DEGREE_EXCEEDED", LimitExceededException::new);
        map.put("TRUNCATED", LimitExceededException::new);
        map.put("FRONTIER_EXCEEDED", LimitExceededException::new);
        map.put("PATH_LIMIT", LimitExceededException::new);
        map.put("PATH_LIMIT_EXCEEDED", LimitExceededException::new);
        map.put("RESULT_TOO_LARGE", LimitExceededException::new);
        map.put("BATCH_TOO_LARGE", RequestTooLargeException::new);
        map.put("REQUEST_TOO_LARGE", RequestTooLargeException::new);
        map.put("SPACE_READONLY", ReadOnlyException::new);
        map.put("PART_READONLY", ReadOnlyException::new);
        map.put("CROSS_PART_UNSUPPORTED", UnsupportedException::new);
        map.put("UNSUPPORTED", UnsupportedException::new);
        map.put("WRITE_CONFLICT", ConflictException::new);
        map.put("REQUEST_CONFLICT", RequestConflictException::new);
        for (String code : new String[] {"NODE_NOT_DRAINED", "NODE_NOT_FOUND", "NODE_DELETING", "NODE_CONFLICT", "PART_NOT_FOUND",
                "GROUP_UNAVAILABLE", "NOT_LEADER", "STALE_CONTROLLER", "INSUFFICIENT_NODES", "TOPOLOGY_CONFLICT",
                "ENDPOINT_NOT_FOUND", "CONFIG_MISMATCH", "REPLICA_MISMATCH", "GRAPH_STATE", "CLUSTER_MAINTENANCE"}) {
            map.put(code, ClusterStateException::new);
        }
        map.put("INTERNAL", InternalException::new);
        map.put("CORRUPT_DATA", InternalException::new);
        map.put("STREAM_FAILED", InternalException::new);
        ERRORS = Collections.unmodifiableMap(map);

        Map<String, Factory> uncertain = new HashMap<String, Factory>();
        uncertain.put("FAILED", InternalException::new);
        uncertain.put("QUERY_TIMEOUT", QueryTimeoutException::new);
        UNCERTAIN_FOR_WRITES = Collections.unmodifiableMap(uncertain);
    }

    private ErrorCodes() {
    }

    /**
     * 按响应码构造异常，不抛出。
     *
     * @param write true 写请求；false 只读请求；null 读写未知（按写请求保守处理）
     */
    public static CengyanException error(Map<String, ?> response, Boolean write) {
        Object rawCode = response.get("code");
        String code = rawCode == null ? "UNKNOWN" : String.valueOf(rawCode);
        Object rawMessage = response.get("message");
        String message = rawMessage == null ? "" : String.valueOf(rawMessage);
        Factory factory;
        if (UNCERTAIN_FOR_WRITES.containsKey(code)) {
            factory = Boolean.FALSE.equals(write) ? UNCERTAIN_FOR_WRITES.get(code) : CommitUncertainException::new;
        } else {
            factory = ERRORS.get(code);
            if (factory == null) factory = CengyanException::new;
        }
        return factory.create(message, code, response);
    }

    /** 响应码为 OK 时原样返回，否则抛出对应异常。 */
    public static Map<String, Object> check(Map<String, Object> response, Boolean write) {
        if ("OK".equals(response.get("code"))) return response;
        throw error(response, write);
    }

    /** 已知的错误码（不含 FAILED），供契约测试使用。 */
    public static java.util.Set<String> knownCodes() {
        return ERRORS.keySet();
    }
}
