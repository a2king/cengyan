"""异常层次。

分类约定：
- ``TransportError``：连接、超时、协议层问题，请求是否送达不确定；
- ``CommitUncertainError``：写请求提交状态无法确认，绝不能盲目重试；
- ``QueryError``：服务端明确拒绝，请求没有生效；
- ``CengyanError``：其余（认证、集群状态、未知码）。

``CengyanConnectionError`` / ``GqlSyntaxError`` 是正式名称。旧名 ``ConnectionError`` /
``SyntaxError`` 仍可用 ``pycengyan.ConnectionError`` 访问，但不在 ``__all__`` 里，
避免 ``from pycengyan import *`` 覆盖 Python 内置异常。
"""
from __future__ import annotations


class CengyanError(RuntimeError):
    def __init__(self, message="", code="UNKNOWN", response=None):
        self.code = code
        self.response = response or {}
        self.query_id = self.response.get("queryId") if isinstance(self.response, dict) else None
        # 批次级进度只在 bulk 路径上填充，单条执行时为 None。
        self.bulk_progress = None
        super().__init__(f"{code} {message}".strip())


# --- 传输层 ---------------------------------------------------------------
class TransportError(CengyanError): pass
class CengyanConnectionError(TransportError): pass
class ReadTimeoutError(TransportError): pass
class ProtocolError(TransportError): pass


class CommitUncertainError(TransportError):
    """写请求的提交状态无法确认：可能已提交，也可能未提交。

    调用方不得盲目重试。应使用异常上的 ``requestId`` 重新确认，或在幂等窗口内
    以同一 requestId 重放该批次。
    """


# --- 认证与权限 -----------------------------------------------------------
class AuthenticationError(CengyanError):
    """令牌缺失、过期或凭据被撤销；``ClusterClient`` 会先尝试重新登录一次。"""


class AuthorizationError(CengyanError): pass


# --- 查询 ----------------------------------------------------------------
class QueryError(CengyanError): pass

class GqlSyntaxError(QueryError): pass
class TypeMismatchError(QueryError): pass
class TypeNotFoundError(QueryError): pass
class GraphNotFoundError(QueryError): pass
class GraphContextError(QueryError): pass
class NameConflictError(QueryError): pass
class SchemaMismatchError(QueryError): pass
class ConstraintError(QueryError): pass
class QueryCanceledError(QueryError): pass
class UniqueViolationError(QueryError): pass
class ReadOnlyError(QueryError): pass
class InvalidArgumentError(QueryError): pass


class BusyError(QueryError):
    """服务端暂时没有执行名额，请求没有执行，稍后重试即可。"""


class QueryTimeoutError(QueryError):
    """查询或事务在截止时间前没有完成。读请求和已明确回滚的事务使用此类型；
    写请求超时且服务端无法确认结果时抛 ``CommitUncertainError``。"""


class LimitExceededError(QueryError):
    """结果被截断或触达度/前沿/路径/结果大小上限；异常的 ``response`` 里可能带部分数据。"""


class RequestTooLargeError(QueryError):
    """请求或批次超过服务端大小上限，请求没有被执行。"""


class ResourceExhaustedError(QueryError):
    """查询内存或临时空间不足。"""


class ConflictError(QueryError):
    """并发写冲突，原样重试通常可以成功。"""


class RequestConflictError(ConflictError):
    """同一 requestId 对应了不同的内容或目标，重试不会成功，必须换 id 或修数据。"""


class BulkInsertError(QueryError): pass


# --- 集群状态 -------------------------------------------------------------
class ClusterStateError(CengyanError): pass
class InternalError(CengyanError): pass
class UnsupportedError(CengyanError): pass


class ResultError(CengyanError, ValueError):
    """结果形状不符合预期。继承 ValueError，兼容旧代码的 ``except ValueError``。"""


# 兼容旧名称（与 Python 内置异常同名，不放进 __all__）。
ConnectionError = CengyanConnectionError
SyntaxError = GqlSyntaxError


_ERRORS = {
    "FORBIDDEN": AuthorizationError,
    "UNAUTHENTICATED": AuthenticationError,
    "SYNTAX": GqlSyntaxError,
    "TYPE_MISMATCH": TypeMismatchError,
    "DIVIDE_BY_ZERO": QueryError,
    "TYPE_NOT_FOUND": TypeNotFoundError,
    "GRAPH_NOT_FOUND": GraphNotFoundError,
    "DROP_ALIAS_AS_GRAPH": GraphContextError,
    "NAME_CONFLICT": NameConflictError,
    "SCHEMA_MISMATCH": SchemaMismatchError,
    "SCHEMA_VERSION_MISMATCH": SchemaMismatchError,
    "CONSTRAINT": ConstraintError,
    "TYPE_NOT_EMPTY": ConstraintError,
    "ID_CONFLICT": ConstraintError,
    "UNIQUE_VIOLATION": UniqueViolationError,
    "INVALID_ARGUMENT": InvalidArgumentError,
    "PROTOCOL_ERROR": InvalidArgumentError,
    "BUSY": BusyError,
    "RESERVATION_EXPIRED": BusyError,
    "QUERY_CANCELED": QueryCanceledError,
    "QUERY_TIMEOUT": QueryTimeoutError,
    # 事务协调器已判定回滚，确定没有生效。
    "TRANSACTION_TIMEOUT": QueryTimeoutError,
    "TRANSACTION_UNKNOWN": CommitUncertainError,
    "QUERY_MEMORY_EXCEEDED": ResourceExhaustedError,
    "VERTEX_DEGREE_EXCEEDED": LimitExceededError,
    "TRUNCATED": LimitExceededError,
    "FRONTIER_EXCEEDED": LimitExceededError,
    "PATH_LIMIT": LimitExceededError,
    "PATH_LIMIT_EXCEEDED": LimitExceededError,
    "RESULT_TOO_LARGE": LimitExceededError,
    "BATCH_TOO_LARGE": RequestTooLargeError,
    "REQUEST_TOO_LARGE": RequestTooLargeError,
    "SPACE_READONLY": ReadOnlyError,
    "PART_READONLY": ReadOnlyError,
    "CROSS_PART_UNSUPPORTED": UnsupportedError,
    "UNSUPPORTED": UnsupportedError,
    "WRITE_CONFLICT": ConflictError,
    "REQUEST_CONFLICT": RequestConflictError,
    "NODE_NOT_DRAINED": ClusterStateError,
    "NODE_NOT_FOUND": ClusterStateError,
    "NODE_DELETING": ClusterStateError,
    "NODE_CONFLICT": ClusterStateError,
    "PART_NOT_FOUND": ClusterStateError,
    "GROUP_UNAVAILABLE": ClusterStateError,
    "NOT_LEADER": ClusterStateError,
    "STALE_CONTROLLER": ClusterStateError,
    "INSUFFICIENT_NODES": ClusterStateError,
    "TOPOLOGY_CONFLICT": ClusterStateError,
    "ENDPOINT_NOT_FOUND": ClusterStateError,
    "CONFIG_MISMATCH": ClusterStateError,
    "REPLICA_MISMATCH": ClusterStateError,
    "GRAPH_STATE": ClusterStateError,
    "CLUSTER_MAINTENANCE": ClusterStateError,
    "INTERNAL": InternalError,
    "CORRUPT_DATA": InternalError,
    "STREAM_FAILED": InternalError,
}

# 服务端对这些码不区分"执行前失败"和"提交途中失败"：
# 写请求（或读写未知时）一律按提交状态未知处理，读请求按普通错误处理。
# FAILED 同时也是服务端兜底码（任何未分类异常），读请求遇到它不代表有写入发生。
_UNCERTAIN_FOR_WRITES = {
    "FAILED": InternalError,
    "QUERY_TIMEOUT": QueryTimeoutError,
}


def error_for_response(response, *, write=None):
    """按响应码构造异常，不抛出。

    ``write`` 为 True 或 None（未知）时，``FAILED`` / ``QUERY_TIMEOUT`` 映射为
    ``CommitUncertainError``；为 False（只读请求）时映射为普通错误。
    """
    code = response.get("code", "UNKNOWN")
    if code in _UNCERTAIN_FOR_WRITES:
        cls = CommitUncertainError if write is not False else _UNCERTAIN_FOR_WRITES[code]
    else:
        cls = _ERRORS.get(code, CengyanError)
    return cls(response.get("message", ""), code, response)


def raise_for_response(response, *, write=None):
    if response.get("code") == "OK":
        return response
    raise error_for_response(response, write=write)
