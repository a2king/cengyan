"""异常层次。

``ConnectionError`` / ``SyntaxError`` 与内置同名，本模块内一律指这里定义的类型；
需要区分时用 ``cengyan.exceptions.ConnectionError`` 全限定名。

分类约定：
- ``TransportError``：连接、超时、协议层问题，请求是否送达不确定；
- ``CommitUncertainError``：写请求提交状态无法确认，绝不能盲目重试；
- ``QueryError``：服务端明确拒绝，请求没有生效；
- ``CengyanError``：其余（认证、集群状态、未知码）。
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
class ConnectionError(TransportError): pass
class ReadTimeoutError(TransportError): pass
class ProtocolError(TransportError): pass


class CommitUncertainError(TransportError):
    """写请求的提交状态无法确认：可能已提交，也可能未提交。

    调用方不得盲目重试。应使用异常上的 ``request_id`` 重新确认，或在幂等窗口内
    以同一 requestId 重放该批次。
    """


# --- 认证与权限 -----------------------------------------------------------
class AuthenticationError(CengyanError):
    """令牌缺失、过期或凭据被撤销；``ClusterClient`` 会先尝试重新登录一次。"""


class AuthorizationError(CengyanError): pass


# --- 查询 ----------------------------------------------------------------
class QueryError(CengyanError): pass

class SyntaxError(QueryError): pass
class TypeMismatchError(QueryError): pass
class TypeNotFoundError(QueryError): pass
class GraphNotFoundError(QueryError): pass
class GraphContextError(QueryError): pass
class NameConflictError(QueryError): pass
class SchemaMismatchError(QueryError): pass
class ConstraintError(QueryError): pass
class BusyError(QueryError): pass
class QueryCanceledError(QueryError): pass
class UniqueViolationError(QueryError): pass
class ReadOnlyError(QueryError): pass

class LimitExceededError(QueryError):
    """结果被截断或触达度/前沿/路径上限；``Result.raw`` 里可能有部分数据。"""


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


_ERRORS = {
    "FORBIDDEN": AuthorizationError,
    "UNAUTHENTICATED": AuthenticationError,
    "FAILED": CommitUncertainError,
    "SYNTAX": SyntaxError,
    "TYPE_MISMATCH": TypeMismatchError,
    "TYPE_NOT_FOUND": TypeNotFoundError,
    "GRAPH_NOT_FOUND": GraphNotFoundError,
    "DROP_ALIAS_AS_GRAPH": GraphContextError,
    "NAME_CONFLICT": NameConflictError,
    "SCHEMA_MISMATCH": SchemaMismatchError,
    "CONSTRAINT": ConstraintError,
    "UNIQUE_VIOLATION": UniqueViolationError,
    "BUSY": BusyError,
    "QUERY_CANCELED": QueryCanceledError,
    "QUERY_MEMORY_EXCEEDED": ResourceExhaustedError,
    "VERTEX_DEGREE_EXCEEDED": LimitExceededError,
    "TRUNCATED": LimitExceededError,
    "FRONTIER_EXCEEDED": LimitExceededError,
    "PATH_LIMIT_EXCEEDED": LimitExceededError,
    "SPACE_READONLY": ReadOnlyError,
    "PART_READONLY": ReadOnlyError,
    "CROSS_PART_UNSUPPORTED": UnsupportedError,
    "NODE_NOT_DRAINED": ClusterStateError,
    "INTERNAL": InternalError,
    "WRITE_CONFLICT": ConflictError,
    "REQUEST_CONFLICT": RequestConflictError,
    # 分片路由在服务端内部使用的同义码。
    "PART_NOT_FOUND": ClusterStateError,
}


def error_for_response(response):
    """按响应码构造异常，不抛出。调用方需要对错误补充上下文时使用。"""
    code = response.get("code", "UNKNOWN")
    return _ERRORS.get(code, CengyanError)(response.get("message", ""), code, response)


def raise_for_response(response):
    if response.get("code") == "OK":
        return response
    raise error_for_response(response)
