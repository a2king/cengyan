__version__ = "1.0.0"

from .cluster import MAX_BATCH_BYTES, MAX_BATCH_ROWS, ClusterClient, ClusterGraph
from .compute_transport import ComputeTransport
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    BulkInsertError,
    BusyError,
    CengyanConnectionError,
    CengyanError,
    ClusterStateError,
    CommitUncertainError,
    ConflictError,
    ConnectionError,
    ConstraintError,
    GqlSyntaxError,
    GraphContextError,
    GraphNotFoundError,
    InternalError,
    InvalidArgumentError,
    LimitExceededError,
    NameConflictError,
    ProtocolError,
    QueryCanceledError,
    QueryError,
    QueryTimeoutError,
    ReadOnlyError,
    ReadTimeoutError,
    RequestConflictError,
    RequestTooLargeError,
    ResourceExhaustedError,
    ResultError,
    SchemaMismatchError,
    SyntaxError,
    TransportError,
    TypeMismatchError,
    TypeNotFoundError,
    UniqueViolationError,
    UnsupportedError,
    error_for_response,
    raise_for_response,
)
from .result import Result

# 异常必须在 __all__ 里：调用方需要 except pycengyan.CommitUncertainError 这类写法。
# 旧名 ConnectionError / SyntaxError 仍可通过属性访问，但不放进 __all__，
# 避免 `from pycengyan import *` 覆盖 Python 内置异常。
__all__ = [
    "__version__", "MAX_BATCH_ROWS", "MAX_BATCH_BYTES",
    "ClusterClient", "ClusterGraph", "ComputeTransport", "Result",
    "CengyanError", "TransportError", "CommitUncertainError", "CengyanConnectionError",
    "ReadTimeoutError", "ProtocolError", "AuthenticationError", "AuthorizationError",
    "QueryError", "GqlSyntaxError", "TypeMismatchError", "TypeNotFoundError", "GraphNotFoundError",
    "GraphContextError", "NameConflictError", "SchemaMismatchError", "ConstraintError",
    "BusyError", "QueryCanceledError", "QueryTimeoutError", "UniqueViolationError", "ReadOnlyError",
    "InvalidArgumentError", "LimitExceededError", "RequestTooLargeError", "ResourceExhaustedError",
    "ConflictError", "RequestConflictError", "BulkInsertError", "ClusterStateError", "InternalError",
    "UnsupportedError", "ResultError", "error_for_response", "raise_for_response",
]

# 兼容旧名称：显式引用，避免被当作未使用的导入清理掉。
_LEGACY_ALIASES = (ConnectionError, SyntaxError)
