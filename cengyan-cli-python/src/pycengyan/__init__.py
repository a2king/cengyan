__version__ = "1.0.0"

from .cluster import ClusterClient, ClusterGraph
from .compute_transport import ComputeTransport
from .exceptions import (
    AuthenticationError,
    AuthorizationError,
    BulkInsertError,
    BusyError,
    CengyanError,
    ClusterStateError,
    CommitUncertainError,
    ConflictError,
    ConnectionError,
    ConstraintError,
    GraphContextError,
    GraphNotFoundError,
    InternalError,
    LimitExceededError,
    NameConflictError,
    ProtocolError,
    QueryCanceledError,
    QueryError,
    ReadOnlyError,
    ReadTimeoutError,
    RequestConflictError,
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

# 异常必须在 __all__ 里：调用方需要 except cengyan.CommitUncertainError 这类写法。
__all__ = [
    "__version__",
    "ClusterClient", "ClusterGraph", "ComputeTransport", "Result",
    "CengyanError", "TransportError", "CommitUncertainError", "ConnectionError",
    "ReadTimeoutError", "ProtocolError", "AuthenticationError", "AuthorizationError",
    "QueryError", "SyntaxError", "TypeMismatchError", "TypeNotFoundError", "GraphNotFoundError",
    "GraphContextError", "NameConflictError", "SchemaMismatchError", "ConstraintError",
    "BusyError", "QueryCanceledError", "UniqueViolationError", "ReadOnlyError",
    "LimitExceededError", "ResourceExhaustedError", "ConflictError", "RequestConflictError",
    "BulkInsertError", "ClusterStateError", "InternalError", "UnsupportedError", "ResultError",
    "error_for_response", "raise_for_response",
]
