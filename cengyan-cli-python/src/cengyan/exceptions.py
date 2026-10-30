from __future__ import annotations


class CengyanError(RuntimeError):
    def __init__(self, message="", code="UNKNOWN", response=None):
        self.code = code
        self.response = response or {}
        self.query_id = self.response.get("queryId") if isinstance(self.response, dict) else None
        super().__init__(f"{code} {message}".strip())


class TransportError(CengyanError): pass
class ConnectionError(TransportError): pass
class ReadTimeoutError(TransportError): pass
class ProtocolError(TransportError): pass
class AuthenticationError(CengyanError): pass
class AuthorizationError(CengyanError): pass
class SessionClosedError(CengyanError): pass
class GraphContextError(CengyanError): pass
class QueryError(CengyanError): pass
class SyntaxError(QueryError): pass
class TypeMismatchError(QueryError): pass
class GraphNotFoundError(QueryError): pass
class BusyError(QueryError): pass
class QueryCanceledError(QueryError): pass
class UniqueViolationError(QueryError): pass
class BulkInsertError(QueryError): pass
class ResultError(CengyanError): pass
class UnsupportedError(CengyanError): pass


_ERRORS = {
    "FORBIDDEN": AuthorizationError,
    "SYNTAX": SyntaxError,
    "TYPE_MISMATCH": TypeMismatchError,
    "GRAPH_NOT_FOUND": GraphNotFoundError,
    "BUSY": BusyError,
    "QUERY_CANCELED": QueryCanceledError,
    "UNIQUE_VIOLATION": UniqueViolationError,
}


def raise_for_response(response):
    if response.get("code") == "OK":
        return response
    code = response.get("code", "UNKNOWN")
    cls = _ERRORS.get(code, CengyanError)
    raise cls(response.get("message", ""), code, response)
