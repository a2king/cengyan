"""错误码到异常类型的映射，以及读写对"提交状态未知"的不同处理。"""
import builtins
import unittest

import pycengyan
from pycengyan import ClusterClient
from pycengyan.exceptions import (
    BusyError,
    CengyanConnectionError,
    ClusterStateError,
    CommitUncertainError,
    GqlSyntaxError,
    InternalError,
    InvalidArgumentError,
    LimitExceededError,
    QueryTimeoutError,
    RequestTooLargeError,
    error_for_response,
)


class Recorder:
    def __init__(self, handler):
        self.handler = handler
        self.requests = []
        self.closed = 0

    def request(self, payload, timeout=None, *, write=False, address=None):
        self.requests.append((dict(payload), write))
        return self.handler(payload)

    def close(self):
        self.closed += 1


def client_with(handler):
    client = ClusterClient([("a", 1)], password="p")
    client._transport = Recorder(handler)
    client.token = "t"
    return client


class UncertainCodesTest(unittest.TestCase):
    def test_failed_is_uncertain_only_for_writes(self):
        self.assertIsInstance(error_for_response({"code": "FAILED"}, write=False), InternalError)
        self.assertIsInstance(error_for_response({"code": "FAILED"}, write=True), CommitUncertainError)
        # 读写未知时保守处理。
        self.assertIsInstance(error_for_response({"code": "FAILED"}), CommitUncertainError)

    def test_query_timeout_is_uncertain_only_for_writes(self):
        self.assertIsInstance(error_for_response({"code": "QUERY_TIMEOUT"}, write=False), QueryTimeoutError)
        self.assertIsInstance(error_for_response({"code": "QUERY_TIMEOUT"}, write=True), CommitUncertainError)

    def test_client_passes_read_write_mode_to_mapping(self):
        client = client_with(lambda payload: {"code": "FAILED", "message": "NullPointerException"})
        with self.assertRaises(InternalError):
            client.graph("g").read("MATCH (n) RETURN n")
        with self.assertRaises(CommitUncertainError):
            client.graph("g").write("INSERT (n:P {id: 1})")

    def test_server_codes_map_to_specific_types(self):
        expected = {
            "INVALID_ARGUMENT": InvalidArgumentError,
            "BATCH_TOO_LARGE": RequestTooLargeError,
            "RESULT_TOO_LARGE": LimitExceededError,
            "PATH_LIMIT": LimitExceededError,
            "NODE_NOT_FOUND": ClusterStateError,
            "GROUP_UNAVAILABLE": ClusterStateError,
            "CLUSTER_MAINTENANCE": ClusterStateError,
            "RESERVATION_EXPIRED": BusyError,
            "TRANSACTION_TIMEOUT": QueryTimeoutError,
            "TRANSACTION_UNKNOWN": CommitUncertainError,
        }
        for code, cls in expected.items():
            self.assertIs(cls, type(error_for_response({"code": code}, write=False)), code)


class WriteIdentityTest(unittest.TestCase):
    def test_relogin_resends_write_with_same_request_id(self):
        sent = []

        def handler(payload):
            if payload["op"] == "login":
                return {"code": "OK", "token": "t2"}
            sent.append(payload["requestId"])
            return {"code": "UNAUTHENTICATED"} if len(sent) == 1 else {"code": "OK"}

        client = client_with(handler)
        client.graph("g").write("INSERT (n:P {id: 1})")
        self.assertEqual(2, len(sent))
        self.assertEqual(sent[0], sent[1])

    def test_caller_request_id_is_kept_and_reads_get_none(self):
        client = client_with(lambda payload: {"code": "OK"})
        client.request({"op": "execute", "query": "q", "requestId": "mine"}, write=True)
        client.request({"op": "execute", "query": "q"})
        self.assertEqual("mine", client._transport.requests[0][0]["requestId"])
        self.assertNotIn("requestId", client._transport.requests[1][0])


class ClientSafetyTest(unittest.TestCase):
    def test_admin_values_cannot_override_protocol_fields(self):
        client = client_with(lambda payload: {"code": "OK"})
        for field in ("op", "token", "session"):
            with self.assertRaises(ValueError):
                client.admin("stats", **{field: "x"})
        self.assertEqual([], client._transport.requests)

    def test_failed_login_in_context_manager_closes_transport(self):
        client = client_with(lambda payload: {"code": "UNAUTHENTICATED", "message": "密码错误"})
        transport = client._transport
        with self.assertRaises(pycengyan.AuthenticationError):
            with client:
                pass
        self.assertEqual(1, transport.closed)

    def test_login_without_token_is_an_authentication_error(self):
        client = client_with(lambda payload: {"code": "OK"})
        with self.assertRaises(pycengyan.AuthenticationError):
            client.login()


class ExportTest(unittest.TestCase):
    def test_star_import_does_not_shadow_builtins(self):
        namespace = {}
        exec("from pycengyan import *", namespace)
        self.assertNotIn("ConnectionError", namespace)
        self.assertNotIn("SyntaxError", namespace)
        self.assertIs(builtins.ConnectionError, ConnectionError)

    def test_legacy_names_remain_available(self):
        self.assertIs(pycengyan.ConnectionError, CengyanConnectionError)
        self.assertIs(pycengyan.SyntaxError, GqlSyntaxError)
        for name in pycengyan.__all__:
            self.assertTrue(hasattr(pycengyan, name), name)


if __name__ == "__main__":
    unittest.main()
