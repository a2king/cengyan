import threading
import unittest

from pycengyan import ClusterClient
from pycengyan.compute_transport import ComputeTransport
from pycengyan.exceptions import (
    AuthenticationError,
    CengyanError,
    CommitUncertainError,
    ConflictError,
    ConnectionError,
    LimitExceededError,
    QueryError,
    RequestConflictError,
    ResourceExhaustedError,
    error_for_response,
)


class Recorder:
    """按顺序返回预置响应，并记录每次请求的 token 和目标节点。"""

    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def request(self, payload, timeout=None, *, write=False):
        self.requests.append(dict(payload))
        outcome = self.responses.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def client_with(responses):
    client = ClusterClient([("a", 1)], password="p")
    client._transport = Recorder(responses)
    return client


class ErrorMappingTest(unittest.TestCase):
    def test_every_public_code_maps_to_a_specific_type(self):
        self.assertIsInstance(error_for_response({"code": "UNAUTHENTICATED"}), AuthenticationError)
        self.assertIsInstance(error_for_response({"code": "WRITE_CONFLICT"}), ConflictError)
        self.assertIsInstance(error_for_response({"code": "REQUEST_CONFLICT"}), RequestConflictError)
        self.assertIsInstance(error_for_response({"code": "TRUNCATED"}), LimitExceededError)
        self.assertIsInstance(error_for_response({"code": "QUERY_MEMORY_EXCEEDED"}), ResourceExhaustedError)
        self.assertIsInstance(error_for_response({"code": "FAILED"}), CommitUncertainError)

    def test_query_failures_are_query_errors_and_unknown_codes_stay_generic(self):
        self.assertIsInstance(error_for_response({"code": "NAME_CONFLICT"}), QueryError)
        self.assertIsInstance(error_for_response({"code": "CONSTRAINT"}), QueryError)
        unknown = error_for_response({"code": "SOMETHING_NEW"})
        self.assertIs(type(unknown), CengyanError)
        self.assertEqual("SOMETHING_NEW", unknown.code)

    def test_commit_uncertain_is_not_a_query_error(self):
        # 业务错误表示"没生效"，提交未确认表示"不知道"，调用方处理方式完全不同。
        self.assertNotIsInstance(CommitUncertainError("", "FAILED"), QueryError)


class TokenRefreshTest(unittest.TestCase):
    def test_expired_token_relogs_in_and_reuses_positions(self):
        client = client_with([
            {"code": "OK", "token": "t1"},
            {"code": "UNAUTHENTICATED", "message": "令牌认证版本已失效"},
            {"code": "OK", "token": "t2"},
            {"code": "OK", "columns": [], "rows": []},
        ])
        Result = client.execute("MATCH (n) RETURN n")
        self.assertEqual("t2", client.token)
        self.assertEqual(["execute", "execute"], [r["op"] for r in client._transport.requests[1::2]])
        self.assertEqual("t1", client._transport.requests[1]["token"])
        self.assertEqual("t2", client._transport.requests[3]["token"])
        self.assertEqual([], Result.rows)

    def test_concurrent_threads_share_one_relogin(self):
        # 两个线程同时发现令牌过期时，只应登录一次。
        client = client_with([{"code": "OK", "token": "t1"}])
        client.login()
        logins = []
        client._transport = Recorder([])

        def fake_request(payload, timeout=None, *, write=False):
            if payload["op"] == "login":
                logins.append(payload)
                return {"code": "OK", "token": "t2"}
            return {"code": "OK", "columns": [], "rows": []}

        client._transport.request = fake_request
        stale = "t1"
        gate = threading.Barrier(2)
        results = []

        def worker():
            gate.wait()
            results.append(client._relogin(stale))

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(["t2", "t2"], results)
        self.assertEqual(1, len(logins))


class AdminModeTest(unittest.TestCase):
    def test_admin_is_read_only_unless_write_is_explicit(self):
        observed = []

        def fake_request(payload, timeout=None, *, write=False):
            if payload["op"] == "login":
                return {"code": "OK", "token": "t"}
            observed.append((payload["command"], write))
            return {"code": "OK"}

        client = client_with([])
        client._transport.request = fake_request
        client.admin("stats", graph="g")
        client.admin("showMembers")
        client.admin("setConfiguration", write=True, voters=[])
        self.assertEqual([("stats", False), ("showMembers", False), ("setConfiguration", True)], observed)


class ConnectionModelTest(unittest.TestCase):
    def test_each_thread_gets_its_own_connection(self):
        opened = []

        class Connection:
            def request(self, payload, timeout=None):
                return {"code": "OK", "thread": threading.current_thread().name}

            def close(self):
                pass

        def factory(host, port, timeout, **kwargs):
            opened.append(threading.current_thread().name)
            return Connection()

        transport = ComputeTransport([("a", 1)], transport_factory=factory)
        results = []
        threads = [threading.Thread(target=lambda: results.append(transport.request({"op": "ping"})))
                   for _ in range(4)]
        try:
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(4, len(opened))
            self.assertEqual(4, {r["thread"] for r in results} and len(results))
        finally:
            transport.close()

    def test_tls_flag_reaches_the_builtin_transport(self):
        from unittest.mock import patch

        captured = {}

        class RecordingTcp:
            def __init__(self, host, port, timeout, tls=False):
                captured["tls"] = tls

            def request(self, payload, timeout=None):
                return {"code": "OK"}

            def close(self):
                pass

        # _open 用 `self._factory is TcpTransport` 判断是否为内置传输，
        # 因此替换模块里的 TcpTransport 名字才能命中 tls 分支。
        with patch("pycengyan.compute_transport.TcpTransport", RecordingTcp):
            transport = ComputeTransport([("a", 1)], transport_factory=RecordingTcp, tls=True)
            try:
                transport._open(("a", 1), 5)
            finally:
                transport.close()
        self.assertIs(True, captured["tls"])

    def test_custom_factory_keeps_three_argument_signature(self):
        calls = []

        def factory(host, port, timeout):  # 没有 tls 参数的老工厂必须继续可用
            calls.append((host, port))

            class Connection:
                def request(self, payload, timeout=None):
                    return {"code": "OK"}

                def close(self):
                    pass

            return Connection()

        transport = ComputeTransport([("a", 1)], transport_factory=factory, tls=True)
        try:
            self.assertEqual("OK", transport.request({"op": "ping"})["code"])
            self.assertEqual([("a", 1)], calls)
        finally:
            transport.close()

    def test_close_releases_every_thread_connection(self):
        closed = []

        class Connection:
            def request(self, payload, timeout=None):
                return {"code": "OK"}

            def close(self):
                closed.append(threading.current_thread().name)

        transport = ComputeTransport([("a", 1)], transport_factory=lambda *a, **k: Connection())

        def worker():
            transport.request({"op": "ping"})

        threads = [threading.Thread(target=worker) for _ in range(3)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        transport.close()
        self.assertEqual(3, len(closed))
        with self.assertRaises(ConnectionError):
            transport.request({"op": "ping"})


if __name__ == "__main__":
    unittest.main()
