"""连接复用、线程退出回收、指定地址失败、协议上限与 TLS 参数。"""
import gc
import io
import math
import ssl
import threading
import unittest
from unittest.mock import patch

from pycengyan import ClusterClient, Result
from pycengyan.compute_transport import ComputeTransport
from pycengyan.encoding import MAX_LINE_BYTES, dumps
from pycengyan.exceptions import CengyanConnectionError, RequestTooLargeError
from pycengyan.transport import TcpTransport


class Pool:
    """记录建连与关闭；fail 集合里的地址请求时断线。"""

    def __init__(self, fail=()):
        self.opened, self.closed, self.fail = [], [], set(fail)

    def __call__(self, host, port, timeout):
        pool, address = self, (host, port)
        pool.opened.append(address)

        class Connection:
            def request(self, payload, timeout=None):
                if address in pool.fail:
                    raise CengyanConnectionError("lost", "CONNECTION_ERROR")
                return {"code": "OK", "address": address}

            def close(self):
                pool.closed.append(address)

        return Connection()


class ConnectionReuseTest(unittest.TestCase):
    def test_alternating_preferred_addresses_reuse_connections(self):
        pool = Pool()
        transport = ComputeTransport([("a", 1), ("b", 2)], transport_factory=pool)
        try:
            for _ in range(5):
                transport.request({"op": "bulkInsert"}, write=True, address=("a", 1))
                transport.request({"op": "bulkInsert"}, write=True, address=("b", 2))
            self.assertEqual(2, len(pool.opened))
        finally:
            transport.close()
        self.assertEqual(2, len(pool.closed))

    def test_failed_preferred_address_does_not_move_default_entry(self):
        pool = Pool(fail={("x", 9)})
        transport = ComputeTransport([("a", 1), ("b", 2), ("c", 3)], transport_factory=pool)
        try:
            entry = transport.address
            response = transport.request({"op": "bulkInsert"}, write=True, address=("x", 9))
            # 指定地址断线后改走默认入口重试，默认入口本身不受影响。
            self.assertEqual(entry, response["address"])
            self.assertEqual(entry, transport.address)
        finally:
            transport.close()

    def test_finished_thread_connections_are_closed_without_close(self):
        pool = Pool()
        transport = ComputeTransport([("a", 1)], transport_factory=pool)
        try:
            threads = [threading.Thread(target=lambda: transport.request({"op": "ping"})) for _ in range(3)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            gc.collect()
            self.assertEqual(3, len(pool.closed))
            self.assertEqual({}, transport._live)
        finally:
            transport.close()
        # close() 不会重复关闭已回收的连接。
        self.assertEqual(3, len(pool.closed))

    def test_write_during_close_is_reported_as_uncertain(self):
        transport = ComputeTransport([("a", 1)], transport_factory=Pool(fail={("a", 1)}))
        original = transport._switch

        def close_then_switch(failed):
            transport.close()
            original(failed)

        transport._switch = close_then_switch
        response = transport.request({"op": "bulkInsert", "requestId": "r1"}, write=True)
        self.assertEqual("FAILED", response["code"])
        self.assertEqual("r1", response["requestId"])


class WireLimitTest(unittest.TestCase):
    def test_compact_encoding_rejects_nan(self):
        self.assertEqual('{"a":1,"b":[1,2]}', dumps({"a": 1, "b": [1, 2]}))
        with self.assertRaises(ValueError):
            dumps({"v": math.nan})

    def test_oversized_and_unencodable_requests_never_connect(self):
        transport = TcpTransport("127.0.0.1", 1)
        with patch.object(TcpTransport, "connect", side_effect=AssertionError("不应建连")):
            with self.assertRaises(RequestTooLargeError):
                transport.request({"op": "execute", "query": "x" * (MAX_LINE_BYTES + 1)})
            with self.assertRaises(ValueError):
                transport.request({"op": "execute", "params": {"v": math.inf}})

    def test_oversized_write_is_not_retried_as_uncertain(self):
        attempts = []

        class Oversized:
            def __init__(self, *args):
                pass

            def request(self, payload, timeout=None):
                attempts.append(1)
                raise RequestTooLargeError("too large", "REQUEST_TOO_LARGE")

            def close(self):
                pass

        client = ClusterClient([("a", 1), ("b", 2)], password="p", transport_factory=Oversized)
        client.token = "t"
        with self.assertRaises(RequestTooLargeError):
            client.graph("g").write("INSERT ...")
        self.assertEqual(1, len(attempts))
        client.close()


class TlsOptionTest(unittest.TestCase):
    def test_ssl_context_reaches_builtin_transport(self):
        captured = {}

        class RecordingTcp:
            def __init__(self, host, port, timeout, tls=False, ssl_context=None):
                captured.update(tls=tls, ssl_context=ssl_context)

            def request(self, payload, timeout=None):
                return {"code": "OK", "token": "t"}

            def close(self):
                pass

        context = ssl.create_default_context()
        with patch("pycengyan.compute_transport.TcpTransport", RecordingTcp):
            client = ClusterClient([("a", 1)], password="p", transport_factory=RecordingTcp, ssl_context=context)
            try:
                client.login()
            finally:
                client.close()
        self.assertIs(context, captured["ssl_context"])
        self.assertTrue(captured["tls"])

    def test_ssl_context_implies_tls(self):
        self.assertTrue(TcpTransport("h", 1, ssl_context=ssl.create_default_context()).tls)
        self.assertFalse(TcpTransport("h", 1).tls)


class CsvExportTest(unittest.TestCase):
    def test_nested_values_are_written_as_json(self):
        result = Result({"columns": ["n", "tags"], "rows": [[{"name": "张三"}, ["a", "b"]]]})
        stream = io.StringIO()
        result.write_csv(stream)
        self.assertIn('"{""name"": ""张三""}"', stream.getvalue())
        self.assertIn('"[""a"", ""b""]"', stream.getvalue())


if __name__ == "__main__":
    unittest.main()
