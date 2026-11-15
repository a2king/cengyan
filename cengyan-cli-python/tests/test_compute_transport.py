import random
import unittest
from unittest.mock import patch

from pycengyan.compute_transport import ComputeTransport
from pycengyan.exceptions import ConnectionError, ProtocolError


class FakeConnections:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.requests = []
        self.closed = 0

    def __call__(self, host, port, timeout):
        owner = self

        class Connection:
            def request(self, payload, timeout=None):
                owner.requests.append((host, port, dict(payload)))
                outcome = owner.outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome

            def close(self):
                owner.closed += 1

        return Connection()


class ComputeTransportTest(unittest.TestCase):
    def test_write_retries_same_request_at_other_nodes(self):
        fake = FakeConnections([ConnectionError("lost"), ConnectionError("lost"), {"code": "OK"}])
        # 三个地址才能保证 _switch 的 offset(1..n-1) 一定换到不同节点。
        with_transport = ComputeTransport([("a", 1), ("b", 2), ("c", 3)], transport_factory=fake, rng=random.Random(1))
        payload = {"op": "write", "graphId": 1, "token": "user-token"}
        try:
            self.assertEqual("OK", with_transport.request(payload, write=True)["code"])
            self.assertEqual(3, len(fake.requests))
            self.assertEqual(1, len({entry[2]["requestId"] for entry in fake.requests}))
            self.assertNotEqual(fake.requests[0][0], fake.requests[1][0])
            self.assertNotIn("requestId", payload)
            self.assertTrue(all(entry[2]["token"] == "user-token" for entry in fake.requests))
        finally:
            with_transport.close()

    def test_four_failed_sends_return_failed_with_original_request_id(self):
        fake = FakeConnections([ConnectionError("lost") for _ in range(4)])
        transport = ComputeTransport([("a", 1), ("b", 2)], transport_factory=fake)
        try:
            response = transport.request({"op": "write", "requestId": "original"}, write=True)
            self.assertEqual("FAILED", response["code"])
            self.assertIn("3", response["message"])
            # 调用方要靠这个 id 在幂等窗口内确认或重放，不能丢。
            self.assertEqual("original", response["requestId"])
            self.assertEqual(4, len(fake.requests))
            self.assertTrue(all(entry[2]["requestId"] == "original" for entry in fake.requests))
        finally:
            transport.close()

    def test_read_disconnect_is_not_retried(self):
        fake = FakeConnections([ConnectionError("lost"), {"code": "OK"}])
        transport = ComputeTransport([("a", 1), ("b", 2)], transport_factory=fake)
        try:
            with self.assertRaises(ConnectionError):
                transport.request({"op": "read"})
            self.assertEqual(1, len(fake.requests))
        finally:
            transport.close()

    def test_read_disconnect_leaves_transport_on_another_node(self):
        fake = FakeConnections([ConnectionError("lost"), {"code": "OK"}])
        transport = ComputeTransport([("a", 1), ("b", 2), ("c", 3)], transport_factory=fake)
        try:
            failed_at = transport.address
            with self.assertRaises(ConnectionError):
                transport.request({"op": "read"})
            # 读不自动重发，但下一次请求必须离开刚才失败的节点。
            self.assertNotEqual(failed_at, transport.address)
            transport.request({"op": "read"})
            self.assertNotEqual(failed_at, fake.requests[1][0])
        finally:
            transport.close()

    def test_business_errors_and_bad_protocol_not_retried(self):
        for outcome in ({"code": "WRITE_CONFLICT"}, ProtocolError("invalid")):
            fake = FakeConnections([outcome])
            transport = ComputeTransport([("a", 1)], transport_factory=fake)
            try:
                if isinstance(outcome, Exception):
                    with self.assertRaises(ProtocolError):
                        transport.request({"op": "write"}, write=True)
                else:
                    self.assertEqual(outcome, transport.request({"op": "write"}, write=True))
                self.assertEqual(1, len(fake.requests))
            finally:
                transport.close()

    def test_address_validation(self):
        for addresses in ([], [("a", 0)], [("a", True)], [("a", 1), ("a", 1)]):
            with self.assertRaises(ValueError):
                ComputeTransport(addresses)

    def test_address_discovery_keeps_current_connection_when_possible(self):
        fake = FakeConnections([{"code": "OK"}])
        transport = ComputeTransport([("a", 1)], transport_factory=fake)
        try:
            transport.request({"op": "ping"})
            transport.update_addresses([("b", 2), ("a", 1), ("a", 1)])
            self.assertEqual(("a", 1), transport.address)
            self.assertEqual(0, fake.closed)
            transport.update_addresses([("b", 2)])
            self.assertEqual(("b", 2), transport.address)
            self.assertEqual(1, fake.closed)
        finally:
            transport.close()

    def test_retry_budget_does_not_restart_original_deadline(self):
        fake = FakeConnections([ConnectionError("lost"), {"code": "OK"}])
        transport = ComputeTransport([("a", 1), ("b", 2)], timeout=1, transport_factory=fake)
        try:
            # 首次计算 deadline、首次发送、第二次尝试。
            with patch("pycengyan.compute_transport.time.monotonic", side_effect=[0.0, 0.1, 1.1]):
                response = transport.request({"op": "write", "requestId": "fixed"}, write=True)
            self.assertEqual("FAILED", response["code"])
            self.assertIn("截止时间", response["message"])
            self.assertEqual(1, len(fake.requests))
            self.assertLessEqual(fake.requests[0][2]["remainingTimeoutMs"], 900)
        finally:
            transport.close()

    def test_infinite_timeouts_are_rejected(self):
        for timeout in (0, -1, float("inf"), float("nan"), True):
            with self.assertRaises(ValueError):
                ComputeTransport([("a", 1)], timeout=timeout)
