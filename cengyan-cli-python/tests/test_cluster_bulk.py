import datetime
import decimal
import unittest
import uuid

from pycengyan.cluster import ClusterGraph
from pycengyan.exceptions import BulkInsertError, CommitUncertainError, SyntaxError


class FakeClient:
    def __init__(self, fail_at=None, code="OK"):
        self.payloads = []
        self.options = []
        self.fail_at = fail_at
        self.code = code

    def request(self, payload, **kwargs):
        self.payloads.append(payload)
        self.options.append(kwargs)
        if len(self.payloads) == self.fail_at:
            raise RuntimeError("提交状态无法确认")
        if payload.get("op") == "execute":
            return {"code": "OK", "columns": [], "rows": []}
        if self.code != "OK":
            return {"code": self.code, "message": "服务端拒绝"}
        return {"code": "OK", "rowsAffected": len(payload["rows"]), "skipped": 0, "duplicates": 0}


class ClusterBulkTest(unittest.TestCase):
    def test_query_helpers_mark_read_and_write_modes(self):
        client = FakeClient()
        graph = ClusterGraph(client, "g")
        graph.read("MATCH (n) RETURN n.id")
        graph.write("INSERT (n {id: 1})")
        self.assertEqual([False, True], [options["write"] for options in client.options])

    def test_generator_is_split_and_each_batch_has_own_id(self):
        client = FakeClient()
        result = ClusterGraph(client, "g").insert_nodes("N", ({"id": i} for i in range(5)), batch_size=2, align_partitions=False)
        self.assertEqual(5, result["confirmed"])
        self.assertEqual(3, result["completedBatches"])
        self.assertEqual(3, len({p["requestId"] for p in client.payloads}))
        # 成功的最后一批判号也要交回调用方。
        self.assertEqual(client.payloads[-1]["requestId"], result["requestId"])

    def test_failed_batch_stops_and_reports_only_confirmed_progress(self):
        client = FakeClient(fail_at=2)
        with self.assertRaises(CommitUncertainError) as caught:
            ClusterGraph(client, "g").insert_nodes("N", [{"id": i} for i in range(10)], batch_size=2, align_partitions=False)
        self.assertEqual(2, caught.exception.response["confirmed"])
        self.assertEqual(2, len(client.payloads))
        self.assertEqual(1, caught.exception.response["failedBatch"])
        # 失败批次的 requestId 必须回传，否则无法在幂等窗口内重放。
        self.assertEqual(client.payloads[-1]["requestId"], caught.exception.response["requestId"])

    def test_caller_owned_request_id_is_reproducible_for_replay(self):
        client = FakeClient()
        graph = ClusterGraph(client, "g")
        rows = [{"id": i} for i in range(5)]
        def identity(index, batch):
            return f"job-1-{index}"

        graph.insert_nodes("N", rows, batch_size=2, request_id=identity, align_partitions=False)
        graph.insert_nodes("N", rows, batch_size=2, request_id=identity, align_partitions=False)
        self.assertEqual(["job-1-0", "job-1-1", "job-1-2"] * 2, [p["requestId"] for p in client.payloads])

    def test_invalid_request_id_is_rejected_before_any_write(self):
        client = FakeClient()
        with self.assertRaises(ValueError):
            ClusterGraph(client, "g").insert_nodes("N", [{"id": 1}], request_id="has space", align_partitions=False)
        self.assertEqual([], client.payloads)

    def test_server_business_error_keeps_its_own_type(self):
        client = FakeClient(code="SYNTAX")
        with self.assertRaises(SyntaxError) as caught:
            ClusterGraph(client, "g").insert_nodes("N", [{"id": 1}], batch_size=1, align_partitions=False)
        self.assertEqual(0, caught.exception.response["confirmed"])
        self.assertEqual(0, caught.exception.response["failedBatch"])

    def test_replay_skips_confirmed_batches_and_reuses_failed_request_id(self):
        rows = [{"id": i} for i in range(6)]
        def identity(index, batch):
            return f"job-1-{index}"

        broken = FakeClient(fail_at=2)
        with self.assertRaises(CommitUncertainError) as caught:
            ClusterGraph(broken, "g").insert_nodes("N", rows, batch_size=2, request_id=identity, align_partitions=False)
        failed_batch = caught.exception.response["failedBatch"]
        self.assertEqual(1, failed_batch)
        self.assertEqual("job-1-1", caught.exception.response["requestId"])

        resumed = FakeClient()
        result = ClusterGraph(resumed, "g").insert_nodes(
            "N", rows, batch_size=2, request_id=identity, start_batch=failed_batch, align_partitions=False)
        # 只重发第 1、2 批，编号与首次调用一致。
        self.assertEqual(["job-1-1", "job-1-2"], [p["requestId"] for p in resumed.payloads])
        self.assertEqual([[2, 3], [4, 5]], [[row["id"] for row in p["rows"]] for p in resumed.payloads])
        self.assertEqual(4, result["confirmed"])

    def test_invalid_start_batch_is_rejected(self):
        for value in (-1, True, 1.5):
            with self.assertRaises(ValueError):
                ClusterGraph(FakeClient(), "g").insert_nodes("N", [{"id": 0}], start_batch=value, align_partitions=False)

    def test_skip_missing_flag_is_edge_only(self):
        client = FakeClient()
        ClusterGraph(client, "g").insert_edges("E", [{"id": "e", "src": "a", "dst": "b"}], skip_missing_endpoints=True)
        self.assertTrue(client.payloads[0]["skipMissing"])

    def test_bulk_accepts_values_the_wire_encoder_supports(self):
        """datetime/Decimal/UUID 在 execute 里可写，bulk 的字节计量不能先抛 TypeError。"""
        client = FakeClient()
        row = {"id": "a", "seen": datetime.date(2026, 11, 10),
               "amount": decimal.Decimal("1.25"), "ref": uuid.uuid4()}
        result = ClusterGraph(client, "g").insert_nodes("N", [row], align_partitions=False)
        self.assertEqual(1, result["confirmed"])

    def test_row_larger_than_batch_limit_is_reported_as_record_too_large(self):
        client = FakeClient()
        with self.assertRaises(BulkInsertError) as caught:
            ClusterGraph(client, "g").insert_nodes("N", [{"id": "x" * 4000}], max_batch_bytes=1024, align_partitions=False)
        self.assertEqual("RECORD_TOO_LARGE", caught.exception.code)
        self.assertEqual([], client.payloads)
