"""insert_nodes 默认的分片对齐路径：全局批次编号、失败位置和重放。"""
import math
import unittest

from pycengyan.cluster import MAX_BATCH_BYTES, MAX_BATCH_ROWS, ClusterGraph
from pycengyan.encoding import encoded_size
from pycengyan.exceptions import BulkInsertError, CommitUncertainError
from pycengyan.partition import part_of

PARTITIONS = 4


class TopologyClient:
    """返回固定拓扑；第 fail_at 个 bulkInsert 断线（提交状态未知）。"""

    def __init__(self, fail_at=None, leaders=None, computes=None):
        self.fail_at = fail_at
        self.leaders = leaders or []
        self.computes = computes or []
        self.bulks = []

    def request(self, payload, **kwargs):
        if payload["op"] == "bulkTopology":
            return {"code": "OK", "partitions": PARTITIONS, "vertexIdType": "INT64", "edgeIdType": "INT64",
                    "leaders": self.leaders, "computes": self.computes}
        self.bulks.append((payload, kwargs))
        if len(self.bulks) == self.fail_at:
            raise RuntimeError("连接断开")
        return {"code": "OK", "rowsAffected": len(payload["rows"])}

    def plan(self):
        return [(p["requestId"], [row["id"] for row in p["rows"]]) for p, _ in self.bulks]


def identity(index, batch):
    return f"job-{index}"


ROWS = [{"id": i} for i in range(40)]


class AlignedBulkTest(unittest.TestCase):
    def test_every_batch_stays_in_one_partition(self):
        client = TopologyClient()
        result = ClusterGraph(client, "g").insert_nodes("P", ROWS, batch_size=5, request_id=identity)
        self.assertEqual(40, result["confirmed"])
        for payload, _ in client.bulks:
            self.assertEqual(1, len({part_of(row["id"], "INT64", PARTITIONS) for row in payload["rows"]}))
        # 批次编号全局连续。
        self.assertEqual([f"job-{i}" for i in range(len(client.bulks))], [rid for rid, _ in client.plan()])
        self.assertEqual(len(client.bulks), result["completedBatches"])

    def test_failure_reports_global_position_and_replay_resends_only_the_rest(self):
        full = TopologyClient()
        ClusterGraph(full, "g").insert_nodes("P", ROWS, batch_size=5, request_id=identity)
        expected = full.plan()

        broken = TopologyClient(fail_at=3)
        with self.assertRaises(CommitUncertainError) as caught:
            ClusterGraph(broken, "g").insert_nodes("P", ROWS, batch_size=5, request_id=identity)
        response = caught.exception.response
        self.assertEqual(2, response["failedBatch"])
        self.assertEqual(2, response["completedBatches"])
        self.assertEqual(sum(len(ids) for _, ids in expected[:2]), response["confirmed"])
        self.assertEqual("job-2", response["requestId"])

        resumed = TopologyClient()
        result = ClusterGraph(resumed, "g").insert_nodes("P", ROWS, batch_size=5, request_id=identity,
                                                          start_batch=response["failedBatch"])
        # 同一编号对应同一批行，服务端才能按 requestId 去重。
        self.assertEqual(expected[2:], resumed.plan())
        self.assertEqual(len(expected), result["completedBatches"])

    def test_batch_goes_to_compute_next_to_partition_leader(self):
        leader_part = part_of(0, "INT64", PARTITIONS)
        client = TopologyClient(leaders=[{"part": leader_part, "host": "10.0.0.7", "port": 9000}],
                                computes=[{"host": "10.0.0.7", "port": 19669}, {"host": "10.0.0.8", "port": 19669}])
        ClusterGraph(client, "g").insert_nodes("P", ROWS, batch_size=5)
        for payload, options in client.bulks:
            part = part_of(payload["rows"][0]["id"], "INT64", PARTITIONS)
            self.assertEqual(("10.0.0.7", 19669) if part == leader_part else None, options["address"])

    def test_invalid_ids_are_reported_with_progress(self):
        for bad in (None, 2 ** 63, "abc", True):
            client = TopologyClient()
            rows = [{"id": i} for i in range(20)] + [{"id": bad}]
            with self.assertRaises(BulkInsertError) as caught:
                ClusterGraph(client, "g").insert_nodes("P", rows, batch_size=5)
            self.assertEqual("INVALID_ARGUMENT", caught.exception.code)
            self.assertEqual(len(client.bulks), caught.exception.response["failedBatch"])
            self.assertEqual(sum(len(p["rows"]) for p, _ in client.bulks), caught.exception.response["confirmed"])

    def test_row_without_id_is_rejected(self):
        with self.assertRaises(BulkInsertError) as caught:
            ClusterGraph(TopologyClient(), "g").insert_nodes("P", [{"name": "a"}])
        self.assertEqual("INVALID_ARGUMENT", caught.exception.code)


class BatchLimitTest(unittest.TestCase):
    def test_limits_follow_server_caps(self):
        graph = ClusterGraph(TopologyClient(), "g")
        for kwargs in ({"batch_size": MAX_BATCH_ROWS + 1}, {"batch_size": 0}, {"batch_size": 2.5},
                       {"max_batch_bytes": MAX_BATCH_BYTES + 1}, {"max_batch_bytes": 100}):
            with self.assertRaises(ValueError, msg=kwargs):
                graph.insert_nodes("P", ROWS, align_partitions=False, **kwargs)

    def test_full_batches_leave_room_for_server_added_fields(self):
        client = TopologyClient()
        rows = [{"id": i, "payload": "x" * 900} for i in range(12000)]
        ClusterGraph(client, "g").insert_nodes("P", rows, batch_size=MAX_BATCH_ROWS, align_partitions=False)
        self.assertGreater(len(client.bulks), 1)
        for payload, _ in client.bulks:
            self.assertLessEqual(encoded_size(payload), MAX_BATCH_BYTES - 64 * 1024)
            self.assertLessEqual(len(payload["rows"]), MAX_BATCH_ROWS)
        self.assertEqual(12000, sum(len(p["rows"]) for p, _ in client.bulks))

    def test_nan_is_rejected_before_any_write(self):
        client = TopologyClient()
        rows = [{"id": 1, "score": 1.0}, {"id": 2, "score": math.nan}]
        with self.assertRaises(BulkInsertError) as caught:
            ClusterGraph(client, "g").insert_nodes("P", rows, align_partitions=False)
        self.assertEqual("INVALID_ARGUMENT", caught.exception.code)
        self.assertEqual([], client.bulks)


if __name__ == "__main__":
    unittest.main()
