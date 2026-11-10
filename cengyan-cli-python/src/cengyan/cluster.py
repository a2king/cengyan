"""独立计算集群客户端。图上下文显式随每次请求发送，不使用服务端 USE 会话。"""
from __future__ import annotations

import json
import uuid

from .compute_transport import ComputeTransport
from .exceptions import BulkInsertError, raise_for_response
from .result import Result


class ClusterClient:
    def __init__(self, addresses, user="root", password="cengyan", timeout=30.0):
        self._transport = ComputeTransport(addresses, timeout)
        self.user, self.password = user, password
        self.token = None

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, *args):
        self.close()

    def login(self):
        response = raise_for_response(self._transport.request({"op": "login", "user": self.user, "password": self.password}))
        self.token = response["token"]
        return self

    def request(self, payload, *, write=False, timeout=None):
        if self.token is None:
            self.login()
        request = dict(payload, token=self.token)
        return raise_for_response(self._transport.request(request, timeout, write=write))

    def execute(self, query, graph=None, params=None, *, write=False, timeout=None):
        """执行一条远程 GQL；graph 显式随请求发送，不依赖服务端会话。"""
        payload = {"op": "execute", "query": query, "params": params or {}}
        if graph is not None:
            payload["graph"] = graph
        return Result(self.request(payload, write=write, timeout=timeout))

    def create_graph(self, name, partitions=16, replicas=1, vertex_id_type="INT64", edge_id_type="INT64"):
        return self.request({"op": "createGraph", "name": name, "partitions": partitions, "replicas": replicas,
                             "vertexIdType": vertex_id_type, "edgeIdType": edge_id_type}, write=True)

    def graph(self, name):
        return ClusterGraph(self, name)

    def refresh_nodes(self):
        response = self.request({"op": "members"})
        addresses = [(node["host"], node["port"]) for node in response["addresses"]]
        self._transport.update_addresses(addresses)
        return addresses

    def admin(self, command, **values):
        return self.request({"op": "admin", "command": command, **values}, write=command not in ("stats", "storage", "showMembers", "showNodes"))

    def close(self):
        self._transport.close()


class ClusterGraph:
    def __init__(self, client, name):
        self.client, self.name = client, name

    def execute(self, query, params=None, *, write=False, timeout=None):
        # 不用正则猜测任意 GQL 的读写属性；调用方对写语句显式指定 write=True。
        return Result(self.client.request({"op": "execute", "graph": self.name, "query": query, "params": params or {}},
                                          write=write, timeout=timeout))

    def read(self, query, params=None, *, timeout=None):
        """只读快捷入口；断线不自动重算。"""
        return self.execute(query, params, write=False, timeout=timeout)

    def write(self, query, params=None, *, timeout=None):
        """写入快捷入口；断线最多复用同一 requestId 重试三次。"""
        return self.execute(query, params, write=True, timeout=timeout)

    def insert_nodes(self, type_name, rows, *, batch_size=1024, max_batch_bytes=4 * 1024 * 1024):
        return self._bulk("node", type_name, rows, batch_size, max_batch_bytes, False)

    def insert_edges(self, type_name, rows, *, batch_size=1024, max_batch_bytes=4 * 1024 * 1024, skip_missing_endpoints=False):
        return self._bulk("edge", type_name, rows, batch_size, max_batch_bytes, skip_missing_endpoints)

    def _bulk(self, kind, type_name, rows, batch_size, max_bytes, skip_missing):
        if isinstance(batch_size, bool) or batch_size < 1 or max_bytes < 1024:
            raise ValueError("批次行数及字节限制无效")
        progress = {"confirmed": 0, "skipped": 0, "duplicates": 0, "completedBatches": 0}
        batch, size = [], 256

        def send():
            payload = {"op": "bulkInsert", "graph": self.name, "kind": kind, "type": type_name, "rows": list(batch),
                       "skipMissing": skip_missing, "requestId": str(uuid.uuid4())}
            try:
                response = self.client.request(payload, write=True)
            except Exception as error:
                response = {"code": "FAILED", "message": str(error), **progress,
                            "failedBatch": progress["completedBatches"]}
                raise BulkInsertError(response["message"], "FAILED", response) from error
            progress["confirmed"] += response.get("rowsAffected", 0)
            progress["skipped"] += response.get("skipped", 0)
            progress["duplicates"] += response.get("duplicates", 0)
            progress["completedBatches"] += 1

        for row in rows:
            encoded_size = len(json.dumps(row, ensure_ascii=False, separators=(",", ":")).encode("utf-8")) + 1
            if encoded_size + 256 > max_bytes:
                raise BulkInsertError("单条记录超过批次字节上限", "RECORD_TOO_LARGE", dict(progress))
            if batch and (len(batch) == batch_size or size + encoded_size > max_bytes):
                send()
                batch, size = [], 256
            batch.append(row)
            size += encoded_size
        if batch:
            send()
        return progress
