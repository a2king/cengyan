"""独立计算集群客户端。图上下文显式随每次请求发送，不使用服务端 USE 会话。"""
from __future__ import annotations

import re
import threading
import uuid

from .compute_transport import ComputeTransport
from .encoding import encoded_size
from .partition import part_of
from .exceptions import (
    AuthenticationError,
    BulkInsertError,
    CengyanError,
    CommitUncertainError,
    error_for_response,
    raise_for_response,
)
from .result import Result

# 服务端对 requestId 的格式要求，与 ComputeServer 的校验保持一致。
_REQUEST_ID = re.compile(r"[A-Za-z0-9_-]{1,120}")
# 批次报文除行内容外的固定开销余量（op/graph/kind/type/requestId 等字段）。
_BATCH_HEADER_BYTES = 256
_MIN_BATCH_BYTES = 1024


def _request_id(value, batch_index, rows):
    value = value(batch_index, rows) if callable(value) else value
    if not isinstance(value, str) or not _REQUEST_ID.fullmatch(value):
        raise ValueError("requestId 必须匹配 [A-Za-z0-9_-]{1,120}")
    return value


class ClusterClient:
    def __init__(self, addresses, user="root", *, password, timeout=30.0, tls=False):
        if not password:
            raise ValueError("必须提供数据库密码：SDK 不再内置默认口令")
        self._transport = ComputeTransport(addresses, timeout, tls=tls)
        self.user, self.password = user, password
        self.token = None
        self._auth_lock = threading.Lock()

    def __enter__(self):
        self.login()
        return self

    def __exit__(self, *args):
        self.close()

    def login(self):
        response = self._transport.request({"op": "login", "user": self.user, "password": self.password})
        raise_for_response(response)
        with self._auth_lock:
            self.token = response["token"]
        return self

    def _relogin(self, stale):
        """令牌过期或凭据版本变更后重新登录；并发线程只让一个去换。"""
        with self._auth_lock:
            if self.token is not None and self.token != stale:
                return self.token  # 别的线程已经换过
            response = self._transport.request({"op": "login", "user": self.user, "password": self.password})
            raise_for_response(response)
            self.token = response["token"]
            return self.token

    def _send(self, request, timeout, write, address):
        if address is None:
            return self._transport.request(request, timeout, write=write)
        return self._transport.request(request, timeout, write=write, address=address)

    def request(self, payload, *, write=False, timeout=None, address=None):
        """发送一条请求。令牌失效时自动重登一次并重发（写请求沿用同一 requestId）。"""
        if self.token is None:
            self.login()
        used = self.token
        request = dict(payload, token=used)
        try:
            return raise_for_response(self._send(request, timeout, write, address))
        except AuthenticationError:
            # 过期后原地重发是安全的：写请求的 requestId 在传输层保持不变，服务端按幂等窗口去重。
            token = self._relogin(used)
            return raise_for_response(self._send(dict(payload, token=token), timeout, write, address))

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

    def admin(self, command, *, write=False, **values):
        """管理命令。会改动集群状态的命令必须显式传 write=True 才能获得断线重试。

        不再按命令名猜读写：新增的管理命令默认按只读处理，不会因为名字没进白名单
        而被当作可重试的写请求。
        """
        return self.request({"op": "admin", "command": command, **values}, write=write)

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

    def insert_nodes(self, type_name, rows, *, batch_size=1024, max_batch_bytes=4 * 1024 * 1024,
                     request_id=None, start_batch=0, align_partitions=True):
        if align_partitions:
            return self._bulk_aligned("node", type_name, rows, batch_size, max_batch_bytes, False, request_id, start_batch)
        return self._bulk("node", type_name, rows, batch_size, max_batch_bytes, False, request_id, start_batch)

    def insert_edges(self, type_name, rows, *, batch_size=1024, max_batch_bytes=4 * 1024 * 1024,
                     skip_missing_endpoints=False, request_id=None, start_batch=0):
        return self._bulk("edge", type_name, rows, batch_size, max_batch_bytes, skip_missing_endpoints,
                          request_id, start_batch)

    def _bulk_aligned(self, kind, type_name, rows, batch_size, max_bytes, skip_missing, request_id, start_batch):
        """按主键分片攒批。每个网络请求只落在一个分片上，服务端可以一次原子提交。"""
        topology = self.client.request({"op": "bulkTopology", "graph": self.name})
        if topology.get("code") not in (None, "OK"):
            raise error_for_response(topology)
        partitions = topology.get("partitions")
        id_type = topology.get("vertexIdType" if kind == "node" else "edgeIdType")
        if not isinstance(partitions, int) or partitions < 1 or not isinstance(id_type, str):
            raise BulkInsertError("分片拓扑无效", "PROTOCOL_ERROR", {"confirmed": 0, "skipped": 0, "duplicates": 0, "completedBatches": 0})
        leaders = {item.get("part"): item for item in topology.get("leaders") or []}
        computes = list(topology.get("computes") or [])

        def address_for(part):
            host = (leaders.get(part) or {}).get("host")
            for compute in computes:
                if compute.get("host") == host:
                    return (compute["host"], compute["port"])
            return None

        progress = {"confirmed": 0, "skipped": 0, "duplicates": 0, "completedBatches": 0}
        buffers = {}
        next_index = 0

        def flush(part):
            nonlocal next_index
            pending = buffers.pop(part, None)
            if not pending:
                return
            result = self._bulk(kind, type_name, pending, batch_size, max_bytes, skip_missing, request_id,
                                max(0, start_batch - next_index), address=address_for(part), index_base=next_index)
            next_index += result["completedBatches"]
            for key in ("confirmed", "skipped", "duplicates"):
                progress[key] += result.get(key, 0)
            progress["completedBatches"] = next_index
            progress["requestId"] = result.get("requestId")

        id_field = "id"
        for row in rows:
            if not isinstance(row, dict) or id_field not in row:
                raise BulkInsertError("按分片写入时每行必须带 id", "INVALID_ARGUMENT", dict(progress))
            part = part_of(row[id_field], id_type, partitions)
            buffers.setdefault(part, []).append(row)
            if len(buffers[part]) >= batch_size:
                flush(part)
        for part in list(buffers):
            flush(part)
        return progress

    def _bulk(self, kind, type_name, rows, batch_size, max_bytes, skip_missing, request_id=None, start_batch=0,
              address=None, index_base=0):
        """分批写入。失败时异常上带 requestId 与批次位置，便于在幂等窗口内确认或重放。

        request_id 为 None 时每批生成随机 uuid4；传入字符串则固定用于每一批（只适用于
        一批就写完的场景）；传入 ``callable(batch_index, rows) -> str`` 时由调用方决定，
        这是断线后重放同一批次的唯一正确做法——用同样的 rows、batch_size 和 request_id
        再调一次，``start_batch`` 跳过已确认的批次，只重发失败的那一批。
        """
        if isinstance(batch_size, bool) or batch_size < 1 or max_bytes < _MIN_BATCH_BYTES:
            raise ValueError("批次行数及字节限制无效")
        if isinstance(start_batch, bool) or not isinstance(start_batch, int) or start_batch < 0:
            raise ValueError("start_batch 必须是非负整数")
        progress = {"confirmed": 0, "skipped": 0, "duplicates": 0, "completedBatches": 0}
        batch, size = [], _BATCH_HEADER_BYTES

        def send():
            index = progress["completedBatches"]
            if index < start_batch:
                # 重放时跳过已确认批次，但仍要占位，保证 batch_index 与首次调用一致。
                progress["completedBatches"] += 1
                return
            identifier = _request_id(request_id, index_base + index, batch) if request_id is not None else str(uuid.uuid4())
            payload = {"op": "bulkInsert", "graph": self.name, "kind": kind, "type": type_name, "rows": list(batch),
                       "skipMissing": skip_missing, "requestId": identifier}
            try:
                response = self.client.request(payload, write=True, address=address)
            except CengyanError as error:
                self._annotate(error, identifier, progress)
                raise
            except Exception as error:
                # 非协议异常（如连接层错误）：提交状态同样无法确认。
                annotated = CommitUncertainError(str(error), "FAILED", {"message": str(error)})
                self._annotate(annotated, identifier, progress)
                raise annotated from error
            if response.get("code") not in (None, "OK"):
                # 按服务端错误码细分成具体异常，再补上批次上下文。
                annotated = error_for_response(response)
                self._annotate(annotated, identifier, progress)
                raise annotated
            progress["confirmed"] += response.get("rowsAffected", 0)
            progress["skipped"] += response.get("skipped", 0)
            progress["duplicates"] += response.get("duplicates", 0)
            progress["completedBatches"] += 1
            progress["requestId"] = identifier

        for row in rows:
            row_size = encoded_size(row) + 2  # 行间 ", " 分隔，末行不计（由头部余量吸收）
            if row_size + _BATCH_HEADER_BYTES > max_bytes:
                raise BulkInsertError("单条记录超过批次字节上限", "RECORD_TOO_LARGE", dict(progress))
            if batch and (len(batch) == batch_size or size + row_size > max_bytes):
                send()
                batch, size = [], _BATCH_HEADER_BYTES
            batch.append(row)
            size += row_size
        if batch:
            send()
        return progress

    @staticmethod
    def _annotate(error, request_id, progress):
        """把失败批次的位置写进异常，调用方据此决定是放弃还是原样重放。"""
        details = dict(progress, requestId=request_id, failedBatch=progress["completedBatches"])
        error.response = dict(error.response or {}, **details)
        error.bulk_progress = details
