"""独立计算集群客户端。图上下文显式随每次请求发送，不使用服务端 USE 会话。"""
from __future__ import annotations

import re
import threading
import uuid

from .compute_transport import ComputeTransport
from .encoding import encoded_size
from .exceptions import (
    AuthenticationError,
    BulkInsertError,
    CengyanError,
    CommitUncertainError,
    error_for_response,
    raise_for_response,
)
from .partition import part_of
from .result import Result

# 服务端对 requestId 的格式要求，与 ComputeServer 的校验保持一致。
_REQUEST_ID = re.compile(r"[A-Za-z0-9_-]{1,120}")
# 服务端 bulkInsert 的单批上限：1..10000 行，整条请求（含服务端追加字段）不超过 4 MiB。
MAX_BATCH_ROWS = 10000
MAX_BATCH_BYTES = 4 * 1024 * 1024
# 服务端在校验 4 MiB 前会追加 _schemaLease / _reservation / _user / remainingTimeoutMs 等字段，
# 并用 Jackson 重新序列化（数字格式与 Python 略有不同）。贴近上限的批次要给这些留余量。
_SERVER_OVERHEAD_BYTES = 64 * 1024
_MIN_BATCH_BYTES = 1024
# 调用方不能通过 admin(**values) 覆盖的协议字段。
_RESERVED_ADMIN_FIELDS = frozenset({"op", "token", "session"})


def _request_id(value, batch_index, rows):
    value = value(batch_index, rows) if callable(value) else value
    if not isinstance(value, str) or not _REQUEST_ID.fullmatch(value):
        raise ValueError("requestId 必须匹配 [A-Za-z0-9_-]{1,120}")
    return value


class ClusterClient:
    def __init__(self, addresses, user="root", *, password, timeout=30.0, tls=False, ssl_context=None,
                 transport_factory=None):
        if not password:
            raise ValueError("必须提供数据库密码：SDK 不再内置默认口令")
        self._transport = ComputeTransport(addresses, timeout, tls=tls, ssl_context=ssl_context,
                                           transport_factory=transport_factory)
        self.user, self.password = user, password
        self.token = None
        self._auth_lock = threading.Lock()

    def __enter__(self):
        try:
            self.login()
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *args):
        self.close()

    def _login_request(self):
        response = raise_for_response(
            self._transport.request({"op": "login", "user": self.user, "password": self.password}), write=False)
        token = response.get("token")
        if not isinstance(token, str) or not token:
            raise AuthenticationError("登录响应缺少令牌", "UNAUTHENTICATED", response)
        return token

    def login(self):
        token = self._login_request()
        with self._auth_lock:
            self.token = token
        return self

    def _relogin(self, stale):
        """令牌过期或凭据版本变更后重新登录；并发线程只让一个去换。"""
        with self._auth_lock:
            if self.token is not None and self.token != stale:
                return self.token  # 别的线程已经换过
            self.token = self._login_request()
            return self.token

    def _send(self, request, timeout, write, address):
        if address is None:
            return self._transport.request(request, timeout, write=write)
        return self._transport.request(request, timeout, write=write, address=address)

    def request(self, payload, *, write=False, timeout=None, address=None):
        """发送一条请求。令牌失效时自动重登一次并重发。

        写请求的 requestId 在这里一次定下：传输层断线重试和令牌过期后的重发都用同一个 id，
        服务端在幂等窗口内按它去重。
        """
        if write and not payload.get("requestId"):
            payload = dict(payload, requestId=str(uuid.uuid4()))
        if self.token is None:
            self.login()
        used = self.token
        try:
            return raise_for_response(self._send(dict(payload, token=used), timeout, write, address), write=write)
        except AuthenticationError:
            token = self._relogin(used)
            return raise_for_response(self._send(dict(payload, token=token), timeout, write, address), write=write)

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
        """用服务端登记的 Compute 地址替换本地地址表（服务端返回 IP，启用 TLS 校验主机名时慎用）。"""
        response = self.request({"op": "members"})
        addresses = [(node["host"], node["port"]) for node in response["addresses"]]
        self._transport.update_addresses(addresses)
        return addresses

    def admin(self, command, *, write=False, **values):
        """管理命令。会改动集群状态的命令必须显式传 write=True 才能获得断线重试。

        不再按命令名猜读写：新增的管理命令默认按只读处理，不会因为名字没进白名单
        而被当作可重试的写请求。
        """
        reserved = _RESERVED_ADMIN_FIELDS.intersection(values)
        if reserved:
            raise ValueError("管理命令参数不能覆盖协议字段：" + ", ".join(sorted(reserved)))
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

    def insert_nodes(self, type_name, rows, *, batch_size=1024, max_batch_bytes=MAX_BATCH_BYTES,
                     request_id=None, start_batch=0, align_partitions=True):
        """批量写点。``align_partitions=True``（默认）时按主键分片攒批，每批只落在一个分片上。

        无论是否对齐，批次编号都是全局的：异常里的 ``failedBatch`` 可以直接作为重放时的
        ``start_batch``，前提是 rows、batch_size、max_batch_bytes、align_partitions 与首次一致。
        """
        run = _BulkRun(self, "node", type_name, batch_size, max_batch_bytes, False, request_id, start_batch)
        if align_partitions:
            return self._bulk_aligned(run, rows)
        return self._bulk(run, rows)

    def insert_edges(self, type_name, rows, *, batch_size=1024, max_batch_bytes=MAX_BATCH_BYTES,
                     skip_missing_endpoints=False, request_id=None, start_batch=0):
        """批量写边。每一行都必须带边主键 ``id``，以及 ``src``、``dst``。

        缺 ``id`` 的行在进入网络请求前就被拒绝，已确认批次的进度保留在异常里。
        """
        run = _BulkRun(self, "edge", type_name, batch_size, max_batch_bytes, skip_missing_endpoints,
                       request_id, start_batch)
        return self._bulk(run, rows)

    def _bulk(self, run, rows):
        """按行数和字节上限顺序切批。"""
        batch = _Batch(run)
        for row in rows:
            size = run.row_size(row)
            # 已满批次先发出，再拒绝缺 id 的边，避免把已凑满的行留在未发送缓冲里。
            batch.make_room(size)
            if run.kind == "edge":
                run.require_id(row)
            batch.append(row, size)
        batch.flush()
        return run.progress

    def _bulk_aligned(self, run, rows):
        """按主键分片攒批。每个网络请求只落在一个分片上，服务端可以一次原子提交。

        各分片共用一个 ``_BulkRun``：批次编号按实际发送顺序全局递增，进度和
        ``failedBatch`` 都是全局值。遇到无效行时已缓冲但未发送的行不发出——
        修好数据后用 ``start_batch=failedBatch`` 重放，切批结果与首次一致。
        """
        topology = self.client.request({"op": "bulkTopology", "graph": self.name})
        if topology.get("code") not in (None, "OK"):
            raise run.annotate(error_for_response(topology, write=False))
        partitions = topology.get("partitions")
        id_type = topology.get("vertexIdType" if run.kind == "node" else "edgeIdType")
        if isinstance(partitions, bool) or not isinstance(partitions, int) or partitions < 1 or not isinstance(id_type, str):
            raise run.annotate(BulkInsertError("分片拓扑无效", "PROTOCOL_ERROR"))
        leaders = {item.get("part"): item for item in topology.get("leaders") or []}
        computes = list(topology.get("computes") or [])

        def address_for(part):
            # 优先把批次发给与分片 Leader 同机的 Compute；匹配不上时走默认入口。
            host = (leaders.get(part) or {}).get("host")
            for compute in computes:
                if host and compute.get("host") == host and isinstance(compute.get("port"), int):
                    return (compute["host"], compute["port"])
            return None

        batches = {}
        for row in rows:
            run.require_id(row)
            try:
                part = part_of(row["id"], id_type, partitions)
            except ValueError as error:
                raise run.annotate(BulkInsertError(f"记录 id 无效：{error}", "INVALID_ARGUMENT")) from error
            size = run.row_size(row)
            batch = batches.get(part)
            if batch is None:
                batch = batches[part] = _Batch(run, address_for(part))
            batch.make_room(size)
            batch.append(row, size)
        for batch in batches.values():
            batch.flush()
        return run.progress


class _BulkRun:
    """一次批量写入的全局状态：参数、全局批次编号与进度。"""

    def __init__(self, graph, kind, type_name, batch_size, max_bytes, skip_missing, request_id, start_batch):
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or not 1 <= batch_size <= MAX_BATCH_ROWS:
            raise ValueError(f"batch_size 必须是 1 到 {MAX_BATCH_ROWS} 之间的整数")
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or not _MIN_BATCH_BYTES <= max_bytes <= MAX_BATCH_BYTES:
            raise ValueError(f"max_batch_bytes 必须是 {_MIN_BATCH_BYTES} 到 {MAX_BATCH_BYTES} 之间的整数")
        if isinstance(start_batch, bool) or not isinstance(start_batch, int) or start_batch < 0:
            raise ValueError("start_batch 必须是非负整数")
        self.graph, self.kind, self.type_name = graph, kind, type_name
        self.batch_size, self.skip_missing = batch_size, bool(skip_missing)
        self.request_id, self.start_batch = request_id, start_batch
        # 固定开销按 requestId 的最长形式计量，保证无论最终用哪个 id 都不会超限。
        self.header = encoded_size({"op": "bulkInsert", "graph": graph.name, "kind": kind, "type": type_name,
                                    "rows": [], "skipMissing": self.skip_missing, "requestId": "x" * 120})
        self.limit = min(max_bytes, MAX_BATCH_BYTES - _SERVER_OVERHEAD_BYTES)
        self.progress = {"confirmed": 0, "skipped": 0, "duplicates": 0, "completedBatches": 0}

    def row_size(self, row):
        """一行在批次里占用的字节（含行间逗号）；无法编码或单行超限时抛 BulkInsertError。"""
        try:
            size = encoded_size(row) + 1
        except (TypeError, ValueError) as error:
            raise self.annotate(BulkInsertError(f"记录无法编码为 JSON：{error}", "INVALID_ARGUMENT")) from error
        if self.header + size > self.limit:
            raise self.annotate(BulkInsertError("单条记录超过批次字节上限", "RECORD_TOO_LARGE"))
        return size

    def require_id(self, row):
        """主键与库内规则一致：调用方给出 id，客户端不推导。"""
        if not isinstance(row, dict) or row.get("id") is None:
            noun = "边" if self.kind == "edge" else "按分片写入时每行"
            raise self.annotate(BulkInsertError(f"{noun}必须带 id", "INVALID_ARGUMENT"))

    def annotate(self, error, request_id=None):
        """把全局进度和失败批次位置写进异常，调用方据此决定是放弃还是原样重放。"""
        details = dict(self.progress, failedBatch=self.progress["completedBatches"])
        if request_id is not None:
            details["requestId"] = request_id
        error.response = dict(error.response or {}, **details)
        error.bulk_progress = details
        return error

    def send(self, rows, address=None):
        index = self.progress["completedBatches"]
        if index < self.start_batch:
            # 重放时跳过已确认批次，但仍要占位，保证 batch_index 与首次调用一致。
            self.progress["completedBatches"] += 1
            return
        identifier = _request_id(self.request_id, index, rows) if self.request_id is not None else str(uuid.uuid4())
        payload = {"op": "bulkInsert", "graph": self.graph.name, "kind": self.kind, "type": self.type_name,
                   "rows": rows, "skipMissing": self.skip_missing, "requestId": identifier}
        try:
            response = self.graph.client.request(payload, write=True, address=address)
        except CengyanError as error:
            self.annotate(error, identifier)
            raise
        except Exception as error:
            # 非协议异常（如连接层错误）：提交状态同样无法确认。
            annotated = CommitUncertainError(str(error), "FAILED", {"message": str(error)})
            raise self.annotate(annotated, identifier) from error
        if response.get("code") not in (None, "OK"):
            # 按服务端错误码细分成具体异常，再补上批次上下文。
            raise self.annotate(error_for_response(response, write=True), identifier)
        self.progress["confirmed"] += response.get("rowsAffected", 0)
        self.progress["skipped"] += response.get("skipped", 0)
        self.progress["duplicates"] += response.get("duplicates", 0)
        self.progress["completedBatches"] += 1
        self.progress["requestId"] = identifier


class _Batch:
    """一个待发送批次的缓冲；满行数立即发出，放不下下一行时先发出。"""

    def __init__(self, run, address=None):
        self.run, self.address = run, address
        self.rows, self.size = [], 0

    def make_room(self, size):
        if self.rows and self.run.header + self.size + size > self.run.limit:
            self.flush()

    def append(self, row, size):
        self.rows.append(row)
        self.size += size
        if len(self.rows) >= self.run.batch_size:
            self.flush()

    def flush(self):
        if self.rows:
            rows, self.rows, self.size = self.rows, [], 0
            self.run.send(rows, self.address)
