"""分布式 Compute 的多地址传输；不自动判断 GQL 是读还是写。

由上层执行计划明确传入 write=True，并在首次发送前确定稳定 requestId。
单机和多机部署均通过同一 Compute 协议访问，不使用旧服务端图会话。

并发模型：每个线程持有自己的连接，锁只保护地址索引和连接登记表，不覆盖网络往返。
服务端按 query_slots 并行执行，客户端不再把并发请求串行化。
"""
from __future__ import annotations

import random
import threading
import time
import uuid

from .exceptions import ConnectionError, ReadTimeoutError
from .transport import TcpTransport


class ComputeTransport:
    def __init__(self, addresses, timeout=30.0, *, transport_factory=None, rng=None, tls=False):
        self.addresses = tuple(addresses)
        if not self.addresses:
            raise ValueError("至少配置一个计算节点地址")
        for host, port in self.addresses:
            if not isinstance(host, str) or not host or isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise ValueError("计算节点地址应为 (host, port)")
        if len(set(self.addresses)) != len(self.addresses):
            raise ValueError("计算节点地址不能重复")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout < float("inf"):
            raise ValueError("Compute 请求必须有有限的正数超时")
        self.timeout = timeout
        self.tls = bool(tls)
        self._factory = transport_factory or TcpTransport
        self._rng = rng or random.SystemRandom()
        self._index = self._rng.randrange(len(self.addresses))
        self._local = threading.local()
        self._live = {}  # id(transport) -> (address, transport)，close/地址刷新时统一清理
        self._closed = False
        self._lock = threading.RLock()

    @property
    def address(self):
        with self._lock:
            return self.addresses[self._index]

    def _open(self, address, remaining):
        host, port = address
        # 自定义工厂保持三参数签名；只有内置 TcpTransport 才接受 tls。
        if self._factory is TcpTransport:
            return self._factory(host, port, min(self.timeout, remaining), tls=self.tls)
        return self._factory(host, port, min(self.timeout, remaining))

    def _connection(self, remaining):
        address = getattr(self._local, "preferred", None) or self.address
        current = getattr(self._local, "connection", None)
        if current is not None and current[0] == address:
            return current[1]
        if current is not None:
            self._discard()
        transport = self._open(address, remaining)
        with self._lock:
            self._live[id(transport)] = (address, transport)
        self._local.connection = (address, transport)
        return transport

    def _forget(self, transport):
        with self._lock:
            self._live.pop(id(transport), None)

    def _discard(self):
        """关闭并注销当前线程的连接。其他线程的连接不受影响。"""
        current = getattr(self._local, "connection", None)
        if current is None:
            return
        self._local.connection = None
        self._close(current[1])

    def _close(self, transport):
        self._forget(transport)
        try:
            transport.close()
        except OSError:
            pass

    def _switch(self):
        if len(self.addresses) > 1:
            with self._lock:
                # 重试不选刚刚失败的入口；多轮允许回到已恢复的节点。
                offset = 1 + self._rng.randrange(len(self.addresses) - 1)
                self._index = (self._index + offset) % len(self.addresses)

    def request(self, payload, timeout=None, *, write=False, address=None):
        with self._lock:
            if self._closed:
                raise ConnectionError("Compute 传输已关闭", "CONNECTION_ERROR")
        # 只改顶层 requestId / remainingTimeoutMs，浅拷贝即可；批量写入的 rows 不再整份复制。
        request = dict(payload)
        budget = self.timeout if timeout is None else timeout
        if isinstance(budget, bool) or not isinstance(budget, (int, float)) or not 0 < budget < float("inf"):
            raise ValueError("Compute 请求必须有有限的正数超时")
        deadline = time.monotonic() + budget
        if write:
            if not request.get("requestId"):
                request["requestId"] = str(uuid.uuid4())
            attempts = 4  # 首次发送 + 最多三次自动重试。
        else:
            attempts = 1
        if address is not None:
            self._local.preferred = address
        try:
            return self._request_attempts(request, deadline, attempts, write)
        finally:
            self._local.preferred = None

    def _request_attempts(self, request, deadline, attempts, write):
        for attempt in range(attempts):
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if write:
                        return {"code": "FAILED", "requestId": request["requestId"],
                                "message": "写请求已到达原始截止时间，提交状态无法确认，数据可能已经提交"}
                    raise ReadTimeoutError("请求已到达截止时间", "READ_TIMEOUT")
                # 服务端业务错误原样返回；只有断线/读超时进入网络重试。
                request["remainingTimeoutMs"] = max(1, int(remaining * 1000))
                return self._connection(remaining).request(request, remaining)
            except (ConnectionError, ReadTimeoutError) as error:
                self._discard()
                self._local.preferred = None
                # 读不重试，但下次请求也不能再连回已失败的节点；写继续在别的节点重试。
                self._switch()
                if not write:
                    raise
                if attempt == attempts - 1:
                    return {
                        "code": "FAILED",
                        "requestId": request["requestId"],
                        "message": "写请求重试 3 次后仍未获得成功确认，提交状态无法确认，数据可能已经提交：" + str(error),
                    }

    def close(self):
        with self._lock:
            self._closed = True
            live = list(self._live.values())
            self._live.clear()
        self._local.connection = None
        for _, transport in live:
            try:
                transport.close()
            except OSError:
                pass

    def update_addresses(self, addresses):
        """显式刷新发现的 Compute 外部地址，不自动重发任何当前读请求。"""
        values = tuple(dict.fromkeys(tuple(address) for address in addresses))
        if not values:
            raise ValueError("发现的计算节点地址不能为空")
        for host, port in values:
            if not isinstance(host, str) or not host or isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise ValueError("发现的计算节点地址无效")
        with self._lock:
            current = self.addresses[self._index]
            self.addresses = values
            self._index = values.index(current) if current in values else self._rng.randrange(len(values))
            stale = [entry for entry in self._live.values() if entry[0] not in values]
            for _, transport in stale:
                self._live.pop(id(transport), None)
        for _, transport in stale:
            try:
                transport.close()
            except OSError:
                pass
        mine = getattr(self._local, "connection", None)
        if mine is not None and mine[0] not in values:
            self._local.connection = None
