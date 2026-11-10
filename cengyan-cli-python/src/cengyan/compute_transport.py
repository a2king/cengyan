"""分布式 Compute 的多地址传输；不自动判断 GQL 是读还是写。

由上层执行计划明确传入 write=True，并在首次发送前确定稳定 requestId。
单机和多机部署均通过同一 Compute 协议访问，不使用旧服务端图会话。
"""
from __future__ import annotations

import copy
import random
import threading
import time
import uuid

from .exceptions import ConnectionError, ReadTimeoutError
from .transport import TcpTransport


class ComputeTransport:
    def __init__(self, addresses, timeout=30.0, *, transport_factory=None, rng=None):
        self.addresses = tuple(addresses)
        if not self.addresses:
            raise ValueError("至少配置一个计算节点地址")
        for host, port in self.addresses:
            if not isinstance(host, str) or not host or isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                raise ValueError("计算节点地址应为 (host, port)")
        if len(set(self.addresses)) != len(self.addresses):
            raise ValueError("计算节点地址不能重复")
        self.timeout = timeout
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout < float("inf"):
            raise ValueError("Compute 请求必须有有限的正数超时")
        self._factory = transport_factory or TcpTransport
        self._rng = rng or random.SystemRandom()
        self._index = self._rng.randrange(len(self.addresses))
        self._transport = None
        self._closed = False
        self._lock = threading.RLock()

    @property
    def address(self):
        return self.addresses[self._index]

    def _connection(self, remaining):
        if self._transport is None:
            host, port = self.address
            self._transport = self._factory(host, port, min(self.timeout, remaining))
        return self._transport

    def _discard(self):
        transport, self._transport = self._transport, None
        if transport is not None:
            try:
                transport.close()
            except OSError:
                pass

    def _switch(self):
        if len(self.addresses) > 1:
            # 重试不选刚刚失败的入口；多轮允许回到已恢复的节点。
            offset = 1 + self._rng.randrange(len(self.addresses) - 1)
            self._index = (self._index + offset) % len(self.addresses)

    def request(self, payload, timeout=None, *, write=False):
        with self._lock:
            if self._closed:
                raise ConnectionError("Compute 传输已关闭", "CONNECTION_ERROR")
            request = copy.deepcopy(payload)
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
            for attempt in range(attempts):
                try:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        if write:
                            return {"code": "FAILED", "message": "写请求已到达原始截止时间，提交状态无法确认，数据可能已经提交"}
                        raise ReadTimeoutError("请求已到达截止时间", "READ_TIMEOUT")
                    # 服务端业务错误原样返回；只有断线/读超时进入网络重试。
                    request["remainingTimeoutMs"] = max(1, int(remaining * 1000))
                    return self._connection(remaining).request(request, remaining)
                except (ConnectionError, ReadTimeoutError) as error:
                    self._discard()
                    if not write:
                        raise
                    if attempt == attempts - 1:
                        return {
                            "code": "FAILED",
                            "message": "写请求重试 3 次后仍未获得成功确认，提交状态无法确认，数据可能已经提交：" + str(error),
                        }
                    self._switch()

    def close(self):
        with self._lock:
            self._closed = True
            self._discard()

    def update_addresses(self, addresses):
        """显式刷新发现的 Compute 外部地址，不自动重发任何当前读请求。"""
        with self._lock:
            values = tuple(dict.fromkeys(tuple(address) for address in addresses))
            if not values:
                raise ValueError("发现的计算节点地址不能为空")
            for host, port in values:
                if not isinstance(host, str) or not host or isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
                    raise ValueError("发现的计算节点地址无效")
            current = self.address
            self.addresses = values
            if current in values:
                self._index = values.index(current)
            else:
                self._discard()
                self._index = self._rng.randrange(len(values))
