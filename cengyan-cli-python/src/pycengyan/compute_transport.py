"""分布式 Compute 的多地址传输；不自动判断 GQL 是读还是写。

由上层执行计划明确传入 write=True，并在首次发送前确定稳定 requestId。
单机和多机部署均通过同一 Compute 协议访问，不使用旧服务端图会话。

并发模型：每个线程按地址持有自己的连接，锁只保护地址索引和连接登记表，不覆盖网络往返。
服务端按 query_slots 并行执行，客户端不再把并发请求串行化。线程结束后，
它持有的连接由 finalizer 关闭并注销，不会一直留到 ``close()``。
"""
from __future__ import annotations

import random
import threading
import time
import uuid
import weakref

from .exceptions import CengyanConnectionError, ReadTimeoutError
from .transport import TcpTransport


def _valid_address(address):
    if not isinstance(address, tuple) or len(address) != 2:
        return False
    host, port = address
    return (isinstance(host, str) and bool(host) and not isinstance(port, bool)
            and isinstance(port, int) and 1 <= port <= 65535)


def _close_quietly(transport):
    try:
        transport.close()
    except OSError:
        pass


class _ThreadConnections:
    """一个线程的连接表：地址 -> 连接。对象随线程的 threading.local 一起回收。"""

    __slots__ = ("connections", "preferred", "__weakref__")

    def __init__(self):
        self.connections = {}
        self.preferred = None


def _release_thread(owner_ref, connections):
    """线程结束或 ComputeTransport 被回收时关闭该线程的连接。

    只关闭仍在登记表里的连接：已被 close()/update_addresses() 关闭的不再重复关闭。
    """
    owner = owner_ref()
    for transport in list(connections.values()):
        if owner is None or owner._forget(transport):
            _close_quietly(transport)
    connections.clear()


class ComputeTransport:
    def __init__(self, addresses, timeout=30.0, *, transport_factory=None, rng=None, tls=False, ssl_context=None):
        self.addresses = tuple(tuple(address) for address in addresses)
        if not self.addresses:
            raise ValueError("至少配置一个计算节点地址")
        if not all(_valid_address(address) for address in self.addresses):
            raise ValueError("计算节点地址应为 (host, port)")
        if len(set(self.addresses)) != len(self.addresses):
            raise ValueError("计算节点地址不能重复")
        _check_timeout(timeout)
        self.timeout = timeout
        self.tls = bool(tls) or ssl_context is not None
        self.ssl_context = ssl_context
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

    def _mine(self):
        holder = getattr(self._local, "holder", None)
        if holder is None:
            holder = _ThreadConnections()
            self._local.holder = holder
            # 弱引用 self：finalizer 不能让 ComputeTransport 永远活着。
            weakref.finalize(holder, _release_thread, weakref.ref(self), holder.connections)
        return holder

    def _open(self, address, remaining):
        host, port = address
        # 自定义工厂保持三参数签名；只有内置 TcpTransport 才接受 tls / ssl_context。
        if self._factory is TcpTransport:
            if self.ssl_context is not None:
                return self._factory(host, port, min(self.timeout, remaining), tls=self.tls, ssl_context=self.ssl_context)
            return self._factory(host, port, min(self.timeout, remaining), tls=self.tls)
        return self._factory(host, port, min(self.timeout, remaining))

    def _connection(self, holder, address, remaining):
        transport = holder.connections.get(address)
        if transport is not None:
            with self._lock:
                alive = id(transport) in self._live
            if alive:
                return transport
            # 已被 close()/update_addresses() 从其他线程关闭，丢掉重新建连。
            holder.connections.pop(address, None)
        transport = self._open(address, remaining)
        with self._lock:
            closed = self._closed
            if not closed:
                self._live[id(transport)] = (address, transport)
        if closed:
            _close_quietly(transport)
            raise CengyanConnectionError("Compute 传输已关闭", "CONNECTION_ERROR")
        holder.connections[address] = transport
        return transport

    def _forget(self, transport):
        """从登记表注销；返回该连接此前是否仍登记（即由调用方负责关闭）。"""
        with self._lock:
            return self._live.pop(id(transport), None) is not None

    def _discard(self, holder, address):
        """关闭并注销当前线程到 address 的连接。其他线程的连接不受影响。"""
        transport = holder.connections.pop(address, None)
        if transport is not None and self._forget(transport):
            _close_quietly(transport)

    def _switch(self, failed):
        with self._lock:
            # 只有失败的正是当前入口时才换：指定地址（如分片 Leader 旁的节点）失败不影响全局入口。
            if len(self.addresses) > 1 and self.addresses[self._index] == failed:
                # 重试不选刚刚失败的入口；多轮允许回到已恢复的节点。
                offset = 1 + self._rng.randrange(len(self.addresses) - 1)
                self._index = (self._index + offset) % len(self.addresses)

    def request(self, payload, timeout=None, *, write=False, address=None):
        with self._lock:
            if self._closed:
                raise CengyanConnectionError("Compute 传输已关闭", "CONNECTION_ERROR")
        # 只改顶层 requestId / remainingTimeoutMs，浅拷贝即可；批量写入的 rows 不再整份复制。
        request = dict(payload)
        budget = self.timeout if timeout is None else timeout
        _check_timeout(budget)
        deadline = time.monotonic() + budget
        if write:
            if not request.get("requestId"):
                request["requestId"] = str(uuid.uuid4())
            attempts = 4  # 首次发送 + 最多三次自动重试。
        else:
            attempts = 1
        holder = self._mine()
        holder.preferred = tuple(address) if address is not None else None
        try:
            return self._request_attempts(holder, request, deadline, attempts, write)
        finally:
            holder.preferred = None

    def _request_attempts(self, holder, request, deadline, attempts, write):
        for attempt in range(attempts):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if write:
                    return {"code": "FAILED", "requestId": request["requestId"],
                            "message": "写请求已到达原始截止时间，提交状态无法确认，数据可能已经提交"}
                raise ReadTimeoutError("请求已到达截止时间", "READ_TIMEOUT")
            target = holder.preferred or self.address
            try:
                # 服务端业务错误原样返回；只有断线/读超时进入网络重试。
                request["remainingTimeoutMs"] = max(1, int(remaining * 1000))
                return self._connection(holder, target, remaining).request(request, remaining)
            except (CengyanConnectionError, ReadTimeoutError) as error:
                self._discard(holder, target)
                holder.preferred = None
                # 读不重试，但下次请求也不能再连回已失败的节点；写继续在别的节点重试。
                self._switch(target)
                if not write:
                    raise
                with self._lock:
                    closed = self._closed
                if closed or attempt == attempts - 1:
                    reason = "传输已关闭" if closed else "重试 3 次后仍未获得成功确认"
                    return {
                        "code": "FAILED",
                        "requestId": request["requestId"],
                        "message": f"写请求{reason}，提交状态无法确认，数据可能已经提交：" + str(error),
                    }

    def close(self):
        with self._lock:
            self._closed = True
            live = list(self._live.values())
            self._live.clear()
        holder = getattr(self._local, "holder", None)
        if holder is not None:
            holder.connections.clear()
        for _, transport in live:
            _close_quietly(transport)

    def update_addresses(self, addresses):
        """显式刷新发现的 Compute 外部地址，不自动重发任何当前读请求。"""
        values = tuple(dict.fromkeys(tuple(address) for address in addresses))
        if not values:
            raise ValueError("发现的计算节点地址不能为空")
        if not all(_valid_address(address) for address in values):
            raise ValueError("发现的计算节点地址无效")
        with self._lock:
            current = self.addresses[self._index]
            self.addresses = values
            self._index = values.index(current) if current in values else self._rng.randrange(len(values))
            stale = [entry for entry in self._live.values() if entry[0] not in values]
            for _, transport in stale:
                self._live.pop(id(transport), None)
        for _, transport in stale:
            _close_quietly(transport)
        holder = getattr(self._local, "holder", None)
        if holder is not None:
            for address in [a for a in holder.connections if a not in values]:
                holder.connections.pop(address, None)


def _check_timeout(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value < float("inf"):
        raise ValueError("Compute 请求必须有有限的正数超时")
