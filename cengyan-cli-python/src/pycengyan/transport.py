from __future__ import annotations

import json
import socket
import ssl

from .encoding import MAX_LINE_BYTES, dumps
from .exceptions import CengyanConnectionError, ProtocolError, ReadTimeoutError, RequestTooLargeError


class TcpTransport:
    """一条 JSON Lines 连接。``tls=True`` 或传入 ``ssl_context`` 时走 TLS。

    Compute 端口本身不终止 TLS；启用 TLS 时需要在 Compute 前面部署 TLS 终止代理。
    """

    def __init__(self, host, port, timeout=30.0, tls=False, ssl_context=None, **kwargs):
        self.host, self.port, self.timeout = host, port, timeout
        self.ssl_context = ssl_context
        self.tls = bool(tls) or ssl_context is not None
        self.sock = None
        self.file = None

    def connect(self):
        try:
            self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
            if self.tls:
                context = self.ssl_context or ssl.create_default_context()
                self.sock = context.wrap_socket(self.sock, server_hostname=self.host)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.file = self.sock.makefile("rwb")
        except OSError as ex:
            self.close()
            raise CengyanConnectionError(str(ex), "CONNECTION_ERROR") from ex

    def request(self, payload, timeout=None):
        # 先编码再连网：编码失败（如 NaN）或超长时请求一个字节都没发出。
        data = dumps(payload).encode("utf-8")
        if len(data) > MAX_LINE_BYTES:
            raise RequestTooLargeError(f"请求 {len(data)} 字节，超过服务端单行 {MAX_LINE_BYTES} 字节上限，未发送",
                                       "REQUEST_TOO_LARGE")
        if self.file is None:
            self.connect()
        timeout = self.timeout if timeout is None else timeout
        self.sock.settimeout(None if timeout == 0 else timeout)
        try:
            self.file.write(data + b"\n")
            self.file.flush()
            line = self.file.readline()
        except socket.timeout as ex:
            raise ReadTimeoutError("read timeout", "READ_TIMEOUT") from ex
        except OSError as ex:
            raise CengyanConnectionError(str(ex), "CONNECTION_ERROR") from ex
        if not line:
            raise CengyanConnectionError("server closed connection", "CONNECTION_ERROR")
        try:
            return json.loads(line.decode("utf-8"))
        except (UnicodeError, ValueError) as ex:
            raise ProtocolError("invalid JSON response", "PROTOCOL_ERROR") from ex

    def close(self):
        try:
            try:
                if self.file:
                    self.file.close()
            finally:
                if self.sock:
                    self.sock.close()
        finally:
            self.file = self.sock = None
