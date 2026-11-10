from __future__ import annotations

import json
import socket
from .exceptions import ConnectionError, ProtocolError, ReadTimeoutError


class TcpTransport:
    def __init__(self, host, port, timeout=30.0, tls=False, **kwargs):
        self.host, self.port, self.timeout, self.tls = host, port, timeout, tls
        self.sock = None; self.file = None

    def connect(self):
        try:
            self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
            if self.tls:
                import ssl
                self.sock = ssl.create_default_context().wrap_socket(self.sock, server_hostname=self.host)
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.file = self.sock.makefile("rwb")
        except OSError as ex:
            self.close()
            raise ConnectionError(str(ex), "CONNECTION_ERROR") from ex

    def request(self, payload, timeout=None):
        if self.file is None: self.connect()
        timeout = self.timeout if timeout is None else timeout
        self.sock.settimeout(None if timeout == 0 else timeout)
        try:
            self.file.write((json.dumps(payload, ensure_ascii=False, default=str) + "\n").encode("utf-8")); self.file.flush()
            line = self.file.readline()
        except socket.timeout as ex: raise ReadTimeoutError("read timeout", "READ_TIMEOUT") from ex
        except OSError as ex: raise ConnectionError(str(ex), "CONNECTION_ERROR") from ex
        if not line: raise ConnectionError("server closed connection", "CONNECTION_ERROR")
        try: return json.loads(line.decode("utf-8"))
        except (UnicodeError, ValueError) as ex: raise ProtocolError("invalid JSON response", "PROTOCOL_ERROR") from ex

    def close(self):
        try:
            try:
                if self.file: self.file.close()
            finally:
                if self.sock: self.sock.close()
        finally: self.file = self.sock = None
