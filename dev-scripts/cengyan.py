"""层岩图数据库的 Python 客户端。

一行一条 JSON，和仓库里的 cengyan-cli 走同一套协议。
先登录，再执行 GQL。以冒号开头的行是管理命令。

日常用法（在 dev-scripts 目录里）：

    python cengyan.py
    python cengyan.py ping
    python cengyan.py exec "SHOW GRAPHS"
    python cengyan.py file some.gql
    python cengyan.py load-fixture F:\\Downloads\\cengyan-ys-relationship-fixture

环境变量：CENGYAN_HOST、CENGYAN_PORT、CENGYAN_USER、CENGYAN_PASSWORD、CENGYAN_TIMEOUT_MS。
默认连 127.0.0.1:19669，用户 root，密码 cengyan。
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from decimal import Decimal
from pathlib import Path

FIXTURE_FILES = (
    "01_schema.gql",
    "02a_org.gql",
    "02b_person.gql",
    "02c_tel.gql",
    "03a_e_tel.gql",
    "03b_fr.gql",
    "03c_pri.gql",
    "03d_inv.gql",
    "03e_inv_skr.gql",
    "02_rebuild_index.txt",
)


class CengyanError(RuntimeError):
    def __init__(self, response):
        self.response = response
        code = response.get("code", "")
        message = response.get("message", "")
        super().__init__(f"{code} {message}".strip())


class Client:
    def __init__(self, host, port, user, password, timeout_ms):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout_ms = timeout_ms
        self.session = ""
        self._sock = None
        self._buf = b""

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def connect(self):
        try:
            self._sock = socket.create_connection((self.host, self.port), timeout=10)
        except OSError as ex:
            raise SystemExit(f"连不上 {self.host}:{self.port}：{ex}") from ex
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._buf = b""

    def close(self):
        if self._sock is not None:
            self._sock.close()
            self._sock = None

    def request(self, payload, timeout_ms=None):
        timeout_ms = self.timeout_ms if timeout_ms is None else timeout_ms
        body = dict(payload)
        if body.get("op") != "ping":
            body["timeoutMs"] = timeout_ms
            body.setdefault("session", self.session)
        # timeoutMs=0 means that the server does not impose a query deadline.
        # Use a blocking socket in that mode so large Spark imports are not
        # accidentally cut off by a client-side 30 second timeout.
        self._sock.settimeout(None if timeout_ms == 0 else max(timeout_ms, 0) / 1000 + 30)
        raw = (json.dumps(body, ensure_ascii=False, default=_json_default) + "\n").encode("utf-8")
        self._sock.sendall(raw)
        response = self._read()
        session = response.get("session")
        if session:
            self.session = session
        return response

    def _read(self):
        while b"\n" not in self._buf:
            chunk = self._sock.recv(65536)
            if not chunk:
                raise ConnectionError("连接已关闭")
            self._buf += chunk
            if len(self._buf) > 8_000_000:
                raise ConnectionError("响应过长")
        line, self._buf = self._buf.split(b"\n", 1)
        return json.loads(line.decode("utf-8"))

    def ping(self):
        return self.request({"op": "ping"}, timeout_ms=10_000)

    def login(self, user=None, password=None):
        # 密码哈希在服务端要几百毫秒，给足等待时间。
        response = self.request(
            {
                "op": "login",
                "user": self.user if user is None else user,
                "password": self.password if password is None else password,
            },
            timeout_ms=max(self.timeout_ms, 60_000),
        )
        expect_ok(response)
        return response

    def execute(self, query, params=None, timeout_ms=None):
        return self.request(
            {"op": "execute", "query": query, "params": params or {}},
            timeout_ms=timeout_ms,
        )

    def bulk_insert(self, kind, type_name, rows, *, ignore_existing=False, timeout_ms=None):
        """Atomically write one bounded node/edge batch on the server."""
        if kind not in ("node", "edge", "rel"):
            raise ValueError("bulk kind must be node or edge")
        if not rows:
            raise ValueError("bulk rows must not be empty")
        if len(rows) > 10_000:
            raise ValueError("bulk rows must be at most 10000")
        return self.request(
            {
                "op": "bulkInsert",
                "kind": kind,
                "type": type_name,
                "rows": rows,
                "ignoreExisting": ignore_existing,
            },
            timeout_ms=timeout_ms,
        )

    def admin(self, command, index="", user="", password="", role="", timeout_ms=None):
        return self.request(
            {
                "op": "admin",
                "command": command,
                "index": index,
                "user": user,
                "password": password,
                "role": role,
            },
            timeout_ms=timeout_ms,
        )

    def run_line(self, line, timeout_ms=None, rebuild_timeout_ms=None):
        text = line.strip()
        if not text or text.startswith("#"):
            return None
        if text.lower() in ("exit", "quit"):
            raise SystemExit(0)
        if text.lower() in ("ping", ":ping"):
            return self.ping()
        if text.startswith(":"):
            return self._admin_line(text[1:].strip(), timeout_ms, rebuild_timeout_ms)
        return self.execute(text, timeout_ms=timeout_ms)

    def _admin_line(self, body, timeout_ms, rebuild_timeout_ms):
        parts = body.split()
        if not parts:
            raise CengyanError({"code": "SYNTAX", "message": "缺少 command"})
        command = parts[0]
        if command.lower() == "login":
            user = parts[1] if len(parts) > 1 else ""
            password = " ".join(parts[2:])
            return self.login(user, password)
        timeout = timeout_ms
        index = user = password = role = ""
        if command == "rebuildIndex":
            index = parts[1] if len(parts) > 1 else ""
            timeout = rebuild_timeout_ms or timeout_ms
        elif command == "createUser":
            user = _part(parts, 1)
            password = _part(parts, 2)
            role = _part(parts, 3)
        elif command == "setPassword":
            user = _part(parts, 1)
            password = _part(parts, 2)
        elif command == "setRole":
            user = _part(parts, 1)
            role = _part(parts, 2)
        elif command == "dropUser":
            user = _part(parts, 1)
        return self.admin(command, index, user, password, role, timeout_ms=timeout)


def _part(parts, index):
    return parts[index] if len(parts) > index else ""


def _json_default(value):
    """Serialize common Spark/Python values used by the JSON protocol."""
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "asDict"):
        return value.asDict(recursive=True)
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"not JSON serializable: {type(value).__name__}")


def expect_ok(response):
    if response.get("code") != "OK":
        raise CengyanError(response)
    return response


def format_result(response):
    code = response.get("code", "")
    if code != "OK":
        return f"{code} {response.get('message', '')}".rstrip()
    columns = response.get("columns") or []
    rows = response.get("rows") or []
    if not columns:
        affected = response.get("rowsAffected", 0)
        return f"OK rowsAffected={affected}" if affected else "OK"
    lines = ["\t".join(str(column) for column in columns)]
    for row in rows:
        lines.append("\t".join(_cell(value) for value in row))
    return "\n".join(lines)


def _cell(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    if value is None:
        return ""
    return str(value)


def iter_statements(path):
    with open(path, encoding="utf-8-sig") as handle:
        for lineno, line in enumerate(handle, 1):
            text = line.strip()
            if not text or text.startswith("#"):
                continue
            yield lineno, text


def run_statements(client, statements, *, progress, continue_on_error, rebuild_timeout_ms):
    done = 0
    failed = 0
    started = time.perf_counter()
    for label, text in statements:
        timeout = rebuild_timeout_ms if text.startswith(":rebuildIndex") else None
        try:
            response = client.run_line(text, timeout_ms=timeout, rebuild_timeout_ms=rebuild_timeout_ms)
        except CengyanError as ex:
            failed += 1
            print(f"失败 {label}: {ex}", file=sys.stderr)
            if not continue_on_error:
                return failed
            continue
        if response is None:
            continue
        done += 1
        if not text.upper().startswith("INSERT"):
            print(f"{label}: {text}", flush=True)
            print(format_result(response), flush=True)
        if response.get("code") != "OK":
            failed += 1
            if text.upper().startswith("INSERT"):
                print(f"失败 {label}: {format_result(response)}", file=sys.stderr)
                print(f"语句: {text[:200]}", file=sys.stderr)
            if not continue_on_error:
                return failed
        elif progress and done % progress == 0:
            elapsed = time.perf_counter() - started
            print(f"已完成 {done} 条，用时 {elapsed:.1f} 秒", flush=True)
    elapsed = time.perf_counter() - started
    print(f"完成 {done} 条，失败 {failed} 条，用时 {elapsed:.1f} 秒")
    return failed


def load_fixture(client, directory, *, drop_first, progress, continue_on_error, rebuild_timeout_ms):
    root = Path(directory)
    missing = [name for name in FIXTURE_FILES if not (root / name).is_file()]
    if missing:
        raise SystemExit(f"样例目录里缺少文件：{', '.join(missing)}")
    if drop_first:
        graph = _graph_name(root / "01_schema.gql")
        response = client.execute(f"DROP GRAPH {graph}", timeout_ms=rebuild_timeout_ms)
        if response.get("code") not in ("OK", "GRAPH_NOT_FOUND"):
            raise CengyanError(response)
        print(f"已尝试删除图 {graph}：{response.get('code')}")
    statements = []
    for name in FIXTURE_FILES:
        path = root / name
        for lineno, text in iter_statements(path):
            statements.append((f"{name}:{lineno}", text))
    print(f"准备执行 {len(statements)} 条，目录 {root}")
    return run_statements(
        client,
        statements,
        progress=progress,
        continue_on_error=continue_on_error,
        rebuild_timeout_ms=rebuild_timeout_ms,
    )


def _graph_name(schema_path):
    for _, text in iter_statements(schema_path):
        parts = text.split()
        if len(parts) >= 3 and parts[0].upper() == "CREATE" and parts[1].upper() == "GRAPH":
            return parts[2]
    raise SystemExit(f"{schema_path} 里没有 CREATE GRAPH")


def open_client(args, login=True):
    client = Client(args.host, args.port, args.user, args.password, args.timeout_ms)
    client.connect()
    if login:
        logged = client.login()
        role = ""
        rows = logged.get("rows") or []
        if rows:
            role = rows[0][1]
        print(f"已登录 {args.user} {role}".rstrip())
    return client


def build_parser():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--host", default=os.environ.get("CENGYAN_HOST", "127.0.0.1"))
    common.add_argument("--port", type=int, default=int(os.environ.get("CENGYAN_PORT", "19669")))
    common.add_argument("--user", default=os.environ.get("CENGYAN_USER", "root"))
    common.add_argument("--password", default=os.environ.get("CENGYAN_PASSWORD", "cengyan"))
    common.add_argument(
        "--timeout-ms",
        type=int,
        default=int(os.environ.get("CENGYAN_TIMEOUT_MS", "30000")),
        help="单条语句等待毫秒数。请求里会带上 timeoutMs，不受配置文件里 1 秒默认值限制。",
    )
    parser = argparse.ArgumentParser(description="层岩图数据库 Python 客户端", parents=[common])
    commands = parser.add_subparsers(dest="command")

    commands.add_parser("ping", help="检查连接，不用登录", parents=[common])

    exec_parser = commands.add_parser("exec", help="登录后执行一条 GQL", parents=[common])
    exec_parser.add_argument("query")

    file_parser = commands.add_parser("file", help="按行执行一个 .gql 或 .txt", parents=[common])
    file_parser.add_argument("path")
    file_parser.add_argument("--progress", type=int, default=1000)
    file_parser.add_argument("--continue-on-error", action="store_true")
    file_parser.add_argument("--rebuild-timeout-ms", type=int, default=600_000)

    load_parser = commands.add_parser("load-fixture", help="导入有数关系样例", parents=[common])
    load_parser.add_argument(
        "directory",
        nargs="?",
        default=r"F:\Downloads\cengyan-ys-relationship-fixture",
    )
    load_parser.add_argument("--drop-first", action="store_true", help="导入前先 DROP GRAPH")
    load_parser.add_argument("--progress", type=int, default=1000)
    load_parser.add_argument("--continue-on-error", action="store_true")
    load_parser.add_argument("--rebuild-timeout-ms", type=int, default=600_000)

    admin_parser = commands.add_parser("admin", help="登录后执行一条管理命令", parents=[common])
    admin_parser.add_argument("name", help="flush、compact、stats、rebuildIndex、createUser 等")
    admin_parser.add_argument("--index", default="")
    admin_parser.add_argument("--target-user", default="")
    admin_parser.add_argument("--target-password", default="")
    admin_parser.add_argument("--role", default="")

    commands.add_parser("repl", help="交互输入，默认就是这个", parents=[common])
    return parser


def repl(client):
    print("输入 GQL，:rebuildIndex 名字 重建索引，exit 退出。")
    while True:
        try:
            line = input("cengyan> ")
        except EOFError:
            print()
            return 0
        text = line.strip()
        if not text:
            continue
        if text.lower() in ("exit", "quit"):
            return 0
        try:
            response = client.run_line(text, rebuild_timeout_ms=600_000)
        except CengyanError as ex:
            print(ex, file=sys.stderr)
            continue
        if response is not None:
            print(format_result(response))


def main(argv=None):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    args = build_parser().parse_args(argv)
    command = args.command or "repl"
    if command == "ping":
        with Client(args.host, args.port, args.user, args.password, args.timeout_ms) as client:
            response = client.ping()
        print(format_result(response))
        return 0 if response.get("code") == "OK" else 1

    client = open_client(args)
    try:
        if command == "exec":
            response = expect_ok(client.execute(args.query))
            print(format_result(response))
            return 0
        if command == "file":
            statements = [(f"{args.path}:{lineno}", text) for lineno, text in iter_statements(args.path)]
            failed = run_statements(
                client,
                statements,
                progress=args.progress,
                continue_on_error=args.continue_on_error,
                rebuild_timeout_ms=args.rebuild_timeout_ms,
            )
            return 1 if failed else 0
        if command == "load-fixture":
            failed = load_fixture(
                client,
                args.directory,
                drop_first=args.drop_first,
                progress=args.progress,
                continue_on_error=args.continue_on_error,
                rebuild_timeout_ms=args.rebuild_timeout_ms,
            )
            return 1 if failed else 0
        if command == "admin":
            timeout = args.timeout_ms
            if args.name == "rebuildIndex":
                timeout = 600_000
            response = expect_ok(
                client.admin(
                    args.name,
                    index=args.index,
                    user=args.target_user,
                    password=args.target_password,
                    role=args.role,
                    timeout_ms=timeout,
                )
            )
            print(format_result(response))
            return 0
        return repl(client)
    finally:
        client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CengyanError as ex:
        print(ex, file=sys.stderr)
        raise SystemExit(1) from ex
    except KeyboardInterrupt:
        raise SystemExit(130) from None
