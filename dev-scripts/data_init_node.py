#!/usr/bin/env python3
"""Run Spark SQL and initialize Cengyan vertices with foreachPartition.

Example::

    spark-submit --py-files dev-scripts/cengyan.py dev-scripts/data_init_node.py \
      --sql "select id, name, capital from company_source" \
      --host 127.0.0.1 --port 19669 \
      --user root --password cengyan --space Company \
      --node-type Company --id-field id \
      --id-type INT --property-fields name,capital --batch 512

For string node IDs, pass ``--id-type STRING --id-length 64``.  The length
is checked in UTF-8 bytes, matching the server's ``STRING(n)`` definition.

The batch setting controls progress reporting and the size of the local
buffer. Cengyan accepts one GQL statement per request, so rows are still sent
one by one; the connection and login session are reused for the whole
partition. Use ``--timeout-ms 0`` for an unlimited server-side query deadline.
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from functools import partial
from itertools import chain
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping


def _identifier(value: str, label: str) -> str:
    if not value or not (value[0].isalpha() or value[0] == "_") or not all(
        char.isalnum() or char == "_" for char in value
    ):
        raise ValueError(f"{label} {value!r} 不是合法 Cengyan 标识符")
    return value


def _fields(value: str) -> list[str]:
    fields = [item.strip() for item in value.split(",") if item.strip()]
    for field in fields:
        _identifier(field, "字段名")
    return fields


def _values(row: Any) -> dict[str, Any]:
    if hasattr(row, "asDict"):
        return row.asDict(recursive=True)
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _string(value: str) -> str:
    if "\n" in value or "\r" in value:
        raise ValueError("Cengyan 字符串属性不支持换行")
    return "'" + value.replace("'", "''") + "'"


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, Decimal):
        return "NULL" if not value.is_finite() else format(value, "f")
    if isinstance(value, float):
        return "NULL" if not math.isfinite(value) else repr(value)
    if isinstance(value, datetime):
        timestamp = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return "DATETIME " + _string(timestamp.isoformat().replace("+00:00", "Z"))
    if isinstance(value, date):
        return "DATETIME " + _string(value.isoformat() + "T00:00:00Z")
    return _string(str(value))


def _id_literal(value: Any, id_type: str, id_length: int) -> str:
    """Render a node ID using the literal form expected by Cengyan GQL."""

    normalized = str(id_type).strip().upper()
    if normalized == "INT":
        if isinstance(value, bool) or value is None:
            raise ValueError(f"Cengyan 点 id 必须是整数：{value!r}")
        if isinstance(value, float) and (not math.isfinite(value) or value != int(value)):
            raise ValueError(f"Cengyan 点 id 必须是整数：{value!r}")
        if isinstance(value, Decimal) and (not value.is_finite() or value != value.to_integral_value()):
            raise ValueError(f"Cengyan 点 id 必须是整数：{value!r}")
        try:
            integer = int(value)
        except (TypeError, ValueError, OverflowError) as ex:
            raise ValueError(f"Cengyan 点 id 必须是整数：{value!r}") from ex
        if integer < 0:
            raise ValueError(f"Cengyan 点 id 必须是非负整数：{value!r}")
        if len(str(integer)) > id_length:
            raise ValueError(f"Cengyan 点 id 超过定义长度 {id_length}：{value!r}")
        return str(integer)
    if normalized == "STRING":
        if value is None:
            raise ValueError("Cengyan 字符串点 id 不能为空")
        text = str(value)
        if not text:
            raise ValueError("Cengyan 字符串点 id 不能为空")
        if len(text.encode("utf-8")) > id_length:
            raise ValueError(f"Cengyan 字符串点 id 超过定义长度 {id_length}：{text!r}")
        return _string(text)
    raise ValueError(f"id 类型必须是 INT 或 STRING：{id_type!r}")


def _id_value(value: Any, id_type: str, id_length: int) -> int | str:
    """Validate and normalize an ID for a parameterized GQL request."""
    normalized = str(id_type).strip().upper()
    _id_literal(value, normalized, id_length)
    if normalized == "INT":
        return int(value)
    return str(value)


def build_vertex_gql(
    row: Any,
    *,
    node_type: str,
    id_field: str,
    property_fields: Iterable[str],
    id_type: str = "INT",
    id_length: int = 19,
) -> str:
    values = _values(row)
    if id_field not in values:
        raise ValueError(f"Spark SQL 结果缺少 id 字段：{id_field}")
    fields = [f"id:{_id_literal(values[id_field], id_type, id_length)}"]
    for field in property_fields:
        if field not in values:
            raise ValueError(f"Spark SQL 结果缺少属性字段：{field}")
        if values[field] is not None:
            fields.append(f"{_identifier(field, '属性字段')}:{_literal(values[field])}")
    return f"INSERT (a:{_identifier(node_type, '节点类型')} {{{', '.join(fields)}}})"


def build_vertex_request(
    row: Any,
    *,
    node_type: str,
    id_field: str,
    property_fields: Iterable[str],
    id_type: str = "INT",
    id_length: int = 19,
) -> tuple[str, dict[str, Any]]:
    """Build a GQL request using parameters for values that can be bound safely."""
    values = _values(row)
    if id_field not in values:
        raise ValueError(f"Spark SQL 结果缺少 id 字段：{id_field}")
    params: dict[str, Any] = {"id": _id_value(values[id_field], id_type, id_length)}
    fields = ["id:$id"]
    for field in property_fields:
        if field not in values:
            raise ValueError(f"Spark SQL 结果缺少属性字段：{field}")
        value = values[field]
        if value is None:
            continue
        name = _identifier(field, "属性字段")
        if isinstance(value, (datetime, date)):
            fields.append(f"{name}:{_literal(value)}")
        else:
            parameter = f"p_{name}"
            fields.append(f"{name}:${parameter}")
            params[parameter] = value
    query = f"INSERT (a:{_identifier(node_type, '节点类型')} {{{', '.join(fields)}}})"
    return query, params


def build_vertex_row(
    row: Any,
    *,
    id_field: str,
    property_fields: Iterable[str],
    id_type: str = "INT",
    id_length: int = 19,
) -> dict[str, Any]:
    """Build the JSON row consumed by the bulkInsert endpoint."""
    values = _values(row)
    if id_field not in values:
        raise ValueError(f"Spark SQL 结果缺少 id 字段：{id_field}")
    result: dict[str, Any] = {"id": _id_value(values[id_field], id_type, id_length)}
    for field in property_fields:
        if field not in values:
            raise ValueError(f"Spark SQL 结果缺少属性字段：{field}")
        result[_identifier(field, "属性字段")] = values[field]
    return result


def _check(response: Mapping[str, Any], statement: str) -> None:
    if response.get("code") != "OK":
        raise RuntimeError(
            f"Cengyan 写入失败：{response.get('code', 'UNKNOWN')} "
            f"{response.get('message', '')}; statement={statement[:300]}"
        )


def cengyan_udf(rows: Iterator[Any], *, config: Mapping[str, Any]) -> None:
    """The per-partition write function passed to foreachPartition."""

    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return
    script_dir = str(Path(__file__).resolve().parent)
    if script_dir not in sys.path:
        sys.path.insert(0, script_dir)
    from cengyan import Client

    client = Client(
        config["host"],
        config["port"],
        config["user"],
        config["password"],
        config["timeout_ms"],
    )
    client.connect()
    try:
        _check(client.login(), "LOGIN")
        use = f"USE {_identifier(config['space'], '图空间')}"
        _check(client.execute(use), use)
        batch: list[Any] = []
        written = 0

        def flush() -> None:
            nonlocal written
            rows = [
                build_vertex_row(
                    item,
                    id_field=config["id_field"],
                    property_fields=config["property_fields"],
                    id_type=config["id_type"],
                    id_length=config["id_length"],
                )
                for item in batch
            ]
            _check(client.bulk_insert("node", config["node_type"], rows),
                    f"bulkInsert node rows={len(rows)}")
            written += len(rows)
            batch.clear()

        for row in chain((first,), iterator):
            batch.append(row)
            if len(batch) >= config["batch"]:
                flush()
        if batch:
            flush()
        print(f"node partition 写入 {written} 条", flush=True)
    finally:
        client.close()


def _sql(args: argparse.Namespace) -> str:
    return args.sql or Path(args.sql_file).read_text(encoding="utf-8-sig")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Spark SQL + foreachPartition 初始化 Cengyan 节点")
    sql = parser.add_mutually_exclusive_group(required=True)
    sql.add_argument("--sql", help="要执行的 Spark SQL")
    sql.add_argument("--sql-file", help="Spark SQL 文件")
    parser.add_argument("--host", "--address", dest="host", required=True)
    parser.add_argument("--port", type=int, default=19669)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", "--passwd", dest="password", required=True)
    parser.add_argument("--space", "--graph", dest="space", required=True)
    parser.add_argument("--node-type", required=True)
    parser.add_argument("--id-field", "--id-col", dest="id_field", required=True)
    parser.add_argument("--id-type", choices=("INT", "STRING"), type=str.upper, default="INT")
    parser.add_argument("--id-length", type=int, default=19,
                        help="ID 最大长度（STRING 按 UTF-8 字节，默认 19）")
    parser.add_argument("--property-fields", "--properties", default="")
    parser.add_argument("--batch", type=int, default=512)
    parser.add_argument("--repartition", type=int, default=0)
    parser.add_argument("--timeout-ms", type=int, default=int(os.environ.get("CENGYAN_TIMEOUT_MS", "0")),
                        help="服务端查询超时；0 表示不限")
    parser.add_argument("--app-name", default="cengyan-spark-init-node")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    for value, label in (
        (args.space, "图空间"),
        (args.node_type, "节点类型"),
        (args.id_field, "id 字段"),
    ):
        _identifier(value, label)
    if args.batch <= 0:
        raise ValueError("--batch 必须大于 0")
    if args.repartition < 0:
        raise ValueError("--repartition 不能小于 0")
    if args.id_length <= 0 or args.id_length > 4096:
        raise ValueError("--id-length 必须在 1 到 4096 之间")
    if args.id_type == "INT" and args.id_length > 19:
        raise ValueError("INT 类型的 --id-length 不能超过 19")

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    try:
        dataframe = spark.sql(_sql(args))
        properties = _fields(args.property_fields)
        if not properties:
            properties = [field for field in dataframe.columns if field != args.id_field]
        required = [args.id_field, *properties]
        missing = [field for field in required if field not in dataframe.columns]
        if missing:
            raise ValueError(f"Spark SQL 结果缺少字段：{', '.join(dict.fromkeys(missing))}")
        if args.repartition:
            dataframe = dataframe.repartition(args.repartition)
        config = {
            "host": args.host,
            "port": args.port,
            "user": args.user,
            "password": args.password,
            "space": args.space,
            "node_type": args.node_type,
            "id_field": args.id_field,
            "id_type": args.id_type,
            "id_length": args.id_length,
            "property_fields": properties,
            "batch": args.batch,
            "timeout_ms": args.timeout_ms,
        }
        dataframe.foreachPartition(partial(cengyan_udf, config=config))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError) as ex:
        print(f"错误：{ex}", file=sys.stderr)
        raise SystemExit(1) from ex
