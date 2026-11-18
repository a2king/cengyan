"""请求与批次的 JSON 编码。

发送和字节计数必须共用同一套规则：此前 transport 用 ``default=str`` 编码，
而批次计量用不带 default 的 ``json.dumps``，于是 datetime/Decimal/UUID 字段
在 execute 里能写、在 bulk 里直接抛 TypeError。任何新值的序列化方式只在这里定义。

- 紧凑分隔符：与服务端 Jackson 的输出一致，计量结果接近服务端看到的字节数；
- ``allow_nan=False``：NaN/Infinity 不是合法 JSON，服务端会拒绝整行请求，
  在客户端就以 ``ValueError`` 拒绝，不让它变成"提交状态未知"。
"""
from __future__ import annotations

import json

# 服务端 LineBasedFrameDecoder 的单行上限；超过时服务端直接断开连接。
MAX_LINE_BYTES = 8 * 1024 * 1024


def dumps(payload):
    """线上的唯一编码形式。"""
    return json.dumps(payload, ensure_ascii=False, default=str, allow_nan=False, separators=(",", ":"))


def encoded_size(payload):
    """payload 写进请求体时的 UTF-8 字节数。

    与 dumps 共用同一套规则和分隔符，保证"计量多少"就是"发出多少"。
    """
    return len(dumps(payload).encode("utf-8"))
