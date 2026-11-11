"""请求与批次的 JSON 编码。

发送和字节计数必须共用同一套规则：此前 transport 用 ``default=str`` 编码，
而批次计量用不带 default 的 ``json.dumps``，于是 datetime/Decimal/UUID 字段
在 execute 里能写、在 bulk 里直接抛 TypeError。任何新值的序列化方式只在这里定义。
"""
from __future__ import annotations

import json


def dumps(payload):
    """线上的唯一编码形式。"""
    return json.dumps(payload, ensure_ascii=False, default=str)


def encoded_size(payload):
    """payload 写进请求体时的 UTF-8 字节数。

    与 dumps 共用同一套规则和分隔符，保证"计量多少"就是"发出多少"。
    """
    return len(dumps(payload).encode("utf-8"))
