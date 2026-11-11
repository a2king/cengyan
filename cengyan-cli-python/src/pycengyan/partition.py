"""与 Java PartitionRouter / EntityId 一致的分片哈希。"""
from __future__ import annotations

import struct

_MASK = (1 << 64) - 1
_P1 = 0x9E3779B185EBCA87
_P2 = 0xC2B2AE3D27D4EB4F
_P3 = 0x165667B19E3779F9
_P4 = 0x85EBCA77C2B2AE63
_P5 = 0x27D4EB2F165667C5


def _u64(value: int) -> int:
    return value & _MASK


def _rotl(value: int, bits: int) -> int:
    value = _u64(value)
    return _u64((value << bits) | (value >> (64 - bits)))


def _round(acc: int, data: int) -> int:
    return _rotl(_u64(acc + _u64(data * _P2)), 31) * _P1


def _merge(acc: int, value: int) -> int:
    return _u64((_u64(acc) ^ _round(0, value)) * _P1 + _P4)


def xxhash64(data: bytes) -> int:
    """xxHash64，seed=0，返回无符号 64 位。"""
    view = memoryview(data)
    offset = 0
    if len(data) >= 32:
        v1, v2, v3, v4 = _u64(_P1 + _P2), _P2, 0, _u64(-_P1)
        while offset + 32 <= len(data):
            v1 = _round(v1, int.from_bytes(view[offset:offset + 8], "little"))
            v2 = _round(v2, int.from_bytes(view[offset + 8:offset + 16], "little"))
            v3 = _round(v3, int.from_bytes(view[offset + 16:offset + 24], "little"))
            v4 = _round(v4, int.from_bytes(view[offset + 24:offset + 32], "little"))
            offset += 32
        mixed = _rotl(v1, 1) + _rotl(v2, 7) + _rotl(v3, 12) + _rotl(v4, 18)
        mixed = _merge(_merge(_merge(_merge(mixed, v1), v2), v3), v4)
    else:
        mixed = _P5
    mixed = _u64(mixed + len(data))
    while offset + 8 <= len(data):
        mixed = _u64(_rotl(mixed ^ _round(0, int.from_bytes(view[offset:offset + 8], "little")), 27) * _P1 + _P4)
        offset += 8
    if offset + 4 <= len(data):
        mixed = _u64(_rotl(mixed ^ (_u64(int.from_bytes(view[offset:offset + 4], "little") * _P1)), 23) * _P2 + _P3)
        offset += 4
    while offset < len(data):
        mixed = _u64(_rotl(mixed ^ (_u64(data[offset] * _P5)), 11) * _P1)
        offset += 1
    mixed ^= mixed >> 33
    mixed = _u64(mixed * _P2)
    mixed ^= mixed >> 29
    mixed = _u64(mixed * _P3)
    return mixed ^ (mixed >> 32)


def canonical_id(value, id_type: str) -> bytes:
    kind = id_type.upper()
    if kind == "STRING":
        if not isinstance(value, str) or isinstance(value, bool):
            raise ValueError("字符串 ID 必须是 str")
        return b"\x02" + value.encode("utf-8")
    if kind == "INT64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("整数 ID 必须是 int")
        return b"\x01" + struct.pack(">q", value)
    raise ValueError("未知 ID 类型：" + id_type)


def part_of(value, id_type: str, partitions: int) -> int:
    if not 1 <= partitions <= 65535:
        raise ValueError("分片数无效")
    return xxhash64(canonical_id(value, id_type)) % partitions
