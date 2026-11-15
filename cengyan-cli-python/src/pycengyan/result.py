from __future__ import annotations

import csv
import json

from .exceptions import ResultError


class Result:
    """一条查询的完整结果。

    rows 是列顺序的元组/列表，iter_rows() 产出按列名映射的 dict。结果集已经在内存里，
    因此 len() 和迭代返回行数，不触发额外请求。
    """

    def __init__(self, response):
        self.raw = response
        self.columns = list(response.get("columns") or [])
        self.rows = list(response.get("rows") or [])
        self.rows_affected = int(response.get("rowsAffected", 0) or 0)
        self.query_id = response.get("queryId")

    def __len__(self):
        return len(self.rows)

    def __iter__(self):
        return self.iter_rows()

    def __repr__(self):
        return f"<Result columns={len(self.columns)} rows={len(self.rows)} affected={self.rows_affected}>"

    @property
    def affected_rows(self):
        return self.rows_affected

    def iter_rows(self):
        # 列名只在构造时解析一次，避免每行重复遍历。
        columns = self.columns
        for row in self.rows:
            yield {column: row[index] if index < len(row) else None for index, column in enumerate(columns)}

    def to_dicts(self): return list(self.iter_rows())
    def to_tuples(self): return [tuple(row) for row in self.rows]

    def first(self):
        return next(iter(self.iter_rows()), None)

    def one(self):
        if len(self.rows) != 1:
            raise ResultError(f"result does not contain exactly one row (got {len(self.rows)})", "RESULT_SHAPE")
        return next(iter(self.iter_rows()))

    def scalar(self):
        return self.rows[0][0] if self.rows and self.rows[0] else None

    def json(self, **kwargs): return json.dumps(self.to_dicts(), ensure_ascii=False, **kwargs)
    def write_json(self, stream, **kwargs): stream.write(self.json(**kwargs))

    def write_jsonl(self, stream):
        for row in self.iter_rows():
            stream.write(json.dumps(row, ensure_ascii=False) + "\n")

    def write_csv(self, stream):
        writer = csv.DictWriter(stream, fieldnames=self.columns)
        writer.writeheader()
        writer.writerows(self.iter_rows())

    def to_json(self, path, **kwargs):
        with open(path, "w", encoding="utf-8") as stream:
            self.write_json(stream, **kwargs)

    def to_jsonl(self, path):
        with open(path, "w", encoding="utf-8") as stream:
            self.write_jsonl(stream)

    def to_csv(self, path):
        with open(path, "w", encoding="utf-8-sig", newline="") as stream:
            self.write_csv(stream)
