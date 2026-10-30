from __future__ import annotations

import csv
import json
from typing import Any, Iterable


class Result:
    def __init__(self, response):
        self.raw = response
        self.columns = list(response.get("columns") or [])
        self.rows = list(response.get("rows") or [])
        self.rows_affected = int(response.get("rowsAffected", 0) or 0)
        self.query_id = response.get("queryId")

    @property
    def affected_rows(self):
        return self.rows_affected

    def iter_rows(self):
        for row in self.rows:
            yield {column: row[index] if index < len(row) else None for index, column in enumerate(self.columns)}

    def to_dicts(self): return list(self.iter_rows())
    def to_tuples(self): return [tuple(row) for row in self.rows]
    def first(self): return next(iter(self.iter_rows()), None)
    def one(self):
        if len(self.rows) != 1:
            raise ValueError("result does not contain exactly one row")
        return next(self.iter_rows())
    def scalar(self):
        return self.rows[0][0] if self.rows and self.rows[0] else None
    def json(self, **kwargs): return json.dumps(self.to_dicts(), ensure_ascii=False, **kwargs)
    def write_json(self, stream, **kwargs): stream.write(self.json(**kwargs))
    def write_jsonl(self, stream):
        for row in self.iter_rows(): stream.write(json.dumps(row, ensure_ascii=False) + "\n")
    def write_csv(self, stream):
        writer = csv.DictWriter(stream, fieldnames=self.columns)
        writer.writeheader(); writer.writerows(self.iter_rows())
    def to_json(self, path, **kwargs):
        with open(path, "w", encoding="utf-8") as stream: self.write_json(stream, **kwargs)
    def to_jsonl(self, path):
        with open(path, "w", encoding="utf-8") as stream: self.write_jsonl(stream)
    def to_csv(self, path):
        with open(path, "w", encoding="utf-8-sig", newline="") as stream: self.write_csv(stream)
