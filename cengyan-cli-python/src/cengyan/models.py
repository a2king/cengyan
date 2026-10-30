from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Property:
    name: str
    type: str
    nullable: bool = True
    comment: Optional[str] = None


@dataclass
class GraphInfo:
    name: str
    graph_id: Optional[int] = None
    comment: Optional[str] = None


@dataclass
class NodeTypeInfo:
    name: str
    id: Optional[int] = None
    schema_version: Optional[int] = None
    id_type: str = "INT"
    id_length: int = 19
    comment: Optional[str] = None


@dataclass
class RelTypeInfo:
    name: str
    id: Optional[int] = None
    schema_version: Optional[int] = None
    comment: Optional[str] = None


@dataclass
class NodeRow:
    id: Any
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class EdgeRow:
    src: Any
    dst: Any
    rank: int = 0
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class BulkProgress:
    batch_index: int
    rows_submitted: int
    rows_affected: int
