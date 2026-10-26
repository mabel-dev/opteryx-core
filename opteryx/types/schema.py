"""
Internal Opteryx schema system - the Draken-native engine.

This module provides schema definitions for Opteryx, eliminating the external
external dependency, specialized for Opteryx's actual use cases.

Opteryx only uses: RelationSchema, SchemaColumn, ConstantColumn
Advanced features (DictionaryColumn, SparseColumn, RLEColumn, FunctionColumn)
are deferred to Phase 9 if needed.

Key design:
- Bound columns (SchemaColumn and its kinds) are native rows of the query's
  ColumnTable (opteryx/compiled/planner/column_table.pyx); a bound relation's
  schema is a compiled class (opteryx/compiled/planner/relation_schema.pyx);
  descriptors are dataclasses here
- No external dependencies (stdlib only + opteryx.types)
- Optimized for the operations Opteryx actually performs
- Full type hints; comprehensive docstrings
"""

from __future__ import annotations

import dataclasses
from typing import Any, Dict, List, Optional

# Bound columns are rows of the query's native ColumnTable; their façade classes
# live with it (opteryx/compiled/planner/column_table.pyx, native plan graph P1).
from opteryx.compiled.planner.column_table import ConstantColumn
from opteryx.compiled.planner.column_table import FunctionColumn
from opteryx.compiled.planner.column_table import SchemaColumn
from opteryx.compiled.planner.column_table import mint_column_identity  # noqa: F401 (re-exported)
from opteryx.compiled.planner.relation_schema import RelationSchema


__all__ = [
    "SchemaColumn",
    "ConstantColumn",
    "FunctionColumn",
    "RelationSchema",
    "ColumnDescriptor",
    "RelationDescriptor",
    "ColumnDisposition",
]


class ColumnDisposition:
    """Column disposition flags (simplified).

    Indicates special treatment for columns:
    - INTERNAL: system-generated column (e.g., __index__)
    - PRIMARY_KEY: part of primary key
    - INDEXED: column has an index
    - NAME: column represents a human name
    - AGE: column represents an age value
    """

    INTERNAL = "INTERNAL"
    PRIMARY_KEY = "PRIMARY_KEY"
    INDEXED = "INDEXED"
    NAME = "NAME"
    AGE = "AGE"


def _column_type_from_dict(data: Dict[str, Any]) -> Any:
    """Pop and parse a persisted column's type: the v2 `column_type` string, else the
    v1 `type` string (its precision/scale/length/element side-cars are subsumed by
    the parameterized form `parse_column_type` reads)."""
    from opteryx.types.logical_type import parse_column_type

    ct_str = data.pop("column_type", None)
    raw_type = data.pop("type", None)
    for legacy in ("precision", "scale", "length", "element_type"):
        data.pop(legacy, None)
    if ct_str is not None:
        return parse_column_type(ct_str)
    if isinstance(raw_type, str):
        return parse_column_type(raw_type)
    return raw_type


@dataclasses.dataclass(frozen=True)
class NestedField:
    """One node of a nested column's declared shape, as the TABLE (a catalog) states it.

    Iceberg identifies a nested field by id, and names change; a JSON object is made of names. A
    scan that renders a nested column as JSON text verifies each data file against this tree and
    refuses a file that disagrees, naming the field - it never renders file names that may have
    drifted (docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.3, option (b)).

    kind: "struct" (children are the members, in order), "list" (one child: the element), "map"
    (two children: key, value) or "scalar". `name` is a struct member's name; a list's element and a
    map's key / value are named "element", "key" and "value" and their names are never compared.
    """

    field_id: int
    name: str
    kind: str
    children: tuple = ()

    KINDS = ("struct", "list", "map", "scalar")

    def __post_init__(self):
        if self.kind not in self.KINDS:
            raise ValueError(f"NestedField kind must be one of {self.KINDS}, not {self.kind!r}")
        arity = {"list": 1, "map": 2, "scalar": 0}.get(self.kind)
        if arity is not None and len(self.children) != arity:
            raise ValueError(f"NestedField {self.name!r}: a {self.kind} has {arity} children, not {len(self.children)}")

    @classmethod
    def from_tuple(cls, node: tuple) -> "NestedField":
        """From the dependency-free spelling a catalog hands over: (kind, field id, name, children)."""
        kind, field_id, name, children = node
        return cls(field_id, name, kind, tuple(cls.from_tuple(child) for child in children))

    def flatten(self) -> list:
        """Pre-order [(kind, field_id, name, number of children)] - the form the native readers verify
        against."""
        out = [(self.kind, self.field_id, self.name, len(self.children))]
        for child in self.children:
            out.extend(child.flatten())
        return out


@dataclasses.dataclass
class ColumnDescriptor:
    """What a SOURCE says about one of its columns — and nothing the engine owns.

    Connectors, virtual datasets, persisted schemas and CREATE TABLE describe their
    columns with this. A bound column (`SchemaColumn`) is made from it by the binder
    in the query's ColumnTable (`opteryx/planner/plan_context.py`), which is where
    the engine state lives: identity, slot, origin and aliases exist ONLY on the
    bound column (architect ruling 2026-09-26, stage 4 option C).
    """

    name: str
    column_type: Optional[Any] = dataclasses.field(default=None, compare=False)
    nullable: bool = True
    # Stable, catalog-assigned column identifier (Iceberg-style field-id); keys
    # per-file manifest statistics so they survive schema evolution.
    field_id: Optional[int] = None
    default: Optional[Any] = None
    description: Optional[str] = None
    disposition: Optional[str] = None
    # A STRUCT / MAP / LIST column's declared shape, when the source states one (a catalog table).
    # Absent for READ_* and folder datasets: the file IS the schema there.
    nested: Optional[NestedField] = dataclasses.field(default=None, compare=False)

    @property
    def category(self):
        """Operator-dispatch category projection of `column_type`."""
        if self.column_type is None:
            return None
        return self.column_type.category

    def __str__(self) -> str:
        if self.column_type is not None:
            return f"{self.name}:{self.column_type}"
        return self.name

    _SCHEMA_VERSION = 2

    def to_dict(self) -> Dict[str, Any]:
        """The persisted (v2) form."""
        from opteryx.types.logical_type import serialize_column_type

        return {
            "_v": self._SCHEMA_VERSION,
            "name": self.name,
            "column_type": serialize_column_type(self.column_type),
            "type": self.column_type.category.name if self.column_type is not None else None,
            "nullable": self.nullable,
            "default": self.default,
            "description": self.description,
            "disposition": self.disposition,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColumnDescriptor":
        """Read a persisted column (v2, or the v1 type quartet).

        Documents written before descriptors carry the bound column's `identity` -
        a random per-query handle that never meant anything once written down - so
        it is discarded here. They may also carry `aliases`; an alias is binder
        state, so a NON-EMPTY list cannot be represented and is refused rather than
        dropped. Any other unknown key fails the constructor.
        """
        data = dict(data)
        data.pop("_v", None)
        data.pop("identity", None)
        aliases = data.pop("aliases", None)
        if aliases:
            from opteryx.exceptions import InvalidInternalStateError

            raise InvalidInternalStateError(
                f"Persisted column '{data.get('name')}' carries aliases {aliases!r}; "
                "a column description has none - aliases belong to a bound column."
            )
        column_type = _column_type_from_dict(data)
        return cls(name=data.pop("name"), column_type=column_type, **data)


@dataclasses.dataclass
class RelationDescriptor:
    """What a SOURCE says about one of its relations: its columns as descriptors,
    plus the relation-level facts it knows. The binder turns it into a bound
    `RelationSchema` (ColumnTable.bind_relation); nothing else produces one."""

    name: str
    columns: List[ColumnDescriptor] = dataclasses.field(default_factory=list)
    aliases: List[str] = dataclasses.field(default_factory=list)
    primary_key: Optional[str] = None
    row_count_metric: Optional[int] = None
    row_count_estimate: Optional[int] = None
    data_size_metric: Optional[int] = None
    data_size_estimate: Optional[int] = None

    def __str__(self) -> str:
        return f"{self.name}({', '.join(str(c) for c in self.columns)})"

    @property
    def column_names(self) -> List[str]:
        return [column.name for column in self.columns]

    def copy(self) -> "RelationDescriptor":
        """An independent copy: its own column list of its own descriptors, so a
        holder that adjusts what it was handed (a relation-level metric, a column
        list) cannot reach back into a cached original.

        Shallow copies made straight from the instance dict — what `copy.copy`
        produces for these plain dataclasses (no __slots__, no copy hooks), without
        its generic __reduce_ex__ protocol, which cost ~2 µs per column on every
        query that read a cached dataset schema (2026-10-18)."""
        clone = object.__new__(RelationDescriptor)
        clone.__dict__.update(self.__dict__)
        columns = []
        for column in self.columns:
            column_copy = object.__new__(type(column))
            column_copy.__dict__.update(column.__dict__)
            columns.append(column_copy)
        clone.columns = columns
        clone.aliases = list(self.aliases)
        return clone

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "columns": [column.to_dict() for column in self.columns],
            "aliases": self.aliases,
            "primary_key": self.primary_key,
            "row_count_metric": self.row_count_metric,
            "row_count_estimate": self.row_count_estimate,
            "data_size_metric": self.data_size_metric,
            "data_size_estimate": self.data_size_estimate,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RelationDescriptor":
        data = dict(data)
        data["columns"] = [ColumnDescriptor.from_dict(column) for column in data.get("columns", [])]
        return cls(**data)
