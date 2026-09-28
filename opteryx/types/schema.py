"""
Internal Opteryx schema system - the Draken-native engine.

This module provides schema definitions for Opteryx, eliminating the external
external dependency, specialized for Opteryx's actual use cases.

Opteryx only uses: RelationSchema, SchemaColumn, ConstantColumn
Advanced features (DictionaryColumn, SparseColumn, RLEColumn, FunctionColumn)
are deferred to Phase 9 if needed.

Key design:
- Bound columns (SchemaColumn and its kinds) are native rows of the query's
  ColumnTable (opteryx/compiled/planner/column_table.pyx); descriptors and
  relation schemas are dataclasses here
- No external dependencies (stdlib only + opteryx.types)
- Optimized for the operations Opteryx actually performs
- Full type hints; comprehensive docstrings
"""

from __future__ import annotations

import copy as copy_module
import dataclasses
from typing import Any, Dict, List, Optional, Tuple

# Bound columns are rows of the query's native ColumnTable; their façade classes
# live with it (opteryx/compiled/planner/column_table.pyx, native plan graph P1).
from opteryx.compiled.planner.column_table import ConstantColumn
from opteryx.compiled.planner.column_table import FunctionColumn
from opteryx.compiled.planner.column_table import SchemaColumn
from opteryx.compiled.planner.column_table import mint_column_identity  # noqa: F401 (re-exported)


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


@dataclasses.dataclass(frozen=True)
class RelationSchema:
    """Table/relation schema definition.

    Opteryx relation schema. IMMUTABLE (architect ruling 2026-09-28, native plan
    graph P5): a plan step's native row mirrors the schema it holds, so an edit is
    a NEW schema (`dataclasses.replace`) assigned through the step - and, in the
    binder, re-bound wherever the old one was shared
    (BindingContext.rebind_schema). `columns` and `aliases` are tuples.

    Attributes:
        name: Schema/table name (required)
        columns: SchemaColumn definitions (required)
        aliases: Alternative names for this schema (default: ())
        primary_key: Name of primary key column (default: None)
        row_count_metric: Actual row count if known (default: None)
        row_count_estimate: Estimated row count (default: None)
        data_size_metric: Actual data size in bytes (default: None)
        data_size_estimate: Estimated data size in bytes (default: None)
    """

    name: str
    columns: Tuple[SchemaColumn, ...] = ()
    aliases: Tuple[str, ...] = ()
    primary_key: Optional[str] = None
    row_count_metric: Optional[int] = None
    row_count_estimate: Optional[int] = None
    data_size_metric: Optional[int] = None
    data_size_estimate: Optional[int] = None

    def __post_init__(self):
        # A list given for either sequence is held as a tuple: the schema is a value.
        object.__setattr__(self, "columns", tuple(self.columns))
        object.__setattr__(self, "aliases", tuple(self.aliases))

    def clone(self) -> "RelationSchema":
        """A distinct schema object of the same value. The fields are values
        (tuples, scalars), so the new object shares them; built straight from
        the instance dict - the binder copies schemas per branch, and
        `dataclasses.replace` re-runs __init__ and __post_init__ each time."""
        new = object.__new__(RelationSchema)
        new.__dict__.update(self.__dict__)
        return new

    def with_columns(self, columns) -> "RelationSchema":
        """This schema with `columns` in place of its own - a new schema."""
        new = self.clone()
        new.__dict__["columns"] = tuple(columns)
        return new

    def __str__(self) -> str:
        """String representation: schema_name(col1, col2, ...)."""
        col_list = ", ".join(str(c) for c in self.columns)
        return f"{self.name}({col_list})"

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"RelationSchema(name={self.name!r}, num_columns={len(self.columns)})"

    def branch_copy(self, memo: dict) -> "RelationSchema":
        """A distinct schema object of the same value, for binder branch isolation.

        The binder binds a join's two legs (and a filter's guarded scope)
        against their own schema objects, because binding RE-BINDS a scope's
        schema (narrowing, alias rows swapped in) and BindingContext.rebind_schema
        replaces a schema by object IDENTITY wherever it is shared: a branch's
        object must not be the scan's, or a branch's narrowing would reach the
        scan. The columns themselves are shared - a column is a row of the
        query's ColumnTable. One shared `memo` per BindingContext.copy keeps a
        schema reachable under two keys copying to one new schema.
        """
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        clone = self.clone()
        memo[id(self)] = clone
        return clone

    @property
    def column_names(self) -> List[str]:
        """Get list of all column names."""
        return [col.name for col in self.columns]

    @property
    def all_column_names(self) -> List[str]:
        """Get all column names including aliases."""
        names = []
        for col in self.columns:
            names.extend(col.all_names)
        return names

    @property
    def num_columns(self) -> int:
        """Get number of columns."""
        return len(self.columns)

    def column(self, name: str, case_insensitive: bool = False) -> Optional[SchemaColumn]:
        """Find column by name (including aliases).

        Args:
            name: Column name to search for
            case_insensitive: If True, perform case-insensitive comparison

        Returns:
            SchemaColumn if found, None otherwise
        """
        if case_insensitive:
            name_lower = name.lower()
            for col in self.columns:
                if col.name.lower() == name_lower:
                    return col
                if col.aliases:
                    for alias in col.aliases:
                        if alias.lower() == name_lower:
                            return col
        else:
            for col in self.columns:
                if col.name == name or (col.aliases and name in col.aliases):
                    return col
        return None

    def find_column(self, name: str, case_insensitive: bool = False) -> Optional[SchemaColumn]:
        """Alias for column() for API compatibility."""
        return self.column(name, case_insensitive=case_insensitive)

    def without_column(self, name: str) -> "RelationSchema":
        """This schema without its first column named `name` - a new schema (or
        this one, when it has no such column)."""
        for i, col in enumerate(self.columns):
            if col.name == name:
                return self.with_columns(self.columns[:i] + self.columns[i + 1 :])
        return self


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
        list) cannot reach back into a cached original."""
        clone = copy_module.copy(self)
        clone.columns = [copy_module.copy(column) for column in self.columns]
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
