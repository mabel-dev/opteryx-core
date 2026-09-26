"""
Internal Opteryx schema system - the Draken-native engine.

This module provides schema definitions for Opteryx, eliminating the external
external dependency, specialized for Opteryx's actual use cases.

Opteryx only uses: RelationSchema, SchemaColumn, ConstantColumn
Advanced features (DictionaryColumn, SparseColumn, RLEColumn, FunctionColumn)
are deferred to Phase 9 if needed.

Key design:
- Dataclass-based for simplicity and performance
- No external dependencies (stdlib only + opteryx.types)
- Optimized for the operations Opteryx actually performs
- Full type hints; comprehensive docstrings
"""

from __future__ import annotations

import copy as copy_module
import dataclasses
from typing import Any, Dict, List, Optional


def mint_column_identity(relation: Optional[str], column: Optional[str]) -> bytes:
    """Mint a unique, opaque column identity with a traceable prefix.

    Identities are the engine's per-column handles; they MUST be unique (the
    name is not — two relations can share a column name). The random suffix
    guarantees uniqueness; the ``rel_col_`` prefix is a debugging affordance so
    that an identity leaked into an error/stack trace can be traced back to a
    physical column. The query's ColumnTable is the only caller.
    """
    from opteryx.utils import random_string

    rel = (relation or "")[:3]
    col = (column or "")[:3]
    return f"{rel}_{col}_{random_string(8)}".encode("utf-8")


def _mint_tagged_identity(tag: str) -> bytes:
    """Mint a unique identity for a non-relation column (e.g. ``$const``, ``$derived``)."""
    from opteryx.utils import random_string

    return f"{tag}_{random_string(8)}".encode("utf-8")


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


@dataclasses.dataclass
class SchemaColumn:
    """Column definition with metadata.

    Opteryx column definition.

    Attributes:
        name: Column name (required)
        column_type: Unified ColumnType carrier (physical DrakenType + optional logical descriptor)
        identity: Unique identifier for this column (default: auto-generated from name)
        nullable: Whether NULL values are allowed (default: True)
        default: Default value if not provided (default: None)
        description: Human-readable description (default: None)
        disposition: Special treatment flag (default: None)
        aliases: Alternative names for this column (default: None)
        origin: The relation(s) the column is read through

    A BOUND column: made from a source's ColumnDescriptor (or minted as a computed
    column) in the query's ColumnTable, which is the only thing that sets its
    identity and slot - see opteryx/planner/plan_context.py.
    """

    name: str
    nullable: bool = True
    identity: Optional[bytes] = None
    # Stable, catalog-assigned column identifier (Iceberg-style field-id),
    # distinct from `identity` above (a random, non-persistent, engine-internal
    # join/dedup handle re-minted on every schema normalization). Used to key
    # per-file manifest min/max statistics so they survive schema evolution
    # without positional drift. None for sources with no catalog-assigned id
    # (e.g. ad-hoc Arrow/pandas inputs, or catalog schemas predating this).
    field_id: Optional[int] = None
    default: Optional[Any] = None
    description: Optional[str] = None
    disposition: Optional[str] = None
    aliases: Optional[List[str]] = dataclasses.field(default_factory=lambda: None)
    origin: Optional[List[str]] = None
    # column_type is the authoritative unified type carrier (physical DrakenType +
    # optional LogicalType descriptor + optional ARRAY element). Deepcopy
    # is safe — LogicalType has __deepcopy__ wired on the nanobind side.
    column_type: Optional[Any] = dataclasses.field(default=None, repr=False, compare=False)
    # The column's number in its query — its position in the query's ColumnTable
    # (opteryx/planner/plan_context.py), which is the only thing that sets it.
    # Not part of the column's value: two columns compare by what they describe.
    slot: Optional[int] = dataclasses.field(default=None, repr=False, compare=False)

    def branch_copy(self, memo: dict) -> "SchemaColumn":
        """Copy for binder branch isolation — see RelationSchema.branch_copy.

        Preserves the concrete subclass and any extra fields via copy.copy,
        then detaches exactly the state the binder mutates in place on a
        column: the aliases list (`aliases.append` in binder.py/project.py),
        origin, and nested STRUCT fields. Everything else is either rebound
        (never mutated) or immutable (identity bytes, ColumnType), so sharing
        it is safe and cheap. `memo` keeps one column object referenced from
        two places copying to ONE new object, exactly like deepcopy's memo —
        losing that aliasing would silently split columns the binder expects
        to be the same object.
        """
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        clone = copy_module.copy(self)
        memo[id(self)] = clone
        if clone.aliases is not None:
            clone.aliases = list(clone.aliases)
        if clone.origin is not None:
            clone.origin = list(clone.origin)
        return clone

    def __post_init__(self):
        """Normalize identity.

        A column identity is a unique, opaque handle — the execution engine keys
        columns by it, so it must NOT be derived from the (non-unique) name. A
        ``None`` identity means a mint site failed to assign one; fail loud rather
        than silently falling back to the name (which collapses distinct columns
        that share a name — every self-join, and any join of tables with a common
        column name — into one).
        """
        if self.identity is None:
            from opteryx.exceptions import InvalidInternalStateError

            raise InvalidInternalStateError(
                f"Column '{self.name}' was constructed without an identity. "
                "Relation-sourced columns must be minted with a unique identity; "
                "the name is not a valid identity."
            )
        if isinstance(self.identity, str):
            self.identity = self.identity.encode("utf-8")

    @property
    def category(self):
        """Operator-dispatch category projection of `column_type` (the one type carrier).

        Returns `None` when no `column_type` is resolved yet. This is a pure projection
        of `column_type` — not a parallel type.
        """
        if self.column_type is None:
            return None
        return self.column_type.category

    def __str__(self) -> str:
        """String representation: name."""
        ct = self.column_type
        if ct is not None:
            return f"{self.name}:{ct}"
        return self.name

    def _to_plain_flatcolumn(self) -> "SchemaColumn":
        """Build a plain SchemaColumn mirroring this column's type + metadata."""
        common = dict(
            identity=self.identity,
            nullable=self.nullable,
            default=self.default,
            description=self.description,
            disposition=self.disposition,
            aliases=self.aliases,
            origin=self.origin,
            # The same column, stripped of its subclass: same identity, same slot.
            slot=self.slot,
        )
        return SchemaColumn(name=self.name, column_type=self.column_type, **common)

    def describe(self) -> "ColumnDescriptor":
        """What this bound column says about its values, as a source would describe
        it - for handing a bound shape BACK to a source (a CTAS target, a view's
        recorded schema). Engine state (identity, slot, origin, aliases) stays behind."""
        return ColumnDescriptor(
            name=self.name,
            column_type=self.column_type,
            nullable=self.nullable,
            field_id=self.field_id,
            default=self.default,
            description=self.description,
            disposition=self.disposition,
        )

    def to_schema_column(self) -> "SchemaColumn":
        """Convert to a SchemaColumn (returns self when already a plain SchemaColumn)."""
        if type(self) is SchemaColumn:
            return self
        return self._to_plain_flatcolumn()

    def __repr__(self) -> str:
        return f"SchemaColumn(name={self.name!r}, column_type={self.column_type}, nullable={self.nullable})"

    @property
    def all_names(self) -> List[str]:
        """Get all names for this column (name + aliases)."""
        names = [self.name]
        if self.aliases:
            names.extend(self.aliases)
        return names


@dataclasses.dataclass
class ConstantColumn(SchemaColumn):
    """Column with a constant value.

    Used for constant expressions (e.g., SELECT 42 AS constant_col).
    Inherits from SchemaColumn with additional constant value semantics.
    """

    value: Any = None

    def __str__(self) -> str:
        """String representation: name = value."""
        return f"{self.name}={self.value}"

    def to_schema_column(self) -> SchemaColumn:
        """Convert to a SchemaColumn, stripping constant value."""
        return self._to_plain_flatcolumn()


@dataclasses.dataclass
class FunctionColumn(SchemaColumn):
    """Column defined by a function/expression.

    Used for computed columns (e.g., SELECT col1 + col2 AS sum_col).
    Inherits from SchemaColumn with additional function expression semantics.
    """

    def __str__(self) -> str:
        """String representation: name (computed)."""
        return f"{self.name}(computed)"

    def to_schema_column(self) -> SchemaColumn:
        """Convert to a SchemaColumn, stripping function metadata."""
        return self._to_plain_flatcolumn()



@dataclasses.dataclass
class RelationSchema:
    """Table/relation schema definition.

    Opteryx relation schema.

    Attributes:
        name: Schema/table name (required)
        columns: List of SchemaColumn definitions (required)
        aliases: Alternative names for this schema (default: [])
        primary_key: Name of primary key column (default: None)
        row_count_metric: Actual row count if known (default: None)
        row_count_estimate: Estimated row count (default: None)
        data_size_metric: Actual data size in bytes (default: None)
        data_size_estimate: Estimated data size in bytes (default: None)
    """

    name: str
    columns: List[SchemaColumn] = dataclasses.field(default_factory=list)
    aliases: List[str] = dataclasses.field(default_factory=list)
    primary_key: Optional[str] = None
    row_count_metric: Optional[int] = None
    row_count_estimate: Optional[int] = None
    data_size_metric: Optional[int] = None
    data_size_estimate: Optional[int] = None

    def __str__(self) -> str:
        """String representation: schema_name(col1, col2, ...)."""
        col_list = ", ".join(str(c) for c in self.columns)
        return f"{self.name}({col_list})"

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"RelationSchema(name={self.name!r}, num_columns={len(self.columns)})"

    def branch_copy(self, memo: dict) -> "RelationSchema":
        """Copy for binder branch isolation.

        The binder binds a join's two legs (and a filter's guarded scope)
        against independent copies of the in-scope schemas, because binding
        MUTATES schemas and their columns in place. This is the hand-rolled
        replacement for `deepcopy(schemas)` — same isolation, same aliasing
        (one shared `memo` per BindingContext.copy keeps a column object
        referenced from two schemas copying to one new object), a fraction of
        the cost: deepcopy's generic machinery was ~10% of binder time on a
        wide-schema workload.
        """
        cached = memo.get(id(self))
        if cached is not None:
            return cached
        clone = copy_module.copy(self)
        memo[id(self)] = clone
        clone.columns = [c.branch_copy(memo) for c in self.columns]
        clone.aliases = list(self.aliases)
        return clone

    def describe(self) -> "RelationDescriptor":
        """This bound relation as a source would describe it - see SchemaColumn.describe."""
        return RelationDescriptor(
            name=self.name,
            columns=[column.describe() for column in self.columns],
            aliases=list(self.aliases),
            primary_key=self.primary_key,
            row_count_metric=self.row_count_metric,
            row_count_estimate=self.row_count_estimate,
            data_size_metric=self.data_size_metric,
            data_size_estimate=self.data_size_estimate,
        )

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

    def pop_column(self, name: str) -> Optional[SchemaColumn]:
        """Remove and return column by name.

        Args:
            name: Column name to remove

        Returns:
            Removed SchemaColumn if found, None otherwise
        """
        for i, col in enumerate(self.columns):
            if col.name == name:
                return self.columns.pop(i)
        return None

    def validate(self) -> bool:
        """Validate schema consistency.

        Returns:
            True if schema is valid, False otherwise
        """
        # Check for duplicate column names
        names = set()
        for col in self.columns:
            if col.name in names:
                return False
            names.add(col.name)
        return True

    def __add__(self, other: "RelationSchema") -> "RelationSchema":
        """Combine two schemas by merging columns.

        Args:
            other: Another RelationSchema to combine with

        Returns:
            New RelationSchema with combined columns
        """
        if not isinstance(other, RelationSchema):
            return NotImplemented

        combined_columns = self.columns + other.columns
        return RelationSchema(
            name=self.name,
            columns=combined_columns,
            aliases=self.aliases + other.aliases,
            primary_key=self.primary_key or other.primary_key,
        )

    def __iadd__(self, other: "RelationSchema") -> "RelationSchema":
        """In-place merge with another schema.

        Args:
            other: Another RelationSchema to combine with

        Returns:
            Self with columns from other schema added
        """
        if not isinstance(other, RelationSchema):
            return NotImplemented

        self.columns.extend(other.columns)
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
