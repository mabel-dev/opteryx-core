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

import dataclasses
from typing import Any, Dict, List, Optional


__all__ = [
    "SchemaColumn",
    "ConstantColumn",
    "FunctionColumn",
    "RelationSchema",
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

    Advanced fields (unused in current Opteryx, kept for future compatibility):
        highest_value: Estimated max value for statistics
        lowest_value: Estimated min value for statistics
        null_count: Number of NULLs for statistics
        fields: For STRUCT types, nested fields
        expectations: Data quality expectations (deferred to Phase 9)
        origin: Column lineage tracking (deferred to Phase 9)
    """

    name: str
    nullable: bool = True
    identity: Optional[bytes] = None
    default: Optional[Any] = None
    description: Optional[str] = None
    disposition: Optional[str] = None
    aliases: Optional[List[str]] = dataclasses.field(default_factory=lambda: None)
    highest_value: Optional[Any] = None
    lowest_value: Optional[Any] = None
    null_count: Optional[int] = None
    fields: Optional[List[SchemaColumn]] = None
    expectations: Optional[Any] = None  # Deferred to Phase 9
    origin: Optional[List[str]] = None  # Deferred to Phase 9
    # column_type is the authoritative unified type carrier (physical DrakenType +
    # optional LogicalType descriptor + optional ARRAY element). Deepcopy
    # is safe — LogicalType has __deepcopy__ wired on the nanobind side.
    column_type: Optional[Any] = dataclasses.field(default=None, repr=False, compare=False)

    def __post_init__(self):
        """Normalize identity."""
        if self.identity is None:
            raw = self.name
        else:
            raw = self.identity
        if isinstance(raw, str):
            self.identity = raw.encode("utf-8")
        else:
            self.identity = raw

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
        )
        return SchemaColumn(name=self.name, column_type=self.column_type, **common)

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

    # D-4 Phase 2: column_type is a stored field (see the dataclass declaration
    # above and the __post_init__ resolution). The former @property has been
    # replaced — per-access recomputation is gone; new readers should rely on
    # this single field. LogicalType has __deepcopy__ wired on the nanobind side
    # so schema deepcopies (binder's merge_schemas) work cleanly.

    # Schema-JSON format version. v2 (D-4 Phase 2 "full break"): the type is carried
    # by a single canonical `column_type` string (e.g. "DECIMAL(15, 2)",
    # "ARRAY<VARCHAR>") instead of the legacy type/precision/scale/element_type
    # quartet. `from_dict` still reads the v1 quartet for backward compatibility
    # with already-persisted schemas (the side-cars are valid InitVar params).
    _SCHEMA_VERSION = 2

    def to_dict(self) -> Dict[str, Any]:
        """Convert column to dictionary for serialization (v2 format)."""
        from opteryx.types.logical_type import serialize_column_type

        return {
            "_v": self._SCHEMA_VERSION,
            "name": self.name,
            # Canonical column_type string is authoritative; `type` (bare LogicalCategory
            # name) is kept for the rare column_type==None case and human readability.
            "column_type": serialize_column_type(self.column_type),
            "type": self.column_type.category.name if self.column_type is not None else None,
            "identity": self.identity.decode("utf-8") if isinstance(self.identity, bytes) else self.identity,
            "nullable": self.nullable,
            "default": self.default,
            "description": self.description,
            "disposition": self.disposition,
            "aliases": self.aliases,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SchemaColumn:
        """Create SchemaColumn from a serialized dict (v2 with v1 fallback)."""
        data = data.copy()
        data.pop("_v", None)
        ct_str = data.pop("column_type", None)

        # v2: reconstruct from the canonical column_type string.
        if ct_str is not None:
            from opteryx.types.logical_type import parse_column_type

            data.pop("type", None)  # column_type supersedes the bare type tag
            column_type = parse_column_type(ct_str)
            return cls(name=data.pop("name"), column_type=column_type, **data)

        # v1 fallback: legacy "type" string (e.g. "DECIMAL(10,2)", "ARRAY<VARCHAR>").
        # parse_column_type handles the parameterized/element forms directly.
        from opteryx.types.logical_type import parse_column_type

        raw_type = data.pop("type", None)
        data.pop("precision", None)
        data.pop("scale", None)
        data.pop("length", None)
        data.pop("element_type", None)

        if isinstance(raw_type, str):
            column_type = parse_column_type(raw_type)
            return cls(name=data.pop("name"), column_type=column_type, **data)
        # already a type object, or unknown — pass through
        return cls(name=data.pop("name"), column_type=raw_type, **data)


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

    def to_dict(self) -> Dict[str, Any]:
        """Convert schema to dictionary for serialization."""
        return {
            "name": self.name,
            "columns": [col.to_dict() for col in self.columns],
            "aliases": self.aliases,
            "primary_key": self.primary_key,
            "row_count_metric": self.row_count_metric,
            "row_count_estimate": self.row_count_estimate,
            "data_size_metric": self.data_size_metric,
            "data_size_estimate": self.data_size_estimate,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> RelationSchema:
        """Create RelationSchema from dictionary."""
        data = data.copy()
        # Convert column dicts to SchemaColumn objects
        if "columns" in data:
            data["columns"] = [
                SchemaColumn.from_dict(col) if isinstance(col, dict) else col
                for col in data["columns"]
            ]
        return cls(**data)

    def to_json(self) -> str:
        """Convert to JSON string."""
        import json

        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, json_str: str) -> RelationSchema:
        """Create from JSON string."""
        import json

        return cls.from_dict(json.loads(json_str))

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
