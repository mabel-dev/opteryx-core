"""
Internal Opteryx schema system - inlined and specialized from orso.schema.

This module provides schema definitions for Opteryx, eliminating the external
orso dependency while specializing for Opteryx's actual use cases.

Opteryx only uses: RelationSchema, FlatColumn, ConstantColumn
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

from opteryx.types import OrsoTypes

__all__ = [
    "FlatColumn",
    "ConstantColumn",
    "FunctionColumn",
    "RelationSchema",
    "ColumnDisposition",
]


class ColumnDisposition:
    """Column disposition flags (simplified from orso).

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
class FlatColumn:
    """Column definition with metadata.

    Replaces orso.schema.FlatColumn with Opteryx specialization.

    Attributes:
        name: Column name (required)
        type: OrsoType for this column (required)
        identity: Unique identifier for this column (default: auto-generated from name)
        nullable: Whether NULL values are allowed (default: True)
        default: Default value if not provided (default: None)
        description: Human-readable description (default: None)
        disposition: Special treatment flag (default: None)
        aliases: Alternative names for this column (default: None)
        element_type: For ARRAY types, type of elements (default: None)
        precision: For DECIMAL, total digits (default: None)
        scale: For DECIMAL, fractional digits (default: None)
        length: For VARCHAR/BLOB, max length (default: None)

    Advanced fields (unused in current Opteryx, kept for future compatibility):
        highest_value: Estimated max value for statistics
        lowest_value: Estimated min value for statistics
        null_count: Number of NULLs for statistics
        fields: For STRUCT types, nested fields
        expectations: Data quality expectations (deferred to Phase 9)
        origin: Column lineage tracking (deferred to Phase 9)
    """

    name: str
    type: OrsoTypes
    nullable: bool = True
    identity: Optional[bytes] = None
    default: Optional[Any] = None
    description: Optional[str] = None
    disposition: Optional[str] = None
    aliases: Optional[List[str]] = dataclasses.field(default_factory=lambda: None)
    element_type: Optional[OrsoTypes] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    length: Optional[int] = None
    highest_value: Optional[Any] = None
    lowest_value: Optional[Any] = None
    null_count: Optional[int] = None
    fields: Optional[List[FlatColumn]] = None
    expectations: Optional[Any] = None  # Deferred to Phase 9
    origin: Optional[List[str]] = None  # Deferred to Phase 9

    def __post_init__(self):
        """Auto-generate identity from name if not provided; ensure identity is bytes."""
        if self.identity is None:
            raw = self.name
        else:
            raw = self.identity
        if isinstance(raw, str):
            self.identity = raw.encode("utf-8")
        else:
            self.identity = raw

    def __str__(self) -> str:
        """String representation: name:type."""
        return f"{self.name}:{self.type.value}"

    def to_flatcolumn(self) -> "FlatColumn":
        """Convert to a FlatColumn (returns self for FlatColumn)."""
        if isinstance(self, FlatColumn):
            return self
        # For subclasses, create a new FlatColumn with the same properties
        return FlatColumn(
            name=self.name,
            type=self.type,
            identity=self.identity,
            nullable=self.nullable,
            default=self.default,
            description=self.description,
            disposition=self.disposition,
            aliases=self.aliases,
            element_type=self.element_type,
            precision=self.precision,
            scale=self.scale,
            length=self.length,
            origin=self.origin,
        )

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"FlatColumn(name={self.name!r}, type={self.type.value}, nullable={self.nullable})"

    @property
    def all_names(self) -> List[str]:
        """Get all names for this column (name + aliases)."""
        names = [self.name]
        if self.aliases:
            names.extend(self.aliases)
        return names

    @property
    def column_type(self):
        """Unified ColumnType for this column (TEMPORARY migration bridge).

        A pure PROJECTION of the legacy `type` (+ side-car `precision`/`scale`/
        `element_type`) into a `ColumnType` — derived, not a second source of truth, so
        it cannot drift from `.type`. It does NOT fabricate: if the legacy data lacks
        the information (e.g. a DECIMAL with no precision), it FAILS rather than guess.

        EXIT PLAN (see plan "Exit Plan for Bridges & Shims"): readers migrate from
        `.type` onto `.column_type`; in Phase 6 `FlatColumn.type` becomes a `ColumnType`
        directly and this property is removed (or aliased to `.type`) along with the
        `orso_to_column_type` bridge and OrsoTypes itself.
        """
        from opteryx.types._orso_types import orso_to_column_type

        return orso_to_column_type(
            self.type,
            precision=self.precision,
            scale=self.scale,
            element_type=self.element_type,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert column to dictionary for serialization."""
        return {
            "name": self.name,
            "type": self.type.value,
            "identity": self.identity.decode("utf-8") if isinstance(self.identity, bytes) else self.identity,
            "nullable": self.nullable,
            "default": self.default,
            "description": self.description,
            "disposition": self.disposition,
            "aliases": self.aliases,
            "element_type": self.element_type.value if self.element_type else None,
            "precision": self.precision,
            "scale": self.scale,
            "length": self.length,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> FlatColumn:
        """Create FlatColumn from dictionary."""
        data = data.copy()
        # Convert type string to OrsoTypes if needed
        if isinstance(data.get("type"), str):
            type_obj, _, _, _, element_type = OrsoTypes.from_name(data["type"])
            data["type"] = type_obj
            if element_type and "element_type" not in data:
                data["element_type"] = element_type
        # Convert element_type string to OrsoTypes if needed
        if isinstance(data.get("element_type"), str):
            element_obj, _, _, _, _ = OrsoTypes.from_name(data["element_type"])
            data["element_type"] = element_obj
        return cls(**data)


@dataclasses.dataclass
class ConstantColumn(FlatColumn):
    """Column with a constant value.

    Used for constant expressions (e.g., SELECT 42 AS constant_col).
    Inherits from FlatColumn with additional constant value semantics.
    """

    value: Any = None

    def __str__(self) -> str:
        """String representation: name = value."""
        return f"{self.name}={self.value}"

    def to_flatcolumn(self) -> FlatColumn:
        """Convert to a FlatColumn, stripping constant value."""
        return FlatColumn(
            name=self.name,
            type=self.type,
            identity=self.identity,
            nullable=self.nullable,
            default=self.default,
            description=self.description,
            disposition=self.disposition,
            aliases=self.aliases,
            element_type=self.element_type,
            precision=self.precision,
            scale=self.scale,
            length=self.length,
        )


@dataclasses.dataclass
class FunctionColumn(FlatColumn):
    """Column defined by a function/expression.

    Used for computed columns (e.g., SELECT col1 + col2 AS sum_col).
    Inherits from FlatColumn with additional function expression semantics.
    """

    def __str__(self) -> str:
        """String representation: name (computed)."""
        return f"{self.name}(computed)"

    def to_flatcolumn(self) -> FlatColumn:
        """Convert to a FlatColumn, stripping function metadata."""
        return FlatColumn(
            name=self.name,
            type=self.type,
            identity=self.identity,
            nullable=self.nullable,
            default=self.default,
            description=self.description,
            disposition=self.disposition,
            aliases=self.aliases,
            element_type=self.element_type,
            precision=self.precision,
            scale=self.scale,
            length=self.length,
        )


@dataclasses.dataclass
class RelationSchema:
    """Table/relation schema definition.

    Replaces orso.schema.RelationSchema with Opteryx specialization.

    Attributes:
        name: Schema/table name (required)
        columns: List of FlatColumn definitions (required)
        aliases: Alternative names for this schema (default: [])
        primary_key: Name of primary key column (default: None)
        row_count_metric: Actual row count if known (default: None)
        row_count_estimate: Estimated row count (default: None)
        data_size_metric: Actual data size in bytes (default: None)
        data_size_estimate: Estimated data size in bytes (default: None)
    """

    name: str
    columns: List[FlatColumn] = dataclasses.field(default_factory=list)
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

    def column(self, name: str, case_insensitive: bool = False) -> Optional[FlatColumn]:
        """Find column by name (including aliases).

        Args:
            name: Column name to search for
            case_insensitive: If True, perform case-insensitive comparison

        Returns:
            FlatColumn if found, None otherwise
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

    def find_column(self, name: str, case_insensitive: bool = False) -> Optional[FlatColumn]:
        """Alias for column() for orso compatibility."""
        return self.column(name, case_insensitive=case_insensitive)

    def pop_column(self, name: str) -> Optional[FlatColumn]:
        """Remove and return column by name.

        Args:
            name: Column name to remove

        Returns:
            Removed FlatColumn if found, None otherwise
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
        # Convert column dicts to FlatColumn objects
        if "columns" in data:
            data["columns"] = [
                FlatColumn.from_dict(col) if isinstance(col, dict) else col
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
