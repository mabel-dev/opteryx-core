"""
Internal Opteryx DataFrame class - minimal replacement for sql.DataFrame.

This provides the interface that Session depends on without requiring the sql package.
Only implements the methods actually used by Opteryx (not a full DataFrame replacement).
"""

from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from opteryx.types.logical_type import LogicalCategory
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema

__all__ = ["DataFrame"]


class DataFrame:
    """Minimal DataFrame implementation for Opteryx Session compatibility.

    Replaces sql.DataFrame with a lightweight in-memory table representation.
    Only implements methods actually used by the query execution pipeline.

    This class is:
    - A base class for Session
    - Used for metadata result tables (e.g., rows_affected counts)
    """

    def __init__(
        self,
        dictionaries: Optional[Iterable[Dict[str, Any]]] = None,
        *,
        rows: Optional[List[tuple]] = None,
        schema: Optional[Union[RelationSchema, List[str]]] = None,
    ):
        """Initialize a DataFrame.

        Args:
            dictionaries: Optional list of dicts (not used; for API compatibility)
            rows: List of tuples representing rows
            schema: RelationSchema or list of column names
        """
        # Allow direct attribute access (needed by Session)
        self._rows = rows or []
        self._description: Optional[Tuple[Tuple[Any, ...], ...]] = None
        self._schema: Optional[RelationSchema] = None

        # Convert schema to RelationSchema if needed
        if schema is None:
            self._schema = RelationSchema(name="table", columns=[])
        elif isinstance(schema, RelationSchema):
            self._schema = schema
        elif isinstance(schema, (list, tuple)):
            # Convert list of column names to RelationSchema
            from opteryx.types.schema import mint_column_identity
            columns = [SchemaColumn(name=str(col), column_type=_lt.VARCHAR, identity=mint_column_identity("table", str(col))) for col in schema]
            self._schema = RelationSchema(name="table", columns=columns)
        else:
            self._schema = schema

    @property
    def description(self) -> Optional[Tuple[Tuple[Any, ...], ...]]:
        """Get cursor description (column metadata).

        Returns list of tuples: (name, type_code, display_size, internal_size, precision, scale, null_ok)
        Matches DB-API specification for compatibility.
        """
        # If _description was explicitly set, return it
        if self._description is not None:
            return self._description

        # Otherwise, compute from schema
        if not self._schema:
            return None

        description = []
        for col in self._schema.columns:
            # Simplified DB-API description tuple
            description.append(
                (
                    col.name,  # name
                    None,  # type_code (sql doesn't use this)
                    None,  # display_size
                    None,  # internal_size
                    None,  # precision
                    None,  # scale
                    col.nullable,  # null_ok
                )
            )
        return tuple(description)

    @property
    def column_names(self) -> List[str]:
        """Get list of column names."""
        if not self._schema:
            return []
        return self._schema.column_names

    @property
    def shape(self) -> Tuple[int, int]:
        """Return shape as (rows, columns)."""
        num_rows = len(self._rows)
        num_cols = len(self._schema.columns) if self._schema else 0
        return (num_rows, num_cols)

    def __len__(self) -> int:
        """Return number of rows."""
        return len(self._rows)

    def __repr__(self) -> str:
        """String representation."""
        num_cols = len(self._schema.columns) if self._schema else 0
        return f"DataFrame(rows={len(self._rows)}, columns={num_cols})"
