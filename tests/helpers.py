"""Test helpers for the new Session + execute_to_morsels API.

These helpers replace deprecated API calls:
- opteryx.query_to_arrow() → execute_and_get_arrow()
- session.execute_to_arrow() → execute_and_get_arrow()
- cursor.rowcount → execute_and_get_rowcount()
- cursor.shape → execute_and_get_shape()
- cursor.fetchall() → execute_and_fetch_all()
"""

from typing import List, Dict, Any, Tuple, Optional, TYPE_CHECKING
import pyarrow as pa
import opteryx
from opteryx import session

if TYPE_CHECKING:
    from draken import Morsel


def execute_and_fetch_all(sql: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
    """Execute SQL and return all rows as list of dicts.

    Replaces: opteryx.query_to_arrow(sql).to_pylist()

    Args:
        sql: SQL query string
        params: Optional query parameters

    Returns:
        List of dicts, one per row

    Example:
        data = execute_and_fetch_all("SELECT * FROM $planets")
        assert len(data) == 9
    """
    session = opteryx.session()
    morsels = list(session.execute_to_morsels(sql, params=params))
    table = _morsels_to_arrow(morsels)
    return table.to_pylist()


def execute_and_get_rowcount(sql: str, params: Optional[List] = None) -> int:
    """Execute SQL and return row count.

    Replaces: opteryx.query(sql).rowcount

    Args:
        sql: SQL query string
        params: Optional query parameters

    Returns:
        Total number of rows

    Example:
        count = execute_and_get_rowcount("SELECT * FROM $planets")
        assert count == 9
    """
    session = opteryx.session()
    morsels = list(session.execute_to_morsels(sql, params=params))
    return sum(m.num_rows for m in morsels)


def execute_and_get_shape(sql: str, params: Optional[List] = None) -> Tuple[int, int]:
    """Execute SQL and return (rows, cols).

    Replaces: opteryx.query(sql).shape

    Args:
        sql: SQL query string
        params: Optional query parameters

    Returns:
        Tuple of (num_rows, num_columns)

    Example:
        shape = execute_and_get_shape("SELECT * FROM $planets")
        assert shape == (9, 20)
    """
    session = opteryx.session()
    morsels = list(session.execute_to_morsels(sql, params=params))

    if not morsels:
        return (0, 0)

    total_rows = sum(m.num_rows for m in morsels)
    num_cols = len(morsels[0].column_names)
    return (total_rows, num_cols)


def execute_and_get_arrow(sql: str, params: Optional[List] = None) -> pa.Table:
    """Execute SQL and return PyArrow Table.

    Replaces: opteryx.query_to_arrow(sql) or session.execute_to_arrow(sql)

    Args:
        sql: SQL query string
        params: Optional query parameters

    Returns:
        PyArrow Table with all results

    Example:
        table = execute_and_get_arrow("SELECT * FROM $planets")
        assert table.shape == (9, 20)
        data = table.to_pydict()
    """
    session = opteryx.session()
    morsels = list(session.execute_to_morsels(sql, params=params))
    return _morsels_to_arrow(morsels)


def execute_and_get_morsels(sql: str, params: Optional[List] = None):
    """Execute SQL and return list of Morsel objects.

    For tests needing direct access to morsels for fine-grained control.

    Args:
        sql: SQL query string
        params: Optional query parameters

    Returns:
        List of Morsel objects

    Example:
        morsels = execute_and_get_morsels("SELECT * FROM table")
        for morsel in morsels:
            print(f"Batch: {morsel.num_rows} rows")
    """
    session = opteryx.session()
    return list(session.execute_to_morsels(sql, params=params))


def execute_with_visibility_filters(sql: str, visibility_filters: Optional[Dict[str, Any]] = None) -> Tuple[int, int]:
    """Execute SQL with visibility filters and return (rows, cols).

    For tests that need row-level visibility filtering.

    Args:
        sql: SQL query string
        visibility_filters: Row visibility filter dict

    Returns:
        Tuple of (num_rows, num_columns)

    Example:
        shape = execute_with_visibility_filters("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4)]})
        assert shape == (1, 20)
    """
    session = opteryx.session()
    morsels = list(session.execute_to_morsels(sql, visibility_filters=visibility_filters))

    if not morsels:
        return (0, 0)

    total_rows = sum(m.num_rows for m in morsels)
    num_cols = len(morsels[0].column_names)
    return (total_rows, num_cols)


def execute_with_memberships(sql: str, memberships: Optional[List[str]] = None, params: Optional[List] = None):
    """Execute SQL with memberships and return morsels.

    For tests that need group-based access control.

    Args:
        sql: SQL query string
        memberships: List of group memberships
        params: Optional query parameters

    Returns:
        List of Morsel objects

    Example:
        morsels = execute_with_memberships("SELECT * FROM $planets", memberships=["group1"])
    """
    session = opteryx.session(memberships=memberships)
    return list(session.execute_to_morsels(sql, params=params))


def _morsels_to_arrow(morsels: List) -> pa.Table:
    """Convert list of Morsels to PyArrow Table.

    Internal helper - not part of public test API.

    Args:
        morsels: List of Morsel objects from execute_to_morsels()

    Returns:
        Single PyArrow Table combining all morsels
    """
    if not morsels:
        return pa.table({})

    # Convert each morsel to Arrow
    arrow_tables = [morsel.to_arrow() for morsel in morsels]

    # Concatenate if multiple
    if len(arrow_tables) == 1:
        return arrow_tables[0]

    return pa.concat_tables(arrow_tables)
