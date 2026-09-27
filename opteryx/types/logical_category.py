# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""`LogicalCategory` - the dispatch projection of a ColumnType.

Its own module because it is a plain Python enum (a member is named NULL, which
a Cython module cannot define) that both the native ColumnType
(opteryx/compiled/planner/column_type.pyx) and opteryx.types.logical_type import;
it imports nothing from opteryx.
"""

from enum import Enum


class LogicalCategory(Enum):
    """The Opteryx SQL type vocabulary AND the operator-dispatch key (Decision B).

    Pure projection enum — reachable only via `ColumnType.category`. 15 canonical
    members; no aliases, no behaviours. Integer/float widths collapse to INTEGER/FLOAT
    (the actual physical width lives on `ColumnType.physical`).

    Unknown/unresolved types are represented by Python `None`, not by a sentinel
    enum member. Check `x is None` rather than comparing against a sentinel.
    """

    NULL = "NULL"
    BOOLEAN = "BOOLEAN"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    INTERVAL = "INTERVAL"
    VARCHAR = "VARCHAR"
    NVARCHAR = "NVARCHAR"
    VARBINARY = "VARBINARY"
    VARIANT = "VARIANT"
    ARRAY = "ARRAY"
    VECTOR = "VECTOR"
