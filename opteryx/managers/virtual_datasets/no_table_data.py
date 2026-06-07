# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
no table
---------

This is used to prepresent no table.

It actually is a table, with one row and one column.
"""

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema

__all__ = ("read", "schema")


def read(at_date=None, variables=None) -> Morsel:
    # Create a Morsel containing one column and one row.
    _ = variables

    vectors = [vector_from_sequence([0], dtype=DrakenType.INT64)]
    return Morsel.from_vectors(["$column"], vectors)


def schema():
    # fmt:off
    return RelationSchema(name="$no_table", columns=[SchemaColumn(name="$column", column_type=_lt.INT64)])
    # fmt:on
