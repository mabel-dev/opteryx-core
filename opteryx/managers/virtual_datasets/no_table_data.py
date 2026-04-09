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

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.tools import single_item_cache
from orso.types import OrsoTypes

from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel

__all__ = ("read", "schema")


@single_item_cache
def read(at_date=None, variables=None) -> Morsel:
    # Create a Morsel containing one column and one row.
    _ = variables

    vectors = [vector_from_sequence([0], dtype=OrsoTypes.INTEGER)]
    return Morsel.from_vectors(["$column"], vectors)


def schema():
    # fmt:off
    return RelationSchema(name="$no_table", columns=[FlatColumn(name="$column", type=OrsoTypes.INTEGER)])
    # fmt:on
