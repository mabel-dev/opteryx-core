# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
one row
--------

The stand-in source for a statement with no FROM clause ("SELECT 1"), and for
an answer served from statistics rather than a read.

It actually is a table, with one row and one column. Named for what it emits
rather than for what the statement lacks — the same reasoning behind Spark's
OneRowRelation and PostgreSQL's Result node. `$no_table` is still accepted in
SQL as an alias (see virtual_data_connector.DATASET_ALIASES).
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
    from opteryx.types.schema import mint_column_identity
    # EXACT: this relation exists to give `SELECT 1` a single row to project from.
    return RelationSchema(name="$one_row", columns=[SchemaColumn(name="$column", column_type=_lt.INT64, identity=mint_column_identity("$one_row", "$column"))], row_count_metric=1)
    # fmt:on
