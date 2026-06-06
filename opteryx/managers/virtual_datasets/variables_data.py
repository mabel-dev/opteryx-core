# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the system variables collection.
"""

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.types.logical_type import LogicalCategory
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema

__all__ = ("read", "schema")


def read(at_date=None, variables=None):
    if not variables:
        # Empty result with expected schema
        vectors = [
            vector_from_sequence([], dtype=LogicalCategory.VARCHAR),
            vector_from_sequence([], dtype=LogicalCategory.VARCHAR),
            vector_from_sequence([], dtype=LogicalCategory.VARCHAR),
            vector_from_sequence([], dtype=LogicalCategory.VARCHAR),
            vector_from_sequence([], dtype=LogicalCategory.VARCHAR),
        ]
        return Morsel.from_vectors(["name", "value", "type", "owner", "visibility"], vectors)

    variables = variables or {}

    names = []
    values = []
    types = []
    owners = []
    visibilities = []

    for variable in variables:
        variable_type, variable_value, variable_owner, variable_visibility = variables.details(
            variable
        )
        names.append(variable)
        values.append(str(variable_value))
        types.append(variable_type.value)
        owners.append(variable_owner.name)
        visibilities.append(variable_visibility.name)

    vectors = [
        vector_from_sequence(names, dtype=LogicalCategory.VARCHAR),
        vector_from_sequence(values, dtype=LogicalCategory.VARCHAR),
        vector_from_sequence(types, dtype=LogicalCategory.VARCHAR),
        vector_from_sequence(owners, dtype=LogicalCategory.VARCHAR),
        vector_from_sequence(visibilities, dtype=LogicalCategory.VARCHAR),
    ]
    return Morsel.from_vectors(["name", "value", "type", "owner", "visibility"], vectors)


def schema():
    # fmt:off
    return  RelationSchema(
        name="$variables",
        columns=[
            SchemaColumn.from_column_type(name="name", column_type=_lt.VARCHAR),
            SchemaColumn.from_column_type(name="value", column_type=_lt.VARCHAR),
            SchemaColumn.from_column_type(name="type", column_type=_lt.VARCHAR),
            SchemaColumn.from_column_type(name="owner", column_type=_lt.VARCHAR),
            SchemaColumn.from_column_type(name="visibility", column_type=_lt.VARCHAR),
        ],
    )
    # fmt:on
