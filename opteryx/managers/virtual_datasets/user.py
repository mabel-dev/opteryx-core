# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the user attributes collection.
"""

from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.exceptions import VariableNotFoundError
from opteryx.types.logical_type import LogicalCategory
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema

__all__ = ("read", "schema")


def read(at_date=None, variables=None):
    variables = variables or {}

    if isinstance(variables, dict):
        memberships = variables.get("user_memberships", [])
    elif hasattr(variables, "__getitem__"):
        try:
            memberships = variables["user_memberships"]
        except (KeyError, VariableNotFoundError, TypeError):
            memberships = []
    else:
        memberships = []

    if hasattr(memberships, "to_pylist"):
        memberships = memberships.to_pylist()

    # Build Draken vectors directly
    memberships_list = list(memberships)
    vectors = [
        vector_from_sequence(["membership"] * len(memberships_list), dtype=LogicalCategory.VARCHAR),
        vector_from_sequence([str(value) for value in memberships_list], dtype=LogicalCategory.VARCHAR),
        vector_from_sequence(["VARCHAR"] * len(memberships_list), dtype=LogicalCategory.VARCHAR),
    ]

    return Morsel.from_vectors(["attribute", "value", "type"], vectors)


def schema():
    # fmt:off
    return  RelationSchema(
        name="$user",
        columns=[
            SchemaColumn.from_column_type(name="attribute", column_type=_lt.VARCHAR),
            SchemaColumn.from_column_type(name="value", column_type=_lt.VARCHAR),
            SchemaColumn.from_column_type(name="type", column_type=_lt.VARCHAR)
        ],
    )
    # fmt:on
