# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the user attributes collection.
"""

from opteryx.compiled.draken.interop.vector_sequence import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.exceptions import VariableNotFoundError
from opteryx.types import OrsoTypes
from opteryx.types.schema import FlatColumn, RelationSchema

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

    # Build Draken vectors directly (no Arrow intermediary)
    memberships_list = list(memberships)
    vectors = [
        vector_from_sequence(["membership"] * len(memberships_list), dtype=OrsoTypes.VARCHAR),
        vector_from_sequence([str(value) for value in memberships_list], dtype=OrsoTypes.VARCHAR),
        vector_from_sequence(["VARCHAR"] * len(memberships_list), dtype=OrsoTypes.VARCHAR),
    ]

    return Morsel.from_vectors(["attribute", "value", "type"], vectors)


def schema():
    # fmt:off
    return  RelationSchema(
        name="$user",
        columns=[
            FlatColumn(name="attribute", type=OrsoTypes.VARCHAR),
            FlatColumn(name="value", type=OrsoTypes.VARCHAR),
            FlatColumn(name="type", type=OrsoTypes.VARCHAR)
        ],
    )
    # fmt:on
