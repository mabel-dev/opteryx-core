# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the user attributes collection.
"""

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.exceptions import VariableNotFoundError
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema

__all__ = ("read", "schema")


def _get_variable(variables, key, default):
    if isinstance(variables, dict):
        return variables.get(key, default)
    try:
        return variables[key]
    except (KeyError, VariableNotFoundError, TypeError):
        return default


def read(at_date=None, variables=None):
    variables = variables or {}

    username = _get_variable(variables, "external_user", "")
    memberships = _get_variable(variables, "user_memberships", [])

    if callable(getattr(memberships, "to_pylist", None)):
        memberships = memberships.to_pylist()

    attributes = []
    values = []

    if username:
        attributes.append("username")
        values.append(username)

    for m in memberships:
        attributes.append("membership")
        values.append(str(m))

    vectors = [
        vector_from_sequence(attributes, dtype=DrakenType.VARCHAR),
        vector_from_sequence(values, dtype=DrakenType.VARCHAR),
        vector_from_sequence(["VARCHAR"] * len(attributes), dtype=DrakenType.VARCHAR),
    ]

    return Morsel.from_vectors(["attribute", "value", "type"], vectors)


def schema():
    # fmt:off
    from opteryx.types.schema import mint_column_identity
    def sc(name):
        return SchemaColumn(name=name, column_type=_lt.VARCHAR, identity=mint_column_identity("$user", name))
    return  RelationSchema(
        name="$user",
        columns=[
            sc("attribute"),
            sc("value"),
            sc("type"),
        ],
    )
    # fmt:on
