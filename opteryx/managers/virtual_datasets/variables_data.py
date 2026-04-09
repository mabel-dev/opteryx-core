# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the system variables collection.
"""

from orso.schema import FlatColumn
from orso.schema import RelationSchema
from orso.types import OrsoTypes

from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel

__all__ = ("read", "schema")


def read(at_date=None, variables=None):
    if not variables:
        # Empty result with expected schema
        vectors = [
            vector_from_sequence([], dtype=OrsoTypes.VARCHAR),
            vector_from_sequence([], dtype=OrsoTypes.VARCHAR),
            vector_from_sequence([], dtype=OrsoTypes.VARCHAR),
            vector_from_sequence([], dtype=OrsoTypes.VARCHAR),
            vector_from_sequence([], dtype=OrsoTypes.VARCHAR),
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
        types.append(variable_type)
        owners.append(variable_owner.name)
        visibilities.append(variable_visibility.name)

    vectors = [
        vector_from_sequence(names, dtype=OrsoTypes.VARCHAR),
        vector_from_sequence(values, dtype=OrsoTypes.VARCHAR),
        vector_from_sequence(types, dtype=OrsoTypes.VARCHAR),
        vector_from_sequence(owners, dtype=OrsoTypes.VARCHAR),
        vector_from_sequence(visibilities, dtype=OrsoTypes.VARCHAR),
    ]
    return Morsel.from_vectors(["name", "value", "type", "owner", "visibility"], vectors)


def schema():
    # fmt:off
    return  RelationSchema(
        name="$variables",
        columns=[
            FlatColumn(name="name", type=OrsoTypes.VARCHAR),
            FlatColumn(name="value", type=OrsoTypes.VARCHAR),
            FlatColumn(name="type", type=OrsoTypes.VARCHAR),
            FlatColumn(name="owner", type=OrsoTypes.VARCHAR),
            FlatColumn(name="visibility", type=OrsoTypes.VARCHAR),
        ],
    )
    # fmt:on
