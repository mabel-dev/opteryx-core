# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the system variables collection.
"""

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema
from opteryx.variables import PLATFORM_ADMIN_ENTITLEMENT, SYSTEM_VARIABLES_DEFAULTS, Visibility

__all__ = ("read", "schema")


def _entitlements_of(variables) -> frozenset:
    """The caller's entitlements, as a set of names.

    Returns EMPTY on anything unexpected — an unreadable entitlement list must
    never be treated as holding an entitlement.
    """
    # Both a plain dict and SystemVariablesContainer support `in`, so membership is
    # tested directly rather than catching the container's VariableNotFoundError —
    # absence is an expected state here, not an exception to swallow.
    if "user_entitlements" not in variables:
        return frozenset()
    held = variables["user_entitlements"]
    if callable(getattr(held, "to_pylist", None)):
        held = held.to_pylist()
    if not isinstance(held, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(str(item) for item in held)


def read(at_date=None, variables=None):
    if not variables:
        # Empty result with expected schema
        vectors = [
            vector_from_sequence([], dtype=DrakenType.VARCHAR),
            vector_from_sequence([], dtype=DrakenType.VARCHAR),
            vector_from_sequence([], dtype=DrakenType.VARCHAR),
            vector_from_sequence([], dtype=DrakenType.VARCHAR),
            vector_from_sequence([], dtype=DrakenType.VARCHAR),
        ]
        return Morsel.from_vectors(["name", "value", "type", "owner", "visibility"], vectors)

    variables = variables or {}

    # RESTRICTED variables are listed only for platform administrators. The
    # entitlement is SERVER-owned (asserted by the submitting service), so a caller
    # cannot reveal these to themselves with a `SET`. Absence of the entitlement is
    # read as "not an admin" — the failure mode is hiding too much, never too little.
    show_restricted = PLATFORM_ADMIN_ENTITLEMENT in _entitlements_of(variables)

    names = []
    values = []
    types = []
    owners = []
    visibilities = []

    for variable in variables:
        variable_type, variable_value, variable_owner, variable_visibility = variables.details(
            variable
        )
        if variable_visibility == Visibility.RESTRICTED and not show_restricted:
            continue
        names.append(variable)
        values.append(str(variable_value))
        types.append(variable_type.category.value)
        owners.append(variable_owner.name)
        visibilities.append(variable_visibility.name)

    vectors = [
        vector_from_sequence(names, dtype=DrakenType.VARCHAR),
        vector_from_sequence(values, dtype=DrakenType.VARCHAR),
        vector_from_sequence(types, dtype=DrakenType.VARCHAR),
        vector_from_sequence(owners, dtype=DrakenType.VARCHAR),
        vector_from_sequence(visibilities, dtype=DrakenType.VARCHAR),
    ]
    return Morsel.from_vectors(["name", "value", "type", "owner", "visibility"], vectors)


def schema():
    # fmt:off
    from opteryx.types.schema import mint_column_identity
    def sc(name):
        return SchemaColumn(name=name, column_type=_lt.VARCHAR, identity=mint_column_identity("$variables", name))
    return  RelationSchema(
        name="$variables",
        columns=[
            sc("name"),
            sc("value"),
            sc("type"),
            sc("owner"),
            sc("visibility"),
        ],
        # ESTIMATE, not a metric: RESTRICTED variables are withheld from callers
        # without `platform_admin`, so the count depends on the caller. Sized to the
        # registered table; exact only for an admin. Declared so this never falls back
        # to _UNKNOWN_ROW_COUNT (1,000,000).
        row_count_estimate=len(SYSTEM_VARIABLES_DEFAULTS),
    )
    # fmt:on
