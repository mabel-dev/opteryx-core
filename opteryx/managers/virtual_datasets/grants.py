# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the grants held by the current session, projected into rows — asked of
the same permissions capability that decides queries (see
`opteryx.managers.permissions`), so what is reported and what is enforced
cannot drift into disagreeing. With no capability registered the engine allows
everything, and this dataset says exactly that.

This dataset REPORTS grants; it never confers them. Grant administration is
the separate `GRANT`/`REVOKE`/`SHOW GRANTS ON` surface, applied through the
same capability. Policies are issued by the platform's policy service and
handed to the session at construction, so within a session the engine can
only ever narrow access, never widen it. `SHOW GRANTS` exists so a caller can
answer "why can't I see this table?" without leaving SQL.
"""

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.exceptions import VariableNotFoundError
from opteryx.managers.permissions import active_permissions_capability
from opteryx.types import logical_type as _lt
from opteryx.types.schema import RelationSchema, SchemaColumn

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

    policies = _get_variable(variables, "access_policies", [])
    if callable(getattr(policies, "to_pylist", None)):
        policies = policies.to_pylist()

    username = _get_variable(variables, "external_user", "")
    rows = active_permissions_capability().grants(username, list(policies or []))

    patterns = []
    levels = []
    roles = []
    actions = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        patterns.append(str(row.get("pattern", "")))
        # The object level the pattern addresses (workspace/collection/dataset),
        # spoken the way the GRANT surface speaks. Empty for a pattern that
        # addresses no single object — never guessed.
        levels.append(str(row.get("level", "")))
        roles.append(str(row.get("role", "")))
        actions.append(str(row.get("actions", "")))

    vectors = [
        vector_from_sequence(patterns, dtype=DrakenType.VARCHAR),
        vector_from_sequence(levels, dtype=DrakenType.VARCHAR),
        vector_from_sequence(roles, dtype=DrakenType.VARCHAR),
        vector_from_sequence(actions, dtype=DrakenType.VARCHAR),
    ]

    return Morsel.from_vectors(["pattern", "level", "role", "actions"], vectors)


def schema():
    # fmt:off
    from opteryx.types.schema import mint_column_identity
    def sc(name):
        return SchemaColumn(name=name, column_type=_lt.VARCHAR, identity=mint_column_identity("$grants", name))
    return RelationSchema(
        name="$grants",
        columns=[
            sc("pattern"),
            sc("level"),
            sc("role"),
            sc("actions"),
        ],
        # ESTIMATE, not a metric: one row per policy the session holds, which
        # varies per caller. Always small — the point is that it must not fall
        # back to _UNKNOWN_ROW_COUNT (1,000,000).
        row_count_estimate=8,
    )
    # fmt:on
