# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
This is a virtual dataset which is calculated at access time.

It is the access policies held by the current session — the exact list
`can_perform_action` matches against, projected into rows.

This dataset REPORTS grants; it never confers them. Opteryx has no GRANT or
REVOKE: policies are issued by the platform's policy service and handed to the
session at construction, so the engine can only ever narrow access, never widen
it. `SHOW GRANTS` exists so a caller can answer "why can't I see this table?"
without leaving SQL.
"""

from draken.draken_native import DrakenType
from draken.interop.vector_sequence import vector_from_sequence
from draken.morsels.morsel import Morsel
from opteryx.exceptions import VariableNotFoundError
from opteryx.managers.permissions import ACTION_MAP
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


def _actions_for(role: str) -> str:
    """The actions a role can perform, derived from ACTION_MAP rather than
    restated here — a second list would drift from the one that is enforced."""
    return ", ".join(sorted(action for action, roles in ACTION_MAP.items() if role in roles))


def read(at_date=None, variables=None):
    variables = variables or {}

    policies = _get_variable(variables, "access_policies", [])
    if callable(getattr(policies, "to_pylist", None)):
        policies = policies.to_pylist()

    patterns = []
    roles = []
    actions = []

    for policy in policies or []:
        if not isinstance(policy, dict):
            continue
        # Defaults match can_perform_action's own reads, so this table cannot
        # show a grant that differs from the one actually enforced.
        role = policy.get("role", "reader")
        patterns.append(str(policy.get("pattern", "")))
        roles.append(str(role))
        actions.append(_actions_for(role))

    vectors = [
        vector_from_sequence(patterns, dtype=DrakenType.VARCHAR),
        vector_from_sequence(roles, dtype=DrakenType.VARCHAR),
        vector_from_sequence(actions, dtype=DrakenType.VARCHAR),
    ]

    return Morsel.from_vectors(["pattern", "role", "actions"], vectors)


def schema():
    # fmt:off
    from opteryx.types.schema import mint_column_identity
    def sc(name):
        return SchemaColumn(name=name, column_type=_lt.VARCHAR, identity=mint_column_identity("$grants", name))
    return RelationSchema(
        name="$grants",
        columns=[
            sc("pattern"),
            sc("role"),
            sc("actions"),
        ],
        # ESTIMATE, not a metric: one row per policy the session holds, which
        # varies per caller. Always small — the point is that it must not fall
        # back to _UNKNOWN_ROW_COUNT (1,000,000).
        row_count_estimate=8,
    )
    # fmt:on
