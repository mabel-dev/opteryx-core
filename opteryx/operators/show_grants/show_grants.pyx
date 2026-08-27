# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Show Grants Node

This is a SQL Query Execution Plan Node.

Answers `SHOW GRANTS ON <kind> <object>` — the stored policies on an object,
one row per policy, `(user, pattern, level, role)`: the console's access-list
screen, as SQL. The rows come from the registered permissions capability's
`grants_on`, which also holds the gate (owner authority covering the object —
the binder pre-checked the same question at bind time, but the capability's
answer at execution is the authoritative one). Distinct from bare
`SHOW GRANTS` ($grants), which reports the SESSION'S OWN grants.
"""

from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.


class ShowGrantsNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.pattern = parameters.get("pattern")
        self.object_kind = parameters.get("object_kind")
        self.object_name = parameters.get("object_name")
        # Stashed by the binder (visit_show_grants_on): the capability needs
        # the acting identity, and there is no BindingContext here.
        self.execution_context = parameters.get("execution_context")
        self.seen = False

    @property
    def name(self):  # pragma: no cover
        return "Show Grants"

    @property
    def config(self):  # pragma: no cover
        return f"on {self.object_kind} {self.object_name}"

    def execute(self, morsel):
        if self.seen:
            yield None
            return

        from draken.draken_native import DrakenType
        from draken.interop.vector_sequence import vector_from_sequence
        from draken.morsels.morsel import Morsel
        from opteryx.managers.permissions import grants_on

        self.seen = True

        rows = grants_on(self.execution_context, self.pattern)

        users = []
        patterns = []
        levels = []
        roles = []
        for row in rows:
            users.append(str(row.get("user", "")))
            patterns.append(str(row.get("pattern", "")))
            levels.append(str(row.get("level", "")))
            roles.append(str(row.get("role", "")))

        vectors = [
            vector_from_sequence(users, dtype=DrakenType.VARCHAR),
            vector_from_sequence(patterns, dtype=DrakenType.VARCHAR),
            vector_from_sequence(levels, dtype=DrakenType.VARCHAR),
            vector_from_sequence(roles, dtype=DrakenType.VARCHAR),
        ]

        yield Morsel.from_vectors(["user", "pattern", "level", "role"], vectors)
