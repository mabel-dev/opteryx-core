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

Answers the two grant listings on an object, both `(user, pattern, level,
role)`, one row per policy:

  SHOW GRANTS ON <kind> <object>            — the policies stored AT the
      object, 1:1 with what a GRANT or REVOKE there would act on: the
      console's access-list screen, as SQL.
  SHOW EFFECTIVE GRANTS ON <kind> <object>  — every policy that COVERS the
      object, the collection and workspace grants above it included, so a
      dataset reachable only through the workspace owner's `w.*` names that
      owner instead of returning nothing.

One operator, because they differ only in which question is asked of the
registered permissions capability (`grants_on` / `effective_grants_on`) — the
columns, the ordering and the gate are one thing, and the console renders both
with one renderer. The capability also holds that gate (owner authority
covering the object — the binder pre-checked the same question at bind time,
but the capability's answer at execution is the authoritative one), and owns
the covering test, which is the matcher that decides real queries.

Distinct from bare `SHOW GRANTS` ($grants), which reports the SESSION'S OWN
grants.
"""

from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.


class ShowGrantsNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.pattern = parameters.get("pattern")
        self.object_kind = parameters.get("object_kind")
        self.object_name = parameters.get("object_name")
        # Which of the two listings this is; set by the logical planner.
        self.effective = bool(parameters.get("effective"))
        # Stashed by the binder (visit_show_grants_on): the capability needs
        # the acting identity, and there is no BindingContext here.
        self.execution_context = parameters.get("execution_context")
        self.seen = False

    @property
    def name(self):  # pragma: no cover
        # One name for both listings: it is the registered operator name, and
        # the key its timing is recorded under. Which listing this is belongs
        # in `config`.
        return "Show Grants"

    @property
    def config(self):  # pragma: no cover
        listing = "effective grants on" if self.effective else "on"
        return f"{listing} {self.object_kind} {self.object_name}"

    def execute(self, morsel):
        if self.seen:
            yield None
            return

        from draken.draken_native import DrakenType
        from draken.interop.vector_sequence import vector_from_sequence
        from draken.morsels.morsel import Morsel
        from opteryx.managers.permissions import effective_grants_on
        from opteryx.managers.permissions import grants_on

        self.seen = True

        if self.effective:
            rows = effective_grants_on(self.execution_context, self.pattern)
        else:
            rows = grants_on(self.execution_context, self.pattern)
            if not rows and self.object_kind != "workspace":
                # An empty attached listing is indistinguishable from "nobody
                # can reach this", and reading it that way is what prompted the
                # effective listing to exist. The message names the other
                # statement; it does not claim a covering policy exists, which
                # would need a second read of the policy store to know.
                self.telemetry.add_message(
                    f"No grants are stored on {self.object_kind} {self.object_name}. "
                    f"For the grants that COVER it - those held at the collection "
                    f"or workspace above it - use SHOW EFFECTIVE GRANTS ON "
                    f"{self.object_kind.upper()} {self.object_name}."
                )

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
