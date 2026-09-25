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
Show Lineage Node

This is a SQL Query Execution Plan Node.

Answers `SHOW LINEAGE FOR <table>` from the receipts binder/view.py's
visit_show_lineage already fetched and elided (via the Scan below it, never
itself read — see serial_engine.py's special-op dispatch). One row per
(snapshot, read-source), the full `_LINEAGE_COLUMNS` shape — SHOW has no
WHERE/column-list grammar to filter or project this with, so this always
returns the whole thing.

Row ORDER is the binder's (newest snapshot first, then source); this does not
re-sort, and it does not elide: by the time rows reach here every name the
caller may not see is already null.
"""

from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.


class ShowLineageNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, step):
        BasePlanNode.__init__(self, properties, step, step.columns, step.pre_update_columns)
        self._lineage = step.lineage
        self.seen = False

    @property
    def name(self):  # pragma: no cover
        return "Show Lineage"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel):
        if self.seen:
            yield None
            return

        from opteryx.models.lineage_history import lineage_to_morsel

        self.seen = True
        yield lineage_to_morsel(self._lineage)
