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
Show Snapshots Node

This is a SQL Query Execution Plan Node.

Answers `SHOW SNAPSHOTS FOR <table>` from the commit history binder/view.py's
visit_show_snapshots already fetched (via the Scan below it, never itself read
— see serial_engine.py's special-op dispatch). One row per live snapshot, the
full `_SNAPSHOT_COLUMNS` shape — SHOW has no WHERE/column-list grammar to
filter or project this with, so this always returns the whole thing.

Row ORDER is the connector's (newest first); this does not re-sort.
"""

from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.


class ShowSnapshotsNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._snapshots = parameters.get("snapshots")
        self.seen = False

    @property
    def name(self):  # pragma: no cover
        return "Show Snapshots"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel):
        if self.seen:
            yield None
            return

        from opteryx.models.snapshot_history import snapshots_to_morsel

        self.seen = True
        yield snapshots_to_morsel(self._snapshots)
