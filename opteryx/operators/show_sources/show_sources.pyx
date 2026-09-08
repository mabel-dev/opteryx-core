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
Show Sources Node

This is a SQL Query Execution Plan Node.

Answers `SHOW SOURCES FOR <table>` from the standing source list
binder/view.py's visit_show_sources already read and elided (via the Scan
below it, never itself read — see serial_engine.py's special-op dispatch).
One row per name, most recent first, the full `_SOURCE_LIST_COLUMNS` shape;
an empty list is one all-null row so `complete` is still reported.

Row ORDER is the catalog's (most recent first, which `position` states);
this does not re-sort and does not elide.
"""

from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.


class ShowSourcesNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._sources = parameters.get("sources")
        self.seen = False

    @property
    def name(self):  # pragma: no cover
        return "Show Sources"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel):
        if self.seen:
            yield None
            return

        from opteryx.models.source_list import sources_to_morsel

        self.seen = True
        yield sources_to_morsel(self._sources)
