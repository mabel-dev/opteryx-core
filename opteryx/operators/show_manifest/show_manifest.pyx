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
Show Manifest Node

This is a SQL Query Execution Plan Node.

Answers `SHOW MANIFEST FOR <table>` from the Manifest binder/view.py's
visit_show_manifest already loaded (via the Scan below it, never itself read
— see serial_engine.py's special-op dispatch). One row per file, the full
`_MANIFEST_COLUMNS` shape — SHOW has no WHERE/column-list grammar to filter
or project this with, so this always returns the whole thing.
"""

from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.

def _collector(manifest):
    from opteryx.models.manifest_io import file_entries_to_manifest_morsel

    return file_entries_to_manifest_morsel(manifest.files, manifest.schema)


class ShowManifestNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._manifest = parameters.get("manifest")
        self.seen = False

    @property
    def name(self):  # pragma: no cover
        return "Show Manifest"

    @property
    def config(self):  # pragma: no cover
        return ""

    def execute(self, morsel):
        if self.seen:
            yield None
            return

        self.seen = True
        yield _collector(self._manifest)
