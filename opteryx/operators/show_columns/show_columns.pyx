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
Show Columns Node

This is a SQL Query Execution Plan Node.

Gives information about a dataset's columns
"""

from typing import Generator, Optional
from opteryx.models import QueryProperties
from opteryx.types.logical_type import LogicalCategory
from draken.interop.vector_sequence import vector_from_sequence

# BasePlanNode/JoinNode in scope via _operators.pyx include.

def _simple_collector(schema):
    """
    We've been given the schema, so just translate to a Draken morsel
    """

    names = []
    types = []
    nullables = []
    aliases = []

    # D-4 Phase 2: display via the unified column_type (carries precision/scale
    # inside the LogicalType, and the ARRAY element). The side-cars no longer
    # exist; for the rare column_type==None case fall back to the bare type name.
    for column in schema.columns:
        ct = column.column_type
        if ct is not None:
            type_label = str(ct)
        else:
            type_label = str(column.type)

        names.append(column.name)
        types.append(type_label)
        nullables.append(column.nullable)
        aliases.append(column.aliases)

    vectors = [
        vector_from_sequence(names, dtype=LogicalCategory.VARCHAR),
        vector_from_sequence(types, dtype=LogicalCategory.VARCHAR),
        vector_from_sequence(nullables, dtype=LogicalCategory.BOOLEAN),
        vector_from_sequence(aliases, dtype=LogicalCategory.VARCHAR),
    ]

    return Morsel.from_vectors(["name", "type", "nullable", "aliases"], vectors)


class ShowColumnsNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self._full = parameters.get("full")
        self._extended = parameters.get("extended")
        self._schema = parameters.get("schema")
        self._column_map = {
            c.schema_column.identity: c.source_column for c in parameters["columns"]
        }
        self.collector = None
        self.seen = False

    @property
    def name(self):  # pragma: no cover
        return "Show Columns"

    @property
    def config(self):  # pragma: no cover
        return ""

    def rename_column(self, dic: dict, renames) -> dict:
        dic["name"] = renames[dic["name"]]
        return dic

    def execute(self, morsel):
        if self.seen:
            yield None
            return

        if not (self._full or self._extended):
            # if it's not full or extended, do just get the list of columns and their
            # types
            self.seen = True
            yield _simple_collector(self._schema)
            return

        if self._full or self._extended:
            # we're going to read the full table, so we can count stuff

            self.telemetry.add_message("SHOW FULL/SHOW EXTENDED not implemented")

            self.seen = True
            yield _simple_collector(self._schema)
            return
