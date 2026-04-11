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
from opteryx.compiled.draken.interop.arrow import vector_from_sequence
from opteryx.compiled.draken.morsels.morsel import Morsel
from opteryx.models import QueryProperties
from opteryx.types import OrsoTypes

from . import BasePlanNode

_DATA_FORMAT = "draken"


def _simple_collector(schema):
    """
    We've been given the schema, so just translate to a Draken morsel
    """

    names = []
    types = []
    nullables = []
    aliases = []

    for column in schema.columns:
        type_label = str(column.type)
        if column.length is not None:
            type_label += f"[{column.length}]"
        if column.scale is not None and column.precision is not None:
            type_label += f"({column.precision},{column.scale})"
        if column.element_type is not None and str(column.type) == "ARRAY":
            type_label += f"<{column.element_type}>"

        names.append(column.name)
        types.append(type_label)
        nullables.append(column.nullable)
        aliases.append(column.aliases)

    vectors = [
        vector_from_sequence(names, dtype=OrsoTypes.VARCHAR),
        vector_from_sequence(types, dtype=OrsoTypes.VARCHAR),
        vector_from_sequence(nullables, dtype=OrsoTypes.BOOLEAN),
        vector_from_sequence(aliases, dtype=OrsoTypes.VARCHAR),
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
