# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Insert Node

Streaming sink: consumes morsels from a child sub-plan, writes one parquet
file per morsel into the target relation's folder, then commits a single
snapshot when EOS is received.
"""

from typing import Generator, Optional

from opteryx import EOS
from opteryx.constants import QueryStatus
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties

from . import BasePlanNode


class InsertNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.relation_name: str = parameters.get("relation_name")
        self.connector = parameters.get("connector")
        self.target_schema = parameters.get("target_schema")
        self.column_mapping = parameters.get("column_mapping")
        self.target_column_names = parameters.get("target_column_names")

        self.create_target = parameters.get("create_target", False)
        self.is_noop = parameters.get("is_noop", False)

        self._file_entries = []
        self._total_rows = 0
        self._created = False
        self.result: Optional[NonTabularResult] = None

    @property
    def name(self):
        return "Insert"

    @property
    def config(self):
        return f"insert into {self.relation_name}"

    def execute(self, morsel) -> Generator:
        from opteryx.connectors.parquet_io.parquet_writer import write_morsel

        if self.is_noop:
            if morsel == EOS:
                self.result = NonTabularResult(
                    record_count=0,
                    status=QueryStatus.SQL_SUCCESS,
                )
            return
            yield  # pragma: no cover

        if self.create_target and not self._created:
            self.connector.create_relation(self.relation_name, self.target_schema)
            self._created = True

        if morsel == EOS:
            # Commit snapshot.
            self.connector.insert(self.relation_name, self._file_entries)
            self.result = NonTabularResult(
                record_count=self._total_rows,
                status=QueryStatus.SQL_SUCCESS,
            )
            return
            yield  # pragma: no cover — make this a generator

        if self.column_mapping is not None and self.target_column_names is not None:
            morsel = self._align_morsel(morsel)

        relation_dir = self.connector._relation_dir(self.relation_name)
        file_entry = write_morsel(morsel, relation_dir)
        self._file_entries.append(file_entry)
        self._total_rows += len(morsel)
        return
        yield  # pragma: no cover — make this a generator

    def _align_morsel(self, morsel):
        """Reorder columns to target-schema order and rename to target names.

        ``self.column_mapping[src_idx] = target_schema_idx``.
        """
        from opteryx.exceptions import InvalidInternalStateError

        n_target = len(self.target_column_names)
        src_for_target = [-1] * n_target
        for src_idx, tgt_idx in enumerate(self.column_mapping):
            src_for_target[tgt_idx] = src_idx
        if any(s < 0 for s in src_for_target):
            raise InvalidInternalStateError("INSERT column mapping is incomplete")

        source_names = morsel.column_names  # list[bytes]
        ordered_source_names = [source_names[s] for s in src_for_target]
        morsel.select(ordered_source_names)
        morsel.rename([n.encode("utf-8") if isinstance(n, str) else n
                       for n in self.target_column_names])
        return morsel
