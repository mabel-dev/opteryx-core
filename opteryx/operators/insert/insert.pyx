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
Insert Node

Streaming sink: consumes morsels from a child sub-plan, writes one parquet
file per morsel into the target relation's folder, then commits a single
snapshot when EOS is received.
"""

from typing import Generator, Optional

# EOS sentinel in scope as _EOS_SENTINEL via the umbrella unit.
from opteryx.constants import QueryStatus
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties

# BasePlanNode in scope via _operators.pyx include.


class InsertNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.relation_name: str = parameters.get("relation_name")
        self.connector = parameters.get("connector")
        self.target_schema = parameters.get("target_schema")
        self.column_mapping = parameters.get("column_mapping")
        self.target_column_names = parameters.get("target_column_names")

        self.create_target = parameters.get("create_target", False)
        self.is_replace = parameters.get("is_replace", False)
        self.is_noop = parameters.get("is_noop", False)

        # CREATE MATERIALIZED VIEW: after the CTAS write commits, the target
        # is registered as an MV with its defining SQL (re-rendered from the
        # stashed AST) and the source tables the binder extracted.
        self.is_materialized_view = parameters.get("is_materialized_view", False)
        self.defining_query = parameters.get("defining_query")
        self.source_tables = parameters.get("source_tables")

        self._file_entries = []
        self._total_rows = 0
        self.result: Optional[NonTabularResult] = None

    @property
    def name(self):
        return "Insert"

    @property
    def config(self):
        return f"insert into {self.relation_name}"

    @property
    def _author(self):
        """The session user this write is attributed to, or None when unauthenticated.

        None is passed through rather than substituted, so a store that requires
        attribution rejects the write instead of recording an invented identity.
        """
        from opteryx.variables import resolve

        return resolve("external_user", self.properties.variables, None) or None

    def _push_impl(self, morsel):
        if self.is_noop:
            if morsel is _EOS_SENTINEL:
                self.result = NonTabularResult(
                    record_count=0,
                    status=QueryStatus.SQL_SUCCESS,
                )
            return

        if morsel is _EOS_SENTINEL:
            # All files are durably written before any catalog mutation - a
            # mid-query failure above this point leaves the target relation
            # completely untouched, whether this is a fresh create or a replace.
            if self.is_replace:
                self.connector.replace_relation(
                    self.relation_name, self.target_schema, self._file_entries,
                    author=self._author,
                )
            elif self.create_target:
                self.connector.create_relation(
                    self.relation_name, self.target_schema, author=self._author
                )
                self.connector.insert(
                    self.relation_name, self._file_entries, author=self._author
                )
            else:
                self.connector.insert(
                    self.relation_name, self._file_entries, author=self._author
                )
            if self.is_materialized_view:
                # Registration happens after the data commit, in the same
                # statement. If it fails the statement fails visibly: the
                # backing table exists but is not registered as an MV -
                # re-running CREATE OR REPLACE MATERIALIZED VIEW repairs it.
                from opteryx.third_party import sqloxide

                defining_sql = sqloxide.ast_to_sql([{"Query": self.defining_query}])[0]
                self.connector.register_materialized_view(
                    self.relation_name,
                    defining_sql,
                    self.source_tables,
                    author=self._author,
                )
            self.result = NonTabularResult(
                record_count=self._total_rows,
                status=QueryStatus.SQL_SUCCESS,
            )
            return

        if self.column_mapping is not None and self.target_column_names is not None:
            morsel = self._align_morsel(morsel)

        file_entry = self.connector.write_morsel(self.relation_name, morsel)
        self._file_entries.append(file_entry)
        self._total_rows += len(morsel)

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
        morsel = morsel.select(ordered_source_names)
        morsel = morsel.rename([n.encode("utf-8") if isinstance(n, str) else n
                                 for n in self.target_column_names])
        return morsel
