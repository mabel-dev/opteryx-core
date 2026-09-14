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

Streaming sink: consumes morsels from a child sub-plan, coalesces them into
row groups of streaming, target-sized data files (DataFileStream - never a file
per batch), then commits a single snapshot when EOS is received.
"""

from typing import Generator, Optional

# EOS sentinel in scope as _EOS_SENTINEL via the umbrella unit.
from opteryx.constants import QueryStatus
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties

# BasePlanNode and Morsel in scope via _operators.pyx include (the latter
# cimported there from draken.morsels.morsel).

# rugo's write_parquet_with_bounds default (rugo/parquet.py) - the coalescing
# buffer below is capped here so a flushed file never spans more than one row
# group. That path only populates FileEntry bounds for single-row-group files,
# so staying within one row group keeps catalog pruning working unchanged.
# Move this together with rugo's default if that ever changes.
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

        # REFRESH MATERIALIZED VIEW desugars to a CoRTAS carrying this flag. It
        # is what lets the operator stamp the view's refresh state on success:
        # the worker only ever stamps trigger-fired refreshes, so without it a
        # manual refresh - the documented recovery path after a failed one -
        # would succeed while leaving `last-refreshed-at-ms` reading as though
        # it had never run.
        self.is_refresh = parameters.get("is_refresh", False)

        # The provenance receipt and its producer, settled by the binder off
        # the bound Scan nodes (see `_read_sources` there). Passed to the
        # connector beside `author`; the connector decides what the store
        # accepts. `[]` here is an assertion the statement read no catalog
        # relation, and only the binder's walk may make it.
        self.read_sources = parameters.get("read_sources")
        self.produced_by = parameters.get("produced_by")

        self._total_rows = 0
        self.result: Optional[NonTabularResult] = None

        # Rows become row groups of streaming, target-sized files - see
        # DataFileStream for the shape and for why never a file per batch.
        self._stream = DataFileStream(
            self.connector,
            self.relation_name,
            coalesce_rows=parameters.get("write_coalesce_rows"),
            target_file_bytes=parameters.get("target_file_bytes"),
        )

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

    @property
    def _commit_message(self):
        """What the snapshot history should say this write WAS, or None to let
        the store describe it however it describes the mechanism.

        Only the two statements that own a materialized view have something to
        add: without this a view's history reads as a run of anonymous appends
        and overwrites, indistinguishable from someone writing the backing table
        by hand. The message carries no identity - `author` is the attribution,
        and a principal named here would be whoever wrote this line rather than
        whoever ran the statement.

        A refresh is told only THAT a refresh ran: which source changed is known
        to the service that fired the trigger and never reaches the engine, so
        the message claims no more than the engine knows.

        CREATE OR REPLACE over a view that already exists gets no message - it
        replaces contents rather than populating them, so "initial population"
        would be false, and the store's own replace wording is at least true.
        """
        if self.is_refresh:
            return "materialized view refreshed"
        if self.is_materialized_view and not self.is_replace:
            return "initial population of materialized view"
        return None

    def _push_impl(self, morsel):
        if self.is_noop:
            if morsel is _EOS_SENTINEL:
                self.result = NonTabularResult(
                    record_count=0,
                    status=QueryStatus.SQL_SUCCESS,
                )
            return

        if morsel is _EOS_SENTINEL:
            try:
                file_entries = self._stream.finish()
            except Exception:
                self._stream.abandon()
                raise
            # All files are durably written before any catalog mutation - a
            # mid-query failure above this point leaves the target relation
            # completely untouched, whether this is a fresh create or a replace.
            # A commit the store refuses removes the outputs before it raises.
            try:
                if self.is_replace:
                    self.connector.replace_relation(
                        self.relation_name, self.target_schema, file_entries,
                        author=self._author,
                        commit_message=self._commit_message,
                        read_sources=self.read_sources,
                        produced_by=self.produced_by,
                    )
                elif self.create_target:
                    self.connector.create_relation(
                        self.relation_name, self.target_schema, author=self._author
                    )
                    self.connector.insert(
                        self.relation_name, file_entries, author=self._author,
                        commit_message=self._commit_message,
                        read_sources=self.read_sources,
                        produced_by=self.produced_by,
                    )
                else:
                    self.connector.insert(
                        self.relation_name, file_entries, author=self._author,
                        read_sources=self.read_sources,
                        produced_by=self.produced_by,
                    )
            except Exception:
                self._stream.discard_outputs()
                raise
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
            if self.is_refresh:
                # The refresh landed. Stamping here rather than in the worker
                # covers manual refreshes too - the worker only sees the
                # trigger-fired ones, and it is the failure path that needs it
                # (a refresh that dies cannot stamp its own state).
                self.connector.mark_materialized_view_refreshed(
                    self.relation_name, status="succeeded", author=self._author
                )
            self.result = NonTabularResult(
                record_count=self._total_rows,
                status=QueryStatus.SQL_SUCCESS,
            )
            return

        if self.column_mapping is not None and self.target_column_names is not None:
            morsel = self._align_morsel(morsel)

        self._total_rows += len(morsel)
        try:
            self._stream.push(morsel)
        except Exception:
            self._stream.abandon()
            raise

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
