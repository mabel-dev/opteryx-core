# cython: language_level=3
# cython: boundscheck=False
# cython: wraparound=False

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Compaction Commit Node

The sink half of OPTIMIZE. Everything below it is an ordinary plan - a scan
pinned to the selected files, sorted when the plan is sort-aware - so this node
does only what the plan cannot: write the rewritten rows out, and swap them for
the files they replace in ONE snapshot.

Rows go through DataFileStream: row groups of streaming, target-sized files,
never a file per batch - see that module for why, and for the production
failure that made it so. What is particular to this sink: the ordering claim
(the sort-aware plan's primary key), the "storage" write profile (bytes a
compaction writes are read many times), and the whole-file retirement.

Every data file is durably written before any catalog mutation, so a failure
before the commit leaves the relation completely untouched. Outputs are
removed when the commit refuses: only the writer knows their paths, and getting
this wrong is what leaked one orphaned output per timed-out pass under the
previous implementation.
"""

from typing import Optional


class CompactionCommitNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.relation_name: str = parameters.get("relation_name")
        self.connector = parameters.get("connector")
        # Manifest paths this pass replaces, stamped on the node by
        # CompactionPlanningStrategy. Empty means selection found nothing to do,
        # which is a successful no-op rather than a commit of nothing.
        self.retired_files = list(parameters.get("retired_files") or [])
        # The snapshot selection was planned against. The catalog refuses the
        # commit if the relation has moved since, so a concurrent writer's work
        # is never erased by a pass that started before it landed.
        self.baseline_snapshot_id = parameters.get("baseline_snapshot_id")
        # The ordering claim written into every output row group - the primary
        # sort column for a sort-aware plan, None for a brute one. Stamped by
        # CompactionPlanningStrategy, which is the only thing that knows which
        # rule fired.
        self.sorted_by: Optional[str] = parameters.get("sorted_by")
        self.result: Optional[NonTabularResult] = None

        self._stream = DataFileStream(
            self.connector,
            self.relation_name,
            coalesce_rows=parameters.get("write_coalesce_rows"),
            target_file_bytes=parameters.get("target_file_bytes"),
            sorted_by=self.sorted_by,
            write_profile="storage",
        )

    @property
    def name(self):  # pragma: no cover
        return "Compaction Commit"

    @property
    def config(self):  # pragma: no cover
        return f"{self.relation_name}, {len(self.retired_files)} files retired"

    @property
    def _author(self):
        from opteryx.variables import resolve

        return resolve("external_user", self.properties.variables, None) or None

    def _push_impl(self, morsel):
        if morsel is not _EOS_SENTINEL:
            try:
                self._stream.push(morsel)
            except Exception:
                self._stream.abandon()
                raise
            return

        try:
            entries = self._stream.finish()
        except Exception:
            self._stream.abandon()
            raise

        if not self.retired_files:
            if entries:
                # Rows arrived for a pass that retires nothing: the scan was
                # not narrowed to the selection. Committing would duplicate
                # every row; a quiet success would hide the planner bug.
                self._stream.discard_outputs()
                raise RuntimeError(
                    f"Compaction Commit: {self.relation_name} wrote "
                    f"{len(entries)} file(s) but retires none"
                )
            # Selection found nothing worth rewriting. A pass that did no
            # work is a success, and committing a snapshot describing
            # nothing would be a lie about what happened.
            self.result = NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
            return

        try:
            self.connector.compaction_commit(
                self.relation_name,
                entries,
                self.retired_files,
                author=self._author,
                baseline_snapshot_id=self.baseline_snapshot_id,
            )
        except Exception:
            # The outputs are unreferenced by anything now, and only this
            # node knows their paths. Remove them before the error leaves,
            # then let it leave - a refused commit is a real failure and
            # must not be reported as a quiet no-op.
            self._stream.discard_outputs()
            raise

        self.result = NonTabularResult(record_count=len(entries), status=QueryStatus.SQL_SUCCESS)
