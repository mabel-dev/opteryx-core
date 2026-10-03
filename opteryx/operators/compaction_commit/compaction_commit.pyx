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

A relation with a vector index compacts with ROW ORIGINS (§5.6): the writer records,
natively, each written row's input file and ordinal, and before the commit this node
has the connector CARRY the inputs' vectors into each output's index files - no model,
no embedding (D-14) - which land in the same commit. The whole pass holds the dataset's
maintenance lease (§5.7), so an index build never runs beside it.

Every data file is durably written before any catalog mutation, so a failure
before the commit leaves the relation completely untouched. Outputs are
removed when the commit refuses: only the writer knows their paths, and getting
this wrong is what leaked one orphaned output per timed-out pass under the
previous implementation.
"""

from typing import Optional

from opteryx.exceptions import md_code


class CompactionCommitNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, step):
        BasePlanNode.__init__(self, properties, step, step.columns, step.pre_update_columns)
        self.relation_name: str = step.relation_name
        self.connector = step.connector
        # Manifest paths this pass replaces, stamped on the node by
        # CompactionPlanningStrategy. Empty means selection found nothing to do,
        # which is a successful no-op rather than a commit of nothing.
        self.retired_files = list(step.retired_files or [])
        # The snapshot selection was planned against. The catalog refuses the
        # commit if the relation has moved since, so a concurrent writer's work
        # is never erased by a pass that started before it landed.
        self.baseline_snapshot_id = step.baseline_snapshot_id
        # The ordering claim written into every output row group - the primary
        # sort column for a sort-aware plan, None for a brute one. Stamped by
        # CompactionPlanningStrategy, which is the only thing that knows which
        # rule fired.
        self.sorted_by: Optional[str] = step.sorted_by
        self.result: Optional[NonTabularResult] = None
        self.row_origins = bool(step.row_origins)
        self._lease = None

        self._stream = DataFileStream(
            self.connector,
            self.relation_name,
            sorted_by=self.sorted_by,
            write_profile="storage",
            row_origins=self.row_origins,
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

    def _release_lease(self):
        if self._lease is not None:
            lease, self._lease = self._lease, None
            return lease.release()
        return True

    def _push_impl(self, morsel):
        if self._lease is None and self.retired_files:
            # Claimed with the first rows, before any output is written: an index build
            # holding the table refuses this compaction loudly (§5.7), and one cannot start
            # while this runs.
            self._lease = self.connector.claim_compaction_lease(
                self.relation_name, f"OPTIMIZE TABLE by {self._author}"
            )
        if morsel is not _EOS_SENTINEL:
            try:
                self._stream.push(morsel)
            except Exception:
                self._stream.abandon()
                self._release_lease()
                raise
            return

        try:
            rows = self._stream.finish()
        except Exception:
            self._stream.abandon()
            self._release_lease()
            raise

        if not self.retired_files:
            if rows:
                # Rows arrived for a pass that retires nothing: the scan was
                # not narrowed to the selection. Committing would duplicate
                # every row; a quiet success would hide the planner bug.
                self._stream.discard_outputs()
                raise RuntimeError(
                    f"Compaction Commit: {self.relation_name} wrote "
                    f"{len(rows)} file(s) but retires none"
                )
            # Selection found nothing worth rewriting. A pass that did no
            # work is a success, and committing a snapshot describing
            # nothing would be a lie about what happened.
            self.result = NonTabularResult(
                record_count=0,
                status=QueryStatus.SQL_SUCCESS,
                message=f"nothing to compact in {md_code(self.relation_name)}",
            )
            return

        try:
            index_files = None
            if self.row_origins:
                self._lease.renew()
                index_files = self.connector.carry_compaction_vectors(
                    self.relation_name,
                    self.retired_files,
                    self.baseline_snapshot_id,
                    [path for row in self._stream.rows for path in row.file_paths()],
                    self._stream.origins,
                )
                self._lease.renew()
            self.connector.compaction_commit(
                self.relation_name,
                rows,
                self.retired_files,
                author=self._author,
                baseline_snapshot_id=self.baseline_snapshot_id,
                index_files=index_files,
            )
        except Exception:
            # The outputs are unreferenced by anything now, and only this
            # node knows their paths. Remove them before the error leaves,
            # then let it leave - a refused commit is a real failure and
            # must not be reported as a quiet no-op. Carried index files are
            # orphans for deep clean, like any uncommitted index file (§5.4).
            self._stream.discard_outputs()
            self._release_lease()
            raise
        if not self._release_lease():
            raise RuntimeError(
                f"Compaction Commit: {self.relation_name} committed, but its maintenance lease had "
                "expired and been claimed again before it finished."
            )

        # Files, not rows. The count this statement is measured by is how many
        # objects the relation is now made of - that is what OPTIMIZE was run to
        # change - and calling them rows would report the one number a
        # compaction never moves.
        self.result = NonTabularResult(
            record_count=len(rows),
            status=QueryStatus.SQL_SUCCESS,
            message=(
                f"{len(rows):,} file(s) written to {md_code(self.relation_name)}, "
                f"{len(self.retired_files):,} retired"
            ),
        )
