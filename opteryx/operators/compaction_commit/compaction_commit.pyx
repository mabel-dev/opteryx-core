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

Compaction is MERGE without the predicate, and this is deliberately the same
shape as `merge.pyx`: buffer references, flush a whole batch as one file, and
commit nothing until EOS. Every data file is durably written before any catalog
mutation, so a failure before the commit leaves the relation completely
untouched.

⛔ OUTPUTS ARE REMOVED WHEN THE COMMIT REFUSES. The catalog raises rather than
returning on a failed row-count invariant precisely so this node can clean up:
only the writer knows which files it wrote. Getting this wrong is what leaked
one orphaned output per timed-out pass under the previous implementation.
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

        self._file_entries = []
        self.result: Optional[NonTabularResult] = None

        self.coalesce_rows = min(
            int(parameters.get("write_coalesce_rows", _MAX_ROWS_PER_ROW_GROUP)),
            _MAX_ROWS_PER_ROW_GROUP,
        )
        # Rows AND projected arena bytes - see MorselBatcher. Rows alone is
        # what failed here in production: a pass over wide string rows filled
        # 262144 rows into one Morsel.combine and the concat refused with
        # `total arena bytes exceed 4 GB`. No row threshold can see payload
        # width, so no value of it was ever safe.
        self._batcher = MorselBatcher(self.coalesce_rows)

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
        if morsel is _EOS_SENTINEL:
            self._flush_pending()

            if not self.retired_files:
                # Selection found nothing worth rewriting. A pass that did no
                # work is a success, and committing a snapshot describing
                # nothing would be a lie about what happened.
                self.result = NonTabularResult(
                    record_count=0, status=QueryStatus.SQL_SUCCESS
                )
                return

            try:
                self.connector.compaction_commit(
                    self.relation_name,
                    self._file_entries,
                    self.retired_files,
                    author=self._author,
                    baseline_snapshot_id=self.baseline_snapshot_id,
                )
            except Exception:
                # The outputs are unreferenced by anything now, and only this
                # node knows their paths. Remove them before the error leaves,
                # then let it leave - a refused commit is a real failure and
                # must not be reported as a quiet no-op.
                self._delete_written_files()
                raise

            self.result = NonTabularResult(
                record_count=len(self._file_entries), status=QueryStatus.SQL_SUCCESS
            )
            return

        self._consume(morsel)

    def _consume(self, morsel):
        """Buffer a morsel by REFERENCE, writing a whole batch at a time.

        References only, never an incremental concat per arrival: concatenating
        into a live accumulator re-copies the growing buffer on every morsel,
        which is quadratic in the number of morsels.
        """
        for batch in self._batcher.push(morsel):
            self._write_batch(batch)

    def _flush_pending(self):
        """Write whatever the batcher still holds, as one data file per batch."""
        for batch in self._batcher.finish():
            self._write_batch(batch)

    def _write_batch(self, batch):
        self._file_entries.append(self.connector.write_morsel(self.relation_name, batch))

    def _delete_written_files(self):
        """Best-effort removal of this pass's outputs after a refused commit.

        Best-effort because the commit already failed and the original error is
        the one worth raising; a failure to clean up must not replace it. What
        survives here is an orphan the storage sweep can find, which is strictly
        better than an orphan nobody knows about.
        """
        for entry in self._file_entries:
            try:
                self.connector.delete_data_file(self.relation_name, entry.file_path)
            except Exception:  # noqa: BLE001 - storage boundary, see docstring
                pass
        self._file_entries = []
