# cython: language_level=3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Data File Stream

Row groups in, target-sized data files out. The one place every writing sink
(INSERT / CTAS, MERGE, OPTIMIZE) turns a morsel stream into files, so the shape
of what lands in storage is decided once:

  - the batcher coalesces arriving morsels into 262,144-row, byte-bounded
    batches (rows AND projected string-arena bytes - a row ceiling alone let a
    wide-row batch overflow Morsel.combine's uint32 arena in production);
  - each batch is ONE parquet row group of the OPEN file, written through the
    connector's streaming writer;
  - the file is closed and the next opened once its uncompressed size crosses
    `target_file_bytes` - TARGET_SIZE_BYTES, the size compaction's selection
    measures files against, in the same unit, so what a write produces is what
    a later OPTIMIZE leaves alone.

⛔ Never a file per batch. That is what every sink did before this class: a
stream of 262,144-row batches through `write_morsel` was a stream of
262,144-row FILES, every one below the compaction sub-floor. INSERT and CTAS
manufactured the small-file problem and OPTIMIZE rewrote it into the same
shape nightly ("Compaction: 43 files -> 43 files", in production).

Failure leaves nothing behind. `abandon` aborts the open file (its upload never
becomes an object) and removes the files already closed; `discard_outputs`
removes the closed files after a commit the store refused. Both are
best-effort, because the failure that brought the caller here is the one worth
raising - what survives is an orphan the storage sweep can find.
"""

_MAX_ROWS_PER_ROW_GROUP = 262144


class DataFileStream:
    def __init__(
        self,
        connector,
        relation_name,
        coalesce_rows=None,
        target_file_bytes=None,
        sorted_by=None,
        write_profile="fast",
        pending_schema=None,
    ):
        self.connector = connector
        self.relation_name = relation_name
        # The schema the target is ABOUT to be created with, for a sink whose
        # relation does not exist yet (CTAS) - None for every write to one that
        # does. Passed straight through to each writer this stream opens; see
        # Writable.open_data_file_writer for what a store does with it.
        self.pending_schema = pending_schema
        # The ordering claim written into every row group - the caller's
        # assertion that the rows it pushes are ordered on that column. None
        # unless the rows really are (a sort-aware OPTIMIZE); never verified.
        self.sorted_by = sorted_by
        self.write_profile = write_profile
        if target_file_bytes is None:
            from opteryx.planner.compaction.constants import TARGET_SIZE_BYTES

            target_file_bytes = TARGET_SIZE_BYTES
        self.target_file_bytes = int(target_file_bytes)
        if self.target_file_bytes <= 0:
            raise ValueError("DataFileStream: target_file_bytes must be positive")
        rows = _MAX_ROWS_PER_ROW_GROUP if coalesce_rows is None else int(coalesce_rows)
        self.coalesce_rows = min(rows, _MAX_ROWS_PER_ROW_GROUP)
        self._batcher = MorselBatcher(self.coalesce_rows)
        self._writer = None
        self.entries = []

    def push(self, morsel):
        """Buffer a morsel by REFERENCE; write whole batches as they fill.

        References only, never an incremental concat per arrival: concatenating
        into a live accumulator re-copies the growing buffer on every morsel,
        which is quadratic in the number of morsels.
        """
        for batch in self._batcher.push(morsel):
            self._write_batch(batch)

    def finish(self):
        """Write what the batcher holds, close the open file, return the entries."""
        for batch in self._batcher.finish():
            self._write_batch(batch)
        self._close_writer()
        return self.entries

    def abandon(self):
        """A failure landed mid-stream: leave nothing behind."""
        if self._writer is not None:
            writer, self._writer = self._writer, None
            try:
                writer.abort()
            except Exception:  # noqa: BLE001 - storage boundary, see module docstring
                pass
        self.discard_outputs()

    def discard_outputs(self):
        """Remove every closed file. For after a commit the store refused."""
        for entry in self.entries:
            try:
                self.connector.delete_data_file(self.relation_name, entry.file_path)
            except Exception:  # noqa: BLE001 - storage boundary, see module docstring
                pass
        self.entries = []

    def _write_batch(self, batch):
        if self._writer is None:
            self._writer = self.connector.open_data_file_writer(
                self.relation_name,
                sorted_by=self.sorted_by,
                sorted_descending=False,
                write_profile=self.write_profile,
                pending_schema=self.pending_schema,
            )
        self._writer.write_row_group(batch)
        if self._writer.uncompressed_size_in_bytes >= self.target_file_bytes:
            self._close_writer()

    def _close_writer(self):
        if self._writer is None:
            return
        writer, self._writer = self._writer, None
        self.entries.append(writer.close())
