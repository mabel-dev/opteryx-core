# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Parquet writer for opteryx-managed relations.

Uses the native rugo writer (rugo.parquet_writer) — NO pyarrow. Morsels are
serialized straight to well-formed, PyArrow-readable parquet bytes.
"""

import os
from typing import Optional

from draken.morsels.morsel import Morsel
from opteryx.utils import unique_id


class LocalDataFileWriter:
    """One parquet file in `relation_dir`, written a row group at a time.

    Streams through rugo's constant-memory writer into a `.tmp` file that is
    renamed into place on `close`, so a reader never sees a partial file.
    `abort` removes the temporary file; nothing is left behind.

    Every row group is folded into the file's native statistics (`FileStats`:
    per column the ordinal min / max and the null count - the dialect ANALYZE
    records), and `close` hands the file back as a native file row.
    """

    def __init__(self, relation_dir: str, sorted_by: Optional[str], sorted_descending: bool,
                 write_profile: str):
        from rugo.parquet import open_parquet_writer

        # lazy: the native manifest module must not be the first thing to load
        # opteryx.compiled.planner.column_type (it imports opteryx.types, which
        # imports it back)
        from opteryx.compiled.planner.native_manifest import FileStats

        self.file_name = f"data-{unique_id()}.parquet"
        self._full_path = os.path.join(relation_dir, self.file_name)
        self._tmp_path = f"{self._full_path}.tmp"
        # Held open across row groups; closed by close() or abort(), not a `with`.
        self._fh = open(self._tmp_path, "wb")  # noqa: SIM115
        self._writer = open_parquet_writer(
            self._fh.write,
            compression="zstd",
            sorted_by=sorted_by,
            sorted_descending=sorted_descending,
            profile=write_profile,
        )
        self._rows = 0
        self._bytes = 0
        self._row_groups = 0
        self._stats = FileStats()
        self._done = False

    @property
    def file_path(self) -> str:
        return self.file_name

    @property
    def record_count(self) -> int:
        return self._rows

    @property
    def uncompressed_size_in_bytes(self) -> int:
        return self._bytes

    def write_row_group(self, morsel: Morsel) -> None:
        if self._done:
            raise ValueError(f"write_row_group on a finished writer for '{self.file_name}'")
        if len(morsel) == 0:
            raise ValueError("cannot write an empty row group")
        self._writer.write_row_group(morsel)
        self._rows += len(morsel)
        self._bytes += morsel.nbytes
        self._row_groups += 1
        self._stats.add_row_group(morsel)

    def close(self):
        """Finish the file; it becomes visible under its name. Returns the
        file as a native file row (a one-file NativeManifest)."""
        if self._done:
            raise ValueError(f"close on a finished writer for '{self.file_name}'")
        if self._row_groups == 0:
            raise ValueError(f"close on '{self.file_name}' with no row groups written; abort it instead")
        self._done = True
        self._writer.close()
        self._fh.close()
        os.replace(self._tmp_path, self._full_path)
        return self._stats.file_row(
            self.file_name,
            "PARQUET",
            self._rows,
            os.path.getsize(self._full_path),
            self._row_groups,
            self._bytes,
        )

    def abort(self) -> None:
        if self._done:
            return
        self._done = True
        self._writer = None
        self._fh.close()
        if os.path.exists(self._tmp_path):
            os.remove(self._tmp_path)


def open_data_file_writer(
    relation_dir: str,
    sorted_by: Optional[str] = None,
    sorted_descending: bool = False,
    write_profile: str = "fast",
) -> LocalDataFileWriter:
    """Open a streaming parquet file in `relation_dir` (must already exist).

    See Writable.open_data_file_writer for the handle's contract."""
    return LocalDataFileWriter(relation_dir, sorted_by, sorted_descending, write_profile)
