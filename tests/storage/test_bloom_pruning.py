# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Bloom-filter row-group pruning on the parquet scan path.

A `WHERE col = value` for a value that is ABSENT but falls INSIDE the column's
[min, max] range cannot be pruned by min/max statistics — only a bloom filter
can skip it. We write the same data twice (bloom on / bloom off) and confirm:

  * bloom on  -> the row group is pruned, zero rows are read from disk
  * bloom off -> the row group is fully scanned
  * results are identical either way (no rows wrongly dropped)
"""

import os
import shutil
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
import draken.draken_native as dn
from draken.vectors.vector import Vector
from draken.morsels.morsel import Morsel
from rugo.parquet import write_parquet

# Even numbers 0..199998 -> min=0, max=199998. Odd values in that range are
# absent but un-prunable by min/max; value 4 is present.
_VALUES = list(range(0, 200_000, 2))


def _write_dataset(folder: str, bloom: bool):
    os.makedirs(folder, exist_ok=True)
    morsel = Morsel.from_vectors(["i"], [Vector(dn.vector_from_sequence(_VALUES))])
    with open(os.path.join(folder, "part.parquet"), "wb") as fh:
        fh.write(write_parquet(morsel, bloom_filters=bloom, dictionary=False))


def _scan_stats(dataset: str, sql_value: int):
    session = opteryx.session()
    morsels = list(
        session.execute_to_morsels(f"SELECT COUNT(*) FROM {dataset} WHERE i = {sql_value}")
    )
    count = morsels[0][0][0] if morsels and morsels[0].num_rows else None
    scan = [v for v in session.telemetry["operations"].values() if v.get("type") == "ReadRel"][0]
    return count, scan["parquet_rows_before_filter"]


def test_bloom_prunes_absent_in_range_value():
    # Relative single-level folders so opteryx can address them as datasets
    # (dotted name == path). Fixed identifier-safe names, cleaned up after.
    base = "bloompruning_tmp"
    on = os.path.join(base, "on")
    off = os.path.join(base, "off")
    try:
        _write_dataset(on, bloom=True)
        _write_dataset(off, bloom=False)
        on_ds = on.replace(os.sep, ".")
        off_ds = off.replace(os.sep, ".")

        # Absent value 5 is inside [0, 199998] -> min/max cannot prune it.
        on_absent_count, on_absent_scanned = _scan_stats(on_ds, 5)
        off_absent_count, off_absent_scanned = _scan_stats(off_ds, 5)

        # Present value 4.
        on_present_count, on_present_scanned = _scan_stats(on_ds, 4)

        # Correctness: same answers regardless of bloom.
        assert on_absent_count == 0
        assert off_absent_count == 0
        assert on_present_count == 1

        # The bloom filter pruned the row group: nothing read from disk.
        assert on_absent_scanned == 0, on_absent_scanned
        # Without a bloom filter the whole row group is scanned.
        assert off_absent_scanned == len(_VALUES), off_absent_scanned
        # A present value can't be pruned either way.
        assert on_present_scanned == len(_VALUES), on_present_scanned
    finally:
        shutil.rmtree(base, ignore_errors=True)


if __name__ == "__main__":
    test_bloom_prunes_absent_in_range_value()
    print("✅ okay")
