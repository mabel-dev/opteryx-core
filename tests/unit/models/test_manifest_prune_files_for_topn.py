# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Manifest.prune_files_for_topn - file pruning for `ORDER BY <col> LIMIT n`.

Drops files that provably cannot hold any of the top-`limit` rows of a
single-column sort, using per-file (lower_bound, upper_bound, record_count)
already in the manifest. See the method's own docstring
(opteryx/models/manifest.py) for the accumulation algorithm.

Precondition enforced by the CALLER (TopNManifestPruningStrategy), not this
method: the sort column must have zero NULLs across the manifest. These
tests exercise the method directly and don't need to re-assert that gate -
see test_topn_manifest_pruning_strategy.py for the gate itself.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

from opteryx.models.file_entry import FileEntry
from opteryx.models.manifest import Manifest
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


def _schema(column_name="project"):
    return RelationSchema(
        name="t",
        columns=[
            SchemaColumn(
                name=column_name,
                column_type=INT64,
                identity=mint_column_identity("t", column_name),
            ),
        ],
    )


def _file(path, lo, hi, record_count):
    return FileEntry(
        file_path=path,
        file_format="PARQUET",
        record_count=record_count,
        file_size_in_bytes=0,
        lower_bounds={0: lo},
        upper_bounds={0: hi},
    )


def test_desc_prunes_file_entirely_below_the_guaranteed_floor():
    # Four disjoint-range files, 10 rows each. LIMIT 5 only needs the top
    # file (10 rows >= 5) - the other three are entirely below its floor.
    files = [
        _file("apple_banana", 1, 20, 10),
        _file("carrot_eggplant", 21, 40, 10),
        _file("grape_jackfruit", 41, 60, 10),
        _file("lemon_melon", 61, 80, 10),
    ]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=True, limit=5)

    assert [f.file_path for f in manifest.files] == ["lemon_melon"]


def test_asc_is_the_mirror_of_desc():
    files = [
        _file("apple_banana", 1, 20, 10),
        _file("carrot_eggplant", 21, 40, 10),
        _file("grape_jackfruit", 41, 60, 10),
        _file("lemon_melon", 61, 80, 10),
    ]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=False, limit=5)

    assert [f.file_path for f in manifest.files] == ["apple_banana"]


def test_desc_needs_two_files_when_the_top_file_is_short_of_the_limit():
    files = [
        _file("top", 90, 100, 3),  # only 3 rows, LIMIT is 5
        _file("middle", 50, 89, 10),
        _file("bottom", 0, 49, 10),
    ]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=True, limit=5)

    # top (3) + middle (10) = 13 >= 5; threshold = min(lo) over {top, middle}
    # = min(90, 50) = 50, so "bottom" (max 49) is provably excluded.
    assert {f.file_path for f in manifest.files} == {"top", "middle"}


def test_threshold_is_the_minimum_lower_bound_across_all_included_files_not_just_the_last():
    # Regression for a specific bug shape: naively taking the LAST file's own
    # lower bound (rather than the running minimum across every file folded
    # into the accumulation) understates how many files must be kept.
    #
    # Ranked by max descending: A(hi=100, lo=50) → C(hi=95, lo=5) → B(hi=90, lo=10).
    # record_count 3 each, LIMIT=5.
    #   after A: accumulated=3 (<5), running lo-min=50
    #   after C: accumulated=6 (>=5), running lo-min=min(50,5)=5   <- stop here
    # Correct threshold is 5 (C's own lo), NOT B's lo=10 - B is never folded
    # into the accumulation and must not influence the threshold at all. A
    # file whose max sits between 5 and 10 (not present here, but D below)
    # must survive: a threshold of 10 would wrongly prune it.
    files = [
        _file("A", 50, 100, 3),
        _file("C", 5, 95, 3),
        _file("B", 10, 90, 3),
        _file("D", 7, 8, 1),  # max=8: survives under threshold=5, dies under threshold=10
    ]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=True, limit=5)

    kept = {f.file_path for f in manifest.files}
    assert "D" in kept, kept


def test_tie_at_the_boundary_is_kept_inclusive():
    files = [
        _file("winner", 100, 100, 5),
        _file("tied", 100, 100, 5),  # same max as the threshold - must survive
        _file("loser", 1, 99, 100),
    ]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=True, limit=5)

    assert {f.file_path for f in manifest.files} == {"winner", "tied"}


def test_file_with_no_bounds_is_always_kept_and_not_used_to_tighten_the_threshold():
    files = [
        _file("has_stats_high", 90, 100, 5),
        FileEntry(
            file_path="no_stats",
            file_format="PARQUET",
            record_count=1000,
            file_size_in_bytes=0,
            lower_bounds=None,
            upper_bounds=None,
        ),
        _file("has_stats_low", 0, 10, 5),
    ]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=True, limit=5)

    kept = {f.file_path for f in manifest.files}
    assert "no_stats" in kept, kept
    assert "has_stats_high" in kept, kept
    assert "has_stats_low" not in kept, kept


def test_limit_larger_than_total_stats_bearing_rows_prunes_nothing():
    files = [
        _file("a", 1, 10, 5),
        _file("b", 11, 20, 5),
    ]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=True, limit=1000)

    assert {f.file_path for f in manifest.files} == {"a", "b"}


def test_accumulated_exactly_equal_to_limit_stops_there():
    files = [
        _file("top", 50, 100, 5),
        _file("bottom", 0, 49, 5),
    ]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=True, limit=5)

    assert [f.file_path for f in manifest.files] == ["top"]


def test_unresolvable_column_is_a_no_op():
    files = [_file("a", 1, 10, 5)]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("does_not_exist", descending=True, limit=1)

    assert [f.file_path for f in manifest.files] == ["a"]


def test_zero_or_negative_limit_is_a_no_op():
    files = [_file("a", 1, 10, 5)]
    manifest = Manifest(files=files, schema=_schema())

    manifest.prune_files_for_topn("project", descending=True, limit=0)
    assert [f.file_path for f in manifest.files] == ["a"]


def test_live_rows_stay_aligned_after_pruning_for_topn():
    # Mirrors the _live_rows bookkeeping prune_files itself relies on for
    # sketch-vector alignment - a second shrink (e.g. WHERE-predicate pruning
    # followed by topn pruning) must keep mapping to ORIGINAL file position,
    # not position-after-first-shrink.
    files = [
        _file("keep_first", 100, 100, 5),
        _file("drop_by_where", 1, 1, 5),
        _file("keep_second", 90, 99, 5),
    ]
    manifest = Manifest(files=files, schema=_schema())

    # Simulate a prior WHERE-predicate prune that already dropped the middle
    # file, the way ManifestPruningStrategy would run before this strategy.
    manifest.files = [files[0], files[2]]
    manifest._live_rows = [0, 2]

    manifest.prune_files_for_topn("project", descending=True, limit=3)

    # keep_first alone (record_count=5) already satisfies LIMIT 3, so
    # keep_second (max=99 < threshold=100) is dropped by this second pass -
    # its surviving row index must still be the ORIGINAL position (0), not
    # its position (0) in the already-once-shrunk 2-file list mistaken for
    # a fresh identity mapping.
    assert manifest._live_rows == [0]
    assert [f.file_path for f in manifest.files] == ["keep_first"]
