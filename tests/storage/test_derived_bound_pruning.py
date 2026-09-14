# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
End-to-end: predicate shapes that are not `column <op> literal` must still prune
files.

`Manifest.prune_files` reads exactly one shape. Every other shape a user writes —
`IN`, `LIKE 'abc%'`, `IS NULL`, a same-column `OR`, or any monotone transform
wrapped around the column — used to prune ZERO files, because it matched neither
branch of that loop. `predicate_bounds.derive_bound_conjuncts` now derives the
equivalent bound and appends it.

Every test asserts BOTH halves, and neither alone would prove anything:

  * the rows are IDENTICAL to the same query with pruning disabled — a pruning
    change that alters results is a bug, not an optimisation;
  * the expected number of files was actually skipped — row equality on its own
    passes for a derivation that never fired.

The datasets are four disjoint, ordered files, which is what makes file pruning
both possible and observable. A real table that is not clustered on the filtered
column prunes nothing here and nothing in production — that is a property of the
data, not of the derivation.
"""

import os
import shutil
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config
from rugo.parquet import write_parquet

# seq/label/ts all rise together and never overlap between files, so a bound on
# any one of them addresses the same file.
FILES = [
    "SELECT seq, label, CAST(ds AS TIMESTAMP) AS ts FROM (VALUES "
    "(1,'aaa','2020-03-01'),(2,'aab','2020-06-01'),(3,'aac','2020-09-01')) AS t(seq,label,ds)",
    "SELECT seq, label, CAST(ds AS TIMESTAMP) AS ts FROM (VALUES "
    "(11,'baa','2021-03-01'),(12,'bab','2021-06-01'),(13,'bac','2021-09-01')) AS t(seq,label,ds)",
    "SELECT seq, label, CAST(ds AS TIMESTAMP) AS ts FROM (VALUES "
    "(21,'caa','2022-03-01'),(22,'cab','2022-06-01'),(23,'cac','2022-09-01')) AS t(seq,label,ds)",
    "SELECT seq, label, CAST(ds AS TIMESTAMP) AS ts FROM (VALUES "
    "(31,'daa','2023-03-01'),(32,'dab','2023-06-01'),(33,'dac','2023-09-01')) AS t(seq,label,ds)",
]

ALL_ROWS = [
    (1, "aaa"), (2, "aab"), (3, "aac"),
    (11, "baa"), (12, "bab"), (13, "bac"),
    (21, "caa"), (22, "cab"), (23, "cac"),
    (31, "daa"), (32, "dab"), (33, "dac"),
]

# One file of nothing but NULL labels, one file with no NULLs at all, so both
# null-count tests have a file they are entitled to eliminate.
NULL_FILES = [
    "SELECT seq, label FROM (VALUES (1,'aaa'),(2,'aab')) AS t(seq,label)",
    "SELECT seq, CAST(NULL AS VARCHAR) AS label FROM (VALUES (11),(12)) AS t(seq)",
    "SELECT seq, label FROM (VALUES (21,'caa'),(22,'cab')) AS t(seq,label)",
]

NULL_ROWS = [(1, "aaa"), (2, "aab"), (11, None), (12, None), (21, "caa"), (22, "cab")]


def _write_dataset(dir_name, statements):
    """One Parquet file per statement under testdata/<dir_name>/, then ANALYZE —
    the bounds under test are the ones a catalog actually produces."""
    ds_dir = Path("testdata") / dir_name
    if ds_dir.exists():
        shutil.rmtree(ds_dir)
    ds_dir.mkdir(parents=True)
    session = opteryx.session()
    for index, sql in enumerate(statements):
        morsel = list(session.execute_to_morsels(sql))[0]
        with open(ds_dir / f"part-{index}.parquet", "wb") as file:
            file.write(write_parquet(morsel))
    dataset = f"testdata.{dir_name}"
    list(session.execute_to_morsels(f"ANALYZE TABLE {dataset}"))
    return dataset, ds_dir


def _run(sql):
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(
            zip(morsel.column(b"seq").to_pylist(), morsel.column(b"label").to_pylist())
        )
    return sorted(rows, key=lambda row: row[0]), dict(session.telemetry).get("files_pruned", 0)


def _check(dataset, predicate, expected_rows, expected_pruned):
    sql = f"SELECT seq, label FROM {dataset} WHERE {predicate}"
    original = config.features.disable_manifest_pruning
    try:
        config.features.disable_manifest_pruning = False
        on_rows, pruned = _run(sql)
        config.features.disable_manifest_pruning = True
        off_rows, off_pruned = _run(sql)
    finally:
        config.features.disable_manifest_pruning = original

    assert on_rows == sorted(expected_rows, key=lambda row: row[0]), (predicate, on_rows)
    assert on_rows == off_rows, (predicate, on_rows, off_rows)
    assert off_pruned == 0, (predicate, off_pruned)
    assert pruned == expected_pruned, (predicate, pruned, expected_pruned)


def _with_dataset(name, statements=None):
    return _write_dataset(name, statements or FILES)


# ---------------------------------------------------------------------------


def test_in_list_prunes_on_its_hull():
    dataset, ds_dir = _with_dataset("_tmp_bounds_in_list")
    try:
        _check(
            dataset,
            "seq IN (21, 22)",
            [row for row in ALL_ROWS if row[0] in (21, 22)],
            expected_pruned=3,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_in_list_hull_cannot_prune_a_file_inside_its_span():
    """The HULL is what is derived, so the file between the two members survives
    even though it holds neither of them. Recorded as the known looseness it is:
    an exact per-member test against each file's bounds would drop it, and is the
    obvious follow-on. Sound, just not tight."""
    dataset, ds_dir = _with_dataset("_tmp_bounds_in_span")
    try:
        _check(
            dataset,
            "seq IN (12, 31)",
            [row for row in ALL_ROWS if row[0] in (12, 31)],
            expected_pruned=1,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_like_prefix_prunes():
    dataset, ds_dir = _with_dataset("_tmp_bounds_like")
    try:
        _check(
            dataset,
            "label LIKE 'ca%'",
            [row for row in ALL_ROWS if row[1].startswith("ca")],
            expected_pruned=3,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_leading_wildcard_like_prunes_nothing_and_still_answers():
    """`%aa` constrains no prefix, so nothing may be eliminated. Asserted so a
    future "optimisation" here has to break a test to ship."""
    dataset, ds_dir = _with_dataset("_tmp_bounds_like_leading")
    try:
        _check(
            dataset,
            "label LIKE '%aa'",
            [row for row in ALL_ROWS if row[1].endswith("aa")],
            expected_pruned=0,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_same_column_or_prunes_on_the_hull():
    dataset, ds_dir = _with_dataset("_tmp_bounds_or")
    try:
        _check(
            dataset,
            "seq = 12 OR seq = 13",
            [row for row in ALL_ROWS if row[0] in (12, 13)],
            expected_pruned=3,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_or_across_columns_prunes_nothing():
    """A row need satisfy only ONE arm, so neither column is constrained.
    Pruning here would delete rows the query must return."""
    dataset, ds_dir = _with_dataset("_tmp_bounds_or_cross")
    try:
        _check(
            dataset,
            "seq = 12 OR label = 'daa'",
            [row for row in ALL_ROWS if row[0] == 12 or row[1] == "daa"],
            expected_pruned=0,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_arithmetic_around_the_column_prunes():
    dataset, ds_dir = _with_dataset("_tmp_bounds_arithmetic")
    try:
        _check(
            dataset,
            "seq * 2 >= 42",
            [row for row in ALL_ROWS if row[0] * 2 >= 42],
            expected_pruned=2,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_subtraction_from_a_constant_prunes_the_right_end():
    """`100 - seq` DECREASES in seq, so the surviving files are the LOW ones.
    Reversing this would prune exactly the files holding the answer."""
    dataset, ds_dir = _with_dataset("_tmp_bounds_reversed")
    try:
        _check(
            dataset,
            "100 - seq >= 90",
            [row for row in ALL_ROWS if 100 - row[0] >= 90],
            expected_pruned=3,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_extract_year_prunes():
    dataset, ds_dir = _with_dataset("_tmp_bounds_year")
    try:
        _check(
            dataset,
            "EXTRACT(year FROM ts) = 2022",
            [row for row in ALL_ROWS if 21 <= row[0] <= 23],
            expected_pruned=3,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_left_prefix_prunes():
    dataset, ds_dir = _with_dataset("_tmp_bounds_left")
    try:
        _check(
            dataset,
            "LEFT(label, 1) = 'd'",
            [row for row in ALL_ROWS if row[1][0] == "d"],
            expected_pruned=3,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_is_null_prunes_files_with_no_nulls():
    dataset, ds_dir = _write_dataset("_tmp_bounds_is_null", NULL_FILES)
    try:
        _check(
            dataset,
            "label IS NULL",
            [row for row in NULL_ROWS if row[1] is None],
            expected_pruned=2,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_is_not_null_prunes_the_all_null_file():
    dataset, ds_dir = _write_dataset("_tmp_bounds_is_not_null", NULL_FILES)
    try:
        _check(
            dataset,
            "label IS NOT NULL",
            [row for row in NULL_ROWS if row[1] is not None],
            expected_pruned=1,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_derived_bound_combines_with_a_plain_one():
    """The derived term is one more conjunct of the SAME conjunction, so it
    composes with the shapes pruning already read."""
    dataset, ds_dir = _with_dataset("_tmp_bounds_combined")
    try:
        _check(
            dataset,
            "seq >= 11 AND label LIKE 'c%'",
            [row for row in ALL_ROWS if row[0] >= 11 and row[1].startswith("c")],
            expected_pruned=3,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Case folding — bounds valid only where the fold is provably the identity
# ---------------------------------------------------------------------------

# Every label lower-case, so LOWER is the identity in every file and the point
# bound applies everywhere.
LOWER_FILES = [
    "SELECT seq, label FROM (VALUES (1,'aaa'),(2,'aab')) AS t(seq,label)",
    "SELECT seq, label FROM (VALUES (11,'baa'),(12,'bab')) AS t(seq,label)",
    "SELECT seq, label FROM (VALUES (21,'caa'),(22,'cab')) AS t(seq,label)",
]

LOWER_ROWS = [(1, "aaa"), (2, "aab"), (11, "baa"), (12, "bab"), (21, "caa"), (22, "cab")]

# The third file holds 'CAA' — an uppercase spelling whose RAW bounds sit far
# from 'caa' (every capital sorts below every lower-case letter), so a bound
# applied to it would drop the row the query must return. This is the test that
# fails if the per-file precondition is ever dropped.
MIXED_FILES = [
    "SELECT seq, label FROM (VALUES (1,'aaa'),(2,'aab')) AS t(seq,label)",
    "SELECT seq, label FROM (VALUES (11,'baa'),(12,'bab')) AS t(seq,label)",
    "SELECT seq, label FROM (VALUES (21,'CAA'),(22,'CAB')) AS t(seq,label)",
]

MIXED_ROWS = [(1, "aaa"), (2, "aab"), (11, "baa"), (12, "bab"), (21, "CAA"), (22, "CAB")]


def test_lower_equality_prunes_when_every_file_is_lower_case():
    dataset, ds_dir = _write_dataset("_tmp_bounds_fold_lower", LOWER_FILES)
    try:
        _check(
            dataset,
            "LOWER(label) = 'caa'",
            [row for row in LOWER_ROWS if row[1].lower() == "caa"],
            expected_pruned=2,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_ilike_prefix_prunes_when_every_file_is_lower_case():
    dataset, ds_dir = _write_dataset("_tmp_bounds_fold_ilike", LOWER_FILES)
    try:
        _check(
            dataset,
            "label ILIKE 'ca%'",
            [row for row in LOWER_ROWS if row[1].lower().startswith("ca")],
            expected_pruned=2,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_a_file_holding_uppercase_is_never_pruned_by_a_lower_bound():
    """THE soundness test for the case-fold path. 'CAA' sorts nowhere near
    'caa' in raw byte order, so applying the derived point bound to this file
    would delete the only matching row. Row equality against pruning-disabled
    is what catches it."""
    dataset, ds_dir = _write_dataset("_tmp_bounds_fold_mixed", MIXED_FILES)
    try:
        _check(
            dataset,
            "LOWER(label) = 'caa'",
            [row for row in MIXED_ROWS if row[1].lower() == "caa"],
            expected_pruned=2,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_ilike_never_prunes_the_file_holding_the_uppercase_match():
    dataset, ds_dir = _write_dataset("_tmp_bounds_fold_mixed_ilike", MIXED_FILES)
    try:
        _check(
            dataset,
            "label ILIKE 'ca%'",
            [row for row in MIXED_ROWS if row[1].lower().startswith("ca")],
            expected_pruned=2,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_upper_fold_does_not_apply_to_lower_case_data():
    """UPPER is not the identity on lower-case data, so its precondition fails
    everywhere and nothing may be pruned — even though the answer is a single
    row in a single file."""
    dataset, ds_dir = _write_dataset("_tmp_bounds_fold_upper", LOWER_FILES)
    try:
        _check(
            dataset,
            "UPPER(label) = 'CAA'",
            [row for row in LOWER_ROWS if row[1].upper() == "CAA"],
            expected_pruned=0,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_unsatisfiable_case_fold_returns_nothing():
    """`LOWER(col) = 'CAA'` can never be true — LOWER emits no uppercase letter
    under either of the engine's folds."""
    dataset, ds_dir = _write_dataset("_tmp_bounds_fold_impossible", MIXED_FILES)
    try:
        _check(dataset, "LOWER(label) = 'CAA'", [], expected_pruned=0)
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Pruning to NOTHING — the zero-file manifest.
#
# A derivation strong enough to eliminate EVERY file is the best case, not an
# edge case, but it used to be the one case that fell off the native execution
# path: `_native_scan_plan` read `manifest.get_file_count() == 0` as the same
# condition as `manifest is None` and tagged both `no_manifest`, sending the scan
# to the per-morsel Python trampoline. "We did not read a manifest" and "we read
# one and it proves there is nothing to read" are different answers. Split
# 2026-09-14; these tests pin the split so it cannot silently regress.
# ---------------------------------------------------------------------------


def _scan_census(sql):
    """Run `sql`; return (rows, column_names, scan_sources, residual_reasons)."""
    session = opteryx.session()
    rows = []
    names = None
    for morsel in session.execute_to_morsels(sql):
        names = list(morsel.column_names)
        for index in range(morsel.num_rows):
            rows.append(tuple(morsel.column(name)[index] for name in names))
    telemetry = dict(session.telemetry)
    return (
        rows,
        names,
        list((telemetry.get("scan_sources") or {}).values()),
        dict(telemetry.get("scan_residual_reasons") or {}),
    )


def test_pruning_every_file_stays_on_the_native_scan():
    """`seq IS NULL` where no file has a null `seq` eliminates all four files. The
    scan must still be admitted natively — an empty read-set is a scan with nothing
    to read, not a scan that cannot be planned."""
    dataset, ds_dir = _with_dataset("_tmp_bounds_prune_all")
    try:
        rows, names, sources, reasons = _scan_census(
            f"SELECT seq, label FROM {dataset} WHERE seq IS NULL"
        )
        assert rows == [], rows
        # Schema visibility survives the empty result — an empty scan still has
        # to say WHICH columns it produced none of.
        assert names == [b"seq", b"label"], names
        assert sources == ["NativeParquetScanSource"], sources
        assert reasons == {}, reasons
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_pruning_every_file_counts_zero_natively():
    """The aggregate form: COUNT(*) over a fully-pruned manifest is one row valued
    0, computed by the native sink over an input that produced no morsels."""
    dataset, ds_dir = _with_dataset("_tmp_bounds_prune_all_count")
    try:
        rows, _, sources, reasons = _scan_census(
            f"SELECT COUNT(*) FROM {dataset} WHERE seq IS NULL"
        )
        assert rows == [(0,)], rows
        assert sources == ["NativeParquetScanSource"], sources
        assert reasons == {}, reasons
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)
