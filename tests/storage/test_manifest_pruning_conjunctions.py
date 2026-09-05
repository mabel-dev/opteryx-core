# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
End-to-end: `Manifest.prune_files` must split a conjunction itself.

`prune_files` matches only `column <op> literal` and `column BETWEEN literal AND
literal`. Handed an AND node whole it matches NEITHER, so a predicate pairing a
prunable range with an unprunable arm - the shape every incremental rollup over
gdelt_events has, `<column> IS NOT NULL AND date_added >= <ts>` - prunes zero
files even though the range arm alone would drop nearly all of them.

Why this was invisible: SplitConjunctivePredicatesStrategy normally gives every
conjunct its own Filter node before pruning runs, so prune_files was handed the
arms pre-split and looked correct. That strategy has a kill switch and does not
run over filters synthesized after it, so file pruning was silently conditional
on an unrelated optimization. These tests therefore run with
`disable_split_conjunctive_predicates` FORCED ON: with it off, every assertion
here passes on the unfixed code and the test proves nothing.

The direction rule is the whole point, and the two halves are asserted together:

  * AND - pruning on ANY single arm is sound. Every conjunct must hold, so a
    file that provably fails one cannot satisfy the predicate. An arm that
    cannot be evaluated is "no information", never "false" - the file survives
    on its other arms.
  * OR  - pruning on any single arm is a WRONG ANSWER. A file need satisfy only
    one disjunct, so dropping it on the strength of one arm deletes rows the
    query must return. `_inner_split` does not descend through OR, so a
    disjunction reaches prune_files whole and prunes nothing.

Every test asserts rows are IDENTICAL to the same query with pruning disabled -
a pruning change that alters results is a bug, not an optimization - AND that
the expected number of files was actually skipped. Row equality alone would pass
for a strategy that never fired at all.
"""

import os
import shutil
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx import config
from rugo.parquet import write_parquet

# Four files, disjoint and ordered on `seq` - the shape that makes file pruning
# both possible and observable. `label` carries a NULL in the second file so
# `label IS NOT NULL` is a real filter, not a no-op that could hide a wrong
# answer behind an unchanged row count.
FILES = [
    "SELECT * FROM (VALUES (1,'a'),(2,'b'),(3,'c')) AS t(seq, label)",
    "SELECT * FROM (VALUES (11,'d'),(12,'e'),(13,NULL)) AS t(seq, label)",
    "SELECT * FROM (VALUES (21,'f'),(22,'g'),(23,'h')) AS t(seq, label)",
    "SELECT * FROM (VALUES (31,'i'),(32,'j'),(33,'k')) AS t(seq, label)",
]

ALL_ROWS = [
    (1, "a"), (2, "b"), (3, "c"),
    (11, "d"), (12, "e"), (13, None),
    (21, "f"), (22, "g"), (23, "h"),
    (31, "i"), (32, "j"), (33, "k"),
]


def _write_dataset(dir_name):
    """One Parquet file per entry in FILES under testdata/<dir_name>/, then
    ANALYZE - the bounds under test are the ones a catalog actually produces."""
    ds_dir = Path("testdata") / dir_name
    if ds_dir.exists():
        shutil.rmtree(ds_dir)
    ds_dir.mkdir(parents=True)
    session = opteryx.session()
    for index, sql in enumerate(FILES):
        morsel = list(session.execute_to_morsels(sql))[0]
        with open(ds_dir / f"part-{index}.parquet", "wb") as file:
            file.write(write_parquet(morsel))
    dataset = f"testdata.{dir_name}"
    list(session.execute_to_morsels(f"ANALYZE TABLE {dataset}"))
    return dataset, ds_dir


def _run(sql):
    """Rows (sorted) and the files_pruned telemetry for one query."""
    session = opteryx.session()
    rows = []
    for morsel in session.execute_to_morsels(sql):
        rows.extend(zip(morsel.column(b"seq").to_pylist(), morsel.column(b"label").to_pylist()))
    return sorted(rows), dict(session.telemetry).get("files_pruned", 0)


def _check(dataset, predicate, expected_rows, expected_pruned):
    sql = f"SELECT seq, label FROM {dataset} WHERE {predicate}"

    # Splitting upstream is what masked the defect; force it off so this
    # exercises prune_files' OWN handling of the conjunction.
    original_split = config.features.disable_split_conjunctive_predicates
    original_prune = config.features.disable_manifest_pruning
    try:
        config.features.disable_split_conjunctive_predicates = True

        config.features.disable_manifest_pruning = False
        on_rows, pruned = _run(sql)

        config.features.disable_manifest_pruning = True
        off_rows, off_pruned = _run(sql)
    finally:
        config.features.disable_split_conjunctive_predicates = original_split
        config.features.disable_manifest_pruning = original_prune

    assert on_rows == sorted(expected_rows), (predicate, on_rows)
    assert on_rows == off_rows, (predicate, on_rows, off_rows)
    assert off_pruned == 0, (predicate, off_pruned)
    assert pruned == expected_pruned, (predicate, pruned, expected_pruned)


def test_and_prunes_on_the_evaluable_arm_alone():
    """`IS NOT NULL AND <range>` - the unprunable arm must not disqualify the
    conjunction. This is the case that pruned nothing."""
    dataset, ds_dir = _write_dataset("_tmp_prune_and_mixed")
    try:
        _check(
            dataset,
            "label IS NOT NULL AND seq >= 21",
            [row for row in ALL_ROWS if row[1] is not None and row[0] >= 21],
            expected_pruned=2,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_and_prunes_when_the_unprunable_arm_is_a_disjunction():
    """An OR nested inside an AND is one conjunct prune_files cannot evaluate.
    It must be ignored - not expanded, not treated as false - while the sibling
    range arm still prunes."""
    dataset, ds_dir = _write_dataset("_tmp_prune_and_nested_or")
    try:
        _check(
            dataset,
            "seq >= 21 AND (label = 'f' OR label = 'j')",
            [row for row in ALL_ROWS if row[0] >= 21 and row[1] in ("f", "j")],
            expected_pruned=2,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_and_of_two_evaluable_arms_prunes_on_both():
    """Both arms evaluable, over DIFFERENT columns so the conjunction survives
    to prune_files as an AND (see the BETWEEN test below for why same-column
    ranges do not). Each arm eliminates files the other keeps: `seq >= 21`
    drops the two low files, `label >= 'i'` drops the third."""
    dataset, ds_dir = _write_dataset("_tmp_prune_and_two_columns")
    try:
        _check(
            dataset,
            "seq >= 21 AND label >= 'i'",
            [row for row in ALL_ROWS if row[0] >= 21 and row[1] is not None and row[1] >= "i"],
            expected_pruned=3,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_same_column_window_is_rewritten_to_between():
    """Two ranges over ONE column never reach prune_files as an AND - they are
    folded into a BETWEEN node, which prune_files matches directly. Recorded
    here so the conjunction tests above are not read as covering this path.

    The exclusive form prunes one file FEWER: BETWEEN carries inclusive/
    exclusive flags, and prune_files' bounds test ignores them and treats both
    ends as inclusive. That is conservative in the safe direction - the file
    whose min is exactly the exclusive upper bound is read and then filtered,
    costing a read, never an answer - so both forms return identical rows."""
    dataset, ds_dir = _write_dataset("_tmp_prune_between")
    try:
        window = [row for row in ALL_ROWS if 11 <= row[0] < 21]
        _check(dataset, "seq >= 11 AND seq <= 20", window, expected_pruned=3)
        _check(dataset, "seq >= 11 AND seq < 21", window, expected_pruned=2)
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_or_prunes_nothing_and_keeps_every_matching_row():
    """The trap. Each arm alone would eliminate files holding rows the other arm
    matches; pruning on either would silently drop them."""
    dataset, ds_dir = _write_dataset("_tmp_prune_or")
    try:
        _check(
            dataset,
            "seq < 11 OR seq >= 31",
            [row for row in ALL_ROWS if row[0] < 11 or row[0] >= 31],
            expected_pruned=0,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


def test_or_with_an_unprunable_arm_prunes_nothing():
    """`IS NULL OR <range>`: the range arm is evaluable, but using it would drop
    the file holding the NULL row."""
    dataset, ds_dir = _write_dataset("_tmp_prune_or_mixed")
    try:
        _check(
            dataset,
            "label IS NULL OR seq >= 31",
            [row for row in ALL_ROWS if row[1] is None or row[0] >= 31],
            expected_pruned=0,
        )
    finally:
        shutil.rmtree(ds_dir, ignore_errors=True)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
