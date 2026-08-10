# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
`-0.0` must not exist past ingestion, on ANY path into the engine.

draken/ops/float_ops.h, architect-locked 2026-05-22:

    CANONICALIZATION (applied at ingestion AND at arithmetic output):
      -0.0 -> +0.0 (fp_canon) … After canon: hash uses raw bits directly.
    -0.0 == 0.0: true (canonicalized away at ingestion).

The second line is a contract between two subsystems that do NOT agree on their
own. Comparison uses `fp_total_eq`, which follows IEEE and says the two zeros are
equal. Hashing, grouping, DISTINCT and the set operations key on RAW BITS, which
they are allowed to do only because canonicalisation is supposed to have removed
the difference first. When it has not, the engine contradicts itself: `f = 0.0`
matches every zero while `GROUP BY f` splits them into two groups, and
`X EXCEPT ALL X` — empty by definition for any X — returns rows.

Only the nanobind constructors canonicalised until 2026-08-09. Every reader
(Parquet, CSV, JSONL) let a stored `-0.0` straight through, so a file could put
the engine into that contradictory state while a SQL literal could not.

The invariant asserted here is deliberately stated as a RELATIONSHIP rather than
as "the output is 0.0": whatever representative the engine picks, values that
compare equal must group together and must cancel in a set difference. A future
change that (say) preserved the sign but fixed the hash would still be correct
and would still pass.
"""

import json
import os
import sys
import tempfile

import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx

# testdata.fuzzing.mixed.f_special carries -0.0 and +0.0 at known positions —
# see dev/generate_fuzz_testdata.py. The counts are asserted rather than
# hardcoded per sign, so regenerating the corpus cannot silently defuse this.
MIXED = "testdata.fuzzing.mixed"
ZEROS = f"SELECT f_special FROM {MIXED} WHERE f_special = 0.0"


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(tuple(morsel[i]))
    return out


def _scalar(sql):
    rows = _rows(sql)
    assert len(rows) == 1 and len(rows[0]) == 1, f"expected one scalar from {sql!r}, got {rows!r}"
    return rows[0][0]


@pytest.fixture(scope="module")
def text_files():
    """A CSV and a JSONL file holding the same four values, two of them -0.0."""
    directory = tempfile.mkdtemp()
    values = (-0.0, 0.0, -0.0, 1.5)
    csv_path = os.path.join(directory, "z.csv")
    with open(csv_path, "w") as handle:
        handle.write("f\n" + "\n".join(repr(v) for v in values) + "\n")
    jsonl_path = os.path.join(directory, "z.jsonl")
    with open(jsonl_path, "w") as handle:
        handle.write("\n".join(json.dumps({"f": v}) for v in values) + "\n")
    return csv_path, jsonl_path


# ---------------------------------------------------------------------------
# the contradiction itself — equality versus everything that hashes
# ---------------------------------------------------------------------------


def test_equal_zeros_form_one_group():
    matched = _scalar(f"SELECT COUNT(*) AS n FROM {MIXED} WHERE f_special = 0.0")
    assert matched > 1, "the corpus no longer has multiple zeros — this test is not testing anything"

    groups = _rows(
        f"SELECT f_special, COUNT(*) AS n FROM {MIXED} WHERE f_special = 0.0 GROUP BY f_special"
    )
    assert len(groups) == 1, f"`= 0.0` matched {matched} rows but GROUP BY made {len(groups)} groups"
    assert groups[0][1] == matched, "the single group does not hold every row that compared equal"


def test_equal_zeros_are_one_distinct_value():
    assert _scalar(f"SELECT COUNT(DISTINCT f_special) AS n FROM {MIXED} WHERE f_special = 0.0") == 1


def test_set_difference_of_a_relation_with_itself_is_empty():
    # `X EXCEPT ALL X` is empty for every X. It returned 14 rows while -0.0 and
    # +0.0 hashed apart — the sharpest form of the bug, because the query
    # disagrees with itself rather than with an expectation.
    assert _scalar(
        f"SELECT COUNT(*) AS n FROM (SELECT f_special FROM {MIXED} "
        f"EXCEPT ALL SELECT f_special FROM {MIXED}) AS t"
    ) == 0


def test_set_intersection_of_a_relation_with_itself_is_itself():
    total = _scalar(f"SELECT COUNT(*) AS n FROM {MIXED}")
    assert _scalar(
        f"SELECT COUNT(*) AS n FROM (SELECT f_special FROM {MIXED} "
        f"INTERSECT ALL SELECT f_special FROM {MIXED}) AS t"
    ) == total


# ---------------------------------------------------------------------------
# every ingestion path, not just the one the bug was found on
# ---------------------------------------------------------------------------


def test_sql_literal_is_canonical():
    # This path was ALWAYS correct; it is the reference the readers must match.
    assert repr(_scalar("SELECT CAST(-0.0 AS FLOAT64) AS z")) == "0.0"


def test_parquet_reader_is_canonical():
    assert {repr(row[0]) for row in _rows(ZEROS)} == {"0.0"}


def test_csv_reader_is_canonical(text_files):
    csv_path, _ = text_files
    rows = _rows(f"SELECT f FROM READ_CSV('{csv_path}')")
    assert {repr(v) for (v,) in rows if v == 0.0} == {"0.0"}
    groups = _rows(
        f"SELECT f, COUNT(*) AS n FROM READ_CSV('{csv_path}') WHERE f = 0.0 GROUP BY f"
    )
    assert groups == [(0.0, 3)], groups


def test_jsonl_reader_is_canonical(text_files):
    _, jsonl_path = text_files
    rows = _rows(f"SELECT f FROM READ_JSONL('{jsonl_path}')")
    assert {repr(v) for (v,) in rows if v == 0.0} == {"0.0"}
    groups = _rows(
        f"SELECT f, COUNT(*) AS n FROM READ_JSONL('{jsonl_path}') WHERE f = 0.0 GROUP BY f"
    )
    assert groups == [(0.0, 3)], groups


# ---------------------------------------------------------------------------
# the fix must not have touched the OTHER special values
# ---------------------------------------------------------------------------


def test_nan_and_infinities_are_unaffected():
    # fp_canon also folds NaN payloads, and it must leave ±inf and the NaN/finite
    # ORDER intact: NaN still ranks above every value, -inf below.
    maximum = _scalar(f"SELECT MAX(f_special) AS m FROM {MIXED}")
    assert maximum != maximum, "MAX over a NaN-bearing column must be NaN (NaN ranks highest)"
    assert _scalar(f"SELECT MIN(f_special) AS m FROM {MIXED}") == float("-inf")

    # The three predicate buckets still partition the relation.
    total = _scalar(f"SELECT COUNT(*) AS n FROM {MIXED}")
    predicate = "f_special > 1000000.0"
    buckets = (
        _scalar(f"SELECT COUNT(*) AS n FROM {MIXED} WHERE {predicate}")
        + _scalar(f"SELECT COUNT(*) AS n FROM {MIXED} WHERE NOT ({predicate})")
        + _scalar(f"SELECT COUNT(*) AS n FROM {MIXED} WHERE ({predicate}) IS NULL")
    )
    assert buckets == total
