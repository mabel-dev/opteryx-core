# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Unit tests for two-pass Parquet late materialization on the native
`LatmatScanSource` (src/cpp/engine/native_latmat_scan_source.hpp; planned by
compiler.py `_latmat_scan_plan`).

  Pass 1  — decode only the predicate columns plus the top-n sort key.
  Reduce  — keep only survivors at least as good as the LIMIT boundary.
  Pass 2  — decode the remaining projected columns, masked to those rows.

The Source is chosen only for `WHERE <pushed predicate> ORDER BY <col> LIMIT n`
with a non-empty pass-2 column set and the feature flag on; every other shape takes
the single-pass `NativeParquetScanSource`. Selection is asserted through
`scan_sources`. LatmatScanSource records no per-pass row-group counters, so the
pass-1 / pass-2 / skip / abandonment counter tests that existed for the deleted
Python trampoline implementation are gone with it.

Correctness is checked against an independent plain-Python oracle: the dataset is
read through rugo and WHERE / ORDER BY / LIMIT / GROUP BY evaluated in Python.

Dataset notes (testdata/clickbench_tiny):
  URL LIKE '%google%'  → 0 matching rows
  URL LIKE '%yandex%'  → 59 matching rows
"""

import functools
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest
import rugo.parquet

import opteryx
from opteryx import config

_TINY_DIR = os.path.join(os.path.dirname(__file__), "../../../testdata/clickbench_tiny")

# ─── helpers ──────────────────────────────────────────────────────────────────


def _contains(value, needle):
    """Python equivalent of `value LIKE '%needle%'` (URL is bytes in this file)."""
    if value is None:
        return False
    if isinstance(value, bytes):
        return needle.encode("utf-8") in value
    return needle in value


def _is_empty(value):
    return value == "" or value == b""


@functools.lru_cache(maxsize=None)
def _oracle_rows(columns=None, url_needle=None):
    """Rows of clickbench_tiny as dicts of Python values, read via rugo. `columns`
    (a tuple) None reads all columns. `url_needle` keeps only rows whose URL
    contains it (`URL LIKE '%needle%'`), applied before the row dicts are built.
    Returns (column_names, rows)."""
    files = sorted(f for f in os.listdir(_TINY_DIR) if f.endswith(".parquet"))
    assert files, f"no parquet files under {_TINY_DIR}"
    names = None
    rows = []
    for f in files:
        read_cols = None if columns is None else list(columns)
        with rugo.parquet.read_parquet(os.path.join(_TINY_DIR, f), columns=read_cols) as reader:
            for morsel in reader:
                raw = list(morsel.column_names)
                names = [n.decode() if isinstance(n, bytes) else n for n in raw]
                cols = [morsel.column(n) for n in raw]
                if url_needle is None:
                    keep = range(morsel.num_rows)
                else:
                    url = cols[names.index("URL")]
                    keep = [i for i in range(morsel.num_rows) if _contains(url[i], url_needle)]
                for i in keep:
                    rows.append({name: c[i] for name, c in zip(names, cols)})
    return names, rows


def _execute(sql, latmat=True):
    """Drain `sql` with the late-materialization flag set as requested. Returns
    (names, rows in emission order, scan_sources)."""
    config.features.parquet_late_materialization = latmat
    session = opteryx.session()
    try:
        names = []
        rows = []
        for morsel in session.execute_to_morsels(sql):
            raw = list(morsel.column_names)
            names = [n.decode() for n in raw]
            cols = [morsel.column(n) for n in raw]
            for i in range(morsel.num_rows):
                rows.append(tuple(c[i] for c in cols))
        telemetry = session.telemetry
        return names, rows, list(telemetry["scan_sources"].values())
    finally:
        session.close()


def _assert_valid_topn(rows, names, oracle_rows, sort_col, limit):
    """`rows` is a valid ascending top-`limit` of `oracle_rows` (dicts) on
    `sort_col`, NULL lowest: right count, exact key sequence, every row a real
    survivor row, none twice. Row identity among boundary ties is unspecified."""
    expect_keys = sorted((r[sort_col] for r in oracle_rows),
                         key=lambda v: (v is not None, v if v is not None else 0))[:limit]
    k = names.index(sort_col)
    assert len(rows) == len(expect_keys), (len(rows), len(expect_keys))
    assert [r[k] for r in rows] == expect_keys, ([r[k] for r in rows], expect_keys)
    universe = {tuple(r[n] for n in names) for r in oracle_rows}
    stray = [r for r in rows if r not in universe]
    assert not stray, f"{len(stray)} returned row(s) are not survivor rows: {stray[0]}"
    assert len(set(rows)) == len(rows), "a survivor row was returned twice"


# ─── test fixtures / config helpers ───────────────────────────────────────────


@pytest.fixture(autouse=True)
def _restore_latmat_config():
    """Restore the late-materialization feature flag after every test."""
    orig_flag = config.features.parquet_late_materialization
    yield
    config.features.parquet_late_materialization = orig_flag


# ─── zero-survivor tests ──────────────────────────────────────────────────────


def test_q24_no_matching_rows_on_latmat_source():
    """Q24 pattern: URL LIKE '%google%' matches 0 rows in clickbench_tiny. The
    query runs on LatmatScanSource and returns exactly what the oracle does."""
    _, oracle = _oracle_rows(("URL",), "google")
    assert len(oracle) == 0, (
        "fixture assumption broken: clickbench_tiny now contains 'google' URLs")

    _, rows, src = _execute(
        "SELECT * FROM testdata.clickbench_tiny"
        " WHERE URL LIKE '%google%'"
        " ORDER BY EventTime LIMIT 10"
    )
    assert rows == []
    assert src == ["LatmatScanSource"], src


# ─── assembly correctness tests ───────────────────────────────────────────────


def test_assembly_correctness_matching_rows_yandex():
    """LIKE '%yandex%' matches 59 rows; the top 20 by EventTime must match the
    oracle."""
    _, survivors = _oracle_rows(("URL", "EventTime", "UserID"), "yandex")
    assert survivors, "fixture assumption broken: no 'yandex' URLs"

    names, rows, src = _execute(
        "SELECT URL, EventTime, UserID"
        " FROM testdata.clickbench_tiny"
        " WHERE URL LIKE '%yandex%'"
        " ORDER BY EventTime LIMIT 20"
    )
    assert src == ["LatmatScanSource"], src
    assert names == ["URL", "EventTime", "UserID"], names
    _assert_valid_topn(rows, names, survivors, "EventTime", 20)


def test_assembly_correctness_select_star():
    """SELECT * with a selective LIKE. All 105 columns must be assembled correctly —
    in particular, pass-2 columns must land in the right positions and carry the
    values of the same row as the pass-1 columns."""
    names_o, survivors = _oracle_rows(None, "yandex")
    assert survivors, "fixture assumption broken: no 'yandex' URLs"

    names, rows, src = _execute(
        "SELECT *"
        " FROM testdata.clickbench_tiny"
        " WHERE URL LIKE '%yandex%'"
        " ORDER BY EventTime LIMIT 5"
    )
    assert src == ["LatmatScanSource"], src
    assert names == names_o, "column order must match the file's column order"
    _assert_valid_topn(rows, names, survivors, "EventTime", 5)


# ─── Source selection (eligibility) tests ─────────────────────────────────────


def test_two_pass_inactive_when_no_predicate():
    """Without a WHERE clause there are no pass-1 columns, so the fused top-n scan
    is not late-materialized: it runs on the single-pass Source."""
    _, _, src = _execute(
        "SELECT URL, EventTime FROM testdata.clickbench_tiny ORDER BY EventTime LIMIT 5")
    assert src == ["NativeParquetScanSource"], src


def test_two_pass_inactive_when_all_projected_columns_in_filter():
    """SELECT URL ... WHERE URL LIKE ... ORDER BY URL — every projected column is a
    pass-1 column, so there is nothing for pass 2 and the scan is single-pass."""
    _, _, src = _execute(
        "SELECT URL FROM testdata.clickbench_tiny WHERE URL LIKE '%yandex%'"
        " ORDER BY URL LIMIT 5"
    )
    assert src == ["NativeParquetScanSource"], src


def test_two_pass_inactive_when_feature_flag_disabled():
    """With the late-materialization feature flag off, an otherwise eligible query
    runs on the single-pass Source."""
    _, _, src = _execute(
        "SELECT * FROM testdata.clickbench_tiny"
        " WHERE URL LIKE '%google%'"
        " ORDER BY EventTime LIMIT 10",
        latmat=False,
    )
    assert src == ["NativeParquetScanSource"], src


# ─── regression / non-interference tests ──────────────────────────────────────


@pytest.mark.parametrize("latmat", [True, False])
def test_non_like_predicate_not_affected(latmat):
    """A plain inequality predicate (AdvEngineID <> 0) answers what the oracle does,
    with the feature on and off."""
    _, oracle = _oracle_rows(("AdvEngineID",))
    expect = sum(1 for r in oracle
                 if r["AdvEngineID"] is not None and r["AdvEngineID"] != 0)
    _, rows, _ = _execute(
        "SELECT COUNT(*) FROM testdata.clickbench_tiny WHERE AdvEngineID <> 0", latmat)
    assert rows == [(expect,)], (rows, expect)


@pytest.mark.parametrize("latmat", [True, False])
def test_aggregate_query_not_affected(latmat):
    """A GROUP BY query without LIKE predicates answers what the oracle does, with
    the feature on and off. Groups tied on COUNT at the LIMIT boundary may come back
    in either identity, so the count sequence is exact and each (UserID, count) pair
    must be a real group."""
    _, oracle = _oracle_rows(("UserID",))
    counts = {}
    for r in oracle:
        counts[r["UserID"]] = counts.get(r["UserID"], 0) + 1
    expect_counts = sorted(counts.values(), reverse=True)[:3]

    _, rows, _ = _execute(
        "SELECT UserID, COUNT(*)"
        " FROM testdata.clickbench_tiny"
        " GROUP BY UserID"
        " ORDER BY COUNT(*) DESC LIMIT 3",
        latmat,
    )
    assert [r[1] for r in rows] == expect_counts, (rows, expect_counts)
    assert all(counts.get(uid) == c for uid, c in rows), rows
    assert len({r[0] for r in rows}) == len(rows), rows


@pytest.mark.parametrize("latmat", [True, False])
def test_q28_like_with_group_by_and_limit(latmat):
    """Q28 shape (URL LIKE '%google%' + GROUP BY + LIMIT) answers what the oracle
    does, with the feature on and off."""
    _, oracle = _oracle_rows(("URL", "SearchPhrase"), "google")
    groups = {}
    for r in oracle:
        if r["SearchPhrase"] is not None and not _is_empty(r["SearchPhrase"]):
            mn, c = groups.get(r["SearchPhrase"], (None, 0))
            url = r["URL"]
            groups[r["SearchPhrase"]] = (url if mn is None or url < mn else mn, c + 1)
    expect = sorted(((p, mn, c) for p, (mn, c) in groups.items()),
                    key=lambda t: t[2], reverse=True)[:10]

    _, rows, _ = _execute(
        "SELECT SearchPhrase, MIN(URL), COUNT(*) AS c"
        " FROM testdata.clickbench_tiny"
        " WHERE URL LIKE '%google%' AND SearchPhrase <> ''"
        " GROUP BY SearchPhrase"
        " ORDER BY c DESC LIMIT 10",
        latmat,
    )
    assert [r[2] for r in rows] == [t[2] for t in expect], (rows, expect)
    assert all(r[0] in groups and groups[r[0]] == (r[1], r[2]) for r in rows), (
        rows, expect)
