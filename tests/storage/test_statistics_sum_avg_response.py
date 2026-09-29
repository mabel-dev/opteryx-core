# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
SUM / AVG answered from the manifest's EXACT sums (StatisticsOnlyResponseStrategy,
docs/MANIFEST_SUM_STATISTIC_DESIGN.md P2).

Every answer here is checked TWICE: against the value computed in Python from the
data, AND for whether the statistics path fired (session telemetry) - an answer
that is right because the scan ran proves nothing about the rewrite, and a
rewrite that fires with a wrong answer is the failure this exists to catch.

  - integer columns at every width and signedness are answered, bit-identical
    to the engine (AVG's double included), NULLs excluded, zero valid rows NULL;
  - a SUM whose total leaves INT64 is NOT answered - the scan runs and the
    engine raises its own overflow (ruling D5: the statistic never decides it);
  - floats, DISTINCT, inline filters, expressions, and any file without a
    trusted sum (a foreign writer's) leave the plan to the scan;
  - merge-on-read deletes make the total unknown.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import io
import os
import shutil
import sys
from pathlib import Path

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pyarrow
import pyarrow.parquet as pq
import pytest

import opteryx
from rugo.parquet import write_parquet

_DATASET = "sum_avg_response"
_DIR = Path("testdata") / _DATASET

_BIG = 18446744073709000000   # UINT64 values above INT64_MAX
_SOURCE = (
    "SELECT CAST(g % 100 AS INT8) - 50 AS i8, "
    "CAST(g AS INT16) AS i16, "
    "CAST(g AS INT32) * 1000 AS i32, "
    "CAST(g AS INT64) - 100000 AS i64, "
    "CAST(g % 200 AS UINT8) AS u8, "
    "CAST(g AS UINT16) AS u16, "
    "CAST(g AS UINT32) AS u32, "
    "CASE WHEN g % 4 = 0 THEN NULL ELSE CAST(g AS UINT64) + CAST({big} AS UINT64) END AS u64, "
    "CASE WHEN g % 7 = 0 THEN NULL ELSE CAST(g AS INT32) END AS nullable, "
    "CAST(NULL AS INT32) AS all_null, "
    "CAST(g AS FLOAT64) AS f "
    "FROM generate_series({a}, {b}) AS g"
)
_RANGES = [(1, 1000), (1001, 5000)]
_G = range(1, 5001)


def _run(sql):
    """(rows, whether the statistics-only rewrite fired)."""
    session = opteryx.session()
    rows = [tuple(m[i]) for m in session.execute_to_morsels(sql) for i in range(m.num_rows)]
    return rows, bool(session.telemetry.get("optimization_statistics_only_response"))


@pytest.fixture(scope="module", autouse=True)
def _dataset():
    if _DIR.exists():
        shutil.rmtree(_DIR)
    _DIR.mkdir(parents=True)
    session = opteryx.session()
    for i, (a, b) in enumerate(_RANGES):
        morsel = list(session.execute_to_morsels(_SOURCE.format(big=_BIG, a=a, b=b)))[0]
        with open(_DIR / f"part-{i}.parquet", "wb") as handle:
            handle.write(write_parquet(morsel))
    yield
    shutil.rmtree(_DIR)


def _avg(values):
    values = [v for v in values if v is not None]
    return float(sum(values)) / float(len(values))


_COLUMNS = {
    "i8": [(g % 100) - 50 for g in _G],
    "i16": list(_G),
    "i32": [g * 1000 for g in _G],
    "i64": [g - 100000 for g in _G],
    "u8": [g % 200 for g in _G],
    "u16": list(_G),
    "u32": list(_G),
    "u64": [None if g % 4 == 0 else g + _BIG for g in _G],
    "nullable": [None if g % 7 == 0 else g for g in _G],
}


@pytest.mark.parametrize("column", sorted(_COLUMNS))
def test_avg_of_every_integer_width_and_sign_is_answered_exactly(column):
    rows, fired = _run(f"SELECT AVG({column}) FROM testdata.{_DATASET}")
    assert fired, f"AVG({column}) was not answered from the manifest"
    # == on the double: the answer must be the engine's, not merely close
    assert rows == [(_avg(_COLUMNS[column]),)]


@pytest.mark.parametrize("column", sorted(c for c in _COLUMNS if c != "u64"))
def test_sum_of_every_integer_width_and_sign_is_answered_exactly(column):
    rows, fired = _run(f"SELECT SUM({column}) FROM testdata.{_DATASET}")
    assert fired, f"SUM({column}) was not answered from the manifest"
    assert rows == [(sum(v for v in _COLUMNS[column] if v is not None),)]


def test_sum_avg_mix_with_count_min_max_in_one_answer():
    rows, fired = _run(
        f"SELECT SUM(i8), COUNT(*), AVG(i32), MIN(i64), MAX(i16), COUNT(nullable) FROM testdata.{_DATASET}"
    )
    assert fired
    assert rows == [
        (
            sum(_COLUMNS["i8"]),
            5000,
            _avg(_COLUMNS["i32"]),
            min(_COLUMNS["i64"]),
            max(_COLUMNS["i16"]),
            len([v for v in _COLUMNS["nullable"] if v is not None]),
        )
    ]


def test_sum_of_column_plus_constant_is_answered_through_the_planners_lowering():
    # The planner lowers SUM(x + k) to base aggregates over the bare column
    # before this strategy runs, so the manifest answers it - and must answer
    # it exactly.
    rows, fired = _run(f"SELECT SUM(i16 + 1) FROM testdata.{_DATASET}")
    assert fired
    assert rows == [(sum(g + 1 for g in _G),)]


def test_a_column_with_no_values_answers_null():
    rows, fired = _run(f"SELECT SUM(all_null), AVG(all_null) FROM testdata.{_DATASET}")
    assert fired
    assert rows == [(None, None)]


def test_a_sum_past_int64_is_left_to_the_engine():
    # The exact total is known, but SUM's output is INT64: the statistics path
    # declines and the engine raises its own overflow - never a wrapped value,
    # never an answer the engine would not give.
    session = opteryx.session()
    with pytest.raises(Exception, match="SUM overflow"):
        list(session.execute_to_morsels(f"SELECT SUM(u64) FROM testdata.{_DATASET}"))
    assert not session.telemetry.get("optimization_statistics_only_response")


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT SUM(f) FROM testdata.{d}", float(sum(_G))),
        ("SELECT SUM(DISTINCT u8) FROM testdata.{d}", sum(set(g % 200 for g in _G))),
        ("SELECT SUM(i16 WHERE i16 > 4000) FROM testdata.{d}", sum(g for g in _G if g > 4000)),
        ("SELECT SUM(i16) FROM testdata.{d} WHERE i16 > 4000", sum(g for g in _G if g > 4000)),
    ],
)
def test_shapes_the_statistics_cannot_answer_are_scanned(sql, expected):
    rows, fired = _run(sql.format(d=_DATASET))
    assert not fired
    assert rows == [(expected,)]


def test_a_foreign_writers_file_makes_the_sum_unknown(tmp_path):
    # One rugo file with sums, one PyArrow file without: the total is unknown,
    # so the scan answers - and gets the right number.
    ds = Path("testdata") / "sum_avg_foreign"
    if ds.exists():
        shutil.rmtree(ds)
    ds.mkdir(parents=True)
    try:
        session = opteryx.session()
        morsel = list(session.execute_to_morsels("SELECT CAST(g AS INT32) AS v FROM generate_series(1, 100) AS g"))[0]
        with open(ds / "rugo.parquet", "wb") as handle:
            handle.write(write_parquet(morsel))
        pq.write_table(pyarrow.table({"v": pyarrow.array(range(101, 201), pyarrow.int32())}), ds / "arrow.parquet")
        rows, fired = _run("SELECT SUM(v), AVG(v) FROM testdata.sum_avg_foreign")
        assert not fired
        assert rows == [(sum(range(1, 201)), _avg(range(1, 201)))]
    finally:
        shutil.rmtree(ds)


def test_deletes_make_the_total_unknown():
    from opteryx.compiled.planner.native_manifest import NativeManifestBuilder, decode_manifest_parquet
    from opteryx.types.logical_type import DrakenType as DT

    builder = NativeManifestBuilder(("v",), (DT.INT32,), True, True)
    for i in range(2):
        row = builder.add_file(f"f{i}.parquet", "PARQUET", 10, 1)
        builder.set_counts(row, 0, null_count=0)
        builder.set_sum(row, 0, 55)
    manifest = builder.build({})
    assert manifest.total_sum(0) == 110

    table = pq.read_table(io.BytesIO(manifest.to_parquet()))
    deleted = table.column("deleted_record_count").to_pylist()
    deleted[1] = 3
    index = table.schema.get_field_index("deleted_record_count")
    table = table.set_column(index, "deleted_record_count", pyarrow.array(deleted, pyarrow.int64()))
    sink = io.BytesIO()
    pq.write_table(table, sink)
    with_deletes = decode_manifest_parquet(sink.getvalue(), ("v",), (DT.INT32,), {}, True, True)
    assert with_deletes.has_deletes()
    assert with_deletes.total_sum(0) is None


if __name__ == "__main__":  # pragma: no cover
    sys.exit(pytest.main([__file__, "-v"]))
