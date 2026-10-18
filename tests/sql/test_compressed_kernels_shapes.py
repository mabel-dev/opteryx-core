# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Compression-aware kernels across operand SHAPES.

A compression-aware kernel processes a COMPRESSED operand (draken_is_compressed —
a constant or a dictionary) over its value array, once per value, and keeps the
operand's encoding in its result; a DENSE operand runs the kernel's row loop. The
two must give the same answer, so every expression runs over a dictionary-encoded
parquet file (dict vectors, plus a constant row group) and a plain-encoded copy
(dense vectors), and the results are compared row by row on `rid`. The dense run
is the oracle: it is the row loop every kernel had before the compressed path.

Each fixture asserts the shapes it promises actually reach the engine, so a reader
change that stops producing compressed vectors fails here instead of silently
reducing this to a dense-only test.
"""

import datetime
import decimal
import math
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from opteryx.connectors import DiskConnector

N = 4000
RG = 1000  # the last row group holds one value per CONSTANT column
WORDS = ["MAIL", "SHIP", "", "REG AIR", "TRUCK", "", "RAIL", "naïve", "FOB"]
BASE_DATE = datetime.date(2023, 11, 20)
BASE_TS = datetime.datetime(2024, 2, 28, 22, 59, 30)
INT64_MIN = -(2**63)
UUIDS = [bytes([k] * 16) for k in (1, 7, 42, 255)]
JSON_DOCS = ['{"a": 1}', "[1, 2]", '"x"', '{"a": {"b": 2}}', '{"c": null}', "3.5"]


def _const_tail(values, value):
    values[N - RG :] = [value] * RG
    return values


def _columns():
    return {
        "rid": list(range(N)),
        "s": _const_tail([None if r % 37 == 0 else WORDS[r % 9] for r in range(N)], "SHIP"),
        "i": _const_tail([None if r % 41 == 0 else (r % 9) - 4 for r in range(N)], 7),
        "j": [None if r % 43 == 0 else ((r % 11) - 5) * 1_000_003 for r in range(N)],
        "jm": [INT64_MIN if r % 500 == 7 else (r % 11) - 5 for r in range(N)],
        "f": _const_tail(
            [None if r % 53 == 0 else (math.nan if r % 13 == 0 else (r % 6) / 2 - 1.25) for r in range(N)], 2.5
        ),
        "m": [None if r % 59 == 0 else decimal.Decimal((r % 7) * 37 - 100) / 40 for r in range(N)],
        "u": [r % 5 for r in range(N)],
        "b": [None if r % 67 == 0 else r % 3 == 0 for r in range(N)],
        "js": [None if r % 71 == 0 else JSON_DOCS[r % len(JSON_DOCS)] for r in range(N)],
        "u64": [None if r % 73 == 0 else (r % 6) * 3_000_000_000_000_000_000 for r in range(N)],
        "m38": [None if r % 79 == 0 else decimal.Decimal((r % 7) * 37 - 100) / 40 for r in range(N)],
        "id": [None if r % 83 == 0 else UUIDS[r % len(UUIDS)] for r in range(N)],
        "d": _const_tail(
            [None if r % 47 == 0 else BASE_DATE + datetime.timedelta(days=(r % 8) * 97) for r in range(N)],
            BASE_DATE,
        ),
        "t": _const_tail(
            [
                None
                if r % 61 == 0
                else BASE_TS + datetime.timedelta(seconds=(r % 10) * 37_261, microseconds=(r % 3) * 250_000)
                for r in range(N)
            ],
            BASE_TS,
        ),
    }


SCHEMA = pa.schema(
    [
        ("rid", pa.int64()),
        ("s", pa.string()),
        ("i", pa.int16()),
        ("j", pa.int64()),
        ("jm", pa.int64()),
        ("f", pa.float64()),
        ("m", pa.decimal128(9, 3)),
        ("u", pa.uint8()),
        ("b", pa.bool_()),
        ("js", pa.string()),
        ("u64", pa.uint64()),
        ("m38", pa.decimal128(30, 3)),
        ("id", pa.uuid()),
        ("d", pa.date32()),
        ("t", pa.timestamp("us")),
    ]
)

SHAPED = ["s", "i", "j", "f", "m", "u", "d", "t", "js", "u64", "m38", "id"]
CONSTANT = ["s", "i", "f", "d", "t"]


@pytest.fixture(scope="module")
def workspaces(tmp_path_factory):
    root = tmp_path_factory.mktemp("compressed_kernels")
    cols = _columns()
    table = pa.table({name: pa.array(cols[name], SCHEMA.field(name).type) for name in SCHEMA.names})
    for ws, use_dictionary in (("cmk_dict", True), ("cmk_plain", False)):
        path = root / ws / "t"
        path.mkdir(parents=True)
        pq.write_table(table, str(path / "data.parquet"), use_dictionary=use_dictionary, row_group_size=RG)
    cwd = os.getcwd()
    os.chdir(root)
    opteryx.register_workspace("cmk_dict", DiskConnector)
    opteryx.register_workspace("cmk_plain", DiskConnector)
    yield cols
    os.chdir(cwd)


def _shapes(ws):
    seen = {name: set() for name in SHAPED}
    sql = f"SELECT {', '.join(SHAPED)} FROM {ws}.t"
    for morsel in opteryx.session().execute_to_morsels(sql):
        for name in SHAPED:
            vec = morsel.column(name.encode())._nb
            seen[name].add("constant" if vec.is_constant else "dict" if vec.is_dict else "dense")
    return seen


def test_dict_fixture_reaches_engine_compressed(workspaces):
    seen = _shapes("cmk_dict")
    for name in SHAPED:
        assert "dict" in seen[name], f"{name} never arrived dict-shaped: {seen[name]}"
    for name in CONSTANT:
        assert "constant" in seen[name], f"{name} never arrived constant-shaped: {seen[name]}"


def test_plain_fixture_reaches_engine_dense(workspaces):
    seen = _shapes("cmk_plain")
    for name in SHAPED:
        assert seen[name] == {"dense"}, f"{name} arrived {seen[name]} from the plain file"


def _rows(ws, expr, where=None):
    sql = f"SELECT rid, {expr} AS x FROM {ws}.t" + (f" WHERE {where}" if where else "")
    out = {}
    for morsel in opteryx.session().execute_to_morsels(sql):
        for rid, x in zip(morsel.column(b"rid").to_pylist(), morsel.column(b"x").to_pylist()):
            assert rid not in out
            out[rid] = x
    return out


def _same(a, b):
    if isinstance(a, float) and isinstance(b, float) and math.isnan(a) and math.isnan(b):
        return True
    return a == b and type(a) is type(b)


def _assert_same(expr, got, want):
    assert got.keys() == want.keys(), f"{expr}: row sets differ"
    bad = [rid for rid in want if not _same(got[rid], want[rid])]
    assert not bad, f"{expr}: {len(bad)} rows differ, e.g. rid {bad[0]}: {got[bad[0]]!r} vs {want[bad[0]]!r}"


PROJECTIONS = [
    # numeric
    "ABS(i)", "ABS(j)", "ABS(f)", "ABS(m)", "ABS(u)",
    "SIGN(i)", "SIGN(f)", "SIGN(u)", "SIGN(m)",
    "SQRT(f)", "SQRT(m)", "SQRT(u)",
    "ROUND(f)", "ROUND(f, 1)", "FLOOR(f)", "CEILING(f, 2)", "ROUND(i)",
    "ROUND(m)", "ROUND(m, 1)", "FLOOR(m)", "CEILING(m, 2)",
    "~i", "~j",
    # temporal
    "EXTRACT(year FROM d)", "EXTRACT(month FROM d)", "EXTRACT(day FROM d)", "EXTRACT(quarter FROM d)",
    "EXTRACT(year FROM t)", "EXTRACT(hour FROM t)", "EXTRACT(minute FROM t)", "EXTRACT(second FROM t)",
    "TRUNC(t, 'second')", "TRUNC(t, 'minute')", "TRUNC(t, 'hour')", "TRUNC(t, 'day')",
    "TRUNC(t, 'week')", "TRUNC(t, 'month')", "TRUNC(t, 'quarter')", "TRUNC(t, 'year')",
    # string
    "LENGTH(s)",
    # temporal casts
    "CAST(d AS TIMESTAMP)", "CAST(t AS DATE)", "t::TIMESTAMP[s]", "t::TIMESTAMP[ms]",
    "CAST(d AS TIMESTAMP)::TIMESTAMP[s]", "j::TIMESTAMP[s]", "j::TIMESTAMP[ms]",
    # decimal casts
    "CAST(m AS DECIMAL(12, 5))", "CAST(m AS DECIMAL(30, 4))", "CAST(m AS INTEGER)", "CAST(m AS FLOAT64)",
    "CAST(m AS FLOAT32)", "CAST(m AS INT16)", "CAST(m AS INT8)", "TRY_CAST(m AS UINT8)",
    "TRY_CAST(m AS DECIMAL(4, 1))", "CAST(CAST(m AS DECIMAL(30, 4)) AS DECIMAL(9, 3))",
    "CAST(i AS DECIMAL(8, 2))", "CAST(j AS DECIMAL(20, 1))", "CAST(u AS DECIMAL(5, 1))",
    "CAST(f AS DECIMAL(10, 3))", "CAST(b AS DECIMAL(3, 1))", "TRY_CAST(j AS DECIMAL(3, 1))",
    # misc casts
    "CAST(i AS FLOAT32)", "CAST(u AS FLOAT32)", "CAST(b AS VARCHAR)",
    # string functions
    "OCTET_LENGTH(s)", "POSITION('A' IN s)", "POSITION('' IN s)", "LEVENSHTEIN(s, 'MAIL')",
    "LEVENSHTEIN('TRUCK', s)", "TO_ASCII(s)", "TO_CHAR(u + 65)", "s[1]", "s[-1]",
    # numeric functions with a constant operand
    "POWER(f, 2)", "POWER(2, i)", "POWER(m, 2)", "LOG(f, 10)", "LOG(u, 2)",
    "TRUNC(f)", "TRUNC(f, 1)", "TRUNC(m)", "TRUNC(m, 1)",
    # temporal functions
    "UNIXTIME(t)", "UNIXTIME(d)", "TIME_BUCKET(2, 'hour', t)", "TIME_BUCKET(1, 'month', d)",
    "TIME_BUCKET(3, 'day', t)", "FORMAT_TIMESTAMP('%Y-%m-%d %H', t)", "FORMAT_TIMESTAMP('%Y', d)",
    "DATEDIFF('days', d, '2025-01-01'::DATE)", "DATEDIFF('hours', '2024-01-01'::TIMESTAMP, t)",
    # pattern / JSON
    "js -> 'a'", "js ->> 'a'", "js -> 'a' -> 'b'",
    "COSINE_SIMILARITY(s, 'MAIL')",
    # arithmetic with one constant operand (operand order kept)
    "i + 1", "10 - i", "j * 3", "j - 7", "j % 3", "u + 1", "u64 + 5", "f * 2.5", "f / 2", "2 / f",
    "m * 2", "m + 1.5", "m38 * 2", "m38 - m38", "i & 3", "s || '!'", "'>' || s",
    "t + INTERVAL '1' DAY", "d + INTERVAL '1' DAY", "t - '2024-01-01'::TIMESTAMP",
]


@pytest.mark.parametrize("expr", PROJECTIONS)
def test_projection_compressed_matches_dense(workspaces, expr):
    _assert_same(expr, _rows("cmk_dict", expr), _rows("cmk_plain", expr))


PREDICATES = [
    "s = ''", "s <> ''", "s RLIKE '^[MS]'", "s NOT RLIKE 'A'",
    "js IS JSON OBJECT", "js IS NOT JSON ARRAY", "js IS JSON SCALAR", "js @? '$.a'",
    # compares with no compressed path of their own (DECIMAL128 / UINT64 / UUID / BOOL)
    "u64 > 3000000000000000000", "u64 = 0", "m38 >= 1.5", "m38 < 0", "b = TRUE", "b <> FALSE",
    "m38 >= CAST(1.5 AS DECIMAL(30, 3))", "m38 = CAST(-0.075 AS DECIMAL(30, 3))",
    # mixed-type compares with a constant (draken_numeric_cmp / draken_temporal_cmp)
    "i > 1.5", "2.5 > i", "m > 1", "m38 >= 1.5", "u < 2.5",
    "m < CAST(0.5 AS FLOAT64)", "CAST(0.5 AS FLOAT64) < m38", "f > CAST(1 AS DECIMAL(5, 2))",
    "d < '2024-06-01 12:00:00'::TIMESTAMP", "'2024-06-01 12:00:00'::TIMESTAMP > d",
    "t >= '2024-03-01'::DATE",
    "id = '2a2a2a2a-2a2a-2a2a-2a2a-2a2a2a2a2a2a'::UUID", "id <> '01010101-0101-0101-0101-010101010101'::UUID",
]


@pytest.mark.parametrize("where", PREDICATES)
def test_predicate_compressed_matches_dense(workspaces, where):
    _assert_same(where, _rows("cmk_dict", "rid", where), _rows("cmk_plain", "rid", where))


# A value that cannot convert raises on BOTH shapes (and is NULL under TRY_CAST,
# covered by the projections above).
FAILING = ["CAST(m AS UINT8)", "CAST(m AS DECIMAL(4, 1))", "CAST(j AS DECIMAL(3, 1))"]


@pytest.mark.parametrize("expr", FAILING)
def test_failing_cast_fails_on_both_shapes(workspaces, expr):
    for ws in ("cmk_dict", "cmk_plain"):
        with pytest.raises(Exception):
            _rows(ws, expr)


def test_int64_overflow_fails_on_both_shapes(workspaces):
    for ws in ("cmk_dict", "cmk_plain"):
        with pytest.raises(Exception, match="overflow"):
            _rows(ws, "jm - 1")


def test_overflow_ignores_filtered_out_value(workspaces):
    where = "jm > -100"
    _assert_same("jm - 1", _rows("cmk_dict", "jm - 1", where), _rows("cmk_plain", "jm - 1", where))


def test_abs_int64_min_fails_on_both_shapes(workspaces):
    for ws in ("cmk_dict", "cmk_plain"):
        with pytest.raises(Exception, match="INT64 absolute value overflow"):
            _rows(ws, "ABS(jm)")


def test_abs_ignores_filtered_out_int64_min(workspaces):
    # The filter removes every INT64_MIN row; on the dict file the value can stay in
    # the dictionary unreferenced, and the compressed path must not raise on it.
    where = "jm > -100"
    _assert_same("ABS(jm)", _rows("cmk_dict", "ABS(jm)", where), _rows("cmk_plain", "ABS(jm)", where))


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
