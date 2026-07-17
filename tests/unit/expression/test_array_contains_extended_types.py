"""
Extended type coverage for the native ARRAY_CONTAINS / `item = ANY(arr)` kernel
(draken_array_contains, function_array_json.cpp) beyond the original INT64 +
string-family port covered in test_array_reducer_split_native.py.

Adds: the full int/uint width family, FLOAT32/FLOAT64, BOOL, TIMESTAMP64 (with
bind-time unit quantization), an out-of-range integer item (must be FALSE, not
an error), a literal ARRAY on the right (`x = ANY([1,2,3])` /
`ARRAY_CONTAINS([1,2,3], x)` — lowered to draken_in_list, not
draken_array_contains), and the fully-literal constant-fold path (both sides
literal) that used to raise `draken_vector_unwrap: expected ... Vector, got
list` via the old GIL AnyOpEq fallback.

Oracle is plain Python over the raw column values pulled from the engine
(same reference `= ANY` semantics as test_array_reducer_split_native.py: NULL
array row -> NULL, empty -> FALSE, NULL elements skipped) — there is no
runnable Python column impl to diff against (native_engine_has_no_python_fallback).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx


def _fetch(sql):
    names = None
    data = {}
    for morsel in opteryx.session().execute_to_morsels(sql):
        if morsel is None or morsel.num_rows == 0:
            continue
        cols = [
            c.decode() if isinstance(c, (bytes, bytearray)) else c
            for c in morsel.column_names
        ]
        if names is None:
            names = cols
            data = {c: [] for c in cols}
        for i, name in enumerate(names):
            data[name].extend(morsel.column(morsel.column_names[i]).to_pylist())
    return data


def _contains_any_eq(array, item):
    if array is None:
        return None
    return any(e == item for e in array if e is not None)


@pytest.fixture()
def parquet_array_dataset():
    """Write a single-column list<T> parquet fixture under testdata/ and yield
    its dotted dataset name + the raw Python rows. Caller supplies the
    pyarrow type and the row data (must cover: multi-element, empty, NULL
    array, and an interior NULL, per the existing suite's convention)."""
    pa = pytest.importorskip("pyarrow")
    import shutil

    import pyarrow.parquet as pq

    made = []

    def _make(name, data, ptype):
        rel = f"testdata/_ac_ext_{name}"
        root = os.path.normpath(os.path.join(os.path.dirname(__file__), "../../..", rel))
        shutil.rmtree(root, ignore_errors=True)
        os.makedirs(root, exist_ok=True)
        tbl = pa.table({"arr": pa.array(data, type=ptype)})
        pq.write_table(tbl, os.path.join(root, "00000.parquet"))
        made.append(root)
        return f"testdata._ac_ext_{name}", data

    yield _make

    for root in made:
        shutil.rmtree(root, ignore_errors=True)


@pytest.mark.parametrize(
    "pa_type_name,item",
    [
        ("int8", 2),
        ("int16", 2),
        ("int32", 2),
        ("uint8", 2),
        ("uint16", 2),
        ("uint32", 2),
        ("uint64", 2),
    ],
)
def test_array_contains_int_width_family(parquet_array_dataset, pa_type_name, item):
    pa = pytest.importorskip("pyarrow")
    ptype = pa.list_(getattr(pa, pa_type_name)())
    data = [[3, 1, 2], [None], [], None, [5], [9, None, 7]]
    dataset, rows = parquet_array_dataset(pa_type_name, data, ptype)
    d = _fetch(f"SELECT arr, ARRAY_CONTAINS(arr, {item}) AS c FROM {dataset}")
    assert d["arr"] == rows
    assert d["c"] == [_contains_any_eq(a, item) for a in rows]


def test_array_contains_out_of_range_item_is_false_not_error(parquet_array_dataset):
    # 9999 cannot fit a UINT8 array element — must be a clean FALSE (per-row,
    # NULL-row-preserving), never a silent wraparound match and never a plan/
    # runtime error.
    pa = pytest.importorskip("pyarrow")
    data = [[3, 1, 2], None, []]
    dataset, rows = parquet_array_dataset("uint8_oob", data, pa.list_(pa.uint8()))
    d = _fetch(f"SELECT ARRAY_CONTAINS(arr, 9999) AS c FROM {dataset}")
    assert d["c"] == [False, None, False]


def test_array_contains_negative_item_over_unsigned_array_is_false(parquet_array_dataset):
    pa = pytest.importorskip("pyarrow")
    data = [[1, 2, 3], None]
    dataset, rows = parquet_array_dataset("uint32_neg", data, pa.list_(pa.uint32()))
    d = _fetch(f"SELECT ARRAY_CONTAINS(arr, -1) AS c FROM {dataset}")
    assert d["c"] == [False, None]


@pytest.mark.parametrize("pa_type_name", ["float32", "float64"])
def test_array_contains_float_family(parquet_array_dataset, pa_type_name):
    pa = pytest.importorskip("pyarrow")
    data = [[1.5, 2.5, 3.5], [None], [], None, [9.25]]
    ptype = pa.list_(getattr(pa, pa_type_name)())
    dataset, rows = parquet_array_dataset(pa_type_name, data, ptype)
    d = _fetch(f"SELECT arr, ARRAY_CONTAINS(arr, 2.5) AS hit, ARRAY_CONTAINS(arr, 99.9) AS miss FROM {dataset}")
    assert d["hit"] == [_contains_any_eq(a, 2.5) for a in rows]
    assert d["miss"] == [_contains_any_eq(a, 99.9) for a in rows]


def test_array_contains_bool_array(parquet_array_dataset):
    pa = pytest.importorskip("pyarrow")
    data = [[True, False], [False], [], None, [True]]
    dataset, rows = parquet_array_dataset("bool", data, pa.list_(pa.bool_()))
    d = _fetch(f"SELECT ARRAY_CONTAINS(arr, true) AS t, ARRAY_CONTAINS(arr, false) AS f FROM {dataset}")
    assert d["t"] == [_contains_any_eq(a, True) for a in rows]
    assert d["f"] == [_contains_any_eq(a, False) for a in rows]


def test_array_contains_timestamp_array_matching_unit(parquet_array_dataset):
    pa = pytest.importorskip("pyarrow")
    import datetime

    d1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
    d2 = datetime.datetime(2024, 1, 2, 0, 0, 0)
    data = [[d1, d2], [], None]
    dataset, rows = parquet_array_dataset("ts_us", data, pa.list_(pa.timestamp("us")))
    d = _fetch(
        f"SELECT ARRAY_CONTAINS(arr, CAST('2024-01-01 12:00:00' AS TIMESTAMP)) AS hit, "
        f"ARRAY_CONTAINS(arr, CAST('2024-01-09 00:00:00' AS TIMESTAMP)) AS miss FROM {dataset}"
    )
    assert d["hit"] == [_contains_any_eq(a, d1) for a in rows]
    assert d["miss"] == [False, False, None]


def test_array_contains_timestamp_array_ms_unit_quantizes_item(parquet_array_dataset):
    # The array's storage unit (ms) differs from the literal's canonical unit
    # (us) — the item must be bind-time converted to ms, not compared raw.
    pa = pytest.importorskip("pyarrow")
    import datetime

    d1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
    dataset, rows = parquet_array_dataset("ts_ms", [[d1]], pa.list_(pa.timestamp("ms")))
    d = _fetch(
        f"SELECT ARRAY_CONTAINS(arr, CAST('2024-01-01 12:00:00' AS TIMESTAMP)) AS hit FROM {dataset}"
    )
    assert d["hit"] == [True]


def test_array_contains_timestamp_item_finer_than_array_unit_is_refused(parquet_array_dataset):
    # A microsecond-precision item can never equal any element of an
    # MS-granularity array — genuinely unrepresentable, not eligible for the
    # native lowering (no blob kind can express "guaranteed false"), so the
    # query is refused at plan time rather than silently wrong.
    pa = pytest.importorskip("pyarrow")
    import datetime

    d1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
    dataset, _ = parquet_array_dataset("ts_ms_lossy", [[d1]], pa.list_(pa.timestamp("ms")))
    with pytest.raises(Exception):
        _fetch(
            f"SELECT ARRAY_CONTAINS(arr, CAST('2024-01-01 12:00:00.123456' AS TIMESTAMP)) "
            f"AS hit FROM {dataset}"
        )


def test_array_contains_literal_array_on_right_over_column():
    # ARRAY_CONTAINS(<literal array>, <column>) / bare `<column> = ANY(<literal
    # array>)` is a DIFFERENT native shape from the array-is-a-column case: an
    # IN-list test, lowered to draken_in_list (not draken_array_contains).
    # `AS r1` is read positionally, not by name — bare `x = ANY(...)` doesn't
    # honour a column alias (pre-existing, reproduces on the already-shipped
    # `'Apollo 11' = ANY(missions)` shape too; not introduced by this change,
    # not fixed here — see the ARRAY_CONTAINS report).
    d = _fetch("SELECT id, id = ANY([2, 4, 6]) AS r1, ARRAY_CONTAINS([2, 4, 6], id) AS r2 FROM $planets")
    values = list(d.values())
    ids, r1, r2 = values[0], values[1], values[2]
    assert r1 == [i in (2, 4, 6) for i in ids]
    assert d["r2"] == [i in (2, 4, 6) for i in ids]


def test_array_contains_fully_literal_constant_folds():
    # Both sides literal — must constant-fold through the SAME native
    # draken_in_list instruction (fold_constants calls execute_bytecode on the
    # compiled bytecode), not the old GIL AnyOpEq path that only accepted
    # materialised Vectors and raised `draken_vector_unwrap: ... got list` for
    # a bare Python list operand.
    d = _fetch("SELECT ARRAY_CONTAINS([1, 2, 3], 2) AS hit, ARRAY_CONTAINS([1, 2, 3], 9) AS miss FROM $planets LIMIT 1")
    assert d["hit"] == [True]
    assert d["miss"] == [False]


if __name__ == "__main__":
    for fn in sorted(k for k in dict(globals()) if k.startswith("test_")):
        if "parquet_array_dataset" in globals().get(fn).__code__.co_varnames[: globals()[fn].__code__.co_argcount]:
            continue  # needs the fixture; run via pytest
        globals()[fn]()
        print("OK", fn)
    print("all passed (fixture-based tests skipped in __main__ mode — run via pytest)")
