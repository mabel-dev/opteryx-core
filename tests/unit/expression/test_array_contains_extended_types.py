"""
Extended type coverage for the native `item = ANY(arr)` kernel
(draken_array_contains, function_array_json.cpp) beyond the original INT64 +
string-family port covered in test_array_reducer_split_native.py.

Adds: the full int/uint width family, FLOAT32/FLOAT64, BOOL, TIMESTAMP64 (with
bind-time unit quantization), an out-of-range integer item (must be FALSE, not
an error), a literal ARRAY on the right (`x = ANY([1,2,3])` — lowered to
draken_in_list, not draken_array_contains), and the fully-literal constant-fold
path (both sides literal) that used to raise `draken_vector_unwrap: expected
... Vector, got list` via the old GIL AnyOpEq fallback.

`= ANY` is the ONLY spelling of this test — the ARRAY_CONTAINS function these
cases were originally written against was removed as duplicate surface area
(the operator forms are the supported spelling). Note that bare `= ANY` does
NOT honour a column alias — the engine names the output column after the
expression — so the computed columns here are read POSITIONALLY via `_values`.

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


def _values(sql):
    """Column value-lists in SELECT order. Bare `= ANY` does not honour `AS`, so
    every computed column in this file is read positionally, not by name."""
    return list(_fetch(sql).values())


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
    arr_col, c = _values(f"SELECT arr, {item} = ANY(arr) FROM {dataset}")
    assert arr_col == rows
    assert c == [_contains_any_eq(a, item) for a in rows]


def test_array_contains_out_of_range_item_is_false_not_error(parquet_array_dataset):
    # 9999 cannot fit a UINT8 array element — must be a clean FALSE (per-row,
    # NULL-row-preserving), never a silent wraparound match and never a plan/
    # runtime error.
    pa = pytest.importorskip("pyarrow")
    data = [[3, 1, 2], None, []]
    dataset, rows = parquet_array_dataset("uint8_oob", data, pa.list_(pa.uint8()))
    (c,) = _values(f"SELECT 9999 = ANY(arr) FROM {dataset}")
    assert c == [False, None, False]


def test_array_contains_negative_item_over_unsigned_array_is_false(parquet_array_dataset):
    pa = pytest.importorskip("pyarrow")
    data = [[1, 2, 3], None]
    dataset, rows = parquet_array_dataset("uint32_neg", data, pa.list_(pa.uint32()))
    (c,) = _values(f"SELECT -1 = ANY(arr) FROM {dataset}")
    assert c == [False, None]


@pytest.mark.parametrize("pa_type_name", ["float32", "float64"])
def test_array_contains_float_family(parquet_array_dataset, pa_type_name):
    pa = pytest.importorskip("pyarrow")
    data = [[1.5, 2.5, 3.5], [None], [], None, [9.25]]
    ptype = pa.list_(getattr(pa, pa_type_name)())
    dataset, rows = parquet_array_dataset(pa_type_name, data, ptype)
    _arr, hit, miss = _values(f"SELECT arr, 2.5 = ANY(arr), 99.9 = ANY(arr) FROM {dataset}")
    assert hit == [_contains_any_eq(a, 2.5) for a in rows]
    assert miss == [_contains_any_eq(a, 99.9) for a in rows]


def test_array_contains_bool_array(parquet_array_dataset):
    pa = pytest.importorskip("pyarrow")
    data = [[True, False], [False], [], None, [True]]
    dataset, rows = parquet_array_dataset("bool", data, pa.list_(pa.bool_()))
    t, f = _values(f"SELECT true = ANY(arr), false = ANY(arr) FROM {dataset}")
    assert t == [_contains_any_eq(a, True) for a in rows]
    assert f == [_contains_any_eq(a, False) for a in rows]


def test_array_contains_timestamp_array_matching_unit(parquet_array_dataset):
    pa = pytest.importorskip("pyarrow")
    import datetime

    d1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
    d2 = datetime.datetime(2024, 1, 2, 0, 0, 0)
    data = [[d1, d2], [], None]
    dataset, rows = parquet_array_dataset("ts_us", data, pa.list_(pa.timestamp("us")))
    hit, miss = _values(
        f"SELECT CAST('2024-01-01 12:00:00' AS TIMESTAMP) = ANY(arr), "
        f"CAST('2024-01-09 00:00:00' AS TIMESTAMP) = ANY(arr) FROM {dataset}"
    )
    assert hit == [_contains_any_eq(a, d1) for a in rows]
    assert miss == [False, False, None]


def test_array_contains_timestamp_array_ms_unit_quantizes_item(parquet_array_dataset):
    # The array's storage unit (ms) differs from the literal's canonical unit
    # (us) — the item must be bind-time converted to ms, not compared raw.
    pa = pytest.importorskip("pyarrow")
    import datetime

    d1 = datetime.datetime(2024, 1, 1, 12, 0, 0)
    dataset, rows = parquet_array_dataset("ts_ms", [[d1]], pa.list_(pa.timestamp("ms")))
    (hit,) = _values(
        f"SELECT CAST('2024-01-01 12:00:00' AS TIMESTAMP) = ANY(arr) FROM {dataset}"
    )
    assert hit == [True]


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
            f"SELECT CAST('2024-01-01 12:00:00.123456' AS TIMESTAMP) = ANY(arr) "
            f"FROM {dataset}"
        )


def test_array_contains_literal_array_on_right_over_column():
    # `<column> = ANY(<literal array>)` is a DIFFERENT native shape from the
    # array-is-a-column case: an IN-list test, lowered to draken_in_list (not
    # draken_array_contains). Read positionally — bare `x = ANY(...)` doesn't
    # honour a column alias (pre-existing, reproduces on the already-shipped
    # `'Apollo 11' = ANY(missions)` shape too).
    ids, r1 = _values("SELECT id, id = ANY([2, 4, 6]) FROM $planets")
    assert r1 == [i in (2, 4, 6) for i in ids]


def test_array_contains_fully_literal_constant_folds():
    # Both sides literal — must constant-fold through the SAME native
    # draken_in_list instruction (fold_constants calls execute_bytecode on the
    # compiled bytecode), not the old GIL AnyOpEq path that only accepted
    # materialised Vectors and raised `draken_vector_unwrap: ... got list` for
    # a bare Python list operand.
    hit, miss = _values("SELECT 2 = ANY([1, 2, 3]), 9 = ANY([1, 2, 3]) FROM $planets LIMIT 1")
    assert hit == [True]
    assert miss == [False]


# --- the removed function spellings ---------------------------------------------
# ARRAY_CONTAINS / _ANY / _ALL were removed: the operator forms are the supported
# spelling and the functions were pure duplicate surface area (ARRAY_CONTAINS was
# already lowered to the identical AnyOpEq node at plan-build time). They must now
# fail as UNKNOWN FUNCTIONS — a clean FunctionNotFoundError naming the function,
# not a bind-time type error, not a silent no-op, and not a crash.


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT ARRAY_CONTAINS(missions, 'Apollo 11') FROM testdata.astronauts",
        "SELECT ARRAY_CONTAINS_ANY(missions, ('Apollo 11')) FROM testdata.astronauts",
        "SELECT ARRAY_CONTAINS_ALL(missions, ('Apollo 11')) FROM testdata.astronauts",
        # predicate position reaches a different bind path than projection
        "SELECT name FROM testdata.astronauts WHERE ARRAY_CONTAINS(missions, 'Apollo 11')",
        "SELECT name FROM testdata.astronauts WHERE ARRAY_CONTAINS_ANY(missions, ('a','b'))",
        "SELECT name FROM testdata.astronauts WHERE ARRAY_CONTAINS_ALL(missions, ('a','b'))",
        # a literal array argument used to be a separate lowering
        "SELECT ARRAY_CONTAINS([1,2,3], 2) FROM $planets",
    ],
)
def test_removed_function_spellings_raise_function_not_found(sql):
    from opteryx.exceptions import FunctionNotFoundError

    with pytest.raises(FunctionNotFoundError) as err:
        _fetch(sql)
    assert "ARRAY_CONTAINS" in str(err.value), str(err.value)


def test_removed_function_arity_does_not_resurrect_it():
    """The old ARRAY_CONTAINS had a dedicated two-argument SqlError at plan-build
    time. That arity check must not survive the removal as a different-shaped
    error — a wrong-arity call is an unknown function like any other."""
    from opteryx.exceptions import FunctionNotFoundError

    with pytest.raises(FunctionNotFoundError):
        _fetch("SELECT ARRAY_CONTAINS(missions) FROM testdata.astronauts")


def test_operator_forms_still_work_for_each_removed_function():
    """Equivalence cover: every removed spelling has a live operator form."""
    # ARRAY_CONTAINS(missions, x)      -> x = ANY(missions)
    (single,) = _values(
        "SELECT 'Apollo 11' = ANY(missions) FROM testdata.astronauts"
    )
    assert any(v is True for v in single)
    # ARRAY_CONTAINS_ANY(missions, s)  -> missions @> s   (alias IS honoured here)
    d = _fetch(
        "SELECT missions @> ('Apollo 11','Gemini 8') AS c FROM testdata.astronauts"
    )
    assert any(v is True for v in d["c"])
    # ARRAY_CONTAINS_ALL(missions, s)  -> missions @>> s
    d = _fetch(
        "SELECT missions @>> ('Apollo 11','Gemini 8') AS c FROM testdata.astronauts"
    )
    assert any(v is True for v in d["c"])
    # and @> / @>> must not agree — contains-ANY is strictly weaker
    assert sum(v is True for v in _fetch(
        "SELECT missions @> ('Apollo 11','Gemini 8') AS c FROM testdata.astronauts")["c"]
    ) > sum(v is True for v in _fetch(
        "SELECT missions @>> ('Apollo 11','Gemini 8') AS c FROM testdata.astronauts")["c"]
    )


if __name__ == "__main__":
    for fn in sorted(k for k in dict(globals()) if k.startswith("test_")):
        if "parquet_array_dataset" in globals().get(fn).__code__.co_varnames[: globals()[fn].__code__.co_argcount]:
            continue  # needs the fixture; run via pytest
        globals()[fn]()
        print("OK", fn)
    print("all passed (fixture-based tests skipped in __main__ mode — run via pytest)")
