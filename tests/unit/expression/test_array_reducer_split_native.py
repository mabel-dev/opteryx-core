"""
Native ARRAY-typed function kernels: GREATEST, LEAST (unary array reducers) and
SPLIT (VARCHAR -> ARRAY). These were REFUSED at plan time before the native
kernels landed (the engine has no Python-per-morsel fallback), so running them as
COLUMN queries at all is the capability being tested — a literal-only smoke test
would pass while the column form was refused, which is why every case here reads a
real column.

Oracle is plain Python over the raw column values pulled from the engine, NOT a
diff against a Python impl (there was no runnable column impl to diff against):
  * GREATEST/LEAST match make_array_greatest (nanmax/nanmin): a NULL row -> NULL,
    NULL elements skipped, an empty / all-null array -> NULL, else max/min.
  * SPLIT matches Python str.split(sep[, maxsplit]).

Covers empty arrays, NULL arrays, arrays with interior NULLs, single-element
arrays (all arise naturally in the astronauts data / its GROUP BY aggregates), and
the const-folded ARRAY-literal crash that used to SIGSEGV.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

import opteryx


def _fetch(sql):
    """Flatten a query to {column_name: [values...]} across all morsels."""
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


def _reduce(array, want_max):
    if array is None:
        return None
    vals = [x for x in array if x is not None]
    if not vals:
        return None
    return max(vals) if want_max else min(vals)


def test_greatest_least_over_varchar_array_column():
    # missions is ARRAY<VARCHAR>; rows include empty arrays (astronauts who never
    # flew) and multi-element arrays. ASCII, so Python max/min == the engine's
    # byte-order str_compare.
    d = _fetch(
        "SELECT missions, GREATEST(missions) AS g, LEAST(missions) AS l "
        "FROM testdata.astronauts"
    )
    assert d, "GREATEST/LEAST over a VARCHAR array column returned no rows"
    for arr, g, l in zip(d["missions"], d["g"], d["l"]):
        assert g == _reduce(arr, True)
        assert l == _reduce(arr, False)


@pytest.fixture(scope="module")
def int_array_dataset():
    # An ARRAY<INT64> column exercising every edge case at once: multi-element,
    # all-null element, empty array, NULL array, single-element, and an array
    # with an INTERIOR null. ARRAY_AGG is not runnable on the native engine, so a
    # written parquet fixture is the way to get a numeric array COLUMN. Referenced
    # by dotted dataset name, so it must live under testdata/.
    pa = pytest.importorskip("pyarrow")
    import shutil

    import pyarrow.parquet as pq

    rel = "testdata/_arr_reducer_int_fixture"
    root = os.path.join(os.path.dirname(__file__), "../../..", rel)
    root = os.path.normpath(root)
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    data = [[3, 1, 2], [None], [], None, [5], [9, None, 7]]
    tbl = pa.table({"arr": pa.array(data, type=pa.list_(pa.int64()))})
    pq.write_table(tbl, os.path.join(root, "00000.parquet"))
    try:
        yield "testdata._arr_reducer_int_fixture", data
    finally:
        shutil.rmtree(root, ignore_errors=True)


def test_greatest_least_over_int_array_column(int_array_dataset):
    dataset, expected_rows = int_array_dataset
    d = _fetch(f"SELECT arr, GREATEST(arr) AS g, LEAST(arr) AS l FROM {dataset}")
    assert d, "GREATEST/LEAST over an int array column returned no rows"
    assert d["arr"] == expected_rows  # rugo decoded every shape, incl. interior null
    for arr, g, l in zip(d["arr"], d["g"], d["l"]):
        assert g == _reduce(arr, True)
        assert l == _reduce(arr, False)


def test_greatest_over_nested_array_fails_clean(int_array_dataset):
    # A nested-array element (ARRAY<ARRAY<INT>>) is not orderable by the reducer
    # and must fail LOUD (never silently wrong / never crash). rugo may also
    # refuse to decode it — either way the query must raise, not succeed.
    pa = pytest.importorskip("pyarrow")
    import shutil

    import pyarrow.parquet as pq

    root = os.path.normpath(
        os.path.join(os.path.dirname(__file__), "../../..", "testdata/_arr_reducer_nested")
    )
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)
    tbl = pa.table(
        {"arr": pa.array([[[1, 2], [3]], [[4]]], type=pa.list_(pa.list_(pa.int64())))}
    )
    pq.write_table(tbl, os.path.join(root, "00000.parquet"))
    try:
        with pytest.raises(Exception):
            _fetch("SELECT GREATEST(arr) AS g FROM testdata._arr_reducer_nested")
    finally:
        shutil.rmtree(root, ignore_errors=True)


def _contains_any_eq(array, item):
    # Reference SQL `item = ANY(array)` three-valued logic: NULL array -> None,
    # empty -> False, True iff any non-null element equals item; a NULL element
    # alone does NOT flip a no-match to NULL in this engine's impl (element skip).
    if array is None:
        return None
    return any(e == item for e in array if e is not None)


def test_array_contains_projection_matches_any_semantics():
    # `'Apollo 11' = ANY(missions)` is native. NULL array row -> NULL (not False).
    # Read positionally: bare `= ANY` names the column after the expression, not
    # the alias.
    raw = _fetch("SELECT missions FROM testdata.astronauts")
    d = _fetch("SELECT 'Apollo 11' = ANY(missions) FROM testdata.astronauts")
    assert d, "`= ANY` projection returned no rows (was it refused?)"
    expected = [_contains_any_eq(a, "Apollo 11") for a in raw["missions"]]
    (values,) = d.values()
    assert values == expected


def test_array_contains_in_filter():
    # Must be admitted as a c-native predicate (WRAP_AS_BOOL), not just a projection.
    raw = _fetch("SELECT name, missions FROM testdata.astronauts")
    expected = sorted(
        n for n, m in zip(raw["name"], raw["missions"]) if _contains_any_eq(m, "Apollo 11")
    )
    d = _fetch("SELECT name FROM testdata.astronauts WHERE 'Apollo 11' = ANY(missions)")
    assert sorted(d.get("name", [])) == expected
    assert expected, "expected at least one Apollo 11 astronaut"


def test_bare_any_eq_over_array_column():
    # The bare `x = ANY(arr)` syntax gets the same native path. (The engine names
    # the output column after the expression, not the alias, so read positionally.)
    raw = _fetch("SELECT missions FROM testdata.astronauts")
    d = _fetch("SELECT 'Apollo 11' = ANY(missions) FROM testdata.astronauts")
    assert d
    (values,) = d.values()
    assert values == [_contains_any_eq(a, "Apollo 11") for a in raw["missions"]]


def test_split_two_arg_matches_python():
    d = _fetch("SELECT name, SPLIT(name, ' ') AS parts FROM testdata.astronauts")
    assert d
    for name, parts in zip(d["name"], d["parts"]):
        assert parts == (None if name is None else name.split(" "))


def test_split_three_arg_limit_matches_python():
    d = _fetch("SELECT name, SPLIT(name, ' ', 2) AS parts FROM testdata.astronauts")
    assert d
    for name, parts in zip(d["name"], d["parts"]):
        assert parts == (None if name is None else name.split(" ", 2))


def test_split_multichar_delimiter():
    # Multi-character delimiter over a constant string (the old Python impl only
    # SIMD-split single chars; the native kernel must handle multi-char too).
    d = _fetch("SELECT SPLIT('a::b::c', '::') AS parts FROM $planets")
    assert d
    for parts in d["parts"]:
        assert parts == ["a", "b", "c"]


def test_split_empty_and_no_delimiter_cases():
    # Empty string -> [''] ; no delimiter present -> [whole string]. str.split
    # semantics (separators not collapsed).
    d = _fetch(
        "SELECT SPLIT('', ',') AS a, SPLIT('nodelim', ',') AS b, "
        "SPLIT('a,,b', ',') AS c FROM $planets LIMIT 1"
    )
    assert d["a"][0] == [""]
    assert d["b"][0] == ["nodelim"]
    assert d["c"][0] == ["a", "", "b"]


def test_split_child_is_typed_not_variant():
    """SPLIT's ARRAY child carries the input's own string type, never VARIANT.

    The parts are substrings of the input, so the element type is fixed and known —
    draken_split tags the child with `str->type` and the SPLIT signature's resolver
    declares the matching ARRAY<element>. A VARIANT child is not merely imprecise:
    VARIANT has no gather/compare path, so it strands the result (see
    test_split_survives_order_by). Guards both halves against drifting back.
    """
    from draken.draken_native import ARRAY, VARCHAR

    morsels = [
        m
        for m in opteryx.session().execute_to_morsels(
            "SELECT SPLIT(name, ' ') AS parts FROM testdata.astronauts"
        )
        if m is not None and m.num_rows > 0
    ]
    assert morsels
    column = morsels[0].column(b"parts")
    assert column.type == ARRAY
    child = column.array_child
    assert child is not None, "SPLIT must produce an ARRAY child vector"
    assert child.type == VARCHAR, f"SPLIT child must be VARCHAR, got {child.type}"


def test_split_survives_order_by():
    """A SPLIT result must survive an ORDER BY/LIMIT.

    ORDER BY materialises the top rows via gather_rows, which has to copy each row's
    elements out of the ARRAY's child. This raised "gather_rows: unsupported column
    type" until the ARRAY gather arm landed — every other SPLIT test here is a plain
    projection that never gathers, so nothing else covers it.
    """
    d = _fetch("SELECT id, SPLIT(name, 'a') AS parts FROM $planets ORDER BY id LIMIT 3")
    assert d["id"] == [1, 2, 3]
    assert d["parts"] == [name.split("a") for name in ("Mercury", "Venus", "Earth")]


def test_greatest_array_literal_evaluates_not_segfault():
    # This query used to SIGSEGV: a constant ARRAY literal was never materialized
    # into a DrakenVector, so a bare Python list reached the nb trampoline and was
    # unchecked-cast to Vector. ARRAY literals now materialize (const-fold), so it
    # returns the reduced value. The load-bearing assertion is that the process
    # survives and computes the right answer.
    d = _fetch("SELECT GREATEST([1,5,3]) AS g, LEAST([1,5,3]) AS l FROM $planets LIMIT 1")
    assert d["g"][0] == 5
    assert d["l"][0] == 1


if __name__ == "__main__":
    for fn in sorted(k for k in dict(globals()) if k.startswith("test_")):
        globals()[fn]()
        print("OK", fn)
    print("all passed")
