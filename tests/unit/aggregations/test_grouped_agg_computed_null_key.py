"""Regression tests for GROUP BY / projection over *computed* NULL keys.

Distinct from test_grouped_agg_null_string_key.py, which covers column-sourced
(parquet) nulls. Here the NULLs are produced by an expression — a `CASE ... THEN
NULL` branch or `NULLIF(...)` — which yields an untyped DRAKEN_NULL vector for the
null branch.

The bug (pre-existing): the CASE assemble kernel was selected by runtime dispatch
keyed on the *first non-None branch result's* type, because the bind-time path
read `src.inferred_type` which is never populated on a CASE node (Node.__getattr__
returns None). When the first branch was `THEN NULL`, dispatch picked the wrong
assemble kernel and produced a fixed/bool vector mislabelled as the (string)
output column — a heap-corrupting type confusion that SIGSEGV'd, typically at a
later GC pass. NULLIF separately returned a raw Python list instead of a typed
Vector, which failed to wrap into a column.

Fixes:
  * CASE kernel type is resolved at bind time from the binder-computed
    schema_column.column_type (the authoritative output type).
  * A bare `THEN NULL` branch result (DRAKEN_NULL vector) is normalised to Python
    None so every assemble kernel skips it, leaving those rows null.
  * NULLIF returns a typed Draken Vector (same type as its first argument).

These ran deterministically as SIGSEGV/bus error before the fix.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import opteryx


def _group(sql):
    """Run `sql` (first column = key, second = count) → {key_or_None: count}."""
    got = {}
    for m in opteryx.session().execute_to_morsels(sql):
        names = m._col_names
        ks = m.column(names[0]).to_pylist()
        cs = m.column(names[1]).to_pylist()
        for k, c in zip(ks, cs):
            got[k] = got.get(k, 0) + c
    return got


def _values(sql):
    """Run a single-column `sql` and return the materialised column as a list."""
    out = []
    for m in opteryx.session().execute_to_morsels(sql):
        out.extend(m.column(m._col_names[0]).to_pylist())
    return out


# Earth is the 3rd planet (id == 3) in $planets (9 rows).


def test_case_then_null_string_key():
    got = _group(
        "SELECT CASE WHEN name='Earth' THEN NULL ELSE name END AS k, COUNT(*) c "
        "FROM $planets GROUP BY CASE WHEN name='Earth' THEN NULL ELSE name END"
    )
    assert len(got) == 9, got           # 8 named + 1 NULL group
    assert got.get(None) == 1, got      # only Earth landed in the NULL group
    assert got.get("Mercury") == 1


def test_case_else_null_string_key():
    # NULL in the ELSE branch (non-NULL branch first) — must agree with above.
    got = _group(
        "SELECT CASE WHEN name<>'Earth' THEN name ELSE NULL END AS k, COUNT(*) c "
        "FROM $planets GROUP BY CASE WHEN name<>'Earth' THEN name ELSE NULL END"
    )
    assert len(got) == 9, got
    assert got.get(None) == 1, got


def test_nullif_string_key():
    got = _group(
        "SELECT NULLIF(name,'Earth') AS k, COUNT(*) c "
        "FROM $planets GROUP BY NULLIF(name,'Earth')"
    )
    assert len(got) == 9, got
    assert got.get(None) == 1, got
    assert got.get("Mars") == 1


def test_case_then_null_int_key():
    got = _group(
        "SELECT CASE WHEN id=3 THEN NULL ELSE id END AS k, COUNT(*) c "
        "FROM $planets GROUP BY CASE WHEN id=3 THEN NULL ELSE id END"
    )
    assert len(got) == 9, got
    assert got.get(None) == 1, got


def test_nullif_int_key():
    got = _group(
        "SELECT NULLIF(id,3) AS k, COUNT(*) c "
        "FROM $planets GROUP BY NULLIF(id,3)"
    )
    assert len(got) == 9, got
    assert got.get(None) == 1, got


def test_case_then_null_float_key():
    got = _group(
        "SELECT CASE WHEN id=3 THEN NULL ELSE gravity END AS k, COUNT(*) c "
        "FROM $planets GROUP BY CASE WHEN id=3 THEN NULL ELSE gravity END"
    )
    # Earth's gravity row becomes the NULL group; the rest group by gravity.
    assert got.get(None) == 1, got


def test_case_then_null_string_projection_only():
    # No GROUP BY — just materialise the computed-null column (this alone crashed).
    vals = _values("SELECT CASE WHEN name='Earth' THEN NULL ELSE name END AS k FROM $planets")
    assert len(vals) == 9
    assert vals.count(None) == 1
    assert "Earth" not in vals


def test_nullif_string_projection_only():
    vals = _values("SELECT NULLIF(name,'Earth') AS k FROM $planets")
    assert len(vals) == 9
    assert vals.count(None) == 1


def test_case_bool_branch_with_null_projection_only():
    # BOOLEAN CASE whose first branch is NULL: assemble_bool must not C-cast the
    # untyped NULL branch as a BoolVector. (GROUP BY on a BOOL key is a separate
    # unsupported path; here we only materialise the column.)
    vals = _values("SELECT CASE WHEN id=3 THEN NULL ELSE (id>5) END AS k FROM $planets")
    assert len(vals) == 9
    assert vals.count(None) == 1


def test_case_all_null_projection_only():
    # Every branch NULL → an all-NULL column, not a crash and not an error.
    vals = _values("SELECT CASE WHEN id>0 THEN NULL ELSE NULL END AS k FROM $planets")
    assert len(vals) == 9
    assert vals.count(None) == 9


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("PASS", name)
    print("ok")
