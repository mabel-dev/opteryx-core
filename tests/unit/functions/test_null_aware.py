"""
Regression tests for the null-aware functions IFNULL and COALESCE.

These exercise the supported SQL execution path. The legacy unit tests called
``if_null`` / ``if_not_null`` directly with NumPy arrays; that contract no
longer exists — the kernels operate on Draken vectors handed down by the
bytecode evaluator.

Bug fixed here: the non-nb function-call path in the bytecode evaluator hands
Cython shim vectors (``draken.vectors.vector.Vector``) to ``if_null``, but
``if_null`` called the nanobind ``vector_iif`` kernel, whose ``unwrap``
expects a raw ``draken.draken_native.Vector``. The shim args were rejected
with ``TypeError: draken_vector_unwrap: expected draken.draken_native.Vector,
got draken.vectors.vector.Vector``, breaking IFNULL, the two-argument COALESCE
(which the optimizer rewrites into IFNULL), and any nesting of these.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx


def _col(sql, name="k"):
    rows = []
    for morsel in opteryx.session().execute_to_morsels(sql):
        rows.extend(morsel.column(name).to_pylist())
    return rows


def test_ifnull_standalone():
    # No nulls in $planets.name → IFNULL returns the original values unchanged.
    out = _col("SELECT IFNULL(name, 'x') AS k FROM $planets")
    assert "x" not in out, out
    assert out[2] == "Earth", out


def test_ifnull_replaces_nulls():
    # NULLIF(name, 'Earth') makes the Earth row NULL; IFNULL must replace it.
    out = _col("SELECT IFNULL(NULLIF(name, 'Earth'), 'REPLACED') AS k FROM $planets")
    assert out.count("REPLACED") == 1, out
    assert "Earth" not in out, out
    assert None not in out, out


def test_coalesce_two_arg_standalone():
    # Two-arg COALESCE is rewritten into IFNULL by the optimizer.
    out = _col("SELECT COALESCE(name, 'x') AS k FROM $planets")
    assert "x" not in out, out
    assert out[2] == "Earth", out


def test_coalesce_two_arg_replaces_nulls():
    out = _col("SELECT COALESCE(NULLIF(name, 'Mars'), 'M') AS k FROM $planets")
    assert out.count("M") == 1, out
    assert "Mars" not in out, out
    assert None not in out, out


def test_ifnull_numeric():
    # Numeric branches require matching physical widths; cast both to INTEGER.
    out = _col(
        "SELECT IFNULL(CAST(NULLIF(id, 3) AS INTEGER), CAST(-1 AS INTEGER)) AS k "
        "FROM $planets"
    )
    assert out.count(-1) == 1, out
    assert 3 not in out, out


def test_null_aware_narrow_int_promotion():
    # $planets.id is INT8; the literal 0 binds as INT64. vector_iif/vector_coalesce
    # now promote the narrow branch to INT64 instead of rejecting the mismatch.
    # (Previously: "vector_iif: fixed-width branch type mismatch".)
    assert _col("SELECT IFNULL(id, 0) AS k FROM $planets") == list(range(1, 10))
    assert _col("SELECT IFNOTNULL(id, 0) AS k FROM $planets") == [0] * 9
    assert _col("SELECT COALESCE(id, 0) AS k FROM $planets") == list(range(1, 10))
    # NULLIF(id,3) makes the id==3 row NULL → replacement applies there only.
    out = _col("SELECT IFNULL(NULLIF(id, 3), -1) AS k FROM $planets")
    assert out.count(-1) == 1 and 3 not in out, out


def test_null_aware_column_column_width_promotion():
    # Both branches are columns of differing width (INT8 vs INT64) — no literal to
    # coerce, so this exercises the kernel-level promotion specifically.
    out = _col("SELECT COALESCE(NULLIF(id, 3), CAST(99 AS INTEGER)) AS k FROM $planets")
    assert out.count(99) == 1 and 3 not in out, out


def test_null_aware_int_float_promotion():
    # int + float branches promote to FLOAT64 (mirrors find_compatible_type).
    out = _col("SELECT COALESCE(NULLIF(id, 3), mass) AS k FROM $planets")
    assert all(isinstance(v, float) for v in out), out
    # Non-null id rows are id as float; the id==3 row falls through to mass
    # (Earth's mass, 5.97) — distinct from the float 3.0 it would be otherwise.
    assert out[0] == 1.0 and out[2] != 3.0 and out[2] == 5.97, out


def test_nested_outer_nb_over_ifnull():
    # Outer nb function (UPPER) consuming the IFNULL result (a shim).
    out = _col("SELECT UPPER(IFNULL(NULLIF(name, 'Earth'), 'rep')) AS k FROM $planets")
    assert "REP" in out, out
    assert "MERCURY" in out, out


def test_nested_coalesce_over_ifnull():
    # Non-nb COALESCE-as-IFNULL consuming another IFNULL result.
    out = _col(
        "SELECT COALESCE(IFNULL(NULLIF(name, 'Earth'), 'inner'), 'outer') AS k "
        "FROM $planets"
    )
    assert "inner" in out, out
    assert "outer" not in out, out


def test_nested_ifnull_over_ifnull():
    out = _col(
        "SELECT IFNULL(IFNULL(NULLIF(name, 'Earth'), 'a'), 'b') AS k FROM $planets"
    )
    assert "a" in out, out
    assert "b" not in out, out


def test_ifnotnull_semantics():
    # IFNOTNULL(value, result): result where value IS NOT NULL, keep NULL where
    # value IS NULL. NULLIF(name, 'Venus') makes the Venus row NULL, so the Venus
    # row stays NULL and every other row becomes 'HAD'. (Previously IFNOTNULL was
    # wired to the IFNULL kernel and behaved with inverted semantics.)
    out = _col("SELECT IFNOTNULL(NULLIF(name, 'Venus'), 'HAD') AS k FROM $planets")
    assert out.count("HAD") == len(out) - 1, out
    assert out.count(None) == 1, out
    # Venus is the second row in $planets.
    assert out[1] is None, out


def test_zero_times_column_fold():
    # The optimizer folds `0 * x` (and `x * 0`) into IFNOTNULL(x, 0); the folded
    # node must bind a function_ref and coerce the constant to the column type.
    # mass has no nulls → all zeros.
    for sql in (
        "SELECT 0 * mass AS k FROM $planets",
        "SELECT mass * 0 AS k FROM $planets",
    ):
        out = _col(sql)
        assert out == [0.0] * len(out), (sql, out)


def test_zero_times_column_fold_preserves_null():
    # NULL must survive the `0 * x` fold: 0 * NULL → NULL, not 0. v is a nullable
    # DOUBLE identifier (subquery output) so the fold fires on an identifier.
    for op in ("0 * v", "v * 0"):
        sql = (
            f"SELECT {op} AS k FROM "
            "(SELECT CAST(NULLIF(id, 3) AS DOUBLE) AS v FROM $planets) AS t"
        )
        out = _col(sql)
        assert out.count(None) == 1, (sql, out)
        assert out.count(0.0) == len(out) - 1, (sql, out)
        # id == 3 is the third row in $planets.
        assert out[2] is None, (sql, out)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
