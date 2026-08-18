"""Regression: comparing two DECIMAL128 (int128-tier) values in a WHERE clause.

TPC-DS Q04 (tests/performance/tpcds/opteryx/queries/query04.sql) failed with:

    RuntimeError: [1]: ExprFilterOperator: predicate evaluation failed (err_op=11):

err_op=11 is BC_COMPARE. Root cause: `draken_compare_dv` (draken/ops/compare_dv.cpp)
had no dispatch case for DRAKEN_DECIMAL128 — its type switch fell to `default:
return nullptr` (declined), unlike DRAKEN_DECIMAL (int64-tier), which reuses the
int64 kernel. The compiler's `is_all_c_native` eligibility check (build_bytecode,
opteryx/compiled/expression/compiled_expression.pyx) admits any BC_COMPARE with an
ordinal op-code onto the no-fallback native ExprFilterOperator path without
verifying the compare kernel actually supports the operand type — matched
same-type/same-scale DECIMAL128 pairs are exactly the case the compiler's
mixed-numeric routing (`draken_numeric_cmp`) deliberately leaves on the "fast"
draken_compare_dv path (see compiled_expression.pyx's comment: "a matched
same-type-same-scale pair stays on the fast draken_compare_dv"), which was true
for every other numeric type but not DECIMAL128. With no Python fallback on the
native engine, the decline surfaced as an unrecoverable, near-empty-message crash.

Q04 hits this via its year-over-year sales-growth predicate: `year_total` is a
`SUM(...)/2` aggregate over DECIMAL(7,2) columns, which widens past the int64
DECIMAL tier (precision > 18) into DECIMAL128 once divided again inside a CASE
(`t_c_secyear.year_total / t_c_firstyear.year_total`) — and the query compares
two such CASE-wrapped ratios directly (`CASE ... END > CASE ... END`).

Fixed by adding a DECIMAL128 (__int128) compare kernel
(draken/ops/int128_compare.h) and wiring it into draken_compare_dv's type switch
(draken/ops/compare_dv.cpp), mirroring the existing DECIMAL/UINT/BOOL kernel-gap
fixes in that file. Deliberately a UNIFORM-path kernel only (CLAUDE.md §11
default posture) — no dict/constant shape specialization.
"""

import os
import sys
from decimal import Decimal

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx

_SESSION = opteryx.session()


def _rows(sql, colnames):
    out = []
    for morsel in _SESSION.execute_to_morsels(sql):
        cols = [morsel.column(c).to_pylist() for c in colnames]
        out.extend(zip(*cols))
    return out


def _col_type(sql, colname):
    phys = None
    for morsel in _SESSION.execute_to_morsels(sql):
        phys = str(morsel.column(colname).type)
    return phys


def test_decimal128_column_vs_column_where_does_not_crash():
    # Two DECIMAL128 columns (precision 30 > int64 tier's 18-digit cap), same
    # scale, compared column-to-column in a WHERE clause — the exact shape
    # draken_compare_dv used to decline with no fallback (err_op=11).
    phys = _col_type("SELECT CAST(id AS DECIMAL(30,10)) AS a FROM $planets LIMIT 1", "a")
    assert "DECIMAL128" in phys, phys

    rows = _rows(
        "SELECT id FROM $planets "
        "WHERE CAST(id AS DECIMAL(30,10)) > CAST(10 - id AS DECIMAL(30,10)) "
        "ORDER BY id",
        ["id"],
    )
    assert rows == [(6,), (7,), (8,), (9,)], rows


def test_decimal128_column_vs_column_all_ordinal_operators():
    # id in [1, 9]; b = 5 (constant DECIMAL128, but kept column-shaped via a
    # trivial per-row expression so the compiler can't constant-fold it away).
    sql_tmpl = (
        "SELECT id FROM $planets "
        "WHERE CAST(id AS DECIMAL(25,8)) {op} CAST(5 + (id - id) AS DECIMAL(25,8)) "
        "ORDER BY id"
    )
    cases = {
        "=": [(5,)],
        "!=": [(1,), (2,), (3,), (4,), (6,), (7,), (8,), (9,)],
        "<": [(1,), (2,), (3,), (4,)],
        "<=": [(1,), (2,), (3,), (4,), (5,)],
        ">": [(6,), (7,), (8,), (9,)],
        ">=": [(5,), (6,), (7,), (8,), (9,)],
    }
    for op, expected in cases.items():
        rows = _rows(sql_tmpl.format(op=op), ["id"])
        assert rows == expected, (op, rows)


def test_decimal128_case_ratio_comparison_matches_tpcds_q04_pattern():
    # The actual crashing shape: two divisions of DECIMAL128 values, each
    # wrapped in a CASE (ELSE NULL), compared with `>` — TPC-DS Q04's
    # year-over-year growth predicate. ratio_c is always 2, ratio_s is always
    # 1, so every row's predicate is true.
    sql = """
    SELECT id,
           CASE WHEN CAST(id AS DECIMAL(30,10)) > 0
                THEN CAST(id * 2 AS DECIMAL(30,10)) / CAST(id AS DECIMAL(30,10))
                ELSE NULL END AS ratio_c,
           CASE WHEN CAST(id AS DECIMAL(30,10)) > 0
                THEN CAST(id AS DECIMAL(30,10)) / CAST(id AS DECIMAL(30,10))
                ELSE NULL END AS ratio_s
    FROM $planets
    WHERE CASE WHEN CAST(id AS DECIMAL(30,10)) > 0
               THEN CAST(id * 2 AS DECIMAL(30,10)) / CAST(id AS DECIMAL(30,10))
               ELSE NULL END
        > CASE WHEN CAST(id AS DECIMAL(30,10)) > 0
               THEN CAST(id AS DECIMAL(30,10)) / CAST(id AS DECIMAL(30,10))
               ELSE NULL END
    ORDER BY id
    """
    rows = _rows(sql, ["id", "ratio_c", "ratio_s"])
    assert [r[0] for r in rows] == [1, 2, 3, 4, 5, 6, 7, 8, 9], rows
    for _id, ratio_c, ratio_s in rows:
        assert ratio_c == Decimal("2.0000000000000000"), (_id, ratio_c)
        assert ratio_s == Decimal("1.0000000000000000"), (_id, ratio_s)


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"✅ {name}")
    print("All DECIMAL128 compare tests passed.")
