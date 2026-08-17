"""Value-level regression tests for EXISTS/IN subqueries that are not a bare
top-level `WHERE` conjunct.

Two separate defects/gaps lived behind one error message
("An EXISTS/IN subquery is only supported as a top-level condition of the
WHERE clause..."), surfaced by four TPC-DS smoke-suite queries
(tests/performance/tpcds/opteryx/queries/query{10,35,45,69}.sql):

1. TRIVIAL — `decorrelate_subquery.py`'s `_is_removable_conjunct`/`_split_out`
   walked the AND-chain without unwrapping `NESTED` (parenthesis) nodes, so a
   PARENTHESIZED AND-of-ANDs (`x AND (NOT EXISTS (...) AND NOT EXISTS (...))`
   — TPC-DS Q69) was refused as "nested under NOT/OR" even though it is
   neither: it is one flat AND chain, just parenthesized. Fixed by unwrapping
   `NESTED` at every level of that walk.

2. GENUINE — `x = 1 OR EXISTS (...)` (TPC-DS Q10/Q35/Q45's shape: two EXISTS
   OR'd together, or an IN OR'd with an unrelated condition) cannot become a
   SEMI/ANTI join at all: a filtering join can only replace a condition that
   is exactly the WHERE clause (or a top-level AND conjunct of it), because
   the join filters ROWS, and a disjunct cannot be expressed that way. This is
   a NEW lowering, not a bug fix: `_materialize_boolean_value` turns the
   EXISTS/IN into a boolean VALUE instead — COUNT(*) grouped by the
   correlation key, reached by a LEFT JOIN so a non-matching outer row
   SURVIVES (instead of an INNER/SEMI/ANTI join silently dropping it), then
   substituted in place as `<count> IS NOT NULL`.

The tests below assert on ACTUAL ROW SETS, not "does not raise". Every
expected value was independently hand-derived from `$planets`'s real data
(gravity/mass columns queried directly, see comments) or cross-checked
against DuckDB reading the same TPC-DS parquet fixtures — never guessed from
the implementation.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx
from opteryx.exceptions import UnsupportedSyntaxError


def _first_col(sql):
    """Every row's first column, as a sorted list — enough to identify which
    planets/ids survived a WHERE clause without depending on column naming."""
    session = opteryx.session()
    values = []
    for morsel in session.execute_to_morsels(sql):
        if morsel is None or not hasattr(morsel, "num_rows") or morsel.num_rows == 0:
            continue
        column = list(morsel.to_arrow().to_pydict().values())[0]
        values.extend(column)
    return sorted(values)


# $planets, by id: gravity > 10 is {5: Jupiter (23.1), 8: Neptune (11.0)}.
# mass < 1 is {1: Mercury (0.33), 4: Mars (0.642), 9: Pluto (0.0146)}.
# (SELECT id, name, gravity, mass FROM $planets ORDER BY id) — verified directly.


def test_and_of_parenthesized_and_flattens():
    """TPC-DS Q69's shape: `x AND (NOT EXISTS (...) AND NOT EXISTS (...))`.

    The parenthesized group is a flat AND chain, not a nested one — both NOT
    EXISTS must be individually AND'able out into SEMI/ANTI joins, same as if
    the parens were never there. Only id=5 has neither gravity>10 (fails the
    first NOT EXISTS for 5 and 8) nor mass<1 (fails the second for 1, 4, 9):
    NOT({5,8}) intersect NOT({1,4,9}) = {2,3,6,7} intersect {2,3,5,6,7,8} = {2,3,6,7}.
    """
    rows = _first_col(
        "SELECT p.id FROM $planets p WHERE p.id > 0 AND ("
        "NOT EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.gravity > 10) "
        "AND NOT EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.mass < 1))"
    )
    assert rows == [2, 3, 6, 7], rows

    # Unparenthesized, the same predicate already worked before this fix — pin
    # that the parenthesized form now answers IDENTICALLY, not just "runs".
    unparenthesized = _first_col(
        "SELECT p.id FROM $planets p WHERE p.id > 0 "
        "AND NOT EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.gravity > 10) "
        "AND NOT EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.mass < 1)"
    )
    assert unparenthesized == rows, (unparenthesized, rows)


def test_exists_under_or_keeps_a_row_with_no_match():
    """The correctness property the LEFT JOIN materialization exists for.

    id=9 (Pluto) has gravity 0.7 — the correlated EXISTS branch has NO match
    for it. If that branch were lowered as an INNER/SEMI join (the wrong
    shape for a disjunct), the row would vanish before the `OR` ever got to
    evaluate `p.id = 9`. The LEFT JOIN lets it survive with a NULL count, so
    `... IS NOT NULL` correctly comes out FALSE and the `OR`'s other side
    still saves the row.
    """
    rows = _first_col(
        "SELECT p.id FROM $planets p WHERE p.id = 9 "
        "OR EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.gravity > 10)"
    )
    # {5, 8}: gravity > 10.  {9}: explicit predicate, despite no EXISTS match.
    assert rows == [5, 8, 9], rows


def test_two_exists_or_together():
    """TPC-DS Q10/Q35's exact shape: two correlated EXISTS OR'd together."""
    rows = _first_col(
        "SELECT p.id FROM $planets p WHERE "
        "EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.gravity > 10) "
        "OR EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.mass < 1)"
    )
    assert rows == [1, 4, 5, 8, 9], rows


def test_not_exists_under_or():
    """The negated form: `IS NULL` rather than `IS NOT NULL` after the join."""
    rows = _first_col(
        "SELECT p.id FROM $planets p WHERE p.id = 1 "
        "OR NOT EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.gravity > 10)"
    )
    # Everything except {5, 8} (gravity > 10); id=1 is already in that set.
    assert rows == [1, 2, 3, 4, 6, 7, 9], rows


def test_in_under_or():
    """TPC-DS Q45's shape: an uncorrelated IN subquery OR'd with an unrelated
    condition (`SUBSTRING(ca_zip,1,5) IN (...) OR i_item_id IN (subquery)`)."""
    rows = _first_col(
        "SELECT p.id FROM $planets p WHERE p.id = 1 "
        "OR p.name IN (SELECT name FROM $planets WHERE gravity > 10)"
    )
    assert rows == [1, 5, 8], rows


def test_not_in_under_or_is_still_refused():
    """NOT IN's NULL-aware "any NULL makes every row UNKNOWN" semantics has no
    equivalent in the COUNT(*)-based materialization — refused by name, not
    silently answered with plain (non-NULL-aware) semantics."""
    sql = (
        "SELECT p.id FROM $planets p WHERE p.id = 1 "
        "OR p.name NOT IN (SELECT name FROM $planets WHERE id > 5)"
    )
    try:
        _first_col(sql)
    except UnsupportedSyntaxError as err:
        assert "IN" in str(err) and "top-level" in str(err), str(err)
        return
    raise AssertionError("NOT IN under OR was answered instead of refused")


def test_correlated_non_equality_under_or_is_still_refused():
    """A correlated non-equality rode the SEMI/ANTI probe as a per-candidate
    residual; there is no such probe once this becomes GROUP BY + JOIN, so it
    must stay refused rather than silently dropped or silently answered."""
    sql = (
        "SELECT p.id FROM $planets p WHERE p.id = 1 "
        "OR EXISTS (SELECT 1 FROM $planets q WHERE q.id = p.id AND q.mass > p.mass)"
    )
    try:
        _first_col(sql)
    except UnsupportedSyntaxError as err:
        assert "EXISTS" in str(err), str(err)
        return
    raise AssertionError("a correlated non-equality under OR was answered instead of refused")


def test_tpcds_q69_shape_matches_duckdb():
    """The actual TPC-DS Q69 shape (customer/customer_address/customer_demographics
    with a parenthesized AND of two NOT EXISTS), against the SF0.01 fixture.
    Row count cross-checked against DuckDB reading the same parquet files:
    8 rows — this pins that count so a future change cannot silently alter it."""
    rows = _first_col(
        "SELECT cd_gender FROM testdata.tpcds_001.customer c, "
        "testdata.tpcds_001.customer_address ca, testdata.tpcds_001.customer_demographics "
        "WHERE c.c_current_addr_sk = ca.ca_address_sk "
        "AND ca_state IN ('KY', 'GA', 'NM') "
        "AND cd_demo_sk = c.c_current_cdemo_sk "
        "AND EXISTS (SELECT * FROM testdata.tpcds_001.store_sales, testdata.tpcds_001.date_dim "
        "  WHERE c.c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk "
        "  AND d_year = 2001 AND d_moy BETWEEN 4 AND 6) "
        "AND (NOT EXISTS (SELECT * FROM testdata.tpcds_001.web_sales, testdata.tpcds_001.date_dim "
        "    WHERE c.c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk "
        "    AND d_year = 2001 AND d_moy BETWEEN 4 AND 6) "
        "  AND NOT EXISTS (SELECT * FROM testdata.tpcds_001.catalog_sales, testdata.tpcds_001.date_dim "
        "    WHERE c.c_customer_sk = cs_ship_customer_sk AND cs_sold_date_sk = d_date_sk "
        "    AND d_year = 2001 AND d_moy BETWEEN 4 AND 6))"
    )
    assert len(rows) == 8, rows


def test_tpcds_q35_shape_matches_duckdb():
    """TPC-DS Q35's shape: EXISTS AND'ed, then a parenthesized OR of two more
    EXISTS. Row count cross-checked against DuckDB: 12 rows."""
    rows = _first_col(
        "SELECT ca_state FROM testdata.tpcds_001.customer c, "
        "testdata.tpcds_001.customer_address ca, testdata.tpcds_001.customer_demographics "
        "WHERE c.c_current_addr_sk = ca.ca_address_sk "
        "AND cd_demo_sk = c.c_current_cdemo_sk "
        "AND EXISTS (SELECT * FROM testdata.tpcds_001.store_sales, testdata.tpcds_001.date_dim "
        "  WHERE c.c_customer_sk = ss_customer_sk AND ss_sold_date_sk = d_date_sk "
        "  AND d_year = 2002 AND d_qoy < 4) "
        "AND (EXISTS (SELECT * FROM testdata.tpcds_001.web_sales, testdata.tpcds_001.date_dim "
        "    WHERE c.c_customer_sk = ws_bill_customer_sk AND ws_sold_date_sk = d_date_sk "
        "    AND d_year = 2002 AND d_qoy < 4) "
        "  OR EXISTS (SELECT * FROM testdata.tpcds_001.catalog_sales, testdata.tpcds_001.date_dim "
        "    WHERE c.c_customer_sk = cs_ship_customer_sk AND cs_sold_date_sk = d_date_sk "
        "    AND d_year = 2002 AND d_qoy < 4))"
    )
    assert len(rows) == 12, rows


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
