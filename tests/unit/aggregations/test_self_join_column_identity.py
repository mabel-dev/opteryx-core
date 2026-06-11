"""Regression test for column-identity collisions across join legs.

The execution engine keys every column by its `schema_column.identity`. Pre-fix,
`SchemaColumn.identity` defaulted to the column *name*, so two distinct columns
that share a name in one row-space — every self-join, and any join of two tables
that happen to share a column name — collapsed onto a single identity. The join
operators then dropped (cross join) or shadowed (align_tables) one of them, and a
reference to one leg's column silently read the other leg's data.

The reported symptom was a grouped aggregate over a CROSS JOIN: per-group SUMs
were `9 * left.numberOfMoons[k]` instead of the constant 210 (the right leg's
`numberOfMoons` had collapsed onto the left's). The scalar form was only
*coincidentally* correct: sum(moons)*9 == 210*9 either way.

The fix mints a genuinely unique identity per column at construction (the
`name`-fallback was replaced with a fail-loud raise; see schema.py). These tests
guard the whole family: self cross/inner joins, and two distinct relations that
share a column name — virtual and physical.

$planets has 9 rows; numberOfMoons sums to 210.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import opteryx


def _rows(sql):
    s = opteryx.session()
    out = []
    for m in s.execute_to_morsels(sql):
        cols = {
            (n.decode() if isinstance(n, bytes) else n): m.column(n).to_pylist()
            for n in m.column_names
        }
        out = [dict(zip(cols.keys(), vals)) for vals in zip(*cols.values())]
    s.close()
    return out


def test_grouped_aggregate_over_cross_join():
    # The reported bug. Each p.id group sees all 9 p2 rows, so SUM(p2.moons)==210.
    rows = _rows(
        "SELECT p.id AS k, SUM(CAST(p2.numberOfMoons AS INTEGER)) AS s "
        "FROM $planets p CROSS JOIN $planets p2 GROUP BY p.id ORDER BY p.id"
    )
    assert len(rows) == 9, rows
    assert all(r["s"] == 210 for r in rows), rows


def test_cross_join_right_leg_column_is_not_aliased_to_left():
    # p2.name must cycle through all planets per p row, not mirror p.name.
    rows = _rows(
        "SELECT p.name AS pn, p2.name AS p2n "
        "FROM $planets p CROSS JOIN $planets p2 ORDER BY p.id, p2.id LIMIT 3"
    )
    # First three rows: p=Mercury fixed, p2 cycles Mercury, Venus, Earth.
    assert [r["pn"] for r in rows] == ["Mercury", "Mercury", "Mercury"], rows
    assert [r["p2n"] for r in rows] == ["Mercury", "Venus", "Earth"], rows


def test_self_inner_join_reads_correct_leg():
    # Match each planet to the planet whose id equals its moon count; the right
    # leg's id must be the matched row's, not a copy of the left's id.
    rows = _rows(
        "SELECT l.id AS lid, r.id AS rid "
        "FROM $planets l INNER JOIN $planets r ON l.numberOfMoons = r.id "
        "ORDER BY l.id"
    )
    # Every match must have rid == l.numberOfMoons (definitionally), and at least
    # one row where lid != rid (proving rid isn't just echoing lid).
    assert rows, rows
    assert any(r["lid"] != r["rid"] for r in rows), rows


def test_two_tables_sharing_a_column_name():
    # $planets and $variables both have a `name` column. v.name must return a
    # variable name, not the planet name from the left leg.
    rows = _rows(
        "SELECT p.id AS k, v.name AS vn "
        "FROM $planets p CROSS JOIN $variables v ORDER BY p.id LIMIT 3"
    )
    planet_names = {"Mercury", "Venus", "Earth", "Mars", "Jupiter",
                    "Saturn", "Uranus", "Neptune", "Pluto"}
    assert all(r["vn"] not in planet_names for r in rows), rows


def test_physical_self_cross_join():
    # Same guarantee on a real (parquet-backed) dataset, not just virtual data.
    rows = _rows(
        "SELECT a.name AS an, b.name AS bn "
        "FROM testdata.planets a CROSS JOIN testdata.planets b "
        "ORDER BY a.id, b.id LIMIT 3"
    )
    assert [r["bn"] for r in rows] == ["Mercury", "Venus", "Earth"], rows


def test_scalar_cross_join_unchanged():
    # The scalar form was coincidentally correct before the fix; it must stay so.
    rows = _rows("SELECT SUM(p2.numberOfMoons) AS s FROM $planets p CROSS JOIN $planets p2")
    assert rows == [{"s": 1890}], rows  # 210 * 9


if __name__ == "__main__":
    for fn in [
        test_grouped_aggregate_over_cross_join,
        test_cross_join_right_leg_column_is_not_aliased_to_left,
        test_self_inner_join_reads_correct_leg,
        test_two_tables_sharing_a_column_name,
        test_physical_self_cross_join,
        test_scalar_cross_join_unchanged,
    ]:
        fn()
        print("PASS", fn.__name__)
