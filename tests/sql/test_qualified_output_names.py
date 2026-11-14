"""An un-aliased qualified column reference (`t.col`) is named `col`, as in standard SQL.

Duplicate output names are refused (results are written to parquet, which reads columns
back by name), so when two outputs would share a bare name, those keep their qualified
names — the rule `SELECT *` over a join already follows.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import pytest

import opteryx
from opteryx.exceptions import AmbiguousIdentifierError

JOIN = "FROM $planets AS p JOIN testdata.satellites AS s ON p.id = s.planetId"


def _names(sql):
    names = None
    for morsel in opteryx.session().execute_to_morsels(sql):
        names = [n.decode() if isinstance(n, bytes) else n for n in morsel.column_names]
    return names


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT p.id FROM $planets AS p", ["id"]),
        ("SELECT $planets.id FROM $planets", ["id"]),
        ("SELECT p.name, COUNT(*) FROM $planets AS p GROUP BY p.name", ["name", "COUNT(*)"]),
        (f"SELECT p.id, s.name {JOIN}", ["id", "name"]),
        # collisions keep the qualifier; only the colliding pair is qualified
        (f"SELECT p.name, s.name, s.radius {JOIN}", ["p.name", "s.name", "radius"]),
        (f"SELECT p.id AS id, s.id {JOIN}", ["id", "s.id"]),
        # aliases and expressions are untouched
        ("SELECT p.id AS k FROM $planets AS p", ["k"]),
        ("SELECT p.id + 1 FROM $planets AS p", ["p.id + 1"]),
    ],
)
def test_qualified_reference_output_name(sql, expected):
    assert _names(sql) == expected


def test_same_column_twice_is_still_refused():
    with pytest.raises(AmbiguousIdentifierError):
        _names("SELECT p.id, p.id FROM $planets AS p")


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
