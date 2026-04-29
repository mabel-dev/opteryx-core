"""
The best way to test a SQL engine is to throw queries at it.

This tests that various optimizations don't break query correctness.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
import pytest

from tests.helpers import execute_and_get_rowcount
from opteryx.utils.formatter import format_sql
from tests import trunc_printable

# fmt:off
STATEMENTS = [
        ("SELECT * FROM $planets WHERE NOT id != 4", 1),
        ("SELECT * FROM $planets WHERE id = 4 + 4", 1),
        ("SELECT * FROM $planets WHERE id * 0 = 1", 0),
        ("SELECT id ^ 1 = 1 FROM $planets LIMIT 10", 9),
        ("SELECT name FROM testdata.astronauts WHERE name = 'Neil A. Armstrong'", 1),
        ("SELECT name FROM $planets WHERE name LIKE '%'", 9),
        ("SELECT name FROM $planets WHERE name ILIKE '%'", 9),
        ("SELECT name FROM $planets WHERE name ILIKE '%th%'", 2),
        ("SELECT name FROM $planets WHERE NOT name NOT ILIKE '%th%'", 7),
        ("SELECT * FROM $planets WHERE NOT name != 'Earth'", 1),
        ("SELECT CASE WHEN surface_pressure IS NULL THEN -100.00 ELSE surface_pressure END FROM $planets", 9),
        ("SELECT * FROM testdata.satellites INNER JOIN $planets ON planetId = $planets.id", 177),
        ("SELECT name FROM testdata.astronauts WHERE 'MIT' = ANY(alma_mater) OR 'Stanford' = ANY(alma_mater) OR 'Harvard' = ANY(alma_mater)", 2),
        ("SELECT name FROM testdata.astronauts WHERE 'Apollo 13' = ANY(missions) AND 'Gemini 8' = ANY(missions)", 1),
        ("SELECT * FROM $planets WHERE id > 5 AND name = 'Earth' AND id < 10", 0),
        ("SELECT * FROM $planets WHERE NOT(id = 1 OR id = 2 OR id = 3)", 6),
        ("SELECT * FROM $planets WHERE NOT(id = 1 OR id = 2 OR id = 3 OR id = 4)", 5),
        ("SELECT * FROM $planets WHERE id > 1 AND id > 3", 6),
        ("SELECT * FROM $planets WHERE id < 8 AND id < 5", 4),
        ("SELECT * FROM $planets WHERE id > 1 AND id < 8 AND id > 3 AND id < 7", 2),
    ]
# fmt:on


@pytest.mark.parametrize("statement, expected_rows", STATEMENTS)
def test_optimization_invoked(statement, expected_rows):
    """
    Test that optimizations don't break query correctness
    """
    count = execute_and_get_rowcount(statement)
    assert count == expected_rows, f"Expected {expected_rows} rows, got {count}"


if __name__ == "__main__":  # pragma: no cover
    import shutil
    print(f"RUNNING BATTERY OF {len(STATEMENTS)} OPTIMIZER TESTS")

    width = shutil.get_terminal_size((80, 20))[0] - 15
    for index, (statement, expected_rows) in enumerate(STATEMENTS):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" {trunc_printable(format_sql(statement), width - 1)}",
            end="",
            flush=True,
        )
        test_optimization_invoked(statement, expected_rows)
        print()

    print("✅ okay")
