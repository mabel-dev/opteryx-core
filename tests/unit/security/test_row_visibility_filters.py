"""
Test the permissions model is correctly allowing and blocking queries being executed

"""

import os
import sys
import decimal
import pytest

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import opteryx
from tests.helpers import execute_and_get_arrow, execute_and_get_rowcount, execute_and_get_shape, execute_and_fetch_all, execute_with_visibility_filters

test_cases = [
    # empty filters give no results
    ("SELECT * FROM $planets", {"$planets": []}, (0, 20)),
    # none filters give all results
    ("SELECT * FROM $planets", {"$planets": None}, (9, 20)),
    # special cases - booleans
    ("SELECT * FROM $planets", {"$planets": [(True, "Eq", True)]}, (9, 20)),
    ("SELECT * FROM $planets", {"$planets": [(False, "Eq", False)]}, (9, 20)),
    ("SELECT * FROM $planets", {"$planets": [(True, "Eq", False)]}, (0, 20)),
    ("SELECT * FROM $planets", {"$planets": [(True, "NotEq", True)]}, (0, 20)),

    # WHERE 'Apollo 11' = ANY(missions)
    ("SELECT * FROM testdata.astronauts", {"testdata.astronauts": [("missions", "AnyOpEq", "Apollo 11")]}, (3, 19)),

    ("SELECT * FROM $planets", {"testdata.planets": [("id", "Eq", 4)]}, (9, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4)]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4.0)]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", decimal.Decimal("4"))]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "NotEq", 4)]}, (8, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Gt", 4)]}, (5, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Lt", 4)]}, (3, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "InList", [1,2,3,4])]}, (4, 20)),
    ("SELECT * FROM $planets", {"$planets": [("name", "NotInList", ["Earth", "Mars"])]}, (7, 20)),
    ("SELECT * FROM $planets", {"$planets": [("gravity", "Lt", 10)]}, (7, 20)),
    ("SELECT * FROM $planets", {"$planets": [("name", "Eq", "Earth")]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [("name", "Like", "%a%")]}, (4, 20)),
    ("SELECT * FROM testdata.satellites", {"$planets": [("id", "Eq", 4)]}, (177, 8)),
    ("SELECT * FROM testdata.satellites", {"$planets": [("id", "NotEq", 4)]}, (177, 8)),
    ("SELECT * FROM testdata.satellites", {"$planets": [("id", "Gt", 4)]}, (177, 8)),
    ("SELECT * FROM testdata.satellites", {"$planets": [("id", "Lt", 4)]}, (177, 8)),
    ("SELECT * FROM testdata.satellites", {"testdata.satellites": [("id", "Eq", 4)]}, (1, 8)),
    ("SELECT * FROM testdata.satellites", {"testdata.satellites": [("id", "NotEq", 4)]}, (176, 8)),
    ("SELECT * FROM testdata.satellites", {"testdata.satellites": [("id", "Gt", 4)]}, (173, 8)),
    ("SELECT * FROM testdata.satellites", {"testdata.satellites": [("id", "Lt", 4)]}, (3, 8)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4)], "testdata.satellites": [("id", "Gt", 4)]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "NotEq", 4)], "testdata.satellites": [("id", "Gt", 4)]}, (8, 20)),

    ("SELECT * FROM $planets AS planets", {"planets": [("id", "Eq", 4)]}, (9, 20)),
    ("SELECT * FROM $planets AS p", {"$planets": [("id", "Eq", 4)]}, (1, 20)),

    ("SELECT * FROM $planets", {"$planets": [[("id", "Eq", 4), ("name", "Like", "M%")], [("id", "Gt", 7)]]}, (3, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("name", "Eq", "Earth"), ("id", "Eq", 4)], [("id", "Gt", 7)]]}, (2, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("id", "Eq", 4)], [("name", "Like", "M%")]]}, (2, 20)),

    # Joins. These project QUALIFIED columns rather than `SELECT *`: planets and
    # satellites share `id`/`name`/`density`, and a result carrying two columns with
    # the same output name is rejected by design (AmbiguousIdentifierError, raised in
    # opteryx/operators/exit/exit.pyx — callers must qualify or alias). These cases
    # were written before that decision and still expected the 28-/40-column
    # `SELECT *` results. What they exist to pin is the ROW COUNT — that a visibility
    # filter attaches to the right relation and survives each join type — and those
    # counts are unchanged from the originals.
    ("SELECT p.id, s.id AS satellite_id FROM $planets AS p INNER JOIN testdata.satellites AS s ON p.id = s.planetId", {"$planets": [("id", "Eq", 3)]}, (1, 2)),
    ("SELECT p.id, s.id AS satellite_id FROM $planets p LEFT JOIN testdata.satellites s ON p.id = s.planetId", {"$planets": [("id", "Gt", 3)], "testdata.satellites": [("id", "Lt", 10)]}, (12, 2)),
    ("SELECT p.id, s.id AS satellite_id FROM $planets p LEFT JOIN testdata.satellites s ON p.id = s.planetId",  {}, (179, 2)),
    ("SELECT p.id, s.id AS satellite_id FROM $planets p LEFT JOIN testdata.satellites s ON p.id = s.planetId",  {"testdata.satellites": [("id", "Lt", 4)]}, (10, 2)),

    ("SELECT p1.id, p2.id AS id2 FROM $planets p1 JOIN $planets p2 ON p1.id = p2.id", {"$planets": [("id", "Gt", 3)], "p2": [("name", "NotEq", "X")]}, (6, 2)),

    ("SELECT * FROM $planets WHERE id = 4", {"$planets": [("id", "Eq", 4)]}, (1, 20)),
    ("SELECT * FROM $planets WHERE name = 'Mars'", {"$planets": [("name", "Eq", "Mars")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE name LIKE 'M%'", {"$planets": [("name", "Like", "M%")]}, (2, 20)),
    ("SELECT * FROM $planets WHERE id > 3 AND name LIKE 'M%'", {"$planets": [("id", "Gt", 3), ("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id < 4 OR name LIKE 'M%'", {"$planets": [("id", "Lt", 4), ("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id = 4 AND name = 'Mars'", {"$planets": [("id", "Eq", 4), ("name", "Eq", "Mars")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id = 4 OR name = 'Mars'", {"$planets": [("id", "Eq", 4), ("name", "Eq", "Mars")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id = 4 AND name LIKE 'M%'", {"$planets": [("id", "Eq", 4), ("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE name LIKE 'M%'", {"$planets": [("id", "Eq", 4), ("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE id = 4", {"$planets": [("id", "Eq", 4), ("name", "NotLike", "M%")]}, (0, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4), ("name", "NotLike", "M%")]}, (0, 20)),

    # SQL and visibility filters together
    ("SELECT * FROM $planets WHERE id = 4", {"$planets": [("name", "Eq", "Earth")]}, (0, 20)),
    ("SELECT * FROM $planets WHERE id = 4", {"$planets": [("name", "Eq", "Mars")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE name = 'Mars'", {"$planets": [("name", "Like", "M%")]}, (1, 20)),  # SQL narrows
    ("SELECT * FROM $planets WHERE name = 'Mercury'", {"$planets": [("name", "Like", "M%")]}, (1, 20)),
    ("SELECT * FROM $planets WHERE name = 'Venus'", {"$planets": [("name", "Like", "M%")]}, (0, 20)),  # SQL blocks
    ("SELECT * FROM $planets WHERE id > 3", {"$planets": [("id", "Lt", 6)]}, (2, 20)),  # id in (4, 5)
    ("SELECT * FROM $planets WHERE id IN (3,4)", {"$planets": [("id", "NotEq", 4)]}, (1, 20)),  # only id=3 remains
    ("SELECT * FROM $planets WHERE id = 4", {"$planets": [("id", "NotEq", 4)]}, (0, 20)),
    ("SELECT * FROM $planets WHERE id = 4", {"$planets": [[("id", "Eq", 4)], [("id", "Eq", 5)]]}, (1, 20)),  # SQL restricts
    # Qualified projection for the same reason as the join block above.
    ("SELECT p.id, s.id AS satellite_id FROM $planets p JOIN testdata.satellites s ON p.id = s.planetId WHERE p.id = 4", {"testdata.satellites": [("id", "Gt", 5)]}, (0, 2)),

    # double list brackets
    ("SELECT * FROM $planets WHERE name LIKE 'M%'", {"$planets": [[("name", "Like", "M%")]]}, (2, 20)),
    ("SELECT * FROM $planets WHERE id > 3 AND name LIKE 'M%'", {"$planets": [[("id", "Gt", 3), ("name", "Like", "M%")]]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("name", "Eq", "Earth"), ("id", "Eq", 4)], [("id", "Gt", 7)]]}, (2, 20)),
    ("SELECT * FROM $planets", {"$planets": [[[("name", "Eq", "Earth"), ("id", "Eq", 4)]], [("id", "Gt", 7)]]}, (2, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("name", "Eq", "Earth"), ("id", "Eq", 4)], [[("id", "Gt", 7)]]]}, (2, 20)),

    # misformed filters
    ("SELECT * FROM $planets", {"$planets": [[[(("id", "Eq", 1) )]]]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [[[("id", "Eq", 1)]]]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [[[[("id", "Eq", 1)]]]]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [[[("id", "Eq", 1)]], [[("id", "Eq", 2)]]]}, (2, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("id", "Eq", 1)], [[("id", "Eq", 2)]]]}, (2, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("id", "Eq", 3), [[("name", "Eq", "Earth")]]]]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [[[[("name", "Eq", "Earth")], ("id", "Eq", 4)]], [("id", "Gt", 7)]]}, (2, 20)),
    ("SELECT * FROM $planets", {"$planets": [[[[(("id", "Eq", 1))]]]]}, (1, 20)),
    ("SELECT * FROM $planets", {"$planets": [[("id", "Eq", 4), ("name", "Eq", "Earth")], [[[[("id", "Gt", 7)]]]]]}, (2, 20)),

    # 1) Absorption: [A] absorbs [A ∧ B]  -> expect just Earth
    ("SELECT * FROM $planets", {"$planets": [[("name","Eq","Earth")], [("name","Eq","Earth"), ("id","Eq",3)]]}, (1, 20)),
    # 2) Duplicate predicate inside a clause -> dedup to single check
    ("SELECT * FROM $planets", {"$planets": [[("id","Eq",4), ("id","Eq",4)]]}, (1, 20)),
    # 3) Duplicate clause -> dedup whole clause set
    ("SELECT * FROM $planets", {"$planets": [[("id","Eq",4)], [("id","Eq",4)]]}, (1, 20)),
    # 4) Factoring with impossible residuals: name='Earth' ∧ (id=4 OR id=5) -> 0 rows
    ("SELECT * FROM $planets", {"$planets": [[("name","Eq","Earth"), ("id","Eq",4)], [("name","Eq","Earth"), ("id","Eq",5)]]}, (0, 20)),
    # 5) Tautology via OR: (id=4) OR (id<>4) -> all rows
    ("SELECT * FROM $planets", {"$planets": [[("id","Eq",4)], [("id","NotEq",4)]]}, (9, 20)),
    # 6) Contradiction clause absorbed by valid clause in OR
    # [(id=4 ∧ id=5), (id=5)] -> effectively id=5 only
    ("SELECT * FROM $planets", {"$planets": [[("id","Eq",4), ("id","Eq",5)], [("id","Eq",5)]]}, (1, 20)),
    # 7) Deeply factored nesting: name='Earth' AND (id=3 OR id=4) -> just Earth (id=3)
    ("SELECT * FROM $planets", {"$planets": [[[("name","Eq","Earth")], [[[("id","Eq",3)] , [("id","Eq",4)]]]]]}, (1, 20)),
    # 8) Extreme nesting noise around a single clause -> still id=3
    ("SELECT * FROM $planets", {"$planets": [[[[[("id","Eq",3)]]]]]}, (1, 20)),
    # 9) ANY OR ANY: ('Apollo 11' ANY missions) OR ('Gemini 8' ANY missions) -> 4 astronauts
    ("SELECT * FROM testdata.astronauts", {"testdata.astronauts": [[("missions","AnyOpEq","Apollo 11")], [("missions","AnyOpEq","Gemini 8")]]}, (4, 19)),
    # 10) ANY AND ANY: ('Apollo 11' ANY missions) AND ('Gemini 8' ANY missions) -> Armstrong only
    ("SELECT * FROM testdata.astronauts", {"testdata.astronauts": [[("missions","AnyOpEq","Apollo 11"), ("missions","AnyOpEq","Gemini 8")]]}, (1, 19)),

    # PATTERN KEYS. A key holding a glob metacharacter is fnmatched against the
    # relation name, so a caller can scope a namespace without enumerating it.
    ("SELECT * FROM $planets", {"$plan*": [("id", "Eq", 4)]}, (1, 20)),
    ("SELECT * FROM $planets", {"$pl?nets": [("id", "Eq", 4)]}, (1, 20)),
    ("SELECT * FROM $planets", {"$plane[st]s": [("id", "Eq", 4)]}, (1, 20)),
    ("SELECT * FROM $planets", {"*": [("id", "Eq", 4)]}, (1, 20)),
    ("SELECT * FROM testdata.satellites", {"testdata.*": [("id", "Eq", 4)]}, (1, 8)),
    # fnmatch's `*` spans the dots, so one key covers a whole namespace's depth.
    ("SELECT * FROM testdata.satellites", {"testdata.*": [("planetId", "Eq", 3)]}, (1, 8)),
    # A pattern that does not match leaves the relation alone.
    ("SELECT * FROM $planets", {"testdata.*": [("id", "Eq", 4)]}, (9, 20)),
    ("SELECT * FROM $planets", {"$satell*": [("id", "Eq", 4)]}, (9, 20)),
    # Matching is case-sensitive: relation names are.
    ("SELECT * FROM $planets", {"$PLAN*": [("id", "Eq", 4)]}, (9, 20)),
    # A pattern key valued None is "no filter", exactly as an exact key valued None is.
    ("SELECT * FROM $planets", {"$plan*": None}, (9, 20)),
    # A pattern deny-all denies, and cannot be widened by a permissive exact key.
    ("SELECT * FROM $planets", {"$plan*": []}, (0, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Eq", 4)], "$plan*": []}, (0, 20)),
    ("SELECT * FROM $planets", {"$planets": [], "$plan*": [("id", "Eq", 4)]}, (0, 20)),

    # EVERY matching key applies, and they are conjunctive - the most restrictive
    # wins, and a pattern can only narrow what an exact key allowed.
    ("SELECT * FROM $planets", {"$planets": [("id", "Gt", 3)], "$plan*": [("id", "Lt", 6)]}, (2, 20)),  # id in (4, 5)
    ("SELECT * FROM $planets", {"$pl*": [("id", "Gt", 3)], "*ets": [("id", "Lt", 6)]}, (2, 20)),
    ("SELECT * FROM $planets", {"$planets": [("id", "Gt", 3)], "$plan*": [("id", "Lt", 2)]}, (0, 20)),
    # An exact key valued None does not suppress a pattern that matches.
    ("SELECT * FROM $planets", {"$planets": None, "$pl*": [("id", "Eq", 4)]}, (1, 20)),
    # Three keys over one relation, all applied.
    ("SELECT * FROM $planets", {"$planets": [("id", "Gt", 2)], "$pl*": [("id", "Lt", 8)], "*ets": [("name", "Like", "M%")]}, (1, 20)),

    # Patterns attach per-relation in a join, same as exact keys.
    ("SELECT p.id, s.id AS satellite_id FROM $planets AS p INNER JOIN testdata.satellites AS s ON p.id = s.planetId", {"$plan*": [("id", "Eq", 3)]}, (1, 2)),
    ("SELECT p.id, s.id AS satellite_id FROM $planets p LEFT JOIN testdata.satellites s ON p.id = s.planetId", {"$plan*": [("id", "Gt", 3)], "testdata.*": [("id", "Lt", 10)]}, (12, 2)),
    # A pattern key does not match an alias, for the same reason an exact key does not.
    ("SELECT * FROM $planets AS p", {"p*": [("id", "Eq", 4)]}, (9, 20)),

]


@pytest.mark.parametrize("sql, filters, shape", test_cases)
def test_visibility_filters(sql, filters, shape):
    """test we can stop users performing some query types"""

    result_shape = execute_with_visibility_filters(sql, filters)
    assert result_shape == shape, result_shape


def test_pattern_match_on_a_relation_without_the_column_fails_rather_than_serves():
    """The property that makes a namespace pattern safe to rely on.

    A pattern is written once and then covers relations that do not exist yet. If a
    relation lands under a covered namespace without the column the filter scopes on,
    the only two options are to serve its rows unscoped or to refuse the query -- and
    the whole reason to prefer patterns over an enumerated list is that the enumerated
    list fails the first way, silently. This pins the second.
    """
    from opteryx.exceptions import ColumnNotFoundError

    with pytest.raises(ColumnNotFoundError):
        execute_with_visibility_filters(
            "SELECT * FROM $planets", {"$plan*": [("billing_account", "Eq", "acme")]}
        )


def test_a_key_with_no_metacharacters_is_never_treated_as_a_pattern():
    """Exact keys stay an O(1) dict lookup: they are not fnmatched, so a relation name
    is not silently able to match some other relation's key."""
    from opteryx.planner.logical_planner.logical_planner import (
        VISIBILITY_PATTERN_CHARACTERS,
    )

    assert not any(c in "opteryx.ops.billing" for c in VISIBILITY_PATTERN_CHARACTERS)
    # `$planets` holds no metacharacter, so it cannot act as a pattern over `$planet`.
    assert execute_with_visibility_filters(
        "SELECT * FROM $planets", {"$planet": [("id", "Eq", 4)]}
    ) == (9, 20)


if __name__ == "__main__":  # pragma: no cover

    import shutil
    import time

    start_suite = time.monotonic_ns()
    passed = 0
    failed = 0

    width = shutil.get_terminal_size((80, 20))[0] - 15

    print(f"RUNNING BATTERY OF {len(test_cases)} TESTS")
    for index, (sql, filters, shape) in enumerate(test_cases):
        print(
            f"\033[38;2;255;184;108m{(index + 1):04}\033[0m"
            f" .",
            end="",
            flush=True,
        )

        try:
            start = time.monotonic_ns()
            test_visibility_filters(sql, filters, shape)
            print(
                f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅",
                end="",
            )
            passed += 1
            if failed > 0:
                print(" \033[0;31m*\033[0m")
            else:
                print()
        except Exception as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(5)}ms ❌ *\033[0m")
            print(">", sql, filters, shape, err)
            failed += 1

    print("--- ✅ \033[0;32mdone\033[0m")

    if failed > 0:
        print("\n\033[38;2;139;233;253m\033[3mFAILURES\033[0m")

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
