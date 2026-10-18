"""An outer WHERE over `CROSS JOIN UNNEST(<function expression>)`.

The Unnest arm of `PredicatePushdownStrategy` decided whether a predicate could be
pushed BELOW the unnest by testing `predicate.relations` against
`node.unnest_column.source`. Only an IDENTIFIER has a `.source`; for
`UNNEST(SPLIT(e.cve_ids, ','))` the unnested column is a Function, so every query of
the form

    SELECT ... FROM t AS e CROSS JOIN UNNEST(SPLIT(e.col, ',')) AS x WHERE e.col ...

died in the optimizer with

    AttributeError: 'Function' object has no attribute 'source'

The same query without the outer WHERE, or with the filter moved into a subquery,
worked, which is how it went unnoticed.

Semantics under test: a predicate that reads only columns of the unnest's INPUT
stream may be pushed below the unnest; one that reads the unnest OUTPUT must stay
above (or be folded into it). Each case is checked against ground truth computed in
Python from the untouched table, and against the subquery form, because a row count
alone passes for a predicate that was silently dropped.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx

SOURCE = "testdata.public.exploit_db"
UNNESTED = f"{SOURCE} AS e CROSS JOIN UNNEST(SPLIT(e.cve_ids, ',')) AS cve"


def _rows(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(tuple(morsel[i]))
    return sorted(out, key=repr)


def _expected(keep_row, keep_value=lambda value: True):
    """(exploit_id, cve) pairs computed in Python from the un-unnested table."""
    out = []
    for exploit_id, cve_ids in _rows(f"SELECT exploit_id, cve_ids FROM {SOURCE}"):
        if cve_ids is None or not keep_row(cve_ids):
            continue
        for value in cve_ids.split(","):
            if keep_value(value):
                out.append((exploit_id, value))
    return sorted(out, key=repr)


def test_predicate_on_input_column_over_unnest_of_a_function():
    """The reported failure: this raised AttributeError in predicate pushdown."""
    got = _rows(f"SELECT e.exploit_id, cve FROM {UNNESTED} WHERE e.cve_ids LIKE '%,%'")
    assert got
    assert got == _expected(lambda ids: "," in ids)


def test_input_column_predicate_agrees_with_the_subquery_form():
    got = _rows(f"SELECT e.exploit_id, cve FROM {UNNESTED} WHERE e.cve_ids LIKE '%,%'")
    want = _rows(
        "SELECT e.exploit_id, cve FROM "
        f"(SELECT * FROM {SOURCE} WHERE cve_ids LIKE '%,%') AS e "
        "CROSS JOIN UNNEST(SPLIT(e.cve_ids, ',')) AS cve"
    )
    assert got == want


def test_predicate_on_the_unnest_output_stays_above():
    """A predicate reading the unnested value cannot be pushed below the unnest."""
    got = _rows(f"SELECT e.exploit_id, cve FROM {UNNESTED} WHERE cve LIKE 'CVE-2009%'")
    assert got
    assert got == _expected(lambda ids: True, lambda value: value.startswith("CVE-2009"))


def test_input_and_output_predicates_together():
    """Each predicate lands on its own side of the unnest and both are applied."""
    got = _rows(
        f"SELECT e.exploit_id, cve FROM {UNNESTED} "
        "WHERE e.cve_ids LIKE '%,%' AND cve LIKE 'CVE-2009%'"
    )
    assert got
    assert got == _expected(lambda ids: "," in ids, lambda value: value.startswith("CVE-2009"))


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
