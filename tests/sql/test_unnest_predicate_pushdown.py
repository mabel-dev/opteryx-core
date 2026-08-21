"""Predicates above a CROSS JOIN UNNEST, when the condition is not a comparator.

`PredicateRewriteStrategy` runs BEFORE `PredicatePushdownStrategy` (see the order
in opteryx/planner/optimizer/__init__.py) and rewrites an ANCHORED LIKE/ILIKE
("x LIKE 'foo%'" / "x LIKE '%foo'") into a bare FUNCTION node — `_STARTS_WITH`,
`_ENDS_WITH`, `_CI_STARTS_WITH`, `_CI_ENDS_WITH`. A FUNCTION node carries
`parameters`; its left/right/centre are all None.

The Unnest branch of predicate pushdown guarded only the NOT case (`centre is not
None`) and then read `condition.left.schema_column.identity` unconditionally, so
every `CROSS JOIN UNNEST ... WHERE col LIKE '%x'` died with

    AttributeError: 'NoneType' object has no attribute 'schema_column'

Only the ANCHORED forms trigger it — the exact forms the rewriter optimizes.
Non-anchored patterns stay COMPARISON_OPERATOR nodes (`InStr`, `Like`, `RLike`)
and always worked, which is why this survived: the shapes nearest the bug pass.

Row counts alone would be a weak assertion here (a predicate silently dropped or
misplaced still returns *some* rows), so each anchored predicate is checked
against a regex equivalent that takes the untouched comparator path, and the
synthetic case is checked against ground truth computed in Python.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import opteryx

# Contiguous addresses, so the minimal CIDR cover has MIXED prefix lengths. A
# suffix LIKE then selects a real subset — with same-width blocks a dropped
# predicate and a working one both return everything, and the test proves nothing.
COVER = (
    "SELECT block FROM (SELECT CIDR_AGG(CAST(v AS IPV4)) AS blocks "
    "FROM GENERATE_SERIES(1, 200) AS v) AS agg CROSS JOIN UNNEST(agg.blocks) AS block"
)

MISSIONS = "SELECT mission FROM testdata.astronauts CROSS JOIN UNNEST(missions) AS mission"


def _values(sql):
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(sql):
        for i in range(morsel.num_rows):
            out.append(morsel[i][0])
    return sorted(out)


def test_anchored_like_over_unnest_does_not_crash_the_optimizer():
    """The reported failure: this raised AttributeError in predicate pushdown."""
    assert _values(f"{COVER} WHERE block LIKE '%/32'")


def test_every_anchored_form_matches_ground_truth():
    """Suffix, prefix and the case-insensitive variants, against Python."""
    cover = _values(COVER)
    assert len(cover) > 1, cover  # a one-block cover would make the subsets trivial

    cases = (
        ("block LIKE '%/32'", lambda b: b.endswith("/32")),
        ("block LIKE '%/30'", lambda b: b.endswith("/30")),
        ("block LIKE '0.0.0.1%'", lambda b: b.startswith("0.0.0.1")),
        ("block ILIKE '%/30'", lambda b: b.lower().endswith("/30")),
        ("block NOT LIKE '%/32'", lambda b: not b.endswith("/32")),
        ("block LIKE '%/32' AND LENGTH(block) > 3", lambda b: b.endswith("/32") and len(b) > 3),
    )
    for predicate, want_fn in cases:
        got = _values(f"{COVER} WHERE {predicate}")
        want = sorted(b for b in cover if want_fn(b))
        assert got == want, (predicate, got, want)


def test_anchored_and_regex_forms_agree_on_a_real_dataset():
    """The rewritten form must return what the un-rewritten form returns.

    RLIKE stays a COMPARISON_OPERATOR, so it never enters the broken branch — it
    is an independent oracle for the anchored LIKE/ILIKE results.
    """
    for anchored, regex in (
        ("mission LIKE 'Apollo%'", "mission RLIKE '^Apollo'"),
        ("mission ILIKE '%11'", "mission RLIKE '11$'"),
    ):
        assert _values(f"{MISSIONS} WHERE {anchored}") == _values(f"{MISSIONS} WHERE {regex}"), (
            anchored,
            regex,
        )


def test_non_anchored_predicates_still_work():
    """The shapes that always passed — a fix must not trade one branch for another."""
    cover = _values(COVER)
    for predicate, want_fn in (
        ("block LIKE '%0.0%'", lambda b: "0.0" in b),
        ("block RLIKE '.*/32'", lambda b: b.endswith("/32")),
        ("block IS NULL", lambda b: False),
        ("LENGTH(block) > 3", lambda b: len(b) > 3),
    ):
        got = _values(f"{COVER} WHERE {predicate}")
        want = sorted(b for b in cover if want_fn(b))
        assert got == want, (predicate, got, want)


# A CROSS JOIN UNNEST leg joined to a relation that carries its own
# column-vs-column filter. The predicate belongs to the OTHER leg entirely.
UNNEST_LEG = "(SELECT name FROM testdata.astronauts CROSS JOIN UNNEST(missions) AS m) a"


def _rows(sql):
    session = opteryx.session()
    return sum(morsel.num_rows for morsel in session.execute_to_morsels(sql))


def test_a_sibling_legs_predicate_is_not_placed_above_the_unnest():
    """A predicate belonging to another leg must not be moved by the Unnest arm.

    The optimizer walks a plan depth-first with ONE shared predicate list, so a
    predicate destined for one leg of a join rides into the other leg's subtree.
    The Unnest arm's column-vs-column clause (`query_columns == known_columns`)
    is trivially true for ANY `a.x = b.y` predicate — it carries no test that
    this unnest emits the columns the predicate reads — so it placed a sibling
    leg's filter directly above the unnest and the physical compile died with

        KeyError: expression references column b'$pl_gra_...' which the stream
        does not carry

    It reproduced in ONE of the two FROM orders only: placement happened when the
    DFS reached the unnest leg before the filter's own leg, which is why an
    UNNEST alongside a column-vs-column filter worked or crashed depending on how
    the query was written.

    Both orders are asserted, and against each other — a row count alone would
    pass for a predicate that had been silently dropped instead of misplaced.
    """
    for predicate in ("p.gravity = p.density", "p.gravity <> p.density", "p.mass > p.density"):
        unnest_first = _rows(
            f"SELECT a.name, p.name AS pn FROM {UNNEST_LEG}, $planets p WHERE {predicate}"
        )
        relation_first = _rows(
            f"SELECT a.name, p.name AS pn FROM $planets p, {UNNEST_LEG} WHERE {predicate}"
        )
        assert unnest_first == relation_first, (predicate, unnest_first, relation_first)

        # ... and both equal the cross product of the two legs' own row counts,
        # so the filter really was applied, to the leg that owns it.
        expanded = _rows(f"SELECT name FROM {UNNEST_LEG}")
        matching = _rows(f"SELECT name FROM $planets p WHERE {predicate}")
        assert unnest_first == expanded * matching, (
            predicate,
            unnest_first,
            expanded,
            matching,
        )


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"{name} ✅")
    print("done")
