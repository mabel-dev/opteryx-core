"""
Generator for the predicate-subquery fuzzer.

WHY A THIRD GENERATOR
---------------------
Neither existing fuzzer emits a predicate subquery. Measured over 4,000
statements from `single_table_grammar.generate`: EXISTS 0%, IN (subquery) 0%,
scalar subquery 0%, correlated anything 0%. Derived tables and CTEs are covered
at ~10% each, so the hole is specifically the PREDICATE subquery family — the
one that goes through `DecorrelateSubqueryStrategy` and
`CorrelatedFiltersStrategy`.

The shape is structurally different from both existing generators, which is why
it needs its own file rather than another CORPUS entry: this one builds TWO
relations in a NESTED SCOPE linked by a correlation predicate, not one relation
(single-table) and not two relations joined by ON (join fuzzer).

WHAT MAKES THIS FAMILY WORTH A HARNESS
--------------------------------------
An exact oracle. Every correlated predicate form has a join rewrite that the
engine performs internally, so both spellings can be generated from the same
case and required to agree:

    EXISTS (SELECT 1 FROM i WHERE i.k = o.k)   ==  o SEMI JOIN i ON i.k = o.k
    NOT EXISTS (...)                           ==  o ANTI JOIN i ON i.k = o.k
    o.k IN (SELECT k FROM i)                   ==  o SEMI JOIN i ON i.k = o.k
    o.k <cmp> (SELECT AGG(v) FROM i WHERE ...) ==  o LEFT JOIN (grouped agg) ...

`NOT IN` deliberately has no unconditional rewrite. It is NOT an anti join: one
NULL anywhere in the inner key makes the predicate UNKNOWN for every outer row,
and a NULL OUTER key is dropped by `NOT IN` but kept by an anti join. Both
distinctions are generated and asserted — see `not_in_null_semantics` in
`subquery_oracles.py`. Getting either wrong is a classic engine bug, so the
generator manufactures NULL keys on purpose (`NULLIF` in a derived relation)
rather than hoping the corpus supplies them.

THE CORRELATION PAIRS ARE VERIFIED, NOT ASSUMED
-----------------------------------------------
`load_pairs()` measures every declared pair against the live engine and REFUSES
one whose semi join is vacuous — 0 rows, or every outer row. A pair that selects
nothing makes every oracle in the file compare `[] == []`, which is the exact
failure mode the fuzzing rewrite exists to remove: an oracle that cannot fail is
indistinguishable from one that passes. So the discriminating property is a load
-time assertion, not a hope about the test data.

WHAT THE ENGINE SUPPORTS
------------------------
`SUPPORT_MATRIX` below records what was measured, in runnable form, and
`test_engine_support_matrix_is_current` re-measures it on every run. The list
is not a comment that can rot: a form that gains support turns the matrix red
and has to be moved into the generator.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from typing import FrozenSet
from typing import Optional
from typing import Sequence
from typing import Tuple

from tests.fuzzing.single_table_grammar import Ty

# Aliases carry this prefix for the same reason `single_table_grammar.Names`
# uses `oz_`: the defect register matches some entries on "Unknown column
# '<alias>'", and a bare `o`/`i` namespace would let such a signature also match
# a real column. Nothing in the corpus is spelled `sq_`.
OUTER = "sq_o"
INNER = "sq_i"
JOINED = "sq_j"


# ─────────────────────────────────────────────────────────────────────────────
# Relations
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Side:
    """One end of a correlation: a FROM source and the columns it exposes.

    `sql` is anything legal after FROM — a base relation name, or a
    parenthesised derived table. The derived form is what lets this generator
    manufacture a NULL key (`NULLIF`) over test data that has none where it is
    needed, without regenerating the corpus.
    """

    sql: str
    key: str  # the correlation key, exposed by `sql`
    ty: Ty
    columns: Tuple[str, ...]  # projectable columns, INCLUDING the key
    filters: Tuple[str, ...] = ()  # predicate templates; `{}` takes the alias
    #: Predicate templates that match NOTHING. Held apart from `filters` and
    #: tagged separately when drawn, because "the inner set is empty" is a shape
    #: two things care about: `not_in_null_semantics` has a branch that is
    #: unreachable without one, and the defect register scopes an entry to it.
    #: An oracle branch or a register entry that can only be reached by accident
    #: is one nobody can reason about.
    empty_filters: Tuple[str, ...] = ()
    values: Tuple[str, ...] = ()  # numeric columns usable as an aggregate input

    def __post_init__(self) -> None:
        if self.key not in self.columns:
            raise AssertionError(
                f"correlation key {self.key!r} is not among the columns {self.sql} exposes"
            )


@dataclass
class Pair:
    """Two relations that share a key, plus what was measured about them.

    The measured fields are filled by `load_pairs()`. They are on the pair
    rather than recomputed per case because the NOT IN oracle needs the
    inner key's nullability, and asking the engine once per case would triple
    that oracle's query count for a fact that cannot change during a run.
    """

    name: str
    outer: Side
    inner: Side
    weight: int
    # An outer column comparable against a numeric aggregate of the inner side.
    # None means this pair does not generate the scalar-subquery forms.
    scalar_outer: Optional[str] = None

    outer_rows: int = -1
    semi_rows: int = -1
    outer_key_nulls: int = -1
    inner_key_nulls: int = -1


# The corpus. Weights, not a uniform choice: every oracle runs at least two
# queries, so a 200,000-row outer relation has to stay rare or the nightly run
# never finishes. It is still present — `wide` is the only relation here that
# crosses a morsel boundary, and a decorrelated join that is correct on one
# morsel and wrong across four is exactly the bug worth catching.
#
# Every pair is re-measured at load. The counts in the comments are what was
# observed when each was added; they are documentation, and `load_pairs` is the
# thing that actually holds them honest.
PAIRS: Tuple[Pair, ...] = (
    Pair(
        name="planets/satellites",  # 9 outer rows, 7 match
        weight=20,
        outer=Side(
            sql="testdata.planets",
            key="id",
            ty=Ty.INTEGER,
            columns=("id", "name", "number_of_moons", "mean_temperature"),
            filters=("{}.id > 3", "{}.number_of_moons > 0", "{}.mean_temperature < 0"),
        ),
        inner=Side(
            sql="testdata.satellites",
            key="planetId",
            ty=Ty.INTEGER,
            columns=("id", "planetId", "name", "radius", "magnitude"),
            filters=("{}.radius > 100.0", "{}.magnitude < 10.0", "{}.id > 50"),
            empty_filters=("{}.radius > 1000000.0",),
            values=("id", "radius", "magnitude"),
        ),
        scalar_outer="id",
    ),
    Pair(
        name="planets/satellites-null-inner-key",  # inner key has 14 NULLs
        weight=15,
        outer=Side(
            sql="testdata.planets",
            key="id",
            ty=Ty.INTEGER,
            columns=("id", "name", "number_of_moons"),
            filters=("{}.id > 3", "{}.number_of_moons > 0"),
        ),
        # NULLIF manufactures the null-key case the corpus does not contain.
        # It is the ONLY way `NOT IN`'s null-collapse branch gets exercised.
        inner=Side(
            sql="(SELECT NULLIF(planetId, 8) AS sq_key, id, radius FROM testdata.satellites)",
            key="sq_key",
            ty=Ty.INTEGER,
            columns=("sq_key", "id", "radius"),
            filters=("{}.radius > 100.0", "{}.id > 50"),
            values=("id", "radius"),
        ),
        scalar_outer="id",
    ),
    Pair(
        name="planets-null-outer-key/satellites",  # outer key has 1 NULL
        weight=15,
        outer=Side(
            sql=(
                "(SELECT NULLIF(id, 5) AS sq_key, name, number_of_moons "
                "FROM testdata.planets)"
            ),
            key="sq_key",
            ty=Ty.INTEGER,
            columns=("sq_key", "name", "number_of_moons"),
            filters=("{}.number_of_moons > 0",),
        ),
        inner=Side(
            sql="testdata.satellites",
            key="planetId",
            ty=Ty.INTEGER,
            columns=("id", "planetId", "name", "radius"),
            filters=("{}.radius > 100.0", "{}.id > 50"),
            # Paired with a NULL OUTER key, which is the one combination where
            # NOT IN's empty-set branch and its anti-join branch disagree: over
            # an empty set the NULL-keyed row SURVIVES, because nothing was ever
            # compared to it.
            empty_filters=("{}.radius > 1000000.0",),
            values=("id", "radius"),
        ),
        scalar_outer="sq_key",
    ),
    Pair(
        name="mixed/planets",  # 2,000 outer rows, 1,106 match
        weight=20,
        outer=Side(
            sql="testdata.fuzzing.mixed",
            key="i_group",
            ty=Ty.INTEGER,
            columns=("row_id", "i_group", "s_low", "f_value", "b_value"),
            filters=("{}.row_id > 1000", "{}.b_value = TRUE", "{}.f_value > 0.0"),
        ),
        inner=Side(
            sql="testdata.planets",
            key="id",
            ty=Ty.INTEGER,
            columns=("id", "name", "number_of_moons", "diameter"),
            filters=("{}.id > 3", "{}.number_of_moons > 0"),
            values=("id", "number_of_moons", "diameter"),
        ),
        scalar_outer="i_group",
    ),
    Pair(
        name="mixed-null-outer-key/planets",  # outer key has 132 NULLs
        weight=15,
        outer=Side(
            sql=(
                "(SELECT NULLIF(i_group, 3) AS sq_key, row_id, s_low "
                "FROM testdata.fuzzing.mixed)"
            ),
            key="sq_key",
            ty=Ty.INTEGER,
            columns=("sq_key", "row_id", "s_low"),
            filters=("{}.row_id > 1000",),
        ),
        inner=Side(
            sql="testdata.planets",
            key="id",
            ty=Ty.INTEGER,
            columns=("id", "name", "number_of_moons"),
            filters=("{}.id > 3",),
            values=("id", "number_of_moons"),
        ),
        scalar_outer="sq_key",
    ),
    Pair(
        name="mixed/satellites",  # 2,000 outer rows, 853 match
        weight=15,
        outer=Side(
            sql="testdata.fuzzing.mixed",
            key="i_group",
            ty=Ty.INTEGER,
            columns=("row_id", "i_group", "s_low", "i_value"),
            filters=("{}.row_id > 1000", "{}.i_value > 0"),
        ),
        inner=Side(
            sql="testdata.satellites",
            key="planetId",
            ty=Ty.INTEGER,
            columns=("id", "planetId", "name", "radius"),
            filters=("{}.radius > 100.0",),
            values=("id", "radius"),
        ),
        scalar_outer="i_group",
    ),
    Pair(
        name="astronauts/planets",  # 357 outer rows, 134 match
        weight=10,
        outer=Side(
            sql="testdata.astronauts",
            key="space_walks",
            ty=Ty.INTEGER,
            columns=("name", "space_walks", "year", "status"),
            filters=("{}.year > 1980", "{}.status = 'Retired'"),
        ),
        inner=Side(
            sql="testdata.planets",
            key="id",
            ty=Ty.INTEGER,
            columns=("id", "name", "number_of_moons"),
            filters=("{}.id > 3",),
            values=("id", "number_of_moons"),
        ),
        scalar_outer="space_walks",
    ),
    Pair(
        name="mixed/wide-varchar",  # VARCHAR key, 2,000 outer rows, 736 match
        weight=15,
        outer=Side(
            sql="testdata.fuzzing.mixed",
            key="s_low",
            ty=Ty.VARCHAR,
            columns=("row_id", "s_low", "i_group"),
            filters=("{}.row_id > 1000", "{}.i_group > 7"),
        ),
        inner=Side(
            sql="(SELECT cat, val, row_id FROM testdata.fuzzing.wide WHERE cat < 'e')",
            key="cat",
            ty=Ty.VARCHAR,
            columns=("cat", "val", "row_id"),
            filters=("{}.val > 0.0",),
            values=("val", "row_id"),
        ),
    ),
    Pair(
        name="mixed/wide-varchar-null-inner-key",  # VARCHAR key with NULLs
        weight=10,
        outer=Side(
            sql="testdata.fuzzing.mixed",
            key="s_low",
            ty=Ty.VARCHAR,
            columns=("row_id", "s_low", "i_group"),
            filters=("{}.row_id > 1000",),
        ),
        inner=Side(
            sql=(
                "(SELECT NULLIF(cat, 'alpha') AS sq_key, val FROM testdata.fuzzing.wide "
                "WHERE cat < 'e')"
            ),
            key="sq_key",
            ty=Ty.VARCHAR,
            columns=("sq_key", "val"),
            filters=("{}.val > 0.0",),
            values=("val",),
        ),
    ),
    Pair(
        name="astronauts/missions-varchar",  # VARCHAR key, 357 outer rows, 270 match
        weight=10,
        outer=Side(
            sql="testdata.astronauts",
            key="status",
            ty=Ty.VARCHAR,
            columns=("name", "status", "year"),
            filters=("{}.year > 1980",),
        ),
        inner=Side(
            sql="(SELECT Rocket_Status, Price FROM testdata.missions)",
            key="Rocket_Status",
            ty=Ty.VARCHAR,
            columns=("Rocket_Status", "Price"),
            filters=("{}.Price > 100.0",),
            values=("Price",),
        ),
    ),
    Pair(
        name="missions/missions-varchar",  # self-referencing, 4,630 rows, 1,030 match
        weight=10,
        outer=Side(
            sql="testdata.missions",
            key="Company",
            ty=Ty.VARCHAR,
            columns=("Company", "Mission_Status", "Rocket_Status"),
            filters=("{}.Mission_Status = 'Success'",),
        ),
        inner=Side(
            sql="(SELECT Company, Price FROM testdata.missions WHERE Price > 100.0)",
            key="Company",
            ty=Ty.VARCHAR,
            columns=("Company", "Price"),
            filters=("{}.Price > 200.0",),
            values=("Price",),
        ),
    ),
    Pair(
        name="wide/mixed",  # 200,000 outer rows across 4 morsels, 60 match
        weight=3,  # deliberately rare: it is the only multi-morsel outer
        outer=Side(
            sql="testdata.fuzzing.wide",
            key="grp_wide",
            ty=Ty.INTEGER,
            columns=("row_id", "grp_wide", "cat", "val"),
            filters=("{}.row_id > 100000", "{}.val > 0.0"),
        ),
        inner=Side(
            sql="testdata.fuzzing.mixed",
            key="i_group",
            ty=Ty.INTEGER,
            columns=("row_id", "i_group", "i_value", "f_value"),
            filters=("{}.row_id > 1000",),
            values=("row_id", "i_value", "f_value"),
        ),
        scalar_outer="grp_wide",
    ),
)


_LOADED: Optional[Tuple[Pair, ...]] = None


def load_pairs() -> Tuple[Pair, ...]:
    """Measure every declared pair against the live engine, and refuse a vacuous one.

    Four facts per pair, all of which an oracle depends on:

    * `outer_rows` / `semi_rows` — the DISCRIMINATING property. A pair whose semi
      join returns nothing makes every oracle here compare `[] == []`, and one
      whose semi join returns every outer row makes the SEMI/ANTI rewrite
      trivially satisfiable. Either way the oracle cannot fail, which is worse
      than not having it. Both are refused at load rather than left to be
      noticed by nobody.
    * `outer_key_nulls` / `inner_key_nulls` — which branch of `NOT IN`'s
      three-way semantics this pair reaches. The generator claims to cover the
      null-collapse case; this is what makes that claim checkable, and
      `assert_null_coverage` below turns it into a hard requirement on the
      corpus as a whole.

    Measured once per process. The corpus is static test data, so a per-case
    re-measure would only add latency; if the data changes, the next run
    re-measures.
    """
    global _LOADED
    if _LOADED is not None:
        return _LOADED

    from tests.fuzzing.harness import scalar

    for pair in PAIRS:
        if pair.outer.ty is not pair.inner.ty:
            raise AssertionError(
                f"pair {pair.name!r} correlates {pair.outer.ty.name} to {pair.inner.ty.name}; "
                f"a cross-type correlation is a different test and belongs in its own pair"
            )
        outer = f"{pair.outer.sql} AS {OUTER}"
        inner = f"{pair.inner.sql} AS {INNER}"
        pair.outer_rows = scalar(f"SELECT COUNT(*) AS n FROM {outer}")
        pair.semi_rows = scalar(
            f"SELECT COUNT(*) AS n FROM {outer} SEMI JOIN {inner} "
            f"ON {INNER}.{pair.inner.key} = {OUTER}.{pair.outer.key}"
        )
        pair.outer_key_nulls = scalar(
            f"SELECT COUNT(*) AS n FROM {outer} WHERE {OUTER}.{pair.outer.key} IS NULL"
        )
        pair.inner_key_nulls = scalar(
            f"SELECT COUNT(*) AS n FROM {inner} WHERE {INNER}.{pair.inner.key} IS NULL"
        )
        if not 0 < pair.semi_rows < pair.outer_rows:
            raise AssertionError(
                f"correlation pair {pair.name!r} is not discriminating: its semi join returns "
                f"{pair.semi_rows} of {pair.outer_rows} outer rows. Every oracle in this file "
                f"would compare two identical trivial results and could never fail. Fix the "
                f"pair (add or relax an inner filter) or delete it."
            )

    _LOADED = PAIRS
    return _LOADED


def assert_null_coverage() -> None:
    """The corpus must reach every branch of `NOT IN`'s three-way semantics.

    `not_in_null_semantics` asserts a different thing depending on whether the
    inner key contains a NULL and whether the outer key does. If the corpus
    happened to contain neither, that oracle would only ever exercise its
    anti-join branch and the two interesting branches — the ones where a wrong
    implementation actually differs — would never run, silently.
    """
    pairs = load_pairs()
    if not any(pair.inner_key_nulls for pair in pairs):
        raise AssertionError(
            "no correlation pair has a NULL inner key, so NOT IN's null-collapse branch "
            "(one NULL makes the whole predicate UNKNOWN) is never exercised"
        )
    if not any(pair.outer_key_nulls for pair in pairs):
        raise AssertionError(
            "no correlation pair has a NULL outer key, so the case that distinguishes "
            "NOT IN from an anti join is never exercised"
        )


# ─────────────────────────────────────────────────────────────────────────────
# What the engine accepts
# ─────────────────────────────────────────────────────────────────────────────

# (label, sql, expected). `expected` is None when the form must RUN, or a
# substring of the error it must raise.
#
# This is the measured support surface, kept runnable rather than as a comment.
# `test_engine_support_matrix_is_current` executes every row: a REFUSED form
# that starts working turns it red and has to be moved into the generator, and
# a SUPPORTED form that stops working turns it red as a plain regression. The
# alternative — a comment saying what was true one afternoon — silently decays
# into a generator that avoids constructs the engine has since gained.
_M_OUTER = "testdata.planets AS sq_o"
_M_INNER = "testdata.satellites AS sq_i"
_M_CORR = "sq_i.planetId = sq_o.id"

SUPPORT_MATRIX: Tuple[Tuple[str, str, Optional[str]], ...] = (
    (
        "correlated EXISTS",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE EXISTS (SELECT 1 FROM {_M_INNER} WHERE {_M_CORR})",
        None,
    ),
    (
        "correlated NOT EXISTS",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE NOT EXISTS "
        f"(SELECT 1 FROM {_M_INNER} WHERE {_M_CORR})",
        None,
    ),
    (
        "IN (subquery)",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id IN "
        f"(SELECT sq_i.planetId FROM {_M_INNER})",
        None,
    ),
    (
        "NOT IN (subquery)",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id NOT IN "
        f"(SELECT sq_i.planetId FROM {_M_INNER})",
        None,
    ),
    (
        "uncorrelated scalar subquery in WHERE",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id > "
        f"(SELECT MIN(sq_i.planetId) FROM {_M_INNER})",
        None,
    ),
    (
        "correlated scalar subquery in WHERE",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id > "
        f"(SELECT MIN(sq_i.id) FROM {_M_INNER} WHERE {_M_CORR})",
        None,
    ),
    (
        "uncorrelated EXISTS",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE EXISTS (SELECT 1 FROM {_M_INNER})",
        "requires a correlated equality predicate",
    ),
    (
        "scalar subquery in the SELECT list",
        f"SELECT sq_o.name, (SELECT MIN(sq_i.planetId) FROM {_M_INNER}) AS m FROM {_M_OUTER}",
        "not yet in the **SELECT** list",
    ),
    (
        "correlated scalar subquery on a non-equality",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id > "
        f"(SELECT MIN(sq_i.id) FROM {_M_INNER} WHERE sq_i.planetId > sq_o.id)",
        "can only be decorrelated on equality correlations",
    ),
    (
        "= ANY (subquery)",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id = ANY "
        f"(SELECT sq_i.planetId FROM {_M_INNER})",
        "ANY**/**ALL** over a subquery is not supported",
    ),
    (
        "> ALL (subquery)",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id > ALL "
        f"(SELECT sq_i.planetId FROM {_M_INNER})",
        "ANY**/**ALL** over a subquery is not supported",
    ),
    (
        "IN (subquery) under an expression",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id + 0 IN "
        f"(SELECT sq_i.planetId FROM {_M_INNER})",
        "belongs to a scope further out",
    ),
    (
        "scalar subquery in HAVING",
        "SELECT sq_o.id FROM testdata.planets AS sq_o GROUP BY sq_o.id "
        f"HAVING COUNT(*) > (SELECT MIN(sq_i.planetId) FROM {_M_INNER})",
        "which the stream does not carry",
    ),
)


# An EXISTS/IN subquery that is not a top-level conjunct of the WHERE clause.
# Decorrelation turns the existence test into a JOIN, and a join expresses
# neither a disjunct nor a negation of a row-level test, so every one of these
# is refused — by the guard at the top of `_build_filter_join`.
#
# THESE ARE NOT IN `SUPPORT_MATRIX`, and the reason is not tidiness. Before that
# guard existed, the four IN spellings made the PLANNER LOOP FOREVER: the driving
# loop in `_rewrite_filters` re-found a node `_split_out` had failed to remove.
# A row in the in-process matrix would, if that regressed, hang the whole test
# suite with no output. `test_subquery_position_is_refused_promptly` runs each of
# these in a SUBPROCESS with a deadline instead, so a return of the hang fails
# the run in bounded time rather than wedging it.
#
# The two EXISTS spellings are here for the same reason and because they share
# the root cause: they used to be refused for `**EXISTS** requires a correlated
# equality predicate` — a misdiagnosis, since the correlation was right there.
# The first pass lifted the correlation out of the subquery, failed to remove the
# EXISTS node, and the second pass then found no correlation left to lift.
POSITIONS_REFUSED_PROMPTLY: Tuple[Tuple[str, str], ...] = (
    (
        "NOT (x IN (subquery))",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE NOT (sq_o.id IN "
        f"(SELECT sq_i.planetId FROM {_M_INNER}))",
    ),
    (
        "(x IN (subquery)) IS NULL",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE (sq_o.id IN "
        f"(SELECT sq_i.planetId FROM {_M_INNER})) IS NULL",
    ),
    (
        "(x IN (subquery)) = TRUE",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE (sq_o.id IN "
        f"(SELECT sq_i.planetId FROM {_M_INNER})) = TRUE",
    ),
    (
        "x NOT IN (subquery) as a disjunct",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id NOT IN "
        f"(SELECT sq_i.planetId FROM {_M_INNER}) OR sq_o.id > 100",
    ),
    (
        "EXISTS as a disjunct",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE sq_o.id > 2 OR EXISTS "
        f"(SELECT 1 FROM {_M_INNER} WHERE {_M_CORR})",
    ),
    (
        "parenthesised NOT (EXISTS ...)",
        f"SELECT sq_o.name FROM {_M_OUTER} WHERE NOT (EXISTS "
        f"(SELECT 1 FROM {_M_INNER} WHERE {_M_CORR}))",
    ),
)

#: What every entry above must be refused WITH. One shared substring, because
#: one guard raises them all — if they stop agreeing, the guard has been
#: bypassed for some of them and the hang can come back for those.
POSITION_REFUSAL_SIGNATURE = "only supported as a top-level condition"


# ─────────────────────────────────────────────────────────────────────────────
# One generated case
# ─────────────────────────────────────────────────────────────────────────────

#: Aggregates the scalar-subquery forms draw from. COUNT is here on purpose even
#: though it is the one that exposes
#: subquery_known_gaps/correlated-scalar-subquery-drops-unmatched-outer-rows:
#: the point of a fuzzer is to keep emitting the broken shape, and the ORACLE
#: declines (naming the entry), not the generator.
_AGGREGATES: Tuple[str, ...] = ("MIN", "MAX", "SUM", "AVG", "COUNT")

_COMPARISONS: Tuple[str, ...] = (">", ">=", "<", "<=", "=", "!=")

FORMS: Tuple[Tuple[str, int], ...] = (
    ("exists", 20),
    ("not_exists", 15),
    ("in", 20),
    ("not_in", 20),
    ("corr_scalar", 15),
    ("uncorr_scalar", 10),
)


@dataclass(frozen=True)
class SubqueryCase:
    """One generated statement, plus everything needed to spell it another way.

    The case holds the PIECES rather than only the finished SQL, because every
    oracle here works by re-spelling the same semantics: as a join, as the
    opposite predicate, as the outer query with the predicate removed. Building
    those from a string would mean parsing it back.
    """

    form: str
    pair: Pair
    projection: str  # the SELECT list, identical in every spelling
    distinct: bool
    order_by: Optional[str]
    outer_extra: Optional[str]  # an extra conjunct over the outer relation
    inner_source: str  # the inner relation
    #: A local predicate carried INSIDE the subquery's WHERE, alongside the
    #: correlation, as a `{}`-templated alias. This is the shape that makes
    #: `_lift_correlations` do real work: it has to split the correlated
    #: equality (which becomes a join key) from the local conjunct (which must
    #: stay inside the relation, where predicate pushdown can still reach the
    #: scan). Getting that split wrong is what the TPC-H Q21 comment in
    #: decorrelate_subquery.py records having got wrong once already.
    inner_conjunct: Optional[str]
    #: An EXISTS nested inside the subquery, correlated to the MIDDLE scope.
    #: `EXISTS (SELECT 1 FROM i AS sq_n WHERE sq_n.k = sq_i.k)` is satisfied by
    #: `sq_i` itself whenever `sq_i.k` is non-NULL, and the enclosing
    #: correlation already guarantees that — so it is an exact no-op, and the
    #: join rewrite is unchanged. That is what makes it an oracle for the
    #: skip-level correlation machinery rather than just another shape.
    nested_existence: bool
    aggregate: Optional[str]  # scalar forms only
    aggregate_arg: Optional[str]  # scalar forms only
    comparison: Optional[str]  # scalar forms only
    tags: FrozenSet[str]

    # ── the pieces every spelling shares ──────────────────────────────────────

    @property
    def outer_key(self) -> str:
        return f"{OUTER}.{self.pair.outer.key}"

    @property
    def inner_key(self) -> str:
        return f"{INNER}.{self.pair.inner.key}"

    def _head(self) -> str:
        return f"SELECT {'DISTINCT ' if self.distinct else ''}{self.projection}"

    def _tail(self) -> str:
        return f" ORDER BY {self.order_by}" if self.order_by else ""

    def _outer_from(self) -> str:
        return f"{self.pair.outer.sql} AS {OUTER}"

    def _and_extra(self, predicate: str) -> str:
        if self.outer_extra is None:
            return predicate
        return f"{predicate} AND {self.outer_extra}"

    def _where_extra(self) -> str:
        return f" WHERE {self.outer_extra}" if self.outer_extra else ""

    def _filtered_inner(self) -> str:
        """The inner relation with `inner_conjunct` folded into it.

        What the JOIN spellings use. The subquery spellings put the same
        predicate in the subquery's own WHERE instead, so the two spellings look
        at an identical inner set by two different routes — which is the point:
        one route makes `_lift_correlations` split a mixed conjunction, the
        other never goes near it.

        The fold uses its own alias (`sq_g`), because `sq_i` is already taken by
        whatever the caller wraps this in.
        """
        if self.inner_conjunct is None:
            return self.inner_source
        return (
            f"(SELECT * FROM {self.inner_source} AS sq_g "
            f"WHERE {self.inner_conjunct.format('sq_g')})"
        )

    def _inner_conjuncts(self) -> str:
        """The subquery-side conjuncts that go with the correlation, if any."""
        parts = []
        if self.inner_conjunct is not None:
            parts.append(self.inner_conjunct.format(INNER))
        if self.nested_existence:
            parts.append(
                f"EXISTS (SELECT 1 FROM {self.inner_source} AS sq_n "
                f"WHERE sq_n.{self.pair.inner.key} = {self.inner_key})"
            )
        return "".join(f" AND {part}" for part in parts)

    # ── the subquery spelling ─────────────────────────────────────────────────

    def exists_predicate(self, negated: bool) -> str:
        keyword = "NOT EXISTS" if negated else "EXISTS"
        return (
            f"{keyword} (SELECT 1 FROM {self.inner_source} AS {INNER} "
            f"WHERE {self.inner_key} = {self.outer_key}{self._inner_conjuncts()})"
        )

    def in_predicate(self, negated: bool) -> str:
        keyword = "NOT IN" if negated else "IN"
        # No `nested_existence` here: inside an IN subquery there is no enclosing
        # correlation guaranteeing the key is non-NULL, so the nested test would
        # REMOVE the NULL keys from the set — which is not a no-op, it is the
        # difference between NOT IN's null-collapse branch and its anti-join one.
        where = ""
        if self.inner_conjunct is not None:
            where = f" WHERE {self.inner_conjunct.format(INNER)}"
        return (
            f"{self.outer_key} {keyword} "
            f"(SELECT {self.inner_key} FROM {self.inner_source} AS {INNER}{where})"
        )

    def scalar_predicate(self, correlated: bool) -> str:
        if correlated:
            where = f" WHERE {self.inner_key} = {self.outer_key}{self._inner_conjuncts()}"
        elif self.inner_conjunct is not None:
            where = f" WHERE {self.inner_conjunct.format(INNER)}"
        else:
            where = ""
        return (
            f"{self.outer_key} {self.comparison} "
            f"(SELECT {self.aggregate}({self.aggregate_arg}) "
            f"FROM {self.inner_source} AS {INNER}{where})"
        )

    @property
    def predicate(self) -> str:
        """The predicate this case's `form` names."""
        if self.form == "exists":
            return self.exists_predicate(negated=False)
        if self.form == "not_exists":
            return self.exists_predicate(negated=True)
        if self.form == "in":
            return self.in_predicate(negated=False)
        if self.form == "not_in":
            return self.in_predicate(negated=True)
        if self.form == "corr_scalar":
            return self.scalar_predicate(correlated=True)
        if self.form == "uncorr_scalar":
            return self.scalar_predicate(correlated=False)
        raise AssertionError(f"unknown form {self.form!r}")

    def spell(self, predicate: str) -> str:
        """The outer query with `predicate` as its WHERE clause."""
        return (
            f"{self._head()} FROM {self._outer_from()} "
            f"WHERE {self._and_extra(predicate)}{self._tail()}"
        )

    @property
    def sql(self) -> str:
        return self.spell(self.predicate)

    # ── the join spellings ────────────────────────────────────────────────────

    def semi_or_anti(self, join: str) -> str:
        """The SEMI/ANTI join rewrite. `join` is "SEMI" or "ANTI".

        SEMI and ANTI emit outer rows only and emit each at most once, which is
        what makes this an exact multiset equivalent of EXISTS / NOT EXISTS
        rather than merely a set one.
        """
        return (
            f"{self._head()} FROM {self._outer_from()} "
            f"{join} JOIN {self._filtered_inner()} AS {INNER} "
            f"ON {self.inner_key} = {self.outer_key}"
            f"{self._where_extra()}{self._tail()}"
        )

    def anti_join_excluding_null_outer_key(self) -> str:
        """ANTI JOIN, minus the outer rows whose key is NULL.

        This — not a bare anti join — is what `NOT IN` equals when the inner key
        has no NULL. `NULL NOT IN (<non-empty>)` is UNKNOWN, so WHERE drops that
        row; an anti join keeps it, because no inner row equals it.
        """
        predicate = f"{self.outer_key} IS NOT NULL"
        return (
            f"{self._head()} FROM {self._outer_from()} "
            f"ANTI JOIN {self._filtered_inner()} AS {INNER} "
            f"ON {self.inner_key} = {self.outer_key} "
            f"WHERE {self._and_extra(predicate)}{self._tail()}"
        )

    def grouped_aggregate_join(self) -> str:
        """LEFT JOIN against the inner side pre-aggregated per key.

        The textbook decorrelation of a correlated scalar aggregate. LEFT, not
        INNER: an outer row with no matching inner group must SURVIVE carrying
        the aggregate's empty-set value, which is NULL for MIN/MAX/SUM/AVG and
        0 for COUNT — hence the COALESCE, which is not decoration but the whole
        difference between the two.
        """
        value = f"{JOINED}.sq_agg"
        if self.aggregate == "COUNT":
            value = f"COALESCE({JOINED}.sq_agg, 0)"
        grouped = (
            f"(SELECT {self.inner_key} AS sq_key, "
            f"{self.aggregate}({self.aggregate_arg}) AS sq_agg "
            f"FROM {self._filtered_inner()} AS {INNER} "
            f"GROUP BY {self.inner_key})"
        )
        predicate = f"{self.outer_key} {self.comparison} {value}"
        return (
            f"{self._head()} FROM {self._outer_from()} "
            f"LEFT JOIN {grouped} AS {JOINED} ON {JOINED}.sq_key = {self.outer_key} "
            f"WHERE {self._and_extra(predicate)}{self._tail()}"
        )

    def cross_joined_aggregate(self) -> str:
        """CROSS JOIN against the inner side aggregated to a single row.

        The exact rewrite of an UNCORRELATED scalar subquery. Comparing against
        an inlined literal instead would require rendering a float back into SQL
        without changing its value, and a rewrite that only holds to 17 digits
        is not an oracle.
        """
        aggregated = (
            f"(SELECT {self.aggregate}({self.aggregate_arg}) AS sq_agg "
            f"FROM {self._filtered_inner()} AS {INNER})"
        )
        predicate = f"{self.outer_key} {self.comparison} {JOINED}.sq_agg"
        return (
            f"{self._head()} FROM {self._outer_from()} "
            f"CROSS JOIN {aggregated} AS {JOINED} "
            f"WHERE {self._and_extra(predicate)}{self._tail()}"
        )

    # ── degenerate spellings the NOT IN oracle needs ──────────────────────────

    def without_the_subquery(self) -> str:
        """The outer query with the subquery predicate removed entirely.

        `x NOT IN (<empty set>)` is TRUE for every row — including one whose key
        is NULL — so this is what NOT IN must equal when the inner set is empty.
        """
        return f"{self._head()} FROM {self._outer_from()}{self._where_extra()}{self._tail()}"

    def inner_relation(self) -> str:
        """The inner set on its own, for probing emptiness and key nullability."""
        return f"SELECT {self.inner_key} AS sq_key FROM {self._filtered_inner()} AS {INNER}"


# ─────────────────────────────────────────────────────────────────────────────
# Generation
# ─────────────────────────────────────────────────────────────────────────────


def _weighted(rng: random.Random, options: Sequence[Tuple[object, int]]):
    return rng.choices(
        [option for option, _ in options], weights=[weight for _, weight in options], k=1
    )[0]


def _inner_filter(rng: random.Random, pair: Pair, tags: set) -> Tuple[str, Optional[str]]:
    """Where this case's inner filter goes: (inner_source, inner_conjunct).

    A local predicate over the inner side can sit in either of two places, and
    the two are NOT the same test:

    * folded into a derived relation — the subquery's WHERE then holds nothing
      but the correlation, so `_lift_correlations` lifts the whole condition;
    * carried as a conjunct of the subquery's own WHERE — now the condition is
      MIXED, and `_lift_correlations` has to split the correlated equality (it
      becomes a join key) from the local one (it must stay inside the relation,
      or predicate pushdown can no longer reach the scan). That split is what
      the TPC-H Q21 comment in decorrelate_subquery.py records having got wrong.

    Both are generated, and both are required to equal the same join rewrite —
    which folds the filter into the relation either way.

    A filter is deliberately never put on the join's ON clause. A SEMI/ANTI ON
    is an equality-only surface here (a theta condition on a semi join is a
    different feature), and the whole value of the rewrite is that it looks at
    literally the same inner set.
    """
    if pair.inner.empty_filters and rng.random() < 0.08:
        template = rng.choice(pair.inner.empty_filters)
        tags.add("inner-filter:empty")
    elif not pair.inner.filters or rng.random() < 0.4:
        return pair.inner.sql, None
    else:
        template = rng.choice(pair.inner.filters)
    if rng.random() < 0.5:
        tags.add("inner-filter:folded")
        return f"(SELECT * FROM {pair.inner.sql} AS sq_f WHERE {template.format('sq_f')})", None
    tags.add("inner-filter:conjunct")
    return pair.inner.sql, template


def generate(rng: random.Random) -> SubqueryCase:
    """One predicate-subquery statement."""
    pairs = load_pairs()
    pair = _weighted(rng, [(pair, pair.weight) for pair in pairs])

    forms = list(FORMS)
    if pair.scalar_outer is None or not pair.inner.values:
        # A scalar form needs a numeric inner column to aggregate and an outer
        # column to compare it against. A VARCHAR-keyed pair that declares
        # neither generates the existence forms only.
        forms = [(form, weight) for form, weight in forms if "scalar" not in form]
    form = _weighted(rng, forms)

    tags = {f"form:{form}", f"pair:{pair.name}"}
    inner_source, inner_conjunct = _inner_filter(rng, pair, tags)

    # Only where it is provably a no-op — see SubqueryCase.nested_existence and
    # the note in in_predicate() for why IN and NOT IN are excluded.
    nested_existence = form in ("exists", "not_exists", "corr_scalar") and rng.random() < 0.2
    if nested_existence:
        tags.add("nested-existence")

    columns = list(pair.outer.columns)
    rng.shuffle(columns)
    projected = columns[: rng.randint(1, 3)]
    projection = ", ".join(f"{OUTER}.{column}" for column in projected)

    distinct = rng.random() < 0.15
    if distinct:
        tags.add("distinct")

    order_by = None
    if rng.random() < 0.2:
        # Drawn from the PROJECTED columns, not from every outer column: an
        # ORDER BY over something a DISTINCT has already collapsed away is not a
        # generated case, it is a generator fault that reads as a binder bug.
        order_by = f"{OUTER}.{rng.choice(projected)}"
        tags.add("order-by")

    outer_extra = None
    if pair.outer.filters and rng.random() < 0.35:
        outer_extra = rng.choice(pair.outer.filters).format(OUTER)
        tags.add("outer-conjunct")

    aggregate = aggregate_arg = comparison = None
    if "scalar" in form:
        aggregate = rng.choice(_AGGREGATES)
        aggregate_arg = "*" if aggregate == "COUNT" else f"{INNER}.{rng.choice(pair.inner.values)}"
        comparison = rng.choice(_COMPARISONS)
        tags.add(f"aggregate:{aggregate}")

    return SubqueryCase(
        form=form,
        pair=pair,
        projection=projection,
        distinct=distinct,
        order_by=order_by,
        outer_extra=outer_extra,
        inner_source=inner_source,
        inner_conjunct=inner_conjunct,
        nested_existence=nested_existence,
        aggregate=aggregate,
        aggregate_arg=aggregate_arg,
        comparison=comparison,
        tags=frozenset(tags),
    )
