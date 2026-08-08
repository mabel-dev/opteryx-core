"""
Generate random SQL JOINs.

The value is in the oracles. A generator that only asserts "did not raise" finds
crashes and binder regressions but is structurally blind to silent wrong answers,
which is where join bugs live.

WHAT IS GENERATED
-----------------
* Eleven join types — INNER/LEFT/RIGHT/FULL, the four LEFT mark joins, and ASOF.
  (RIGHT SEMI/ANTI are refused by the planner; see `_JOIN_TYPES`.)
* Two- and three-relation statements, drawn WITH replacement, so self-joins and
  repeated relations happen. Everything is aliased.
* Equi ON conditions of one or more conjuncts, plus NON-EQUALITY (theta) conjuncts
  on INNER — the only join type that supports one.
* ASOF `MATCH_CONDITION(a.x <op> b.y)` over all four operators, with an optional
  `USING` equi-partition.
* Join keys across every admissible type family INCLUDING BOOLEAN and VARBINARY,
  and deliberately cross-type (`INTEGER = FLOAT`, `DATE = TIMESTAMP`).

THE ORACLES
-----------
Five join-algebra identities, two metamorphic equivalences, a leg-reordering
equivalence for three-relation statements, three ASOF-specific checks, and — the
only ones with INDEPENDENT ground truth — `INNER == CROSS JOIN + WHERE` and
`SEMI/ANTI == EXISTS/NOT EXISTS`.

That last pair earns its place. The algebra identities are necessary but NOT
sufficient: when the engine silently dropped a theta ON conjunct it dropped it from
every join type, so all five identities held over its own wrong answers. Only a
comparison against a different plan caught it.

WHAT AN EXCEPTION MEANS HERE

WHAT AN EXCEPTION MEANS HERE
----------------------------
Everything this generator emits is intended to run, so a raise is a finding. The
only exception is a defect already recorded in `join_known_gaps.py` with a live
repro that `test_registered_defect_still_reproduces` re-checks every run — a
mechanism for defects owned elsewhere and for ratified refusals, not a way to quiet
an inconvenient failure.

A WRONG ANSWER IS NEVER ABSORBED. The oracles raise `AssertionError`, and the
register declines to match that type at all, so there is no path — none — by which
a broken identity becomes a green run. The two known wrong answers here (NaN join
keys, ASOF tie order) are handled the way the single-table fuzzer handles its own:
a `test_wrong_answer_*` test asserting the broken behaviour directly, plus a
STRUCTURAL exclusion scoped to the query shape and naming its register entry, both
of which disappear when the defect is fixed.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import datetime
import random
import time
from collections import Counter
from dataclasses import dataclass
from typing import List, Optional, Tuple

import pytest

import opteryx
from opteryx.types import LogicalCategory
from opteryx.types.logical_type import _CATEGORY_OF
from opteryx.utils import random_string
from opteryx.utils.formatter import format_sql
from tests.fuzzing import harness
from tests.fuzzing import join_known_gaps as known_gaps
from tests.helpers import execute_and_get_shape

# Kept low enough that the regression suite stays fast; the nightly schedule
# raises it via TEST_ITERATIONS.
DEFAULT_ITERATIONS = 100


def random_value(t):
    if t == LogicalCategory.VARCHAR:
        return f"'{random_string(4)}'"
    if t == LogicalCategory.VARBINARY:
        return f"b'{random_string(8)}'"
    if t in (LogicalCategory.DATE, LogicalCategory.TIMESTAMP):
        # Use a fixed reference date to ensure reproducibility.
        # The ::TIMESTAMP cast is required, not decorative: Opteryx does not
        # implicitly coerce a string literal to a temporal column type, and
        # rejects `date_col = '1930-01-01'` with IncompatibleTypesError. Without
        # the cast every temporal predicate this generator emits dies in the
        # binder, so no temporal join or filter is ever actually executed.
        reference_date = datetime.datetime(2024, 1, 1, 0, 0, 0)
        if random.random() < 0.5:
            return f"'{reference_date + datetime.timedelta(seconds=random.randint(-1000000, 1000000))}'::TIMESTAMP"
        return f"'{(reference_date + datetime.timedelta(seconds=random.randint(-1000000, 1000000))).date()}'::TIMESTAMP"
    if random.random() < 0.5:
        return random.randint(-1000000, 1000000)
    return random.randint(-1000000, 1000000) / 1000



# ---------------------------------------------------------------------------------
# Source relations
# ---------------------------------------------------------------------------------
# The $satellites, $astronauts and $missions virtual datasets no longer exist — only
# $planets survives in opteryx.managers.virtual_datasets. The same four relations are
# still present as parquet under testdata/, so the join fuzzer reads them from there.
#
# `testdata.fuzzing.mixed` is here for its TYPES, not its size: it is the only
# relation in reach carrying BOOLEAN, VARBINARY, DECIMAL, DATE and TIMESTAMP columns,
# and without it BOOLEAN and VARBINARY join keys cannot be generated at all.
#
# `testdata.fuzzing.wide` is deliberately NOT here. At 200,000 rows a low-cardinality
# key against it is a cartesian bomb — `mixed ⋈ wide ON b_value = flag` is
# 200,000,000 rows — and this fuzzer is testing join semantics, not the scheduler's
# tolerance for that.
FUZZ_TABLES = (
    "testdata.planets",
    "testdata.satellites",
    "testdata.missions",
    "testdata.astronauts",
    "testdata.fuzzing.mixed",
)


@dataclass(frozen=True)
class FuzzColumn:
    """A column as the generators need it: a name plus a dispatch category.

    Mirrors the `.name` / `.category` surface of `SchemaColumn`, which is all the
    generators below ever touch.
    """

    name: str
    category: LogicalCategory
    #: Does this column contain a NaN? See `Relation.key_fields`.
    has_nan: bool = False


@dataclass(frozen=True)
class Relation:
    """One ALIASED relation reference in a generated statement.

    Everything is aliased, always. That is what makes self-joins and three-relation
    statements expressible at all: `testdata.planets AS r0 JOIN testdata.planets AS
    r1` is the only way to write a self-join, and once one shape needs aliases,
    having a second unaliased shape would just be two code paths for one idea.
    """

    table: str
    alias: str
    fields: Tuple[FuzzColumn, ...]
    rows: int

    @property
    def ref(self) -> str:
        return f"{self.table} AS {self.alias}"

    def qualified(self, column: FuzzColumn) -> str:
        return f"{self.alias}.{column.name}"

    @property
    def names(self) -> set:
        return {column.name for column in self.fields}

    @property
    def key_fields(self) -> Tuple[FuzzColumn, ...]:
        """The columns admissible as a JOIN KEY — everything except NaN-bearing ones.

        STRUCTURAL EXCLUSION for join_known_gaps/
        nan-join-keys-match-themselves-in-every-join-but-inner: a NaN key matches
        itself in every join type except INNER, so `testdata.satellites.albedo =
        testdata.satellites.magnitude` makes INNER say 0 and SEMI say 6. That breaks
        the algebra identities on every case that draws one of the three NaN-bearing
        columns, which would leave the fuzzer permanently red on a defect it has
        already reported.

        Scoped to the DATA, not to an error message: the exclusion is exactly the
        columns that actually contain a NaN, discovered by reading them (see
        get_tables), so it shrinks by itself as the corpus changes and disappears
        entirely when the register entry does. Those columns remain fully available
        for projection and for WHERE predicates — only the join KEY is withheld.
        """
        return tuple(column for column in self.fields if not column.has_nan)


_tables_cache = None


def get_tables():
    """Lazy initialization of tables to avoid expensive setup during test collection.

    Ask the engine what each relation actually contains rather than carrying a
    hardcoded copy of the schemas — a hardcoded table drifts silently the moment the
    test data changes, and a fuzzer built on a stale schema generates queries that
    only ever exercise the binder's error path. The row count comes from the engine
    for the same reason: the cardinality guards below are only as honest as it.
    """
    global _tables_cache
    if _tables_cache is not None:
        return _tables_cache

    _tables_cache = []
    for table in FUZZ_TABLES:
        session = opteryx.session()
        morsels = list(session.execute_to_morsels(f"SELECT * FROM {table}"))
        if not morsels:
            raise ValueError(f"fuzzing source table {table!r} returned no data")
        # NaN detection reads the VALUES rather than asking the engine
        # `WHERE col != col`: whether a NaN comparison answers TRUE, FALSE or
        # UNKNOWN is itself an open question (the single-table register's
        # nan-rows-fall-outside-every-predicate-bucket), so a detector built on it
        # would be resting on the thing it is meant to route around.
        nan_columns = set()
        for name, physical in morsels[0].schema.items():
            if _CATEGORY_OF[physical] is not LogicalCategory.FLOAT:
                continue
            for row in harness.rows(f"SELECT {name} FROM {table}"):
                value = row[0]
                if isinstance(value, float) and value != value:
                    nan_columns.add(name)
                    break
        fields = tuple(
            FuzzColumn(name=name, category=_CATEGORY_OF[physical],
                       has_nan=name in nan_columns)
            for name, physical in morsels[0].schema.items()
        )
        _tables_cache.append((table, fields, harness.scalar(f"SELECT COUNT(*) FROM {table}")))
    return _tables_cache


# ---------------------------------------------------------------------------------
# What may be joined to what
# ---------------------------------------------------------------------------------

# Join types that carry rows from the right relation into the output. The reducing
# joins (SEMI/ANTI) project only the left relation, so a select list naming
# right-hand columns is invalid for them.
_REDUCING_JOINS = ("LEFT ANTI JOIN", "LEFT SEMI JOIN", "ANTI JOIN", "SEMI JOIN")

# RIGHT SEMI / RIGHT ANTI are absent ON PURPOSE. The planner refuses them outright —
# `UnsupportedSyntaxError: RIGHT SEMI JOIN not supported, use LEFT variations only.` —
# and per architect ruling that refusal stands rather than being implemented as the
# operand swap it would be. It is pinned by a live repro in join_known_gaps.py, so
# the refusal is asserted rather than assumed; do not re-add them here without
# deleting that register entry first.
_JOIN_TYPES = (
    "JOIN",
    "INNER JOIN",
    "LEFT JOIN",
    "LEFT OUTER JOIN",
    "RIGHT JOIN",
    "FULL OUTER JOIN",
    "LEFT ANTI JOIN",
    "LEFT SEMI JOIN",
    "ANTI JOIN",
    "SEMI JOIN",
    "ASOF JOIN",
)

_INNER_JOINS = ("JOIN", "INNER JOIN")

# Categories that may be compared against each other as a join key.
#
# The generator used to require left.category == right.category, which excluded
# cross-type join keys — a real bug class — by construction. It is widened to these
# families rather than to anything at all, because `VARCHAR = INTEGER` is SQL the
# engine is *right* to reject: generating it would test the binder's error path over
# and over instead of the join.
_JOINABLE_FAMILIES = (
    (LogicalCategory.INTEGER, LogicalCategory.FLOAT, LogicalCategory.DECIMAL),
    (LogicalCategory.DATE, LogicalCategory.TIMESTAMP),
    (LogicalCategory.VARCHAR, LogicalCategory.NVARCHAR),
)

# ARRAY is never a join key — equality on a collection is ambiguous, and the engine
# is right not to define it. ARRAY columns still reach the SELECT list, which is
# where the FULL OUTER ARRAY-payload defect lived.
_UNJOINABLE = (LogicalCategory.ARRAY,)

# A VARCHAR = VARBINARY join key is deliberately NOT generated. The two hash at
# different physical widths and the implicit key coercion covers NUMERIC pairs only
# (see compiler._join_key_coercions: "NON-numeric keys are untouched"), so it is a
# plausible silent-wrong-answer class — but whether it should coerce, refuse, or is
# simply out of scope is an architect's call, not a fuzzer's assumption. Same-type
# VARBINARY keys ARE generated.


def joinable(left: LogicalCategory, right: LogicalCategory) -> bool:
    """Whether these two categories may legitimately be equated as a join key."""
    if left in _UNJOINABLE or right in _UNJOINABLE:
        return False
    if left == right:
        return True
    return any(left in family and right in family for family in _JOINABLE_FAMILIES)


# Categories that can be ORDERED, so can carry an inequality — a theta join conjunct
# or an ASOF MATCH_CONDITION. BOOLEAN is absent because the engine has no ordering
# for it: `a.b_value >= b.b_value` is rejected by the operator map, which carries
# only Eq/NotEq for BOOLEAN × BOOLEAN.
_ORDERABLE = (
    LogicalCategory.INTEGER,
    LogicalCategory.FLOAT,
    LogicalCategory.DECIMAL,
    LogicalCategory.DATE,
    LogicalCategory.TIMESTAMP,
    LogicalCategory.VARCHAR,
    LogicalCategory.NVARCHAR,
    LogicalCategory.VARBINARY,
)


def comparable(left: LogicalCategory, right: LogicalCategory) -> bool:
    """Whether these two categories may be compared with `<`/`>`/`<=`/`>=`."""
    return left in _ORDERABLE and right in _ORDERABLE and joinable(left, right)


def _pick_pair(rng_columns_left, rng_columns_right, predicate):
    """A (left, right) column pair satisfying `predicate`, or None if there is none.

    Enumerates rather than rejection-samples: with BOOLEAN and VARBINARY keys in play
    some relation pairs have very few admissible combinations, and a `while not
    predicate(...)` retry loop over those spins.
    """
    candidates = [
        (left, right)
        for left in rng_columns_left
        for right in rng_columns_right
        if predicate(left.category, right.category)
    ]
    if not candidates:
        return None
    return random.choice(candidates)


# ---------------------------------------------------------------------------------
# The generated statement
# ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class JoinLeg:
    """One `<JOIN TYPE> <relation> ON <...>` step of a generated statement."""

    join_type: str
    relation: Relation
    #: Rendered equality conjuncts. For ASOF these are the USING key names instead.
    equi: Tuple[str, ...]
    #: Rendered NON-equality conjuncts. Only ever populated for an INNER join — see
    #: the architect ruling recorded on `generate_join`.
    theta: Tuple[str, ...]
    #: ASOF only: the match inequality, kept in PARTS. The reachability oracle
    #: below rebuilds it as `left.col <op> (SELECT MIN/MAX(col) FROM right)`, which
    #: needs the operand names, not just the rendered string.
    match_left: Optional[str]
    match_operator: Optional[str]
    match_right: Optional[str]
    #: Bare (unqualified) name of the right-hand match column, for that aggregate.
    match_right_column: Optional[str]
    #: True when this leg's ON references ONLY the head relation, so the leg can be
    #: reordered against its siblings without changing the meaning.
    independent: bool

    @property
    def is_asof(self) -> bool:
        return self.join_type == "ASOF JOIN"

    @property
    def match_condition(self) -> str:
        return f"{self.match_left} {self.match_operator} {self.match_right}"

    @property
    def is_reducing(self) -> bool:
        return self.join_type in _REDUCING_JOINS

    @property
    def is_plain_equi(self) -> bool:
        """A straightforward keyed join — no ASOF, no theta conjunct."""
        return not self.is_asof and not self.theta

    def clause(self, equi=None, theta=None, join_type=None) -> str:
        equi = self.equi if equi is None else tuple(equi)
        theta = self.theta if theta is None else tuple(theta)
        join_type = join_type or self.join_type
        if self.is_asof:
            rendered = f"ASOF JOIN {self.relation.ref} MATCH_CONDITION({self.match_condition})"
            if equi:
                # ASOF's equi-partition. `USING (key)` must follow MATCH_CONDITION —
                # the parser rejects the other order.
                rendered += f" USING ({', '.join(equi)})"
            return rendered
        return f"{join_type} {self.relation.ref} ON {' AND '.join(equi + theta)}"


@dataclass(frozen=True)
class JoinSpec:
    """A generated statement, kept in parts so the oracles can rebuild variants.

    The oracles need the relations and each leg's condition independently — they
    re-express the same join under a different join type, or with the legs reordered,
    and compare. A generator that only returns finished SQL text cannot support that.
    """

    head: Relation
    legs: Tuple[JoinLeg, ...]
    select_clause: str
    where: Optional[str]

    @property
    def relations(self) -> Tuple[Relation, ...]:
        return (self.head,) + tuple(leg.relation for leg in self.legs)

    @property
    def is_two_relation(self) -> bool:
        return len(self.legs) == 1

    @property
    def cross_product(self) -> int:
        size = self.head.rows
        for leg in self.legs:
            size *= leg.relation.rows
        return size

    def sql(self, where: Optional[str] = ..., join_type: Optional[str] = None,
            legs: Optional[Tuple[JoinLeg, ...]] = None) -> str:
        """Render this statement, optionally overriding the predicate or leg order."""
        if where is ...:
            where = self.where
        legs = self.legs if legs is None else legs
        rendered = [leg.clause(join_type=join_type) for leg in legs]
        query = f"{self.select_clause} FROM {self.head.ref} " + " ".join(rendered)
        if where:
            query = f"{query} WHERE {where}"
        return query


def generate_condition(relation: Relation) -> str:
    """One WHERE predicate over `relation`, qualified by its alias."""
    candidates = [c for c in relation.fields if c.category not in _UNJOINABLE]
    where_column = random.choice(candidates)
    column = relation.qualified(where_column)
    if random.random() < 0.1:
        where_operator = random.choice(["IS", "IS NOT"])
        if where_column.category == LogicalCategory.BOOLEAN:
            where_value = random.choice(["TRUE", "FALSE", "NULL"])
        else:
            where_value = "NULL"
    elif where_column.category in (LogicalCategory.VARCHAR, LogicalCategory.VARBINARY) and random.random() < 0.5:
        where_operator = random.choice(
            ["LIKE", "ILIKE", "NOT LIKE", "NOT ILIKE", "RLIKE", "NOT RLIKE"]
        )
        where_value = (
            random_value(where_column.category).replace("1", "%").replace("A", "%").replace("6", "_")
        )
    elif where_column.category == LogicalCategory.BOOLEAN:
        # BOOLEAN has only Eq/NotEq in the operator map — no ordering, no BETWEEN.
        where_operator = random.choice(["=", "!=", "<>"])
        where_value = random.choice(["TRUE", "FALSE"])
    elif random.random() < 0.8:
        where_operator = random.choice(["==", "<>", "=", "!=", "<", "<=", ">", ">="])
        where_value = f"{str(random_value(where_column.category))}"
    else:
        return (
            f"{column} BETWEEN {str(random_value(where_column.category))} "
            f"AND {str(random_value(where_column.category))}"
        )
    return f"{column} {where_operator} {where_value}"


def _equi_conjuncts(left_rel: Relation, right_rel: Relation) -> Tuple[str, ...]:
    """One or more `left.col = right.col` conjuncts, or () if none are admissible.

    A BOOLEAN key is never allowed to be the ONLY conjunct. With an NDV of 2 it is a
    near-cartesian product on its own — `mixed ⋈ mixed ON b_value = b_value` is
    2,000,000 rows — and a fuzzer that spends its budget materialising those is
    testing the scheduler, not join keys. Paired with any other conjunct the key is
    still hashed, compared and NULL-handled exactly as it would be alone, which is
    the thing under test.
    """
    conditions = []
    boolean_only = True
    last_value, this_value = -1.0, random.random()
    # Multiple conditions by cycling over ever-increasing random values until a
    # lower one comes up — a cheap geometric-ish decay.
    while this_value > last_value:
        last_value, this_value = this_value, random.random()
        pair = _pick_pair(left_rel.key_fields, right_rel.key_fields, joinable)
        if pair is None:
            return ()
        left_column, right_column = pair
        conditions.append(
            f"{left_rel.qualified(left_column)} = {right_rel.qualified(right_column)}"
        )
        if left_column.category != LogicalCategory.BOOLEAN:
            boolean_only = False

    if boolean_only:
        pair = _pick_pair(
            left_rel.key_fields,
            right_rel.key_fields,
            lambda l, r: joinable(l, r) and l != LogicalCategory.BOOLEAN,
        )
        if pair is None:
            return ()
        left_column, right_column = pair
        conditions.append(
            f"{left_rel.qualified(left_column)} = {right_rel.qualified(right_column)}"
        )
    return tuple(conditions)


def _theta_conjunct(left_rel: Relation, right_rel: Relation) -> Optional[str]:
    """One `left.col <op> right.col` inequality, or None if none are admissible.

    DECIMAL is excluded HERE and only here: the binder refuses a non-equality join
    condition on a DECIMAL column outright ("JOINs on DECIMAL types only supports
    Equals and Not Equals", binder/join.py), so generating one only exercises that
    refusal. It is pinned by join_known_gaps/theta-join-on-decimal-is-refused rather
    than assumed. An ASOF MATCH_CONDITION on a DECIMAL column takes a different path
    and DOES run, so it is not excluded there.
    """
    def theta_comparable(left, right):
        return (
            comparable(left, right)
            and left != LogicalCategory.DECIMAL
            and right != LogicalCategory.DECIMAL
        )

    pair = _pick_pair(left_rel.key_fields, right_rel.key_fields, theta_comparable)
    if pair is None:
        return None
    left_column, right_column = pair
    operator = random.choice(["<", "<=", ">", ">="])
    return (
        f"{left_rel.qualified(left_column)} {operator} "
        f"{right_rel.qualified(right_column)}"
    )


def _make_leg(join_type: str, left_rel: Relation, right_rel: Relation,
              independent: bool) -> Optional[JoinLeg]:
    """Build one join leg, or None when this relation pair admits no condition."""
    if join_type == "ASOF JOIN":
        pair = _pick_pair(left_rel.key_fields, right_rel.key_fields, comparable)
        if pair is None:
            return None
        left_column, right_column = pair
        operator = random.choice([">=", ">", "<=", "<"])
        # ASOF's optional equi-partition is `USING (name)`, which needs a column of
        # that NAME in both relations — guaranteed for a self-join, chancy otherwise,
        # so it is only offered where it exists.
        # The USING partition is an equi key, so it takes the same exclusions any
        # other join key does — the NaN one, and ARRAY.
        #
        # ARRAY matters here beyond "we do not join on arrays": `ON a.arr = b.arr` is
        # correctly REFUSED (IncorrectTypeError), but `USING (arr)` is ACCEPTED and
        # returns ZERO rows from an ASOF whose LEFT semantics require |left| — see
        # join_known_gaps/asof-using-an-array-key-drops-every-row. The exclusion
        # disappears with that entry.
        def keyable(relation):
            return {
                column.name
                for column in relation.key_fields
                if column.category not in _UNJOINABLE
            }

        shared = sorted(keyable(left_rel) & keyable(right_rel))
        using: Tuple[str, ...] = ()
        if shared and random.random() < 0.4:
            using = (random.choice(shared),)
        return JoinLeg(
            join_type, right_rel, using, (),
            match_left=left_rel.qualified(left_column),
            match_operator=operator,
            match_right=right_rel.qualified(right_column),
            match_right_column=right_column.name,
            independent=independent,
        )

    equi = _equi_conjuncts(left_rel, right_rel)
    if not equi:
        return None

    # THETA CONJUNCTS ARE INNER-ONLY, by architect ruling. Every other join type
    # refuses a non-equality ON conjunct at plan time — it used to DROP it silently
    # and answer the equi-only join instead, which is the P0 this generator found.
    # Generating one on a LEFT/FULL/SEMI/ANTI join would therefore only exercise the
    # refusal, over and over; `test_regression_theta_on_condition_is_inner_only`
    # pins that refusal directly instead.
    theta: Tuple[str, ...] = ()
    if join_type in _INNER_JOINS and random.random() < 0.25:
        conjunct = _theta_conjunct(left_rel, right_rel)
        if conjunct is not None:
            theta = (conjunct,)
    return JoinLeg(join_type, right_rel, equi, theta, None, None, None, None, independent)


def generate_join(relations: List[Relation]) -> Optional[JoinSpec]:
    """Build one statement over `relations` (2 or 3 of them, already aliased)."""
    head, rest = relations[0], relations[1:]

    if len(rest) == 1:
        join_type = random.choice(_JOIN_TYPES)
    else:
        # Three-relation statements use only the join types that keep every relation
        # projectable and every leg independently expressible: a reducing leg would
        # collapse the row to one side mid-chain, and ASOF's LEFT semantics do not
        # compose with a following leg in a way this generator can build an oracle
        # for. Both are covered thoroughly in the two-relation shapes.
        join_type = random.choice(
            ("JOIN", "INNER JOIN", "LEFT JOIN", "LEFT OUTER JOIN", "RIGHT JOIN",
             "FULL OUTER JOIN")
        )

    legs = []
    for index, relation in enumerate(rest):
        if index == 0:
            leg = _make_leg(join_type, head, relation, independent=True)
        else:
            # A later leg may key against ANY preceding relation — keying only ever
            # against the head produces left-deep-only plans and never exercises a
            # bushy one. `independent` records whether it happened to reference the
            # head alone, which is what makes leg reordering a valid oracle.
            available = [head] + [previous.relation for previous in legs]
            anchor = random.choice(available)
            leg_type = random.choice(
                ("JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN")
            )
            leg = _make_leg(leg_type, anchor, relation, independent=anchor is head)
        if leg is None:
            return None
        legs.append(leg)

    # Relations whose columns may be projected. A reducing join emits only its left
    # side, so nothing after one is available.
    projectable = [head]
    for leg in legs:
        if leg.is_reducing:
            break
        projectable.append(leg.relation)

    selected = [
        relation.qualified(column)
        for relation in projectable
        for column in relation.fields
        if random.random() < 0.2
    ]
    if not selected:
        # `SELECT *` is the natural fallback, but it is not always legal SQL here. A
        # result carrying two columns with the same output name is rejected by
        # ratified decision (see opteryx/operators/exit/exit.pyx: "per architect
        # decision this errors here (rather than emit duplicate names or
        # auto-suffix). Queries must qualify/alias such columns explicitly"), and
        # `*`-expansion yields the BARE name — so any two relations sharing a column
        # name make it invalid. Every self-join shares all of them.
        #
        # This is NOT a bug class being filtered out: it is the generator declining
        # to emit SQL the dialect deliberately refuses, the same reason `joinable()`
        # will not emit `VARCHAR = INTEGER`. `*` is kept wherever it IS legal.
        seen: set = set()
        collides = False
        for relation in projectable:
            if seen & relation.names:
                collides = True
                break
            seen |= relation.names
        if not collides:
            selected = ["*"]
        else:
            # Explicit qualified references ARE accepted even when the output names
            # collide, so one column per projectable relation is always legal.
            selected = [relation.qualified(relation.fields[0]) for relation in projectable]
    select_clause = "SELECT " + ", ".join(selected)

    predicates = []
    for relation in projectable:
        if random.random() < 0.3:
            if predicates:
                predicates.append(random.choice(["AND", "OR", "AND NOT"]))
            predicates.append(generate_condition(relation))
            while random.random() < 0.1:
                predicates.append(random.choice(["AND", "OR", "AND NOT"]))
                predicates.append(generate_condition(relation))

    return JoinSpec(
        head=head,
        legs=tuple(legs),
        select_clause=select_clause,
        where=" ".join(predicates) if predicates else None,
    )


def choose_relations() -> List[Relation]:
    """Two or three aliased relations for one case, self-joins included.

    Relations are drawn WITH replacement: `planets AS r0 JOIN planets AS r1` is a
    self-join, which the previous generator excluded outright (`while table1 ==
    table2`) and which is its own bug surface — both legs share a schema, every
    output name collides, and the two sides of the join read the same buffers.
    """
    tables = get_tables()
    count = 2 if random.random() < 0.75 else 3
    chosen = [tables[random.choice(range(len(tables)))] for _ in range(count)]
    return [
        Relation(table=table, alias=f"r{index}", fields=fields, rows=rows)
        for index, (table, fields, rows) in enumerate(chosen)
    ]


# ---------------------------------------------------------------------------------
# Oracles
# ---------------------------------------------------------------------------------
# Above this many rows an oracle that MATERIALISES its result stands down and says so
# in the ledger. Counting oracles are unaffected. A BOOLEAN key with an NDV of 2 can
# produce millions of rows from 2,000-row relations, and rendering those to compare
# multisets is minutes of wall clock for no extra signal.
_MATERIALISE_CAP = 50_000

# Above this cross product the cross-join ground-truth oracle stands down: it builds
# the full cartesian product before filtering, so `missions × mixed` alone would be
# 9.26 million rows.
_CROSS_CAP = 400_000


def count(sql: str) -> int:
    return harness.scalar(f"SELECT COUNT(*) FROM {sql}")


def check_join_algebra(spec: JoinSpec) -> None:
    """Five identities that hold for any two relations and any ON condition.

    Exact equalities over row counts, independent of the data, each failing loudly if
    the join emits, drops or duplicates a row it should not — including the NULL-key
    and unmatched-row handling a "did not raise" check cannot see.

    NECESSARY BUT NOT SUFFICIENT, and the theta P0 is why that matters: when the
    engine silently dropped a non-equality ON conjunct it dropped it from EVERY join
    type, so all five identities still held over its own wrong answers. That is what
    `check_inner_equals_cross_filter` is for.

    The WHERE clause is deliberately excluded: a filter over a LEFT join is not
    equivalent to the same filter over the INNER join it decomposes into, so the
    identities only hold for the unfiltered join.
    """
    leg = spec.legs[0]
    t1, t2 = spec.head.ref, leg.relation.ref
    on = " AND ".join(leg.equi + leg.theta)

    left_rows = count(t1)
    inner = count(f"{t1} INNER JOIN {t2} ON {on}")
    left = count(f"{t1} LEFT JOIN {t2} ON {on}")
    semi = count(f"{t1} LEFT SEMI JOIN {t2} ON {on}")
    anti = count(f"{t1} LEFT ANTI JOIN {t2} ON {on}")
    anti_reverse = count(f"{t2} LEFT ANTI JOIN {t1} ON {on}")
    full = count(f"{t1} FULL OUTER JOIN {t2} ON {on}")
    inner_swapped = count(f"{t2} INNER JOIN {t1} ON {on}")
    right_mirror = count(f"{t2} RIGHT JOIN {t1} ON {on}")

    context = (
        f"\n  ON {on}\n  |{t1}|={left_rows} inner={inner} left={left} semi={semi} "
        f"anti={anti} anti_rev={anti_reverse} full={full}"
    )

    # Every left row either matches or it does not.
    assert semi + anti == left_rows, f"SEMI + ANTI != |{t1}|{context}"
    # A LEFT join is the matched pairs plus one row per unmatched left row.
    assert left == inner + anti, f"LEFT != INNER + ANTI{context}"
    # An inner join is symmetric in its operands.
    assert inner == inner_swapped, f"INNER is not commutative{context}"
    # A FULL OUTER join is the matched pairs plus the unmatched rows of each side.
    assert full == inner + anti + anti_reverse, f"FULL != INNER + ANTI + ANTI_reverse{context}"
    # RIGHT is LEFT with the operands exchanged.
    assert right_mirror == left, f"RIGHT is not the mirror of LEFT{context}"


def check_inner_equals_cross_filter(spec: JoinSpec) -> None:
    """An INNER join is the cartesian product filtered by its ON condition.

    THE ONLY ORACLE HERE WITH INDEPENDENT GROUND TRUTH. Everything else compares the
    engine against itself, which is exactly why the theta P0 survived: the engine
    dropped the non-equality conjunct consistently, so its own answers stayed
    self-consistent and all five algebra identities passed over them.

    `CROSS JOIN ... WHERE p` reaches the answer through a different plan — no build
    table, no probe, no key hash, the predicate evaluated as an ordinary filter — so
    agreement is real evidence and disagreement names the join path as the culprit.

    Two-relation INNER joins only, and only when the cartesian product is small
    enough to build.
    """
    leg = spec.legs[0]
    on = " AND ".join(leg.equi + leg.theta)
    joined = count(f"{spec.head.ref} INNER JOIN {leg.relation.ref} ON {on}")
    crossed = count(f"{spec.head.ref} CROSS JOIN {leg.relation.ref} WHERE {on}")
    assert joined == crossed, (
        "INNER JOIN disagrees with the cartesian product filtered by the same "
        f"predicate — the join path is not evaluating the whole ON clause\n"
        f"  ON {on}\n"
        f"  INNER JOIN        -> {joined}\n"
        f"  CROSS JOIN WHERE  -> {crossed}"
    )


def check_semi_anti_match_exists(spec: JoinSpec) -> None:
    """SEMI and ANTI agree with EXISTS / NOT EXISTS over the same predicate.

    Second ground-truth oracle, and the one that pinned the theta P0's SEMI/ANTI half
    (the engine said 7/2 where the truth was 4/5). A correlated EXISTS is planned and
    executed as a subquery decorrelation, not as a mark join, so it is genuinely
    independent of the SEMI/ANTI path.
    """
    leg = spec.legs[0]
    on = " AND ".join(leg.equi + leg.theta)
    head, right = spec.head, leg.relation
    semi = count(f"{head.ref} LEFT SEMI JOIN {right.ref} ON {on}")
    anti = count(f"{head.ref} LEFT ANTI JOIN {right.ref} ON {on}")
    exists = harness.scalar(
        f"SELECT COUNT(*) FROM {head.ref} WHERE EXISTS "
        f"(SELECT 1 FROM {right.ref} WHERE {on})"
    )
    not_exists = harness.scalar(
        f"SELECT COUNT(*) FROM {head.ref} WHERE NOT EXISTS "
        f"(SELECT 1 FROM {right.ref} WHERE {on})"
    )
    assert semi == exists, (
        f"LEFT SEMI JOIN disagrees with EXISTS over the same predicate\n  ON {on}\n"
        f"  SEMI   -> {semi}\n  EXISTS -> {exists}"
    )
    assert anti == not_exists, (
        f"LEFT ANTI JOIN disagrees with NOT EXISTS over the same predicate\n  ON {on}\n"
        f"  ANTI       -> {anti}\n  NOT EXISTS -> {not_exists}"
    )


def check_asof_left_semantics(spec: JoinSpec) -> None:
    """An ASOF join emits every left row EXACTLY once.

    ASOF is a nearest-match, not a fan-out: each probe row takes at most one build
    row, and an unmatched one is still emitted with a NULL right half. So the row
    count is pinned to |left| exactly — too many means the nearest-match selection
    fanned out, too few means unmatched probe rows were dropped, and both are the
    failure modes a bisect-per-partition matcher actually has.
    """
    leg = spec.legs[0]
    emitted = count(f"{spec.head.ref} {leg.clause()}")
    left_rows = count(spec.head.ref)
    assert emitted == left_rows, (
        "ASOF is LEFT semantics — every left row exactly once\n"
        f"  {leg.clause()}\n"
        f"  emitted {emitted}, |left| {left_rows}"
    )


def check_asof_match_condition_holds(spec: JoinSpec) -> None:
    """Every pair an ASOF emits must satisfy the MATCH_CONDITION that selected it.

    The cheapest oracle here and the one with the most teeth: it needs no ground
    truth at all, because a matched pair violating its own condition is wrong on its
    face. It found the string-key defect immediately —
    `planets AS a ASOF JOIN planets AS b MATCH_CONDITION(a.name >= b.name)` returned
    ('Mercury','Pluto'), and 'Pluto' is not <= 'Mercury'. 1,999 of 2,000 matched rows
    violated it on a VARCHAR self-join while every numeric column was clean, because
    the ASOF bisect ordered string keys through a numeric normalizer.

    Deliberately weaker than "is it the NEAREST match": which qualifying row gets
    picked is an ordering question, and `check_asof_matches_are_reachable` covers the
    existence half. This covers the half that needs no interpretation.
    """
    leg = spec.legs[0]
    violations = harness.scalar(
        f"SELECT COUNT(*) FROM (SELECT {leg.match_left} AS lv, {leg.match_right} AS rv "
        f"FROM {spec.head.ref} {leg.clause()}) AS t "
        f"WHERE t.rv IS NOT NULL AND NOT (t.lv {leg.match_operator} t.rv)"
    )
    assert violations == 0, (
        f"{violations} ASOF matches do not satisfy their own MATCH_CONDITION\n"
        f"  {spec.head.ref} {leg.clause()}"
    )


def check_asof_matches_are_reachable(spec: JoinSpec) -> None:
    """An ASOF matches a left row exactly when SOME build row could satisfy it.

    ASOF picks the NEAREST qualifying build row, so *which* one it picks is a matter
    of ordering — but *whether* one exists is not, and that is exactly checkable:
    for `l.x >= r.y` a qualifying build row exists iff `l.x >= MIN(r.y)`, and
    symmetrically `<=` / `<` against `MAX`. That reduces the whole nearest-match
    machinery to one aggregate and one comparison, evaluated on a completely
    different plan — no build table, no per-group sort, no bisect.

    It catches the two failures a bisect-per-partition matcher actually has: a probe
    that finds nothing when a qualifying row exists (bisect off the end of the
    group), and one that matches when nothing qualifies (reaching past the group).

    NOT the same check as `check_asof_left_semantics`, which counts EMITTED rows;
    this one counts MATCHED ones, and an unmatched row is emitted too.

    Only run without USING: an equi-partition makes reachability a per-partition
    question, and the aggregate form would have to repeat the partition key — a
    second thing to get wrong rather than an independent check.
    """
    leg = spec.legs[0]
    aggregate = "MIN" if leg.match_operator.startswith(">") else "MAX"
    matched = harness.scalar(
        f"SELECT COUNT(*) FROM (SELECT {leg.match_right} AS witness "
        f"FROM {spec.head.ref} {leg.clause()}) AS t WHERE t.witness IS NOT NULL"
    )
    reachable = harness.scalar(
        f"SELECT COUNT(*) FROM {spec.head.ref} WHERE {leg.match_left} "
        f"{leg.match_operator} (SELECT {aggregate}({leg.match_right_column}) "
        f"FROM {leg.relation.table})"
    )
    assert matched == reachable, (
        "the set of ASOF-matched left rows is not the set that has a qualifying "
        f"build row\n  {leg.clause()}\n"
        f"  matched (non-NULL right half)      -> {matched}\n"
        f"  reachable ({leg.match_left} {leg.match_operator} "
        f"{aggregate}) -> {reachable}"
    )


def check_tautology_invariance(spec: JoinSpec) -> bool:
    """Conjoining a tautology to the predicate must not change the result.

    Compared as multisets, so a transformation that duplicates or drops rows fails
    here even when the distinct row set is unchanged. Returns False when it stood
    down on size.
    """
    original = spec.sql()
    if count(f"({original}) AS o") > _MATERIALISE_CAP:
        return False
    with_tautology = spec.sql(where=f"1=1 AND ({spec.where})" if spec.where else "1=1")

    before = harness.result_multiset(original)
    after = harness.result_multiset(with_tautology)
    assert before == after, (
        "adding a tautology changed the result\n"
        f"  original:    {original}\n"
        f"  transformed: {with_tautology}\n"
        f"  rows {len(before)} -> {len(after)}"
    )
    return True


def check_condition_order_invariance(spec: JoinSpec) -> bool:
    """Reversing the ON conjuncts must not change the result."""
    leg = spec.legs[0]
    conjuncts = leg.equi + leg.theta
    if len(conjuncts) < 2:
        return False
    original = spec.sql()
    if count(f"({original}) AS o") > _MATERIALISE_CAP:
        return False
    reversed_legs = (
        JoinLeg(
            join_type=leg.join_type,
            relation=leg.relation,
            equi=tuple(reversed(leg.equi)),
            theta=leg.theta,
            match_left=leg.match_left,
            match_operator=leg.match_operator,
            match_right=leg.match_right,
            match_right_column=leg.match_right_column,
            independent=leg.independent,
        ),
    ) + spec.legs[1:]
    transformed = spec.sql(legs=reversed_legs)
    before = harness.result_multiset(original)
    after = harness.result_multiset(transformed)
    assert before == after, (
        "reversing the ON conjuncts changed the result\n"
        f"  original: {original}\n"
        f"  reversed: {transformed}\n"
        f"  rows {len(before)} -> {len(after)}"
    )
    return True


def check_leg_order_invariance(spec: JoinSpec) -> bool:
    """Swapping two INDEPENDENT INNER legs must not change the result.

    `A JOIN B ON p1 JOIN C ON p2`, where p2 references only A, is the same relation
    as `A JOIN C ON p2 JOIN B ON p1`. This is the three-relation shape's own oracle,
    and it is the one that exercises join REORDERING — the planner is free to build
    either as a left-deep or bushy tree, and DPccp's cost model decides. A disagreement
    means a reordering changed the answer, not just the plan.

    INNER only: outer joins do not commute in general, so the identity would not hold.
    """
    if len(spec.legs) != 2:
        return False
    first, second = spec.legs
    if not second.independent:
        return False
    if first.join_type not in _INNER_JOINS or second.join_type not in _INNER_JOINS:
        return False
    original = spec.sql()
    if count(f"({original}) AS o") > _MATERIALISE_CAP:
        return False
    swapped = spec.sql(legs=(second, first))
    before = harness.result_multiset(original)
    after = harness.result_multiset(swapped)
    assert before == after, (
        "swapping two independent INNER join legs changed the result\n"
        f"  original: {original}\n"
        f"  swapped:  {swapped}\n"
        f"  rows {len(before)} -> {len(after)}"
    )
    return True


# ---------------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------------

_ALL_ORACLE_NAMES = (
    "join_algebra",
    "inner_equals_cross_filter",
    "semi_anti_match_exists",
    "asof_left_semantics",
    "asof_match_condition_holds",
    "asof_matches_are_reachable",
    "tautology_invariance",
    "condition_order_invariance",
    "leg_order_invariance",
)

# Oracles that can legitimately never fire on a short run, so `test_sql_fuzzing_join`
# does not demand coverage of them: each needs a shape the generator produces only
# sometimes (a second ON conjunct, two independent INNER legs, an un-partitioned
# ASOF), and each can also stand down on size.
_OPTIONAL_ORACLES = (
    "condition_order_invariance",
    "leg_order_invariance",
    "asof_matches_are_reachable",
)


class _RunLedger:
    """What this run actually did.

    Without it, a run in which every generated statement hit a registered defect — or
    in which every materialising oracle stood down on size — reports success while
    having asserted nothing. Modelled on the single-table fuzzer's ledger.
    """

    def __init__(self) -> None:
        self.cases = 0
        self.statements_executed = 0
        self.oracle_runs: Counter = Counter()
        self.oracle_skips: Counter = Counter()
        self.known_gap_hits: Counter = Counter()
        self.shapes: Counter = Counter()

    def report(self) -> str:
        lines = [
            "",
            "═══ join fuzzer ═══",
            f"cases:               {self.cases}",
            f"statements executed: {self.statements_executed}",
            "",
            "oracle invocations by kind (skipped = stood down on size or shape):",
        ]
        for name in _ALL_ORACLE_NAMES:
            skipped = self.oracle_skips.get(name, 0)
            note = f"   ({skipped} skipped)" if skipped else ""
            lines.append(f"  {self.oracle_runs.get(name, 0):6d}  {name}{note}")
        lines += ["", "shapes generated:"]
        for shape, hits in sorted(self.shapes.items()):
            lines.append(f"  {hits:6d}  {shape}")
        if self.known_gap_hits:
            lines += ["", "registered defects hit (see join_known_gaps.py):"]
            for gap_id, hits in self.known_gap_hits.most_common():
                lines.append(f"  {hits:6d}  {gap_id}")
        return "\n".join(lines)


LEDGER = _RunLedger()


def _classify(error: Exception) -> None:
    """Re-raise, unless this exact failure is already a registered defect.

    `join_known_gaps.match` never matches an `AssertionError`, so no oracle violation
    — no wrong answer — can reach the absorbing branch.
    """
    defect = known_gaps.match(error)
    if defect is None:
        raise error
    LEDGER.known_gap_hits[defect.id] += 1
    print(f"  known defect: {defect.id} ({type(error).__name__})")


def _applicable_oracles(spec: JoinSpec):
    """The oracles whose precondition this statement satisfies, as (name, callable).

    Preconditions are STRUCTURAL — a property of the generated shape, never of an
    error message — so an oracle can only be stood down by a query shape that is
    visible in the ledger, not by a failure it found inconvenient.
    """
    oracles = []
    if spec.is_two_relation:
        leg = spec.legs[0]
        if leg.is_asof:
            oracles.append(("asof_left_semantics", check_asof_left_semantics))
            oracles.append(
                ("asof_match_condition_holds", check_asof_match_condition_holds)
            )
            if not leg.equi:
                oracles.append(
                    ("asof_matches_are_reachable", check_asof_matches_are_reachable)
                )
        else:
            # The five identities hold for ANY ON condition, theta included — but a
            # theta conjunct is INNER-only by ruling, so re-expressing the same ON
            # under LEFT/SEMI/ANTI (which is what the oracle does) would hit the
            # refusal rather than test anything.
            if leg.is_plain_equi:
                oracles.append(("join_algebra", check_join_algebra))
                oracles.append(("semi_anti_match_exists", check_semi_anti_match_exists))
            if spec.cross_product <= _CROSS_CAP:
                oracles.append(
                    ("inner_equals_cross_filter", check_inner_equals_cross_filter)
                )
        if not leg.is_asof:
            oracles.append(
                ("condition_order_invariance", check_condition_order_invariance)
            )
    else:
        oracles.append(("leg_order_invariance", check_leg_order_invariance))
    # STRUCTURAL EXCLUSION for join_known_gaps/asof-tie-breaking-is-not-deterministic:
    # an ASOF whose match column has ties returns a different tied row on different
    # executions, so any oracle that runs the query TWICE and compares is comparing
    # against noise. The ASOF oracles that remain are single-execution ones, which is
    # where the real signal is anyway. Delete this with the register entry.
    if not any(leg.is_asof for leg in spec.legs):
        oracles.append(("tautology_invariance", check_tautology_invariance))
    return oracles


def _describe(spec: JoinSpec) -> str:
    """A coarse shape label for the ledger — what kinds of statement got generated."""
    parts = [f"{len(spec.relations)}-relation"]
    if len({relation.table for relation in spec.relations}) < len(spec.relations):
        parts.append("self-join")
    if any(leg.is_asof for leg in spec.legs):
        parts.append("asof")
    if any(leg.theta for leg in spec.legs):
        parts.append("theta")
    if any(leg.is_reducing for leg in spec.legs):
        parts.append("reducing")
    return " ".join(parts)


def run_case(seed: int) -> Optional[JoinSpec]:
    """Generate and check one statement. Raises on the first oracle that fails.

    Each step is classified independently rather than the case being abandoned at the
    first registered defect: the algebra identities read only the ON condition, so
    they remain worth checking even when the generated WHERE clause is one the engine
    cannot currently execute.
    """
    random.seed(seed)

    spec = generate_join(choose_relations())
    if spec is None:
        # No admissible join condition for the relations drawn — e.g. an ASOF between
        # two relations with no orderable comparable column pair. Not a finding.
        return None

    statement = spec.sql()
    print(format_sql(statement))
    LEDGER.cases += 1
    LEDGER.shapes[_describe(spec)] += 1

    start_time = time.time()
    try:
        shape = execute_and_get_shape(statement)
    except Exception as error:  # noqa: BLE001 - _classify re-raises unless registered
        _classify(error)
    else:
        print(f"Shape: {shape}, Execution Time: {time.time() - start_time:.2f} seconds")
        LEDGER.statements_executed += 1

    for name, oracle in _applicable_oracles(spec):
        try:
            outcome = oracle(spec)
        except AssertionError:
            # Always fatal. There is deliberately no path that silences a wrong
            # answer: the register declines to match AssertionError at all.
            raise
        except Exception as error:  # noqa: BLE001 - _classify re-raises unless registered
            _classify(error)
            continue
        if outcome is False:
            LEDGER.oracle_skips[name] += 1
        else:
            LEDGER.oracle_runs[name] += 1
    return spec


def test_sql_fuzzing_join():
    """Fuzz joins, failing on the first case that breaks an oracle.

    Runs as one test rather than one-test-per-case: the nightly schedule sets
    TEST_ITERATIONS to 100000, and parametrizing over that many cases makes
    collection, not execution, the cost.
    """
    iterations = harness.iterations(DEFAULT_ITERATIONS)
    seeds = harness.case_seeds("join", iterations)
    print(f"join fuzzer: {len(seeds)} cases ({len(seeds) - iterations} pinned)")

    for case, seed in enumerate(seeds):
        print(f"\n--- case {case} seed {seed}")
        try:
            run_case(seed)
        except Exception as error:
            raise AssertionError(
                f"join fuzzing failed on seed {seed} (case {case} of {len(seeds)})\n"
                f"reproduce with: TEST_SEED unset, add {seed} to tests/fuzzing/seeds/join.txt\n"
                f"{type(error).__name__}: {error}"
            ) from error

    print(LEDGER.report())
    assert LEDGER.cases > 0, "no fuzz cases ran at all"
    assert LEDGER.statements_executed > 0, (
        f"{LEDGER.cases} cases ran but not one statement executed — every generated "
        f"query hit a registered defect. The fuzzer tested nothing.\n{LEDGER.report()}"
    )
    if LEDGER.cases < 60:
        return
    silent = [
        name
        for name in _ALL_ORACLE_NAMES
        if name not in _OPTIONAL_ORACLES and not LEDGER.oracle_runs.get(name)
    ]
    assert not silent, (
        f"these oracles never fired across {LEDGER.cases} cases, so they are "
        f"asserting nothing: {silent}. Either the generator no longer produces the "
        f"shapes they need, or their precondition in _applicable_oracles() has "
        f"stopped matching.\n{LEDGER.report()}"
    )


@pytest.mark.parametrize("defect", known_gaps.REGISTER, ids=lambda d: d.id)
def test_registered_defect_still_reproduces(defect):
    """Every registered defect must still be broken.

    This is what stops the register from becoming a place bugs go to be forgotten.
    When a defect is fixed this goes red, and the only way to make it green is to
    delete the entry — which puts the construct back into ordinary fuzzing.
    """
    if defect.error_type == "WrongAnswer":
        # A wrong-answer entry has no exception to match; each is pinned by its own
        # test_wrong_answer_* test. Assert only that the repro still EXECUTES, so one
        # that has rotted into a syntax error is caught.
        for row in harness.rows(defect.repro):
            for value in row:
                repr(value)
        return

    with pytest.raises(Exception) as raised:  # noqa: PT011 - the type IS the assertion
        for row in harness.rows(defect.repro):
            for value in row:
                repr(value)

    actual = type(raised.value).__name__
    assert actual == defect.error_type, (
        f"registered defect `{defect.id}` now raises {actual}, not {defect.error_type}. "
        f"If it is fixed, delete the register entry.\n  {defect.repro}\n  {raised.value}"
    )
    assert defect.signature in str(raised.value), (
        f"registered defect `{defect.id}` no longer matches its signature "
        f"{defect.signature!r}; the register would stop absorbing it.\n  {raised.value}"
    )


# ---------------------------------------------------------------------------------
# Durable pins for the defects this fuzzer found.
#
# The seeds in seeds/join.txt keep each case in the generated corpus, but a seed is
# an input to the GENERATOR: change the generator and the same seed produces a
# different join. These are SQL, so they do not drift. Same split the single-table
# fuzzer uses — see the header of seeds/single_table_select.txt.
# ---------------------------------------------------------------------------------


def test_regression_full_outer_array_probe_payload():
    """FULL OUTER emits its NULL probe half against an ARRAY column.

    The unmatched-build tail gathers every probe column as the null row against a
    plan-typed, zero-row schema morsel. Gathering an ARRAY row recurses on the
    column's child vector even when every row asked for is NULL — an all-NULL ARRAY
    half still emits a typed, empty child — and the schema morsel carried no child,
    so the query was unrunnable:

        RuntimeError: native engine: error code 1: FULL OUTER: ARRAY probe payload
        has no child vector to emit NULLs against

    Reversing the operands worked, because then the ARRAY landed on the build side,
    which gathers against real retained morsels.
    """
    on = "testdata.astronauts.year = testdata.planets.id"
    rows = list(
        harness.rows(
            "SELECT testdata.astronauts.missions, testdata.planets.name "
            f"FROM testdata.astronauts FULL OUTER JOIN testdata.planets ON {on}"
        )
    )

    def count(sql):
        return harness.scalar(f"SELECT COUNT(*) FROM {sql}")

    inner = count(f"testdata.astronauts INNER JOIN testdata.planets ON {on}")
    anti = count(f"testdata.astronauts LEFT ANTI JOIN testdata.planets ON {on}")
    anti_reverse = count(f"testdata.planets LEFT ANTI JOIN testdata.astronauts ON {on}")
    assert len(rows) == inner + anti + anti_reverse

    # The unmatched BUILD rows are the ones the tail emits, and their ARRAY column
    # must be NULL — not an empty array, and not a row silently dropped.
    unmatched_build = [row for row in rows if row[1] is not None]
    assert len(unmatched_build) == anti_reverse, (
        f"the tail emitted {len(unmatched_build)} unmatched build rows, not {anti_reverse}"
    )
    assert all(row[0] is None for row in unmatched_build), (
        "an unmatched build row came back with a non-NULL probe-side ARRAY"
    )


def test_regression_decimal_join_key_coercion():
    """An equi-join between a DECIMAL key and an INTEGER/FLOAT one runs.

    Two keys of different physical types hash differently, so the compiler
    materializes a CAST column on the narrower side and keys on that. The cast node
    was minted with `parameters=[]`, and a DECIMAL cast reads its (precision, scale)
    from exactly there, so every such join died at plan time with
    `ValueError: CAST to DECIMAL requires (precision, scale)`.

    `testdata.planets.gravity` is the DECIMAL column; `satellites.radius` is FLOAT64
    and `satellites.id` INT64, which covers both cross-type directions.
    """
    for probe_key in ("radius", "id"):
        on = f"testdata.planets.gravity = testdata.satellites.{probe_key}"
        matched = harness.scalar(
            f"SELECT COUNT(*) FROM testdata.planets INNER JOIN testdata.satellites ON {on}"
        )
        left = harness.scalar(
            f"SELECT COUNT(*) FROM testdata.planets LEFT JOIN testdata.satellites ON {on}"
        )
        unmatched = harness.scalar(
            f"SELECT COUNT(*) FROM testdata.planets LEFT ANTI JOIN testdata.satellites ON {on}"
        )
        assert left == matched + unmatched, (
            f"gravity = {probe_key}: LEFT ({left}) != INNER ({matched}) + ANTI ({unmatched})"
        )
        assert matched > 0, (
            f"gravity = {probe_key} now matches nothing; the coercion has stopped "
            f"bringing the two key representations together, which is the silent "
            f"wrong answer it exists to prevent"
        )


def test_regression_ilike_accepts_varbinary():
    """ILIKE takes a VARBINARY subject, like every other member of the LIKE family.

    LIKE, NOT LIKE, RLIKE and NOT RLIKE all accepted a VARBINARY column, and a
    VARBINARY *pattern* was already legal against a VARCHAR subject — only a
    VARBINARY subject under ILIKE was refused, by a kernel gate in draken_like and
    by the missing operator-map rows. Case folding on VARBINARY is the same ASCII
    byte fold VARCHAR gets; only NVARCHAR needs the Unicode codepoint fold.

    `testdata.astronauts.birth_place` is VARBINARY holding JSON text.
    """

    def matching(predicate):
        return harness.scalar(
            f"SELECT COUNT(*) FROM testdata.astronauts WHERE birth_place {predicate}"
        )

    total = matching("IS NOT NULL")

    # Each rewrite target of an ILIKE pattern — prefix, suffix, contains, and the
    # general glob — has its own case-folding gate, so each is exercised.
    for cased, uncased in (
        ('LIKE b\'{"state%\'', 'ILIKE b\'{"STATE%\''),        # -> _STARTS_WITH
        ("LIKE b'%Mobile\"}'", "ILIKE b'%MOBILE\"}'"),        # -> _ENDS_WITH
        ("LIKE b'%Birmingham%'", "ILIKE b'%BIRMINGHAM%'"),    # -> draken_contains
        ('LIKE b\'_"state%\'', 'ILIKE b\'_"STATE%\''),        # -> draken_like
    ):
        sensitive = matching(cased)
        assert sensitive > 0, f"`{cased}` matches nothing; the corpus has changed"
        assert matching(uncased) == sensitive, (
            f"`{uncased}` does not agree with the exactly-cased `{cased}`"
        )

    assert matching("ILIKE b'%TX%'") + matching("NOT ILIKE b'%TX%'") == total
    assert matching("ILIKE ANY (b'%BIRMINGHAM%', b'%MOBILE%')") == matching(
        "ILIKE ANY (b'%Birmingham%', b'%Mobile%')"
    )


if __name__ == "__main__":  # pragma: no cover
    test_sql_fuzzing_join()
    for _defect in known_gaps.REGISTER:
        test_registered_defect_still_reproduces(_defect)
    test_regression_full_outer_array_probe_payload()
    test_regression_decimal_join_key_coercion()
    test_regression_ilike_accepts_varbinary()
    print("✅ okay")


def test_regression_theta_on_condition_is_inner_only():
    """A non-equality ON conjunct runs on INNER and is REFUSED everywhere else.

    It used to be SILENTLY DROPPED by every other join type, which answered the
    equi-only join instead: `planets JOIN satellites ON id = planetId AND mass >
    radius` gave LEFT 179 (truth 161), SEMI 7 (truth 4), ANTI 2 (truth 5). The
    algebra identities could not see it, because dropping the conjunct uniformly
    left them all satisfied over the wrong answers.
    """
    from opteryx.exceptions import NotSupportedError

    on = ("testdata.planets.id = testdata.satellites.planetId "
          "AND testdata.planets.mass > testdata.satellites.radius")
    inner = harness.scalar(
        f"SELECT COUNT(*) FROM testdata.planets INNER JOIN testdata.satellites ON {on}")
    crossed = harness.scalar(
        f"SELECT COUNT(*) FROM testdata.planets CROSS JOIN testdata.satellites WHERE {on}")
    assert inner == crossed == 156, (
        f"INNER with a theta conjunct is no longer the filtered cartesian product "
        f"(INNER {inner}, CROSS+WHERE {crossed})")

    for join_type in ("LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN",
                      "LEFT SEMI JOIN", "LEFT ANTI JOIN"):
        with pytest.raises(NotSupportedError) as raised:
            harness.scalar(
                f"SELECT COUNT(*) FROM testdata.planets {join_type} "
                f"testdata.satellites ON {on}")
        assert "not an equality between the two relations" in str(raised.value), (
            f"{join_type} with a theta ON conjunct no longer refuses with the "
            f"expected message: {raised.value}")

    # The equi-only join is untouched by the guard.
    equi = "testdata.planets.id = testdata.satellites.planetId"
    assert harness.scalar(
        f"SELECT COUNT(*) FROM testdata.planets LEFT JOIN testdata.satellites "
        f"ON {equi}") == 179


def test_regression_asof_string_and_cross_type_match_columns():
    """Every ASOF match is the NEAREST qualifying row, for every key type.

    Two defects, one test. The ASOF bisect ordered its keys through `sort_num_key`,
    which is only order-preserving within one physical type:

      * a STRING-family match column got a meaningless integer, so matches violated
        the MATCH_CONDITION outright — 1,999 of 2,000 on a VARCHAR self-join;
      * a CROSS-TYPE one (INT64 against FLOAT64) normalised each side by its own
        type, so 173 of 177 matched rows violated it.

    Checked against the nearest value computed here, not just against the condition:
    "satisfies the condition" would pass a matcher that returned any qualifying row.
    """
    from decimal import Decimal

    def nearest(values, probe, operator_):
        probe = Decimal(str(probe))
        ordered = [(Decimal(str(v)), v) for v in values if v is not None]
        if operator_ == ">=":
            candidates = [pair for pair in ordered if pair[0] <= probe]
            return max(candidates)[1] if candidates else None
        if operator_ == ">":
            candidates = [pair for pair in ordered if pair[0] < probe]
            return max(candidates)[1] if candidates else None
        if operator_ == "<=":
            candidates = [pair for pair in ordered if pair[0] >= probe]
            return min(candidates)[1] if candidates else None
        candidates = [pair for pair in ordered if pair[0] > probe]
        return min(candidates)[1] if candidates else None

    def nearest_text(values, probe, operator_):
        present = [v for v in values if v is not None]
        if operator_ == ">=":
            candidates = [v for v in present if v <= probe]
            return max(candidates) if candidates else None
        if operator_ == ">":
            candidates = [v for v in present if v < probe]
            return max(candidates) if candidates else None
        if operator_ == "<=":
            candidates = [v for v in present if v >= probe]
            return min(candidates) if candidates else None
        candidates = [v for v in present if v > probe]
        return min(candidates) if candidates else None

    cases = (
        # (left relation, left col, right relation, right col, bare right col, numeric?)
        ("testdata.planets", "name", "testdata.planets", "name", "name", False),
        ("testdata.fuzzing.mixed", "bin_value", "testdata.fuzzing.mixed",
         "bin_value", "bin_value", False),
        ("testdata.satellites", "id", "testdata.planets",
         "orbital_velocity", "orbital_velocity", True),
        ("testdata.satellites", "radius", "testdata.planets",
         "diameter", "diameter", True),
    )
    for left_table, left_col, right_table, right_col, bare, numeric in cases:
        values = [row[0] for row in harness.rows(f"SELECT {bare} FROM {right_table}")]
        for operator_ in (">=", ">", "<=", "<"):
            rows = list(harness.rows(
                f"SELECT a.{left_col} AS l, b.{right_col} AS r FROM {left_table} AS a "
                f"ASOF JOIN {right_table} AS b "
                f"MATCH_CONDITION(a.{left_col} {operator_} b.{right_col})"))
            assert rows, f"{left_col} {operator_} {right_col} produced no rows"
            for probe, matched in rows:
                if probe is None:
                    continue
                expected = (nearest(values, probe, operator_) if numeric
                            else nearest_text(values, probe, operator_))
                if expected is None:
                    assert matched is None, (
                        f"{left_col} {operator_} {right_col}: matched {matched!r} for "
                        f"{probe!r} when no build row qualifies")
                else:
                    assert matched is not None, (
                        f"{left_col} {operator_} {right_col}: no match for {probe!r} "
                        f"when {expected!r} qualifies")
                    if numeric:
                        assert Decimal(str(matched)) == Decimal(str(expected)), (
                            f"{left_col} {operator_} {right_col}: matched {matched!r} "
                            f"for {probe!r}, nearest is {expected!r}")
                    else:
                        assert matched == expected, (
                            f"{left_col} {operator_} {right_col}: matched {matched!r} "
                            f"for {probe!r}, nearest is {expected!r}")


def test_regression_asof_coercion_does_not_leak_into_the_output():
    """The synthetic CAST an ASOF coercion mints stays out of the payload.

    It is appended after the leg's real columns purely to be ordered on. Letting it
    into the build payload emitted a column the declared output layout did not have,
    which shifted every column after it — `SELECT *` came back with the wrong values
    in the right-hand columns.
    """
    import opteryx

    session = opteryx.session()
    # `satellites` and `missions` share no column name, so `SELECT *` over the pair
    # is legal (see the `SELECT *` note in generate_join) and shows the full emitted
    # column set. satellites.id is INT64, missions.Price FLOAT64 — a coerced pair.
    morsels = list(session.execute_to_morsels(
        "SELECT * FROM testdata.satellites AS a ASOF JOIN testdata.missions AS b "
        "MATCH_CONDITION(a.id < b.Price)"))
    assert morsels, "expected rows"
    names = [name.decode() for name in morsels[0].column_names]
    expected = 8 + 8   # satellites columns + missions columns
    assert len(names) == expected, (
        f"the ASOF emitted {len(names)} columns, not {expected} — a synthetic "
        f"coercion column has leaked into the payload: {names}")
    assert not [name for name in names if "join key" in name], (
        f"a synthetic coercion column reached the output by name: {names}")


def test_wrong_answer_nan_join_key_still_matches_itself():
    """Pins join_known_gaps/nan-join-keys-match-themselves-in-every-join-but-inner.

    Asserts the WRONG counts. When INNER and SEMI agree this goes red, and the fix is
    to delete this test, the register entry, and the NaN exclusion in
    `Relation.key_fields`.
    """
    on = "r0.albedo = r1.magnitude"
    relation = "testdata.satellites"

    def count(join_type):
        return harness.scalar(
            f"SELECT COUNT(*) FROM {relation} AS r0 {join_type} {relation} AS r1 ON {on}")

    assert harness.scalar(
        f"SELECT COUNT(*) FROM {relation} WHERE albedo IS NULL") == 0, (
        "satellites.albedo has acquired a NULL; this pin is about NaN, not NULL")
    assert count("INNER JOIN") == 0, "the INNER answer changed; re-derive this pin"
    assert count("LEFT SEMI JOIN") != 0, (
        "a NaN join key no longer matches itself under SEMI — the defect is FIXED. "
        "Delete this test, the `nan-join-keys-match-themselves-in-every-join-but-inner` "
        "register entry, and the exclusion in Relation.key_fields.")


def test_wrong_answer_asof_tie_breaking_is_still_unstable():
    """Pins join_known_gaps/asof-tie-breaking-is-not-deterministic.

    Asserts the INSTABILITY, so this pin is statistical: "I did not see it" is not
    proof it is gone. Bounded attempts — a test that looped until it saw a difference
    would hang once the tie order became deterministic.
    """
    query = (
        "SELECT r0.name, r1.s_high FROM testdata.astronauts AS r0 ASOF JOIN "
        "testdata.fuzzing.mixed AS r1 "
        "MATCH_CONDITION(r0.undergraduate_major > r1.s_null)"
    )
    baseline = harness.result_multiset(query)
    for _ in range(8):
        if harness.result_multiset(query) != baseline:
            return
    raise AssertionError(
        "9 executions of the same ASOF query returned identical results — the tie "
        "order may now be deterministic. Confirm, then delete this test, the "
        "`asof-tie-breaking-is-not-deterministic` register entry, and the ASOF "
        "exclusion in _applicable_oracles()."
    )


def test_wrong_answer_asof_using_an_array_key_still_drops_every_row():
    """Pins join_known_gaps/asof-using-an-array-key-drops-every-row.

    Asserts the WRONG count. When `USING (<array>)` either refuses or partitions
    properly this goes red, and the fix is to delete this test, the register entry,
    and the ARRAY exclusion in `_make_leg`'s USING selection.
    """
    relation = "testdata.fuzzing.mixed"
    match = "MATCH_CONDITION(a.i_null <= b.d_null)"
    left_rows = harness.scalar(f"SELECT COUNT(*) FROM {relation}")

    assert harness.scalar(
        f"SELECT COUNT(*) FROM {relation} AS a ASOF JOIN {relation} AS b {match}"
    ) == left_rows, "the un-partitioned ASOF stopped honouring LEFT semantics too"
    assert harness.scalar(
        f"SELECT COUNT(*) FROM {relation} AS a ASOF JOIN {relation} AS b {match} "
        f"USING (i_group)"
    ) == left_rows, "a scalar USING key stopped honouring LEFT semantics too"
    assert harness.scalar(
        f"SELECT COUNT(*) FROM {relation} AS a ASOF JOIN {relation} AS b {match} "
        f"USING (arr_str)"
    ) == 0, (
        "ASOF with an ARRAY USING key no longer drops every row — the defect is "
        "FIXED. Delete this test, the `asof-using-an-array-key-drops-every-row` "
        "register entry, and the ARRAY exclusion in _make_leg."
    )

