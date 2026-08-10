"""
Oracles for the predicate-subquery fuzzer.

READ `single_table_oracles.py`'s DOCSTRING FIRST. It records the two traps the
previous metamorphic fuzzer fell into, and neither is reintroduced here:

1. **Multisets, never sets.** A `set` discards duplicates, which makes every
   multiplicity bug invisible. That matters more here than anywhere: the whole
   risk of a decorrelation rewrite is that it turns a SEMI join into an INNER
   one and MULTIPLIES outer rows. Set-comparing an `EXISTS` against its join
   rewrite would compare equal while the join returned each planet once per
   satellite. Everything here compares sorted LISTS.
2. **Nothing catches.** No oracle returns a sentinel that a caller can read as
   "passed". An oracle whose queries raise fails the case, and the DRIVER
   decides whether that failure is a registered defect.

WHY THESE ORACLES ARE SHARPER THAN THE SINGLE-TABLE ONES
--------------------------------------------------------
The single-table fuzzer's oracles are metamorphic: it wraps a query, negates a
predicate, adds a tautology, and requires the answer not to move. Those find
real bugs but they only ever compare the engine against ITSELF on a nearby
query, so a wrong answer that is wrong CONSISTENTLY survives every one of them.

This family has something better. Every correlated predicate form has an exact
join rewrite — the very rewrite `DecorrelateSubqueryStrategy` performs
internally — so the subquery spelling can be compared against a join spelling
that reaches the engine through a completely different planner path. A bug in
the decorrelation shows up as a disagreement even when it is perfectly
consistent, because the join spelling never goes near that code.

WHY `disable_decorrelate_subquery` IS NOT IN THE DIFFERENTIAL
-------------------------------------------------------------
`config.features` exposes a kill switch per optimizer strategy, and the obvious
move is to A/B the subquery against itself with decorrelation off. Measured: it
does not produce a different plan, it produces no plan at all —

    EXISTS  -> ValueError: compiled_expression: UNARY_OPERATOR missing centre operand
    IN      -> NotImplementedError: compiled_expression: unsupported node type 39

Decorrelation is not an optimization for this family, it is mandatory lowering:
with it off, a SUBQUERY node reaches the compiled expression and the engine
rejects it. A differential against that flag would compare a result to an error
on every single case. `disable_correlated_filters` IS a genuine optimization and
is in the set.
"""

from __future__ import annotations

import random
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable
from typing import Iterator
from typing import List
from typing import Sequence

from opteryx import config
from tests.fuzzing.harness import scalar
from tests.fuzzing.single_table_oracles import OracleViolation
from tests.fuzzing.single_table_oracles import multiset_positional
from tests.fuzzing.single_table_oracles import row_count
from tests.fuzzing.subquery_grammar import SubqueryCase


@dataclass
class OracleResult:
    name: str
    queries_executed: int


def _first_difference(left: Sequence[str], right: Sequence[str]) -> str:
    for index, (one, other) in enumerate(zip(left, right)):
        if one != other:
            return f"[{index}] {one} != {other}"
    longer, label = (left, "subquery") if len(left) > len(right) else (right, "rewrite")
    if len(left) == len(right):
        return "(no element differs)"
    return f"[{min(len(left), len(right))}] only the {label} has {longer[min(len(left), len(right))]}"


def _require_same(subquery: str, other: str, why: str) -> None:
    """Two spellings of the same semantics must produce the same multiset of rows."""
    produced = multiset_positional(subquery)
    expected = multiset_positional(other)
    if produced != expected:
        raise OracleViolation(
            f"{why}: the subquery form returned {len(produced)} rows, the equivalent "
            f"returned {len(expected)}\n"
            f"  first difference: {_first_difference(produced, expected)}\n"
            f"  subquery:   {subquery}\n"
            f"  equivalent: {other}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Oracles
# ─────────────────────────────────────────────────────────────────────────────


def subquery_matches_join_rewrite(case: SubqueryCase, rng: random.Random) -> OracleResult:
    """The headline oracle: each predicate form equals its exact join rewrite.

    `EXISTS`/`IN` are a SEMI join, `NOT EXISTS` is an ANTI join, a correlated
    scalar aggregate is a LEFT join to the pre-grouped inner side, and an
    uncorrelated one is a CROSS join to the one-row aggregate. These are not
    approximations — they are the rewrites the engine itself performs, so an
    engine that answers them differently is wrong on one of the two paths.

    `NOT IN` is deliberately absent: it has no unconditional join equivalent.
    See `not_in_null_semantics`.
    """
    if case.form == "not_in":
        raise AssertionError(
            "subquery_matches_join_rewrite must not be offered a NOT IN case; NOT IN is not "
            "an anti join and its oracle is not_in_null_semantics"
        )

    if case.form in ("exists", "in"):
        rewrite, why = case.semi_or_anti("SEMI"), f"{case.form} is a SEMI join"
    elif case.form == "not_exists":
        rewrite, why = case.semi_or_anti("ANTI"), "NOT EXISTS is an ANTI join"
    elif case.form == "corr_scalar":
        rewrite = case.grouped_aggregate_join()
        why = "a correlated scalar aggregate is a LEFT join to the grouped inner side"
    elif case.form == "uncorr_scalar":
        rewrite = case.cross_joined_aggregate()
        why = "an uncorrelated scalar aggregate is a CROSS join to a one-row relation"
    else:
        raise AssertionError(f"no join rewrite defined for form {case.form!r}")

    _require_same(case.sql, rewrite, why)
    return OracleResult("subquery_matches_join_rewrite", 2)


def not_in_null_semantics(case: SubqueryCase, rng: random.Random) -> OracleResult:
    """`NOT IN` has THREE exact answers, and which one applies is measured.

    This is the oracle that pays for the whole file. `NOT IN` looks like an anti
    join and is not one, and an engine that implements it as one is wrong in two
    separate ways that no other oracle here can see:

    * **The inner key contains a NULL.** `x <> NULL` is UNKNOWN, not TRUE, so
      `x NOT IN (…)` can never be TRUE for ANY outer row once a single NULL is
      in the set — the result is EMPTY, however many rows would "not match".
      An anti join happily returns them.
    * **The inner set is empty.** `x NOT IN (<nothing>)` is TRUE for every row,
      including one whose own key is NULL. Nothing is being compared, so the
      NULL never turns anything UNKNOWN.
    * **Otherwise.** Now it IS an anti join — but only after dropping outer rows
      whose key is NULL, because `NULL NOT IN (<non-empty>)` is UNKNOWN and the
      WHERE clause drops it while an anti join keeps it.

    Emptiness and null-ness are measured per case rather than taken from the
    pair, because a generated inner filter changes both.
    """
    if case.form != "not_in":
        raise AssertionError("not_in_null_semantics only applies to a NOT IN case")

    inner = f"({case.inner_relation()}) AS sq_probe"
    inner_rows = scalar(f"SELECT COUNT(*) AS n FROM {inner}")
    inner_nulls = scalar(f"SELECT COUNT(*) AS n FROM {inner} WHERE sq_probe.sq_key IS NULL")

    if inner_rows == 0:
        _require_same(
            case.sql,
            case.without_the_subquery(),
            "NOT IN over an EMPTY set is TRUE for every outer row",
        )
        return OracleResult("not_in_null_semantics", 4)

    if inner_nulls:
        produced = row_count(case.sql)
        if produced != 0:
            raise OracleViolation(
                f"NOT IN over a set containing {inner_nulls} NULL key(s) must return NOTHING — "
                f"a single NULL makes the predicate UNKNOWN for every outer row — but it "
                f"returned {produced} rows.\n  {case.sql}"
            )
        return OracleResult("not_in_null_semantics", 3)

    _require_same(
        case.sql,
        case.anti_join_excluding_null_outer_key(),
        "NOT IN over a non-empty NULL-free set is an ANTI join over non-NULL outer keys",
    )
    return OracleResult("not_in_null_semantics", 4)


def exists_and_in_agree(case: SubqueryCase, rng: random.Random) -> OracleResult:
    """`EXISTS (… WHERE i.k = o.k)` and `o.k IN (SELECT i.k …)` are the same test.

    Both are existential and both are two-valued in the direction that matters:
    a NULL on either side yields UNKNOWN, which WHERE drops, and a match yields
    TRUE. So they agree row for row even when either key is NULL.

    Its value is that it does NOT go through a join spelling. If a decorrelation
    bug corrupted BOTH the subquery path and the join path in the same way —
    which is entirely possible, since they share the SEMI join operator
    downstream — `subquery_matches_join_rewrite` would pass and this would still
    fail, because EXISTS and IN take different routes through
    `DecorrelateSubqueryStrategy` (`_decorrelate_exists` vs `_decorrelate_in`).
    """
    _require_same(
        case.spell(case.exists_predicate(negated=False)),
        case.spell(case.in_predicate(negated=False)),
        "EXISTS and IN over the same correlation are the same existence test",
    )
    return OracleResult("exists_and_in_agree", 2)


def existence_partitions_the_outer_relation(
    case: SubqueryCase, rng: random.Random
) -> OracleResult:
    """|EXISTS| + |NOT EXISTS| == |outer|. Exactly, with nothing left over.

    EXISTS is the one predicate in SQL that is never UNKNOWN: the subquery
    either returns a row or it does not. So unlike every three-valued predicate
    the single-table fuzzer partitions, this needs no third bucket — and that
    makes it a strictly stronger statement. A decorrelation that loses a row,
    duplicates one, or drops the outer rows with a NULL key fails here without
    any join spelling being involved at all.
    """
    if case.distinct:
        raise AssertionError(
            "existence_partitions_the_outer_relation cannot run on a DISTINCT case: "
            "dedup happens inside each half, so the halves do not sum to the whole"
        )
    matched = row_count(case.spell(case.exists_predicate(negated=False)))
    unmatched = row_count(case.spell(case.exists_predicate(negated=True)))
    total = row_count(case.without_the_subquery())
    if matched + unmatched != total:
        raise OracleViolation(
            f"EXISTS and NOT EXISTS do not partition the outer relation: "
            f"{matched} + {unmatched} = {matched + unmatched}, but the relation has {total} "
            f"rows\n  {case.spell(case.exists_predicate(negated=False))}"
        )
    return OracleResult("existence_partitions_the_outer_relation", 3)


def count_star_matches_materialised_rows(case: SubqueryCase, rng: random.Random) -> OracleResult:
    """`SELECT COUNT(*) FROM (Q)` must equal the number of rows Q produces.

    Cheap, applies to every form, and covers the one thing the equivalence
    oracles cannot: whether the aggregate path over a decorrelated plan sees the
    same rows the scan path emits. The decorrelated join sits between them.
    """
    materialised = row_count(case.sql)
    counted = scalar(f"SELECT COUNT(*) AS n FROM ({case.sql}) AS sq_count")
    if materialised != counted:
        raise OracleViolation(
            f"COUNT(*) over the query disagrees with the rows it returns: materialised "
            f"{materialised} rows, COUNT(*) said {counted}\n  {case.sql}"
        )
    return OracleResult("count_star_matches_materialised_rows", 2)


# Strategies that are OPTIMIZATIONS — disabling one may change the plan's cost
# but must never change its answer.
#
# `disable_decorrelate_subquery` is deliberately absent; see the module
# docstring. Everything here was measured to still PRODUCE a result for this
# query family, because a flag that makes the query raise turns this oracle into
# a comparison between a result and an error on every case.
_DIFFERENTIAL_STRATEGIES = (
    "disable_correlated_filters",
    "disable_predicate_pushdown",
    "disable_predicate_rewrite",
    "disable_predicate_compaction",
    "disable_predicate_ordering",
    "disable_split_conjunctive_predicates",
    "disable_constant_folding",
    "disable_boolean_simplification",
    "disable_projection_pushdown",
    "disable_join_ordering",
    "disable_join_elimination",
    "disable_join_rewrite",
    "disable_cross_join_filter_pushdown",
    "disable_distinct_pushdown",
    "disable_group_key_reduction",
    "disable_operator_fusion",
    "disable_project_fusion",
    "disable_hash_map_variant",
    "disable_manifest_pruning",
)
for _flag in _DIFFERENTIAL_STRATEGIES:
    if not isinstance(getattr(config.features, _flag, None), bool):
        raise AssertionError(
            f"config.features has no boolean `{_flag}`; the optimizer differential oracle is "
            f"pointing at a flag that no longer exists"
        )


@contextmanager
def _strategy_disabled(flag: str) -> Iterator[None]:
    """Turn one optimizer strategy off for the duration of the block.

    try/finally restores state; it does not hide anything, because an exception
    inside the block still propagates.
    """
    previous = getattr(config.features, flag)
    setattr(config.features, flag, True)
    try:
        yield
    finally:
        setattr(config.features, flag, previous)


def optimizer_strategy_differential(case: SubqueryCase, rng: random.Random) -> OracleResult:
    """The same query must answer the same with any one optimizer strategy off.

    A failure names the exact strategy that changed the result, which is most of
    the debugging already done.
    """
    flag = rng.choice(_DIFFERENTIAL_STRATEGIES)
    optimized = multiset_positional(case.sql)
    with _strategy_disabled(flag):
        unoptimized = multiset_positional(case.sql)
    if optimized != unoptimized:
        raise OracleViolation(
            f"optimizer strategy `{flag}` changes the answer: {len(optimized)} rows with it "
            f"enabled, {len(unoptimized)} with it disabled\n"
            f"  first difference: {_first_difference(optimized, unoptimized)}\n  {case.sql}"
        )
    return OracleResult("optimizer_strategy_differential", 2)


# ─────────────────────────────────────────────────────────────────────────────
# Applicability
# ─────────────────────────────────────────────────────────────────────────────

Oracle = Callable[[SubqueryCase, random.Random], OracleResult]

ALL_ORACLE_NAMES = frozenset(
    {
        "subquery_matches_join_rewrite",
        "not_in_null_semantics",
        "exists_and_in_agree",
        "existence_partitions_the_outer_relation",
        "count_star_matches_materialised_rows",
        "optimizer_strategy_differential",
    }
)


def applicable_oracles(case: SubqueryCase) -> List[Oracle]:
    """Which oracles can legitimately run against this case.

    Every oracle also asserts its own precondition on entry, so a mistake here
    surfaces as an AssertionError rather than as an oracle that quietly stops
    firing.
    """
    oracles: List[Oracle] = [
        count_star_matches_materialised_rows,
        optimizer_strategy_differential,
    ]

    if case.form == "not_in":
        oracles.append(not_in_null_semantics)
    elif case.form == "corr_scalar" and case.aggregate == "COUNT":
        # A correlated COUNT over an empty correlation group must be 0, and the
        # engine decorrelates it with an INNER join, so the outer row is dropped
        # instead. The join rewrite is RIGHT and the engine is WRONG, so this
        # oracle would fail every time on a defect that is already registered.
        # See subquery_known_gaps/correlated-scalar-subquery-drops-unmatched-outer-rows.
        #
        # The exclusion is structural — a query SHAPE, not an error message —
        # and it disappears with the register entry.
        pass
    else:
        oracles.append(subquery_matches_join_rewrite)

    # EXISTS and IN are interchangeable only over the plain correlation these
    # forms share; a scalar case has an aggregate in it and is not an existence
    # test at all.
    if case.form in ("exists", "not_exists", "in", "not_in"):
        oracles.append(exists_and_in_agree)
        if not case.distinct:
            oracles.append(existence_partitions_the_outer_relation)

    return oracles
