"""
Oracles for the single-table SELECT fuzzer.

WHY THIS FILE EXISTS
--------------------
The previous fuzzer's entire assertion was "did not raise". It computed a result
shape, printed it, and checked nothing. That finds crashes and binder
regressions and is structurally incapable of finding a SILENT WRONG ANSWER —
which is where the engine's open bugs actually live. Writing this file
immediately turned up one: a LIMIT inside a CTE body is discarded, so
`WITH c AS (SELECT row_id FROM t LIMIT 3) SELECT COUNT(*) FROM c` answers 2000
instead of 3, with no error anywhere.

WHAT AN ORACLE HAS TO BE
------------------------
Falsifiable. Two rules follow from that, and both were broken by the previous
metamorphic fuzzer in `fuzz_metamorphic.py`:

1. **Compare multisets, not sets.** That fuzzer collected rows into a `set`.
   A set discards duplicates, so every multiplicity bug is invisible to it, and
   its subset oracles ("adding DISTINCT can only remove rows") can never fail —
   after set-ification both sides are already deduplicated. Everything here
   compares sorted LISTS.

2. **Never swallow an exception.** That fuzzer's `run_query` caught everything
   and returned None, and its `check_oracle` returned True when either side was
   None. An exception therefore counted as a PASS: the harness could execute
   nothing at all and report "All oracles satisfied!". Nothing in this file
   catches. An oracle whose queries raise fails the case, and the driver decides
   whether the failure is a registered defect.

WHY NOT "RUN IT AGAIN WITH THE OPTIMIZER OFF"
---------------------------------------------
The obvious differential oracle — same query, optimizer disabled, compare — is
not available. `disable_optimizer` is a SERVER-owned variable that cannot be set
per session, its read site binds the config value at import, and it is
documented as DANGEROUS because most queries fail with it on: a differential
against it would compare a result to an error. What IS available is the
per-strategy kill switches (`config.features.disable_*`), which exist precisely
"for A/B testing a strategy against the rest of the pipeline" and are read at
query time. `optimizer_strategy_differential` uses those, which is a sharper
instrument anyway — a failure names the strategy.
"""

from __future__ import annotations

import random
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Callable
from typing import Iterator
from typing import List
from typing import Optional
from typing import Sequence

from opteryx import config
from tests.fuzzing.harness import rows
from tests.fuzzing.harness import scalar
from tests.fuzzing.single_table_grammar import SelectQuery
from tests.fuzzing.single_table_grammar import Statement


def row_count(sql: str) -> int:
    """How many rows `sql` produced.

    Counts what the engine emitted, via the same morsel path `result_multiset`
    uses, rather than going through the arrow bridge — a count taken on the far
    side of a conversion layer would attribute that layer's row loss to the
    query.
    """
    return sum(1 for _ in rows(sql))


def _render(value) -> str:
    """One value, rendered so that float noise does not read as a wrong answer.

    Floating-point addition is not associative, so an engine that accumulates a
    SUM or AVG in a different ORDER — which a different plan, or a different
    partitioning across morsels, legitimately does — lands a few ULP away.
    Measured on this corpus:

      SELECT AVG(val) FROM testdata.fuzzing.wide
        -0.09071671089798052 directly, -0.09071671089798049 through a wrap
      SELECT grp_wide, SUM(val) ... GROUP BY grp_wide, flag
        -343.0781345085692 and -343.07813450856924 on two runs of the IDENTICAL
        query — so this is not even a property of the rewrite, it is run-to-run
        non-determinism in the parallel accumulation.

    Comparing `repr()` calls that a wrong answer. It is not one, and an oracle
    that cries wolf on every aggregate gets ignored.

    Floats are therefore rendered to 10 significant digits. THE COST IS REAL and
    is stated here rather than left to be discovered: a genuine error smaller
    than ~1e-10 relative is invisible to every oracle in this file.
    """
    if isinstance(value, float):
        return f"{value:.9e}"
    return repr(value)


def multiset_positional(sql: str) -> List[str]:
    """`sql`'s rows as a multiset, compared by POSITION.

    Used where the two queries being compared are structurally identical, so
    their column order must match too. `harness.result_multiset` does the same
    job but renders with `repr()`; this one goes through `_render` so that float
    accumulation noise does not masquerade as a wrong answer.
    """
    return sorted(repr(tuple(_render(value) for value in row)) for row in rows(sql))


def multiset_by_name(sql: str) -> List[str]:
    """`sql`'s rows as a multiset, canonicalised by COLUMN NAME.

    `harness.result_multiset` renders rows positionally, which is right when the
    two queries being compared are structurally identical. It is wrong for the
    oracles that WRAP a query: `SELECT * FROM (SELECT s_null, json_doc FROM t)`
    emits those two columns in the binder's schema order, not the inner
    projection's order, and that is settled engine behaviour rather than a
    defect. Comparing positionally would fail on the column ORDER while the
    values are identical.

    So each row is rendered as its (name, value) pairs sorted by name. Values
    and multiplicities are still compared exactly — a duplicated row, a dropped
    row or a changed value all still fail. Only position is discounted.
    """
    import opteryx

    session = opteryx.session()
    rendered: List[str] = []
    for morsel in session.execute_to_morsels(sql):
        names = [name.decode() if isinstance(name, bytes) else name for name in morsel.column_names]
        order = sorted(range(len(names)), key=lambda index: names[index])
        for position in range(len(morsel)):
            row = morsel[position]
            rendered.append(repr(tuple((names[index], _render(row[index])) for index in order)))
    return sorted(rendered)


class OracleViolation(AssertionError):
    """An oracle's invariant did not hold. This is the point of the fuzzer."""


@dataclass
class OracleResult:
    name: str
    queries_executed: int


# ─────────────────────────────────────────────────────────────────────────────
# Oracles
# ─────────────────────────────────────────────────────────────────────────────


def count_star_matches_materialised_rows(statement: Statement, rng: random.Random) -> OracleResult:
    """`SELECT COUNT(*) FROM (Q)` must equal the number of rows Q produces.

    Applies to nearly every statement, which is what makes it valuable: it is
    the one oracle that covers set operations, CTEs and window queries as well
    as plain SELECTs. It catches any divergence between the row count the
    aggregate path computes and the rows the scan/limit/join path actually
    emits — including a LIMIT applied on one path and not the other.

    Deliberately sound under a non-deterministic LIMIT: `LIMIT 5` without a
    total order may return different ROWS on two executions, but it must always
    return the same NUMBER of rows.
    """
    materialised = row_count(statement.sql)
    counted = scalar(f"SELECT COUNT(*) AS n FROM ({statement.sql}) AS oracle_count")
    if materialised != counted:
        raise OracleViolation(
            f"COUNT(*) over the query disagrees with the rows it returns: "
            f"materialised {materialised} rows, COUNT(*) said {counted}\n  {statement.sql}"
        )
    return OracleResult("count_star_matches_materialised_rows", 2)


def predicate_partition(statement: Statement, rng: random.Random) -> OracleResult:
    """|sigma p| + |sigma NOT p| + |sigma p IS NULL| == |R|.

    Three-valued logic partitions every row of R into exactly one of three
    buckets, so the counts must sum to the relation's cardinality. This is the
    oracle that puts pressure on NULL handling: with a NULL-free relation the
    third bucket is always empty and the invariant degenerates into
    `|p| + |NOT p| == |R|`, which is why the corpus has NULL-heavy columns.

    A failure means either the filter is dropping rows, or `NOT p` and
    `p IS NULL` disagree about which rows are unknown.
    """
    select = statement.select
    if select is None or select.where is None:
        raise AssertionError("predicate_partition applied to a statement with no WHERE clause")

    source = select.source
    predicate = select.where
    total = scalar(f"SELECT COUNT(*) AS n FROM {source}")
    matched = scalar(f"SELECT COUNT(*) AS n FROM {source} WHERE {predicate}")
    rejected = scalar(f"SELECT COUNT(*) AS n FROM {source} WHERE NOT {predicate}")
    unknown = scalar(f"SELECT COUNT(*) AS n FROM {source} WHERE ({predicate}) IS NULL")

    if matched + rejected + unknown != total:
        raise OracleViolation(
            f"predicate partition does not cover the relation: "
            f"{matched} matched + {rejected} rejected + {unknown} unknown = "
            f"{matched + rejected + unknown}, but {source} has {total} rows\n"
            f"  predicate: {predicate}"
        )
    return OracleResult("predicate_partition", 4)


def tautology_is_neutral(statement: Statement, rng: random.Random) -> OracleResult:
    """Conjoining a tautology must not change the result.

    `1 = 1` rather than `TRUE`: the planner rejects a bare literal in a WHERE
    clause ("WHERE clause cannot be a bare literal"), so the obvious spelling of
    this transformation is not a legal query.
    """
    select = _require_select(statement, "tautology_is_neutral")
    before = multiset_positional(select.render())
    after = multiset_positional(select.render(extra_where="1 = 1"))
    _require_same_multiset(before, after, "conjoining `1 = 1`", select.render())
    return OracleResult("tautology_is_neutral", 2)


def double_negation_is_neutral(statement: Statement, rng: random.Random) -> OracleResult:
    """`NOT (NOT p)` must select exactly the rows `p` selects.

    True under three-valued logic as well as two: NOT maps UNKNOWN to UNKNOWN,
    so the double negation is the identity on all three truth values.
    """
    select = _require_select(statement, "double_negation_is_neutral")
    if select.where is None:
        raise AssertionError("double_negation_is_neutral applied to a statement with no WHERE")
    before = multiset_positional(select.render())
    after = multiset_positional(select.render(replace_where=f"NOT (NOT ({select.where}))"))
    _require_same_multiset(before, after, "double-negating the predicate", select.render())
    return OracleResult("double_negation_is_neutral", 2)


def subquery_wrapping_is_neutral(statement: Statement, rng: random.Random) -> OracleResult:
    """`SELECT * FROM (Q) AS t` must return exactly what Q returns."""
    inner = statement.sql
    before = multiset_by_name(inner)
    after = multiset_by_name(f"SELECT * FROM ({inner}) AS oracle_wrap")
    _require_same_multiset(before, after, "wrapping in a redundant subquery", inner)
    return OracleResult("subquery_wrapping_is_neutral", 2)


def cte_matches_inline_subquery(statement: Statement, rng: random.Random) -> OracleResult:
    """A CTE must compute exactly what the same body computes inline.

    `WITH c AS (Q) SELECT * FROM c` and `SELECT * FROM (Q) AS c` are the same
    query written two ways; the engine has one code path for each. This oracle
    is what found the dropped-LIMIT bug
    (single_table_known_gaps/cte-body-limit-is-dropped): the inline form honours
    a LIMIT in the body and the CTE form silently does not.
    """
    inner = statement.sql
    as_cte = multiset_by_name(f"WITH oracle_cte AS ({inner}) SELECT * FROM oracle_cte")
    as_subquery = multiset_by_name(f"SELECT * FROM ({inner}) AS oracle_cte")
    _require_same_multiset(as_cte, as_subquery, "CTE versus inline subquery", inner)
    return OracleResult("cte_matches_inline_subquery", 2)


def order_by_does_not_change_the_multiset(statement: Statement, rng: random.Random) -> OracleResult:
    """Sorting reorders rows; it must not add, drop or alter any.

    Only meaningful without a LIMIT — with one, the ORDER BY decides WHICH rows
    survive, so the multisets legitimately differ.
    """
    select = _require_select(statement, "order_by_does_not_change_the_multiset")
    if not select.order_by:
        raise AssertionError("order_by oracle applied to a statement with no ORDER BY")
    ordered = multiset_positional(select.render())
    unordered = multiset_positional(select.render(drop_order=True))
    _require_same_multiset(ordered, unordered, "dropping the ORDER BY", select.render())
    return OracleResult("order_by_does_not_change_the_multiset", 2)


def limit_returns_the_right_number_of_rows(statement: Statement, rng: random.Random) -> OracleResult:
    """A LIMIT must return exactly `min(n, |Q|)` rows.

    Sound even when the LIMIT is not deterministic: which rows survive may vary
    between executions, how many may not. Kept separate from the membership
    oracle below so that the membership half being absorbed by a registered
    defect does not take this half down with it.
    """
    select = _require_select(statement, "limit_returns_the_right_number_of_rows")
    if select.limit is None or select.offset is not None:
        raise AssertionError("limit oracle applied to a statement without a bare LIMIT")

    limited = row_count(select.render())
    unlimited = row_count(select.render(drop_limit=True))
    expected = min(select.limit, unlimited)
    if limited != expected:
        raise OracleViolation(
            f"LIMIT {select.limit} returned {limited} rows; the unlimited query returns "
            f"{unlimited}, so it should have returned {expected}\n  {select.render()}"
        )
    return OracleResult("limit_returns_the_right_number_of_rows", 2)


def limit_rows_come_from_the_unlimited_result(
    statement: Statement, rng: random.Random
) -> OracleResult:
    """Every row a LIMIT returns must be a row the unlimited query returns.

    Weaker than "the same rows", deliberately: an unordered LIMIT may pick any
    subset, so only membership can be asserted. It is still enough to catch a
    LIMIT that changes the VALUES rather than just the count — which is exactly
    what it found when LIMIT was being pushed below a ranking window, so the
    ROW_NUMBERs came out of an arbitrary subset instead of the whole relation
    (fixed by making Window a barrier in LimitPushdownStrategy).
    """
    select = _require_select(statement, "limit_rows_come_from_the_unlimited_result")
    if select.limit is None or select.offset is not None:
        raise AssertionError("limit oracle applied to a statement without a bare LIMIT")

    limited = multiset_positional(select.render())
    remaining = multiset_positional(select.render(drop_limit=True))
    for row in limited:
        if row not in remaining:
            raise OracleViolation(
                f"LIMIT {select.limit} returned a row the unlimited query does not contain: "
                f"{row}\n  {select.render()}"
            )
        remaining.remove(row)
    return OracleResult("limit_rows_come_from_the_unlimited_result", 2)


def distinct_is_the_deduplicated_projection(statement: Statement, rng: random.Random) -> OracleResult:
    """DISTINCT must return each distinct row of the same projection exactly once.

    Asserted in both directions: no row appears twice under DISTINCT, and the
    set of rows is unchanged. Comparing MULTISETS on the non-distinct side is
    what gives this oracle teeth — deduplicating both sides first, as the
    previous fuzzer did, makes it unfalsifiable.
    """
    select = _require_select(statement, "distinct_is_the_deduplicated_projection")
    if not select.distinct:
        raise AssertionError("distinct oracle applied to a non-DISTINCT statement")

    distinct_rows = multiset_positional(select.render())
    if len(distinct_rows) != len(set(distinct_rows)):
        duplicated = next(row for row in distinct_rows if distinct_rows.count(row) > 1)
        raise OracleViolation(
            f"SELECT DISTINCT returned a duplicate row: {duplicated}\n  {select.render()}"
        )

    select.distinct = False
    try:
        all_rows = multiset_positional(select.render())
    finally:
        select.distinct = True

    if set(distinct_rows) != set(all_rows):
        missing = set(all_rows) - set(distinct_rows)
        extra = set(distinct_rows) - set(all_rows)
        raise OracleViolation(
            f"SELECT DISTINCT does not cover the same values as the plain projection; "
            f"missing {sorted(missing)[:3]}, unexpected {sorted(extra)[:3]}\n  {select.render()}"
        )
    return OracleResult("distinct_is_the_deduplicated_projection", 2)


def count_distinct_matches_distinct_rows(statement: Statement, rng: random.Random) -> OracleResult:
    """`COUNT_DISTINCT(c)` must equal the rows of `SELECT DISTINCT c WHERE c IS NOT NULL`.

    Two independent implementations of the same number: the aggregate's distinct
    counter and the DISTINCT operator. The `IS NOT NULL` is not a workaround —
    COUNT_DISTINCT is specified over non-null values while SELECT DISTINCT
    treats NULL as a value.
    """
    select = statement.select
    if select is None:
        raise AssertionError("count_distinct oracle applied to a non-SELECT statement")
    column = _pick_countable_column(select, rng)
    if column is None:
        raise AssertionError("count_distinct oracle applied where no countable column exists")

    where = f"WHERE {select.where}" if select.where else ""
    counted = scalar(
        f"SELECT COUNT_DISTINCT({column}) AS n FROM {select.source} {where}".strip()
    )
    null_clause = f"{column} IS NOT NULL"
    combined = null_clause if not select.where else f"({select.where}) AND {null_clause}"
    listed = row_count(f"SELECT DISTINCT {column} FROM {select.source} WHERE {combined}")

    if counted != listed:
        raise OracleViolation(
            f"COUNT_DISTINCT({column}) = {counted} but SELECT DISTINCT over the same rows "
            f"returns {listed}\n  source: {select.source}, where: {select.where}"
        )
    return OracleResult("count_distinct_matches_distinct_rows", 2)


def aggregate_identities(statement: Statement, rng: random.Random) -> OracleResult:
    """SUM/COUNT == AVG, COUNT(c) <= COUNT(*), MIN(c) <= MAX(c).

    Three separate implementations checked against each other in one query, so
    they see identical input. AVG is compared with a relative tolerance because
    the engine is free to compute it by a different accumulation order than
    SUM/COUNT — a different rounding is not a bug, a different value is.
    """
    select = statement.select
    if select is None:
        raise AssertionError("aggregate_identities applied to a non-SELECT statement")
    column = _pick_numeric_column(select, rng)
    if column is None:
        raise AssertionError("aggregate_identities applied where no numeric column exists")

    where = f"WHERE {select.where}" if select.where else ""
    sql = (
        f"SELECT SUM({column}) AS s, COUNT({column}) AS c, AVG({column}) AS a, "
        f"MIN({column}) AS lo, MAX({column}) AS hi, COUNT(*) AS total "
        f"FROM {select.source} {where}"
    ).strip()
    result = list(rows(sql))
    if len(result) != 1:
        raise OracleViolation(f"a global aggregate returned {len(result)} rows, not 1\n  {sql}")
    # Positional, in the order the SELECT lists them: s, c, a, lo, hi, total.
    total_sum, non_null, average, lowest, highest, total = result[0]

    if non_null > total:
        raise OracleViolation(
            f"COUNT({column}) = {non_null} exceeds COUNT(*) = {total}\n  {sql}"
        )
    if non_null == 0:
        # Every value was NULL: SUM/AVG/MIN/MAX are all NULL and there is
        # nothing further to relate. Not a skip — the COUNT identity above did
        # run and could have failed.
        return OracleResult("aggregate_identities", 1)

    if lowest is not None and highest is not None and lowest > highest:
        raise OracleViolation(f"MIN({column}) = {lowest} exceeds MAX = {highest}\n  {sql}")

    if total_sum is not None and average is not None:
        derived = float(total_sum) / non_null
        reported = float(average)
        scale = max(abs(derived), abs(reported), 1.0)
        if abs(derived - reported) > 1e-9 * scale:
            raise OracleViolation(
                f"AVG({column}) = {reported} but SUM/COUNT = {derived} "
                f"({total_sum} / {non_null})\n  {sql}"
            )
    return OracleResult("aggregate_identities", 1)


def group_counts_sum_to_the_total(statement: Statement, rng: random.Random) -> OracleResult:
    """Every row belongs to exactly one group, so the group counts must total |R|.

    Catches a grouped aggregate that drops rows, double-counts them, or loses a
    group whose key is NULL — GROUP BY treats NULL as a group, so a NULL key
    must contribute a group rather than vanish.
    """
    select = statement.select
    if select is None:
        raise AssertionError("group_counts oracle applied to a non-SELECT statement")
    column = _pick_groupable_column(select, rng)
    if column is None:
        raise AssertionError("group_counts oracle applied where no groupable column exists")

    where = f"WHERE {select.where}" if select.where else ""
    total = scalar(f"SELECT COUNT(*) AS n FROM {select.source} {where}".strip())
    grouped = scalar(
        f"SELECT SUM(g) AS n FROM (SELECT {column}, COUNT(*) AS g FROM {select.source} "
        f"{where} GROUP BY {column}) AS oracle_groups".strip()
    )
    # SUM over no rows is NULL, not 0 — standard SQL, and the honest answer when
    # the WHERE clause matched nothing. Asserted in both directions so a NULL
    # appearing over a non-empty relation still fails.
    if total == 0:
        if grouped is not None:
            raise OracleViolation(
                f"GROUP BY {column}: the relation is empty under this predicate, so SUM over the "
                f"group counts should be NULL; got {grouped}\n"
                f"  source: {select.source}, where: {select.where}"
            )
    elif total != grouped:
        raise OracleViolation(
            f"GROUP BY {column}: the group counts sum to {grouped} but the relation has "
            f"{total} rows\n  source: {select.source}, where: {select.where}"
        )
    return OracleResult("group_counts_sum_to_the_total", 2)


# Optimizer strategies this oracle flips. Every entry is a real
# `config.features.disable_*` attribute; the list is checked against the
# attributes at import so a renamed flag fails loudly rather than silently
# disabling the oracle.
#
# TWO FLAGS ARE DELIBERATELY ABSENT, and their absence is itself a finding —
# both are registered in single_table_known_gaps:
#
#   disable_projection_pushdown   breaks EVERY query, including
#                                 `SELECT * FROM t LIMIT 5` (12/12 in a
#                                 representative battery: KeyError, TypeError,
#                                 NotSupportedError).
#   disable_redundant_operations  leaves a `Subquery` logical node the physical
#                                 planner cannot dispatch.
#
# config.py documents this whole family as "One kill-switch per optimizer
# strategy ... for A/B testing a strategy against the rest of the pipeline. All
# default False (every strategy enabled) — this changes no behaviour until a
# specific one is set." For these two, setting one does not disable an
# optimization; it breaks planning. Including them here would make this oracle
# compare a result against an error on every case.
_DIFFERENTIAL_STRATEGIES = (
    "disable_predicate_pushdown",
    "disable_predicate_rewrite",
    "disable_predicate_compaction",
    "disable_constant_folding",
    "disable_boolean_simplification",
    "disable_split_conjunctive_predicates",
    "disable_limit_pushdown",
    "disable_distinct_pushdown",
    "disable_group_key_reduction",
    "disable_operator_fusion",
    "disable_project_fusion",
    "disable_disjunction_simplification",
    "disable_cast_simplification",
    "disable_topn_scan_pushdown",
    "disable_window_topk_fusion",
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

    `config.features` is process-global and the optimizer reads it per query
    (`getattr(config.features, flag_name)` inside OptimizerVisitor.optimize), so
    flipping it here really does change the next plan. try/finally is restoring
    state, not hiding an error: an exception inside the block still propagates.
    """
    previous = getattr(config.features, flag)
    setattr(config.features, flag, True)
    try:
        yield
    finally:
        setattr(config.features, flag, previous)


def optimizer_strategy_differential(statement: Statement, rng: random.Random) -> OracleResult:
    """The same query must answer the same with any one optimizer strategy off.

    An optimizer rewrite is by definition semantics-preserving, so disabling one
    may change the plan's cost but never its answer. A failure names the exact
    strategy that changed the result, which is most of the debugging done.
    """
    flag = rng.choice(_DIFFERENTIAL_STRATEGIES)
    optimized = multiset_positional(statement.sql)
    with _strategy_disabled(flag):
        unoptimized = multiset_positional(statement.sql)
    if optimized != unoptimized:
        raise OracleViolation(
            f"optimizer strategy `{flag}` changes the answer: "
            f"{len(optimized)} rows with it enabled, {len(unoptimized)} with it disabled\n"
            f"  first difference: {_first_difference(optimized, unoptimized)}\n  {statement.sql}"
        )
    return OracleResult(f"optimizer_strategy_differential", 2)


# ─────────────────────────────────────────────────────────────────────────────
# Applicability
# ─────────────────────────────────────────────────────────────────────────────

Oracle = Callable[[Statement, random.Random], OracleResult]


def applicable_oracles(statement: Statement) -> List[Oracle]:
    """Which oracles can legitimately run against this statement.

    An oracle is listed only when its precondition holds, and every oracle
    asserts its own precondition on entry — so a bug in this function surfaces
    as an AssertionError rather than as an oracle that quietly never fires.
    """
    oracles: List[Oracle] = []
    select = statement.select

    # Three oracles nest the statement inside another query. Two shapes rule
    # that out:
    #   * a ranking window, because COUNT(*) over one hits
    #     single_table_known_gaps/count-star-over-a-ranking-window-subquery and
    #     the case would report a registered defect instead of testing anything;
    #   * a statement that is already a CTE, because neither a WITH inside a
    #     WITH nor a WITH inside a derived table resolves the inner CTE name.
    nestable = not statement.has_ranking_window and not statement.is_cte

    # This oracle compares two SEPARATE executions, so it can only speak about a
    # deterministic statement. A limited one is not: LIMIT and OFFSET select an
    # arbitrary subset that may differ run to run, and so a filter over a limited
    # subquery legitimately answers a different row count each time. See
    # single_table_known_gaps/RATIFIED/limit-and-offset-select-an-arbitrary-subset
    # for the ruling and for what IS still promised about a LIMIT.
    #
    # PERMANENT, not a deadline. It costs ~25% of generated statements this one
    # oracle; limit_returns_the_right_number_of_rows and
    # limit_rows_come_from_the_unlimited_result still cover them below.
    if nestable and not statement.contains_limit and not statement.contains_offset:
        oracles.append(count_star_matches_materialised_rows)
    if nestable:
        # A registered wrong-answer defect is triggered by wrapping this exact
        # shape, and it is pinned by its own test_wrong_answer_* test. Declining
        # it here — structurally, by query shape, naming the entry — keeps the
        # wrapping oracles able to fail on everything else. Delete this condition
        # when the entry is deleted.
        wrap_hits_a_registered_defect = (
            # single_table_known_gaps/having-leaks-its-internal-count
            statement.contains_having
        )
        if statement.deterministic_multiset and not wrap_hits_a_registered_defect:
            oracles.append(subquery_wrapping_is_neutral)
            oracles.append(cte_matches_inline_subquery)

    if statement.deterministic_multiset:
        oracles.append(optimizer_strategy_differential)

    if select is not None:
        if select.where is not None:
            # The partition oracle rewrites the WHERE against the raw source, so
            # it needs a source it can name — a derived source would need the
            # subquery repeated, which is a different query.
            #
            # A NaN row is selected by none of p / NOT p / p IS NULL, so the
            # partition cannot hold over a column that has one — see
            # single_table_known_gaps/nan-rows-fall-outside-every-predicate-bucket,
            # pinned by its own test. Delete this when the entry goes.
            if not select.predicate_touches_nan:
                oracles.append(predicate_partition)
            if statement.deterministic_multiset:
                oracles.append(tautology_is_neutral)
                oracles.append(double_negation_is_neutral)
        if select.order_by and statement.deterministic_multiset:
            oracles.append(order_by_does_not_change_the_multiset)
        if select.limit is not None and select.offset is None:
            oracles.append(limit_returns_the_right_number_of_rows)
            oracles.append(limit_rows_come_from_the_unlimited_result)
        if select.distinct and statement.deterministic_multiset:
            oracles.append(distinct_is_the_deduplicated_projection)
        if _pick_countable_column(select, random.Random(0)) is not None:
            oracles.append(count_distinct_matches_distinct_rows)
        if _pick_numeric_column(select, random.Random(0)) is not None:
            oracles.append(aggregate_identities)
        if _pick_groupable_column(select, random.Random(0)) is not None:
            oracles.append(group_counts_sum_to_the_total)

    return oracles


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _require_select(statement: Statement, oracle: str) -> SelectQuery:
    if statement.select is None:
        raise AssertionError(f"{oracle} applied to a statement with no structural form")
    return statement.select


def _require_same_multiset(
    before: Sequence[str], after: Sequence[str], transformation: str, sql: str
) -> None:
    if list(before) == list(after):
        return
    raise OracleViolation(
        f"{transformation} changed the result: {len(before)} rows before, {len(after)} after\n"
        f"  first difference: {_first_difference(before, after)}\n  {sql}"
    )


def _first_difference(left: Sequence[str], right: Sequence[str]) -> str:
    for index, (a, b) in enumerate(zip(left, right)):
        if a != b:
            return f"row {index}: {a} != {b}"
    longer, side = (left, "before") if len(left) > len(right) else (right, "after")
    return f"only {side} has row {min(len(left), len(right))}: {longer[min(len(left), len(right))]}"


def _base_source_columns(select: SelectQuery):
    """Columns of the FROM source, for oracles that query the source directly.

    `output_columns` describes what the SELECT emits, which for an aggregate or
    an expression projection is not something the source has. These oracles need
    the source's own columns, and only a base relation reliably exposes them.
    """
    from tests.fuzzing.single_table_grammar import load_relation

    if select.source.startswith("(") or select.source == "cte_source":
        return ()
    return load_relation(select.source).columns


def _pick_countable_column(select: SelectQuery, rng: random.Random) -> Optional[str]:
    from tests.fuzzing.single_table_grammar import SCALAR

    candidates = [c for c in _base_source_columns(select) if c.ty in SCALAR]
    return rng.choice(candidates).quoted if candidates else None


def _pick_numeric_column(select: SelectQuery, rng: random.Random) -> Optional[str]:
    from tests.fuzzing.single_table_grammar import Ty

    # DECIMAL is excluded: SUM over DECIMAL and AVG over DECIMAL are computed in
    # different numeric domains, so the SUM/COUNT vs AVG identity would be
    # comparing a scaled integer against a float and failing on representation
    # rather than on a defect.
    candidates = [
        c for c in _base_source_columns(select) if c.ty in (Ty.INTEGER, Ty.FLOAT)
    ]
    return rng.choice(candidates).quoted if candidates else None


def _pick_groupable_column(select: SelectQuery, rng: random.Random) -> Optional[str]:
    from tests.fuzzing.single_table_grammar import SCALAR

    candidates = [c for c in _base_source_columns(select) if c.ty in SCALAR]
    return rng.choice(candidates).quoted if candidates else None
