"""
Single-table SELECT fuzzer.

A query engine that cannot reliably SELECT from one table is not a query engine,
so this is the fuzzer that matters most. It generates one statement per case from
`reference/`, executes it, and runs every oracle whose precondition the statement
satisfies.

WHAT CHANGED, AND WHY
---------------------
* **Seeds are random.** The previous version seeded from the pytest parametrize
  index (`random.seed(i)` for i in 0..999), so every run generated the same 974
  statements forever — a fixed regression suite wearing a fuzzer's name, able to
  find a bug only on the first run after the change that introduced it. Seeds now
  come from `harness.case_seeds`: pinned regressions first, then random ones.
  `TEST_SEED` reproduces a run; the derived seed is printed for every case, and
  is the only handle a failure gives you.

* **`TEST_ITERATIONS` is honoured.** `.github/workflows/fuzzer.yaml` sets it to
  100,000 for the nightly run. The old `TEST_CYCLES = 1000` ignored it entirely,
  so the nightly job ran exactly the same 1,000 cases as CI.

* **The oracles can fail.** The old assertion was "did not raise": it computed a
  shape, printed it, and checked nothing, which is structurally blind to a silent
  wrong answer. See `single_table_oracles.py`.

* **A run that executes nothing says so.** `test_zz_fuzzing_actually_ran` fails
  if no query executed, and fails if an oracle never fired across a run large
  enough that it should have. An oracle that has quietly become unreachable is
  indistinguishable from a passing one unless something checks.

REPRODUCING A FAILURE
---------------------
    TEST_SEED=<seed from the output> python -m pytest \\
        tests/fuzzing/test_sql_fuzzer_single_table_select.py

and add the seed to `seeds/single_table_select.txt` so it runs on every future
invocation.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import random
from collections import Counter

import pytest

from tests.fuzzing import single_table_known_gaps as known_gaps
from tests.fuzzing import single_table_oracles as oracles
from tests.fuzzing.harness import case_seeds
from tests.fuzzing.harness import rows
from tests.fuzzing.single_table_grammar import EXCLUSIONS
from tests.fuzzing.single_table_grammar import choose_relation
from tests.fuzzing.single_table_grammar import generate

# Cases per run when TEST_ITERATIONS is unset. Small enough that the fuzzing
# suite stays usable as a pre-commit check; the nightly job overrides it.
DEFAULT_ITERATIONS = 200

# How many oracles to run per case. Every applicable oracle every time would
# multiply the query count per case by ~8; a random sample keeps each case cheap
# while the run as a whole still covers every oracle many times over.
ORACLES_PER_CASE = 2

# Below this many cases, "an oracle never fired" is expected rather than
# suspicious — with 20 cases and a random oracle sample, several will not come
# up. Above it, an oracle that never fires has become unreachable.
_COVERAGE_FLOOR = 150

SEEDS = case_seeds("single_table_select", int(os.environ.get("TEST_ITERATIONS", DEFAULT_ITERATIONS)))


class _RunLedger:
    """What this run actually did.

    Collected at module scope because pytest runs each parametrized case
    independently and nothing else would notice that, say, every case errored
    out in generation and no query was ever executed.
    """

    def __init__(self) -> None:
        self.cases = 0
        self.statements_executed = 0
        self.oracle_runs: Counter = Counter()
        self.oracle_queries = 0
        self.known_gap_hits: Counter = Counter()
        self.constructs: Counter = Counter()

    def report(self) -> str:
        lines = [
            "",
            "═══ single-table SELECT fuzzer ═══",
            f"cases:                {self.cases}",
            f"statements executed:  {self.statements_executed}",
            f"oracle invocations:   {sum(self.oracle_runs.values())} "
            f"({self.oracle_queries} extra queries)",
            "",
            "oracle invocations by kind:",
        ]
        for name in sorted(_ALL_ORACLE_NAMES):
            lines.append(f"  {self.oracle_runs.get(name, 0):6d}  {name}")
        if self.known_gap_hits:
            lines += ["", "registered defects hit (see single_table_known_gaps.py):"]
            for gap_id, count in self.known_gap_hits.most_common():
                lines.append(f"  {count:6d}  {gap_id}")
        lines += ["", f"distinct SQL constructs generated: {len(self.constructs)}"]
        return "\n".join(lines)


_ALL_ORACLE_NAMES = {
    "count_star_matches_materialised_rows",
    "predicate_partition",
    "tautology_is_neutral",
    "double_negation_is_neutral",
    "subquery_wrapping_is_neutral",
    "cte_matches_inline_subquery",
    "order_by_does_not_change_the_multiset",
    "limit_returns_the_right_number_of_rows",
    "limit_rows_come_from_the_unlimited_result",
    "distinct_is_the_deduplicated_projection",
    "count_distinct_matches_distinct_rows",
    "aggregate_identities",
    "group_counts_sum_to_the_total",
    "optimizer_strategy_differential",
}

LEDGER = _RunLedger()


@pytest.mark.parametrize("seed", SEEDS)
def test_sql_fuzzing_single_table(seed):
    """One generated statement, executed, then checked by its applicable oracles."""
    # Printed unconditionally and first: when this case dies, the seed is what
    # reproduces it, and a seed printed only on the success path is no use.
    print(f"\nSeed: {seed}")

    rng = random.Random(seed)
    relation = choose_relation(rng)
    statement = generate(rng, relation)
    print(statement.sql)

    LEDGER.cases += 1
    LEDGER.constructs.update(statement.tags)

    # Everything this generator emits is intended to run, so a raise is a
    # finding: either an engine defect or a `reference/` inaccuracy. The only
    # thing this except does is look the failure up in the defect register —
    # `_classify` RE-RAISES anything the register does not already account for,
    # so nothing is swallowed here.
    try:
        row_count = _execute_and_read_every_value(statement.sql)
    except Exception as error:  # noqa: BLE001 - _classify re-raises unless registered
        _classify(error)
        return
    print(f"Rows: {row_count}")
    LEDGER.statements_executed += 1

    available = oracles.applicable_oracles(statement)
    if not available:
        return
    chosen = rng.sample(available, min(ORACLES_PER_CASE, len(available)))
    for oracle in chosen:
        try:
            result = oracle(statement, rng)
        except oracles.OracleViolation:
            # Always fatal. There is deliberately no path that silences a wrong
            # answer: known wrong answers are kept out by a structural exclusion
            # in applicable_oracles(), not by matching the violation's text.
            raise
        except Exception as error:  # noqa: BLE001 - _classify re-raises unless registered
            _classify(error)
            continue
        LEDGER.oracle_runs[result.name] += 1
        LEDGER.oracle_queries += result.queries_executed
        print(f"  oracle ok: {result.name}")


def _execute_and_read_every_value(sql: str) -> int:
    """Execute `sql`, touch every value, and return the row count.

    Reading the values matters. A shape-only execution counts rows without
    decoding them, so `CAST(<binary> AS VARCHAR)` and `REVERSE` over multi-byte
    UTF-8 both look fine — the corruption only surfaces when something reads the
    string. "Did not raise" has to mean the whole result was producible.
    """
    produced = 0
    for row in rows(sql):
        produced += 1
        for value in row:
            repr(value)
    return produced


def _classify(error: Exception) -> None:
    """Re-raise, unless this exact failure is already a registered defect."""
    defect = known_gaps.match(error)
    if defect is None:
        raise error
    LEDGER.known_gap_hits[defect.id] += 1
    print(f"  known defect: {defect.id} ({type(error).__name__})")


@pytest.mark.parametrize("defect", known_gaps.REGISTER, ids=lambda d: d.id)
def test_registered_defect_still_reproduces(defect):
    """Every registered defect must still be broken.

    This is what stops the register from becoming a place bugs go to be
    forgotten. When a defect is fixed this test goes red, and the only way to
    make it green is to delete the entry — which puts the construct back into
    ordinary fuzzing.
    """
    from tests.fuzzing.harness import rows

    def drain(sql):
        """Execute and READ every value.

        Counting rows is not enough: `CAST(<binary> AS VARCHAR)` only fails when
        the resulting string is decoded, so a repro checked by shape alone would
        look fixed while still being broken.
        """
        for row in rows(sql):
            for value in row:
                repr(value)

    if defect.error_type == "WrongAnswer":
        # A wrong-answer entry has no exception to match; each is pinned by its
        # own test_wrong_answer_* test. Assert only that the repro still
        # executes, so one that has rotted into a syntax error is caught.
        drain(defect.repro)
        return

    with pytest.raises(Exception) as raised:  # noqa: PT011 - the type IS the assertion
        drain(defect.repro)

    actual = type(raised.value).__name__
    assert actual == defect.error_type, (
        f"registered defect `{defect.id}` now raises {actual}, not {defect.error_type}. "
        f"If it is fixed, delete the register entry.\n  {defect.repro}\n  {raised.value}"
    )
    assert defect.signature in str(raised.value), (
        f"registered defect `{defect.id}` no longer matches its signature "
        f"{defect.signature!r}; the register would stop absorbing it.\n  {raised.value}"
    )


def test_cte_body_limit_is_honoured():
    """A LIMIT in a CTE body must survive planning — it used to be discarded.

    `extract_ctes` planned only the CTE's `body`, and ORDER BY / LIMIT / OFFSET are
    siblings of `body` in the AST that `plan_query` hoists into it. So the CTE form
    returned the whole relation while the identical inline subquery returned 3.
    """
    from tests.fuzzing.harness import scalar

    inner = "SELECT row_id FROM testdata.fuzzing.mixed LIMIT 3"
    assert scalar(f"SELECT COUNT(*) AS n FROM ({inner}) AS s") == 3
    assert scalar(f"WITH c AS ({inner}) SELECT COUNT(*) AS n FROM c") == 3


def test_cte_body_order_by_and_offset_are_honoured():
    """ORDER BY and OFFSET rode on the same dropped AST siblings as LIMIT."""
    from tests.fuzzing.harness import rows

    inner = "SELECT row_id FROM testdata.fuzzing.mixed ORDER BY row_id DESC LIMIT 2 OFFSET 1"
    via_cte = list(rows(f"WITH c AS ({inner}) SELECT row_id FROM c ORDER BY row_id"))
    via_subquery = list(rows(f"SELECT row_id FROM ({inner}) AS s ORDER BY row_id"))

    assert via_subquery == [(1997,), (1998,)], via_subquery
    assert via_cte == via_subquery, via_cte


@pytest.mark.parametrize("entry", known_gaps.RATIFIED, ids=lambda e: e.id)
def test_ratified_semantic_is_named_in_code(entry):
    """A ruling must be CITED by the code that stands down because of it.

    `RATIFIED` has no "still reproduces" gate — a ruling has nothing to fix, so
    there is no failure to require. This is what stands in its place: an entry
    nothing acts on is not a ruling, it is a note, and a note in this file reads
    to the next person as a silenced defect. Requiring the citation also means an
    exclusion can never outlive the ruling it claims to rest on, because deleting
    the entry turns every citation into a dangling name.
    """
    import pathlib

    here = pathlib.Path(__file__).parent
    citing = [
        path.name
        for path in sorted(here.glob("*.py"))
        if path.name != "single_table_known_gaps.py" and entry.id in path.read_text()
    ]
    assert citing, (
        f"ratified semantic `{entry.id}` is named nowhere outside the register. "
        f"Either some oracle/generator stands down because of it and must say so by "
        f"name, or nothing does and the entry should be deleted.\n{entry.detail}"
    )


def test_limited_statements_keep_the_guarantees_that_survive_ratification():
    """The ratified arbitrariness is bounded — these two properties still hold.

    Ruling `limit-and-offset-select-an-arbitrary-subset` says WHICH rows a LIMIT
    returns is arbitrary. It does not say HOW MANY, and it does not license
    inventing rows. Asserted here directly because the ratification retired the
    oracle that used to exercise this shape, and "arbitrary" must not be allowed
    to quietly widen into "unchecked".
    """
    from tests.fuzzing.harness import rows
    from tests.fuzzing.harness import scalar

    total = scalar("SELECT COUNT(*) AS n FROM testdata.fuzzing.wide")
    universe = {row[0] for row in rows("SELECT row_id FROM testdata.fuzzing.wide")}

    for offset in (0, 6):
        query = f"SELECT row_id FROM testdata.fuzzing.wide LIMIT 20 OFFSET {offset}"
        for _ in range(4):
            got = [row[0] for row in rows(query)]
            assert len(got) == 20, f"{query} returned {len(got)} rows, not 20"
            assert len(set(got)) == 20, f"{query} returned a duplicate row"
            assert set(got) <= universe, f"{query} returned a row not in the relation"

    # OFFSET past the end yields nothing — not an arbitrary tail.
    assert list(rows(f"SELECT row_id FROM testdata.fuzzing.wide OFFSET {total}")) == []


def test_wrong_answer_not_over_boolean_tautology_still_overlaps():
    """Pins single_table_known_gaps/not-over-a-boolean-tautology-disjunction-...

    Asserts the OVERLAP. `p` and `NOT p` cannot both select a row; when they stop
    doing so this goes red and both this test and the register entry go.
    """
    from tests.fuzzing.harness import scalar

    predicate = (
        "((b_value = TRUE) OR NOT (b_value = TRUE)) OR NOT (b_value = b_null)"
    )

    def matching(where):
        return scalar(f"SELECT COUNT(*) AS n FROM testdata.fuzzing.mixed WHERE {where}")

    total = matching("row_id IS NOT NULL")
    assert matching(predicate) == total, (
        "the un-negated predicate no longer matches every row; re-derive the expectation before "
        "trusting this test"
    )
    assert matching(f"NOT ({predicate})") != 0, (
        "`p` and `NOT p` no longer overlap — the defect is FIXED. Delete this test and the "
        "`not-over-a-boolean-tautology-disjunction-overlaps-itself` register entry."
    )


def test_wrong_answer_nan_row_still_falls_outside_every_bucket():
    """Pins single_table_known_gaps/nan-rows-fall-outside-every-predicate-bucket.

    Asserts that the NaN row is in NO bucket. Whichever semantics the architect
    settles on, the fix makes the three counts sum to 177 and this goes red —
    along with the register entry and the `predicate_touches_nan` exclusion in
    `applicable_oracles`.
    """
    from tests.fuzzing.harness import scalar

    def matching(where):
        return scalar(f"SELECT COUNT(*) AS n FROM testdata.satellites WHERE {where}")

    total = scalar("SELECT COUNT(*) AS n FROM testdata.satellites")
    assert total == 177, f"testdata.satellites has {total} rows, not 177; the corpus has changed"
    assert matching("density IS NULL") == 0, "satellites.density has acquired a NULL"

    predicate = "density > 72971.564572"
    buckets = (
        matching(predicate)
        + matching(f"NOT ({predicate})")
        + matching(f"({predicate}) IS NULL")
    )
    assert buckets == 176, (
        f"the three predicate buckets now cover {buckets} of 177 rows. If that is 177 the defect "
        f"is FIXED — delete this test, the register entry, and the predicate_touches_nan "
        f"exclusion in applicable_oracles()."
    )


def test_is_null_over_a_mixed_type_comparison_reports_unknown():
    """Regression for the fixed is-null-over-a-mixed-type-comparison defect.

    draken_numeric_cmp used to return validity=nullptr, so a null operand gave a
    cleared data bit on a row still marked VALID: UNKNOWN read back as a definite
    FALSE and `(f_null > 0) IS NULL` answered 0. The WHERE filter drops the row
    either way, which is why only the UNKNOWN bucket was lost.

    Each case is checked as a three-way partition rather than against the bare
    UNKNOWN count, so a fix that over-counts UNKNOWN by stealing from the matched
    or rejected bucket fails here too.
    """
    from tests.fuzzing.harness import scalar

    def matching(predicate):
        return scalar(f"SELECT COUNT(*) AS n FROM testdata.fuzzing.mixed WHERE {predicate}")

    rows = matching("1 = 1")
    assert rows == 2000, f"the corpus has changed: {rows} rows, not 2000"
    nulls = matching("f_null IS NULL")
    assert nulls == 1016, f"the corpus has changed: f_null is NULL in {nulls} rows, not 1016"

    # (predicate, expected UNKNOWN count). The first was the registered repro;
    # the rest are the other coercion shapes the register recorded.
    cases = (
        ("f_null > 0.0", nulls),  # FLOAT vs FLOAT literal — was always correct
        ("f_null > 0", nulls),  # FLOAT vs INTEGER literal
        ("f_null > i_null", 1226),  # FLOAT vs INTEGER column
        ("(d_null / -3) > 0.0", 411),  # DECIMAL arithmetic vs FLOAT
        ("d_null > 0", 411),  # DECIMAL vs INTEGER literal
        ("i_null > 0.0", 411),  # INTEGER vs FLOAT literal
    )
    for predicate, unknown in cases:
        buckets = (
            matching(predicate),
            matching(f"NOT ({predicate})"),
            matching(f"({predicate}) IS NULL"),
        )
        assert buckets[2] == unknown, (
            f"`({predicate}) IS NULL` reports {buckets[2]} UNKNOWN rows, expected {unknown}"
        )
        assert sum(buckets) == rows, (
            f"the three buckets of `{predicate}` cover {sum(buckets)} of {rows} rows "
            f"(matched={buckets[0]}, rejected={buckets[1]}, unknown={buckets[2]})"
        )


def test_is_null_over_temporal_and_empty_string_predicates_reports_unknown():
    """The same validity defect in draken_numeric_cmp's two sibling kernels.

    Both returned validity=nullptr and so answered `(p) IS NULL` FALSE for every
    row, fixed alongside the numeric one:

      * draken_temporal_cmp — a mixed-domain DATE32-vs-TIMESTAMP64 comparison.
      * draken_string_empty — `col <> ''` / `col = ''`, which lower to the unary
        IsNotEmpty/IsEmpty kernel. Its comment DOCUMENTED the filter-only
        reasoning as intentional, which is why nothing caught it.

    Neither shape is reachable from the generator — it emits no mixed-domain
    temporal comparison and no comparison against '' — so this test, not the
    fuzzer, is what holds them fixed. The non-nullable rows are asserted too:
    those still take the validity==nullptr path and must be unaffected.
    """
    from tests.fuzzing.harness import scalar

    def matching(predicate):
        return scalar(f"SELECT COUNT(*) AS n FROM testdata.fuzzing.mixed WHERE {predicate}")

    rows = matching("1 = 1")
    assert rows == 2000, f"the corpus has changed: {rows} rows, not 2000"
    for column in ("ts_null", "dt_null", "s_null"):
        nulls = matching(f"{column} IS NULL")
        assert nulls == 411, f"the corpus has changed: {column} is NULL in {nulls} rows, not 411"

    cases = (
        # draken_temporal_cmp — TIMESTAMP64 vs DATE32, both directions, and
        # against a literal of the opposite domain.
        ("ts_null > dt_null", 411),
        ("dt_null > ts_null", 411),
        ("ts_null > CAST('2020-01-01' AS DATE)", 411),
        # draken_string_empty — both spellings.
        ("s_null <> ''", 411),
        ("s_null = ''", 411),
        # Non-nullable operands: no validity mask is allocated at all.
        ("ts_value > dt_value", 0),
        ("s_low <> ''", 0),
    )
    for predicate, unknown in cases:
        buckets = (
            matching(predicate),
            matching(f"NOT ({predicate})"),
            matching(f"({predicate}) IS NULL"),
        )
        assert buckets[2] == unknown, (
            f"`({predicate}) IS NULL` reports {buckets[2]} UNKNOWN rows, expected {unknown}"
        )
        assert sum(buckets) == rows, (
            f"the three buckets of `{predicate}` cover {sum(buckets)} of {rows} rows "
            f"(matched={buckets[0]}, rejected={buckets[1]}, unknown={buckets[2]})"
        )


def test_wrong_answer_wrapping_still_drops_a_twice_projected_column():
    """Pins single_table_known_gaps/wrapping-drops-a-column-projected-twice."""
    import opteryx

    def column_names(sql):
        session = opteryx.session()
        morsels = list(session.execute_to_morsels(sql))
        assert morsels, f"expected rows from {sql!r}"
        return [name.decode() for name in morsels[0].column_names]

    inner = "SELECT id AS x, id FROM testdata.planets"
    assert column_names(inner) == ["x", "id"], "the direct projection has changed"
    assert column_names(f"SELECT * FROM ({inner}) AS s") == ["x"], (
        "wrapping no longer drops the un-aliased duplicate — the defect is FIXED. Delete this "
        "test, the register entry, and the de-duplication in _build_projection."
    )


def test_wrong_answer_having_column_leak_is_still_present():
    """Pins single_table_known_gaps/having-leaks-its-internal-count.

    Asserts the current, wrong column sets. Goes red when the projection is
    fixed, at which point this test and the register entry both go.

    The two wrapped forms must agree with EACH OTHER here — the CTE form used to
    also lose the user's alias, and that half is fixed. Asserting them separately
    keeps a regression on that half visible instead of folding it into the leak.
    """
    import opteryx

    def column_names(sql):
        session = opteryx.session()
        morsels = list(session.execute_to_morsels(sql))
        assert morsels, f"expected rows from {sql!r}"
        return [name.decode() for name in morsels[0].column_names]

    query = (
        "SELECT i_group, COUNT(row_id) AS a1 FROM testdata.fuzzing.mixed "
        "GROUP BY i_group HAVING COUNT(*) <= 5000"
    )
    assert column_names(query) == ["i_group", "a1"], "the direct form's projection has changed"
    assert column_names(f"SELECT * FROM ({query}) AS s") == ["i_group", "a1", "COUNT(*)"], (
        "the subquery form no longer leaks HAVING's internal COUNT(*) — the defect may be fixed"
    )
    assert column_names(f"WITH c AS ({query}) SELECT * FROM c") == [
        "i_group",
        "a1",
        "COUNT(*)",
    ], (
        "the CTE form's projection has changed. If it now matches the direct form the defect is "
        "FIXED — delete this test and the register entry."
    )


def test_catalog_coverage_is_accounted_for():
    """Every catalog function is either generated or explicitly excluded.

    Without this, `reference/` gaining a function silently adds something the
    fuzzer does not test, and nobody finds out. A new function fails here until
    someone either makes it generatable or writes down why not.
    """
    import json
    from pathlib import Path

    from tests.fuzzing.single_table_grammar import FUNCTION_OVERLOADS

    catalog_path = Path(__file__).resolve().parents[2] / "reference" / "function_signatures.json"
    with catalog_path.open() as handle:
        catalog = json.load(handle)

    generated = {overload.name for overload in FUNCTION_OVERLOADS}
    unaccounted = sorted(set(catalog) - generated - set(EXCLUSIONS))
    assert not unaccounted, (
        "these reference/ functions are neither generated nor explained in "
        f"single_table_grammar.EXCLUSIONS: {unaccounted}"
    )


def test_zz_fuzzing_actually_ran():
    """The run must have executed queries, and every oracle must have fired.

    Named `zz` so pytest's file-order collection runs it last. A fuzzer that
    generated nothing, executed nothing, or silently lost an oracle reports
    success on every other test in this file; this is the only thing that
    notices.
    """
    print(LEDGER.report())

    assert LEDGER.cases > 0, "no fuzz cases ran at all"
    assert LEDGER.statements_executed > 0, (
        f"{LEDGER.cases} cases ran but not one statement executed — every generated query hit a "
        f"registered defect. The fuzzer tested nothing.\n{LEDGER.report()}"
    )

    if LEDGER.cases < _COVERAGE_FLOOR:
        return

    silent = sorted(name for name in _ALL_ORACLE_NAMES if not LEDGER.oracle_runs.get(name))
    assert not silent, (
        f"these oracles never fired across {LEDGER.cases} cases, so they are asserting nothing: "
        f"{silent}. Either the generator no longer produces the shapes they need, or their "
        f"precondition in applicable_oracles() has stopped matching.\n{LEDGER.report()}"
    )


if __name__ == "__main__":  # pragma: no cover
    for _seed in SEEDS:
        test_sql_fuzzing_single_table(_seed)
    for _defect in known_gaps.REGISTER:
        test_registered_defect_still_reproduces(_defect)
    test_catalog_coverage_is_accounted_for()
    test_zz_fuzzing_actually_ran()
    print("\n✅ okay")
