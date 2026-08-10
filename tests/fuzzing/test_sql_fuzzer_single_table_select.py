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
            blocked = _ORACLES_BLOCKED_BY_REGISTER.get(name)
            suffix = f"  [blocked by {blocked}]" if blocked else ""
            lines.append(f"  {self.oracle_runs.get(name, 0):6d}  {name}{suffix}")
        if self.known_gap_hits:
            lines += ["", "registered defects hit (see single_table_known_gaps.py):"]
            for gap_id, count in self.known_gap_hits.most_common():
                lines.append(f"  {count:6d}  {gap_id}")
        lines += ["", f"distinct SQL constructs generated: {len(self.constructs)}"]
        return "\n".join(lines)


_ALL_ORACLE_NAMES = {
    "aggregate_filter_matches_where",
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

# Oracle -> the register entry that stops it running. An oracle here is expected
# to be silent, and `test_zz_fuzzing_actually_ran` exempts it from the
# "asserting nothing" check — but only while its defect is REGISTERED. When the
# defect is fixed the register entry goes, the id here dangles, and this file
# fails until the oracle is put back into applicable_oracles().
#
# This is not a second allowlist. A blocked oracle is silent because a WRONG
# ANSWER is already pinned by an explicit test_wrong_answer_* test, which is what
# the register's contract requires of a WrongAnswer entry; the map exists so the
# silence is DECLARED and self-expiring rather than a hole in the ledger.
_ORACLES_BLOCKED_BY_REGISTER: dict = {}

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
    from contextlib import contextmanager

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

    # A defect the optimizer differential oracle found needs its strategy turned
    # off to appear at all. Without honouring this the entry could carry no repro
    # that reproduces, and the register's one real gate — "every entry must still
    # be broken" — would silently not apply to it.
    @contextmanager
    def _as_registered():
        if defect.disabled_strategy is None:
            yield
        else:
            with oracles._strategy_disabled(defect.disabled_strategy):
                yield

    if defect.error_type == "WrongAnswer":
        # A wrong-answer entry has no exception to match; each is pinned by its
        # own test_wrong_answer_* test. Assert only that the repro still
        # executes, so one that has rotted into a syntax error is caught.
        with _as_registered():
            drain(defect.repro)
        return

    with pytest.raises(Exception) as raised:  # noqa: PT011 - the type IS the assertion
        with _as_registered():
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


def test_nan_withholding_cites_a_live_register_entry():
    """An aggregate withheld from NaN columns must point at an open defect.

    Same contract as test_pending_exclusion_cites_a_live_register_entry: the
    narrowing costs reach, so it expires with the entry that justifies it rather
    than being inherited by whoever reads the code next.
    """
    from tests.fuzzing.single_table_grammar import _AGGREGATES_WITHHELD_FROM_NAN

    registered = {defect.id for defect in known_gaps.REGISTER}
    dangling = sorted(
        f"{aggregate} (cites `{gap_id}`)"
        for aggregate, gap_id in _AGGREGATES_WITHHELD_FROM_NAN.items()
        if gap_id not in registered
    )
    assert not dangling, (
        f"these aggregates are withheld from NaN-bearing columns by a register entry that no "
        f"longer exists: {dangling}. The defect is fixed, so the withholding must go."
    )


def test_string_concatenation_requires_homogeneous_string_types():
    """Pins RATIFIED/string-concatenation-requires-homogeneous-string-types.

    Replaces test_wrong_answer_concat_of_a_varbinary_literal_still_leaks, which
    asserted the leak this ruling removed. Three things are checked, because the
    ruling is not "VARBINARY is refused" and a test that only checked the
    refusals would let the useful half rot:

      * homogeneous concat WORKS, for every string type and through both
        spellings — this is what the deleted register entries wrongly claimed
        had no kernel;
      * mixed concat is refused in ONE class, IncorrectTypeError, whether the
        operands are columns or literals and whether it is written `||` or
        CONCAT. The literal/column split is exactly what used to make a
        difference, so both are asserted;
      * no answer anywhere contains a Python object repr. Asserted directly
        rather than inferred from the refusals: constant folding was the path
        that leaked, and a future fold that answers instead of refusing must not
        be able to reintroduce it quietly.
    """
    import pytest

    from opteryx.exceptions import IncompatibleTypesError, IncorrectTypeError
    from tests.fuzzing.harness import rows

    def value(sql):
        """First value, decoded. A VARBINARY result comes back as `bytes`, and
        comparing `str(b'ab')` would assert on the repr rather than the content —
        which is the exact confusion this ruling exists to remove."""
        answer = list(rows(sql))[0][0]
        return answer.decode("ascii") if isinstance(answer, bytes) else str(answer)

    def _type_of(sql):
        """The DECLARED type of the first column, read from the morsel schema."""
        import opteryx

        for morsel in opteryx.session().execute_to_morsels(sql):
            return morsel.column_types[0].name
        raise AssertionError(f"no morsel produced for {sql!r}")

    # Homogeneous — every string type, literal and column, `||` and CONCAT.
    assert value("SELECT b'a' || b'b' AS x") == "ab"
    assert value("SELECT CONCAT(b'a', b'b') AS x") == "ab"
    assert value("SELECT CONCAT(b'a', b'b', b'c') AS x") == "abc"
    assert value("SELECT CONCAT_WS(b'-', b'a') AS x") == "a"
    assert value("SELECT 'a' || 'b' AS x") == "ab"
    assert value(
        "SELECT name::VARBINARY || name::VARBINARY AS x FROM testdata.planets LIMIT 1"
    ) == "MercuryMercury"

    # The return type FOLLOWS the operand type — CONCAT and `||` must agree, since
    # one desugars into the other. This was the visible tell that CONCAT's catalog
    # return type was a hardcoded VARCHAR: `b'a' || b'b'` said VARBINARY while
    # `CONCAT(b'a', b'b')` said VARCHAR for the very same bytes.
    assert _type_of("SELECT b'a' || b'b' AS x") == "VARBINARY"
    assert _type_of("SELECT CONCAT(b'a', b'b') AS x") == "VARBINARY"
    assert _type_of("SELECT CONCAT_WS(b'-', b'a', b'b') AS x") == "VARBINARY"
    assert _type_of("SELECT CONCAT('a', 'b') AS x") == "VARCHAR"

    # A non-string operand is NOT coerced. CONCAT used to cast it to VARCHAR;
    # that is gone, and the cast is the caller's to write.
    with pytest.raises(Exception, match="VARCHAR"):
        list(rows("SELECT CONCAT(-327484, 'a') AS x FROM testdata.planets LIMIT 1"))
    assert value("SELECT CONCAT(CAST(-327484 AS VARCHAR), 'a') AS x FROM testdata.planets LIMIT 1") == "-327484a"
    assert value("SELECT CONCAT(CAST(id AS VARCHAR), name) AS x FROM testdata.planets LIMIT 1") == "1Mercury"

    # A NULL operand stays legal and stays TRANSPARENT to overload selection — it
    # is `||`'s dedicated NULL rule, and CONCAT must not disagree with it.
    assert value("SELECT CONCAT(name, NULL) AS x FROM testdata.planets LIMIT 1") == "None"
    assert value("SELECT name || NULL AS x FROM testdata.planets LIMIT 1") == "None"

    # Mixed operands are refused for every spelling and both operand kinds. The
    # CLASS tracks function-vs-operator, which is the engine's existing split:
    # CONCAT is refused by overload resolution like any other function whose
    # argument is the wrong type (binder.py -> IncompatibleTypesError, the same
    # error `UPPER(id)` gets), `||` by the operator map (IncorrectTypeError, the
    # same error `id || ''` gets). Both name the cast that fixes it. What must
    # NOT come back is NotSupportedError — "we have not built it yet" for
    # something ruled out on purpose was half the original defect.
    mixed_function_calls = [
        "SELECT CONCAT('p', b'a') AS x",
        "SELECT CONCAT(b'a', 'p') AS x",
        "SELECT CONCAT(-327484, b'a') AS x",
        "SELECT CONCAT_WS('-', 'p', b'a') AS x",
        "SELECT CONCAT_WS('-', b'a', b'b') AS x",
        "SELECT CONCAT(name, b'a') AS x FROM testdata.planets",
    ]
    for sql in mixed_function_calls:
        with pytest.raises(IncompatibleTypesError):
            list(rows(sql))

    mixed_operators = [
        "SELECT 'p' || b'a' AS x",
        "SELECT name || b'a' AS x FROM testdata.planets",
        "SELECT HASH(name) || '' AS x FROM testdata.planets",
        "SELECT name::NVARCHAR || name AS x FROM testdata.planets",
        # No string operand at all: nothing gives the result a type. This one
        # only ever "worked" by being folded through the deleted Python closure,
        # which picked VARCHAR out of the air.
        "SELECT NULL || NULL AS x",
    ]
    for sql in mixed_operators:
        with pytest.raises(IncorrectTypeError):
            list(rows(sql))

    # No repr may reach a result, by any route.
    for sql in (
        "SELECT CONCAT(b'a', b'b') AS x",
        "SELECT b'a' || b'b' AS x",
        "SELECT CONCAT(CAST(-327484 AS VARCHAR), 'a') AS x",
    ):
        answer = value(sql)
        assert "draken_native" not in answer and "object at 0x" not in answer, (
            f"{sql} put a Python object repr in the answer: {answer!r}"
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


def test_not_over_a_boolean_tautology_disjunction_partitions_cleanly():
    """Regression for the fixed not-over-a-boolean-tautology-disjunction defect.

    _simplify_and_chain used to dedup the conjuncts of a flattened AND chain by
    `Node.uuid`. De Morgan over `NOT ((A OR NOT A) OR NOT B)` yields
    `A != TRUE AND A = TRUE AND B`, and the first two conjuncts carried the SAME
    uuid — the binder copies uuid across expressions that render alike, and the
    NOT(A = B) => A != B rule inverts the comparison in place without refreshing
    it. Dedup deleted the contradiction, so `NOT p` matched 492 rows while `p`
    matched all 2,000.

    Both the BOOLEAN and the nullable-INTEGER right operand are covered: the
    defect fired on each.
    """
    from tests.fuzzing.harness import scalar

    def matching(where):
        return scalar(f"SELECT COUNT(*) AS n FROM testdata.fuzzing.mixed WHERE {where}")

    total = matching("row_id IS NOT NULL")

    for right in ("b_value = b_null", "i_null > 0"):
        predicate = f"((b_value = TRUE) OR NOT (b_value = TRUE)) OR NOT ({right})"
        # the left disjunct is a tautology, so `p` holds for every row...
        assert matching(predicate) == total, predicate
        # ...and the three buckets must partition the relation exactly once.
        buckets = (
            matching(predicate)
            + matching(f"NOT ({predicate})")
            + matching(f"({predicate}) IS NULL")
        )
        assert buckets == total, f"{predicate} covers {buckets} of {total} rows"


def test_nan_row_is_visible_to_a_pushed_predicate():
    """Regression for the fixed nan-rows-fall-outside-every-predicate-bucket defect.

    testdata.satellites has one NaN density (row 176). Under the locked float
    semantics (draken/ops/float_ops.h, NaN ranks above every value, NaN is a
    VALUE and not a NULL) that row satisfies `density > <anything finite>`.

    It did not, and the reason was never the comparison — it was row-group
    pruning. Parquet keeps NaN out of min/max to spec, so `col_max <= 72971.56`
    "proved" no row could match and the whole row group went, taking the NaN
    with it. The tell is that blocking the pushdown changed the answer, so that
    is what this asserts: the same predicate over a derived table the pruning
    cannot reach must agree with the pushed form, and the three buckets must
    partition the relation.
    """
    from tests.fuzzing.harness import scalar

    predicate = "density > 72971.564572"

    def pushed(where):
        return scalar(f"SELECT COUNT(*) AS n FROM testdata.satellites WHERE {where}")

    def unpushed(where):
        # `density + 0.0` makes the predicate reference a computed column, so it
        # cannot be pushed into the scan and no bound is ever consulted.
        return scalar(
            "SELECT COUNT(*) AS n FROM (SELECT density + 0.0 AS density "
            f"FROM testdata.satellites) AS t WHERE {where}"
        )

    total = scalar("SELECT COUNT(*) AS n FROM testdata.satellites")
    assert total == 177, f"testdata.satellites has {total} rows, not 177; the corpus has changed"
    assert pushed("density IS NULL") == 0, "satellites.density has acquired a NULL"

    # The NaN row is the one and only match — NaN outranks every finite value.
    assert pushed(predicate) == 1, "the NaN row is not being selected by `> <finite>`"
    # Pruning must not be able to change an answer.
    assert pushed(predicate) == unpushed(predicate)
    assert pushed(f"NOT ({predicate})") == unpushed(f"NOT ({predicate})")

    buckets = (
        pushed(predicate)
        + pushed(f"NOT ({predicate})")
        + pushed(f"({predicate}) IS NULL")
    )
    assert buckets == total, f"{predicate} covers {buckets} of {total} rows"

    # A NaN-valued LITERAL used to kill the query in the planner: the selectivity
    # estimator returned NaN and `int(row_count * selectivity)` raised
    # `ValueError: cannot convert float NaN to integer`. Nothing is >= NaN except
    # a NaN, and no planet has one.
    assert scalar(
        "SELECT COUNT(*) AS n FROM testdata.planets WHERE orbital_period >= SQRT(-390664.0)"
    ) == 0


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


def test_pending_exclusion_cites_a_live_register_entry():
    """A locally-withheld parameter type must point at an open defect.

    `_PENDING_EXCLUSIONS` narrows what the generator will pass to a function
    where the CATALOG says the type is allowed. That is a real reduction in
    reach, so it has to expire: when the cited register entry goes, this fails
    and the narrowing has to be either removed or moved into the registrar as a
    proper `excludes`, which is a decision someone has to make rather than
    inherit.
    """
    from tests.fuzzing.single_table_grammar import _PENDING_EXCLUSIONS

    registered = {defect.id for defect in known_gaps.REGISTER}
    dangling = sorted(
        f"{function} (cites `{gap_id}`)"
        for function, (_, gap_id) in _PENDING_EXCLUSIONS.items()
        if gap_id not in registered
    )
    assert not dangling, (
        f"these generator narrowings cite a register entry that no longer exists: {dangling}. "
        f"The defect is resolved, so either delete the _PENDING_EXCLUSIONS entry or record the "
        f"restriction as an `excludes` in the registrar and regenerate reference/."
    )


def test_catalog_coverage_is_accounted_for():
    """Every catalog function is generated, declined by the catalog, or excluded.

    Without this, `reference/` gaining a function silently adds something the
    fuzzer does not test, and nobody finds out. A new function fails here until
    someone either makes it generatable or writes down why not.

    Three ways to be accounted for, in descending order of how much they are
    worth: GENERATED; declined by a fact the CATALOG states (a `value_format`
    this generator cannot mint, an `element_of` link, a parameter type nothing
    in the corpus satisfies) — recorded in `CATALOG_DECLINED`, which needs no
    argument from anybody because `reference/` already carries the reason; or
    argued for by hand in `EXCLUSIONS`.
    """
    import json
    from pathlib import Path

    from tests.fuzzing.single_table_grammar import CATALOG_DECLINED
    from tests.fuzzing.single_table_grammar import FUNCTION_OVERLOADS

    catalog_path = Path(__file__).resolve().parents[2] / "reference" / "function_signatures.json"
    with catalog_path.open() as handle:
        catalog = json.load(handle)

    generated = {overload.name for overload in FUNCTION_OVERLOADS}
    unaccounted = sorted(set(catalog) - generated - set(EXCLUSIONS) - set(CATALOG_DECLINED))
    assert not unaccounted, (
        "these reference/ functions are neither generated, nor declined by a constraint "
        "reference/ records, nor explained in single_table_grammar.EXCLUSIONS: "
        f"{unaccounted}"
    )

    # An EXCLUSIONS entry for something the catalog already declines is dead
    # weight — two reasons for one omission, and the hand-written one rots.
    redundant = sorted(set(EXCLUSIONS) & set(CATALOG_DECLINED))
    assert not redundant, (
        "these are excluded by hand AND declined by reference/; drop the EXCLUSIONS "
        f"entry and let the catalog speak: {redundant}"
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

    registered = {defect.id for defect in known_gaps.REGISTER}
    expired = sorted(
        f"{oracle} (cites `{gap_id}`)"
        for oracle, gap_id in _ORACLES_BLOCKED_BY_REGISTER.items()
        if gap_id not in registered
    )
    assert not expired, (
        f"these oracles are blocked by a register entry that no longer exists: {expired}. "
        f"The defect is fixed, so the oracle must go back into applicable_oracles() and its "
        f"entry here must be deleted."
    )

    if LEDGER.cases < _COVERAGE_FLOOR:
        return

    silent = sorted(
        name
        for name in _ALL_ORACLE_NAMES - set(_ORACLES_BLOCKED_BY_REGISTER)
        if not LEDGER.oracle_runs.get(name)
    )
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
    test_wrong_answer_qualify_is_ignored()
    test_wrong_answer_aggregate_filter_is_ignored()
    test_catalog_coverage_is_accounted_for()
    test_zz_fuzzing_actually_ran()
    print("\n✅ okay")
