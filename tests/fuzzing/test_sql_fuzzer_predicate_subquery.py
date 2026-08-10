"""
Predicate-subquery fuzzer.

Neither the single-table fuzzer nor the join fuzzer emits a predicate subquery:
measured over 4,000 statements from `single_table_grammar.generate`, EXISTS,
IN (subquery), scalar subquery and correlated anything are all at 0%. This
covers that family — the one that goes through `DecorrelateSubqueryStrategy` and
`CorrelatedFiltersStrategy`, and the one whose bug history is longest.

WHY IT IS A SEPARATE HARNESS
----------------------------
It has an oracle the other two cannot have. Every correlated predicate form has
an EXACT join rewrite, so both spellings are generated from one case and
required to agree — see `subquery_oracles.py`. That is a comparison against a
different planner path, not the metamorphic self-comparison the single-table
fuzzer is limited to, so it catches a wrong answer that is wrong CONSISTENTLY.

REPRODUCING A FAILURE
---------------------
    TEST_SEED=<seed from the output> python -m pytest \\
        tests/fuzzing/test_sql_fuzzer_predicate_subquery.py

and add the seed to `seeds/predicate_subquery.txt` so it runs on every future
invocation regardless of `TEST_SEED`.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import random
import subprocess
from collections import Counter
from pathlib import Path

import pytest

from tests.fuzzing import subquery_known_gaps as known_gaps
from tests.fuzzing import subquery_oracles as oracles
from tests.fuzzing.harness import case_seeds
from tests.fuzzing.harness import rows
from tests.fuzzing.subquery_grammar import FORMS
from tests.fuzzing.subquery_grammar import POSITION_REFUSAL_SIGNATURE
from tests.fuzzing.subquery_grammar import POSITIONS_REFUSED_PROMPTLY
from tests.fuzzing.subquery_grammar import SUPPORT_MATRIX
from tests.fuzzing.subquery_grammar import assert_null_coverage
from tests.fuzzing.subquery_grammar import generate
from tests.fuzzing.subquery_grammar import load_pairs

# Cases per run when TEST_ITERATIONS is unset. Every case executes at least
# three queries (the statement plus two oracles, several of which run more), so
# this is deliberately below the single-table fuzzer's default.
DEFAULT_ITERATIONS = 150

# How many oracles to run per case. All of them every time would put ~10 queries
# on every case; a random sample keeps each case cheap while a run still covers
# each oracle many times.
ORACLES_PER_CASE = 2

# Below this many cases, "an oracle never fired" is expected rather than
# suspicious. Above it, an oracle that never fires has become unreachable.
_COVERAGE_FLOOR = 120

_REPO_ROOT = Path(__file__).resolve().parents[2]

SEEDS = case_seeds("predicate_subquery", int(os.environ.get("TEST_ITERATIONS", DEFAULT_ITERATIONS)))


class _RunLedger:
    """What this run actually did.

    Module scope, because pytest runs each parametrized case independently and
    nothing else would notice that every case died in generation and no query
    was ever executed.
    """

    def __init__(self) -> None:
        self.cases = 0
        self.statements_executed = 0
        self.oracle_runs: Counter = Counter()
        self.oracle_queries = 0
        self.known_gap_hits: Counter = Counter()
        self.forms: Counter = Counter()
        self.pairs: Counter = Counter()

    def report(self) -> str:
        lines = [
            "",
            "═══ predicate-subquery fuzzer ═══",
            f"cases:                {self.cases}",
            f"statements executed:  {self.statements_executed}",
            f"oracle invocations:   {sum(self.oracle_runs.values())} "
            f"({self.oracle_queries} extra queries)",
            "",
            "oracle invocations by kind:",
        ]
        for name in sorted(oracles.ALL_ORACLE_NAMES):
            lines.append(f"  {self.oracle_runs.get(name, 0):6d}  {name}")
        lines += ["", "cases by form:"]
        for form, count in sorted(self.forms.items()):
            lines.append(f"  {count:6d}  {form}")
        lines += ["", "cases by correlation pair:"]
        for pair, count in sorted(self.pairs.items()):
            lines.append(f"  {count:6d}  {pair}")
        if self.known_gap_hits:
            lines += ["", "registered defects hit (see subquery_known_gaps.py):"]
            for gap_id, count in self.known_gap_hits.most_common():
                lines.append(f"  {count:6d}  {gap_id}")
        return "\n".join(lines)


LEDGER = _RunLedger()


@pytest.mark.parametrize("seed", SEEDS)
def test_sql_fuzzing_predicate_subquery(seed):
    """One generated statement, executed, then checked by its applicable oracles."""
    # Printed unconditionally and FIRST: when this case dies, the seed is what
    # reproduces it, and a seed printed only on the success path is no use.
    print(f"\nSeed: {seed}")

    rng = random.Random(seed)
    case = generate(rng)
    print(case.sql)

    LEDGER.cases += 1
    LEDGER.forms[case.form] += 1
    LEDGER.pairs[case.pair.name] += 1

    # Everything this generator emits is intended to run, so a raise is a
    # finding. The only thing this except does is look the failure up in the
    # register — `_classify` RE-RAISES anything not already accounted for, so
    # nothing is swallowed.
    try:
        produced = _execute_and_read_every_value(case.sql)
    except Exception as error:  # noqa: BLE001 - _classify re-raises unless registered
        _classify(error, case.tags)
        return
    print(f"Rows: {produced}")
    LEDGER.statements_executed += 1

    available = oracles.applicable_oracles(case)
    if not available:
        return
    for oracle in rng.sample(available, min(ORACLES_PER_CASE, len(available))):
        try:
            result = oracle(case, rng)
        except oracles.OracleViolation:
            # Always fatal. There is deliberately no path that silences a wrong
            # answer: known wrong answers are kept out by a structural exclusion
            # in applicable_oracles(), not by matching the violation's text.
            raise
        except Exception as error:  # noqa: BLE001 - _classify re-raises unless registered
            _classify(error, case.tags)
            continue
        LEDGER.oracle_runs[result.name] += 1
        LEDGER.oracle_queries += result.queries_executed
        print(f"  oracle ok: {result.name}")


def _execute_and_read_every_value(sql: str) -> int:
    """Execute `sql`, touch every value, and return the row count.

    Reading the values matters: a shape-only execution counts rows without
    decoding them, so a corrupted string or a mis-typed column looks fine.
    "Did not raise" has to mean the whole result was producible.
    """
    produced = 0
    for row in rows(sql):
        produced += 1
        for value in row:
            repr(value)
    return produced


def _classify(error: Exception, tags) -> None:
    """Re-raise, unless this exact failure is already a registered defect.

    `tags` are the case's shape tags: an entry may require the failure to have
    come from the shape it is about, so a broad message cannot absorb an
    unrelated new bug. See RegisteredDefect.requires_tag.
    """
    defect = known_gaps.match(error, tags)
    if defect is None:
        raise error
    LEDGER.known_gap_hits[defect.id] += 1
    print(f"  known defect: {defect.id} ({type(error).__name__})")


# ─────────────────────────────────────────────────────────────────────────────
# The register must stay honest
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("defect", known_gaps.REGISTER, ids=lambda d: d.id)
def test_registered_defect_still_reproduces(defect):
    """Every registered defect must still be broken.

    This is what stops the register becoming a place bugs go to be forgotten.
    When one is fixed this goes red, and the only way to green is to delete the
    entry — which puts the construct back into ordinary fuzzing.
    """

    def drain(sql):
        for row in rows(sql):
            for value in row:
                repr(value)

    if defect.error_type == "WrongAnswer":
        # A wrong-answer entry has no exception to match; each is pinned by its
        # own test_wrong_answer_* test below. Assert only that the repro still
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


# A subquery in an unsupported POSITION used to make the planner loop forever;
# it is now refused. Generous on purpose: the deadline separates non-termination
# from slowness, and planning over nine planets and 177 satellites needs neither.
_REFUSAL_DEADLINE = 20.0


@pytest.mark.parametrize(
    "label,sql", POSITIONS_REFUSED_PROMPTLY, ids=[row[0] for row in POSITIONS_REFUSED_PROMPTLY]
)
def test_subquery_position_is_refused_promptly(label, sql):
    """An EXISTS/IN outside a top-level conjunct must be REFUSED, and must return.

    Both halves matter, and the second is why this runs in a subprocess rather
    than under `pytest.raises`.

    Before the guard at the top of `_build_filter_join`, the four IN spellings
    did not raise — they never came back. `_build_filter_join` discarded the
    found flag from `_split_out`, which can only remove a target that is the
    whole condition or a conjunct of a top-level AND chain; under NOT / OR /
    IS NULL the condition returned unchanged, still holding the IN node, and
    `_rewrite_filters`' `while finder(...)` loop found it again. Forever, at 88 MB
    and climbing slowly, with no error and no result.

    An in-process assertion here would, if that regressed, hang the entire suite
    with no output. A subprocess with a deadline fails the run in bounded time
    instead, and still fails it if the query is answered rather than refused.

    The two EXISTS spellings are covered by the same test because they were the
    same bug: they DID terminate, but only by looping twice and then reporting
    `**EXISTS** requires a correlated equality predicate` about a correlation
    the first pass had already lifted out. Both now name the position.
    """
    script = (
        "import sys; sys.path.insert(1, %r)\n"
        "import opteryx\n"
        "session = opteryx.session()\n"
        "try:\n"
        "    for morsel in session.execute_to_morsels(%r):\n"
        "        pass\n"
        "    print('ANSWERED')\n"
        "except Exception as error:\n"
        "    print('REFUSED', type(error).__name__, error)\n" % (str(_REPO_ROOT), sql)
    )
    try:
        completed = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            timeout=_REFUSAL_DEADLINE,
            cwd=str(_REPO_ROOT),
        )
    except subprocess.TimeoutExpired:
        raise AssertionError(
            f"`{label}` did not terminate within {_REFUSAL_DEADLINE}s. The planner "
            f"non-termination is back: _build_filter_join must refuse a subquery "
            f"_split_out cannot remove from the predicate.\n  {sql}"
        ) from None

    output = completed.stdout.strip()
    assert output.startswith("REFUSED UnsupportedSyntaxError"), (
        f"`{label}` should be refused with UnsupportedSyntaxError.\n  {sql}\n"
        f"  exit={completed.returncode}\n  stdout={output[:400]}\n"
        f"  stderr={completed.stderr.strip()[-400:]}"
    )
    assert POSITION_REFUSAL_SIGNATURE in output, (
        f"`{label}` is refused, but not by the positional guard — it no longer says "
        f"{POSITION_REFUSAL_SIGNATURE!r}, so some other path is refusing it and the guard "
        f"may not be reached at all.\n  {sql}\n  {output[:400]}"
    )


def test_wrong_answer_correlated_scalar_subquery_drops_unmatched_outer_rows():
    """Pins subquery_known_gaps/correlated-scalar-subquery-drops-unmatched-outer-rows.

    A wrong answer cannot be absorbed by message matching, so the broken
    behaviour is asserted DIRECTLY here. When the decorrelation stops using an
    INNER join this goes red, and the register entry and the
    `applicable_oracles` exclusion that cites it both come out.

    Mercury (id 1) and Venus (id 2) have no satellites. Every assertion below
    records what the engine returns today; the comment on each records what SQL
    requires.
    """
    from tests.fuzzing.harness import rows as read

    def ids(predicate):
        sql = f"SELECT sq_o.id FROM testdata.planets AS sq_o WHERE {predicate}"
        return sorted(row[0] for row in read(sql))

    moonless = "(SELECT COUNT(*) FROM testdata.satellites AS sq_i WHERE sq_i.planetId = sq_o.id)"
    biggest = "(SELECT MAX(sq_i.radius) FROM testdata.satellites AS sq_i WHERE sq_i.planetId = sq_o.id)"

    assert ids("sq_o.id IS NOT NULL") == [1, 2, 3, 4, 5, 6, 7, 8, 9], "the corpus has changed"
    assert ids("sq_o.id IN (SELECT sq_i.planetId FROM testdata.satellites AS sq_i)") == [
        3,
        4,
        5,
        6,
        7,
        8,
        9,
    ], "the corpus has changed: Mercury and Venus are no longer the moonless planets"

    assert ids(f"{moonless} = 0") == []  # SQL requires [1, 2]
    assert ids(f"{moonless} < 1") == []  # SQL requires [1, 2]
    assert ids(f"{biggest} IS NULL") == []  # SQL requires [1, 2]
    assert ids(f"COALESCE({biggest}, -1.0) < 0.0") == []  # SQL requires [1, 2]
    assert ids(f"sq_o.id > {moonless}") == [3, 4, 9]  # SQL requires [1, 2, 3, 4, 9]

    # The half that IS right, asserted so a "fix" that swings the other way —
    # keeping unmatched rows where the comparison should have dropped them —
    # cannot pass this test.
    assert ids(f"sq_o.id > {biggest}") == []
    assert ids(
        "sq_o.id >= (SELECT MIN(sq_i.id) FROM testdata.satellites AS sq_i "
        "WHERE sq_i.planetId = sq_o.id)"
    ) == [3, 4, 5]


# ─────────────────────────────────────────────────────────────────────────────
# The generator's assumptions must stay true
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "label,sql,expected", SUPPORT_MATRIX, ids=[row[0] for row in SUPPORT_MATRIX]
)
def test_engine_support_matrix_is_current(label, sql, expected):
    """What the generator may emit is MEASURED on every run, not remembered.

    The generator was built against a measurement of which subquery forms this
    engine accepts. A comment recording that measurement decays: a form that
    gains support stays out of the generator forever, and a form that loses it
    starts failing every case with no indication that the ground moved. This
    re-measures instead. `expected` is None for a form that must run, or a
    substring of the refusal it must produce.

    A refusal recorded here is a statement about what the engine DOES, not an
    endorsement of it — three of these refusals are registered defects in
    `subquery_known_gaps.py` because the diagnosis misdescribes the limitation.
    """

    def drain():
        for row in rows(sql):
            for value in row:
                repr(value)

    if expected is None:
        drain()
        return

    with pytest.raises(Exception) as raised:  # noqa: PT011 - the message IS the assertion
        drain()
    assert expected in str(raised.value), (
        f"`{label}` no longer fails with {expected!r}. If the engine now SUPPORTS it, move it "
        f"into the generator; if the message changed, update the matrix.\n  {sql}\n"
        f"  {type(raised.value).__name__}: {raised.value}"
    )


def test_not_in_over_an_empty_subquery_is_true_for_every_row():
    """The third branch of `not_in_null_semantics`, asserted directly.

    Whether that branch fires during a fuzz run depends on which inner filter is
    drawn, so the corpus carries a filter that matches nothing (see the note on
    the planets/satellites pair) AND the behaviour is pinned here, where it runs
    every time.

    The NULL-keyed outer row is the whole point. `NULL NOT IN (<non-empty>)` is
    UNKNOWN and gets dropped, but `NULL NOT IN (<empty>)` is TRUE and survives —
    nothing was ever compared to it. An engine that implements NOT IN as
    "anti join, then drop NULL keys" gets this row wrong.
    """
    from tests.fuzzing.harness import rows as read

    nothing = "SELECT sq_i.planetId FROM testdata.satellites AS sq_i WHERE sq_i.radius > 1000000.0"
    assert list(read(nothing)) == [], "the corpus has changed: that filter matches a satellite"

    nullable = "(SELECT NULLIF(id, 5) AS sq_key FROM testdata.planets) AS sq_o"
    everything = sorted((row[0] for row in read(f"SELECT sq_o.sq_key FROM {nullable}")), key=repr)
    assert everything.count(None) == 1, "the corpus has changed: planet 5 is not the NULL key"

    kept = f"SELECT sq_o.sq_key FROM {nullable} WHERE sq_o.sq_key NOT IN ({nothing})"
    survived = sorted((row[0] for row in read(kept)), key=repr)
    assert survived == everything, (
        "NOT IN over an EMPTY set must be TRUE for every outer row, including the one whose "
        f"own key is NULL: got {survived}, expected {everything}"
    )

    # ...and the positive form over an empty set is FALSE for every row.
    assert list(read(f"SELECT sq_o.sq_key FROM {nullable} WHERE sq_o.sq_key IN ({nothing})")) == []


def test_register_shape_tags_are_reachable():
    """A `requires_tag` naming a shape the generator never emits is a dead entry.

    The tag is what stops a broad error message absorbing an unrelated bug, so
    it has to be a shape that really occurs — otherwise the entry absorbs
    NOTHING, the defect fails the run on every hit, and the register looks like
    it is doing a job it is not.
    """
    required = {
        defect.requires_tag for defect in known_gaps.REGISTER if defect.requires_tag is not None
    }
    if not required:
        return
    rng = random.Random(0)
    emitted = set()
    for _ in range(2000):
        emitted |= generate(rng).tags
    missing = sorted(required - emitted)
    assert not missing, (
        f"these register entries require a case shape the generator did not emit once in "
        f"2,000 draws, so they can never absorb anything: {missing}"
    )


def test_every_correlation_pair_discriminates():
    """No pair may make the oracles compare two identical trivial results.

    `load_pairs` refuses a vacuous pair, so this mostly asserts that the check
    ran. It also prints the measured shape of the corpus, which is the fastest
    way to see that a data change has quietly made a pair uninteresting.
    """
    pairs = load_pairs()
    assert pairs, "no correlation pairs are declared"
    for pair in pairs:
        print(
            f"  {pair.name:38s} outer={pair.outer_rows:7d} semi={pair.semi_rows:7d} "
            f"outer-key NULLs={pair.outer_key_nulls:5d} inner-key NULLs={pair.inner_key_nulls:6d}"
        )
        assert 0 < pair.semi_rows < pair.outer_rows, (
            f"correlation pair {pair.name!r} selects {pair.semi_rows} of {pair.outer_rows} "
            f"outer rows, so every oracle over it compares two trivial results"
        )


def test_null_keys_are_present_in_the_corpus():
    """`NOT IN`'s interesting branches must be reachable.

    Without a NULL inner key the null-collapse branch never runs; without a NULL
    outer key the case that distinguishes NOT IN from an anti join never runs.
    Either way `not_in_null_semantics` would silently degrade to asserting only
    its anti-join branch — which is the one a wrong implementation gets right.
    """
    assert_null_coverage()


def test_zz_fuzzing_actually_ran():
    """The run must have executed queries, covered every form, and fired every oracle.

    Named `zz` so pytest's file-order collection runs it last. A fuzzer that
    generated nothing, executed nothing, or silently lost an oracle reports
    success on every other test in this file; this is the only thing that
    notices.
    """
    print(LEDGER.report())

    assert LEDGER.cases > 0, "no fuzz cases ran at all"
    assert LEDGER.statements_executed > 0, (
        f"{LEDGER.cases} cases ran but not one statement executed — every generated query hit "
        f"a registered defect. The fuzzer tested nothing.\n{LEDGER.report()}"
    )

    if LEDGER.cases < _COVERAGE_FLOOR:
        return

    missing = sorted({form for form, _ in FORMS} - set(LEDGER.forms))
    assert not missing, (
        f"these predicate forms were never generated across {LEDGER.cases} cases: {missing}. "
        f"Either their weight is unreachable or the pair filter in generate() excludes them "
        f"everywhere.\n{LEDGER.report()}"
    )

    silent = sorted(name for name in oracles.ALL_ORACLE_NAMES if not LEDGER.oracle_runs.get(name))
    assert not silent, (
        f"these oracles never fired across {LEDGER.cases} cases, so they are asserting "
        f"nothing: {silent}. Either the generator no longer produces the shapes they need, or "
        f"their precondition in applicable_oracles() has stopped matching.\n{LEDGER.report()}"
    )


if __name__ == "__main__":  # pragma: no cover
    for _seed in SEEDS:
        test_sql_fuzzing_predicate_subquery(_seed)
    for _defect in known_gaps.REGISTER:
        test_registered_defect_still_reproduces(_defect)
    for _label, _sql in POSITIONS_REFUSED_PROMPTLY:
        test_subquery_position_is_refused_promptly(_label, _sql)
    test_wrong_answer_correlated_scalar_subquery_drops_unmatched_outer_rows()
    test_not_in_over_an_empty_subquery_is_true_for_every_row()
    test_register_shape_tags_are_reachable()
    test_every_correlation_pair_discriminates()
    test_null_keys_are_present_in_the_corpus()
    test_zz_fuzzing_actually_ran()
    print("\n✅ okay")
