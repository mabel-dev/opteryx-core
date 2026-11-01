"""
Reference fuzzer: random tables and queries, run on Opteryx and on DuckDB, compared.

Modelled on Jepsen's `compare` workload (ParticleDB 1.1.4 analysis), which found
most of that database's bugs by checking it against PostgreSQL. The other fuzzers
here compare the engine with ITSELF; this is the one with an independent answer.
See `reference_oracle.py` for how outcomes are judged, `reference_grammar.py` for
what is generated, and `reference_known_gaps.py` for the differences already known.

A failing case prints its seed and a shrunk reproduction (both SQL texts plus the
data as VALUES). Pin the seed in `seeds/reference.txt` to keep it in every run.

Runs nightly with the other fuzzers (`.github/workflows/fuzzer.yaml`); the
regression suite ignores tests/fuzzing.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))

import random
import re
from collections import Counter
from pathlib import Path

import pytest

from tests.fuzzing import reference_known_gaps as known_gaps
from tests.fuzzing.harness import case_seeds
from tests.fuzzing.harness import iterations
from tests.fuzzing.reference_grammar import excluded
from tests.fuzzing.reference_grammar import random_query
from tests.fuzzing.reference_oracle import Outcome
from tests.fuzzing.reference_oracle import canonical
from tests.fuzzing.reference_oracle import compare
from tests.fuzzing.reference_oracle import judge
from tests.fuzzing.reference_oracle import report
from tests.fuzzing.reference_oracle import run_opteryx
from tests.fuzzing.reference_oracle import shrink

DEFAULT_ITERATIONS = 200
SEEDS = case_seeds("reference", iterations(DEFAULT_ITERATIONS))

HITS: Counter = Counter()


@pytest.mark.parametrize("seed", SEEDS)
def test_reference_fuzzing(seed):
    print(f"\nSeed: {seed}")
    query = random_query(random.Random(seed))

    withheld = excluded(query)
    if withheld is not None:
        HITS[f"withheld:{withheld}"] += 1
        return

    try:
        verdict, opteryx_sql, _ = compare(query)
    except Exception as error:  # a crash: absorbed only by a registered CRASH entry
        gap = known_gaps.match_crash(error)
        if gap is None:
            raise
        HITS[gap.id] += 1
        print(f"  known crash: {gap.id}")
        return
    print(opteryx_sql)
    HITS[verdict.kind] += 1
    if verdict.agrees:
        return

    if verdict.kind != "wrong_answer":
        gap = known_gaps.match_verdict(verdict)
        if gap is not None:
            HITS[gap.id] += 1
            print(f"  known gap: {gap.id}")
            return

    shrunk, shrunk_verdict, executions = shrink(query, verdict)
    pytest.fail(
        f"seed {seed}: Opteryx and DuckDB disagree (shrunk in {executions} executions)\n"
        f"{report(shrunk, shrunk_verdict)}",
        pytrace=False,
    )


@pytest.mark.parametrize("gap", known_gaps.REGISTER, ids=lambda g: g.id)
def test_registered_gap_still_reproduces(gap):
    """Each entry's repro must still produce the recorded difference. When this
    fails the defect is fixed (or has changed): delete the entry, and for a
    WITHHELD entry remove the exclusion that names it."""
    if gap.kind == known_gaps.WITHHELD and gap.query is not None:
        verdict, opteryx_sql, duckdb_sql = compare(gap.query())
        assert gap.still_present(verdict), (
            f"{gap.id} no longer reproduces: {verdict.kind}\n  {opteryx_sql}\n  {duckdb_sql}\n"
            f"  opteryx: {verdict.opteryx.describe()}  reference: {verdict.reference.describe()}"
        )
        return
    if gap.kind == known_gaps.WITHHELD:
        outcome = run_opteryx(gap.sql)
        assert gap.still_present(outcome.rows, outcome.error), (
            f"{gap.id} no longer reproduces: {gap.sql}\n  -> {outcome.describe()} {outcome.rows!r}"
        )
        return
    if gap.kind == known_gaps.CRASH:
        with pytest.raises(Exception) as raised:
            compare(gap.query())
        assert known_gaps.match_crash(raised.value) is gap, f"{gap.id} now raises {raised.value!r}"
        return
    verdict, opteryx_sql, duckdb_sql = compare(gap.query())
    assert known_gaps.match_verdict(verdict) is gap, (
        f"{gap.id} no longer reproduces: {verdict.kind}\n  {opteryx_sql}\n  {duckdb_sql}\n"
        f"  opteryx: {verdict.opteryx.describe()}  reference: {verdict.reference.describe()}"
    )


def test_withheld_shapes_cite_live_entries():
    """Every gap id the generator names at an exclusion site is a live entry, so a
    deleted entry cannot leave its exclusion silently in force."""
    source = (Path(__file__).parent / "reference_grammar.py").read_text()
    cited = set(re.findall(r'"([a-z0-9]+(?:-[a-z0-9]+){2,})"', source))
    assert cited, "found no cited gap ids in reference_grammar.py"
    missing = cited - set(known_gaps.BY_ID)
    assert not missing, f"reference_grammar.py cites gap ids with no register entry: {missing}"


def test_oracle_is_falsifiable():
    """The comparison must be able to fail: different values, a missing row and a
    duplicated row all count as wrong answers; equal values in different Python
    types do not."""
    from tests.fuzzing.reference_grammar import Lit, Query, Ty

    query = Query(tables=(), items=(Lit(Ty.INT, 1),))
    assert judge(Outcome([(1,)], None), Outcome([(2,)], None), query).kind == "wrong_answer"
    assert judge(Outcome([(1,)], None), Outcome([(1,), (1,)], None), query).kind == "wrong_answer"
    assert judge(Outcome([], None), Outcome([(1,)], None), query).kind == "wrong_answer"
    assert judge(Outcome([(1,)], None), Outcome([(1,)], None), query).agrees
    import decimal

    assert canonical(decimal.Decimal("1.50"), 15) == canonical(1.5, 15)
    assert canonical(-0.0, 15) == canonical(0, 15)
    assert canonical(0.1 + 0.2, 15) != canonical(0.3 + 1e-12, 15)


def test_zz_fuzzing_actually_ran():
    """Runs last: the fuzz cases must have compared something, and the per-entry
    hit counts are printed so a class that suddenly gets louder is visible."""
    print("\n" + "\n".join(f"  {key}: {count}" for key, count in sorted(HITS.items())))
    assert HITS["agree"] > 0, f"no case reached an agreeing comparison: {dict(HITS)}"


if __name__ == "__main__":  # pragma: no cover
    for _seed in SEEDS:
        test_reference_fuzzing(_seed)
    for _gap in known_gaps.REGISTER:
        test_registered_gap_still_reproduces(_gap)
    test_withheld_shapes_cite_live_entries()
    test_oracle_is_falsifiable()
    test_zz_fuzzing_actually_ran()
    print("\n✅ okay")
