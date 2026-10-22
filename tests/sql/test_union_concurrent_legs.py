"""UNION ALL legs run at the same time, and the answer is the one they gave one after another.

The compiler declares each union's legs as independent pipeline ranges
(`add_concurrent_span`); the engine runs a span's legs on their own driver threads against
the shared worker pool, waiting on each pipeline's own completion latch. What that must
not change is pinned here:

* the rows themselves, with legs that are several pipelines long (a join, an aggregate);
* a shared CTE read by several legs at once is freed only after the LAST of them;
* a nested union, an erroring leg, an outer LIMIT that abandons the query while legs are
  running, and several sessions doing all of this at once.

Row ORDER is deliberately not asserted here: the union's consumer pipeline runs many workers
that race into the next stage, so the order of the final output was never deterministic -
before or after legs ran concurrently. What the per-leg buffers do guarantee, and what
`make pipeline-latch-test` proves at the C++ level, is that the union's buffer is read in leg
order. The same test proves the pipeline latch itself: a pipeline waits for its own workers,
not the pool. Expected values are computed in Python from
the raw rows.

Run as a script (CLAUDE.md §10) or under pytest.
"""

import os
import signal
import sys
import threading

sys.path.insert(1, os.path.join(os.path.dirname(__file__), "..", ".."))

import pytest

import opteryx


@pytest.fixture(autouse=True)
def fail_instead_of_hanging():
    """A deadlock in the leg scheduler must be a failing test, not a hung run."""
    signal.signal(signal.SIGALRM, lambda *_: pytest.fail("the query did not finish in 120 s (deadlock?)"))
    signal.alarm(120)
    yield
    signal.alarm(0)


def run(statement):
    """Every row of `statement`, as tuples in column order."""
    session = opteryx.session()
    out = []
    for morsel in session.execute_to_morsels(statement):
        morsel.materialize()
        names = list(morsel.column_names)
        out.extend(zip(*(morsel.column(name).to_pylist() for name in names)))
    return out


MOONS = "SELECT p.name AS k, COUNT(*) AS n FROM $planets AS p INNER JOIN testdata.satellites AS s ON s.planetId = p.id GROUP BY p.name"
SELF = "SELECT p.name AS k, COUNT(*) AS n FROM $planets AS p INNER JOIN $planets AS q ON p.id = q.id GROUP BY p.name"

MOON_COUNTS = dict(run(MOONS))
NAMES = [name for (name,) in run("SELECT name FROM $planets")]
THREE_LEGS = f"SELECT 1 AS leg, k, n FROM ({MOONS}) a UNION ALL SELECT 2, k, n FROM ({SELF}) b UNION ALL SELECT 3, name, id FROM $planets"


def expected_three_legs():
    ids = dict(run("SELECT name, id FROM $planets"))
    return (
        [(1, k, n) for k, n in MOON_COUNTS.items()]
        + [(2, name, 1) for name in NAMES]
        + [(3, name, ids[name]) for name in NAMES]
    )


def test_legs_made_of_several_pipelines_give_the_same_rows():
    assert sorted(run(THREE_LEGS)) == sorted(expected_three_legs())


def test_a_nested_union_gives_every_leg_its_rows():
    statement = (
        f"SELECT 1 AS leg, k FROM ({MOONS}) a UNION ALL "
        f"(SELECT 2, k FROM ({SELF}) b UNION ALL SELECT 3, name FROM $planets)"
    )
    for _ in range(30):
        assert sorted(run(statement)) == sorted(
            [(1, k) for k in MOON_COUNTS] + [(2, name) for name in NAMES] + [(3, name) for name in NAMES]
        )


def test_a_shared_cte_read_by_every_leg_is_freed_after_the_last_reader():
    statement = (
        f"WITH c AS ({MOONS}) "
        "SELECT 1 AS leg, k, n FROM c UNION ALL SELECT 2, k, n FROM c UNION ALL SELECT 3, k, n FROM c"
    )
    expected = sorted((leg, k, n) for leg in (1, 2, 3) for k, n in MOON_COUNTS.items())
    for _ in range(60):
        assert sorted(run(statement)) == expected


def test_an_erroring_leg_raises_and_leaves_the_engine_usable():
    failing = f"SELECT 1 AS leg, k FROM ({MOONS}) a UNION ALL SELECT 2, CAST(name AS INTEGER) FROM $planets"
    for _ in range(8):
        with pytest.raises(Exception) as raised:
            run(failing)
        assert "whole number" in str(raised.value), raised.value
        assert sorted(run(THREE_LEGS)) == sorted(expected_three_legs())


def test_a_limit_that_abandons_the_query_while_legs_run_does_not_hang_or_lose_rows():
    statement = f"SELECT leg, k FROM ({THREE_LEGS}) u LIMIT 3"
    for _ in range(40):
        assert len(run(statement)) == 3


def test_sessions_on_several_threads_each_get_the_right_answer():
    expected = sorted(expected_three_legs())
    failures = []

    def worker():
        try:
            for _ in range(15):
                if sorted(run(THREE_LEGS)) != expected:
                    failures.append("wrong rows")
        except Exception as error:  # a thread's failure must reach the assertion below
            failures.append(repr(error))

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert failures == []


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    run_tests()
