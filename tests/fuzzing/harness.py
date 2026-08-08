"""
Shared harness for the SQL fuzzers.

Three things live here, because getting any of them wrong silently disarms a
fuzzer without making it go red:

* **Seeding.** A fuzzer seeded from its own loop index generates the identical
  corpus on every run, forever. That is a fixed regression suite wearing a
  fuzzer's name — it can only find a bug on the first run after the change that
  introduced it, and never explores anything new. Seeds here are random by
  default, printed on every case, and overridable for reproduction.

* **Result materialisation.** Collecting rows into a `set` discards duplicates,
  which makes every multiplicity bug invisible and renders subset oracles
  unfalsifiable. `result_multiset` returns a sorted *list*, so duplicates count.

* **Not swallowing errors.** A `run_query` that catches every exception and
  returns `None`, combined with an oracle that treats `None` as "passed",
  produces permanent green while executing nothing. Nothing here catches.
"""

import os
import random
from pathlib import Path
from typing import Iterator, List, Tuple

import opteryx

_SEED_DIR = Path(__file__).parent / "seeds"


def iterations(default: int) -> int:
    """Case count for this run. `TEST_ITERATIONS` overrides the fuzzer's default.

    Short in CI, long on the nightly schedule (see .github/workflows/fuzzer.yaml).
    Each fuzzer passes its own default, which must keep the regression suite fast.
    """
    return int(os.environ.get("TEST_ITERATIONS", default))


def base_seed() -> int:
    """Root seed for this run.

    Random unless `TEST_SEED` pins it. The caller must print the derived seed
    for every case — the seed is the only reproduction handle a fuzzer has.
    """
    pinned = os.environ.get("TEST_SEED")
    if pinned is not None:
        return int(pinned)
    return int.from_bytes(os.urandom(8), "big")


def regression_seeds(name: str) -> List[int]:
    """Seeds pinned in `seeds/<name>.txt`, run before the random ones.

    When a random seed finds a bug, add it to that file. It then runs on every
    future invocation regardless of `TEST_SEED`, so the bug cannot come back
    unnoticed once fixed.
    """
    path = _SEED_DIR / f"{name}.txt"
    if not path.exists():
        return []
    seeds = []
    for line in path.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            seeds.append(int(line))
    return seeds


def rows(sql: str) -> Iterator[Tuple]:
    """Execute `sql` and yield each row as a tuple of Python values.

    Reads the morsels directly rather than going through `tests.helpers`, whose
    accessors convert to arrow first. The oracle has to compare what the engine
    produced, not what the arrow bridge made of it — and routing every fuzzed
    query through a conversion layer would attribute that layer's failures to
    the query.
    """
    session = opteryx.session()
    for morsel in session.execute_to_morsels(sql):
        for i in range(len(morsel)):
            yield morsel[i]


def result_multiset(sql: str) -> List[str]:
    """Execute `sql` and return its rows as an order-insensitive multiset.

    Rows are rendered positionally, so two queries that compute the same values
    under different column *names* compare equal — which is what a metamorphic
    oracle wants. Duplicates are preserved: the sorted list is a multiset, not a
    set, so a query that emits a row twice does not compare equal to one that
    emits it once.
    """
    return sorted(repr(row) for row in rows(sql))


def scalar(sql: str):
    """Execute `sql` and return the single value of its single row."""
    result = list(rows(sql))
    if len(result) != 1 or len(result[0]) != 1:
        raise AssertionError(f"expected a single scalar from {sql!r}, got {result!r}")
    return result[0][0]


def case_seeds(name: str, count: int) -> List[int]:
    """The seeds for one fuzzing run: pinned regressions first, then random ones."""
    pinned = regression_seeds(name)
    root = base_seed()
    rng = random.Random(root)
    return pinned + [rng.getrandbits(63) for _ in range(count)]
