"""ClickBench worker-count sweep — re-measure after the engine code-removal.

Runs three single-column COUNT(*) GROUP BY queries through the full engine
(execute_to_morsels) at a range of MAX_EXECUTION_WORKERS values. The W=1 column
is the serial engine baseline — the number that moves if the global regression
was fixed by removing code. Prior (regressed) serials: Q08 ~110ms, Q13 ~888ms,
Q16 ~1452ms.
"""

import gc
import os
import sys
import time

sys.path.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import opteryx
from opteryx import config
from opteryx.managers.execution import parallel_engine

parallel_engine._MAX_WORKER_CAP = 64  # lift cap so 10..16 are reachable

DS = "scratch.hits"
QUERIES = {
    "Q08 low-card  (AdvEngineID)": f"SELECT AdvEngineID, COUNT(*) FROM {DS} WHERE AdvEngineID <> 0 GROUP BY AdvEngineID ORDER BY COUNT(*) DESC;",
    "Q13 med-card  (SearchPhrase)": f"SELECT SearchPhrase, COUNT(*) AS c FROM {DS} WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10;",
    "Q16 high-card (UserID)": f"SELECT UserID, COUNT(*) FROM {DS} GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10;",
}
WORKERS = [1, 2, 4, 8]
ITERS = 3


def run_once(sql: str) -> None:
    session = opteryx.session()
    try:
        for _ in session.execute_to_morsels(sql):
            pass
        return session._telemetry._reading.get("native_engine_dop")
    finally:
        session.close()


def best_ms(sql: str, expect_dop: int) -> float:
    """Fastest of ITERS runs, having verified the engine ran at `expect_dop`.

    The check is the point, not ceremony: `config.MAX_EXECUTION_WORKERS` was for a
    long time frozen at import by the system-variable table, so this sweep set W in
    a loop and measured ONE width — printing a W=1..N table and a "speedup vs W=1"
    column derived from N identical timings. A sweep that cannot detect that is
    worse than no sweep, because it reports a scaling curve that never happened.
    """
    samples = []
    for _ in range(ITERS):
        gc.collect()
        start = time.monotonic_ns()
        dop = run_once(sql)
        samples.append((time.monotonic_ns() - start) / 1e6)
        if dop != expect_dop:
            raise SystemExit(
                f"W={expect_dop} was requested but the engine ran at dop={dop} — "
                f"the worker setting is not reaching the engine, so every column "
                f"of this sweep would be the same width."
            )
    return min(samples)


print(f"cpu_count={os.cpu_count()}  GIL_enabled={sys._is_gil_enabled()}")
print(f"dataset={DS}  iterations={ITERS} (reporting min)\n")
print("warming...", flush=True)
run_once(f"SELECT COUNT(*) FROM {DS};")

results = {}
for qname, sql in QUERIES.items():
    for w in WORKERS:
        config.MAX_EXECUTION_WORKERS = w
        results[(qname, w)] = best_ms(sql, w)
        print(f"  {qname:<30} W={w:<2} -> {results[(qname, w)]:8.1f} ms", flush=True)

print("\n" + "=" * 80)
header = f"{'query':<30} " + " ".join(f"W={w:>2}" for w in WORKERS)
print(header)
print("-" * len(header))
for qname in QUERIES:
    row = f"{qname:<30} " + " ".join(f"{results[(qname, w)]:5.0f}" for w in WORKERS)
    print(row)
print("\nspeedup vs W=1:")
for qname in QUERIES:
    base = results[(qname, 1)]
    row = f"{qname:<30} " + " ".join(f"{base / results[(qname, w)]:4.2f}x" for w in WORKERS)
    print(row)
