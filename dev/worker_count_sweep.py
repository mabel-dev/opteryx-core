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
    finally:
        session.close()


def best_ms(sql: str) -> float:
    samples = []
    for _ in range(ITERS):
        gc.collect()
        start = time.monotonic_ns()
        run_once(sql)
        samples.append((time.monotonic_ns() - start) / 1e6)
    return min(samples)


print(f"cpu_count={os.cpu_count()}  GIL_enabled={sys._is_gil_enabled()}")
print(f"dataset={DS}  iterations={ITERS} (reporting min)\n")
print("warming...", flush=True)
run_once(f"SELECT COUNT(*) FROM {DS};")

results = {}
for qname, sql in QUERIES.items():
    for w in WORKERS:
        config.MAX_EXECUTION_WORKERS = w
        results[(qname, w)] = best_ms(sql)
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
