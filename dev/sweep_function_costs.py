#!/usr/bin/env python3
"""
Crash-resilient driver for estimate_function_costs.py.

A handful of kernels can't be driven by the estimator's generic inputs and
*segfault* in native code rather than raising (e.g. BASE64_DECODE). An in-process
segfault can't be caught and would abort the whole sweep, losing every result.

This driver runs the estimator in child processes, in batches. If a batch's
subprocess dies (non-zero exit), it re-runs that batch one function at a time to
isolate the crasher, records it under "crashed", and keeps going. The surviving
results from every batch are merged into a single output file with the same shape
estimate_function_costs.py produces, plus a "crashed" list.

Usage (run from dev/):
    python sweep_function_costs.py --output function_costs.json
"""

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(1, os.path.join(sys.path[0], ".."))

from opteryx.expression.functions import get_catalog

HERE = Path(__file__).resolve().parent
ESTIMATOR = HERE / "estimate_function_costs.py"


def run_estimator(funcs, common) -> tuple[dict, bool]:
    """Run the estimator on `funcs` in a child process.

    Returns (functions_dict, crashed). `crashed` is True if the subprocess died
    (segfault / non-zero exit) — in which case its results are lost.
    """
    fd, out_path = tempfile.mkstemp(suffix=".json")
    os.close(fd)
    try:
        proc = subprocess.run(
            [sys.executable, str(ESTIMATOR), "--functions", ",".join(funcs),
             "--output", out_path, *common],
            capture_output=True, text=True,
        )
        if proc.returncode != 0:
            return {}, True
        data = json.loads(Path(out_path).read_text())
        return data.get("functions", {}), False
    finally:
        if os.path.exists(out_path):
            os.unlink(out_path)


def main() -> int:
    parser = argparse.ArgumentParser(description="Crash-resilient function cost sweep.")
    parser.add_argument("--output", type=Path, default=Path("function_costs.json"))
    parser.add_argument("--sample-size", type=int, default=250_000)
    parser.add_argument("--budget", type=float, default=0.15)
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--batch", type=int, default=15)
    parser.add_argument("--seed", type=int, default=1234)
    args = parser.parse_args()

    common = [
        "--sample-size", str(args.sample_size),
        "--budget", str(args.budget),
        "--reps", str(args.reps),
        "--seed", str(args.seed),
    ]

    names = sorted({d.name for d in get_catalog().list_functions(include_deprecated=True)})
    print(f"sweeping {len(names)} function(s) in batches of {args.batch}\n")

    merged: dict = {}
    crashed: list = []
    batches = [names[i:i + args.batch] for i in range(0, len(names), args.batch)]

    for bi, batch in enumerate(batches, 1):
        funcs, died = run_estimator(batch, common)
        if not died:
            merged.update(funcs)
            print(f"  batch {bi}/{len(batches)} ok ({len(batch)} fns)")
            continue
        # A subprocess crashed somewhere in this batch — isolate one at a time.
        print(f"  batch {bi}/{len(batches)} CRASHED — isolating {len(batch)} fns")
        for fn in batch:
            one, died1 = run_estimator([fn], common)
            if died1:
                crashed.append(fn)
                print(f"    CRASH: {fn} (segfault — excluded)")
            else:
                merged.update(one)

    measured = sum(1 for ovs in merged.values() for o in ovs if o["success"])
    failed = sum(1 for ovs in merged.values() for o in ovs if not o["success"])

    payload = {
        "method": "marginal per-row cost via bytecode evaluator (func - identity baseline)",
        "driver": "sweep_function_costs.py (subprocess-isolated)",
        "timestamp": time.time(),
        "sample_size": args.sample_size,
        "measured_kernels": measured,
        "failed_kernels": failed,
        "crashed_functions": sorted(crashed),
        "functions": merged,
    }
    args.output.write_text(json.dumps(payload, indent=2))

    print("\n" + "=" * 64)
    print(f"measured {measured} kernel(s), {failed} failed, {len(crashed)} crashed")
    if crashed:
        print("crashed (segfault, excluded): " + ", ".join(sorted(crashed)))
    print(f"written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
