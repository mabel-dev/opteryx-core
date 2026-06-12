"""
Remote-IO benchmark harness for the C++ parquet pipeline (WP-2).

Drives `iter_row_groups_ipc` — the execution-time IO path (footer fetch,
row-group pruning, C++ read + decompress + decode + IPC serialize, pool
commit, deserialize to Draken vectors) — against local files and against
dev/throttle_server.py emulating remote blob storage, and records wall
clock plus the pipeline's diagnostic counters per run.

Dev tooling only. PyArrow is used here solely to GENERATE test data
(sanctioned for dev/), never on the path being measured.

Usage:
    python dev/bench_remote_io.py                    # all profiles, 3 repeats
    python dev/bench_remote_io.py --profiles local remote --repeats 5
    python dev/bench_remote_io.py --out dev/bench_results/baseline.json

Profiles (bandwidth is per connection; aggregate = workers x bandwidth):
    local    direct file path, no server (NVMe regression guard)
    lan      via throttle server, unthrottled (server-overhead floor)
    remote   rtt=50ms,  bw=100 Mbps/conn  (healthy object store)
    hostile  rtt=150ms, bw=50 Mbps/conn   (degraded object store)

Output: JSON with per-scenario, per-profile median wall clock, row counts
(asserted identical across profiles — correctness guard), and the
OPTERYX_IO_DIAG_JSON counters (http_request_count, latency histogram,
worker_blocked_ns, deserialize_ns, ipc bytes serialized/committed).
"""

import argparse
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time

sys.path.insert(1, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_data")
DATA_FILE = os.path.join(DATA_DIR, "remote_io_bench.parquet")
RESULTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_results")

ROW_GROUPS = 8
ROWS_PER_GROUP = 250_000
LOW_CARD_VALUES = [f"category_{i:02d}" for i in range(16)]

PROFILES = {
    "local": None,
    "lan": {"rtt_ms": 0, "bandwidth_mbps": 0},
    "remote": {"rtt_ms": 50, "bandwidth_mbps": 100},
    "hostile": {"rtt_ms": 150, "bandwidth_mbps": 50},
}

ALL_COLUMNS = ["id", "val", "cat", "uid"]

SCENARIOS = [
    {"name": "wide_projection", "columns": ALL_COLUMNS, "predicates": None},
    {"name": "narrow_int", "columns": ["id"], "predicates": None},
    {"name": "strings_low_card", "columns": ["cat"], "predicates": None},
    {"name": "strings_high_card", "columns": ["uid"], "predicates": None},
    # id is sequential, so this prunes all but the last row group via
    # footer min/max stats — measures pruning + footer cost, not data cost.
    {
        "name": "rg_pruned",
        "columns": ["id", "val"],
        "predicates": [("id", "GtEq", (ROW_GROUPS - 1) * ROWS_PER_GROUP)],
    },
]


def generate_dataset():
    """Create the benchmark parquet file if missing (pyarrow, dev-only)."""
    if os.path.isfile(DATA_FILE):
        return
    import random

    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(DATA_DIR, exist_ok=True)
    rng = random.Random(17)
    total = ROW_GROUPS * ROWS_PER_GROUP
    table = pa.table(
        {
            "id": pa.array(range(total), type=pa.int64()),
            "val": pa.array([rng.random() * 1000.0 for _ in range(total)], type=pa.float64()),
            "cat": pa.array([LOW_CARD_VALUES[i % len(LOW_CARD_VALUES)] for i in range(total)]),
            "uid": pa.array([f"{rng.getrandbits(96):024x}" for _ in range(total)]),
        }
    )
    pq.write_table(
        table,
        DATA_FILE,
        row_group_size=ROWS_PER_GROUP,
        compression="zstd",
        use_dictionary=["cat"],
        write_statistics=True,
    )
    sys.stderr.write(f"generated {DATA_FILE} ({os.path.getsize(DATA_FILE)} bytes)\n")


def start_server(profile_cfg, port):
    """Start throttle_server.py as a subprocess; wait for its READY line.

    A subprocess (not a thread) so the server's Python execution never
    competes for this process's GIL while the pipeline is being measured.
    """
    cmd = [
        sys.executable,
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "throttle_server.py"),
        "--root", DATA_DIR,
        "--port", str(port),
        "--rtt-ms", str(profile_cfg["rtt_ms"]),
        "--bandwidth-mbps", str(profile_cfg["bandwidth_mbps"]),
    ]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    ready = proc.stdout.readline()
    if not ready.startswith("READY"):
        proc.kill()
        raise RuntimeError(f"throttle server failed to start: {ready!r}")
    actual_port = int(ready.strip().split("port=")[1])
    return proc, actual_port


def run_scan(path, columns, predicates, decode_workers, diag_path):
    """One measured scan through iter_row_groups_ipc. Returns (wall_s, rows, diag)."""
    from opteryx.connectors.parquet_io.pool_reader import iter_row_groups_ipc

    os.environ["OPTERYX_IO_DIAG_JSON"] = diag_path
    try:
        t0 = time.monotonic()
        rows = 0
        row_groups = 0
        for _scan_rg, row_group in iter_row_groups_ipc(
            None, [path], columns,
            decode_workers=decode_workers,
            predicates=predicates,
            footer_bytes_cache=None,
        ):
            row_groups += 1
            for vec in row_group.values():
                rows += len(vec)
                break  # one column's length is the row count
        wall = time.monotonic() - t0
    finally:
        del os.environ["OPTERYX_IO_DIAG_JSON"]

    diag = None
    if os.path.isfile(diag_path):
        with open(diag_path) as f:
            lines = f.read().strip().splitlines()
        if lines:
            diag = json.loads(lines[-1])
        os.remove(diag_path)
    return wall, rows, row_groups, diag


def main():
    parser = argparse.ArgumentParser(description="Remote-IO benchmark for the parquet pipeline")
    parser.add_argument("--profiles", nargs="+", default=list(PROFILES), choices=list(PROFILES))
    parser.add_argument("--scenarios", nargs="+", default=[s["name"] for s in SCENARIOS])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--decode-workers", type=int, default=4)
    parser.add_argument("--out", default=None, help="output JSON path (default: dev/bench_results/remote_io_<ts>.json)")
    args = parser.parse_args()

    generate_dataset()
    file_name = os.path.basename(DATA_FILE)
    scenarios = [s for s in SCENARIOS if s["name"] in set(args.scenarios)]

    results = []
    expected_rows = {}  # scenario -> rows, asserted identical across profiles

    for profile in args.profiles:
        cfg = PROFILES[profile]
        proc = None
        if cfg is None:
            path = DATA_FILE
        else:
            proc, port = start_server(cfg, 0)
            path = f"http://127.0.0.1:{port}/{file_name}"

        try:
            for scenario in scenarios:
                walls, diags = [], []
                rows = row_groups = None
                for rep in range(args.repeats):
                    diag_path = os.path.join(tempfile.gettempdir(), f"opteryx_io_diag_{os.getpid()}_{rep}.jsonl")
                    wall, rows, row_groups, diag = run_scan(
                        path, scenario["columns"], scenario["predicates"],
                        args.decode_workers, diag_path,
                    )
                    walls.append(wall)
                    if diag is not None:
                        diags.append(diag)

                key = scenario["name"]
                if key in expected_rows and expected_rows[key] != rows:
                    raise RuntimeError(
                        f"row count mismatch for {key}: {profile} returned {rows}, "
                        f"earlier profile returned {expected_rows[key]} — results differ across profiles"
                    )
                expected_rows[key] = rows

                entry = {
                    "profile": profile,
                    "scenario": key,
                    "columns": scenario["columns"],
                    "rows": rows,
                    "row_groups": row_groups,
                    "repeats": args.repeats,
                    "wall_s_median": statistics.median(walls),
                    "wall_s_all": walls,
                    "diag_last": diags[-1] if diags else None,
                }
                results.append(entry)
                sys.stderr.write(
                    f"{profile:8s} {key:20s} median={entry['wall_s_median']:.3f}s "
                    f"rows={rows} rgs={row_groups}\n"
                )
        finally:
            if proc is not None:
                proc.kill()
                proc.wait()

    os.makedirs(RESULTS_DIR, exist_ok=True)
    out_path = args.out or os.path.join(RESULTS_DIR, f"remote_io_{int(time.time())}.json")
    with open(out_path, "w") as f:
        json.dump(
            {
                "decode_workers": args.decode_workers,
                "row_groups": ROW_GROUPS,
                "rows_per_group": ROWS_PER_GROUP,
                "file_bytes": os.path.getsize(DATA_FILE),
                "results": results,
            },
            f,
            indent=2,
        )
    sys.stderr.write(f"results written to {out_path}\n")


if __name__ == "__main__":
    main()
