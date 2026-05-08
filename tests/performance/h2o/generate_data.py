"""
H2O db-benchmark — synthetic data generator (Python port).

Generates the groupby and join fixtures used by the H2O db-benchmark
(https://github.com/duckdblabs/db-benchmark), the de-facto community
benchmark for analytical engines. The upstream `_data/groupby-datagen.R`
and `_data/join-datagen.R` scripts are ported here so we don't need R.

Sizes (per upstream convention; nas=0, sort=0):
    small   N=1e7  K=1e2   ~0.5GB on disk (per groupby x table)
    medium  N=1e8  K=1e2   ~5GB
    large   N=1e9  K=1e2   ~50GB   (opt-in, not validated by `make h2o`)

Output layout (mirrors testdata.<dataset>.<table> resolution):

    testdata/h2o/<size>/x_groupby/x_groupby.parquet   # groupby fixture
    testdata/h2o/<size>/x/x.parquet                   # join LHS
    testdata/h2o/<size>/small/small.parquet           # join RHS
    testdata/h2o/<size>/medium/medium.parquet         # join RHS
    testdata/h2o/<size>/big/big.parquet               # join RHS

Idempotent — files that already exist are skipped.

Dev dependencies
----------------
    numpy    - generation (RNG + arrays)
    pyarrow  - Parquet writer

PyArrow is **banned in Opteryx production code** (`CLAUDE.md` §4) — the
build-time `check_no_pyarrow()` in `setup.py` enforces this. It is
permitted here because:
  - this script is dev tooling under `tests/performance/`, never imported
    by Opteryx,
  - the runner (`run.py`) does not import PyArrow,
  - we previously used DuckDB to write Parquet, but DuckDB emits the
    legacy `converted_type='INT_32'` annotation on INT32 columns and
    omits the modern `logical_type=StringType()` on UTF8 columns, which
    Rugo's schema discovery does not accept (manifests as 0×0 reads).
    PyArrow writes both legacy and modern metadata, producing files that
    Rugo reads cleanly.

Install into your dev venv:

    pip install numpy pyarrow

Usage
-----
    python tests/performance/h2o/generate_data.py --size small
    python tests/performance/h2o/generate_data.py --size medium
    python tests/performance/h2o/generate_data.py --size large    # 50GB; opt-in
    python tests/performance/h2o/generate_data.py --size small --workload groupby
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
DATA_ROOT = ROOT / "testdata" / "h2o"

# Upstream H2O size definitions: N rows, K group-cardinality factor.
# nas=0 (no nulls), sort=0 (random order) — the variant publish runs use.
SIZES = {
    "small":  {"N": 10_000_000,    "K": 100},
    "medium": {"N": 100_000_000,   "K": 100},
    "large":  {"N": 1_000_000_000, "K": 100},
}

# RNG seeds — deterministic so re-runs produce identical bytes.
SEED_GROUPBY = 0xDB0001
SEED_JOIN_X = 0xDB0002
SEED_JOIN_SMALL = 0xDB0003
SEED_JOIN_MEDIUM = 0xDB0004
SEED_JOIN_BIG = 0xDB0005


def _require_deps():
    try:
        import numpy as np  # noqa: F401
    except ImportError:
        sys.exit("numpy is required. Install: pip install numpy")
    try:
        import pyarrow  # noqa: F401
        import pyarrow.parquet  # noqa: F401
    except ImportError:
        sys.exit("pyarrow is required. Install: pip install pyarrow")


def _write_parquet(table_name: str, columns: dict, out_path: Path) -> None:
    """Write a dict of {colname: numpy array} to a Parquet file via PyArrow.

    PyArrow emits both legacy (`converted_type`) and modern (`logical_type`)
    Parquet metadata, which Rugo's schema discovery requires. SNAPPY
    compression matches the existing `testdata/job/*.parquet` fixtures.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = len(next(iter(columns.values())))
    print(f"  [{table_name}] building arrow table ({n:,} rows)")

    arrays = []
    names = []
    for name, arr in columns.items():
        names.append(name)
        k = arr.dtype.kind
        if k in ("U", "S", "O"):
            arrays.append(pa.array(arr, type=pa.string()))
        elif k == "f":
            arrays.append(pa.array(arr, type=pa.float64()))
        elif k in ("i", "u"):
            # Match groupby-datagen.R / join-datagen.R: ints fit in INT32.
            pa_type = pa.int64() if arr.dtype.itemsize >= 8 else pa.int32()
            arrays.append(pa.array(arr, type=pa_type))
        else:
            arrays.append(pa.array(arr))

    table = pa.Table.from_arrays(arrays, names=names)
    print(f"  [{table_name}] -> {out_path.relative_to(ROOT)}")
    pq.write_table(table, out_path.as_posix(), compression="snappy")


def _gen_groupby(N: int, K: int, seed: int) -> dict:
    """Port of `_data/groupby-datagen.R` (no NAs, no sort).

    Columns:
        id1, id2 : str  — sample("id001"..f"id{K:03d}")
        id3      : str  — sample("id0000000001"..) cardinality N/K
        id4, id5 : int  — 1..K
        id6      : int  — 1..N/K
        v1       : int  — 1..5
        v2       : int  — 1..15
        v3       : f64  — runif(0,100), rounded to 6dp
    """
    import numpy as np

    rng = np.random.default_rng(seed)

    id_str_K = np.array([f"id{i:03d}" for i in range(1, K + 1)])
    NK = N // K
    id_str_NK = np.array([f"id{i:010d}" for i in range(1, NK + 1)])

    cols = {
        "id1": id_str_K[rng.integers(0, K, size=N)],
        "id2": id_str_K[rng.integers(0, K, size=N)],
        "id3": id_str_NK[rng.integers(0, NK, size=N)],
        "id4": (rng.integers(0, K, size=N) + 1).astype(np.int32),
        "id5": (rng.integers(0, K, size=N) + 1).astype(np.int32),
        "id6": (rng.integers(0, NK, size=N) + 1).astype(np.int32),
        "v1":  (rng.integers(0, 5, size=N) + 1).astype(np.int32),
        "v2":  (rng.integers(0, 15, size=N) + 1).astype(np.int32),
        "v3":  np.round(rng.uniform(0.0, 100.0, size=N), 6),
    }
    return cols


def _gen_join_x(N: int, K: int, seed: int) -> dict:
    """Port of `_data/join-datagen.R` (LHS x table, no NAs).

    Distinct schema from groupby: ints first, strings later.
        id1..id3 : int
        id4..id6 : str
        v1       : f64
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    NK = N // K

    cols = {
        "id1": (rng.integers(0, K, size=N) + 1).astype(np.int32),
        "id2": (rng.integers(0, K, size=N) + 1).astype(np.int32),
        "id3": (rng.integers(0, NK, size=N) + 1).astype(np.int32),
        "id4": np.array([f"id{i:03d}" for i in range(1, K + 1)])[
            rng.integers(0, K, size=N)
        ],
        "id5": np.array([f"id{i:03d}" for i in range(1, K + 1)])[
            rng.integers(0, K, size=N)
        ],
        "id6": np.array([f"id{i:010d}" for i in range(1, NK + 1)])[
            rng.integers(0, NK, size=N)
        ],
        "v1":  np.round(rng.uniform(0.0, 100.0, size=N), 6),
    }
    return cols


def _gen_join_small(K: int, seed: int) -> dict:
    """Small RHS: K rows (e.g. 100). Joined on id1 (int)."""
    import numpy as np
    rng = np.random.default_rng(seed)
    return {
        "id1": np.arange(1, K + 1, dtype=np.int32),
        "id4": np.array([f"id{i:03d}" for i in range(1, K + 1)]),
        "v2":  np.round(rng.uniform(0.0, 100.0, size=K), 6),
    }


def _gen_join_medium(K: int, seed: int) -> dict:
    """Medium RHS: K*K rows. Carries id1, id2, id4, id5 + v2."""
    import numpy as np
    rng = np.random.default_rng(seed)
    n = K * K
    cols = {
        "id1": (rng.integers(0, K, size=n) + 1).astype(np.int32),
        "id2": (rng.integers(0, K, size=n) + 1).astype(np.int32),
        "id4": np.array([f"id{i:03d}" for i in range(1, K + 1)])[
            rng.integers(0, K, size=n)
        ],
        "id5": np.array([f"id{i:03d}" for i in range(1, K + 1)])[
            rng.integers(0, K, size=n)
        ],
        "v2":  np.round(rng.uniform(0.0, 100.0, size=n), 6),
    }
    return cols


def _gen_join_big(N: int, K: int, seed: int) -> dict:
    """Big RHS: same row count as x. Joined on id3 (int, NK cardinality).

    Carries every join column so j5 can project them all.
    """
    import numpy as np
    rng = np.random.default_rng(seed)
    NK = N // K

    cols = {
        "id1": (rng.integers(0, K, size=N) + 1).astype(np.int32),
        "id2": (rng.integers(0, K, size=N) + 1).astype(np.int32),
        "id3": (rng.integers(0, NK, size=N) + 1).astype(np.int32),
        "id4": np.array([f"id{i:03d}" for i in range(1, K + 1)])[
            rng.integers(0, K, size=N)
        ],
        "id5": np.array([f"id{i:03d}" for i in range(1, K + 1)])[
            rng.integers(0, K, size=N)
        ],
        "id6": np.array([f"id{i:010d}" for i in range(1, NK + 1)])[
            rng.integers(0, NK, size=N)
        ],
        "v2":  np.round(rng.uniform(0.0, 100.0, size=N), 6),
    }
    return cols


def generate(size: str, workload: str) -> None:
    if size not in SIZES:
        sys.exit(f"unknown size {size!r} (expected one of {list(SIZES)})")
    spec = SIZES[size]
    N, K = spec["N"], spec["K"]
    base = DATA_ROOT / size

    print(f"[h2o] size={size}  N={N:,}  K={K}  workload={workload}")
    print(f"[h2o] output: {base.relative_to(ROOT)}/")

    # Upstream uses different schemas for groupby-x vs join-x; we ship them
    # as two distinct directories so each query references a single `x` per
    # workload. The runner translates `x` -> `testdata.h2o.<size>.x` for
    # joins and `x` -> `testdata.h2o.<size>.x_groupby` for groupby queries.

    if workload in ("groupby", "both"):
        path = base / "x_groupby" / "x_groupby.parquet"
        if path.exists():
            print(f"  [x_groupby] already present, skipping")
        else:
            print("  [x_groupby] generating")
            cols = _gen_groupby(N, K, SEED_GROUPBY)
            _write_parquet("x_groupby", cols, path)

    if workload in ("join", "both"):
        for table, gen in [
            ("x",      lambda: _gen_join_x(N, K, SEED_JOIN_X)),
            ("small",  lambda: _gen_join_small(K, SEED_JOIN_SMALL)),
            ("medium", lambda: _gen_join_medium(K, SEED_JOIN_MEDIUM)),
            ("big",    lambda: _gen_join_big(N, K, SEED_JOIN_BIG)),
        ]:
            path = base / table / f"{table}.parquet"
            if path.exists():
                print(f"  [{table}] already present, skipping")
                continue
            print(f"  [{table}] generating")
            cols = gen()
            _write_parquet(table, cols, path)

    print(f"[h2o] done. Run `make h2o` to execute the benchmark.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--size",
        choices=list(SIZES.keys()),
        default="small",
        help="data scale (default: small ~0.5GB)",
    )
    parser.add_argument(
        "--workload",
        choices=["groupby", "join", "both"],
        default="both",
        help="which fixtures to generate (default: both)",
    )
    args = parser.parse_args()

    _require_deps()
    if args.size == "large":
        print("[h2o] WARNING: 'large' generates ~50GB; not validated by make h2o")

    generate(args.size, args.workload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
