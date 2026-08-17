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
    numpy    - generation (fast RNG + array ops); explicitly permitted for
               tests/dev use by `CLAUDE.md` §4. Not required by the writer —
               only used to generate values quickly before they're handed
               to Draken as plain Python lists.

Parquet is written via Rugo's own native writer (`rugo.parquet.write_parquet`)
— no PyArrow. We previously used PyArrow because DuckDB's writer emitted the
legacy `converted_type='INT_32'` annotation on INT32 columns and omitted the
modern `logical_type=StringType()` on UTF8 columns, which Rugo's schema
discovery didn't accept (manifests as 0×0 reads). Rugo's own writer emits
metadata its own reader parses cleanly (see
`tests/rugo/test_native_parquet_writer.py::test_rugo_can_parse_own_footer`),
so that workaround is gone.

Install into your dev venv:

    pip install numpy

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
# join-datagen.R uses a single set.seed(108) for x + all three RHS tables,
# since they share one key space — matched here with one seed for all four.
SEED_JOIN = 0xDB0002


def _require_deps():
    try:
        import numpy as np  # noqa: F401
    except ImportError:
        sys.exit("numpy is required. Install: pip install numpy")


def _write_parquet(table_name: str, columns: dict, out_path: Path) -> None:
    """Write a dict of {colname: numpy array} to a Parquet file via Rugo's
    own native writer — no PyArrow (see module docstring)."""
    from draken.draken_native import DrakenType
    from draken.interop.vector_sequence import vector_from_sequence
    from draken.morsels.morsel import Morsel
    from rugo.parquet import write_parquet

    out_path.parent.mkdir(parents=True, exist_ok=True)
    n = len(next(iter(columns.values())))
    print(f"  [{table_name}] building morsel ({n:,} rows)")

    names = []
    vectors = []
    for name, arr in columns.items():
        names.append(name.encode())
        k = arr.dtype.kind
        if k in ("U", "S", "O"):
            dtype = DrakenType.VARCHAR
        elif k == "f":
            dtype = DrakenType.FLOAT64
        elif k in ("i", "u"):
            # Match groupby-datagen.R / join-datagen.R: ints fit in INT32.
            dtype = DrakenType.INT64 if arr.dtype.itemsize >= 8 else DrakenType.INT32
        else:
            raise TypeError(f"{table_name}.{name}: unsupported numpy dtype {arr.dtype!r}")
        # .tolist() coerces numpy scalars (np.int32, np.str_, ...) to native
        # Python int/float/str — the type vector_from_sequence's nanobind
        # constructors expect.
        vectors.append(vector_from_sequence(arr.tolist(), dtype))

    morsel = Morsel.from_vectors(names, vectors)
    print(f"  [{table_name}] -> {out_path.relative_to(ROOT)}")
    data = write_parquet(morsel)
    out_path.write_bytes(data)


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


def _split_xlr(n: float, rng) -> dict:
    """Port of upstream `split_xlr(n)`.

    A random permutation of 1..round(n*1.1), split into three disjoint
    slices: `x` (90%, shared by LHS and RHS), `l` (10%, LHS-only — these
    keys never appear in any RHS table, giving INNER vs LEFT joins a real
    difference), `r` (10%, RHS-only — never appear in the LHS).
    """
    import numpy as np

    total = int(round(n * 1.1))
    n90 = int(round(n * 0.9))
    n = int(round(n))
    key = rng.permutation(total) + 1
    return {"x": key[:n90], "l": key[n90:n], "r": key[n:total]}


def _sample_all(x, size: int, rng):
    """Port of upstream `sample_all(x, size)`.

    Every value in `x` appears at least once; padded to `size` by sampling
    `x` with replacement, then the whole thing is shuffled. When
    `size == len(x)` this degenerates to an exact random permutation of
    `x` (used for the RHS tables' designated join-key column, which must
    be unique).
    """
    import numpy as np

    pad_n = size - len(x)
    if pad_n > 0:
        x = np.concatenate([x, rng.choice(x, size=pad_n, replace=True)])
    return rng.permutation(x)


def _id_lookup(domain_size: int):
    """`f"id{i}"` for i in 1..domain_size (upstream `sprintf("id%.0f", i)` — no
    zero-padding, unlike the groupby id1/id2/id3 strings)."""
    import numpy as np

    return np.char.add("id", np.arange(1, domain_size + 1).astype(str))


def _gen_join_tables(N: int, seed: int) -> dict:
    """Port of `_data/join-datagen.R` end to end.

    Generates LHS `x` and all three RHS tables (`small`, `medium`, `big`)
    from one shared key space, matching upstream cardinalities exactly:
        id1 domain = N/1e6   id2 domain = N/1e3   id3 domain = N
    Each RHS table's *designated* join key (small.id1, medium.id2,
    big.id3) is an exact unique permutation of its domain — a proper
    foreign key — everything else is sampled with replacement, same as
    upstream. id4/id5/id6 are string mirrors of id1/id2/id3 (not an
    independent low-cardinality column), also matching upstream.
    """
    import numpy as np

    rng = np.random.default_rng(seed)
    key1 = _split_xlr(N / 1e6, rng)
    key2 = _split_xlr(N / 1e3, rng)
    key3 = _split_xlr(N, rng)

    lookup1 = _id_lookup(len(key1["x"]) + len(key1["l"]) + len(key1["r"]))
    lookup2 = _id_lookup(len(key2["x"]) + len(key2["l"]) + len(key2["r"]))
    lookup3 = _id_lookup(len(key3["x"]) + len(key3["l"]) + len(key3["r"]))

    # LHS: x
    lhs_keys1 = np.concatenate([key1["x"], key1["l"]])
    lhs_keys2 = np.concatenate([key2["x"], key2["l"]])
    lhs_keys3 = np.concatenate([key3["x"], key3["l"]])
    id1 = _sample_all(lhs_keys1, N, rng).astype(np.int32)
    id2 = _sample_all(lhs_keys2, N, rng).astype(np.int32)
    id3 = _sample_all(lhs_keys3, N, rng).astype(np.int32)
    x_cols = {
        "id1": id1,
        "id2": id2,
        "id3": id3,
        "id4": lookup1[id1 - 1],
        "id5": lookup2[id2 - 1],
        "id6": lookup3[id3 - 1],
        "v1":  np.round(rng.uniform(0.0, 100.0, size=N), 6),
    }

    # RHS: small — n=N/1e6, designated key id1 (unique)
    n_small = len(key1["x"]) + len(key1["r"])
    rhs_keys1 = np.concatenate([key1["x"], key1["r"]])
    s_id1 = _sample_all(rhs_keys1, n_small, rng).astype(np.int32)
    small_cols = {
        "id1": s_id1,
        "id4": lookup1[s_id1 - 1],
        "v2":  np.round(rng.uniform(0.0, 100.0, size=n_small), 6),
    }

    # RHS: medium — n=N/1e3, designated key id2 (unique); id1 informational
    n_medium = len(key2["x"]) + len(key2["r"])
    rhs_keys2 = np.concatenate([key2["x"], key2["r"]])
    m_id1 = _sample_all(rhs_keys1, n_medium, rng).astype(np.int32)
    m_id2 = _sample_all(rhs_keys2, n_medium, rng).astype(np.int32)
    medium_cols = {
        "id1": m_id1,
        "id2": m_id2,
        "id4": lookup1[m_id1 - 1],
        "id5": lookup2[m_id2 - 1],
        "v2":  np.round(rng.uniform(0.0, 100.0, size=n_medium), 6),
    }

    # RHS: big — n=N, designated key id3 (unique); id1/id2 informational
    rhs_keys3 = np.concatenate([key3["x"], key3["r"]])
    b_id1 = _sample_all(rhs_keys1, N, rng).astype(np.int32)
    b_id2 = _sample_all(rhs_keys2, N, rng).astype(np.int32)
    b_id3 = _sample_all(rhs_keys3, N, rng).astype(np.int32)
    big_cols = {
        "id1": b_id1,
        "id2": b_id2,
        "id3": b_id3,
        "id4": lookup1[b_id1 - 1],
        "id5": lookup2[b_id2 - 1],
        "id6": lookup3[b_id3 - 1],
        "v2":  np.round(rng.uniform(0.0, 100.0, size=N), 6),
    }

    return {"x": x_cols, "small": small_cols, "medium": medium_cols, "big": big_cols}


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
        join_paths = {t: base / t / f"{t}.parquet" for t in ("x", "small", "medium", "big")}
        missing = [t for t, p in join_paths.items() if not p.exists()]
        if not missing:
            print("  [x/small/medium/big] already present, skipping")
        else:
            # All four tables share one key space (see _gen_join_tables), so
            # regenerating any of them regenerates all of them deterministically.
            print(f"  [{', '.join(missing)}] generating (shared key space)")
            tables = _gen_join_tables(N, SEED_JOIN)
            for table in missing:
                _write_parquet(table, tables[table], join_paths[table])

    print(f"[h2o] done. Run `make h2o` to execute the benchmark.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--size",
        choices=list(SIZES.keys()),
        default="medium",
        help="data scale (default: medium ~5GB; `small` is retained for "
             "regenerating historical fixtures but is no longer benchmarked)",
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
