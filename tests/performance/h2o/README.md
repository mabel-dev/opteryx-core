# H2O db-benchmark for Opteryx

The H2O db-benchmark
([duckdblabs/db-benchmark](https://github.com/duckdblabs/db-benchmark), the
actively maintained fork of the original `h2oai/db-benchmark`) is the de
facto community standard for comparing analytical engines on **groupby**
and **join** workloads. It's where DuckDB, Polars, DataFusion, etc.
publish their numbers.

This harness runs the H2O suite against Opteryx so we can do head-to-head
comparisons on aggregation and join performance — particularly relevant
for the Draken native-aggregate work.

It mirrors the structure of `tests/performance/job/`.

## Layout

```
tests/performance/h2o/
├── generate_data.py   # one-time synthetic data generator (no R required)
├── runner.py          # benchmark + comparison front-end invoked by `make h2o`
├── run.py             # lower-level Opteryx runner
├── queries/           # 10 groupby (g1..g10) + 5 join (j1..j5) .sql files
└── results/           # per-run CSV: <git-sha>-<timestamp>.csv
```

Data lands in `testdata/h2o/<size>/<table>/<table>.parquet`, resolved as
`testdata.h2o.<size>.<table>` by the dataset registry. The runner rewrites
bare table names (`x`, `small`, `medium`, `big`) to that fully-qualified
form before execution.

## Sizes

| size   | rows (`x`) | on-disk (~) | default? |
|--------|-----------:|------------:|----------|
| small  |        1e7 | 0.5 GB      | **yes**  |
| medium |        1e8 | 5 GB        | yes (opt-in to generate) |
| large  |        1e9 | 50 GB       | no — opt-in only, not validated by `make h2o` |

`K=100` (group-cardinality factor) for all sizes, matching upstream.

## Setup (one-time)

```bash
pip install numpy                 # dev dep; not an Opteryx runtime dep
PYTHONPATH=. python tests/performance/h2o/generate_data.py --size small
```

`numpy` does the random generation (fast RNG over 1e7+ elements); explicitly
permitted for `tests/`/`dev/` use by `CLAUDE.md` §4. Parquet is written by
Rugo's own native writer (`rugo.parquet.write_parquet`) — no PyArrow.

(We tried DuckDB's writer first; it emits the legacy `INT_32` `converted_type`
on INT32 columns and omits the modern `StringType()` `logical_type` on UTF8
columns, which Rugo's schema discovery does not accept. We then used PyArrow
as a workaround — it writes both metadata variants — until Rugo got its own
native writer, which emits metadata its own reader parses cleanly. That
removed the need for PyArrow entirely.)

The generator is idempotent: existing parquet files are skipped. Generation
time scales with `N`: small ≈ 1 min, medium ≈ 10 min, large ≈ 1.5 hr on a
modern laptop.

## Schemas

**Groupby table** (`x_groupby`):
```
id1, id2 : VARCHAR (cardinality K=100, "id001"..)
id3      : VARCHAR (cardinality N/K, "id0000000001"..)
id4..id6 : INTEGER
v1, v2   : INTEGER
v3       : DOUBLE
```

**Join tables** (note: distinct schema from groupby). Cardinalities match
upstream `join-datagen.R` exactly — id1/id2/id3 domains are N/1e6, N/1e3, N
respectively (NOT the groupby table's flat K=100), and id4/id5/id6 are
string mirrors of id1/id2/id3 (`f"id{value}"`), not an independent column:
```
x       : id1..id3 INT,  id4..id6 VARCHAR,  v1 DOUBLE   (N rows)
small   : id1 INT,        id4 VARCHAR,      v2 DOUBLE   (N/1e6 rows)
medium  : id1, id2 INT,   id4, id5 VARCHAR, v2 DOUBLE   (N/1e3 rows)
big     : id1..id3 INT,   id4..id6 VARCHAR, v2 DOUBLE   (N rows)
```
Each RHS table's *designated* join key (small.id1, medium.id2, big.id3) is
an exact unique permutation of its domain — a proper foreign key, matching
upstream's `stopifnot(uniqueN(...) == n)`. ~10% of LHS keys and ~10% of RHS
keys are deliberately private to their side (upstream `split_xlr`), so
INNER (j2) and LEFT (j3) joins produce genuinely different row counts.

## Running

```bash
make h2o                                    # both workloads, small size, 2 runs each
python tests/performance/h2o/runner.py \
    --workload groupby --size medium        # just groupby, on the 5GB fixture
python tests/performance/h2o/runner.py \
    --filter '^g1$' --iterations 5          # iterate on g1
```

Each query runs **twice** by default (cold + warm), per the upstream H2O
convention — published numbers report both timings.

## Results

CSV at `results/<git-sha>-<timestamp>.csv`:

| column        | notes                              |
|---------------|------------------------------------|
| workload      | `groupby` \| `join`                |
| size          | `small` \| `medium` \| `large`     |
| query         | `g1`..`g10`, `j1`..`j5`            |
| run           | 1 (cold) or 2 (warm), or higher with `--runs` |
| status        | `ok` \| `timeout` \| `error`       |
| elapsed_ms    | wall-clock                         |
| row_count     | rows produced                      |
| error_msg     | truncated to 500 chars             |

Compare to upstream H2O numbers at
[duckdblabs.github.io/db-benchmark](https://duckdblabs.github.io/db-benchmark/)
— the table format on that page reports cold/warm separately, so our
schema lines up.

## Known gaps

These are queries that rely on functions Opteryx may not yet implement.
They will surface as `error` rows, not crashes — that's expected.

| query | likely gap                                                     |
|-------|----------------------------------------------------------------|
| g6    | `median(v3)` and `stddev(v3)` aggregates                       |
| g8    | `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)` window     |
| g9    | `corr(v1, v2)` aggregate, and possibly `pow()`                 |

Per repo policy (`CLAUDE.md` §8) these are reported, not papered over —
the runner captures the error and moves on.

## Cold vs. warm runs

The first run of each query reads parquet from disk; on subsequent runs
the OS page cache (and any in-process caches) typically deliver lower
latency. Reporting both is how the upstream benchmark publishes numbers,
so the two-run shape allows direct comparison.

If you need fully-cold numbers, drop the OS page cache between runs (Linux:
`echo 3 > /proc/sys/vm/drop_caches`; macOS: reboot, or `sudo purge`) — the
runner does **not** do this automatically.
