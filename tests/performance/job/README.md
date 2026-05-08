# Join Order Benchmark (JOB) for Opteryx

The Join Order Benchmark — Leis et al., "How Good Are Query Optimizers,
Really?" (VLDB 2015) — is a 113-query suite over the IMDB snapshot designed
to stress join enumeration. It is the workload to use when working on
Opteryx's cost-based optimizer; ClickBench and TPC-H do not exercise join
ordering meaningfully.

This harness mirrors the structure of `tests/performance/clickbench/` and
`tests/performance/tpch/`.

## Layout

```
tests/performance/job/
├── fetch_data.py     # one-time downloader (data + queries)
├── run.py            # runner — invoked by `make job`
├── queries/          # 113 .sql files (1a..33c) — populated by fetch_data.py
└── results/          # per-run CSV: <git-sha>-<timestamp>.csv
```

Data lands in `testdata/job/<table>/<table>.parquet` (one parquet per table,
matching the `testdata.<dataset>.<table>` layout used by the other
benchmarks). The 21 IMDB tables: `aka_name`, `aka_title`, `cast_info`,
`char_name`, `comp_cast_type`, `company_name`, `company_type`,
`complete_cast`, `info_type`, `keyword`, `kind_type`, `link_type`,
`movie_companies`, `movie_info`, `movie_info_idx`, `movie_keyword`,
`movie_link`, `name`, `person_info`, `role_type`, `title`.

## Setup (one-time)

```bash
python tests/performance/job/fetch_data.py
```

Downloads:

  1. `imdb.tgz` (~1.3GB compressed) from `event.cwi.nl/da/job/imdb.tgz`,
     extracted to `testdata/_downloads/job/csv/`.
  2. The 113 query files from `gregrahn/join-order-benchmark`.

Each CSV is converted to a single SNAPPY-compressed Parquet file using the
official JOB schema. The conversion uses **PyArrow as a one-shot dev tool**
(plus Python stdlib `csv` for the read side, since JOB's CSVs use
backslash-escaped quotes that PyArrow's csv reader doesn't handle). PyArrow
is required here because its parquet root-schema name (`arrow_schema`) is
the only one Rugo's converter strips, leaving column names usable; DuckDB
writes `duckdb_schema.<col>` and the columns end up unresolvable.

This is dev tooling, not engine usage — PyArrow never runs at benchmark
time and is not imported by `run.py`. The build-time PyArrow scan only
flags production code paths.

If your dev venv doesn't have it: `pip install pyarrow`.

The script is idempotent: re-running skips any artefact that already
exists. Pass `--force-convert` to re-emit the Parquet files (e.g. if the
schema or compression changes).

Disk footprint after setup: ~1.3GB tarball + ~6GB CSV + ~2GB Parquet.

## Running

```bash
make job
```

Or directly:

```bash
python tests/performance/job/run.py [--timeout 300] [--filter '^1[abc]$']
```

The runner walks all 113 queries in canonical order (1a, 1b, 1c, 2a, …, 33c),
opens a fresh `opteryx.session()` per query, drives `execute_to_morsels()`
to completion, and writes one row per query to a CSV in `results/`.

### Per-query timeout

Default 300 s, adjustable with `--timeout`. The timeout is enforced between
morsels — it cannot interrupt a single blocking C/Cython call. In practice
that is fine for JOB: the long-runners spend most of their time in
iterator drains the timeout can observe.

### Output

```
results/<git-sha>-<timestamp>.csv
```

Columns: `query, status, elapsed_ms, row_count, error_msg`, where
`status ∈ {ok, timeout, error}`. The terminal also prints a summary
(counts, total wall, median + p95 of successful queries).

## Expected runtime

Wall time depends heavily on optimizer quality. On a developer laptop
today, expect that some queries time out and others error — that is the
point of the benchmark. Track the `ok` count and median over time; both
should improve as the cost-based optimizer matures.

The simple queries (1a, 2a, 3a) should return non-timeout results.

## Interpreting results

  - `ok` count, total wall, median/p95: top-line numbers to track over commits.
  - Per-query `elapsed_ms`: regressions show up here first.
  - `error_msg` for `error` rows: groups failures by class — missing function,
    planner crash, exec crash. Triage and file separately, do not fix as
    part of the benchmark setup.

`run.py` rewrites bare IMDB table names to `testdata.job.<table>` at
runtime, scoped to the FROM clause so identical names used as column
aliases or column refs (`AS movie_keyword`, `mi.info`) are not clobbered.
The `.sql` files in `queries/` are committed verbatim from upstream.

## Why PyArrow for the conversion?

Two reasons:

  1. The parquet root-schema name. Rugo's parquet→orso converter only
     strips the `arrow_schema.` prefix from column names. PyArrow writes
     parquets with that root, so columns come through as `id`, `country_code`,
     etc. DuckDB (the obvious alternative, already a dev dep elsewhere)
     writes `duckdb_schema.<col>` and the columns are unresolvable.
  2. Streaming write. PyArrow's `ParquetWriter` lets us batch row-by-row
     out of the Python `csv` reader, keeping memory bounded on the larger
     tables (cast_info ~36M rows).

PyArrow here is dev tooling — same status as DuckDB in the ClickBench/
TPC-H calibration runners. It is **not** used by `run.py` and **not**
shipped with Opteryx.
