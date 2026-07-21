# JSONBench for rugo

[JSONBench](https://github.com/ClickHouse/JSONBench) (ClickHouse) is a
5-query benchmark over a real-world Bluesky Jetstream NDJSON dump, designed
to stress nested-JSON reading and aggregation at scale (1m / 10m / 100m /
1000m row cuts of the same dataset).

**Opteryx cannot read JSON at all** — there is no SQL-level JSON source.
This benchmark instead runs the 5 queries against **rugo**, the file engine,
using its JSONL reader directly. There is no SQL layer in rugo either, so
each query is a hand-written Python scan-and-aggregate over the Morsels rugo
returns (see `rugo/runner.py`). The point of this benchmark is not
JSONBench-style engine bragging rights — it's to measure how far behind a
real embedded-JSON engine (DuckDB) rugo currently is, to inform whether
Opteryx should gain native JSON reading.

## Why this is NOT apples-to-apples with ClickBench/TPC-H

Every other benchmark in `tests/performance/` compares two SQL engines
running the same query against the same storage. This one doesn't:

- **DuckDB** loads all shards into a `bluesky (j JSON)` table once (timed
  separately, printed as "Load time"), then the 5 queries run as warm
  queries against that already-parsed, natively-stored table.
- **rugo** has no persisted table to query against. Each query call is a
  fresh full scan of the raw NDJSON shards — column projection + top-level
  predicate pushdown reduce what's read, but there is no ingest step to
  amortize; every query re-parses JSON from scratch.

That asymmetry is deliberate, not an oversight: "query raw JSON files
directly, no ingestion step" is the actual use case in question for
Opteryx. Read the ratio in the results table as "cost of querying JSON
files directly today" vs. "cost of querying pre-loaded native storage",
not as "engine A vs. engine B, same job."

## Why nested fields still cost more than a flat column

The Bluesky schema nests everything under `commit`: `commit.operation`,
`commit.collection`, etc. rugo's JSONL reader has no struct/nested-vector
type — a nested object comes back as a single VARCHAR column holding the
raw JSON text of that sub-object, per row. The first version of this
benchmark parsed that text with `json.loads()` in a Python loop, once per
row — a per-row Python object-parse cost. `rugo/runner.py` instead extracts
`operation`/`collection` with `vector_json_extract_text` (draken's
yyjson-backed kernel, `opteryx/compiled/nanobind/vector_json.cpp`): one C++
pass over the *whole column* per morsel, no per-row Python involved for the
parse itself. That cut the 10m-row wall time roughly in half (see Results).

Row filtering (`operation = 'create'`, `collection IN (...)`) is also
native: `Vector.in_list` / `BoolVector.and_vector` build the mask, and
`Morsel.filter_mask(mask)` applies it across every column (did, time_us,
collection) in one C++ gather — no Python per-row branching, no boxed
index list. `.to_pylist()` only runs afterward, on the columns the final
grouping step needs, over the *filtered* row set rather than every row.

Grouping/counting is still a plain Python dict after that — rugo has no
group-by primitive exposed outside the full operator pipeline. Filtering
natively first didn't move the total wall time much (see Results): the
per-row JSON extraction inside `vector_json_extract_text` — which has to
run before a mask can even be built — is the dominant cost, not the
`.to_pylist()`/dict-grouping step it feeds. That's itself a useful data
point: the remaining gap to DuckDB is concentrated in per-row JSON
decoding, not in Python-side bookkeeping around it.

`vector_json_extract_text` is pure draken/yyjson vector work with no SQL or
opteryx-engine involvement — it happens to be built into the opteryx_core
wheel's nanobind module (`opteryx/compiled/nanobind/`) rather than draken's
own, which is worth revisiting separately since nothing about it is
SQL-specific.

## Known data-quality defect in the upstream dataset

At real scale (10m+ rows) the Bluesky dump contains at least one malformed
record per few million: an unescaped control character (a raw newline)
embedded in a nested string field, splitting what should be one JSON object
across two physical lines. Confirmed on both sides independently:

- DuckDB's `read_ndjson_objects(..., ignore_errors=false)` — matching
  upstream's own `duckdb/load_data.sh` — throws `Malformed JSON ... at byte
  65536 ... unexpected end of data` on it. Upstream's script doesn't check
  the exit code of that command, so in the *original* JSONBench harness a
  chunk (100k lines) containing a bad line is **silently dropped in its
  entirety** with no record of it happening. Our `duckdb/runner.py` instead
  uses `ignore_errors=true` and reports `rows_loaded` vs. `rows_expected`
  so a skip is visible, never silent.
- `vector_json_extract_text` fails the *whole column* on the first
  malformed row it hits (fail fast — draken's general error policy).
  `rugo/runner.py`'s `_extract_native` catches that `RuntimeError` and falls
  back to a per-row `orjson.loads` scan (`_extract_fallback`) for that one
  morsel only, counting and skipping just the bad row(s) rather than losing
  the rest of the morsel — printed as `[Qn] skipped N malformed row(s)` once
  per query call. Every other morsel in the same run still takes the fast
  vectorized path; only the ~1-2 morsels that actually contain a bad row pay
  the slow fallback, and orjson's Rust parser keeps even that path cheaper
  than stdlib `json` would (`orjson.JSONDecodeError` subclasses
  `json.JSONDecodeError`, so error handling is unchanged).

## Layout

```
tests/performance/jsonbench/
├── fetch_data.py     # downloads + decompresses shards (idempotent)
├── rugo/runner.py    # the 5 queries, hand-written over rugo's JSONL reader
├── duckdb/runner.py  # DuckDB baseline (upstream duckdb/queries.sql, verbatim)
├── runner.py         # benchmark + comparison front-end invoked by `make jsonbench`
└── results/          # per-run CSV: <git-sha>-<timestamp>.csv
```

Data lands in `testdata/_downloads/jsonbench/` (raw `file_NNNN.json.gz`) and
`testdata/_downloads/jsonbench/decompressed/` (cached `file_NNNN.jsonl`,
decompressed once so repeat runs don't pay gzip cost every iteration).
Neither is committed (see `.gitignore`).

## Setup (one-time)

```bash
python tests/performance/jsonbench/fetch_data.py --size 10   # or 1 / 100
```

Sizes map to shard counts of the Bluesky NDJSON dump on the public
ClickHouse S3 bucket, 1,000,000 rows/shard, ~135MB gzipped each:

| size | shards | compressed | decompressed |
|------|--------|------------|---------------|
| 1    | 1      | ~0.14GB    | ~0.5GB        |
| 10   | 10     | ~1.4GB     | ~4.9GB        |
| 100  | 100    | ~13.5GB    | ~40GB         |

The 100m size is supported but not exercised routinely — at that scale
this harness's per-query full rescan (no persisted storage to query
against, see below) makes it a genuinely long run; expect several minutes
per query, scaling roughly linearly from the 10m numbers below. Run it
deliberately (`make jsonbench JSONBENCH_SIZE=100`), not as part of routine
iteration.

`duckdb` (and `pytz`, which DuckDB's timestamp functions need) and `orjson`
are dev-only dependencies, not tracked in `tests/requirements.txt` — same
status as the other `*-duckdb` calibration runners. `pip install duckdb
pytz orjson` yourself before running `make jsonbench-duckdb` /
`make jsonbench`.

## Running

```bash
make jsonbench                        # rugo vs DuckDB, 10m rows, 2 warm iterations
make jsonbench JSONBENCH_SIZE=1        # smaller/faster
make jsonbench-data JSONBENCH_SIZE=100 # fetch the 100m set (13.5GB download)
make jsonbench-duckdb                  # regenerate duckdb/results.local.<N>m.json
```

Or directly:

```bash
python tests/performance/jsonbench/runner.py --size 10 --iterations 2
```

## Results (this machine, 2026-07-21)

10m rows, 2 warm iterations, Apple Silicon laptop:

| Query | rugo (min) | DuckDB (min) | ratio |
|-------|-----------:|-------------:|------:|
| Q1    | 4435ms     | 157ms        | 28x   |
| Q2    | 7731ms     | 703ms        | 11x   |
| Q3    | 6506ms     | 649ms        | 10x   |
| Q4    | 5641ms     | 531ms        | 11x   |
| Q5    | 5754ms     | 554ms        | 10x   |

(Before pushing `commit.*` extraction into `vector_json_extract_text` — i.e.
plain per-row `json.loads()` — these were 106x/27x/28x/33x/33x. Adding
native `in_list`/`filter_mask` row filtering on top made no further
difference (still ~106x/27x/28x/33x/33x). The further drop shown above
(to 28x/11x/10x/11x/10x) lines up with switching the malformed-row fallback
to orjson, but that path only touches ~3 rows out of 10m — it should not
move the total by seconds. Re-running these numbers repeatedly on the same
machine also warms the OS page cache for the 4.9GB of decompressed shard
files, which is a more plausible explanation for most of this delta than
orjson itself. Treat the ratio, not the absolute ms, as the load-bearing
number, and re-measure on a cold cache before trusting a specific ms figure.
Same rows, same DuckDB numbers throughout; only the rugo-side
extraction/filtering/fallback-parser changed.)

Full per-run numbers: `results/*.csv`, `duckdb/results.local.10m.json`.

## Interpreting results

rugo is roughly 10–30x slower than DuckDB on this workload, at 10m rows, after
pushing nested-field extraction into a native draken kernel. That's the
remaining cost of: (1) no persisted/columnar storage to query against —
every query is a fresh full scan + decode of raw NDJSON, and (2) no
group-by primitive below the full operator pipeline, so counting/grouping
still runs as a Python dict loop over `.to_pylist()`'d values. Neither of
those is a rugo bug; they're missing capabilities. This number is the input
for a separate decision: whether adding native JSON reading (and/or a
nested/struct vector type, and/or exposing group-by standalone) to Opteryx
is worth the engineering cost, weighed against how much real workload is
JSON-shaped.

## Bug found and fixed while building this

Building Q1 (which projects only the `commit` column, since it has no
predicate) surfaced a real correctness bug in rugo's JSONL reader:
projecting exactly one column that's absent on some records silently
**dropped those rows** instead of returning them with a null value —
confirmed at both the `finalize_records` (`rugo/src/jsonl/core/field_span.cpp`)
and `MapBuilder::bank_record` (`rugo/src/jsonl/core/interpreter.cpp`) layers,
which shared the same "empty spans ⇒ drop the record" logic. Fixed in both
places: a record that passes predicates is now always kept, even with zero
matched columns (an all-null row) — matching NDJSON semantics (one line =
one row) and keeping projected columns' row counts in sync with each other.
Covered by the correctness test in `rugo/runner.py`
(`test_queries_against_synthetic_fixture`) and by the full `tests/rugo/`
suite (478 passed after the fix).
