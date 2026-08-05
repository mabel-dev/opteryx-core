# JSONBench for Opteryx

[JSONBench](https://github.com/ClickHouse/JSONBench) (ClickHouse) is a
5-query benchmark over a real-world Bluesky Jetstream NDJSON dump, designed
to stress nested-JSON reading and aggregation at scale (1m / 10m / 100m /
1000m row cuts of the same dataset).

The 5 queries run as **real Opteryx SQL** against `READ_JSONL(path)` (see
`opteryx/runner.py`) — filtering, grouping, and JSON-key extraction
(`->`/`->>`) all go through the normal planner/optimizer/native-execution
path, the same as every other Opteryx query. That wasn't always true: this
benchmark originally ran a hand-written Python scan-and-aggregate over
**rugo**'s JSONL reader directly (`rugo/runner.py`, kept for its own test
coverage of rugo's raw kernels) because `READ_JSONL` couldn't bind a column
whose values are nested JSON objects (Bluesky's `commit` field) or arrays.
That gap is closed — see `docs/json_variant_type_plan.md` for the VARIANT
type `->`/`->>` operate on, and the "Bugs found" section below for what
else had to be fixed along the way. The point of this benchmark is not
JSONBench-style engine bragging rights — it's to measure how far behind a
real embedded-JSON engine (DuckDB) Opteryx currently is on this workload.

## Why this is NOT apples-to-apples with ClickBench/TPC-H

Every other benchmark in `tests/performance/` compares two SQL engines
running the same query against the same storage. This one doesn't:

- **DuckDB** loads all shards into a `bluesky (j JSON)` table once (timed
  separately, printed as "Load time"), then the 5 queries run as warm
  queries against that already-parsed, natively-stored table.
- **Opteryx** has no persisted table to query against. Each query is a
  fresh `READ_JSONL` scan over a glob of the raw NDJSON shards — column
  projection + predicate pushdown reduce what's read, but there is no ingest
  step to amortize; every query re-parses JSON from scratch.

That asymmetry is deliberate, not an oversight: "query raw JSON files
directly, no ingestion step" is the actual use case in question for
Opteryx. Read the ratio in the results table as "cost of querying JSON
files directly today" vs. "cost of querying pre-loaded native storage",
not as "engine A vs. engine B, same job."

## Why nested fields still cost more than a flat column

The Bluesky schema nests everything under `commit`: `commit.operation`,
`commit.collection`, etc. `commit ->> 'operation'` runs
`vector_json_extract_text` (draken's yyjson-backed kernel,
`opteryx/compiled/nanobind/vector_json.cpp`) — one C++ pass over the
*whole column* per morsel to pull the key out of every row's raw JSON
text, no per-row Python anywhere in the query. Row filtering
(`operation = 'create'`, `collection IN (...)`) and grouping/counting are
both native too — a real `GROUP BY`/aggregate through the same operator
pipeline every other SQL query uses, not a Python dict loop. Q1 (no
predicate — extracts and groups every row unfiltered) is still the
highest-ratio query against DuckDB (see Results): that per-row JSON-key
extraction, which has to run before any filter or group-by can act, is
the dominant remaining cost, not anything Python-side.

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
- rugo's JSONL parser detects these directly (`rugo/src/jsonl/core/interpreter.cpp`,
  `structural_scan.hpp`, `field_span.cpp`): a raw control character inside a string
  invalidates the record, so the record is dropped, the parser resyncs at the next
  physical line boundary, and the occurrence is counted (`malformed_count`, surfaced
  through `read_jsonl`'s result). With `ignore_errors => true` the query keeps going;
  with the default `fail_on_error=true` it raises naming the line. Row counts match
  orjson's own valid-line count exactly — 999,998 of 1,000,000 on each of shards
  5/6/7 at the 10m size, 9,999,994 across all ten.

**The benchmark reads the raw downloaded shards, unmodified.** An earlier version of
this runner worked around the defect in Python by writing "cleaned" copies of each
shard with the bad lines stripped, and querying those — which meant the benchmark was
quietly measuring a doctored copy of the dataset rather than the dataset. That is
gone; the parser handles it, as it should.

## Layout

```
tests/performance/jsonbench/
├── fetch_data.py       # downloads + decompresses shards (idempotent)
├── opteryx/runner.py   # the 5 queries as real Opteryx SQL against READ_JSONL (+ shard_glob)
├── rugo/runner.py      # the original hand-written scan-and-aggregate over rugo's JSONL reader (kept for its own tests, no longer used by runner.py)
├── duckdb/runner.py    # DuckDB baseline (upstream duckdb/queries.sql, verbatim)
├── runner.py           # benchmark + comparison front-end invoked by `make jsonbench`
└── results/            # per-run CSV: <git-sha>-<timestamp>.csv
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
make jsonbench                        # Opteryx SQL vs DuckDB, 10m rows, 2 warm iterations
make jsonbench JSONBENCH_SIZE=1        # smaller/faster
make jsonbench-data JSONBENCH_SIZE=100 # fetch the 100m set (13.5GB download)
make jsonbench-duckdb                  # regenerate duckdb/results.local.<N>m.json
```

Or directly:

```bash
python tests/performance/jsonbench/runner.py --size 10 --iterations 2
```

## Results (this machine, 2026-08-05)

10m rows, 2 warm iterations, Apple Silicon laptop:

| Query | Opteryx SQL (min) | rugo hand-rolled (min, 2026-07-21) | DuckDB (min) | Opteryx ratio |
|-------|------------------:|------------------------------------:|-------------:|--------------:|
| Q1    | 1228ms            | 4435ms                              | 157ms        | 7.8x          |
| Q2    | 1678ms            | 7731ms                              | 703ms        | 2.4x          |
| Q3    | 1543ms            | 6506ms                              | 649ms        | 2.4x          |
| Q4    | 1550ms            | 5641ms                              | 531ms        | 2.9x          |
| Q5    | 1539ms            | 5754ms                              | 554ms        | 2.8x          |
| TOTAL | 7538ms            | 30067ms                             | 2594ms       | **2.9x**      |

The gap to DuckDB went 11.6x → 5.8x → **2.9x** over this work: first from replacing the
hand-rolled Python scan-and-aggregate with real SQL (native filter/group/aggregate
instead of a Python dict loop), then from reading the shards as a single globbed scan
rather than a `UNION ALL` per shard — one scan node over N files instead of N scan
nodes the planner unions together, worth almost exactly 2x on its own (14,942ms →
7,538ms, same queries, same data).

Q1 remains the highest-ratio query, and for the same structural reason as before: it
is the only one with no predicate, so all 10m rows pay the `->>` JSON-key extraction
with nothing to shrink the row set first. Treat the ratio, not the absolute ms, as the
load-bearing number, and re-measure on a cold cache before trusting a specific figure.

Full per-run numbers: `results/*.csv`, `duckdb/results.local.10m.json`.

## Interpreting results

Opteryx is roughly 2.4–7.8x slower than DuckDB on this workload at 10m rows, running
real SQL against `READ_JSONL` over the raw NDJSON. Two things account for the
remainder: (1) there is no persisted/columnar storage to query against — every query
is a fresh full scan + JSON decode of the raw files, where DuckDB's numbers are
against an already-parsed native table with load time excluded; and (2) the per-row
JSON-key extraction `vector_json_extract_text` pays before any filter or group-by can
act (see Q1). Neither is a bug; they are the cost of the thing being measured. This
number is the input for a separate decision: whether Opteryx should gain a
persisted/native JSON storage format so repeated queries stop re-parsing, weighed
against how much real workload is JSON-shaped.

## Bugs found and fixed while building this

**Original hand-rolled Python runner (2026-07):** Building Q1 (which
projects only the `commit` column, since it has no predicate) surfaced a
real correctness bug in rugo's JSONL reader: projecting exactly one column
that's absent on some records silently **dropped those rows** instead of
returning them with a null value — confirmed at both the `finalize_records`
(`rugo/src/jsonl/core/field_span.cpp`) and `MapBuilder::bank_record`
(`rugo/src/jsonl/core/interpreter.cpp`) layers, which shared the same
"empty spans ⇒ drop the record" logic. Fixed in both places: a record that
passes predicates is now always kept, even with zero matched columns (an
all-null row) — matching NDJSON semantics (one line = one row) and keeping
projected columns' row counts in sync with each other. Covered by the
correctness test in `rugo/runner.py` (`test_queries_against_synthetic_fixture`)
and by the full `tests/rugo/` suite (478 passed after the fix).

**SQL rewrite (2026-08):** Getting `READ_JSONL` + `->`/`->>` to actually run
these 5 queries correctly surfaced five more, unrelated to the above:

- `rugo/src/jsonl/core/value_parser.cpp`'s `evaluate_predicate()` had no
  case for `ValueType::Boolean` — every predicate pushed against a BOOL
  JSONL column silently matched zero rows (fell through to the "unsupported
  type" default). `rugo/src/jsonl/_jsonl_reader.pxi` also encoded a Python
  `True`/`False` predicate literal as `str(True)` (`"True"`) instead of
  JSON's `"true"`, a second bug in the same area. Fixed both: a real
  `ValueType::Boolean` branch in `evaluate_predicate` (reusing the int
  comparator, `false=0 < true=1`, so all six ops behave consistently), and
  correct JSON-literal encoding on the Cython side.
- The same Cython predicate-encoding path also mishandled `bytes` values —
  Opteryx's bound VARCHAR literals arrive as `bytes`, and `str(b'commit')`
  produces the Python repr (`"b'commit'"`), not the string's own content.
  This broke **every** `WHERE varchar_col = 'literal'` predicate pushed to
  `READ_JSONL`, silently returning zero rows — not just the case above.
  Present in both `rugo/src/jsonl/_jsonl_reader.pxi` (predicate encoding)
  and its Volnitsky raw-prefilter needle-building, plus the identical
  pattern in `rugo/src/csv/_csv_reader.pxi` (`READ_CSV` predicate
  pushdown). Fixed in all three.
- `draken_native.cpp`'s `concat_owners` (the kernel behind `UNION ALL`)
  had no case for `DRAKEN_VARIANT`, even though it shares VARCHAR's exact
  German-string storage and `concat_string` needed no changes to handle it
  — an honest missing case, not a deliberate exclusion (unlike VARIANT's
  real, intentional blocks on CAST/comparison/GROUP BY keys). Fixed by
  routing VARIANT into the existing `concat_string` path.
- `opteryx/planner/optimizer/strategies/projection_pushdown.py` had an
  already-diagnosed-and-partially-fixed bug (see the large comment at that
  file's UNION-leg-width fallback): a `UNION` leg reached via a bare
  `SELECT *` gets its own, independently-minted schema-column identities,
  which can come back narrower than the union's real width when matched
  against the wrong sibling's identities — previously fixed for `Scan`/
  `Subquery` legs, but the fix's condition never got extended to
  `READ_JSONL`/`READ_PARQUET`/`READ_CSV` (`is_pushable_function_dataset`),
  even though they're treated identically everywhere else in the same
  function. `SELECT * FROM READ_JSONL(...) UNION ALL SELECT * FROM
  READ_JSONL(...)` failed loud (not a silent wrong answer) with "this
  file's columns [...] do not match the expected []". Fixed by adding
  `is_pushable_function_dataset` to the same condition.
- A 3+-leg chained `UNION ALL` over `READ_JSONL` (i.e. a union whose own
  leg is itself a union — needed to combine more than 2 shards into one
  relation) crashed at bind time with `KeyError` in `set_ops.py`'s
  `visit_union`. Root cause in `logical_planner.py`'s `get_subplan_schemas`:
  it collected *every* leaf relation alias in a nested union's subtree, but
  the binder only keeps the **left** side's schema alive in
  `context.schemas` once a nested union finishes binding (the right side is
  explicitly popped after being folded into the left) — so the outer union
  later tried to resolve the (already-popped) right-side alias. Fixed by
  making the alias-collector reuse the inner union's own already-correct
  `left_relation_names` instead of re-deriving from raw graph children,
  correct at any nesting depth by induction.

None of the five were introduced by this rewrite — they were pre-existing
gaps/bugs in code paths the old hand-rolled `rugo/runner.py` never
exercised (it called rugo's reader directly, bypassing the SQL binder,
`UNION`, and Opteryx's own predicate-literal encoding entirely).

**Malformed-record handling in rugo's JSONL parser (2026-08).** Pointing the
benchmark at the *raw* shards (rather than pre-cleaned copies) exposed that
rugo did not handle the dataset's malformed records at all — it silently
**invented rows** from them. A 1,000,000-line shard returned 1,000,028 rows,
identically with `fail_on_error=True` and `False`: the defect was neither
dropped nor raised, just mis-parsed into garbage. Four distinct causes, all
fixed, all verified against orjson's own valid-line count (999,998 per
affected shard, 9,999,994 across all ten):

- **The FSA treated a raw newline inside a string as ordinary content**
  (`interpreter.cpp`). RFC 8259 requires control characters in a string to be
  escaped, so a raw `0x0A` there is always invalid — but the state machine kept
  consuming until it found some later, unrelated quote to close on, fabricating
  records from fragments of two real lines. Now: flag, drop the record, resync.
- **The masked SIMD scanner suppressed the newline marker entirely**
  (`structural_scan.hpp`). Its `structb & ~in_str` drops in-string structurals,
  so a raw newline mid-string never even produced a marker for the FSA to react
  to. Newlines now bypass the in-string mask exactly like real delimiter quotes
  already did (`| real_q | newline`). The unmasked scan never had this bug —
  only the adaptive high-in-string-density path did, which is why it reproduced
  on the real dump but not on small fixtures.
- **`scan_container` had the same blind spot** (`interpreter.cpp`), and this is
  the one that actually mattered for Bluesky: every field of interest lives in a
  nested `commit: {...}`, and nested container values are bulk-scanned by
  `scan_container`, never by the FSA. It now reports unclosed on a raw newline,
  and its caller discards the whole record rather than banking it with a
  truncated slice as the field's value.
- **Thread-range splitting could bisect a malformed record**
  (`field_span.cpp`). `interpret_jsonl_threaded` ends each range at "the next
  `\n`" — which, on a raw embedded newline, splits one record across two
  ranges. The first range's truncated fragment is discarded correctly, but the
  second began mid-record on the garbage tail and banked every nested `{...}`
  still in it as a phantom row. Observed exactly: shard 6's boundary landed
  inside a malformed labeler-service record and its ~30 nested policy objects
  each became a spurious row (shards 5 and 7 were unaffected purely because
  their boundaries fell elsewhere). Ranges now only begin at a line whose first
  non-whitespace byte is `{`, the same assumption the FSA's `START_RECORD`
  already encodes.

`MapBuilder::finish` also used to bank a trailing record left open at
buffer/chunk end as if it were complete; it now discards and flags it. And
`malformed_count` was added to `RecordSet`/`read_jsonl`'s result so a run with
`ignore_errors => true` reports how many records it dropped instead of the drop
being invisible.
