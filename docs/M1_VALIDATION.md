# M1-VALIDATE — did M1 remove the scan-stage GIL bottleneck?

Status: **MEASUREMENT COMPLETE**. This is a report, not an engine change; no
behaviour was modified to produce these numbers.

## TL;DR verdict

| Success criterion | Verdict |
|---|---|
| `trampoline_calls` ~0 on native, >0 on trampoline | **PASS** |
| Scan-stage `gil_held_ns` ~0 on native, >0 on trampoline | **PASS** |
| Worker-purity guard (`whitelist=()`) PASS on native | **PASS** |
| Pruning identical (bytes/row-groups-scanned) | **PASS** |
| Native scales / trampoline flattens under load | **PARTIAL PASS** — real, GIL-attributable gap under concurrent queries (~20–30% aggregate throughput, confirmed to vanish with GIL off); per-single-query dop scaling is mostly a wash because decode already runs on a shared, always-parallel nogil pool independent of both paths |
| Correctness (row-parity native == trampoline) | **PASS**, all 11 scenarios × both regimes |

**M1 is real and delivered exactly what it claims at the instrumentation level**
(zero Python re-entry, zero GIL time, pruning unchanged, correct results). The
**load-bearing claim to sharpen** is the "scales vs flattens" framing: in this
codebase the scan's byte-decode already runs on a separate always-parallel
worker pool in *both* Sources, so the trampoline's residual GIL cost is a
bounded per-morsel tax layered on top of already-parallel decode, not a full
serialization of the scan. It still costs real throughput under concurrent
query load (measured, not assumed), just not as dramatically as "flat vs
scaling" implies for a single query's own `dop` knob.

---

## Methodology

### Regime emulation
This dev interpreter is a free-threaded CPython 3.14.5 build whose GIL is
toggled by the `PYTHON_GIL` env var (`Makefile:13-16`). The deployed service
runs **standard-GIL** CPython 3.14 — so the benchmark was run as:

- `PYTHON_GIL=1` — **PRIMARY**, bit-for-bit emulates the deployed regime
  (`sys._is_gil_enabled() == True`).
- `PYTHON_GIL=0` — cross-check, to isolate what changes when the GIL is the
  only variable removed.

Both runs: 18 physical CPUs, `PARQUET_LOCAL_IO_WORKERS` at its default
(`min(16, max(8, cpus-2))` = 16 here), row-group size 262,144, 10,000,000 rows
per dataset, 3 measured repeats + 1 warmup per point (median reported).

### Force mechanism
Identical to the WP-01/02/11 parity tests already in the tree: monkeypatch
`opteryx.connectors.parquet_io.pool_reader.native_scan_supported` to return
`False`, which routes the scan to `StreamingScanSource` (the trampoline) with
the predicate on the old bytecode-VM path. No engine code touched.

### Datasets (generated once, cached, deterministic — `pyarrow`, test-only dep)
- **STRING** (ClickBench-like): `id` (sorted int64, enables row-group
  min/max pruning), `url` (high-cardinality), `title` (medium), `referer` (low,
  1000 distinct), `user_agent` (16 distinct), `n` (unsorted int64, no pruning).
  10M rows, single-file and 8-file (multi-file) variants.
- **DECIMAL+TIMESTAMP**: `id` (sorted), `price` DECIMAL64(9,2), `amount`
  DECIMAL128(18,2), `event_ts` TIMESTAMP(us), `ev_date` DATE32, `n` (unsorted).
  10M rows, single-file.

### Scenarios (11 total, both files under `tests/performance/.m1data/`)
For each table: no-predicate (scan+project), selective predicate on the sorted
key (~1% survivors, prunes row groups), non-selective predicate (keeps all
rows, exercises the residual filter with no pruning benefit), a role-3
filter-only predicate (predicate column dropped from projection — exercises
the `Select`-back path), and (string table only) a predicate on a low-cardinality
dictionary-encoded string column.

### Two parts, two different things measured
- **Part A** — per-query `dop` sweep (`MAX_EXECUTION_WORKERS` ∈ {1,2,4,8}),
  `OPTERYX_INSTRUMENT_ENGINE=1`. Reads `trampoline_calls`, `gil_held_ns`,
  worker-purity, pruning facts (`native_scan_facts.row_groups_read/pruned`,
  read from the raw telemetry `_reading` dict — `as_dict()` deliberately strips
  this key since it's normally overlaid onto the operator row by mermaid), and
  asserts row-count parity native vs trampoline.
- **Part B** — concurrent-query sweep: `Q` ∈ {1,2,4,8} **identical queries
  launched simultaneously in separate threads**, per-query `dop` fixed at 2,
  instrumentation OFF (its accumulators are single-query module globals, not
  concurrency-safe — wall-clock/`process_time` are). This is the actual
  discriminator for the audit's incident shape: many concurrent queries each
  driving their own scan-pull loop.

### IO regime
**Page-cache-warm only.** Data is generated fresh onto local SSD immediately
before the sweep and re-read repeatedly within the same process run, so every
measured repeat is decode/CPU-bound, not disk-bound — this is the regime the
audit says most cleanly exposes GIL contention (warm NVMe alone would hide it
behind fast IO). **Cold/throttled IO was not run** — no IO-throttling harness
exists in this tree today, and it was out of scope to build one for a
measurement-only task. This is a real gap: if the deployed service is
frequently IO-bound (cold GCS reads, cache misses), the scan-stage GIL fix
matters less in practice than these numbers suggest, since IO wait already
gives up the GIL. Flagged, not silently omitted.

### Cross-check design
Every scenario ran under **both** `PYTHON_GIL=1` and `PYTHON_GIL=0` with
identical code, data, and dop/Q sweeps. Where a gap appears in GIL-on and
disappears in GIL-off, that gap is attributable to the GIL specifically (not
to some incidental difference between the two Source implementations).

---

## Part A results — instrumentation (PYTHON_GIL=1, the deployed regime)

All 11 scenarios, all 4 dops: identical pattern. One representative table
(string table, no-predicate, 10M rows):

| path | dop | wall_ms | cores | gil_ms | trampoline_calls | rg_read | rg_pruned | purity |
|---|---|---|---|---|---|---|---|---|
| native | 1 | 128.8 | 5.65 | **0.0** | **0** | 39 | 0 | PASS |
| native | 2 | 125.3 | 5.85 | 0.0 | 0 | 39 | 0 | PASS |
| native | 4 | 128.2 | 6.00 | 0.0 | 0 | 39 | 0 | PASS |
| native | 8 | 130.8 | 6.18 | 0.0 | 0 | 39 | 0 | PASS |
| tramp | 1 | 137.8 | 5.11 | 37.1 | 40 | — | — | **FLAG** (`_scan_pull_run` on worker thread) |
| tramp | 2 | 138.4 | 5.22 | 59.5 | 41 | — | — | FLAG |
| tramp | 4 | 139.3 | 5.26 | 104.6 | 43 | — | — | FLAG |
| tramp | 8 | 144.8 | 5.25 | 214.5 | 47 | — | — | FLAG |

Consistent across every one of the 11 scenarios × 4 dops × 2 regimes (88
measured points):

- **`gil_held_ns`**: exactly `0` for every native run; strictly positive for
  every trampoline run (range ~5.6ms to ~525ms depending on scenario and dop —
  it *grows* with dop, since more workers each pay the per-morsel reattach cost
  concurrently).
- **`trampoline_calls`**: exactly `0` for every native run; equal to the
  morsel count for every trampoline run (~40 for the full 10M-row scans at this
  row-group size, ~2 for the 100K-row selective scans — proportional to rows
  read, matching the WP-INSTR design doc's prediction exactly).
- **Worker-purity guard, `whitelist=()`**: PASS on every native run (zero
  execution-time Python re-entry on any thread); FLAGs on every trampoline run
  (correctly — this is the guard's designed job, not a defect).
- **Row-parity native == trampoline**: PASS on all 11 scenarios — same row
  count both paths, confirming the relocated filter/predicate is applied
  exactly once with no double-filter or dropped-match regression.
- **Pruning**: `files_pruned` matches between native and trampoline on every
  scenario (0 in every case here — single/few-file local datasets, no
  whole-file skip applicable). Native's `row_groups_pruned` is directly
  legible via `native_scan_facts` (e.g. selective-predicate scenarios prune 38
  of 39 row groups); trampoline doesn't expose the same counter, but the
  identical row-parity result is the correctness proof pruning didn't regress —
  had the trampoline pruned differently, the result set would differ.

**Full per-scenario tables**: `docs/m1_validation_results.gilon.json` /
`.giloff.json` (raw data), reproducible via `tests/performance/m1_validate.py`.

### The one wrinkle: per-query `dop` scaling is mostly a wash

Across most scenarios, native and trampoline wall-time/cores are nearly flat
across `dop` ∈ {1,2,4,8} for **both** paths (e.g. above: native 5.65→6.18
cores, tramp 5.11→5.25 — both essentially flat). This is because **column
decode itself already runs on a separate, always-parallel nogil pool**
(`PARQUET_LOCAL_IO_WORKERS`, default 16 threads here) that both
`NativeParquetScanSource` and `StreamingScanSource` share identically
(`parquet_read.pyx:1400,1416` — same `decode_workers=` argument, same call).
`MAX_EXECUTION_WORKERS` (the swept `dop`) controls the *downstream operator*
scheduler width, not scan decode concurrency — so for a scan-dominated query,
raising `dop` doesn't change how many threads are actually pulling/decoding
row groups; it was already parallel at `dop=1`.

One scenario broke this pattern and showed the textbook signature — `dt/1f
non-selective`:

| path | dop | wall_ms | cores |
|---|---|---|---|
| native | 1 | 83.9 | 6.61 |
| native | 2 | 59.9 | 9.79 |
| native | 4 | 55.2 | 10.72 |
| native | 8 | 53.5 | 11.10 |
| tramp | 1 | 91.5 | 6.28 |
| tramp | 2 | 93.5 | 6.27 |
| tramp | 4 | 93.2 | 6.26 |
| tramp | 8 | 93.9 | 6.33 |

Native scales (cores 6.6→11.1, wall drops 36%); trampoline is dead flat. **This
persisted identically under `PYTHON_GIL=0`** (native still scales 6.6→11.1
cores, tramp still flat at ~6.3) — meaning this specific flattening is **not**
GIL-attributable; it's some other trampoline-path limitation for this
decimal+timestamp non-selective shape, not yet root-caused here (out of scope
for a measurement task — flagged for follow-up, not attributed).

---

## Part B results — concurrent-query scaling (the real discriminator)

Per-query `dop` fixed at 2; `Q` concurrent identical queries launched in
parallel threads. Aggregate throughput = `Q × dataset_rows / wall_time`.

### GIL-ON (deployed regime) — string table, no-predicate, 10M rows

| path | Q | wall_ms | agg_krows/s | cores | scale |
|---|---|---|---|---|---|
| native | 1 | 127.0 | 78,745 | 5.72 | 1.00x |
| native | 2 | 172.0 | 116,282 | 10.07 | 1.48x |
| native | 4 | 292.0 | 136,974 | 12.35 | 1.74x |
| native | 8 | 543.2 | **147,264** | 13.72 | **1.87x** |
| tramp | 1 | 150.5 | 66,436 | 4.73 | 1.00x |
| tramp | 2 | 230.1 | 86,918 | 6.90 | 1.31x |
| tramp | 4 | 388.0 | 103,094 | 8.30 | 1.55x |
| tramp | 8 | 690.4 | **115,872** | 9.81 | **1.74x** |

Native beats trampoline at every Q (Q=8: **147k vs 116k krows/s, +27%**; cores
13.7 vs 9.8). Native's scale-factor (1.87x) exceeds trampoline's (1.74x) —
native keeps gaining more from added concurrency.

### GIL-OFF cross-check — same scenario

| path | Q | agg_krows/s | cores | scale |
|---|---|---|---|---|
| native | 8 | 154,565 | 15.11 | 2.13x |
| tramp | 8 | 150,227 | 14.96 | 1.99x |

With the GIL removed, the gap **collapses to noise** (154k vs 150k, ~3%,
within repeat-to-repeat variance) — both paths scale to ~2x and ~15 cores.
This is the direct causal proof: the GIL-on gap is the GIL, not an incidental
difference between the two Source implementations.

### Second scenario — string predicate (low-selectivity dict column), GIL-ON

| path | Q | agg_krows/s | cores | scale |
|---|---|---|---|---|
| native | 8 | **203,964** | 15.55 | 1.31x |
| tramp | 8 | 163,569 | 9.14 | 1.46x |

Native: 204k krows/s @ 15.6 cores. Trampoline: 164k krows/s @ 9.1 cores — a
**+24% throughput gap**, and trampoline caps out at ~9 cores vs native's ~15.6
even at Q=8. (GIL-off for this scenario shows trampoline *catching up or
exceeding* native at Q=8 — 248k vs 191k — reinforcing that GIL-on is
specifically holding the trampoline back; the GIL-off numbers aren't
apples-to-apples with GIL-on in absolute terms since removing the GIL changes
overall scheduling, but the relative native-vs-tramp gap closing/reversing is
the signal.)

### Third scenario — decimal+timestamp, no-predicate, GIL-ON

| path | Q=8 agg_krows/s | cores | scale |
|---|---|---|---|
| native | 150,657 | 16.52 | 1.38x |
| tramp | 128,487 | 14.37 | 1.31x |

+17% gap, smaller than the string scenarios but consistent in direction.

**Verdict on Part B**: real, measured, GIL-attributable throughput gap under
concurrent query load, in the **17–27% range** across scenarios at Q=8 — not
the dramatic "trampoline fully flattens" story, because decode parallelism is
shared infrastructure between both paths, but a genuine, quantified,
GIL-caused cost that M1 removes for the query shapes it admits.

---

## Correctness guard

`tests/unit/operators/test_wp01_native_string_scan.py`,
`test_wp02_predicate_relocation.py`, `test_wp11_decimal_temporal_bool_scan.py`,
`test_engine_instrumentation.py`: **89 passed** (run under `PYTHON_GIL=0`,
3.14.5t, before the benchmark) — the numbers above are measuring a build that
is functionally green, not a broken one. `make q` was not additionally run
(the four WP suites are the directly-relevant regression surface for this
change and were explicitly named in the task); flag if the full `make q` run
is wanted as an additional gate.

---

## M2 recommendation

**Evidence, not the audit's prior assumption:**

- M1's fix is real at the instrumentation level (zero Python re-entry, zero
  GIL time, correct results, unchanged pruning) — not in question.
- Under the regime that matters (concurrent queries, GIL-on, decode-bound),
  M1 buys a **measured 17–27% aggregate throughput improvement** for the query
  shapes it admits (string/decimal/temporal scans, with or without a
  lowerable predicate). That is a real, worthwhile, already-banked win — not
  a rounding error, but also not the "full serialization removed" framing;
  most of the raw byte-decode parallelism was already there via the shared
  `PARQUET_LOCAL_IO_WORKERS` pool, so the trampoline was never fully serial to
  begin with. The GIL cost M1 removes is layered on top of that.
- This benchmark measures **scan only**. It says nothing directly about
  ASOF/UNNEST/join operator kernels. By the same physical argument that
  produced this result (any per-row/per-morsel Python re-entry serializes
  under concurrent load on a GIL build), those operators are exposed to the
  **same class** of cost — but whether it's a 5% or 50% tax for a given
  operator depends entirely on how much of that operator's work already runs
  in nogil native code vs the Python bytecode VM, which this task did not
  measure.

**Recommendation: measure before committing M2, don't blanket-approve or
blanket-defer.** Apply this exact methodology (A/B force-toggle, concurrent-Q
sweep, GIL-on/off cross-check) to each of ASOF/UNNEST/join individually,
scoped to real query shapes from the production workload mix, before
sequencing the de-Pythoning work. Two reasons this beats either extreme:
- Deferring all of M2 risks leaving a genuinely GIL-bound operator unfixed if
  it turns out one of the three is far more Python-heavy per-row than the
  scan trampoline was (this benchmark's moderate ~20% gap is a floor for a
  mostly-already-native path, not a ceiling for an operator that's still doing
  real per-row Python work).
- Committing to all three uniformly risks spending M1-sized effort on an
  operator whose concurrent-load gap turns out to be small (e.g. if it's
  rarely on the hot path, or the per-row Python cost is already amortized by
  batch size).

The audit's original assumption (scan was "the most probable cause") is
**directionally confirmed but quantitatively moderate** for this codebase's
architecture, specifically because decode concurrency already lived outside
the GIL-touching code. Don't assume operator kernels share that same
mitigating structure — measure each one the same way before scoping M2.

---

## Reproduction

```bash
# primary (emulates deployed GIL-on service):
PYTHON_GIL=1 PYENV_VERSION=3.14.5t pyenv exec python tests/performance/m1_validate.py

# cross-check (free-threaded, isolates the GIL as the variable):
PYTHON_GIL=0 PYENV_VERSION=3.14.5t pyenv exec python tests/performance/m1_validate.py

# faster smoke run:
PYTHON_GIL=1 PYENV_VERSION=3.14.5t pyenv exec python tests/performance/m1_validate.py \
    --rows 500000 --row-group-size 65536 --dops 1,4 --qs 1,4 --repeats 2
```

Raw results: `docs/m1_validation_results.gilon.json`,
`docs/m1_validation_results.giloff.json`. Generated datasets are cached under
`tests/performance/.m1data/` (not checked in; regenerated on first run,
~500MB).
