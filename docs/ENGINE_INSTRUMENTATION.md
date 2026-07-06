# Native Execution-Engine Instrumentation (WP-INSTR)

This is the measurement harness that later execution-engine performance work
depends on for its pass/fail criteria. It **adds no behaviour** to query results —
it only reads what the engine records. Everything here is off by default and
near-zero cost when disabled.

The headline thing it measures: the native engine crosses back into Python once
per morsel per worker through the scan-pull trampoline (`_scan_pull_run`) for any
scan outside the narrow native gate (int/float only, no predicates, local
parquet). On a standard-GIL build that serialises the scan stage. These
instruments let a later package **prove** it removed that Python touch.

## The four instruments

| # | Reading / tool | Where | Flag |
|---|----------------|-------|------|
| 1 | `gil_held_ns` | query telemetry | `OPTERYX_INSTRUMENT_ENGINE` |
| 2 | `scan_sources` | query telemetry | always on (plan-time fact) |
| 3 | `dev/instrument_engine.py` allocation harness | dev tool | uses flag for #4 fields |
| 4 | `worker_gil_sites` + `assert_native_worker_purity` | telemetry + dev tool | `OPTERYX_INSTRUMENT_ENGINE` |

### 1. `gil_held_ns` — execution-time GIL nanoseconds
Sums the wall-clock nanoseconds spent inside the known execution-time `with gil`
bodies — the scan-pull trampoline (`_scan_pull_run`, once per morsel per worker)
and the carrier-flip error stash (`_stash_exc`). Each site is bracketed with
`clock_gettime(CLOCK_MONOTONIC)` **only when armed**; disarmed, each site reads a
single C flag and branches straight past.

* A native-gated numeric scan touches no execution Python → `gil_held_ns == 0`.
* A string / predicate scan → clearly non-zero. **That number is the baseline
  later packages must drive back to ~0.**

### 2. `scan_sources` — per-scan Source selection
Maps each parquet scan node identity to the Source the compiler wired it to:
`"NativeParquetScanSource"` (zero-Python native pull) or `"StreamingScanSource"`
(the GIL trampoline). Recorded at plan time, so it costs nothing and is always
present. A later package asserts e.g. *"string scans now select
NativeParquetScanSource."*

### 3. Allocation harness — O(morsels), not O(rows)
`dev/instrument_engine.py:measure_query_allocations(sql)` drains a query while
sampling `sys.getallocatedblocks()` and reports:

* `peak_block_delta` / `blocks_per_row` — peak live Python-block footprint. For
  **both** Sources this is O(morsels) (bounded by morsel size), so `blocks_per_row`
  falls toward zero as rows grow. This is the proof native operators do **not**
  hold O(rows) memory.
* `trampoline_calls` — the number of per-morsel Python re-entries through
  `_scan_pull_run`. **This is the real discriminator**: the trampoline's transient
  per-pull allocations are freed before the next morsel boundary, so the live-block
  metric can't see them, but the re-entry count is `0` for a native scan and grows
  with the scan (∝ morsels ∝ rows) for the trampoline. (Requires the flag armed.)

> Honest note: because the trampoline yields native-carrier morsels (Draken
> buffers), not per-row Python objects, its **live memory** is also O(morsels) —
> it does *not* grow O(rows). Its distinguishing cost is per-morsel Python
> re-entry (`trampoline_calls`, `gil_held_ns`), not sustained memory. The harness
> reports both so the distinction is explicit rather than assumed.

### 4. Worker-thread purity guard
`worker_gil_sites` is the enumerated `(thread, site, calls, ns)` breakdown of every
instrumented GIL body that ran on a worker thread.
`dev/instrument_engine.py:assert_native_worker_purity(telemetry, whitelist=…)`
raises `WorkerPurityError` if any site outside the whitelist ran.

* Whitelist today: `("_scan_pull_run", "_stash_exc")`.
* On a native numeric query the list is empty → passes under any whitelist.
* Passing `whitelist=()` turns any execution-time Python re-entry into a failure —
  how a test *deliberately flags* the trampoline path.

**What it catches / limitations.** It is an *enumerated* guard, not a universal
`settrace`: it counts entries into the **instrumented** execution-time `with gil`
bodies. A worker re-entering Python through some other, not-yet-instrumented
`with gil` body is invisible until that body is wrapped in the WP-INSTR block in
`opteryx/operators/_operators.pyx`. This is the invariant future packages extend —
**each de-Pythoned function is removed from the whitelist; each newly discovered
GIL body is added to the instrumentation.**

## Enabling

The GIL instrumentation (instruments 1 & 4) is armed by `execute_native` for the
span of one run when `OPTERYX_INSTRUMENT_ENGINE` is truthy.

```bash
# env var (read at opteryx import)
OPTERYX_INSTRUMENT_ENGINE=1 python your_script.py
```

```python
# in a test: execute_native reads the flag per-call, so monkeypatch is enough
import opteryx.config as config
config.OPTERYX_INSTRUMENT_ENGINE = True
```

`scan_sources` needs no flag. The armed accumulators are **module globals mutated
only under the GIL**, so they are correct for one query at a time but **not** across
concurrent queries in one process — this is a diagnostic instrument.

## Reading the telemetry

```python
import opteryx
s = opteryx.session()
for _ in s.execute_to_morsels("SELECT followers FROM 'testdata/flat/formats/parquet'"):
    pass
t = s._telemetry.as_dict()
t["scan_sources"]      # {'<scan-id>': 'NativeParquetScanSource'}
t["gil_held_ns"]       # 0 for native; >0 for trampoline (armed only)
t["worker_gil_sites"]  # [] for native; [{thread_id, site, calls, ns}, ...] (armed)
```

## Running the harness (`dev/instrument_engine.py`)

```bash
# Full readout + purity guard for one query
OPTERYX_INSTRUMENT_ENGINE=1 python dev/instrument_engine.py \
    --sql "SELECT followers FROM 'testdata/flat/formats/parquet'" --guard

# Self-contained scaling demo: generates sized numeric + string parquet relations
# (native rugo writer, no pyarrow) under OUT_DIR and prints both trends.
OPTERYX_INSTRUMENT_ENGINE=1 python dev/instrument_engine.py --demo-scaling /tmp/instr

# Allocation scaling against your own SQL ({n} substituted per --scale size).
# NB: a scan-pushed LIMIT forces the trampoline Source, so use distinct datasets
# (or --demo-scaling) to compare the NATIVE path across sizes.
python dev/instrument_engine.py \
    --sql "SELECT text FROM 'testdata/flat/formats/parquet' LIMIT {n}" \
    --scale 20000,100000
```

Example `--demo-scaling` output (numeric stays native with zero re-entry; the
string trampoline's re-entry count grows with rows, both flat in memory):

```
== numeric scaling ==
  rows      morsels  peak_blocks  blocks/row   trampoline_c   source
  100000    2        1376         0.0138       0              NativeParquetScanSource
  200000    4        1418         0.0071       0              NativeParquetScanSource
  400000    8        1482         0.0037       0              NativeParquetScanSource
== string scaling ==
  rows      morsels  peak_blocks  blocks/row   trampoline_c   source
  100000    2        1287         0.0129       10             StreamingScanSource
  200000    4        1312         0.0066       12             StreamingScanSource
  400000    8        1382         0.0035       16             StreamingScanSource
```

## Tests

`tests/unit/operators/test_engine_instrumentation.py` exercises all four
instruments (native vs trampoline distinction, off-by-default, the deliberate
trampoline flagging, allocation discriminator).

## Cost when disabled

Each instrumented site is a single C-flag branch when disarmed. Measured
off-vs-on wall clock for both a native and a trampoline query sits within ±2%
run-to-run noise (no measurable regression).
