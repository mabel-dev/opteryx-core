# Native Execution-Engine Instrumentation (WP-INSTR)

A measurement harness the execution-engine performance work uses for its pass/fail
criteria. It **adds no behaviour** to query results — it only reads what the engine
records.

## The instruments

| Reading / tool | Where | Cost |
|----------------|-------|------|
| `scan_sources` | query telemetry | always on (plan-time fact) |
| native-scan refusals | `NativeScanRefusedError` + `dev/native_residual_census.py` | plan time |
| allocation harness | `dev/instrument_engine.py` | dev tool only |

### `scan_sources` — per-scan Source selection
Maps each parquet scan node identity to the native Source the compiler wired it to:
`"NativeParquetScanSource"` (single pass) or `"LatmatScanSource"` (two-pass late
materialization). Recorded at plan time, so it costs nothing and is always present.

### Native-scan refusals — WHY a scan is not read
There is no fallback reader: the per-morsel Python trampoline
(`StreamingScanSource`) was deleted 2026-10-03. A scan that neither native Source
admits is refused at compile time with `NativeScanRefusedError` (a
`NotSupportedError`), whose `relation` and `reason` fields name the scan and the
`_native_scan_plan` guard that fired — e.g. `footer_gate: column 'x' is not in
<file> (row group 0)`, `unlowerable_predicate`, `no_manifest`.

`dev/native_residual_census.py` tallies refusals over the clickbench + tpch battery;
`tests/unit/operators/test_native_scan_residual_gate.py` gates the count at zero and
proves the census can see a refusal. See [`NATIVE_RESIDUAL_PLAN.md`](NATIVE_RESIDUAL_PLAN.md).

### Allocation harness — O(morsels), not O(rows)
`dev/instrument_engine.py:measure_query_allocations(sql)` drains a query while
sampling `sys.getallocatedblocks()` and reports `peak_block_delta` /
`blocks_per_row` — the peak live Python-block footprint. It is O(morsels) (bounded
by morsel size), so `blocks_per_row` falls toward zero as rows grow: the proof that
native operators do not hold O(rows) memory.

```bash
# Readout for one query
python dev/instrument_engine.py --sql "SELECT followers FROM 'testdata/flat/formats/parquet'"

# Self-contained scaling demo: generates sized numeric + string parquet relations
# (native rugo writer) under OUT_DIR and prints both trends.
python dev/instrument_engine.py --demo-scaling /tmp/instr

# Allocation scaling against your own SQL ({n} substituted per --scale size).
python dev/instrument_engine.py \
    --sql "SELECT text FROM 'testdata/flat/formats/parquet' LIMIT {n}" \
    --scale 20000,100000
```

## Removed: the execution-time GIL instrument

`gil_held_ns`, `worker_gil_sites`, the worker purity guard
(`assert_native_worker_purity`) and the `OPTERYX_INSTRUMENT_ENGINE` flag were
removed 2026-10-05. The instrument was armed only by `execute_native`, whose
execution never enters a Python operator body; its two remaining sites
(`_dispatch_push`, `_stash_exc` in `opteryx/operators/_operators.pyx`) run only on
the serial engine's push chain (INSERT … VALUES), which never armed it. It could not
record anything, so its "reads zero" tests proved nothing.

## Tests

`tests/unit/operators/test_engine_instrumentation.py` covers `scan_sources` and the
allocation harness.
