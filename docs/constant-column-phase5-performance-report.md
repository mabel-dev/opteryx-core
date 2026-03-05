# Constant Column Phase 5 Performance Report

Date: March 5, 2026  
Branch state: local working tree after Phase 5 fixes (including constant groupby finalize-mode fix)

## Scope

This report captures reproducible measurements for constant-column benefits against a non-constant baseline path.

Harness:
- `tests/performance/benchmarks/bench_constant_columns_phase5.py`

Scenarios compared:
1. `constant`: key column is `ConstantVector("g")`
2. `materialized`: key column is a normal Arrow string column with repeated `"g"` values

Metrics:
1. Group-by runtime (`COUNT(*) GROUP BY key`)
2. Predicate runtime (`key = 'g'`)
3. DRKM spill write runtime and payload size
4. Peak RSS (subprocess `ru_maxrss`)

## Reproduction

```bash
python tests/performance/benchmarks/bench_constant_columns_phase5.py --rows 250000 --repeat 5
python tests/performance/benchmarks/bench_constant_columns_phase5.py --rows 1000000 --repeat 3
```

## Results

### Run A (`rows=250000`, `repeat=5`)

| case | groupby (ms) | predicate (ms) | spill (ms) | spill (MB) | peak RSS (MB) |
|---|---:|---:|---:|---:|---:|
| constant | 0.12 | 0.03 | 0.36 | 1.91 | 109.17 |
| materialized | 64.24 | 3.12 | 0.38 | 3.10 | 123.42 |

Relative (`materialized / constant`):
- groupby: `516.94x`
- predicate: `120.59x`
- spill bytes: `1.62x`

### Run B (`rows=1000000`, `repeat=3`)

| case | groupby (ms) | predicate (ms) | spill (ms) | spill (MB) | peak RSS (MB) |
|---|---:|---:|---:|---:|---:|
| constant | 0.46 | 0.03 | 0.93 | 7.63 | 126.53 |
| materialized | 109.74 | 9.24 | 5.01 | 12.40 | 182.27 |

Relative (`materialized / constant`):
- groupby: `239.22x`
- predicate: `271.88x`
- spill bytes: `1.62x`

## Interpretation

1. Constant-key group-by fastpath produces large, consistent wins in wall time.
2. Constant predicate fastpath also shows large speedups versus non-constant string vector path.
3. Constant-native spill reduces payload size materially (`~1.62x` smaller in both runs).
4. Peak RSS is lower for constant case in both runs.

## Non-Constant Regression Signal

1. The materialized case exercises non-constant execution path as baseline in the same harness.
2. Quick suite remained green after these changes:
   - `python tests/integration/sql_battery/run_shapes_battery.py` (all passed)

## Conclusion

Phase 5 performance goals for constant columns are demonstrably materializing for runtime and memory/size metrics, with no observed quick-suite regression.
