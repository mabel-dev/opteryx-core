# Rugo + Draken Dictionary Native Hardening Release Notes

Date: 2026-03-05
Scope: Final hardening pass for the 5-phase dictionary-native rollout.

## Summary

Dictionary-native execution is now the stable default in motor paths for decode, expression, grouping, and spill.
Temporary rollout gates and strict/fallback scaffolding used during phased rollout have been retired.

## Behavior Changes

1. Native Parquet dictionary decode is always enabled (subject to ratio control), not feature-gated.
2. Dictionary expression fast paths are always enabled for dictionary candidates.
3. Dictionary group-by fast paths are always enabled for eligible dictionary shapes.
4. Group-by compiled kernels no longer accept or branch on `enable_dict_fastpath`; selection is shape-based.
5. Unsupported dictionary expression motor-path cases fail explicitly (no silent degrade path in the motor).

## Retired Environment Flags

The following env vars are retired and ignored (with compatibility warning if set):

1. `FEATURE_DRAKEN_DICT_EXPR_STRICT`
2. `FEATURE_DRAKEN_DICT_EXPR_FASTPATH`
3. `FEATURE_DRAKEN_DICT_GROUPBY_FASTPATH`
4. `FEATURE_PARQUET_NATIVE_DICTIONARY`

Active control retained:

1. `PARQUET_DICT_MAX_CARDINALITY_RATIO`

## Performance Outcomes (Local Benchmarks)

1. Decode (200k rows, low-cardinality strings): dictionary path used less RSS and less output storage while improving decode latency versus materialized fallback.
2. Expressions (200k rows): numeric range predicates improved by ~2.1x-2.3x; `LIKE/ILIKE` improved by ~5.2x-6.2x versus materialized baseline.
3. Group-by (200k rows): `COUNT(*)` improved up to ~1.5x; `COUNT(DISTINCT)` improved ~2.1x-2.8x versus materialized baseline.
4. Spill (200k rows, lz4): dictionary spill reduced payload size substantially and improved write/read latency versus materialized spill.

## Validation

1. Targeted dictionary unit/integration suites pass.
2. Quick battery (`make t`) passes under stable defaults.
3. Broad `tests/unit tests/draken tests/rugo` collection still has unrelated pre-existing environment/import blockers:
   - `parse_yaml` import
   - `AsyncMemoryPool` import
   - missing `paged_memory_pool`
   - missing `opteryx_catalog`

## Upgrade Notes

1. Remove retired dictionary rollout env vars from deployment configs.
2. Keep `PARQUET_DICT_MAX_CARDINALITY_RATIO` tuned to workload cardinality distribution.
3. Treat broad-suite collection blockers as separate platform/environment work, not dictionary rollout blockers.

