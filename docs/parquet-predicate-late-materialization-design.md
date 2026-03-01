# Parquet Predicate Filtering And Late Materialization Design

## Objective

Restore predicate correctness for Parquet scans first, then add an optional
late-materialization optimization for selective queries.

Core principle:

- Correct results are mandatory.
- Performance optimizations are only valid after correctness is guaranteed.

---

## Context

The scan path has been simplified to Parquet-only execution. This removed
legacy format decoders and old reader stacks. As part of that simplification,
predicate handling in the Parquet scan path currently relies heavily on
metadata pruning and does not consistently apply row-level predicate masking in
all scan shapes.

This document defines a two-phase plan:

1. correctness-first row-level filtering
2. selective late materialization

---

## Current State (Summary)

What we have now:

1. Manifest-level file pruning (BRIN-like stats pruning at planning time)
2. Parquet footer min/max row-group pruning
3. Projected-column range reads and decode
4. Row-group emission to execution pipeline

What is missing or incomplete for pushed predicates:

1. Guaranteed row-level predicate evaluation/masking against decoded row-group data
2. Separation of filter columns versus final projection columns
3. Optional second-stage fetch for non-filter projected columns

---

## Proposed Pipeline

Target end-state pipeline:

1. Prune files using manifest statistics
2. Prune row groups using Parquet footer statistics
3. Read columns required for predicate evaluation
4. Evaluate predicates and build row mask
5. Read remaining projected columns only when needed (optional optimization)
6. Apply row mask and emit final projected morsel

---

## Architectural Boundary

Keep responsibilities split to avoid overloading the I/O layer:

- `opteryx/parquet_io/*`
- Owns byte-range planning, fetch scheduling, decode, and metadata pruning.
- Does not own SQL predicate semantics.

- `opteryx/operators/parquet_read_node.py`
- Owns SQL-level predicate evaluation and row masking.
- Owns final projection assembly and emitted morsel correctness.

Rationale:

- Keeps storage/decode subsystem reusable and simpler.
- Avoids embedding SQL expression semantics deep in I/O scheduler code.
- Reduces blast radius for future predicate/function changes.

---

## Phase 1: Correctness First

### Goal

Guarantee correct predicate results for Parquet scans with pushed predicates.

### Behavior

For each row group:

1. Determine `filter_columns` from pushed predicates.
2. Determine `projection_columns` from query projection.
3. Read and decode `required_columns = projection_columns ∪ filter_columns` in one pass.
4. Evaluate predicate expression at row-level.
5. Build boolean mask.
6. Apply mask to all decoded required columns.
7. Drop filter-only columns not requested by projection.
8. Emit correctly filtered projected morsel.

### Notes

- If predicate extraction cannot produce evaluable row-level expressions,
  fail open for pruning but still apply row-level evaluation where expression
  objects exist.
- `LIMIT` behavior stays unchanged and applies after filtering.
- Repeated/list fallback path must preserve the same filtering correctness.

### Expected Outcome

- Restores correctness for integration battery and shape tests.
- Minimal scheduler complexity increase.
- No second-read optimization yet.

---

## Phase 2: Late Materialization (Optional Optimization)

### Goal

Reduce bytes read/decode for selective predicates on wide tables.

### Behavior

For each row group:

1. Read/decode filter columns only.
2. Evaluate predicates and compute mask/selectivity.
3. If no rows match, skip projection read entirely.
4. If rows match and projection has additional columns, read/decode only those columns.
5. Apply mask to projection columns and emit.

### Heuristic Gate

Enable late materialization only when all are true:

1. Estimated selectivity is below a threshold
2. There are projected columns not in filter columns
3. Projected bytes are materially larger than filter bytes

Fallback to Phase 1 one-pass read when heuristic says no benefit.

### Risks

1. More scheduler complexity
2. Potential latency increase for non-selective predicates
3. More intricate cancellation and in-flight accounting

---

## Effort vs Benefit

### Phase 1

Estimated effort:

- 1 to 2 engineering days

Benefits:

1. High correctness impact
2. High test stability impact
3. Low operational risk

Recommendation:

- Do immediately.

### Phase 2

Estimated effort:

- 4 to 8 engineering days including tuning and test hardening

Benefits:

1. Meaningful performance gains for selective wide scans
2. Limited gains for narrow projections or non-selective predicates

Recommendation:

- Implement behind a feature flag after Phase 1 is stable.

---

## Correctness Requirements

The following must hold before Phase 1 is considered complete:

1. Row-level predicate outcomes match existing SQL semantics for AND/OR/NOT combinations already pushed.
2. Null semantics remain SQL-correct.
3. Emitted columns exactly match requested projection.
4. `LIMIT` and ordering behavior remain unchanged.
5. Row counts in SQL battery parity tests match expected values.

---

## Observability

Add or validate telemetry for:

1. `parquet_filter_columns_read`
2. `parquet_projection_columns_read`
3. `parquet_rows_before_filter`
4. `parquet_rows_after_filter`
5. `parquet_filter_selectivity`
6. `parquet_late_materialization_used` (Phase 2)

These metrics are needed to justify Phase 2 and tune thresholds.

---

## Rollout Strategy

1. Ship Phase 1 without feature flag, because it is correctness behavior.
2. Gate Phase 2 with `FEATURE_PARQUET_LATE_MATERIALIZATION` default off.
3. Run A/B benchmarks for selective and non-selective query sets.
4. Enable Phase 2 by default only after parity and regression targets are met.

---

## Open Questions

1. Which predicate forms are guaranteed pushed into `ParquetReadNode.predicates` today?
2. Do we need explicit handling for complex expressions that cannot be vectorized?
3. Should repeated/list-column fallback remain full-file decode in both phases?
4. What selectivity threshold should be default for Phase 2 gate?

---

## Task List

### Phase 1 - Correctness

- [x] Add explicit extraction of `filter_columns` in `ParquetReadNode`.
- [x] Build `required_columns = projection ∪ filter_columns` for row-group reads.
- [x] Ensure row-group reader returns all required columns for evaluation.
- [x] Add row-level predicate evaluation on decoded row groups before emit.
- [x] Apply boolean mask to all required columns.
- [x] Drop filter-only columns before morsel construction.
- [x] Keep identity/name mapping correct after masking and projection.
- [x] Ensure repeated/list fallback path applies identical predicate semantics.
- [x] Add unit tests for filtered projection where filter column is not projected.
- [x] Add unit tests for null-sensitive predicates.
- [x] Add integration parity tests for SQL battery predicate cases.
- [x] Add telemetry counters for rows before/after filter.

### Phase 2 - Late Materialization

- [ ] Add feature flag `FEATURE_PARQUET_LATE_MATERIALIZATION`.
- [ ] Implement filter-only first pass per row group.
- [ ] Implement conditional projection-column second pass.
- [ ] Add selectivity and projected-bytes heuristic gate.
- [ ] Add no-match fast path that skips projection fetch.
- [ ] Add scheduling safeguards for second-pass reads.
- [ ] Add cancellation behavior for limit/early-stop in two-pass mode.
- [ ] Add telemetry for late-materialization usage and savings.
- [ ] Add benchmarks for selective vs non-selective workloads.
- [ ] Run A/B with feature on/off and document results.

### Exit Criteria

- [ ] Phase 1: SQL battery predicate correctness parity achieved.
- [ ] Phase 1: No regressions in parquet scheduler tests.
- [ ] Phase 2: Demonstrated win on selective wide scans.
- [ ] Phase 2: No material regression on non-selective scans.
