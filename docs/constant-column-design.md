# Constant Column Native Encoding Design

## Context

Constant columns (all rows share one value, optionally with nulls) are common:
1. `SELECT 1 AS batch_id, ...`
2. constant projections from folded expressions
3. constant dimensions in grouped outputs

Today, many constants are expanded to full-width arrays in Python expression paths, which defeats memory and execution efficiency.

## Goals

1. Represent constant columns in Draken with O(1) value storage (+ optional null bitmap).
2. Preserve constant representation through execution motor paths where possible.
3. Speed up predicate/grouping behavior on constant columns without regressing non-constant paths.
4. Keep design aligned with engine principles (no new Python/Arrow/NumPy in motor kernels).

## Non-Goals (v1)

1. Arrow-side constant detection/import (no O(n) constant scanning during Arrow import).
2. Constant-specialized sort/distinct kernels.
3. New Parquet constant encoding (constants are produced in planner/operator/runtime, not from Parquet format).
4. Arrow-first execution paths outside Draken motor flow are not part of v1 optimization guarantees.

## Engine-Principle Constraints

1. Python is glue only: constant motor kernels must not rely on Python per-row loops.
2. Arrow is boundary only: constant kernels do not call Arrow compute.
3. No new NumPy in motor code for constant paths.
4. Fail visibly on malformed constant buffers; no silent broad fallback.

## Key Design

### 1) Draken Type + Buffer

Add `DRAKEN_CONSTANT = 62` to `third_party/mabel/draken/core/buffers.h`.

Add a buffer:

```c
typedef struct {
    DrakenType type;          // DRAKEN_CONSTANT
    DrakenType value_type;    // scalar value type
    void* value;              // owned scalar payload (or owned child payload handle)
    size_t length;            // logical row count
    uint8_t* null_bitmap;     // optional row validity bitmap
} DrakenConstantBuffer;
```

Notes:
1. `length` is `size_t` (matches existing Draken buffers).
2. `null_bitmap == nullptr` means all rows valid.
3. Constant value is ignored on null rows.
4. v1 supported constant value types: `INT64`, `FLOAT64`, `BOOL`, `STRING`.
5. All-null constant columns are valid in v1:
   - `null_bitmap` marks all rows null.
   - `value` may point to a type-valid placeholder payload and is never read for null rows.

### 2) ConstantVector Wrapper

Create:
1. `third_party/mabel/draken/vectors/constant_vector.pxd`
2. `third_party/mabel/draken/vectors/constant_vector.pyx`

Core API (v1):
1. `__getitem__`
2. `to_pylist`
3. `to_arrow` (expand to full Arrow array)
4. `take` (v1 may materialize)
5. `hash_into`
6. `compress_into`
7. predicates: `equals`, `not_equals`, `in_list`

`take()` semantics (v1):
1. Empty or all-null take result remains `ConstantVector`.
2. Any non-empty index selection over a constant input remains `ConstantVector` with updated length/null bitmap.
3. v1 does not require fast-pathing arbitrary mixed-type index containers; non-native index inputs may materialize indices first, but result should remain constant when possible.

`hash_into()` semantics (must match existing grouping/distinct semantics):
1. Native 64-bit primitives hash/key as their raw 64-bit value.
2. Variable-width values (for example `STRING`) hash/key via XXHash64(value bytes).
3. Null rows use existing engine null-hash behavior.

Implementation pattern:
1. `.pyx/.pxd` first-class implementation (consistent with current Draken vectors).
2. Generated `.cpp` is an artifact, not the design target.
3. Add manual native helper only if profiling proves necessary.

### 3) Planner/Executor Handoff

Constants must be emitted before Python literal expansion paths.

Execution contract:
1. Planner/binder marks constant output expressions (`ConstantColumn`/literal-folded outputs).
2. Execution node producing the projection emits `ConstantVector` directly.
3. Downstream operators consume constant vectors without forcing expansion unless required.

Primary node targets:
1. `ProjectionNode` (first and required)
2. follow-on support in aggregate/group output paths where constants appear

### 4) Expression and Grouping Behavior

Expression path:
1. Add constant-aware dispatch in expression ops for supported predicates.
2. Avoid per-row materialized comparisons for constant-vs-literal cases.

Grouping path:
1. Detect constant group keys and avoid unnecessary hash/group state work when semantics allow.
2. Preserve existing null semantics.

### 5) Arrow Interop Scope (v1)

1. `ConstantVector.to_arrow()` is supported (expanded Arrow array).
2. Arrow import does not attempt constant detection in v1.
3. Constant import detection can be a v2 metadata-driven optimization.

### 6) Spill/DRKM Behavior (v1)

1. v1 should preserve constant representation across DRKM spill/restore where practical.
2. If constant-native DRKM segment support is not implemented in the first pass, spill must materialize explicitly and visibly (telemetry-backed) with correctness parity.
3. Preferred direction: add constant-native DRKM encoding/decoding to avoid expansion churn in spill-heavy workloads.

## Implementation Plan

### Phase 1: Foundation

Core:
- [ ] Add `DRAKEN_CONSTANT` enum entry.
- [ ] Add `DrakenConstantBuffer` with `size_t length`.
- [ ] Implement `ConstantVector` skeleton (`.pxd/.pyx`) with ownership + null semantics.

Tests:
- [ ] Buffer lifecycle and ownership tests.
- [ ] Null bitmap semantics tests.
- [ ] Basic value-type coverage tests.
- [ ] All-null constant column tests.

### Phase 2: Projection Wiring

Core:
- [ ] Add planner/executor handoff for constant outputs.
- [ ] Update `ProjectionNode` output path to emit `ConstantVector` for eligible columns.
- [ ] Ensure constant outputs bypass Python literal full-width expansion paths.

Tests:
- [ ] `SELECT 42`/`SELECT 42, 'x', NULL` projection shape tests.
- [ ] Mixed constant + non-constant projection tests.

### Phase 3: Predicate + Grouping Consumption

Core:
- [ ] Add constant-aware predicate dispatch for supported ops.
- [ ] Add constant group-key optimization path in group planning/runtime.
- [ ] Add explicit malformed-buffer validation checks.

Tests:
- [ ] Predicate parity tests vs materialized baseline.
- [ ] Group-by parity tests for constant keys.
- [ ] Null behavior parity tests.

### Phase 4: Hardening + Export

Core:
- [ ] Finalize `to_arrow()` behavior and docs.
- [ ] Implement or explicitly defer constant-native DRKM spill format with measured impact.
- [ ] Add regression guards preventing Python/Arrow/NumPy creep in constant motor paths.
- [ ] Add benchmark harness and reproducible report.

Tests/Benchmarks:
- [ ] Memory benchmark: large constant column vs materialized baseline.
- [ ] Predicate microbench on constant columns.
- [ ] Spill benchmark: constant-native vs materialized spill path.
- [ ] Non-constant regression benchmark.

## Implementation Status (2026-03-05)

Completed in code:
1. `DRAKEN_CONSTANT` + `DrakenConstantBuffer` are implemented in Draken core buffers.
2. `ConstantVector` is implemented with native ownership/null handling, predicates, hashing, `take`, and Arrow export.
3. DRKM spill/restore supports constant-native encode/decode paths (no forced materialization for supported constant types).
4. `vector_from_sequence` detects constant Python sequences and emits `ConstantVector`.
5. Expression predicate fastpath supports constant vectors (`Eq`, `NotEq`, `InList`, `NotInList`, `Lt`, `Gt`, `LtEq`, `GtEq`) with telemetry.
6. Projection/evaluation now keeps constant literals native on `Morsel` paths:
   - literal-only projections keep `ConstantVector` end-to-end
   - mixed non-literal + literal evaluation keeps literal outputs as `ConstantVector`
7. Projection telemetry now includes `draken_constant_columns_emitted` on native morsel projection paths.
8. Unit coverage exists for:
   - constant vector semantics
   - constant expression fastpath
   - morsel literal projection + mixed projection constant preservation
   - group-by constant-output telemetry (single-group hit vs multi-group fallback)
9. `ConstantVector.to_arrow()` no longer depends on `pyarrow.compute`; null application uses direct Arrow buffer assembly.
10. Initial constant motor guard tests are in place (`constant_vector.pyx`: no NumPy dependency, no Arrow compute dependency).
11. Runtime constant group-key fastpath is implemented for:
    - single-key `COUNT(*)`
    - single-key `COUNT(col)`
    - single-key `SUM` / `MIN` / `MAX` / `AVG` / `COUNT(DISTINCT)`
    when the key input vector is `ConstantVector`, with telemetry counters:
    - `draken_constant_groupby_fastpath_hits`
    - `draken_constant_groupby_fastpath_fallbacks`
12. Phase-5 guard coverage now checks multiple constant motor files (`constant_vector.pyx`, compiled group-state store, DRKM morsel I/O) for:
    - no NumPy imports
    - no `pyarrow.compute` usage
    - no `.to_pylist(...)` calls in compiled constant group-by path
13. Reproducible benchmark harness added:
    - `tests/performance/benchmarks/bench_constant_columns_phase5.py`
    - compares constant vs materialized repeated-key paths for group-by runtime, predicate runtime, and DRKM spill bytes/time.
14. Fixed backend finalize-mode interaction:
    - when constant fastpath populates `_states`, finalize no longer incorrectly short-circuits through empty int64-typed finalize modes.
    - this restores correct grouped outputs for constant-key aggregates like `SUM`.
15. Reproducible performance report artifact added:
    - `docs/constant-column-phase5-performance-report.md`
    - includes runtime + memory/size measurements for constant vs materialized baseline at two scales.
16. Projection constant handoff now also covers Arrow-input projection literals:
    - `ProjectionNode` routes Arrow inputs with literal projections through a Morsel evaluation path.
    - this preserves `ConstantVector` emission and telemetry (`draken_constant_columns_emitted`) even when upstream operators emitted Arrow tables.

Still pending for full completion:
1. End-to-end planner/executor constant handoff across remaining non-Projection projection-producing paths (for example legacy aggregate/group nodes that still force Arrow materialization before expression projection).

## Telemetry

Add counters:
1. `draken_constant_columns_emitted`
2. `draken_constant_predicate_fastpath_hits`
3. `draken_constant_predicate_fastpath_fallbacks`
4. `draken_constant_groupby_fastpath_hits`
5. `draken_constant_groupby_fastpath_fallbacks`
6. `draken_constant_spill_materializations`

## Test Plan

### Unit

1. `ConstantVector` lifecycle, `__getitem__`, null bitmap semantics.
2. Type dispatch for int/float/string constant payloads.
3. `hash_into`/`compress_into` correctness.

### Integration

1. Projection emits `ConstantVector` on eligible constant outputs.
2. Filter parity on constant columns vs materialized baseline.
3. Group-by parity with constant keys.
4. Spill/restore parity on constant columns.

### Regression Guards

1. Source-level guard tests for constant motor paths:
   - no Arrow compute tokens
   - no new NumPy tokens
   - no Python materialization tokens in motor sections

### Performance

1. Memory reduction for large constant columns.
2. Constant predicate and group-key speedups.
3. No measurable regressions on non-constant workloads.

## Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Constant path bypassed by early Python materialization | Enforce planner/executor constant handoff and projection emission |
| Ownership/null correctness bugs | strict constructor/destructor invariants + bitmap tests |
| Engine-principle drift | add motor-path regression guard tests |
| Over-scoped v1 | keep Arrow constant import detection out of v1 |

## Success Criteria

1. Constant columns are emitted as `ConstantVector` in supported projection paths.
2. Constant storage is O(1) with respect to row count (plus bitmap when needed).
3. Predicate/grouping correctness matches materialized baseline.
4. Documented time/RAM benefits demonstrated on benchmark harness.
5. No regression on non-constant execution paths.

## Release Gates

1. Correctness:
   - Targeted constant unit/integration suites pass.
   - No regressions in quick battery (`make t`) attributable to constant paths.
2. Performance:
   - Constant projection/filter workloads show measurable speedup vs materialized baseline.
   - Constant memory footprint remains O(1)+bitmap and materially below full-column materialization.
3. Stability:
   - Fastpath fallback rates are low and explained by unsupported shapes.
   - If spill materialization remains in v1, its rate/impact is quantified and accepted explicitly.
