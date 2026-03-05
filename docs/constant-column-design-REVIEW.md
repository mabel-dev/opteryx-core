# Constant Column Design — Review Corrections

## Review Scope

This review updates `docs/constant-column-design.md` based on:
1. Current engine implementation patterns in Draken/Rugo.
2. Current planner/expression/operator behavior in Opteryx.
3. `docs/engine-principles.md` constraints (Python glue, Arrow boundary, no new NumPy in motor).

## What Is Solid

1. The core idea is correct: represent constant columns as a native Draken vector to reduce RAM and avoid per-row work.
2. The proposed integration points are directionally right: buffer/type, vector wrapper, planner/operator production, expression/group-by consumption.
3. The test intent is good: unit + integration + benchmarks.

## Required Corrections

### 1) Literal Materialization Happens Early Today

Current expression evaluation materializes literals to full-width NumPy/PyArrow arrays in Python (`opteryx/managers/expression/__init__.py`), so constant-native benefits are lost unless constants are introduced before that path.

Correction:
1. Constant columns must be produced by planner/operator pathways, not by relying on Python literal evaluation.
2. Use real node names in the plan: `ProjectionNode`, `FilterNode`, `AggregateAndGroupNode`/`DrakenAggregateAndGroupNode` (not generic “ProjectionOperator/SelectionOperator” terms).

### 2) `DRAKEN_CONSTANT` Does Not Exist Yet

`third_party/mabel/draken/core/buffers.h` currently ends string-like types at:
1. `DRAKEN_STRING = 60`
2. `DRAKEN_DICTIONARY = 61`

Correction:
1. Add `DRAKEN_CONSTANT = 62`.
2. Add `DrakenConstantBuffer`.

### 3) Cython Implementation Pattern Was Mischaracterized

The prior review implied hand-authored vector kernels live in dedicated `.cpp` files (for example `dictionary_vector.cpp`). In this codebase, `.cpp` is generated from Cython for most vector kernels.

Correction:
1. Keep the implementation centered on `.pyx` + `.pxd`.
2. Generated `.cpp` is an output artifact, not a hand-maintained design target.
3. If manual C/C++ helpers are needed, keep them small and only where profiling justifies them.

### 4) Planner Constant Folding Is Already a Strong Hook

Planner/binder already produce `ConstantColumn` and literal rewrites in several places. The design should explicitly route these planner-level constants into Draken constant vectors at execution boundaries.

Correction:
1. Add a planner-to-executor handoff contract: “constant schema output column” -> “ConstantVector emission” in execution.

### 5) Length Type Must Match Existing Buffers

Most Draken buffers use `size_t length`.

Correction:
1. `DrakenConstantBuffer.length` should be `size_t`, not `uint32_t`.

### 6) Arrow Constant Import Detection Should Be Deferred

Scanning Arrow arrays to “discover const-ness” is O(n) and fights the intended optimization.

Correction:
1. v1 should support `ConstantVector.to_arrow()` export.
2. v1 should not attempt Arrow constant-array detection/import.
3. Arrow import optimization can be a v2 metadata-driven enhancement.

### 7) Engine-Principles Alignment Must Be Explicit

Add explicit constraints for this design:
1. No new NumPy in constant motor kernels.
2. No Arrow compute inside constant motor kernels.
3. No Python fallback loops inside hot constant predicate/group-by paths.
4. Fail visibly for malformed constant buffers (bad value pointer, invalid length/bitmap state), rather than silent degrade.

## Corrected Design Edits

### Edit A: Buffer Spec

Use this shape in `buffers.h`:

```c
typedef struct {
    DrakenType type;          // DRAKEN_CONSTANT
    DrakenType value_type;    // scalar logical type
    void* value;              // owned scalar payload (or owned child payload handle)
    size_t length;            // logical row count
    uint8_t* null_bitmap;     // optional row validity bitmap
} DrakenConstantBuffer;
```

### Edit B: Planner/Executor Contract (New Early Milestone)

Add a milestone before vector/kernel work:
1. Detect constant output columns in projection planning/binding.
2. Emit constant-native vectors from execution nodes that materialize those outputs.
3. Ensure constant outputs do not route through Python literal expansion paths.

### Edit C: Arrow Interop Scope

Replace import-detection language with:
1. `ConstantVector.to_arrow()` supported (expands to full array).
2. Arrow import to constant representation is out of scope for v1.

### Edit D: Implementation Structure

Replace “`.pxd` + hand-written `.cpp` + `.pyx`” dependency with:
1. `.pxd` declarations.
2. `.pyx` kernels and wrapper methods (`cdef inline`/typed loops).
3. Optional small native helper only if profiling proves needed.

## Revised Implementation Sequence

### Phase 1: Types + Vector Foundation

1. Add `DRAKEN_CONSTANT = 62`.
2. Add `DrakenConstantBuffer` with `size_t length`.
3. Add `constant_vector.pxd` + `constant_vector.pyx`.
4. Implement: `__getitem__`, `to_pylist`, `to_arrow`, null checks, ownership/free paths.
5. Unit tests for lifecycle, null bitmap semantics, scalar type correctness.

### Phase 2: Planner/Operator Wiring

1. Map planner/binder constant outputs to constant-native vector emission.
2. Update `ProjectionNode` path first (lowest risk, highest return).
3. Add integration tests proving `SELECT 42 ...` and similar shapes emit `ConstantVector` rather than full-width materialized vectors.

### Phase 3: Expression + Grouping Consumption

1. Add constant-aware predicate handling in expression ops dispatch.
2. Add constant group-key shortcut in group-by planning/runtime where safe.
3. Maintain strict failure behavior for unsupported/malformed cases.

### Phase 4: Export + Hardening

1. Finalize Arrow export behavior.
2. Add regression guards to prevent reintroduction of Python/Arrow/NumPy in constant motor paths.
3. Publish benchmark and memory results with reproducible harness.

## Revised Risks

| Risk | Correct Mitigation |
|---|---|
| Early Python materialization bypasses constant path | Emit constants from planner/operator boundary, not expression literal expansion |
| Arrow import detection overhead | Defer constant-import detection to v2; export-only in v1 |
| Incorrect ownership/null semantics | Explicit constructor/destructor invariants + bitmap tests |
| Engine-principle drift (NumPy/Arrow/Python in motor) | Add source-token regression guard tests for constant motor files |

## Revised Success Criteria

1. Constant outputs are represented as `ConstantVector` on supported projection paths.
2. Memory for large constant columns is O(1) with respect to row count (plus null bitmap when present).
3. Constant predicate evaluation avoids per-row scalar comparisons where possible.
4. Constant group keys avoid unnecessary hash/group state work where semantics permit.
5. Arrow export works correctly; no v1 requirement for Arrow constant import detection.
6. No regressions on non-constant paths.
7. Engine-principles guardrails are enforced by tests.

## Summary

The design direction is good, but execution details needed correction:
1. Align with real planner/operator names and actual expression materialization behavior.
2. Use existing buffer conventions (`size_t`).
3. Keep v1 Arrow scope to export-only for constants.
4. Implement in native Draken/Cython patterns without introducing new NumPy/Arrow motor dependencies.
5. Add explicit regression guards so the constant path stays a motor-path optimization, not a compatibility-layer fallback.
