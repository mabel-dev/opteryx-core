# Draken GROUP BY Engine File Split Design

## Status

- **Status:** Proposed
- **Priority:** High
- **Owner:** Implementation team
- **Primary Target:** `opteryx/compiled/aggregations/group_by_engine.pyx`
- **Related Work:** `docs/draken-groupby-per-aggregate-state-redesign-plan.md`

---

## Problem Statement

`group_by_engine.pyx` has grown into a monolithic implementation that mixes:

- engine orchestration
- mode detection
- state layout and growth
- fixed-key ingest
- dictionary-key ingest
- object/string-key ingest
- multi-key ingest
- object aggregate handling
- finalize chunk construction
- telemetry helpers
- debug helpers

This creates several problems:

1. **Unsafe edit surface**
   - small changes require touching a very large file
   - partial edits are hard to review
   - accidental corruption is easier

2. **Poor migration ergonomics**
   - the per-aggregate state redesign requires repeated edits across many ingest paths
   - those edits are harder to reason about when all paths live together

3. **Weak locality of concerns**
   - state helpers, ingest kernels, and finalize logic are interleaved
   - key-shape-specific logic is not isolated

4. **Higher review and debugging cost**
   - diffs are noisy
   - unrelated logic appears in the same patch
   - compile failures are harder to localize

The engine should be split into smaller Cython modules with explicit ownership boundaries while preserving performance and specialization.

---

## Goals

### Correctness goals

- preserve existing behavior during extraction
- avoid semantic changes during the split itself
- keep failure modes explicit
- make it easier to validate per-area invariants

### Performance goals

- preserve current hot-path specialization
- avoid introducing Python fallback paths
- avoid dynamic dispatch in ingest hot loops
- keep Cython/C++ boundaries explicit and efficient

### Maintainability goals

- isolate logic by responsibility and key shape
- reduce patch size for future migrations
- make Phase 2+ redesign work safer and more incremental

---

## Non-Goals

This split does **not** aim to:

- redesign SQL semantics
- replace Carchar
- rewrite kernels in Python
- introduce runtime-polymorphic abstractions in hot paths
- complete the per-aggregate redesign by itself

This is a structural refactor to enable safer implementation work.

---

## Design Principles

1. **Behavior-preserving extraction first**
   - move code without changing semantics
   - only after extraction should redesign changes continue

2. **Split by responsibility, not by arbitrary line count**
   - each file should own a coherent concern

3. **Keep hot paths statically specialized**
   - no generic Python dispatch wrappers in ingest loops

4. **Prefer explicit shared helpers over duplication**
   - but do not over-abstract hot code

5. **Engine remains the orchestration root**
   - `group_by_engine.pyx` should become the coordinator, not the implementation dump

---

## Proposed Target Layout

### 1. `opteryx/compiled/aggregations/group_by_engine.pyx`

**Role:** thin orchestration layer

Owns:

- `CarcharGroupStateEngine` type definition
- public entrypoints:
  - `ingest`
  - `seal`
  - `finalize`
  - `finalize_morsels`
  - `stats`
  - `debug_dump`
- high-level mode routing
- imports and wiring between split modules

Should **not** own:

- large ingest implementations
- large finalize builders
- telemetry helper bodies
- object/string aggregate internals
- state growth/init details

Target outcome:
- this file becomes the control plane
- most method bodies delegate to specialized helpers

---

### 2. `opteryx/compiled/aggregations/group_by_state.pyx`

**Role:** state ownership, initialization, growth, invariants

Owns:

- state initialization helpers
- state growth helpers
- per-aggregate state initialization
- per-aggregate state growth
- state-size assertions
- offset helpers
- state access helpers

Candidate contents:

- `_state_count`
- `_multi_offset`
- `_initialize_per_aggregate_states`
- `_grow_per_aggregate_states`
- `_assert_per_aggregate_state_sizes`
- `_get_per_aggregate_state`
- new-group insertion helpers if they are primarily state-allocation logic

Why:
- this is the core of the redesign
- it should be isolated from ingest routing

---

### 3. `opteryx/compiled/aggregations/group_by_mode_init.pyx`

**Role:** mode detection and backend shape selection

Owns:

- `_maybe_init_carchar_mode`
- helper predicates used only for mode selection
- key/value kind classification logic
- supported-shape validation

Why:
- this logic is large and branch-heavy
- it is not part of ingest hot loops
- it changes when supported query shapes evolve

---

### 4. `opteryx/compiled/aggregations/group_by_ingest_fixed.pyx`

**Role:** single-key fixed-width ingest paths

Owns:

- `_ingest_fixed_width_key`
- `_ingest_int64_key`
- `_ingest_int64_key_with_const_accessor`
- `_ingest_integer_key`

Why:
- these are closely related
- they share fixed-width key semantics
- they are a natural home for numeric single-key ingest work

---

### 5. `opteryx/compiled/aggregations/group_by_ingest_fixed_multi.pyx`

**Role:** multi-aggregate fixed-width single-key ingest paths

Owns:

- `_ingest_int64_key_multi`
- `_ingest_integer_key_multi`

Why:
- these are the primary Phase 2 migration hotspots
- isolating them makes SUM/MIN/MAX/AVG migration much safer

---

### 6. `opteryx/compiled/aggregations/group_by_ingest_multi_key_fixed.pyx`

**Role:** multi-column fixed-key ingest

Owns:

- `_ingest_multi_fixed_key`
- `_ingest_multi_fixed_key_multi`
- `_build_multi_fixed_key_vectors` if tightly coupled

Why:
- multi-key fixed-width logic is distinct from single-key logic
- it has its own serialization/finalize concerns

---

### 7. `opteryx/compiled/aggregations/group_by_ingest_dict.pyx`

**Role:** dictionary-key ingest paths

Owns:

- `_ingest_dictionary_key`
- `_ingest_dictionary_key_multi`
- dictionary-key-specific helpers if any should be moved out of generic readers

Why:
- dictionary handling has distinct branching and decode behavior
- it is a separate migration surface for per-aggregate kernels

---

### 8. `opteryx/compiled/aggregations/group_by_ingest_object.pyx`

**Role:** object/string-like key ingest and object aggregate handling

Owns:

- `_ingest_object_key`
- `_ingest_object_key_multi`
- `_ingest_object_minmax_for_states`
- `_ingest_object_minmax_multi_for_states`
- `_ingest_any_value_var_for_states`
- `_ingest_any_value_var_multi_for_states`
- string/object payload arena helpers if they are engine-specific

Why:
- object/string logic is the most invasive and least like numeric fixed-width paths
- it should be isolated from numeric migration work

---

### 9. `opteryx/compiled/aggregations/group_by_finalize_engine.pyx`

**Role:** finalize chunk construction and output vector assembly

Owns:

- `_build_chunk_morsel`
- `_build_chunk_morsel_multi`
- `_empty_morsel`
- `_output_names`
- finalize fast-column helpers if they remain engine-specific

Potentially also:

- `_build_single_fixed_key_vector`
- `_build_encoded_key_vector`
- `_build_multi_encoded_key_vector`
- `_build_native_object_vector`
- `_build_object_state_vector`
- `_build_multi_object_state_vector`

Why:
- finalize is large and logically separate from ingest
- it is also a future redesign hotspot for Phase 3/4

---

### 10. `opteryx/compiled/aggregations/group_by_telemetry.pyx`

**Role:** telemetry and timing helpers

Owns:

- `initialize_groupby_readings`
- `record_finalize_backend_time`
- `record_finalize_rows_to_vectors_time`
- `record_finalize_morsel_build_time`
- `record_finalize_rows_count`
- `record_finalize_chunk_emitted`
- `record_finalize_fast_path_hit`
- `record_feature_groupby_engine_*`
- `record_dict_groupby_fastpath_hit`
- `record_groupby_key_store_bytes`
- `record_constant_groupby_vector`
- `record_ingest_state_assign_time`
- `record_ingest_hit_miss_counts`
- `record_groupby_hash_time`
- `record_groupby_reserve_time`
- `record_groupby_accumulate_time`
- `record_bloom_stats`

Why:
- these are already conceptually separate
- they add noise to the main engine file

---

### 11. `opteryx/compiled/aggregations/group_by_key_helpers.pyx`

**Role:** key extraction and payload serialization helpers

Owns:

- `_read_dictionary_fixed_key`
- `_append_single_encoded_key`
- `_append_multi_encoded_key`
- `_extract_stringlike_key`
- `_append_single_payload_key`
- `_append_single_fixed_payload_key`
- `_append_multi_fixed_payload_key_from_vectors`
- `_append_multi_payload_key`

Why:
- these helpers are shared across multiple ingest paths
- they are not themselves ingest orchestration

---

## Proposed Class/Method Strategy

There are two viable implementation strategies.

### Option A: keep methods on `CarcharGroupStateEngine`

Pattern:

- declare methods on the class in `group_by_engine.pyx`
- implement bodies in included or cimported split modules

Pros:

- minimal call-shape changes
- preserves `self.method(...)` structure
- easier incremental extraction

Cons:

- Cython file organization can still be awkward
- may require careful declaration ordering

### Option B: move heavy logic to module-level `cdef` helpers

Pattern:

- `group_by_engine.pyx` keeps thin methods
- methods delegate to module-level helpers like:
  - `_ingest_int64_key_multi_impl(self, morsel, key_vector)`
  - `_build_chunk_morsel_multi_impl(self, start, stop)`

Pros:

- easiest physical split
- explicit ownership by module
- simpler extraction mechanics

Cons:

- slightly more indirection
- helper naming becomes important

### Recommendation

Use **Option B**.

Reason:
- it is the most practical way to split a large Cython file safely
- it keeps the public class stable
- it avoids fighting Cython method-definition placement rules
- it makes future extraction and testing easier

Example pattern:

```/dev/null/group_by_engine_split_example.pyx#L1-18
cdef void _ingest_int64_key_multi_impl(
    CarcharGroupStateEngine self,
    Morsel morsel,
    Int64Vector key_vector,
) except *:
    # extracted body lives here
    ...

cdef class CarcharGroupStateEngine:
    cdef void _ingest_int64_key_multi(self, Morsel morsel, Int64Vector key_vector) except *:
        _ingest_int64_key_multi_impl(self, morsel, key_vector)
```

This preserves the engine API while moving implementation out of the monolith.

---

## Dependency Rules Between Split Files

To avoid circular imports and tangled ownership:

### Allowed dependency direction

- `group_by_engine.pyx` depends on everything
- ingest modules may depend on:
  - `group_by_state`
  - `group_by_key_helpers`
  - `group_by_telemetry`
  - kernel modules
- finalize module may depend on:
  - `group_by_state`
  - finalize helper modules
- `group_by_state.pyx` should depend on as little as possible
- `group_by_telemetry.pyx` should depend on nothing engine-specific beyond `self._readings`

### Avoid

- ingest modules depending on finalize modules
- finalize modules depending on ingest modules
- key helper modules depending on ingest modules
- circular helper imports

---

## Extraction Order

### Phase A: low-risk helper extraction

Extract first:

1. `group_by_telemetry.pyx`
2. `group_by_key_helpers.pyx`
3. `group_by_state.pyx`

Why:
- these are shared helpers
- lower semantic risk
- immediate reduction in file size

### Phase B: finalize extraction

Extract next:

4. `group_by_finalize_engine.pyx`

Why:
- large but logically separate
- not in ingest hot path
- reduces noise before ingest work

### Phase C: ingest extraction by shape

Then extract:

5. `group_by_ingest_fixed.pyx`
6. `group_by_ingest_fixed_multi.pyx`
7. `group_by_ingest_multi_key_fixed.pyx`
8. `group_by_ingest_dict.pyx`
9. `group_by_ingest_object.pyx`

Why:
- this aligns with actual migration work
- each file becomes a focused implementation surface

### Phase D: mode init extraction

Finally:

10. `group_by_mode_init.pyx`

Why:
- large but not urgent for Phase 2
- can be extracted once ingest/finalize are stabilized

---

## Recommended Immediate Split Boundary

If we want the smallest useful first step, do this first:

1. extract telemetry helpers
2. extract key helper functions
3. extract `_ingest_int64_key_multi` and `_ingest_integer_key_multi` into `group_by_ingest_fixed_multi.pyx`

Why this first:
- it directly supports the per-aggregate redesign
- it reduces risk in the hottest current migration area
- it avoids trying to split the whole file in one pass

---

## How This Supports the Per-Aggregate Redesign

The redesign plan is currently blocked by the difficulty of safely editing a monolithic file.

This split directly helps:

### Phase 2

- `SUM`, `MIN`, `MAX`, `AVG` migration becomes localized to:
  - `group_by_ingest_fixed_multi.pyx`
  - `group_by_ingest_multi_key_fixed.pyx`
  - `group_by_ingest_dict.pyx`
  - `group_by_ingest_object.pyx`

### Phase 3

- numeric finalize migration becomes localized to:
  - `group_by_finalize_engine.pyx`
  - `group_by_state.pyx`

### Phase 4

- object/string aggregate migration becomes localized to:
  - `group_by_ingest_object.pyx`
  - `group_by_finalize_engine.pyx`

### Phase 5

- flattened storage removal becomes localized to:
  - `group_by_state.pyx`
  - ingest modules
  - finalize module

---

## Risks

1. **Cython module boundary friction**
   - extension-type access across files must be handled carefully
   - signatures must remain explicit

2. **Accidental semantic drift during extraction**
   - moving code and changing behavior at the same time is risky

3. **Circular dependency pressure**
   - especially if helpers are split without clear ownership

4. **Temporary duplication**
   - some helper logic may need short-lived duplication during extraction

5. **Compile churn**
   - Cython refactors can produce many small compile issues if done too broadly at once

---

## Mitigations

- extract without behavior changes first
- use module-level `cdef` helper functions
- keep `CarcharGroupStateEngine` as the single state owner
- move one coherent slice at a time
- compile after each extraction step
- update the redesign plan after each structural milestone

---

## Success Criteria

This split is successful when:

- `group_by_engine.pyx` becomes primarily orchestration
- ingest logic is separated by key/shape family
- finalize logic is isolated
- state helpers are isolated
- Phase 2 redesign work can proceed in small, reviewable patches
- no Python fallback path is introduced
- performance-sensitive paths remain specialized

---

## Proposed Milestone Tracker

### Milestone 1: Helper extraction

- [ ] Extract telemetry helpers
- [ ] Extract key helper functions
- [ ] Extract state helper functions
- [ ] Compile successfully

### Milestone 2: Finalize extraction

- [ ] Extract finalize chunk builders
- [ ] Extract finalize vector wrapper helpers
- [ ] Compile successfully

### Milestone 3: Ingest extraction

- [ ] Extract fixed-width single-key ingest
- [ ] Extract fixed-width multi-aggregate ingest
- [ ] Extract multi-key fixed ingest
- [ ] Extract dictionary ingest
- [ ] Extract object/string ingest
- [ ] Compile successfully

### Milestone 4: Resume redesign work

- [ ] Resume SUM migration
- [ ] Resume MIN/MAX migration
- [ ] Resume AVG migration
- [ ] Update redesign plan

---

## Recommendation

Proceed with a **behavior-preserving structural split first**, not more redesign edits inside the current monolith.

Recommended first implementation sequence:

1. extract `group_by_telemetry.pyx`
2. extract `group_by_key_helpers.pyx`
3. extract `group_by_state.pyx`
4. extract `group_by_ingest_fixed_multi.pyx`
5. resume per-aggregate `SUM` migration there
6. update `docs/draken-groupby-per-aggregate-state-redesign-plan.md` to reflect the new structure

This gives the best balance of safety, momentum, and alignment with the per-aggregate redesign.

---

## Notes

This design intentionally prefers:

- explicit specialization
- explicit module boundaries
- behavior-preserving extraction
- failure over silent degradation

The main thesis is:

> `group_by_engine.pyx` should become a coordinator over specialized Cython modules, not the single implementation container for every GROUP BY concern.