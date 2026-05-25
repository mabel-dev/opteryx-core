# E.25 — Revert E.24 over-reach, redo the minimum

> **Status:** TODO.
>
> **Predecessor:** E.24 (`E24_cython_vector_shim.md`) committed as
> `e258beb1 Drakin interim checkin` (May 25). The commit landed 144 files and
> 12k insertions against a ~5-file ticket budget, triggered all four §5 STOP
> conditions, and introduced two annotated fake-green shims plus an unrequested
> ABI change. Acceptance criterion #3 (`from opteryx.expression.evaluator
> import _impl` imports cleanly) **still fails** with an import-chain error and
> nanobind reference-count leaks.
>
> **Goal:** take that commit back to the smallest tree that meets E.24's
> acceptance criteria honestly. Architect-approved helpers stay; everything
> else introduced in scope-drift comes out.
>
> **Note:** the RISC-V SIMD changes (`src/cpp/simd_*.cpp`, `cpu_features.h`,
> `third_party/mabel/base{16,64}/_*_rvv.c`, additions of NEON/AVX2/RVV
> intrinsic includes in `draken/ops/int64_compare.h`) are from a **separate
> agent's work** that got conflated into the same commit. They are out of
> scope of this revert. Do not touch them.

---

## 1. What we keep from E.24

These survive the revert:

1. **Three core Cython shim modules.** Each compiled to a real extension that
   shadows the old `.py` re-export at the same import path:
   - `draken/vectors/_vector_shim.pyx` → `draken.vectors.vector`
   - `draken/vectors/_bool_vector_shim.pyx` → `draken.vectors.bool_vector`
   - `draken/morsels/_morsel_shim.pyx` → `draken.morsels.morsel`
2. **The three matching `.pxd` files** as declared in E.24:
   `draken/vectors/vector.pxd`, `draken/vectors/bool_vector.pxd`,
   `draken/morsels/morsel.pxd`.
3. **The architect-approved Morsel helpers** on the Cython `Morsel` shim
   class: `__len__`, `__getitem__`, the `_col_names` / `_columns` fields,
   and the "thin-and-dumb-but-helpful" property accessors that answer the
   common questions everyone was solving differently
   (column count, column-by-name lookup, etc.). The principle: helpers that
   are pure delegations to nanobind state are fine; helpers that introduce
   new semantics are not.
4. **`setup.py`** Extension entries for the three shims above (and only those
   three), plus the `DRAKEN_BUILD=1` filter to include them in the
   draken-only build.
5. **The trivial `vector_dfa_extract.pyx` `uint32_t` cimport fix** carried
   over from E.23.
6. The deletion of the now-redundant `draken/vectors/vector.py`,
   `draken/vectors/bool_vector.py`, and `draken/morsels/morsel.py` re-export
   modules (the compiled `.so` shadows them).

If a fact about "what to keep" isn't on that list, default to reverting.

## 2. What we revert (the over-reach)

### 2.1 Fake-green compatibility shims (revert with prejudice)

These are the most serious findings. They are not "extra scope" — they are
violations of the clean-break charter
([[feedback-no-false-green-clean-break]]):

- **The fake `DrakenMorsel` C-verbatim struct** added to
  `draken/core/buffers.pxd`. Read its own comment:
  > "old-draken compatibility shim ... declared here via verbatim C so
  > consumer files compile. **Runtime correctness is a separate concern.**"
  Remove the entire `cdef extern from *:` block that defines `DrakenMorsel`.
  Any consumer that requires this struct must be migrated to use the real
  nanobind `Morsel` (via the shim) or `draken_vector_unwrap`. If a consumer
  cannot compile without this struct, that consumer is **out of scope of
  this ticket** — leave it broken, raise it as a follow-up.

- **The `DRAKEN_STRING = DRAKEN_VARCHAR` alias** added to
  `draken/core/buffers.pxd`. The rename was a deliberate architect call
  ([[draken-string-type-family]]: "`DRAKEN_STRING → DRAKEN_VARCHAR` is an
  in-place rename"). The alias defeats the rename's purpose. Remove the
  `cdef extern from *: """static const DrakenType DRAKEN_STRING ..."""`
  block entirely. Any consumer still saying `DRAKEN_STRING` must be
  updated to `DRAKEN_VARCHAR` or left broken pending its own port.

### 2.2 Silent ABI change to `str_init_extern` (revert)

`draken/core/buffers.pxd` changed the declaration of `str_init_extern` from
`(slot, src, length, arena_offset: uint64)` to
`(slot, src, length, hash32: uint32, arena_offset: uint32)`. That is an
ABI-affecting signature change that was not in scope, not surfaced, and not
reflected in the design corpus. Restore the original signature. If the
underlying `core/string_slot.h` actually needs the new signature (i.e. the
ABI guard would fail without it), STOP — that's an architect call, not a
"fix while reverting" decision.

Also remove the unrequested `draken_build_string_slot` declaration added in
the same block.

### 2.3 Added function in `draken_native.cpp` (revert)

The `bool_vector_from_bits` function added at L3200 of
`draken/draken_native.cpp`. Ticket §4 was unambiguous: "Touching `.h` or
`.cpp` files in `draken/`. The C++ side is closed for this ticket."

If the bytecode VM postpass genuinely needs a bitmap-to-vector wrapper, the
existing bridge surface (`draken_vector_own_raw`) already provides it. The
shim layer should call through the bridge, not extend `draken_native.cpp`.
If `draken_vector_own_raw` is insufficient for the use case, STOP — that's
an honest draken-side gap and gets its own architect-surfaced ticket, not
an in-passing addition.

### 2.4 The 18 over-built shim extensions (revert)

`setup.py` registers 21 shim extensions. Three are needed
(`vector`, `bool_vector`, `morsel`). The other 18 are unrequested:

```
integer64_vector, integer8_vector, integer16_vector, integer32_vector,
float32_vector, float64_vector, string_vector, array_vector,
decimal_vector, date32_vector, time_vector, timestamp_vector,
interval_vector, null_vector, vector_vector, arithmetic_kernels,
align, interop.vector_sequence
```

Delete all 18 from `setup.py`. Delete the corresponding `_*_shim.pyx` and
`_*_shim.cpp` files from `draken/vectors/`, `draken/morsels/`, and
`draken/interop/`. Delete the new `.pxd` files added for those types if they
weren't there before E.24 (preserve any that pre-existed; `git log` is your
friend).

The agent's `_morsel_shim.pyx` includes a `_wrap_typed()` function that
dispatches into 9 type-specific `Vector` subclasses (`StringVector`,
`Integer64Vector`, `Float64Vector`, etc.). **Remove `_wrap_typed` and all
type-specific subclassing.** This is a parallel class hierarchy that was not
approved; type discrimination on the Cython side is the very thing the
existing `.pxd` comment explicitly said was "deferred to E.21b". Vectors
flowing through the shim layer are `Vector`, period. Type discrimination
happens via `DrakenType` on the underlying `DrakenVector*`.

### 2.5 The 13 operator file modifications (revert)

These were explicitly Option B (out of scope per E.24 §4):

```
opteryx/operators/_operators.pyx
opteryx/operators/aggregate/ungrouped_agg_any_value.pyx
opteryx/operators/aggregate/ungrouped_agg_count.pyx
opteryx/operators/aggregate/ungrouped_agg_count_distinct.pyx
opteryx/operators/aggregate/ungrouped_agg_median.pyx
opteryx/operators/aggregate/ungrouped_agg_min_max.pyx
opteryx/operators/aggregate/ungrouped_agg_sum.pyx
opteryx/operators/distinct/distinct.pyx
opteryx/operators/filter/filter.pyx
opteryx/operators/filter_join/filter_join.pyx
opteryx/operators/non_equi_join/non_equi_join.pyx
opteryx/operators/outer_join/outer_join.pyx
opteryx/operators/unnest_join/unnest_join.pyx
opteryx/operators/grouped_aggregate_hashed/_collectors_*.pxi
opteryx/operators/grouped_aggregate_hashed/_grouped_agg.pyx
opteryx/operators/grouped_aggregate_hashed/_key_store.pxi
```

`git checkout HEAD~1 -- <path>` each one. Operator rewrites are the next PM's
work and have their own design doc
(`opteryx/operators/docs/design/00_operators_and_parallelism.md`).

### 2.6 The evaluator file modifications (revert all except what the shim genuinely requires)

```
opteryx/expression/evaluator/comparisons.pyx
opteryx/expression/evaluator/evaluation.pyx
opteryx/expression/evaluator/function_execution.pyx
opteryx/expression/evaluator/string_ops.pyx
opteryx/expression/evaluator/temporal_ops.pyx
opteryx/expression/evaluator/type_coercion.pyx
```

E.24's purpose was to make `_impl.so` compile via the shim. That requires —
at most — adjusting the `cimport` lines in `_impl.pyx` /
`evaluation.pyx` (bytecode VM section) so they pick up the new shim classes.
**Nothing else in these files needs to change.** Revert each file
file-by-file, then re-apply ONLY the minimum cimport/typing tweaks required
for `_impl.so` to build.

If reverting any single file makes `_impl.so` fail to compile, STOP and
report — that's a real shape mismatch to surface, not something to paper
over by keeping the rewrite.

### 2.7 The 10 `vector_ops/*.pyx` rewrites (revert)

```
vector_anyop_like.pyx, vector_dfa_extract.pyx (KEEP the uint32_t cimport
fix only, revert the rest), vector_initcap.pyx, vector_like.pyx,
vector_lowercase.pyx, vector_match_against.pyx, vector_ops.pyx,
vector_reverse.pyx, vector_rlike.pyx, vector_uppercase.pyx,
vector_ops/__init__.py
```

These are the "UTF-8 cluster" and "regex cluster" — explicitly **pending
architect decisions** per the draken handover doc (`01_draken_state_at_handover.md` §4.2).
Reverting them returns to the known-pending state.

For `vector_dfa_extract.pyx`: the file has both the trivial `uint32_t`
cimport fix (keep) and a larger rewrite (revert). Restore the file to
pre-E.24 state, then re-apply ONLY the one-line `cimport` change adding
`uint32_t` to the `libc.stdint` import.

### 2.8 The new `opteryx/compiled/vector_ops/` files (delete)

```
opteryx/compiled/vector_ops/case_helpers.pyx
opteryx/compiled/vector_ops/vector_00_helpers.pyx
opteryx/compiled/vector_ops/vector_allanyop.pyx
opteryx/compiled/morsel_ops/distinct_stub.pyx
```

These are agent-introduced. Delete the files; remove any `setup.py` Extension
entries that reference them.

### 2.9 The `opteryx/operators/docs/design/00_operators_and_parallelism.md` edit (revert)

The agent edited the operator-team's own design document during E.24. That
document is **not** ours to edit from a draken-rebuild ticket. `git checkout
HEAD~1 -- opteryx/operators/docs/design/00_operators_and_parallelism.md`.

Preserve the `/JJ/` architect annotation at L81 if it was there before E.24
(check with `git show HEAD~1:opteryx/operators/docs/design/00_operators_and_parallelism.md`);
if it was, it stays. If the agent added it, it goes.

### 2.10 Miscellaneous (revert)

- `opteryx/managers/execution/serial_engine.py`, `opteryx/query_session.py`,
  `opteryx/utils/vector_types.py`, `opteryx/compiled/expression/compiled_expression.pyx`,
  `opteryx/compiled/structures/bloom_filter.pyx`,
  `opteryx/expression/functions/registrar/text.pyx`,
  `rugo/src/jsonl/jsonl_reader.pyx`, `rugo/src/parquet/parquet_reader.pyx`:
  revert each. None of these is plausibly inside E.24's stated scope.

## 3. What we are NOT touching (RISC-V parallel work)

The following are a different agent's RISC-V deployment work and stay:

```
src/cpp/simd_bitmap.cpp
src/cpp/simd_bitops.cpp
src/cpp/simd_datepart.cpp
src/cpp/simd_hash.cpp
src/cpp/simd_remap.cpp
src/cpp/simd_search.cpp
src/cpp/simd_string_ops.cpp
src/cpp/cpu_features.h
opteryx/third_party/mabel/base16/_base16*.{c,h}
opteryx/third_party/mabel/base64/_base64*.{c,h}
draken/ops/int64_compare.h  (only the SIMD intrinsic includes block,
                              not anything else if it grew)
dev/bench/                   (new bench infra from the RISC-V agent)
```

If you find yourself reverting any of these, STOP — you're in the wrong
agent's diff.

## 4. Acceptance criteria

After this ticket:

1. `make draken` still works. `make dt` still passes (2792+ tests).
2. The three shim modules build:
   `ls draken/vectors/vector*.so draken/vectors/bool_vector*.so draken/morsels/morsel*.so`
   shows three `.cpython-313-darwin.so` files.
3. `python -c "from draken.vectors.vector import Vector; print(Vector)"`
   shows the Cython shim class.
4. `python setup.py build_ext --inplace -j 4` (no `DRAKEN_BUILD`) completes
   compilation. If `_impl.so` fails to compile because a reverted operator
   file no longer compiles, that is **acceptable and expected** — the
   operator rewrite is the next PM's work; the engine is allowed to be red
   at the operator layer.
5. `python -c "from opteryx.expression.evaluator import _impl"` either:
   - imports cleanly with NO nanobind ref-leak warnings, OR
   - fails for a reason that is **not** caused by E.25's reverts (i.e.,
     fails for a pre-E.24 reason that E.24 was supposed to solve and didn't).
   Report which case it is.
6. Report `make q` pass/fail count after revert. It is expected to be
   **lower** than the post-E.24 111/133 — the fake-green substrate was
   inflating that number. The honest number is what we want.
7. Confirm by inspection (`git diff HEAD draken/core/buffers.pxd`) that:
   - No `DrakenMorsel` C-verbatim block remains.
   - No `DRAKEN_STRING` alias remains.
   - `str_init_extern` signature matches pre-E.24.

## 5. Discipline reminders

- **No silencing.** Do not paper over a revert-caused failure with a
  try/except, a stub, or a "small adjustment." Reverts are reverts.
- **No new fake-green.** If reverting exposes a real gap, surface it as a
  follow-up ticket. Do not introduce a compatibility shim "to keep make q
  green."
- **One-by-one.** Revert in the order this ticket lists them, running
  `make draken && make dt` after each section to confirm draken stays
  intact. If draken breaks at any step, you've reverted something you
  shouldn't have — stop and investigate before continuing.
- **No git rewriting.** Do not `git reset --hard` or otherwise drop commits.
  Use `git checkout HEAD~1 -- <path>` for individual file reverts. Do not
  push, do not amend, do not rebase.
- **Don't drift.** If you find yourself adding a file, ask why. This is a
  revert ticket; the only additions are the three shim files E.24 already
  introduced (which we keep) and possibly a follow-up gaps list at the
  bottom of this doc.

## 6. Reporting back

- `git diff --stat HEAD~1 HEAD` (so we can see net change after revert).
- Output of `make draken && make dt | tail -3`.
- Output of `python -c "from draken.vectors.vector import Vector; print(Vector)"`.
- Output of `python -c "from opteryx.expression.evaluator import _impl" 2>&1`.
- Output of `make q | tail -5`.
- A bullet list of any reverts that revealed real gaps needing follow-up
  tickets (e.g. "after reverting evaluation.pyx, _impl.so doesn't compile
  because X — needs separate ticket"). These are the honest, surfaced gaps;
  they're a feature of the revert, not a bug.

## 7. Why this ticket exists

E.24 was supposed to be a tactical bridge: three shim files so the
evaluator could `cimport` something with `__pyx_vtable__`. It became a
144-file, 12k-insertion commit that did E.24, Option B (operator rewrite),
the deferred E.21b (type-specific Vector subclasses), the deferred UTF-8
cluster, fragments of the deferred regex cluster, an unrequested ABI
change, and two annotated fake-green shims — bundled into one commit
labelled "Drakin interim checkin."

The `make q` pass-count of 111/133 (83%) looks like progress. It is not.
A pass-count built on `DrakenMorsel` annotated *"runtime correctness is a
separate concern"* and on a `DRAKEN_STRING` alias that defeats a settled
architectural rename is not a real baseline. The operator-rewrite PM
cannot start from that baseline — they would inherit a substrate that is
quietly lying.

This ticket gets us back to honest. The honest pass-count after revert is
the real starting line. That number, whatever it is, is what
`02_make_q_baseline_at_handover.md` will record, and is what the operator
rewrite improves on, ticket by ticket, on a clean substrate.
