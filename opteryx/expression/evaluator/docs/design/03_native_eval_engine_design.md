# 03 — Native Eval Engine Design

**Status:** Phase 1 ✅ complete. Phase 2 ✅ complete. Phase 4 ✅ Stage B+C complete — `draken_compare_dv` covers INT64/FLOAT64/TIMESTAMP64/DATE32/VARCHAR/NVARCHAR/VARBINARY; `draken_arithmetic_dv` covers INT64/FLOAT64. Phase 3 ✅ incremental delivered (bind-time nb_func flag; typed _nb access; direct arity dispatch). Full Phase 3 dispatch table deferred pending DV stack.

---

## Context

The eval engine is the connective tissue between operators and the data substrate.
Every operator that filters, projects, or computes passes through it. The data
substrate (`DrakenVector`) is 100% native. The functions that operate on it are
being moved to native. The operators are next. The eval engine is one of the
largest surfaces in this chain — if it stays Python-orchestrated, the whole
pipeline stalls at the GIL regardless of how native everything else is.

### Current state

| Path | Reality |
|---|---|
| Pure bitmap VM (`c_execute_bytecode_inner`) | Genuinely `noexcept nogil`. Operates on `uint8_t*` bitmaps only. Done. |
| General bytecode VM (`execute_bytecode`) | Compiled C dispatch loop, but operates on Python objects throughout. Not native. |
| Tree-walker (`_eval_value` / `evaluate_draken`) | Pure Python. `BC_LEGACY` falls back here. Currently only `NT_CASE` reaches it from the linearizer. |

The general VM is "Cython-compiled Python dispatch calling C++ kernels." The
compilation step makes it faster than pure Python, but the orchestration layer —
stack management, type resolution, function dispatch, operand routing — is still
Python objects under the GIL.

---

## End state

The general VM becomes an extension of `c_execute_bytecode_inner`:

- Stack holds `DrakenVector*` (raw C pointers), not Python extension types.
- Function dispatch is a C-level indexed call through a static `eval_fn_t` table.
- All type discriminants are C integers baked into the instruction word at bind time.
- No Python objects anywhere on the hot path.
- GIL acquired only at frame entry (to fetch column pointers from the morsel) and
  frame exit (to hand the result back to Python).
- `BC_LEGACY` is empty — every node type has a native opcode.

---

## The four blockers

### Phase 1 — Instruction word carries Python objects ✅ COMPLETE (2026-05-27)

**Files:** [`opteryx/compiled/expression/compiled_expression.pxd`](../../../compiled/expression/compiled_expression.pxd), [`opteryx/compiled/expression/compiled_expression.pyx`](../../../compiled/expression/compiled_expression.pyx), [`opteryx/expression/evaluator/evaluation.pyx`](../evaluation.pyx), [`opteryx/expression/evaluator/comparisons.pyx`](../comparisons.pyx), [`opteryx/expression/evaluator/arithmetic.pyx`](../arithmetic.pyx), [`opteryx/expression/evaluator/temporal_ops.pyx`](../temporal_ops.pyx)

**Delivered:**

- `left_orso_type: PyObject*` and `right_orso_type: PyObject*` removed from `BytecodeInstr`. Replaced with `int16_t left_type_code` / `int16_t right_type_code` (new `BCTypeCode` enum: `BC_TYPE_NONE=0`, `BC_TYPE_DATE=1`, `BC_TYPE_TIMESTAMP=2`).
- `BCBinaryOpCode` enum (12 ops) and `BCUnaryOpCode` enum (9 ops) added to `.pxd`.
- BC_UNARY_OP: `compare_op_str` fully eliminated — instruction stores `op_code` int, no string held, no string comparison at execution time. `_unary_op_kernel` takes `int op_code`.
- BC_BINARY_OP: date/interval/StringConcat switch cases now dispatch on `op_code` int. `compare_op_str` retained for `call_arithmetic_op` kernel registry call (tagged for removal when registry adopts int dispatch).
- BC_COMPARE: `draken_compare_int`, `draken_compare`, and `_int64_temporal_compare` all take `int16_t` type codes. Inline `from opteryx.types import OrsoTypes` inside `_binary_op_from_vecs` body eliminated.
- `make q`: 133/133 ✅

---

### Phase 2 — BC_LEGACY must reach zero ✅ COMPLETE (2026-05-27)

**Files:** [`opteryx/compiled/expression/compiled_expression.pxd`](../../../compiled/expression/compiled_expression.pxd), [`opteryx/compiled/expression/compiled_expression.pyx`](../../../compiled/expression/compiled_expression.pyx), [`opteryx/expression/evaluator/evaluation.pyx`](../evaluation.pyx)

**Delivered:**

**2a — BC_CASE native opcode.** `BC_CASE = 18` added to the `BCOpcode` enum. The linearizer
handles `NT_CASE` by emitting `BC_CASE` with the Python node stored in `source_node`
(held alive via `_held_refs`). The executor calls `evaluate_case(node, morsel)` via a
module-level cached import (avoiding the circular import that prevented a top-level import).
`NT_CASE` no longer falls through to `BC_LEGACY`.

**2b — BC_EXTRACTION pre-resolved at bind time.** At linearisation time the
`MapAccessOp` / `ArrowOp` / `LongArrowOp` callable is imported once and stored in
`callable_ref`. The key is built into a constant Draken Vector (length=1) at bind time
and stored in `literal_obj`. The executor is now a single call:
`(<object>slot.callable_ref)(v_left, <object>slot.literal_obj)` — no inline imports,
no string dispatch on the op name at execution time.

`draken.draken_native` added as a module-level import to `compiled_expression.pyx`
(needed for `vector_from_constant` / `vector_from_string_sequence` at bind time).

**BC_LEGACY is now unreachable** for all node types produced by the linearizer. The
opcode is retained as a safety net but its fallthrough comment documents it as dead code.

- `make q`: 133/133 ✅

---

### Phase 3 — BC_FUNCTION dispatch is Python

**Files:** [`opteryx/expression/evaluator/evaluation.pyx`](../evaluation.pyx), [`opteryx/compiled/expression/compiled_expression.pyx`](../../../compiled/expression/compiled_expression.pyx), function registry

Current execution path for every function call:

```python
callable_obj = <object>slot.callable_ref          # unpack Python callable
is_nb_callable = type(callable_obj).__name__ == "nb_func"  # runtime string compare
func_args = []                                     # new Python list per call
for j in range(arity):
    nb = getattr(item, "_nb", None)                # Python attr lookup per arg
    func_args.append(nb if nb is not None else item)
legacy_result = callable_obj(*func_args)           # Python *args call
```

**Target:** store a `uint16_t function_id` in the instruction word instead of a
`PyObject*`. At execution time: `fn_table[function_id](args, arity, n_rows)` — one
C-level indexed call.

The dispatch table is a module-level C array:
```cython
ctypedef object (*eval_fn_t)(...)   # signature TBD — see decision below
cdef eval_fn_t fn_table[MAX_FN_ID]
```

Functions not yet ported to native get a slot that calls the Python callable
via the existing path (backwards compatible; not GIL-free but correct).

**Architect decision required — calling convention.**

The problem: function arguments are currently `Vector` / `BoolVector` / scalar Python
objects — Python extension types. A C function pointer cannot take Python extension
types as C-level arguments. The calling convention cannot be fully C-level until
the stack holds `DrakenVector*` (Phase 4). Options within current constraints:

| Option | Signature | GIL-free | Notes |
|---|---|---|---|
| A | `cpdef Vector fn(list args, Py_ssize_t n_rows)` | No | Uniform arity. Still passes a Python list — better than `*args` but not native. |
| B | `cpdef Vector fn_1(Vector, Py_ssize_t)`, `fn_2(Vector, Vector, Py_ssize_t)`, … | No | Typed args, no list. Needs per-arity dispatch variants (0–6). Eliminates `getattr` and list-building. |
| C | Defer Phase 3 until Phase 4 | — | Do Phases 1, 2, 4 in order; function dispatch becomes trivial once stack is `DrakenVector*`. |

Option B eliminates the most Python overhead without requiring Phase 4 first.
Option C is cleaner but delays the win.

**Architect call (2026-05-27, draken-PM): Option C.** Reasoning:

- After Phase 4 the stack already holds `DrakenVector*`, so the
  function-dispatch signature collapses to:
  ```c
  typedef VecResult (*eval_fn_t)(const DrakenVector** args,
                                  uint32_t arity,
                                  uint32_t n_rows,
                                  DrakenFrameArena* arena);
  ```
  Pure C, no Python types in the signature, no GIL needed at the
  dispatch step itself. The arena parameter lets the kernel allocate
  intermediate buffers from the same per-frame pool.
- Implementing Option B first means writing a per-arity dispatch
  layer in terms of `Vector` (the Cython shim), then rewriting it
  in terms of `DrakenVector*` in Phase 4. Real rework. The shim
  pattern with `_nb` access in dispatch costs `getattr` per call
  per arg — exactly the overhead Phase 3 is supposed to remove. We
  don't want to relocate that cost into Phase 3 just to delete it in
  Phase 4.
- The "win delay" is mostly notional. Phases 1, 2, 4 in sequence
  produce the overall improvement; Phase 3 becomes a trivial reshape
  at the end (build the `eval_fn_t` table from the existing function
  registry, dispatch by `function_id` index — under an hour once the
  stack is `DrakenVector*`).

**Sequence:** Phase 1 → Phase 2 → Phase 4 → Phase 3 (trivial closer).

---

### Phase 4 — Stack holds Python objects ✅ Stage B complete (2026-05-27)

**IN-list fold (2026-05-27).** `BC_CMP_INLIST_INLINE = 4` added to `BCCompareFlag`. The lineariser now detects right-hand-side `NT_LITERAL` nodes whose value is a `CarcharSetWrapper`, `PerfectHashSet`, `list`, `tuple`, `set`, or `frozenset`. When found, the set is stored in `slot.literal_obj` with the `BC_CMP_INLIST_INLINE` flag set and no `BC_LOAD_LIT_SET` instruction is emitted. `BC_COMPARE` pops ONE item from the stack instead of TWO and reads the right operand directly from the instruction word. Sets can never be `DrakenVector*` and must not appear as stack operands; this fold is a hard prerequisite for the `DrakenVector*` stack.

**Stage B — C-level kernel fast paths wired (2026-05-27).**

`draken_frame_arena_create/destroy/release`, `draken_compare_dv`, and `draken_arithmetic_dv` are now cimported and wired into `execute_bytecode`:

- Arena created at `execute_bytecode` entry; destroyed in `finally` at exit.
- **BC_COMPARE** (normal, non-inline-list path): `_DRAKEN_CMP_OP[slot.op_code]` translates to the draken_dv op code (0=EQ, 1=NE, 2=GT, 3=GE, 4=LT, 5=LE). If `dv_op >= 0`, calls `draken_compare_dv(dv_op, v_left.unified(), v_right.unified(), left_type_code, right_type_code, n_rows, arena)`. On non-NULL return: releases data/validity from arena (transfers ownership to Python), wraps as `BoolVector` via `from_decoded`, and skips the Python fallback entirely. Returns NULL → falls through to existing `draken_compare_int` / `draken_compare` path.
- **BC_BINARY_OP** (`BOP_PLUS ≤ op_code ≤ BOP_MODULO`): calls `draken_arithmetic_dv(op_code, v_left.unified(), v_right.unified(), n_rows, arena)`. On non-NULL return: releases data/validity, wraps as `Vector` via `vec_from_decoded`. Returns NULL → falls through to `_binary_op_from_vecs`.
- Temporal scalars in the bytecode path are already int-encoded Vectors (via `BC_LOAD_LIT_SCALAR` → `_scalar_to_draken_constant`); no pre-coercion needed before the C call. The type hint codes (`left_type_code` / `right_type_code`) are passed directly to the kernel.
- DrakenVector struct allocated by each kernel stays arena-tracked for the frame duration; only `data` and `validity` buffers are released (to transfer ownership to Python). The struct itself is freed when the arena is destroyed at frame exit.

New cimports added to `evaluation.pyx`:
- `from draken.core.buffers cimport DrakenVector, DrakenType, DRAKEN_BOOL`
- `from draken.core.frame_arena cimport DrakenFrameArena, draken_frame_arena_create, draken_frame_arena_destroy, draken_frame_arena_release`
- `from draken.ops.compare_dv cimport draken_compare_dv`
- `from draken.ops.arithmetic_dv cimport draken_arithmetic_dv`
- `from draken.vectors.vector cimport from_decoded as vec_from_decoded`

- `make q`: 133/133 ✅

**Stage C — compare_dv delivered (already in cpp at Stage B cut).** `draken_compare_dv` already covers DATE32, VARCHAR, NVARCHAR, VARBINARY in addition to INT64/FLOAT64/TIMESTAMP64 — the C++ implementation was ahead of the pxd documentation. The `.pxd` comment has been updated to reflect actual coverage. This means the BC_COMPARE C fast path now fires for virtually all common comparison types; the Python fallback is reached only for DECIMAL and cross-type pairs.

**Stage C — arithmetic_dv open.** `draken_arithmetic_dv` still covers INT64 + FLOAT64 only. DATE arithmetic (DATE + INTERVAL, TIMESTAMP + INTERVAL) and DECIMAL arithmetic still route to the Python fallback. These are less common than comparisons; the arithmetic fast path already covers the dominant numeric workload.

---

### Phase 4 — kernel interface deliverables (draken-PM)

The eval-PM is blocked on three C-level interfaces. This section is the formal request; draken-PM to fill in the commitment block once decisions are made.

**Status (2026-05-27, draken-PM):**

| Deliverable | Status |
|---|---|
| 1 — `cdef extern` for `draken_frame_arena_*` | ✅ Delivered (`draken/core/frame_arena.pxd`) |
| New: `draken_frame_arena_adopt(arena, ptr)` | ✅ Delivered (needed by #2 / #3 to fold kernel results into arena scope) |
| 2 — `draken_compare_dv` (Stage B: INT64 + FLOAT64) | ✅ Delivered (`draken/ops/compare_dv.{h,pxd,cpp}`); ops EQ/NE/GT/GE/LT/LE |
| 3 — `draken_arithmetic_dv` (Stage B: INT64 + FLOAT64) | ✅ Delivered (`draken/ops/arithmetic_dv.{h,pxd,cpp}`); ops PLUS/MINUS/MULTIPLY/DIVIDE/MODULO |
| Stage C — `draken_compare_dv` extension | ✅ DATE32, TIMESTAMP64, VARCHAR / NVARCHAR / VARBINARY added. ⚠️ DECIMAL returns NULL — see §descriptor below. |
| Stage C — `draken_arithmetic_dv` extension | ⚠️ Not extended — needs descriptor-access design (see below). DATE/TIMESTAMP arithmetic is also cross-type by nature (date+interval), needs different signature. |

### Descriptor-access blocker for DECIMAL / TIMESTAMP unit / DATE arithmetic

The current `draken_compare_dv` / `draken_arithmetic_dv` signatures take bare `DrakenVector*`. The logical-type descriptor (DECIMAL precision/scale, TIMESTAMP unit, INTERVAL offset_minutes) lives on `VectorOwner` — the C++ wrapper around `DrakenVector` — not on the `DrakenVector` struct itself.

That descriptor is needed for:
- **DECIMAL compare:** scales must be aligned before int64 byte comparison can give the right answer (`Decimal('1.5')` at scale=1 vs `Decimal('1.50')` at scale=2 — same value, different unscaled int64s).
- **DECIMAL arithmetic:** `dec_add(a, sa, b, sb)` etc. take scale args explicitly.
- **TIMESTAMP arithmetic / formatting:** unit (microseconds vs milliseconds vs seconds) affects semantics.
- **Cross-type temporal arithmetic** (date+interval, timestamp-timestamp=interval): different output type, depends on descriptor.

At the `DrakenVector*`-stack boundary the eval-PM is migrating to, this info is lost unless explicitly threaded through. Three resolution options for the eval-PM to choose:

**Option α — Pass descriptor args explicitly when calling these functions.**

Extend `draken_compare_dv` / `draken_arithmetic_dv` (or add `draken_compare_dv_decimal` / `draken_arithmetic_dv_decimal` siblings) with per-type descriptor args:

```c
DrakenVector* draken_arithmetic_dv_decimal(
    int op_code,
    DrakenVector* left,  uint8_t left_scale,
    DrakenVector* right, uint8_t right_scale,
    uint32_t n_rows,
    DrakenFrameArena* arena);
```

Eval-PM's anchor list still holds the `Vector` Python wrappers (which carry `_nb`, which has access to the descriptor via `logical_type_*` properties). Extract scale/unit at dispatch-time, pass through to the function.

Pros: minimal new infrastructure; reuses existing kernels; clean per-type signatures.
Cons: eval-PM dispatch needs per-type-tag specialization in BC_COMPARE / BC_BINARY_OP handlers — `if left.type == DRAKEN_DECIMAL: extract scales; call decimal variant`. A few extra lines per type at dispatch.

**Option β — Carry descriptors alongside the `DrakenVector*` stack.**

Add a parallel `DrakenLogicalType*` array next to the `DrakenVector*` stack. The eval-PM's stack push/pop maintains both in lockstep. Compare/arith functions take both:

```c
DrakenVector* draken_compare_dv(
    ..., DrakenVector* left, const DrakenLogicalType* left_lt,
    ..., DrakenVector* right, const DrakenLogicalType* right_lt,
    ...);
```

Pros: descriptor info always available; no per-type dispatch branching at the eval-PM level.
Cons: every stack op carries the cost; most ops don't need it.

**Option γ — Embed descriptor pointer in `DrakenVector` struct.**

Change `DrakenVector` to carry `const DrakenLogicalType* logical_type` directly. ABI change; touches every Vector allocation site.

Pros: descriptor flows for free with the value; no signature changes anywhere.
Cons: ABI bump (the guard pin in `_abi_guard.cpp` covers this); touches every Vector construction site; risks reintroducing the "shape-as-type" or similar coupling we just cleaned up.

**Draken-PM recommendation: Option α.** It keeps `DrakenVector` lean and ABI-stable, doesn't add cost to the common path (the int64/float64/bool stack ops don't pay for descriptors they don't use), and the eval-PM dispatch already needs per-type-tag branching at BC_COMPARE / BC_BINARY_OP. The dispatch shape is:

```cython
cdef DrakenVector* result
if left_type == DRAKEN_DECIMAL:
    result = draken_arithmetic_dv_decimal(op, left, left_scale, right, right_scale, n, arena)
elif left_type == DRAKEN_TIMESTAMP64 and right_type == DRAKEN_INTERVAL:
    result = draken_temporal_arith_dv(op, left, left_unit, right, n, arena)
elif left_type == DRAKEN_INT64 or left_type == DRAKEN_FLOAT64:
    result = draken_arithmetic_dv(op, left, right, n, arena)  # existing
else:
    result = NULL  # falls back to Python
```

If you (eval-PM) confirm Option α, I'll add:
- `draken_compare_dv_decimal(op, left, sa, right, sb, n, arena)` — scale-aware decimal compare
- `draken_arithmetic_dv_decimal(op, left, sa, sp, right, sb, rp, n, arena)` — scale + precision-aware
- `draken_arithmetic_dv_temporal(op, left, left_type, left_unit, right, right_type, right_unit, n, arena)` — handles date+interval, timestamp+interval, date-date=interval, timestamp-timestamp=interval

If you prefer β or γ, surface and I'll redesign. The status table above reflects current state under "no descriptor access yet" — meaning DECIMAL falls back to Python until this is resolved, and temporal arithmetic stays Python-mediated regardless.

**Architect decisions answered:**

- *DrakenVector\* struct arena-allocated?* **Yes.** `draken_compare_dv` will allocate the result struct via `draken_frame_arena_alloc(arena, sizeof(DrakenVector))` and fold the kernel's returned data/validity buffers into the arena via `draken_frame_arena_adopt`. Caller doesn't free anything; arena destroy cleans up.
- *Does `draken_compare_dv` cover LIKE/RLIKE?* **No** — int-dispatchable ops only initially (EQ, NE, LT, LE, GT, GE, IN_LIST). String LIKE/RLIKE/regex stay Python-mediated; the eval-PM's Python fallback path handles them. Per your stated preference.

**Eval-PM is unblocked NOW on Phase 4 stack rewrite.** The pieces you can build immediately (the BC_AND/OR/XOR/NOT path is already C-level via `bitmap_ops`, and BC_COMPARE / BC_BINARY_OP can use Python-mediated fallbacks until Stage B lands):
1. Wire arena into `execute_bytecode` (create at entry, destroy at exit).
2. Replace `cdef list stack` with `cdef DrakenVector* stack[MAX_STACK_DEPTH_C]` + anchor list for borrowed column pointers.
3. Port BC_AND/OR/XOR/NOT directly (already C-level via `bitmap_ops`).
4. Port BC_COMPARE / BC_BINARY_OP / BC_UNARY_OP / BC_FUNCTION / BC_CAST / BC_EXTRACTION / BC_CASE as Python-mediated; anchor result, extract `unified()` pointer for stack slot.

When Stage B lands (int64+float64 `draken_compare_dv` and `draken_arithmetic_dv`), update the BC_COMPARE / BC_BINARY_OP handlers to call them for the common type combos and fall back to Python for the rest. No re-architecture needed.

---

**Deliverable 1 — `cdef extern` for `draken_frame_arena_*`** ✅ DELIVERED

The arena C API already exists in `draken/core/frame_arena.h` and is compiled into `draken_native.so`. It has no Cython declaration. Add `cdef extern from "core/frame_arena.h"` declarations to a draken `.pxd` (preferred — keeps the arena accessible to any consumer) or directly in `evaluation.pyx` (acceptable as a start). Minimum surface needed by the eval engine:

```cython
cdef extern from "core/frame_arena.h":
    ctypedef struct DrakenFrameArena:
        pass  # opaque
    DrakenFrameArena* draken_frame_arena_create() nogil
    void              draken_frame_arena_destroy(DrakenFrameArena*) nogil
    void*             draken_frame_arena_alloc(DrakenFrameArena*, size_t nbytes) nogil
    void              draken_frame_arena_release(DrakenFrameArena*, void* ptr) nogil
```

**Deliverable 2 — `draken_compare_dv`**

Signature (open to draken-PM revision):

```c
// draken/core/compare_dv.h  (new header, or added to an existing one)
//
// Compare left and right DrakenVectors element-wise using op_code.
// Result is a DRAKEN_BOOL vector whose data bitmap is allocated from `arena`.
// Returns NULL on OOM or unsupported type combination.
// `left_type_hint` / `right_type_hint`: BCTypeCode (0=none, 1=date, 2=timestamp).
DrakenVector* draken_compare_dv(
    int              op_code,         // BCCompareOpCode (OP_EQ, OP_GT, OP_IN_LIST, …)
    DrakenVector*    left,
    DrakenVector*    right,
    int16_t          left_type_hint,
    int16_t          right_type_hint,
    uint32_t         n_rows,
    DrakenFrameArena* arena
);
```

The returned `DrakenVector*` points to arena memory (struct + data bitmap both arena-allocated). The caller does not free it directly; it is released either via `draken_frame_arena_release` at frame exit (for the final result) or freed by `draken_frame_arena_destroy` (for intermediates).

Internally, this function covers the type dispatch currently done in Python by `draken_compare_int` / `_timestamp_compare` / `_date32_compare` / etc. The Python-level functions can remain as thin wrappers for callers that still need Python objects; `draken_compare_dv` is an additive C-level entry point.

**Deliverable 3 — `draken_arithmetic_dv`**

Signature:

```c
// Perform a binary arithmetic op on two DrakenVectors.
// Result is allocated from `arena`; type determined by left/right types and op_code.
// Returns NULL on OOM or unsupported combination.
DrakenVector* draken_arithmetic_dv(
    int              op_code,         // BCBinaryOpCode (BOP_PLUS, BOP_MINUS, …)
    DrakenVector*    left,
    DrakenVector*    right,
    uint32_t         n_rows,
    DrakenFrameArena* arena
);
```

The string-concat and date+interval paths can remain Python-mediated for now (the eval-PM will fall back to Python for those op codes and wrap the result); the numeric arithmetic paths (PLUS/MINUS/MULTIPLY/DIVIDE/MODULO on int64/float64) are the high-priority cases.

**What the eval-PM will do once these land:**

1. Wire arena into `execute_bytecode` (frame create at entry, destroy at exit).
2. Replace `cdef list stack` with `cdef DrakenVector* stack[MAX_STACK_DEPTH_C]` + anchor list for borrowed column pointers.
3. Port each opcode handler to push/pop `DrakenVector*`.
4. BC_AND/OR/XOR/NOT: use `c_and/or/xor/not_bitmap` directly (already C-level).
5. BC_COMPARE: call `draken_compare_dv`.
6. BC_BINARY_OP: call `draken_arithmetic_dv`; Python fallback for unimplemented ops.
7. BC_UNARY_OP, BC_FUNCTION, BC_CAST, BC_EXTRACTION, BC_CASE: Python-mediated initially; anchor result, extract `unified()` pointer for stack slot.

**Architect decisions required from draken-PM:**

- Is `DrakenVector*` struct itself arena-allocated (as above), or stack-allocated by the caller with only the data buffers arena-allocated? (The eval-PM prefers arena-allocated struct for simplicity; caller pattern in the design doc §Phase 4 shows struct also released via `release()`.)
- Should `draken_compare_dv` cover ALL compare ops (including string LIKE/RLIKE) or only the int-dispatchable ones? (Latter preferred initially — string ops can stay Python-mediated and fall back gracefully.)

---

**Files (original scope):** [`opteryx/expression/evaluator/evaluation.pyx`](../evaluation.pyx), [`draken/core/alloc.h`](../../../../draken/core/alloc.h), [`draken/core/vector_alloc.h`](../../../../draken/core/vector_alloc.h), new `draken/core/frame_arena.h` (does not exist yet — draken-PM deliverable)

`cdef list stack = [None] * cap` is the symptom. The cause: stack items are
`Vector` / `BoolVector` — Python extension types. Cython cannot produce a C array
of Python objects.

**Fix:** the stack holds `DrakenVector*`:
```cython
cdef DrakenVector* stack[MAX_STACK_DEPTH]   # valid C array of C pointers
```

This requires a **frame arena** — a per-evaluation-call pool from which computed
intermediate `DrakenVector` structs and their backing data buffers are allocated.
The arena is freed in one shot at frame exit. Columns borrowed from the morsel
are not freed by the frame (borrowed pointers, not owned).

This is the standard pattern in C++ query engines (Velox, DuckDB both use
per-pipeline memory pools scoped to morsel evaluation).

**What already exists in draken:**
- [`draken/core/alloc.h`](../../../../draken/core/alloc.h) — mimalloc-backed `draken_malloc` / `draken_free`. Every allocation in draken goes through this.
- [`draken/core/vector_alloc.h`](../../../../draken/core/vector_alloc.h) — `draken_vector_from_dense` / `draken_vector_from_constant` / `draken_vector_from_dict`. These construct `DrakenVector` structs from caller-owned buffers.

**What does not yet exist:** a frame-scoped arena that batches allocations and
frees them in one shot. The draken-PM needs to provide:

```c
// draken/core/frame_arena.h  (proposed — does not exist yet)
typedef struct DrakenFrameArena DrakenFrameArena;

DrakenFrameArena* draken_frame_arena_create(void);
void*             draken_frame_arena_alloc(DrakenFrameArena*, size_t nbytes);
void              draken_frame_arena_destroy(DrakenFrameArena*);   // frees all at once
```

With this, the stack rewrite in `evaluation.pyx` becomes:

```cython
cdef DrakenFrameArena* arena = draken_frame_arena_create()
cdef DrakenVector* stack[MAX_STACK_DEPTH]
cdef Py_ssize_t sp = 0

# ... dispatch loop, all intermediate vectors allocated from arena ...

# result at stack[0]: borrow a Python Vector wrapper for the return
result = Vector(draken_make_python_handle(stack[0]))   # GIL re-acquired here

draken_frame_arena_destroy(arena)   # frees all intermediates
return result
```

The `c_execute_bytecode_inner` bitmap VM and the general VM converge into one
`noexcept nogil` function. The GIL is acquired only once: at frame entry (to
resolve morsel column pointers) and once at frame exit (to wrap the result).

**Architect call (2026-05-27, draken-PM): arena design closed.**

Closing each decision:

**1. Granularity — tracked-pointer allocator backed by `draken_malloc`.**

Not a custom bump allocator, not a slab-of-structs. The arena holds an internal
list of every pointer it has issued; `destroy` walks the list and `draken_free`s
each. Backing is `draken_malloc` (mimalloc), so individual allocations get
mimalloc's existing per-size-class fast paths for free.

Rationale: simplest workable thing that's predictably correct. Bump allocation
is an optimisation we can layer in later if measurement shows allocator pressure
during evaluation. Premature optimisation here is the exact pattern that bit
the producer-surface design.

**2. Thread safety — none. One arena per frame.**

Confirmed. The arena lives in one cdef function call; no concurrent access.
No mutex, no atomic, no thread-local. If the future parallelism work surfaces
a need for shared arenas, that's a separate ticket.

**3. Result extraction — `release()`, not copy.**

The arena exposes `draken_frame_arena_release(arena, ptr)` which removes a
pointer from the internal tracking list. After release, the caller owns
that pointer and must `draken_free` it (or hand it to `draken_vector_own_raw`,
which will). No-op if the pointer is NULL or not tracked.

At frame exit, the eval engine releases each buffer the result `DrakenVector`
owns (data, validity if present), then wraps via the existing
`draken_vector_own_raw` bridge. The arena then `destroy`s, freeing all
remaining intermediates in one shot.

This composes cleanly with the existing bridge — no new ownership-transfer
primitive, no copy at the boundary.

**The "draken_make_python_handle" reference:** that function doesn't exist
as a new primitive — it's the existing `draken_vector_own_raw` (or
`draken_vector_own_string` for VARCHAR/NVARCHAR/VARBINARY) from
`draken/core/draken_bridge.h`. The eval-PM hadn't seen its current name;
no new bridge function is needed.

---

### Phase 4 — implementation commitment (draken-PM)

I will deliver the following before the Cython stack rewrite can be written:

**`draken/core/frame_arena.h`** (new header):

```c
#pragma once
#include "alloc.h"
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DrakenFrameArena DrakenFrameArena;

// Construct a new arena. Returns NULL on OOM.
DrakenFrameArena* draken_frame_arena_create(void);

// Free the arena and draken_free every buffer still tracked by it.
// Buffers that were released via draken_frame_arena_release are NOT
// freed (caller owns them).
void draken_frame_arena_destroy(DrakenFrameArena*);

// Allocate `nbytes` via draken_malloc and track. Caller does not free
// directly; the arena frees on destroy unless the pointer is released
// first. Returns NULL on OOM (in which case nothing is tracked).
void* draken_frame_arena_alloc(DrakenFrameArena*, size_t nbytes);

// Remove `ptr` from arena tracking. After this call the caller owns
// `ptr` and must draken_free it (or hand to draken_vector_own_raw).
// No-op when ptr is NULL or not in this arena's tracking list.
void draken_frame_arena_release(DrakenFrameArena*, void* ptr);

#ifdef __cplusplus
}
#endif
```

**`draken/core/frame_arena.cpp`** — tracked-pointer implementation backed
by `std::vector<void*>` + `draken_malloc` / `draken_free`. ~30 lines.

**`setup.py`** — adds `frame_arena.cpp` to `draken.draken_native`'s
sources list (next to `bitmap_ops.cpp`). Symbol lives in
`draken_native.so` and is visible via the `RTLD_GLOBAL` bridge pattern
to consumer extensions.

**Native tests** under `draken/tests/native/test_frame_arena.py`:
- create/destroy with no allocations
- alloc + destroy (verifies pointer is freed; can verify via ASAN run)
- alloc + release + destroy (verifies released pointer is NOT freed by
  destroy and caller is responsible)
- multiple allocs, release one, destroy frees the others
- release of NULL is no-op
- release of pointer not in arena is no-op

**Status (2026-05-27): DELIVERED.**

- `draken/core/frame_arena.h` + `.cpp` — implemented per the API above.
- `setup.py` — sources wired into `draken.draken_native`.
- `draken/tests/native/test_frame_arena.py` — passes; the broader
  `make dt` suite is at 2882 green.
- Symbol is callable from Cython via `cdef extern from "frame_arena.h"`
  and reachable through the `RTLD_GLOBAL` bridge pattern.

Eval-PM is unblocked on Phase 4 — start the stack rewrite whenever
the order brings it. If during the rewrite the API turns out to be
missing something (e.g. an iteration accessor to walk tracked
pointers, a bulk-release helper), surface and I'll extend it. The
current API is the minimum that supports the documented caller
pattern; deliberately small.

### Phase 4 — caller pattern

With the arena available:

```cython
# at frame entry
cdef DrakenFrameArena* arena = draken_frame_arena_create()
if arena is NULL:
    raise MemoryError()
cdef DrakenVector* stack[MAX_STACK_DEPTH]
cdef Py_ssize_t sp = 0

# ... dispatch loop ...
# all intermediate vectors allocated via draken_frame_arena_alloc(arena, ...)

# at frame exit — transfer the result out of arena ownership
cdef DrakenVector* result = stack[0]
draken_frame_arena_release(arena, result.data)
if result.validity != NULL:
    draken_frame_arena_release(arena, result.validity)
# selection is either draken_identity_sel/draken_zero_sel (global, never
# freed) or owned (release if owned) — eval engine knows which based on
# how it constructed the vector.

# Wrap via existing bridge — takes ownership of the released buffers.
py_result = draken_vector_own_raw(
    result.data, result.validity, result.length, result.type
)

draken_frame_arena_destroy(arena)   # frees everything still tracked
return py_result
```

**One caveat to flag:** the result `DrakenVector` *struct itself* is
allocated from the arena (it's a small struct). The bridge function
constructs its own `VectorOwner` from the buffer pointers, so the
struct can be discarded at `destroy` time — only the data/validity
buffers need to be released. This composes correctly as written.

If the eval engine wants to avoid the small per-frame struct allocation,
the result `DrakenVector` can be stack-allocated (`cdef DrakenVector
result_struct`) and only the buffers come from the arena. Either shape
works; the arena doesn't care.

---

## Summary table

| Phase | Blocker | Architect decision | Removes from hot path | Status |
|---|---|---|---|---|
| 1 | Instruction word PyObject* for op codes and types | None | 3× `PyObject*` unpack per BC_COMPARE / BC_BINARY_OP / BC_UNARY_OP | ✅ Done |
| 2 | BC_LEGACY (NT_CASE, BC_EXTRACTION inline imports) | None | Tree-walker fallback; runtime imports in dispatch loop | ✅ Done |
| 3 | BC_FUNCTION Python callable dispatch | Option C (defer until Phase 4) | Python list, `getattr`, `*args` call per function invocation | ⏳ Blocked on Phase 4 |
| 4 | Stack holds Python objects | Frame arena delivered; DrakenVector\* stack needs kernel C interfaces | IN-list fold ✅. DrakenVector\* stack blocked on `draken_compare_dv` + `call_arithmetic_op_dv` | 🔄 Partial |
