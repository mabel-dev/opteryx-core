# E.20 — Expression Evaluator Survey

> Status: COMPLETE — survey only. No code changes. Written 2026-05-24.
> Answers the Part B question from E.20 ticket: where is the evaluator,
> what does it import, how many call sites, what sequencing recommendation?

---

## 1. Location

The entire expression evaluator lives in one directory:

```
opteryx/expression/evaluator/
```

Ten `.pyx` files compiled into a **single shared object** (`_impl.cpython-*.so`) via
`_impl.pyx` using Cython textual `include`. Total: **3,078 lines**.

| File | Lines | Role |
|---|--:|---|
| `evaluation.pyx` | 1,499 | Main tree-walker + bytecode VM (two distinct sections — see §3) |
| `comparisons.pyx` | 353 | All comparison dispatch (typed kernels: int64, float64, string, bool, decimal, timestamp, date32, interval) |
| `type_coercion.pyx` | 233 | Temporal/numeric type coercion + cast routing |
| `case_eval.pyx` | 212 | CASE expression evaluation |
| `arithmetic.pyx` | 175 | Binary arithmetic op routing |
| `_impl.pyx` | 172 | Compilation orchestrator: OP_* DEF constants + textual include of all leaf modules |
| `temporal_ops.pyx` | 155 | Date/timestamp arithmetic kernels |
| `json_ops.pyx` | 80 | JSON extraction helpers |
| `string_ops.pyx` | 91 | String operation dispatch |
| `arithmetic_dispatch.pyx` | 76 | Kernel registry lookup (`call_arithmetic_op`) |
| `function_execution.pyx` | 32 | `apply_bounded_function` entry point |

There are **no other locations**: no inline operator dispatch files, no separate expression
package outside `opteryx/expression/evaluator/`. All vector dispatch funnels through this
directory.

---

## 2. Import Shape Today

`evaluation.pyx` has a hard structural split at line ~650 (the bytecode VM section boundary).
The two halves import draken differently.

### 2.1 Upper half — tree-walker (lines 1–650): Python `import` only

```python
# draken vector constructors / classes (Python-level)
from draken.vectors.bool_vector      import BoolVector
from draken.vectors.integer64_vector import Integer64Vector
from draken.vectors.string_vector    import StringVector
from draken.vectors.scalar_constructors import from_scalar
from draken.morsels.morsel           import Morsel
from draken.interop.vector_sequence  import vector_from_sequence, ...

# C′ nanobind extensions (opteryx/compiled/nanobind/)
from opteryx.compiled.nanobind.vector_accessors         import vector_string_is_empty, ...
from opteryx.compiled.nanobind.vector_misc              import vector_in_list
from opteryx.compiled.nanobind.vector_string_search     import vector_contains
from opteryx.compiled.nanobind.vector_selection_concat  import vector_concat, ...
from opteryx.compiled.nanobind.vector_bool_ops          import ...
from opteryx.compiled.nanobind.vector_json              import vector_json_extract
from opteryx.compiled.nanobind.vector_temporal_arith    import ...
from opteryx.compiled.nanobind.vector_special           import vector_map_access_string, ...

# Remaining old Cython vector_ops (not yet ported to C′)
from opteryx.compiled.vector_ops import vector_bitwise_not
```

Additional `vector_ops` imports across sibling modules:

| File | `vector_ops` symbols imported |
|---|---|
| `comparisons.pyx` | `vector_allop_eq/neq`, `vector_anyop_eq/neq/gt/lt/gte/lte/like/ilike`, `vector_contains`, `vector_like`, `vector_rlike` |
| `string_ops.pyx` | `vector_like`, `vector_rlike` |
| `case_eval.pyx` | `assemble_bool`, `assemble_fixed`, `assemble_flat_string`, `decide_one_branch`, `group_indices_and_perm`, `_make_const_int16`, `_make_range_int32` |
| `arithmetic.pyx` | `vector_string_concat_binary` (imported inline inside functions) |
| `json_ops.pyx` | `vector_contains_all`, `vector_contains_any` |
| `temporal_ops.pyx` | `vector_in_list` (via nanobind now; stale comment in .c) |

### 2.2 Lower half — bytecode VM (lines 650–1,499): `cimport` of draken Cython types

```cython
from draken.core.buffers    cimport DrakenVector          # buffers.pxd — EXISTS ✓
from draken.morsels.morsel  cimport Morsel                # morsel.pxd  — MISSING ✗
from draken.vectors.bool_vector cimport (
    BoolVector,
    bool_vector_from_bits,
    c_and_bitmap, c_not_bitmap, c_or_bitmap, c_xor_bitmap, c_get_bitmap_ptrs,
)                                                          # bool_vector.pxd — MISSING ✗
from draken.vectors.vector  cimport Vector, simd_popcount # vector.pxd   — MISSING ✗
```

`type_coercion.pyx` also has:
```cython
from draken.vectors.vector cimport Vector                 # vector.pxd   — MISSING ✗
```

**Three of the four `.pxd` files required by the evaluator do not exist in the new draken.**
`buffers.pxd` survives; `morsel.pxd`, `bool_vector.pxd`, and `vector.pxd` were not
regenerated in the rebuild. This is the compile blocker — the evaluator cannot be built
as part of the full `setup.py build_ext` until this is resolved.

### 2.3 Summary by import layer

| Import type | Count (lines) | Status |
|---|---|---|
| Python `import draken.*` — vector classes, Morsel | ~35 | Live; will redirect to `draken_native` surface as part of Python-import phase |
| C′ nanobind extensions (`opteryx/compiled/nanobind/`) | ~12 import sites | Live; working today |
| Old Cython `vector_ops` (not yet C′) | ~8 import sites | Live but pending per-op nanobind migration |
| `cimport draken.core.buffers` — `DrakenVector` struct | 1 site | Works — `buffers.pxd` exists |
| `cimport draken.morsels.morsel` — `Morsel` | 1 site | **BROKEN** — `morsel.pxd` missing |
| `cimport draken.vectors.bool_vector` — bitmap ops | 1 site (7 symbols) | **BROKEN** — `bool_vector.pxd` missing |
| `cimport draken.vectors.vector` — `Vector`, `simd_popcount` | 2 sites | **BROKEN** — `vector.pxd` missing |

---

## 3. Call Sites

### 3.1 Tree-walker path (upper half)

All dispatch in `_eval_value()`, `draken_compare_int()`, `draken_between()`, and
`apply_bounded_function()`. Approximately **99 vector_\* call sites** across `.pyx` files
(by grep on source files, excluding generated `.c`/`.cpp`).

Concentration:
- `evaluation.pyx` — ~45 sites (main dispatch: `_eval_value`, unary ops, scalar checks)
- `comparisons.pyx` — ~35 sites (all typed comparison kernels, IN LIST, LIKE, RLIKE)
- `arithmetic.pyx` + `arithmetic_dispatch.pyx` — ~10 sites
- Remaining 5 modules — ~9 sites combined

### 3.2 Bytecode VM path (lower half, ~850 lines from line 650)

The VM is a GIL-free three-phase filter executor:
- Phase 1 `_execute_bytecode_prepass`: resolves `BC_LOAD_COL` columns, mallocs scratch bitmap buffers
- Phase 2 `c_execute_bytecode_inner` (`noexcept nogil`): pure C bitmap operations, no Python objects
- Phase 3 `_execute_bytecode_postpass`: wraps result bitmap into a `BoolVector`

All `cimport` statements are for this path. The 7 bitmap symbols from `bool_vector.pxd`
(`bool_vector_from_bits`, `c_and_bitmap`, `c_not_bitmap`, `c_or_bitmap`, `c_xor_bitmap`,
`c_get_bitmap_ptrs`) are used in Phases 1 and 3. `simd_popcount` is used for row counting.
`Morsel` and `Vector` cimports are used for typed argument passing at C level.

This path **cannot be rewritten incrementally** — it is a single coherent no-GIL unit.
Any fix must handle all four cimport lines at once.

---

## 4. Is It Isolated or Distributed?

**Hybrid, with a clear isolation boundary:**

- **Isolated coordination**: `opteryx/expression/evaluator/` is the single location where
  expression trees are walked and vector operations are dispatched. No operator file
  (outside `opteryx/compiled/operators/`) calls `vector_*` functions directly.
  All 21 operator rewrites will route through this evaluator — fixing it once benefits all.

- **Distributed kernels**: within the evaluator, dispatch is spread across 10 modules by
  operation domain (comparisons, arithmetic, case, temporal, string, json, type coercion).
  These are textually included into one `.so`, so they compile together.

- **The bitmap VM is internally isolated**: the `cimport` block (lines 650–1,499 of
  `evaluation.pyx`) is a self-contained no-GIL section. It does not appear in any other
  file. It is not called from operator files. It is called only from the Python-level
  `execute_bytecode()` and `evaluate_bitmap()` entry points at the top of the same file.

---

## 5. Sequencing Recommendation

**Hybrid: `.pxd` unblock first, then in-place per-op migrations.**

### 5.1 Blocker: the missing `.pxd` files (one phase, before any operator rewrites)

The 3 missing `.pxd` files block the evaluator compile, which blocks the full `make q`
regression suite. This is distinct from the `DRAKEN_BUILD=1` workaround used for
`draken/tests/`.

There are two options:

**Option A — Stub `.pxd` files** (lower effort, ~2h):
Add minimal `.pxd` stubs for `morsel.pxd`, `bool_vector.pxd`, and `vector.pxd` that
expose only the symbols the evaluator actually cimports (8 symbols total). This is a
valid pattern when the implementation is in a `.so` and the `.pxd` just declares the
C-level interface. `buffers.pxd` already does this correctly.

**Option B — Rewrite the bitmap VM** (higher effort, ~8–12h):
Replace the 4 cimport lines with operations on `DrakenVector*` directly (struct already
accessible via `buffers.pxd`). Replace `c_and_bitmap` etc. with equivalent C-level
inline ops; replace `BoolVector.from_bits` with `draken_vector_own_raw`. This is the
"correct" long-term direction but is a non-trivial rewrite of the GIL-free inner loop.

**Recommendation: Option A first.** It is a targeted fix that unblocks the compile with
minimal risk. Option B is the right architectural direction but should be its own scoped
ticket after the suite is green.

### 5.2 vector_ops Cython migrations: in-place

The remaining `vector_ops` Cython functions imported by the evaluator
(`vector_bitwise_not`, `vector_like`, `vector_rlike`, `vector_anyop_*`, `vector_allop_*`,
`assemble_bool`, `assemble_fixed`, `assemble_flat_string`, `decide_one_branch`,
`group_indices_and_perm`, `vector_string_concat_binary`, `vector_contains_all/any`) are
not compiled into the evaluator — they are imported at Python level. When each of these
gets its own nanobind C′ port, the evaluator caller just changes one import line.
These updates happen **in-place per operator phase** — no pre-work needed.

### 5.3 What does NOT need pre-work before operator rewrites

The operator files call the evaluator's public entry points (`evaluate_draken`,
`evaluate_and_append_draken`, `execute_bytecode`). They do not reach into the evaluator
internals. The evaluator's internal dispatch is not visible to operators. Therefore:

- **Operator rewrites do not require refactoring the evaluator first** (other than
  unblocking the compile via Option A above).
- The tree-walker and comparison/arithmetic dispatch layers use Python `import` of draken
  classes — these are updated as part of the Python-import phase (Phase 12 in `E0_consumer_rewrite_scoping.md`), not per-operator.

### 5.4 Recommended sequence

1. **E.21 (or sub-task of E.20)**: Add 3 minimal `.pxd` stubs (Option A) → evaluator compiles → `make q` can run.
2. **E.22+**: Begin operator rewrites. Evaluator dispatch updates (the `import draken.*` redirections) happen as part of Phase 12, deferred until all cimport sites are done.
3. **Later**: Scope the bitmap VM rewrite (Option B) as a standalone performance ticket.

---

## 6. Key Entry Points (for future reference)

| Function | File:line | Purpose |
|---|---|---|
| `evaluate_draken(node, morsel)` | `evaluation.pyx:407` | Tree-walker for predicate/WHERE |
| `evaluate_and_append_draken(nodes, morsel)` | `evaluation.pyx:539` | Projection / computed columns |
| `execute_bytecode(bc, morsel)` | `evaluation.pyx:1181` | Postfix bytecode VM (generic) |
| `evaluate_bitmap(bc, morsel)` | `evaluation.pyx:~1450` | Three-phase GIL-free bitmap VM |
| `_eval_value(node, morsel)` | `evaluation.pyx:~200` | Single-node type/op router |
| `draken_compare_int(op_code, left, right)` | `comparisons.pyx:175` | Primary comparison dispatcher |
| `draken_between(col, lower, upper)` | `comparisons.pyx:~320` | BETWEEN operator |
| `call_arithmetic_op(op, left, right)` | `arithmetic_dispatch.pyx:22` | Arithmetic kernel registry |
| `evaluate_case(node, morsel)` | `case_eval.pyx:~80` | CASE expression |
