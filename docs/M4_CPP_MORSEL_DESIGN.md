# C++-First Morsel / Vector — Investigation & Staged Design

Status: **investigation + design, awaiting architect sign-off. No code cut.**
Date: 2026-06-16

Goal restated: make the morsel/vector that flows through the execution engine a
genuine C++ object, with the Python `Morsel`/`Vector` reduced to a real
boundary-only shim — so the operator chain and per-morsel assembly run nogil
(GIL released at the scan edge, the whole pipeline below Python-free). This is
the actual enabler for M4: under the GIL, concurrent pipeline segments serialize,
so M4 cannot deliver a speedup on CPU-bound operators until the operator surface
is nogil.

---

## Part A — Reconciling the discrepancy (honest finding)

**The architect believed C++-first morsel/operator work was done. It was never
scoped.** This is not a regression and not a revert — the work does not exist in
any design doc or in code. Here is precisely what *was* done and why the
confusion is understandable.

### A.1 What IS C++-first today (real)

1. **Column DATA is genuinely C++-owned and freed off-GIL.** The bytes live in a
   C++ `VectorOwner` (`draken/draken_native.cpp:87`) holding `OwnedBuffer<T> =
   std::unique_ptr<T, DrakenFree>` over `draken_malloc`'d blocks; the payload is
   the 40-byte POD `DrakenVector` struct (`draken/core/buffers.h:152`, frozen
   ABI). Destruction is RAII via `draken_free` (mimalloc) and needs no GIL.
   **This half of "C++-first" is true.**

2. **A nogil C-ABI kernel surface exists** (the Phase-9 work): `draken/ops/`
   kernels take/return the POD `DrakenVector`/`VecResult` and are declared
   `nogil`. ~55 cast/binary/extraction kernels are registered and real
   (`draken/ops/kernels/kernel_registry.cpp`).

### A.2 What is NOT C++-first (the surprise)

1. **The `Vector` is a PyObject.** `draken/vectors/vector.pxd:16` —
   `cdef class Vector` holding `_nb` (the nanobind PyObject that owns the
   `VectorOwner`) + `_dv` (borrowed `const DrakenVector*`). It is a Cython
   extension type = heap-allocated, refcounted PyObject. The name "shim" is
   **accurate to its original design intent** (design doc `E24_cython_vector_shim`
   — it was *designed* as a thin typed Cython wrapper over a nanobind handle),
   but it is **load-bearing**: every operator manipulates these PyObjects.

2. **The `Morsel` is a PyObject** (`draken/morsels/morsel.pxd:16`,
   `_morsel_shim.pyx:88`) — `cdef class Morsel` holding
   `std::vector<PyObject*> _columns` (a C++ vector of *borrowed-then-INCREF'd
   Vector PyObjects*) + a Python `list _col_names`. The C++ vector is a GIL
   optimization (avoids per-access PyObject indexing), **not** a C++ column store —
   the elements are PyObjects and every `_get_column` returns a PyObject.

3. **The operator chain holds the GIL end-to-end.** `_operators.pyx`: the push
   pipeline is `cpdef void push(self, Morsel) except *` → `cdef void
   _dispatch_push(self, Morsel)` → `cpdef void _push_impl`, and the source side
   is `cdef Morsel next_morsel(self)`. All take/return the PyObject `Morsel`;
   **none are `nogil`**. `drive_scan` (`_operators.pyx:683`) is a Python
   generator. The GIL is held for the entire operator chain; only narrow inner
   kernel loops (`c_hash`, distinct perfect-hash, `draken_compare_dv`) drop it
   via `with nogil` after resolving raw pointers under the GIL.

4. **No design doc ever specified a C++ morsel.** The draken design docs `00`–`09`
   are the *vector library* rebuild (data model, ownership, dispatch, binding,
   statistics, value encoding, consumer contract). `E24` explicitly designs a
   *Cython* vector shim. There is no doc proposing a C++ morsel object threaded
   through operators running nogil. **The architect conflated "draken is
   C++-first" (true — the vector library and kernels) with "the execution
   morsel/operator chain is C++-first/nogil" (never built).**

### A.3 Phase-9 executor flip — real state (partial, asymmetric)

The nogil C-kernel ABI is wired (`BytecodeInstr.kernel_fn` / `ctx_ptr` +
`BC_INSTR_C_NATIVE` flag), but the executor only *honours* it for **BC_CAST**.
`evaluation.pyx`:

- **BC_CAST** — LIVE: calls `(<cast_fn_t>slot.kernel_fn)(ctx, dv_left)` directly.
- **BC_BINARY_OP** — NOT flipped: explicitly reverts to the Python
  `slot.callable_ref` closure (comment: "binop reverts to resolved kernel").
- **BC_FUNCTION** — NOT flipped: always calls the Python/nanobind `callable_ref`.
- **BC_EXTRACTION** — NOT flipped: hardcoded `_vector_*` nanobind calls;
  `kernel_fn` is set by the binder but dead at runtime.
- Boolean/comparison predicate VM (`evaluate_bitmap`, `draken_compare_dv`) IS
  nogil.

**Net: roughly only filter-predicate evaluation + casts run nogil today; binary
arithmetic, scalar functions, and extractions still take the GIL.** This is a
hard dependency for making `filter`/`project` nogil (see Part C).

### A.4 One-line summary for the architect

> The column **bytes** are C++/off-GIL. The **containers** (`Morsel`, `Vector`)
> are PyObjects and the **entire operator chain runs under the GIL**. The
> "C++-first" work was scoped to draken's vector library + kernel ABI, never to
> the morsel container or the operator surface. "Shim" was an accurate name for a
> deliberately-thin Cython wrapper — it just never stopped being load-bearing.

---

## Part B — The C++ morsel/vector type proposal

The target representation, owned by C++, no PyObject in the hot path:

```cpp
// draken/morsels/cxx_morsel.h  (proposed)
struct CxxColumn {
    DrakenVector view;                 // 40-byte POD; data[selection[i]] access
    std::shared_ptr<VectorOwner> own;  // keeps bytes alive; RAII off-GIL free
};

struct CxxMorsel {
    std::vector<CxxColumn>   columns;  // owns columns by value; move semantics
    std::vector<std::string> names;    // bytes, not PyObject
    uint32_t                 zero_col_rows = 0;
    // move-only; no copy. RAII frees columns when the last shared_ptr drops.
};
```

Key properties:
- **No PyObject anywhere.** `names` are `std::string`; columns are
  `VectorOwner`s held by `shared_ptr` (sharing is needed: joins/exchange fan a
  column into multiple output morsels without copying bytes).
- **Move semantics + RAII** replace `Py_INCREF`/`_morsel_decref`.
- **Operators get a nogil C-ABI surface** over `CxxMorsel&`/`CxxColumn` calling
  the existing `DrakenVector`/`VecResult` kernels — the same POD types the
  kernels already speak. No nanobind in the operator body.
- The existing per-morsel ops (`take`, `mask`, `slice`, `combine`,
  `partition_by_hash`, `align_tables`, `hash_keys`) port to free functions over
  `CxxMorsel` — most already have a nogil kernel underneath; today they bounce
  through the PyObject wrapper to reach it.

**The Python `Morsel`/`Vector` become true boundary shims:** constructed *only*
at the cursor/fetch boundary (result → user Python) by wrapping each
`CxxColumn.own` into a nanobind `Vector` PyObject. Nowhere else.

This is consistent with `VectorOwner` already being the C++ owner and with
`buffers.h`'s frozen `DrakenVector` ABI — we are not changing the data model,
only lifting the *container* from PyObject to C++.

---

## Part C — Non-big-bang transition (keep the suite green at every step)

A full rewrite of every operator at once would be red for weeks and violates
"broken but honest" only in the sense that it's unverifiable mid-flight. The
staged plan converts **one pipeline at a time** behind a shim↔C++ seam.

### C.0 The seam (enables incremental conversion)

Add a bidirectional, cheap adapter:
- `cxx_from_pymorsel(Morsel) -> CxxMorsel` — borrows the already-resolved
  `VectorOwner` shared refs (no byte copy; `_columns_to_pointers` already proves
  the pointers are stable for the morsel lifetime).
- `pymorsel_from_cxx(CxxMorsel) -> Morsel` — wraps each `CxxColumn` back into a
  `Vector` PyObject (the boundary shim, used at conversion edges and at the
  cursor).

An operator that has been converted accepts/produces `CxxMorsel`; an unconverted
neighbour gets the adapter at the edge. The suite stays green because the
*observable* result (the PyObject morsel reaching the cursor) is byte-identical.

### C.1 Stage ordering (scan → filter → grouped-agg first; the M4 target)

1. **Stage 0 — infrastructure.** Define `CxxMorsel`/`CxxColumn`, the seam, and a
   nogil C-ABI for the core morsel ops (`take`/`mask`/`slice`/`combine`/
   `hash_keys`/`partition_by_hash`/`align_tables`). No operator converted yet;
   prove the seam round-trips byte-identical (`make q` green at W=1).

2. **Stage 1 — scan emits `CxxMorsel`.** The native scan path
   (`parquet_read.pyx`, already a nogil cdef state machine below the wrap) builds
   `CxxMorsel` directly instead of `Morsel.from_vectors(...)`, releasing the GIL
   across assembly. Adapter to PyObject `Morsel` at `chain_head.push` until the
   next operator is converted. This removes the residual GIL hold flagged in
   `native_scan_morsel_path`.

3. **Stage 2 — grouped-aggregate-hashed nogil.** The M4 target breaker. Its hot
   work (hash keys → probe → accumulate, `merge()`) is already kernel-backed; lift
   the operator body to nogil over `CxxMorsel`. **No Phase-9 dependency** — group
   agg does not run the expression VM in its core path. This is why it goes before
   filter/project.

4. **Stage 3 — filter / project nogil.** *Gated on the Phase-9 executor flip*
   (Part A.3): these call the bytecode VM, which still takes the GIL for
   binary-op/function/extraction. Sequencing options below.

5. **Stage 4+ — joins, sort, distinct, window, set-ops, limit, exchange** — one
   per increment, each behind the seam.

6. **Stage N — boundary-only shim.** Once a full pipeline (scan→filter→grouped-agg
   →exit) is C++ end-to-end, `drive_scan`/push become nogil over `CxxMorsel`, and
   the PyObject `Morsel`/`Vector` are constructed only at the cursor. The shim is
   finally a shim.

Each stage gates on `make q` (182), `make tpch` (22), `make clickbench` (43)
identical, and ships behind the seam so partial conversion is always green.

### C.2 Composition with Phase-9 kernel ABI

Stage 3 (filter/project) **requires** the Phase-9 executor flip for BC_BINARY_OP
/ BC_FUNCTION / BC_EXTRACTION (currently GIL-bound). The flip is "all-or-nothing"
per `phase_9c_cast_kernels` unless a per-kernel real/stub gate is added. Two
sequencing options (architect decision Q3):
- **(a)** Finish the Phase-9 flip first, then convert filter/project — cleanest,
  but front-loads Phase-9 completion.
- **(b)** Convert the operators that *don't* need it first (grouped-agg, joins,
  sort, distinct — kernel-backed, no VM), deferring filter/project until Phase-9
  lands. Lets the M4 pipeline (scan→filter→grouped-agg) get *most* of its win
  (grouped-agg is the breaker/bottleneck) while filter stays GIL-bound but cheap.

### C.3 Composition with the M4 parallel scheduler

M4 (`parallel_engine.py`, `MAX_EXECUTION_WORKERS`) clones a pipeline segment per
worker and runs them on a `CppThreadPool`. Today those clones **serialize on the
GIL** the moment they do CPU work in any operator — which is exactly why Stage 1
measured geomean 0.98× on high-card ClickBench (the parallel filter/scan can't
actually run concurrently). **This task is the missing half of M4:** once the
scan→filter→grouped-agg segment is nogil `CxxMorsel`, M4's worker clones run truly
concurrently and the pipeline thesis (already CONFIRMED at 1.27×–2.39× on
favourable shapes *with GIL contention still present*) should extend to the
high-card regressors. The native thread-safe concurrent scan pull (Stage 5 of
`native_scan_morsel_path`) and the `CxxMorsel` operator surface together give M4 a
fully GIL-free segment.

---

## Part D — Architect decisions (LOCKED 2026-06-16)

- **Q1 — Scope: FULL C++-first, every operator.** Commit up front to converting
  all operators to nogil C++ over `CxxMorsel` (not a measure-first M4-segment
  pilot).
- **Q2 — Ownership: TRUE `CxxMorsel`** owning columns as `shared_ptr<VectorOwner>`.
  The Python `Vector`/`Morsel` PyObject is constructed ONLY at the cursor boundary.
  No interim "PyObject container + view" half-measure.
- **Q3 — Phase-9 FIRST.** Finish the all-or-nothing executor flip (BC_BINARY_OP /
  BC_FUNCTION / BC_EXTRACTION → C kernels, completing any stub kernels) BEFORE
  converting filter/project. The expression VM must be fully nogil before the
  operators that drive it can be.
- **Q4 — Operator bodies move to C++** (nanobind-bound), for maximum nogil purity —
  not Cython `cdef nogil` wrappers.

## Part E — Delivery sequence (reflecting locked decisions)

Single contract throughout: every stage gates on `make q` 182 / `make tpch` 22 /
`make clickbench` 43 **identical**, ships behind the shim↔C++ seam so partial
conversion is always green, and surfaces any new design-impacting choice before
acting. Header/`.hpp`/draken changes use `make compile` (not `make c`).

- **P9 — Complete the Phase-9 executor flip (prerequisite).** Implement the
  remaining real C kernels (function kernels deferred to "9a-fn"; any stub) and
  flip `evaluation.pyx` so BC_BINARY_OP / BC_FUNCTION / BC_EXTRACTION dispatch
  through `kernel_fn` under `BC_INSTR_C_NATIVE`. Add a per-kernel real/stub gate
  if needed to avoid the all-or-nothing trap. Outcome: expression VM fully nogil.
- **S0 — `CxxMorsel` infra + seam.** `draken/morsels/cxx_morsel.h` (`CxxColumn`,
  `CxxMorsel`, move/RAII), the C++ morsel-op surface (`take`/`mask`/`slice`/
  `combine`/`hash_keys`/`partition_by_hash`/`align_tables`), and
  `cxx_from_pymorsel` / `pymorsel_from_cxx`. Prove byte-identical round-trip; no
  operator converted.

  **S0.0 LANDED (2026-06-16):** `VectorOwner`/`OwnedBuffer`/`DrakenFree` extracted
  from `draken_native.cpp` into `draken/core/vector_owner.h` (behaviour-neutral;
  make compile + q 182 green) so the native scan + CxxMorsel can reference the type.

  **S0 OWNERSHIP CRUX (decision needed):** `CxxColumn` must SHARE a `VectorOwner` for
  the seam to be zero-copy both ways AND for joins/exchange to fan one column into
  many outputs (the reason `shared_ptr` was chosen). But the nanobind `Vector` stores
  `VectorOwner` BY VALUE INLINE (lifetime = Py refcount), and nanobind's
  `stl/shared_ptr.h` is NOT in the vendored slice.
  - **Option A (locked-`shared_ptr`, clean):** change the `Vector` binding to a
    `shared_ptr<VectorOwner>` holder. Requires VENDORING `nanobind/stl/shared_ptr.h`
    (§4 dependency — needs architect agreement) + refactoring 8 `nb::cast(std::move
    (owner))` + 5 `inst_ptr<VectorOwner>` sites + the `nb::class_` decl +
    `draken_vector_unwrap`. Zero-copy bidirectional seam; enables fan-out. Highest-risk
    change in the initiative (every Vector flows through it) — gate q/tpch/clickbench.
  - **Option B (no new dep, interim):** keep the inline-`VectorOwner` Vector binding.
    `CxxMorsel` holds `shared_ptr<VectorOwner>`; `cxx_from_pymorsel` builds it via an
    aliasing shared_ptr with a Py-keep-alive deleter (zero-copy in); the boundary
    handoff out uses the existing `draken_vector_own_*` ownership-transfer (works for
    unique owners at a segment END boundary — fine for contiguous scan→filter→agg).
    No fan-out zero-copy (joins/exchange would copy until Option A lands). Lower risk,
    unblocks S1/S2, defers the holder change.
- **S1 — Scan emits `CxxMorsel`** (GIL released across assembly), adapter at the
  chain head.
- **S2 — Grouped-aggregate-hashed → C++ nogil** (M4 breaker / bottleneck).
- **S3 — Filter / project → C++ nogil** (unblocked by P9).
- **S4..Sk — joins, sort, distinct, window, set-ops, limit, exchange → C++ nogil**,
  one operator per increment behind the seam.
- **SN — Boundary-only shim.** `drive_scan` + push pipeline run nogil over
  `CxxMorsel` end-to-end; `Vector`/`Morsel` PyObjects built only at the cursor.
- **M4 re-measure.** With a fully nogil scan→filter→grouped-agg segment, re-run
  the M4 parallel scheduler on the high-card ClickBench regressors to confirm the
  pipeline thesis now beats the GIL-bound 0.98× baseline.

Open items to surface as they arise (not blocking the sequence): per-kernel
real/stub gate design for P9; whether `exchange.py` (currently pure Python) is
rewritten as part of S4..Sk or earlier; `shared_ptr` atomicity cost under M4
concurrent fan-out vs an intrusive refcount.
