# C++-First Morsel / Vector — Investigation, Landed State & Remaining Design

Status: **substrate + boundary LANDED and green (uncommitted); GIL release is the
remaining third.** Gates at time of writing: `make q` 190 / `make tpch` 22 /
`make clickbench` 43, all green.
**⚠️ Known dead-end to unwind:** the morsel that flows between operators is still a
`cdef class Morsel` PyObject (with a cached C++ read pointer). That is the
Q2-forbidden half-measure and it structurally **cannot** go nogil — the fix (the
carrier becomes `shared_ptr<CxxMorsel>` through `cdef nogil` operator methods) is
the load-bearing remaining step, S-B (§D.1).
Date: 2026-06-16 (investigation) → 2026-06-17 (landed-state update).

Goal: make the morsel/vector that flows through the execution engine a genuine
C++ object, with the Python `Morsel`/`Vector` reduced to a real boundary-only
shim — so the operator chain and per-morsel assembly run **nogil** (GIL released
at the scan edge, the whole pipeline below Python-free). This is the actual
enabler for M4: under the GIL, concurrent pipeline segments serialize, so M4
cannot deliver a CPU-bound speedup until the operator surface is nogil.

**Reading guide:** Part A is the original finding (unchanged — still accurate).
Parts B–C describe what was *built* this initiative. Part D is the **remaining**
work (the GIL release). Part E is composition with P9.1 + M4. Part F is the open
decisions. Where the implementation **diverged from the originally-locked
decisions**, it is called out explicitly (§B.1, §D.0).

---

## Part A — Reconciling the discrepancy (the finding)

**The architect believed C++-first morsel/operator work was done. It was never
scoped.** Not a regression, not a revert — the morsel-container / operator-chain
C++-first layer did not exist in any design doc or in code.

### A.1 What WAS C++-first (real, pre-initiative)
1. **Column DATA is C++-owned, freed off-GIL.** Bytes live in a C++ `VectorOwner`
   (now `draken/core/vector_owner.h`) holding `OwnedBuffer<T>` over `draken_malloc`
   blocks; payload is the 40-byte POD `DrakenVector` (`draken/core/buffers.h`,
   frozen ABI). RAII free, no GIL.
2. **A nogil C-ABI kernel surface exists** (Phase-9): `draken/ops/` kernels
   take/return POD `DrakenVector`/`VecResult`, declared `nogil`, registered in
   `kernel_registry.cpp`.

### A.2 What was NOT (the surprise)
- `Vector` and `Morsel` were **PyObjects** (Cython cdef classes). `Morsel` held a
  `std::vector<PyObject*>` of borrowed `Vector` PyObjects — a GIL micro-opt, not a
  C++ store.
- **The operator chain held the GIL end-to-end** (`_operators.pyx`: `push` /
  `_dispatch_push` / `next_morsel` all take/return PyObject `Morsel`, none nogil).
- **No design doc ever specified a C++ morsel.** The "shim" name (design doc
  `E24_cython_vector_shim`) was accurate to intent — a deliberately-thin Cython
  wrapper — but stayed *load-bearing*. The architect conflated "draken is
  C++-first" (true: vector lib + kernels) with "the execution morsel/operator
  chain is C++-first/nogil" (never built).

### A.3 Phase-9 executor flip — state at investigation time
The nogil C-kernel ABI was wired (`BytecodeInstr.kernel_fn`/`ctx_ptr` +
`BC_INSTR_C_NATIVE`), but the executor only honoured it for **BC_CAST**;
BC_BINARY_OP/FUNCTION/EXTRACTION reverted to the Python closure. (This has since
advanced — see §C.3.)

---

## Part B — The C++ morsel type (LANDED)

```cpp
// draken/morsels/cxx_morsel.h  (BUILT)
struct CxxColumn {
    DrakenVector view;                 // 40-byte POD; data[selection[i]] access
    std::shared_ptr<VectorOwner> own;  // keeps bytes alive; RAII off-GIL free
};
struct CxxMorsel {
    std::vector<CxxColumn>   columns;  // owns columns; move-only
    std::vector<std::string> names;    // bytes, not PyObject
    uint32_t                 zero_col_rows = 0;
};
```
- No PyObject in the struct; columns shared via `shared_ptr<VectorOwner>` (sharing
  is needed so joins/exchange fan one column into many outputs without copying).
- C++ morsel-op surface exists: `cxx_select`/`cxx_take`/`cxx_slice`/`cxx_mask`/
  `cxx_combine`/`cxx_hash` (some in `cxx_morsel_ops.h`, some in `draken_native.cpp`
  reusing `vector_take_impl`/`vector_mask_impl`/`concat_owners`).

### B.1 ⚠️ Carrier is a Q2-violating dead-end (to be unwound in S-B)
The locked Q2 was "**pure** `CxxMorsel` carrier; the Python `Vector`/`Morsel`
PyObject constructed ONLY at the cursor; no interim half-measure." **What landed is
that exact forbidden half-measure** — not a defensible divergence. State it plainly.

What landed (the hybrid):
- The Cython `Morsel` cdef class **is still the operator-chain currency** — passed
  between operators by `push(self, Morsel)` / `next_morsel(self) -> Morsel`. It no
  longer holds PyObject columns; it carries `cdef public object _cxx` (a nanobind
  handle to the C++ `CxxMorsel`), `cdef const CxxMorsel* _cxx_ptr` (a cached raw C++
  pointer into it, via `extern "C" cxx_morsel_raw_ptr`), and `cdef list _col_names`.
- The column **read** path is nogil-capable: `cdef const DrakenVector* _col_view(i)
  noexcept nogil { return &_cxx_ptr.columns[i].view; }`.

**Why this is a dead end, not a stepping stone:** a `cdef class Morsel` is a
PyObject, so `push(self, Morsel morsel)` takes a **refcounted PyObject** — Cython
**cannot** mark such a method `nogil` (PyObject params are forbidden in nogil
functions). Therefore, while the chain currency is a PyObject `Morsel`,
`push`/`next_morsel`/`_dispatch_push` can **never** be nogil, and the operator chain
can never release the GIL — no matter how nogil-capable the column reads are. The
doc previously rationalised this as "costs nothing while the chain is gil-held";
that is circular — **the PyObject container is part of *why* the chain is gil-held.**
The cached `_cxx_ptr`/`_col_view` read logic is salvageable (it already reads
`_cxx_ptr.columns[i].view` with no PyObject — it ports directly onto a `CxxMorsel*`);
the `cdef class Morsel`-as-chain-currency is what must be deleted.

**What Q2 actually requires (the target):** the morsel flowing between operators is
a **C++ value — `shared_ptr<CxxMorsel>`** (or a moved `CxxMorsel`) — held in Cython
as a real C++ type (`cdef shared_ptr[CxxMorsel]`, declared from `cxx_morsel.h` via
the pxd). Operator core methods become `cdef … nogil` over that C++ type; the
pull/push loop in `drive_scan` runs `with nogil`; the transform ops are C-level over
`CxxMorsel` returning `shared_ptr<CxxMorsel>` (no nanobind per op). The Python
`Morsel`/`Vector` PyObjects are constructed **only at the cursor**. That — the
carrier ceasing to be a PyObject — is what makes the shim a genuine shim.

---

## Part C — What was built (the transition, as executed)

The conversion was done behind a **fail-loud seam**, one operator family at a time,
suite green at every step. Verified landed:

### C.1 Substrate flows through the whole chain (DONE)
- **Scan emits `CxxMorsel` unconditionally** (no flag, no fallback) —
  `parquet_read.pyx` `from_cxx_vectors`.
- **Cursor is the SOLE materialization point** — `query_session.execute_to_morsels`
  calls `item.materialize()`; this is the only sanctioned PyObject build.
- **Engine-internal PyObject accessors FAIL LOUD** — `_ensure_pyobject` raises if a
  Cxx-backed morsel reaches a non-converted path. (This is what keeps "green ==
  real": an unconverted operator screams instead of silently materializing.)
- **All operators read/emit the substrate**: filter, projection VM, sort (both
  heap_sort top-N and full morsel_ops/sort), grouped-aggregate-hashed
  (factory/engine/node/key_store + collectors), distinct, inner-join, outer-join
  (via null_filter), filter-join (semi/anti), cross-join, window, set-ops. Result
  morsels preserve the input representation (Cxx stays Cxx).
- Shared Morsel accessors made Cxx-aware (the seam): `_get_column`/`_col_view`/
  `_cxx_column`/`_columns_to_pointers`/`_resolve_columns_to_indices`/`hash`/
  `hash_keys`/`_column_index_from_name`/`_ensure_name_map` (reads), and
  `_take_inplace`/`_empty_inplace`/`filter_mask`/`combine`/`align_tables`/`copy`/
  `append_vector`/`select`/`take`/`slice`/`rename` (mutators/emitters, Cxx-native).
- **→ "What done means" criteria 1 (C++ morsel threaded through) and 3 (Python
  shim only at boundary) are MET.** Criterion 2 (every operator nogil) is PARTIAL
  (Part D).

### C.2 Perf note (a regression found + fixed)
The first carrier version read columns *by name through the nanobind edge*
(`self._cxx.names()` allocating a Python list **per column access**) — a §3
violation in the hot path that degraded ClickBench. Fixed by the cached
`_cxx_ptr` + `_col_view` + cached `_col_names` (§B.1). Result: back within noise of
best-ever (94.88s vs 92.75s best). **Expected: at 1 worker with the GIL still held,
the substrate is roughly neutral** — its dividend is nogil parallelism, not yet
collected. Coming back *neutral* (not faster) at W=1 is the correct signal.

### C.3 Phase-9 executor flip — advanced (P9.1, partial)
The expression VM's binary-op path is being migrated from the Python closure to
the single C kernel `draken_binop` (`draken/ops/kernels/binop_dispatch.cpp`),
registered and dispatched via `BC_INSTR_C_NATIVE` in `evaluation.pyx`'s
`BC_BINARY_OP` branch, gated by the `_c_native_binop` allow-list (single source of
truth, deterministic + fail-loud, no silent fallback). **Routed C-native today:**
int/float arithmetic + true-divide, integer bitwise, DECIMAL×DECIMAL /
DECIMAL128×DECIMAL128, string concat (`||`), IP-in-CIDR. **Still on the Python
closure:** temporal (`date/ts ± interval`, `interval ± interval` — arithmetic just
restored, see [[temporal_arith_restored]]), decimal×integer / cross-kind decimal.
Canonical binop kernel = `draken_binop` (architect; the old 9a per-op `draken_add`
and the live `draken_arithmetic_dv` are to be retired at the end).

---

## Part D — Remaining work: release the GIL (NOT done)

This is the third that delivers the M4 speedup. **Verified not-done:** `grep "with
nogil" opteryx/operators/_operators.pyx` → nothing; `push`/`_dispatch_push`/
`next_morsel`/`push_left`/`push_right` are all gil-holding `cpdef`/`cdef`.

### D.0 Two blockers to putting `with nogil` on an operator body
1. **Per-morsel transform ops still cross nanobind.** `self._cxx.take(...)`,
   `.slice/.select/.rename`, `cxx_morsel_from_vectors(...)`, `filter_mask`,
   `combine`, `align_tables` are one GIL-bound nanobind call per morsel-op. The
   *impls* are pure C++ (`vector_take_impl`/`vector_mask_impl`/`vector_slice_impl`/
   `concat_owners` in `draken_native.cpp`) — they just need to be reachable from
   Cython at C level (header-ize, or C-ABI export) returning `shared_ptr<CxxMorsel>`.
2. **The expression VM still has the Python closure** for the non-C-native binop
   families (temporal, decimal-mixed). A `with nogil` filter/projection body cannot
   call a Python closure. → finish §C.3.

### D.1 Stages (each gates q190/tpch22/cb43, behind the fail-loud seam)
- **S-A — close the binop closure (finishes §C.3).** Port temporal + decimal
  loose-ends through `draken_binop`; then delete `resolve_binary_op` + the executor
  fast-path branch + `draken_arithmetic_dv` + the dead 9a per-op kernels. Outcome:
  expression VM fully nogil-capable. Gate-verifiable; on the critical path for S-C's
  filter/project.
- **S-B — change the carrier type (the load-bearing step; unwinds §B.1).** Replace
  the `cdef class Morsel` chain currency with a C++ value: `shared_ptr<CxxMorsel>`
  threaded through `cdef … nogil` operator methods (`push`/`next_morsel`/
  `_dispatch_push` change signature from `Morsel` to `shared_ptr[CxxMorsel]`), and
  `drive_scan`'s pull/push loop runs `with nogil`. This *requires* the transform ops
  (take/mask/combine/slice/select/hash/align) to be **C-level functions over
  `CxxMorsel` returning `shared_ptr<CxxMorsel>`** — header-ize/export the pure-C++
  impls (`vector_take_impl`/`vector_mask_impl`/`vector_slice_impl`/`concat_owners`)
  so they are reachable from Cython with no nanobind per op (§D.0 blocker #1). The
  `_col_view` read logic ports directly onto `CxxMorsel*`. The Python `Morsel`/
  `Vector` are built only at the cursor (and the scan builds `CxxMorsel` natively).
  After S-B the operator chain is structurally nogil-capable; this is the step that
  was blocked by the §B.1 dead-end. **Detailed implementation plan:
  `docs/M4_S_B_CARRIER_FLIP_PLAN.md`** (atomic signature flip + gil-wrapped bodies →
  incremental nogil; `MorselState` EOS enum; C++-exception error path via `except +`;
  sub-steps S-B.0..S-B.5; recommended 2-operator de-risk spike).
- **S-C — flip ONE hot operator body to `with nogil` end-to-end** = grouped-agg
  (the M4 breaker; its core hash→probe→accumulate is already kernel-backed and does
  NOT run the VM). Measure at M4 `MAX_EXECUTION_WORKERS > 1`. **First hard evidence
  the whole initiative pays off** (or that it doesn't).
- **S-D — roll `with nogil` across the remaining operators** (filter/project gated
  on S-A; joins/sort/distinct/window/setops are kernel-backed). When the full
  scan→…→exit chain is nogil, `drive_scan`/push run nogil over the substrate and the
  `Morsel`/`Vector` PyObjects are built only at the cursor — the container becomes a
  true shim (closes the §B.1 divergence).

---

## Part E — Composition with P9.1 and M4
- **P9.1 (binop C kernel, §C.3) is the prerequisite for S-A/S-C's filter/project**:
  a `with nogil` VM cannot call the Python closure.
- **M4 parallel scheduler:** this nogil operator surface is the *enabler*. The M4
  finding ([[m4_parallel_group_agg_built]]) that round-robin (0.94×) and
  hash-partition (0.62–0.72×) both LOSE on ClickBench is precisely because
  concurrent operators serialize on the GIL (+ recombination cost). nogil bodies
  (S-C) are what let M4's worker clones run truly concurrently — S-C's measurement
  is the go/no-go for the whole M4 parallel direction.

---

## Part F — Open decisions for the architect
1. **Next track:** S-A (finish binop closure, gate-verifiable, no speedup yet) vs
   S-B+S-C (prove the nogil dividend on grouped-agg, higher risk, highest
   information). *(This doc is the consolidated artifact requested before that
   choice.)*
2. **Carrier for S-B:** keep the hybrid (nanobind handle + cached raw ptr) and
   export the transform impls as C-ABI, OR move to a pure `shared_ptr[CxxMorsel]`
   Cython carrier (Option A — would also vendor `nanobind/stl/shared_ptr.h`). The
   transforms-nogil requirement may force this.
3. **Originally-locked Q3 ("Phase-9 FIRST") was reversed in practice** — the
   substrate flip landed first (operators read via `_col_view`; the VM under the
   GIL is fine while the whole body is gil-held). P9.1 proceeds incrementally. This
   reversal was correct (substrate flip had no P9 dependency); flagged for the
   record.
4. **Q2 carrier — REJECTED the hybrid (architect, 2026-06-17).** The landed
   `cdef class Morsel`-as-chain-currency is the Q2-forbidden half-measure and a
   structural dead-end (§B.1): a PyObject carrier can never flow nogil. The fix is
   the carrier-type change to `shared_ptr<CxxMorsel>` through `cdef nogil` operator
   methods — folded into **S-B as the load-bearing step**. Decision needed only on
   timing (S-B is now the gating piece for any nogil work, so it precedes S-C's
   measurement). Open sub-question: whether S-B also forces the Option-A `Vector`
   holder change (`shared_ptr<VectorOwner>` binding + vendoring
   `nanobind/stl/shared_ptr.h`) or the existing aliasing-shared_ptr seam suffices at
   the cursor boundary.
