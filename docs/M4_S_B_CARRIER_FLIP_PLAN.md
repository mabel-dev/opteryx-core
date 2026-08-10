# S-B — Carrier Flip: `cdef class Morsel` → `shared_ptr<CxxMorsel>` (implementation plan)

Status: **S-B.1 LANDED (2026-06-17) — atomic carrier flip complete, all bodies
gil-wrapped, GIL still held inside every body; S-B.2+ (true-nogil bodies) next.**

> **⭐ S-B.1 LANDED (2026-06-17, architect chose the literal flip).** The chain
> currency is now `shared_ptr[CxxMorsel]` (the Q2 fix). `push`/`_dispatch_push`/
> `_emit_cdef`/`push_left`/`push_right` are `cdef int … (shared_ptr[CxxMorsel],
> ErrCtx*) noexcept nogil`; `next_morsel` returns `shared_ptr[CxxMorsel]` (NULL =
> exhausted, still gil-held); `push_cxx` retired (absorbed into `push`).
> **Key Cython constraint discovered:** a `noexcept nogil` cdef function may NOT
> hold named Python-object locals (they need the GIL for cleanup on exit) — so the
> transitional gil-wrapped body lives in a SEPARATE gil-held helper, and the nogil
> method only decodes the carrier + calls it with no named local:
>   - single-input cdef operators put the body in `cpdef _push_impl(self, Morsel)`
>     (the base `_dispatch_push` default decodes + dispatches);
>   - joins keep a minimal nogil `push_left`/`push_right` wrapper + a
>     `cdef void _push_<side>_gil(self, Morsel) except *` helper.
> **Error model:** the shared `PipelineContext._exc` stashes the first body
> exception (per-node `_cxx_push_exc` fallback when no ctx); the driver
> (`drive_scan` / the new `push_one`/`push_left_one`/`push_right_one` Python
> drivers used by `parallel_engine` + unit tests) re-raises once at the gil
> boundary. **Cursor = ExitNode** (sole Python-Morsel build via `cxx_to_morsel`).
> Gates: `make q` 190/190, TPC-H results 7/7, ClickBench 43/43, touched unit tests
> green. Carrier round-trip provably preserves int+float NULLs (GROUP BY/DISTINCT).
Parent: `docs/M4_CPP_MORSEL_DESIGN.md` (§B.1 dead-end, §D.1 S-B). This is the
load-bearing step that unwinds the Q2-violating hybrid carrier and makes the
operator chain structurally nogil-capable. It touches **every operator** and the
**morsel ABI**, so it is staged to keep `make q`/`tpch`/`clickbench` green at every
sub-step.

> **⭐ SPIKE FINDING (2026-06-17) — error model is STATUS CODES, not `except +`.**
> The de-risk spike (`scratch/sb_nogil_spike.pyx`, throwaway) validated the nogil
> mechanics — virtual-vtable `cdef` dispatch under `with nogil`, a
> `shared_ptr<CxxMorsel>` carrier, and a `MorselState` EOS flag all work — but it
> **disproved the intended `except +` exception path**: Cython only permits `except
> +` on `extern` functions, so a C++ exception thrown inside a `cdef` *class* method
> leaks as a `SystemError`. **Decision:** operator nogil methods are
> `cdef int push(self, shared_ptr[CxxMorsel] m, ErrCtx* err) noexcept nogil` returning
> a **status code**; `drive_scan` checks it each call and raises the Python exception
> **once at the gil boundary** (a per-pipeline `ErrCtx{code,msg}` carries detail). Real
> C++ exceptions only become available if operator *bodies* become pure C++ (Q4);
> until then, status codes. Detail in §6.3.

---

## 1. Goal / end-state
The value flowing **between operators** is a C++ value — `shared_ptr[CxxMorsel]`
held in Cython as a real C++ type — not a `cdef class Morsel` PyObject. Operator
core methods are `cdef … nogil` over that type; `drive_scan`'s pull/push loop runs
`with nogil`; the Python `Morsel`/`Vector` are built **only at the cursor**. The
`cdef class Morsel` is deleted from the chain (it survives only as the boundary
shim built at `execute_to_morsels`).

## 2. Chain mechanics today (grounded, `_operators.pyx`)
- `cdef class BasePlanNode` with `_downstream` link.
- Push side: `cpdef void push(self, Morsel) except *` → `cdef void
  _dispatch_push(self, Morsel)` (operator body) → `cdef void _emit_cdef(self,
  Morsel)` → `self._downstream.push(morsel)`.
- Source side: `cdef Morsel next_morsel(self) except *` (scan).
- `drive_scan(scan, chain_head, …)`: loops `chain_head.push(morsel)`, terminates
  with `_EOS_SENTINEL` (a `Morsel`).
- Joins: `push_left` / `push_right`.
- Currency is the `Morsel` PyObject throughout; **none of these are `nogil`** (the
  blocker: a PyObject param forbids `nogil`).

## 3. End-state surface (what each becomes)
- Currency: `shared_ptr[CxxMorsel]` (Cython `cdef shared_ptr[CxxMorsel]`, declared
  from `cxx_morsel.h` via `cxx_morsel.pxd`).
- The **same** methods change signature (no parallel `_cxx` surface — one surface):
  `cdef int push(self, shared_ptr[CxxMorsel], ErrCtx*) noexcept nogil` /
  `_dispatch_push` / `_emit_cdef` (→ `self._downstream.push(m, err)`) / `next_morsel`
  (→ `shared_ptr[CxxMorsel]`) / `push_left` / `push_right`. Bodies start `with gil:`
  (S-B.1) and convert to true-nogil one at a time. (Signature shape validated by the
  spike — see §6.3.)
- EOS: a `MorselState` enum on `CxxMorsel` (`DATA`, `END_OF_STREAM`; extensible) —
  EOS is a valid morsel carrying a flag, representable nogil. (Decision §6.2.)
- Errors: status-code return + a per-pipeline `ErrCtx{code,msg}` set nogil, raised as
  a Python exception **once at the gil boundary** (`drive_scan`/cursor). NOT `except
  +` — that fails on cdef class methods. (Decision §6.3, corrected by the spike.)
- Cursor: `execute_to_morsels` builds the Python `Morsel` from the final
  `shared_ptr[CxxMorsel]` (the one sanctioned PyObject build).

## 3b. Boundary/carrier mechanics — bridges BUILT + VALIDATED (S-B.1a, 2026-06-17)
**✅ S-B.1a DONE.** The bridges the flip depends on are built and round-trip
byte-identical; the flip is now de-risked. Landed (uncommitted, q190/tpch22/cb43):
- C-ABI in draken_native.cpp: `cxx_morsel_shallow_copy` (heap CxxMorsel sharing
  column owners), `cxx_morsel_to_handle` (shallow copy → NEW-ref nanobind handle),
  reusing `cxx_morsel_raw_ptr`/`cxx_morsel_delete`.
- Cython bridges in `_morsel_shim.pyx`: `cdef shared_ptr[CxxMorsel] morsel_to_cxx(Morsel)`
  and `cdef Morsel cxx_to_morsel(shared_ptr[CxxMorsel])`. (`from libcpp.memory cimport
  shared_ptr`.) Plus a THROWAWAY `cpdef _sb1a_roundtrip` validation hook — delete when
  S-B.1b wires the bridges.
- VALIDATED: int + arena-backed string columns round-trip byte-identical; TRIPLE
  round-trip holds (shallow-copy owner-sharing keeps the bytes/arena alive); the
  original is intact after round-trip (independent ownership). Behaviour-neutral
  (only the throwaway hook calls them) → all gates green.

The original probe finding (kept for the record): the flip is **gated on these
bridges**; "wrap each body in `with gil:`" is not a one-liner. The remaining design
notes for S-B.1b:
- **Chain owns the morsel.** Currency = `shared_ptr<CxxMorsel>` owning a **heap**
  `CxxMorsel` (scan builds it; the C-ABI transforms `cxx_*_c` return new heap
  `CxxMorsel*` — already started). The chain is then pure libcpp/nogil; **nanobind is
  NOT in the chain** — only at scan-emit and the cursor.
- **Bridges needed (don't exist):**
  (1) `morsel_to_cxx(Morsel) -> shared_ptr[CxxMorsel]` — build a heap `CxxMorsel`
      from the Morsel's columns/`_cxx`.
  (2) `cxx_to_morsel(shared_ptr[CxxMorsel]) -> Morsel` — wrap a `CxxMorsel` as a
      Python Morsel (cursor + transition). For a *transient* (non-consumed) morsel
      this needs a cheap **shallow copy** (`CxxMorsel` is move-only; copy the
      `columns` vector — the per-column `shared_ptr<VectorOwner>` are shared, bytes
      are not).
  (3) scan-emit C bridge (owners → heap `CxxMorsel`) and the cursor C/nanobind bridge
      (`CxxMorsel` → Python `Vector`s).
- **Body↔emit interleaving:** a gil-wrapped body runs on a `Morsel` but emits into a
  `shared_ptr` chain via `_emit_cdef` → `downstream.push`; the conversion must sit at
  that emit point, not just around the whole body.
- **Plan:** land + round-trip-validate the bridges (byte-identical
  `Morsel ⇄ shared_ptr[CxxMorsel]`) as S-B.1a, THEN the signature flip (S-B.1b).

## 4. How it stays green — atomic signature flip, then incremental nogil bodies
(Revised after architect review — the dual-path seam was over-engineering: the hot
chain is all `cdef class …Node(BasePlanNode)`, so there is ONE surface to flip, not
two to bridge. Prerequisite: the §3b bridges.)

There is exactly **one** push/next surface; it changes type in a single mechanical
pass, then bodies convert to nogil one operator at a time.

- **S-B.2 (signature flip, green after one pass):** change `push`/`_dispatch_push`/
  `_emit_cdef`/`next_morsel`/`push_left`/`push_right` on `BasePlanNode` + every
  `cdef` operator from `Morsel` to `shared_ptr[CxxMorsel]`, and `drive_scan` to loop
  over it. **Every body is wrapped `with gil:` around its existing logic**, with
  `morsel_to_cxx`/`cxx_to_morsel` at body entry/exit. Behaviour is identical, so the
  suite is green the moment it compiles — "red" is only the duration of the edit
  (hours), not a sustained state. **The carrier is now a C++ value (the Q2 fix)**,
  every body still visibly gil-wrapped.
- **S-B.3+ (incremental nogil bodies, green per operator):** drop the `with gil:`
  from one operator at a time, rewriting its body over `shared_ptr[CxxMorsel]` using
  the S-B.0 C-level transforms/reads. A converted operator's body is true-nogil; the
  `morsel_to_cxx`/`cxx_to_morsel` adapters disappear from it. Order: scan →
  grouped-agg → exit (the M4 segment), then the rest.
- **Cold Python-class operators** (Explain, FunctionDataset — not `cdef`, not on the
  hot path) keep a thin `Morsel`-side adapter at their edge; they are the only place
  a `cxx↔Morsel` bridge persists, and they never need to go nogil.
- **No permanent fallback.** The `with gil:` wrap is visible and is removed
  per-operator; once removed, the operator is nogil and there is no gil path to
  regress to. `morsel_to_cxx`/`cxx_to_morsel` survive only at the cursor and the cold
  Python operators.

Adapters: `cdef shared_ptr[CxxMorsel] morsel_to_cxx(Morsel)` (lifts the existing
`_cxx`), `cdef Morsel cxx_to_morsel(shared_ptr[CxxMorsel])` (the existing
`Morsel.from_cxx`). Used inside gil-wrapped bodies during transition, at the cursor,
and at cold Python operators — nowhere in a converted nogil body.

## 5. Sub-steps (each gates q190/tpch22/cb43; header/draken changes use `make compile`)
- **S-B.0 — C-level surface + morsel state enum (no carrier change, behaviour-neutral).**
  (a) Header-ize/C-ABI-export the pure-C++ transform impls (`vector_take_impl`/
  `vector_mask_impl`/`vector_slice_impl`/`concat_owners`) so the morsel `.so` calls
  them at C level; `cxx_morsel.pxd` declares `shared_ptr[CxxMorsel]` + nogil free
  functions (`cxx_take`/`cxx_mask`/`cxx_slice`/`cxx_combine`/`cxx_select`/`cxx_hash`
  → `shared_ptr[CxxMorsel]`) + read accessors. (b) Add a `MorselState`
  enum to `CxxMorsel` (`DATA`, `END_OF_STREAM`; extensible) — EOS becomes a valid
  morsel carrying a flag, representable nogil (replaces `_EOS_SENTINEL`-as-Morsel).
  Nothing calls the new surface yet. Gate: build + suite unaffected.
- **S-B.1 — Atomic signature flip + status error path (green after one mechanical pass).**
  Change `push`/`_dispatch_push`/`_emit_cdef`/`next_morsel`/`push_left`/`push_right`
  on `BasePlanNode` + every `cdef` operator from `Morsel` to `shared_ptr[CxxMorsel]`
  (`cdef int … (…, ErrCtx*) noexcept nogil` status-code return), and `drive_scan` to
  loop over it (EOS via the `MorselState` enum; status checked each call, raised once
  at the boundary). **Every body is wrapped `with gil:` around its existing logic**
  (entry/exit via `morsel_to_cxx`/`cxx_to_morsel`; a body that errors sets `err`,
  returns non-OK). Behaviour identical → green once it compiles. Cold Python-class
  operators (Explain, FunctionDataset) get a thin `Morsel`-side adapter at their edge.
  **Carrier is now a C++ value — the Q2 fix lands here**, every body still visibly
  gil-wrapped. *De-risk DONE: `scratch/sb_nogil_spike.pyx` validated nogil-vtable
  dispatch + `shared_ptr` carrier + `MorselState` EOS + status-to-boundary raise, and
  corrected the error path from `except +` to status codes (§6.3).*
- **S-B.2 — Convert the scan body to nogil.** `next_morsel` returns the
  natively-built `CxxMorsel` with no gil re-acquire (the scan already assembles it).
  Gate.
- **S-B.3 — Convert grouped-aggregate-hashed + exit to nogil** (the M4 segment;
  grouped-agg is kernel-backed and does NOT run the expression VM → no S-A
  dependency). Drop their `with gil:`; rewrite over `shared_ptr[CxxMorsel]` via the
  S-B.0 surface; a body that errors sets `err` and returns non-OK (status path, §6.3).
  Gate after each.
- **S-B.4 — Measure (= S-C).** scan→grouped-agg→exit nogil at M4
  `MAX_EXECUTION_WORKERS > 1` — first hard go/no-go on the M4 thesis.
- **S-B.5 — Convert the rest.** filter/project (after **S-A** closes the binop
  closure — they run the VM), joins/sort/distinct/window/set-ops (kernel-backed).
  Drop each `with gil:`. When the full chain is nogil, `Morsel`/`Vector` are built
  **only at the cursor** (cold-operator adapters the only other crossing); the
  `cdef class Morsel`-as-chain-currency is gone.

## 6. Decisions (resolved with architect 2026-06-17)
1. **Transition strategy — atomic signature flip, NOT a dual-path seam.** The hot
   chain is all `cdef` classes, so there is one surface; the flip (gil-wrapped bodies)
   is green after a single mechanical pass — "red" is only the duration of the edit,
   not a sustained state. The earlier dual-path proposal was over-engineering;
   dropped.
2. **EOS — a `MorselState` enum on `CxxMorsel`** (not a bare bool / null shared_ptr),
   EOS = `END_OF_STREAM`, room for future stream states (S-B.0).
3. **Exceptions — status-code return (CORRECTED by the S-B.1 spike).** The intended
   answer was C++ exceptions via `cdef … except +`, but the spike
   (`scratch/sb_nogil_spike.pyx`) proved **`except +` does NOT work on cdef *class*
   methods** — Cython: "only extern functions can throw C++ exceptions"; a C++
   exception thrown in a `cdef void … nogil` body leaks as `SystemError`. **Validated
   working pattern:** operator methods are `cdef int push(self, shared_ptr[CxxMorsel]
   m, ErrCtx* err) noexcept nogil` returning a **status code**, with a per-pipeline
   `ErrCtx{code,msg}` set nogil on failure; `drive_scan` checks the status nogil,
   breaks, and raises the Python exception **once at the gil boundary**. (This is the
   C-application status pattern — forced because Cython cdef-class methods can't
   propagate C++ exceptions. If/when operator bodies move to **pure C++ (Q4)**, they
   can throw real C++ exceptions caught at the boundary; until then, status codes.)
   Spike confirmed: nogil virtual-vtable dispatch + `shared_ptr<CxxMorsel>` carrier +
   `MorselState` EOS flag + status-to-boundary raise all work cleanly.
4. **Sequencing — settled (no decision):** grouped-agg first (no VM dependency, the M4
   breaker); filter/project after S-A.
5. **Open (small) — Option-A `Vector` holder:** confirm at S-B.5 whether the cursor's
   `cxx_to_morsel` needs the `shared_ptr<VectorOwner>` `Vector` binding (vendoring
   `nanobind/stl/shared_ptr.h`) or the existing aliasing-shared_ptr seam suffices
   (cursor is gil-held → likely suffices, Option A not forced).

## 7. Status / next
**Mechanics spike DONE** (`scratch/sb_nogil_spike.pyx`, throwaway): nogil
virtual-vtable dispatch + `shared_ptr<CxxMorsel>` carrier + `MorselState` EOS flag +
status-code error path all validated; corrected the error model from `except +` to
status codes (§6.3). **Next: S-B.0** (C-level transform surface + `MorselState` enum,
behaviour-neutral) → full **S-B.1** (atomic signature flip, gil-wrapped bodies) →
S-B.2/S-B.3 (scan + grouped-agg nogil) → measure (S-B.4).
