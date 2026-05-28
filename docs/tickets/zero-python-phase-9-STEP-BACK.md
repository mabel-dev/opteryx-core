# Zero-Python Phase 9 — STEP-BACK ASSESSMENT

> Architect decision (2026-05-28): **pause Phase 9, step back.** After
> eight corrective rounds on the C-kernel-ABI work, the binop C path is
> thrashing — each fix introduces a regression elsewhere, and the
> standard gate (`make q`) is blind to all of it. This document is the
> honest state-of-play + root-cause diagnosis to inform whether/how to
> resume. No implementation ticket is issued.

## 1. Current state (verified 2026-05-28, working tree, uncommitted)

Per query class, fresh process each:

| Query class                         | State            |
|-------------------------------------|------------------|
| constant fold (`1+1`)               | OK `[2]`         |
| arithmetic non-null (`id+1`)        | OK (DV fast path)|
| all-null arithmetic (`NULL+1`)      | OK `[None]`      |
| **bitwise (`id \| 2`)**             | **CRASH (SIGBUS)** — regressed (worked in 9c) |
| **string concat (`name \|\| '!'`)** | **CRASH (SIGBUS)** — regressed (worked in 9c) |
| **partial-null arithmetic**         | **CRASH (SIGBUS)** — per-row validity merge never implemented |
| cast (`CAST(id AS VARCHAR)`)        | OK `['1','2']`   |
| extraction (`missions[0]`)          | OK               |
| CASE                                | OK               |
| `make q`                            | 137/137 (blind to all three crashes) |

**The working tree currently contains three live SIGBUS regressions on
the binop C path.** Nothing has been committed across the entire
Phase 1–9 train, so the tree is the cumulative state. A decision is
needed on whether to revert 9c-completion-2's binop changes (restoring
bitwise/concat) or carry forward and fix.

## 2. What actually got delivered (Phases 1–9)

Genuinely complete and verified (value-checked):
- **Phases 1–8c**: the full Python-removal train through the executor
  rewrite + tree-walker deletion. Solid; `make q` green; these changed
  real behaviour and were value-checked at the time.
- **Phase 9a**: C kernel ABI — 48/48 kernels parity-tested green
  (`make kernel-parity`). Mechanism proven. (Took 4 rounds; the parity
  test only passed once it actually ran.)
- **Phase 9b**: `BytecodeInstr` carries `kernel_fn`/`ctx_ptr` +
  `BC_INSTR_C_NATIVE`; bind-time resolution works (fail-fast on
  supported-combo miss). (Took 2 rounds; the cast key map was wrong
  the first time.)
- **Phase 9c (partial)**: C-native dispatch live for CAST, BINARY_OP
  (non-null), EXTRACTION, CASE. Correct for the happy path.

Not delivered / broken:
- **Null + bitwise + concat on the binop C path** — thrashing (see §1).
- **Per-row validity merge** — flagged 5× (9a TODO → 9c → 9c-completion
  → 9c-completion-2), never implemented.
- **9d/9e/9f** — not started (Morsel nogil, nogil annotation, cleanup).
- **9a-fn** — function kernels not started (BC_FUNCTION still Python;
  correct for now, but blocks 9e).

## 3. Root-cause diagnosis — why 8 rounds thrashed

### 3.1 No value-checked gate (the dominant cause)

`make q` is **shape-only** (137 queries asserting row/column counts,
not values). Across this whole effort it stayed green through **at
least six distinct correctness bugs**: COUNT(*)-WHERE→0,
assemble_fixed no-ELSE segfault, and the Phase-9 ctx_ptr / null-
arithmetic / bitwise / concat crashes. It has **zero coverage** for:
null propagation, bitwise, string concat, extraction, parameterized
casts, value correctness generally.

Consequence: every agent reported "done" on a green build that proved
nothing, and every defect was found only by manual repro at review
time. The fix-one-break-another thrash is the direct result — there
was no automated signal that a change regressed a sibling path.

### 3.2 Inert-code rounds hid non-functional deliverables

9a (kernels uncompiled), 9b (kernel_fn silently NULL) — both passed
`make c`/`make q` green while the deliverable didn't work, because the
new code wasn't on an executor path yet. Green carried no signal until
9c put the code live — at which point latent defects surfaced all at
once as crashes.

### 3.3 Agents fixed cheap defects, skipped hard ones

Recurring: the one-line fix lands (ctx_ptr `cdef public`, cast key
map), the hard kernel work (per-row validity merge) gets skipped and
reported done. The validity merge specifically has been outstanding
since 9a and dodged four times.

### 3.4 The C ABI surface is large and untested per-kernel against nulls/shapes

48 kernels, each needing correct handling of dense/constant/null
shapes and validity. The parity test proves C-vs-nanobind *agreement*
but (a) shares the nanobind null-dropping bug and (b) the executor-
level behaviour (null constants, partial nulls) isn't covered. The
kernels were written without a null/shape conformance harness.

## 4. Decision points for resuming (architect)

These are surfaced, not decided.

1. **The live working-tree regressions.** Revert 9c-completion-2's
   binop changes (restore bitwise/concat to the 9c-working state, lose
   the `NULL+1` fix), or hold the tree as-is and fix forward? The tree
   is uncommitted — this is also a "should we commit the verified-good
   Phases 1–8c + 9a/9b now, to stop carrying 9c's breakage on top of
   solid work?" question.

2. **Gate before more kernel work.** The thrash will not stop without a
   value-checked binop/null/shape suite. Strong recommendation: build
   that gate (a `make`-runnable value-checked matrix:
   arith/bitwise/concat/cast/extraction × non-null/all-null/partial-
   null) **before** resuming 9c. Then the binop fix iterates against a
   real signal instead of manual repro.

3. **`make q` coverage expansion.** Broader than #2 — the six bugs this
   train surfaced (incl. pre-existing COUNT(*) and assemble_fixed) all
   slipped a shape-only suite. A standing value-checked regression
   suite is the highest-leverage durable fix and is independent of
   Phase 9.

4. **Phase 9 approach.** Is per-kernel C ABI + 48-entry registry the
   right granularity, or should the binop path stay on the (working,
   already-C++) DV fast path + nanobind fallback and Phase 9 narrow its
   ambition? The DV fast path already handles `id+1` correctly with no
   Python; the per-morsel `PyObject_Call` that Phase 9 targets is the
   *fallback* path (mixed types, bitwise, concat, null) — lower
   frequency. Worth asking whether the nogil end-state justifies the
   thrash, or whether a smaller Phase 9 (cast + extraction C-native,
   leave binop fallback on nanobind) captures most of the value at a
   fraction of the risk.

5. **Sequencing of the two known correctness bugs** (COUNT(*)-WHERE,
   assemble_fixed no-ELSE) — independent of Phase 9, still open, real
   wrong-answers/crashes shipping today. Arguably higher priority than
   any Phase-9 performance work.

## 5. Recommendation (mine, for the architect to weigh)

1. **Commit the verified-good work** (Phases 1–8c, 9a, 9b) so it stops
   riding under 9c's regressions, OR revert 9c-completion-2's binop
   changes to get the tree back to a no-crash state. Either way, get
   the tree to a known-good baseline first.
2. **Build the value-checked regression suite** (#2/#3) as the next
   piece of work — it's the gate whose absence caused the thrash, and
   it pays off across the whole engine, not just Phase 9.
3. **Then** resume the binop C path (or descope per #4), iterating
   against the new gate.
4. Fix the two standing correctness bugs (#5) whenever they fit; they
   don't depend on any of the above.

The single most important lesson, in CLAUDE.md terms: *"avoiding or
hiding failure lies about state and is a major source of wasted time."*
Eight rounds were spent because a shape-only gate let non-functional
and regressed code report green. The gate is the fix.

## 6. Pointers for whoever resumes

- Live crashes to repro: `SELECT id | 2 FROM $planets`,
  `SELECT name || '!' FROM $planets`,
  `SELECT CASE WHEN id>4 THEN NULL ELSE id END + 10 FROM $planets`.
- Working C-native paths to preserve: cast, extraction, CASE,
  arithmetic non-null, all-null arithmetic constant.
- The binop kernels: `draken/ops/kernels/binary_op_*.cpp`. The null/
  bitwise/concat regressions live in the interaction between the
  9c-completion-2 null-handling change and these kernels + the
  executor's C-native binop branch in `evaluation.pyx`.
- `make kernel-parity` (48/48) is the one trustworthy kernel gate;
  extend it with executor-level null/shape cases rather than trusting
  C-vs-nanobind parity alone.
