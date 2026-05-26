# 02 — Briefing for the operator-rewrite PM

> Status: briefing record. Written at handover from draken-PM (substrate
> work) and eval-PM (evaluator migration) to you (operators + scheduler
> + parallelism). The conch is yours.
>
> This document is a briefing, not a plan. Plans come from your tickets.

---

## 1. Read these first, in this order

Skipping these costs days. Two prior PMs (me as draken-PM, and the
eval-PM) repeatedly burned cycles by mis-specifying tickets that
contradicted documents already in the tree.

1. **`CLAUDE.md`** at the repo root — non-negotiable engineering
   contract. Internalise §1, §3, §9, §11. Especially §11 (the
   DrakenVector model and the **encoding-shape-is-layout-not-type**
   rule — see `draken-encoding-shape-is-layout-not-type` memory).
2. **`opteryx/operators/docs/design/00_operators_and_parallelism.md`**
   — your primary spec. The parallelism architecture, operator
   contract, and six unresolved design forks at §12 (must close before
   Phase 1 begins).
3. **`opteryx/operators/docs/design/01_draken_state_at_handover.md`**
   — the substrate you inherit. **Note: this doc was written before
   the post-handover work** (zombie sweep, DICTIONARY/CONSTANT
   deletion, several new draken primitives, eval-PM migration). See §3
   below for the current-state delta.
4. **`opteryx/expression/evaluator/docs/design/00_pm_briefing.md`** +
   **`02_guidance_after_pause.md`** — eval-PM's briefing and the
   course-correction they ran. The patterns (and anti-patterns) they
   surfaced apply directly to operator work.
5. **`draken/docs/design/09_delivery.md`** — operational state of
   draken. Mostly stable.

After that, read the actual operator code: `opteryx/operators/`.
21 files. Plus `_operators.pyx` (the umbrella), `_factory.pxi`, the
collectors under `grouped_aggregate_hashed/`.

## 2. What is yours

In scope:

- All 21 files in `opteryx/operators/`.
- The execution engine: `opteryx/managers/execution/serial_engine.py`
  (and whatever replaces it for the scheduler).
- The scheduler / pipeline DAG / parallelism implementation when you
  reach Phase 2+ of `00_operators_and_parallelism.md`.
- `opteryx/compiled/structures/bloom_filter.pyx` — used by joins, has
  a typed-Vector cimport blocker.
- The collectors under `opteryx/operators/grouped_aggregate_hashed/`
  including the `_collectors_*.pxi` files and `_factory.pxi`.
- `opteryx/operators/_operators.pyx` (the umbrella that orchestrates
  push pipelines).
- `opteryx/operators/parquet_read/parquet_read.pyx` — has a known ILIKE
  IndexError surfaced by eval-PM during their work. Pass-1/pass-2
  merge logic. Eval-PM did not investigate; that's yours.

Out of scope — explicitly NOT yours:

- `draken/` C++ internals, headers, `draken_native.cpp`. The substrate
  is frozen. **Protocol: surface gaps, do not extend.** See §9 below.
- `opteryx/expression/` and `opteryx/expression/evaluator/` — eval-PM's
  lane, just landed.
- `rugo/` — separate library, migrated separately (E.31 in progress).
- `opteryx/connectors/`, `opteryx/managers/virtual_datasets/`,
  `opteryx/models/manifest.py` — non-operator surfaces that drift-prone
  agents have tried to fold in. Leave them.
- UTF-8 cluster, regex cluster, heavy specials — draken-side work
  (mostly), surface back if you hit one.
- Producer-surface design questions — see §3 and §9.

## 3. The state you inherit (current, supersedes the stale parts of `01_…`)

### 3.1 What works

- `make draken` builds cleanly. **2816+ native tests pass** via `make dt`.
- Draken ABI: clean. `DRAKEN_DICTIONARY` and `DRAKEN_CONSTANT` deleted
  with `#error` sentinels in `_abi_guard.cpp` preventing return.
- `str_init_extern` pxd matches `.h` (5-arg signature).
- 6 sanctioned `.so` files in `draken/`:
  `_abi_guard`, `_mimalloc_smoke`, `draken_native`, `morsels/morsel`,
  `vectors/bool_vector`, `vectors/vector`. **No zombies.**
- Cython shim layer for Vector/BoolVector/Morsel — provides
  `__pyx_vtable__` for cimport-using callers.
- 24+ nanobind C′ extensions in `opteryx/compiled/nanobind/` —
  consumer-shaped (`Vector → Vector`).
- **Eval-engine migration complete** as of the most recent handover.
  Operators that previously called the broken evaluator now have a
  working dispatch substrate.

### 3.2 New draken primitives added during the recent cycle (use these)

- `draken.draken_native.vector_fp16_zeros(length, dimension)` — fresh
  zero-initialised FP16 Vector.
- `draken.draken_native.vector_fp16_with_nulls(length, dimension)` —
  fresh FP16 Vector with all-null bitmap (set bits to mark rows valid).
- `opteryx.compiled.nanobind.vector_bool_ops.vector_uint64_eq_scalar(buffer, length, target)`
  — element-wise scalar equality on a uint64 buffer → BoolVector. No
  Python loop needed for the hash-join fast path.
- `draken/ops/decimal_arith.h` — element-wise scale-aware decimal
  arithmetic (E.32). Add/sub/mul/div/mod/neg with PostgreSQL rules.
- Several producer-side constructors that were already in
  `draken_native.cpp`: `vector_from_sequence` (int64),
  `vector_int{8,16,32}_from_sequence`, `vector_float{32,64}_from_sequence`,
  `vector_from_string_sequence`, `vector_date32_from_sequence`,
  `vector_timestamp_from_sequence`, `vector_decimal_from_sequence`,
  `vector_interval_from_sequence`, `vector_fp16_from_sequence`,
  `vector_array_from_sequence`. Use these — do not invent new
  Python-loop constructors.

### 3.3 Known compile blockers (concrete first-ticket fodder)

These will fail `make compile` today. Each is a small targeted fix:

| # | File | Issue | Fix shape |
|---|---|---|---|
| 1 | `opteryx/managers/execution/serial_engine.py:28` | `from draken import Morsel` — Morsel isn't exported from package root | Change to `from draken.morsels.morsel import Morsel` |
| 2 | `opteryx/compiled/structures/bloom_filter.pyx` | cimports `Integer64Vector` (deleted typed-Vector subclass) | Migrate to uniform `Vector` + `DrakenType` dispatch via shim |
| 3 | `opteryx/operators/_operators.pyx` via `_factory.pxi` | cimports typed-Vector subclasses (`Integer64Vector`, `DecimalVector`, etc.) | Same as #2 |
| 4 | `opteryx/operators/grouped_aggregate_hashed/_key_store.pxi` | cimports `DRAKEN_STRING` (deleted alias); calls `str_init_extern` with 4-arg signature (now 5-arg with hash32) | Replace `DRAKEN_STRING` with `DRAKEN_VARCHAR`; add hash32 to `str_init_extern` call |

Item #1 is the smallest — `make q` is at 0/133 entirely because of that one line. Fix it first; the eval-PM migration is structurally complete, just unreachable through `make q` until that import is corrected.

### 3.4 In-flight gaps you'll encounter

- **38 hot-path Python imports** inside `cdef`/`cpdef` bodies (E.30a
  audit). ~25 of them die naturally during your typed-Vector migration
  (they're imports of the deleted typed-Vector subclasses). The
  remaining ~13 are pure-hoist work — move to module level. **Do not**
  hoist the typed-Vector ones in their current shape; migrate them.
- **The producer-surface design remains open.** Most producer
  primitives the existing code needs already exist in `draken_native`
  (see §3.2). For anything missing, see §9.
- **`parquet_read.pyx` ILIKE IndexError** in pass-1/pass-2 merge —
  surfaced by eval-PM, never investigated. Yours to triage when you
  reach that operator.
- **`opteryx/compiled/vector_ops/case_helpers.pyx`** was restored
  during eval-PM work. Five functions (`decide_one_branch`,
  `group_indices_and_perm`, `assemble_fixed`, `assemble_bool`,
  `assemble_flat_string`) — 4 of them compile with the shim; the 5th
  (`assemble_flat_string`) uses `StringVectorBuilder` (producer-side
  gap) and should stay stubbed with `NotImplementedError` until the
  producer-surface design closes.

## 4. The architecture you must respect

All settled. Do not relitigate. From `01_draken_state_at_handover.md`
§2.2 plus subsequent decisions:

- **One Vector class, dispatched by `DrakenType`.** No typed-Vector
  subclass hierarchy. `Integer64Vector`, `Float64Vector`, etc. don't
  exist by design — you migrate callers off them, you don't recreate
  them. Anyone proposing a `DecimalVector` or `Integer64Vector` cdef
  class is repeating the E.24/E.32 anti-pattern.
- **Encoding shape is layout, not type.** No `DRAKEN_DICTIONARY` or
  `DRAKEN_CONSTANT` enum values; they were deleted. Branching on
  encoding shape happens via `vec.data_length`, `vec.length`, and the
  `selection` pointer relationship — never via the `type` field. See
  `draken-encoding-shape-is-layout-not-type` memory.
- **`feedback-no-false-green-clean-break`.** If a migration step makes
  something stop compiling, that is *information*. Surface it. Do not
  introduce a compatibility shim, a typedef alias, a verbatim-C
  struct, a Python-level fallback, or any "TODO: optimise later"
  comment. Recognition list in §7 of the eval-PM briefing applies
  verbatim.
- **String type family:** `DRAKEN_VARCHAR` (default, ASCII),
  `DRAKEN_NVARCHAR` (opt-in Unicode), `DRAKEN_VARBINARY` (opaque
  bytes). `DRAKEN_STRING` is the old name — deleted. The migration of
  `_key_store.pxi` (compile blocker #4) is the typical case.
- **`feedback-hash-no-parity`** — hash values are disposable. Don't
  require cross-version hash compatibility.

## 5. The §12 design forks — close before Phase 1

`00_operators_and_parallelism.md` §12 lists six unresolved decisions.
Per Risk #7 and #8 in that doc, decisions §3.1 (`_push_impl` purity)
and §6 (coalesce-after-filter) must close before Phase 1 operator
audits begin. Surface to the architect early. Recommendations in the
doc are mostly sensible; closing them in a single review session is a
~30-minute call, not a multi-day exercise.

The decisions:

1. §3.1 — `_push_impl` purity contract (plain cdef / `nogil`
   signature / `with nogil:` block).
2. §4 — LIMIT counter strategy and non-determinism.
3. §5.4 — thread pool: per-session vs per-query.
4. §5.4 — termination primitive: `threading.Event` / C atomic /
   `volatile`.
5. §6 — coalesce-after-filter strategy.
6. §8 — multiplicity declaration: catalog vs operator-`__init__`.

## 6. How to scope and write tickets

Three patterns from the last week cost real time:

### 6.1 Drift kills

- **E.24** was scoped at 5 files, touched 144, introduced annotated
  fake-green shims. Reverted as E.25.
- **E.32 (decimal agent)** drifted into reintroducing a typed-Vector
  subclass hierarchy starting with `DecimalVector`. Caught and
  reverted before propagation.
- **Recent eval-PM run** drifted into operator territory until
  course-corrected; legitimate work mixed with out-of-scope
  modifications, ended up reverted from operator files.

Every ticket needs explicit STOP conditions. Examples that worked:

- "If file count exceeds ~5, stop and surface."
- "If you find yourself extending `draken_native.cpp`, stop."
- "If you'd write `cdef object` to make something compile, stop."
- "If a fix needs a 'TODO: optimise later' comment, stop."

### 6.2 Trust but verify

Every "I am done" claim from an agent gets independently verified
before acceptance. Patterns:

- `git diff --stat HEAD` to see scope.
- Read the diff for the three big anti-patterns: `cdef object`,
  typed-Vector subclasses, compatibility shims.
- Read agent-introduced comments for fake-green tells ("for
  compatibility", "TODO: optimise later", "runtime correctness is a
  separate concern", "acceptable for now").
- Check whether the agent extended `draken_native.cpp` or `draken/`
  files. The protocol forbids it without explicit surface-and-approve.

### 6.3 Anti-pattern recognition

If you find yourself writing, or accepting from an agent, any of
these, recognise and reject:

- `cdef object foo = something_returning_Vector(...)` — should be
  `cdef Vector`.
- `cdef class FooVector(Vector)` — the deleted hierarchy, do not
  recreate.
- `[h == target for h in hashes]` — Python loop over per-row data; the
  primitive doesn't exist, surface and stop.
- `from <module> import foo` inside `cdef`/`cpdef` bodies — hoist or
  eliminate (per E.30a category).
- `# TODO: long-term fix is X, for now Y` — Y is fake-green if X is
  correct.
- `cdef extern from *: """C struct that doesn't exist in the .h"""` —
  the E.24 `DrakenMorsel` smuggling pattern.

## 7. Hard-won lessons specific to operator-PM area

### 7.1 Operators are both consumers and producers

The producer side is where the last two days of drift happened. When
an operator's `finalize()` builds a Vector from accumulated state,
it's a producer. The producer surface (sequence constructors, scalar
constructors, etc.) lives in `draken_native.cpp` per §3.2. Use what's
there; don't reinvent or smuggle via `cdef object`.

### 7.2 The collector internals need restructuring, not just import hoisting

The `_collectors_*.pxi` files have a deeper anti-pattern than just
inline imports. The pattern:

```cython
cdef object finalize(self, ...):
    from draken.interop.arrow import vector_from_sequence  # ← inline import
    vals = self._values[:num_groups]                       # ← Python list
    return vector_from_sequence(vals)                      # ← Python call
```

is wrong on three counts: `cdef object` return, hot-path Python
import, and building Python lists in a hot path. The correct shape is
typed C-level work returning a typed `Vector`. Migration of these
files is non-trivial; surface the scope before tackling.

**Per-morsel vs per-row dispatch — pick the right template:**

The §9 Ticket 2 worked example (`arithmetic.pyx`) uses **per-morsel
dispatch**: Vectors treated as opaque Python objects, dispatched via
`getattr(v, "type", None) == _draken_native.X`, results built via
nanobind calls. That's correct for the per-morsel case (`_push_impl`
called once per morsel, dispatches once per call).

Collectors are **per-row inner loops**. They can't pay
Python-attribute-access per row. They need C-level typed buffer access.
The pattern (post-typed-subclass-deletion) is:

```cython
from draken.vectors.vector cimport Vector
from draken.core.buffers cimport DrakenType, DRAKEN_INT64
from libc.stdint cimport int64_t, uint8_t, uint32_t

cdef class IntegerCollector:
    cdef int64_t* _data
    cdef const uint32_t* _selection
    cdef const uint8_t* _validity
    cdef uint32_t _length

    cpdef void ingest(self, Vector vec) except *:
        # ONE dispatch per morsel.
        cdef DrakenType t = vec.unified().type
        if t != DRAKEN_INT64:
            raise TypeError(...)
        # Pull pointers ONCE before the loop.
        self._data      = <int64_t*>vec.unified().data
        self._selection = vec.unified().selection
        self._validity  = vec.unified().validity
        self._length    = vec.unified().length
        # Pure-C inner loop, zero Python access per row.
        cdef uint32_t i, idx
        for i in range(self._length):
            idx = self._selection[i]
            if self._validity == NULL or (
                self._validity[i >> 3] & (1u << (i & 7u))
            ):
                # use self._data[idx]
                ...
```

The contract:
- Type dispatch happens **once per morsel**, never per row.
- Buffer pointers are cached once and reused across rows.
- Inner loop uses raw C pointers (`int64_t*`, `uint32_t*`) — no
  `getattr`, no `cdef object`, no Python calls.
- Access pattern is the uniform `data[selection[i]]` per CLAUDE.md
  §11 — works for Dense / Constant / Dict shapes without
  shape-awareness in the kernel.
- The shim's `unified()` returns non-const `DrakenVector*`, so
  producer-side collectors that need to write into a result buffer
  have mutation access through the same interface.

Old `<Integer64Vector>vec._values` did the same thing (typed C
access), just routed through a typed-subclass cast. The subclass is
gone; the cast goes through `Vector.unified()` instead. Same C-level
access, different Python-level shape.

**`buffers.pxd` exposes the type tags** for cimport-level dispatch:
`DRAKEN_INT64`, `DRAKEN_FLOAT64`, `DRAKEN_VARCHAR`, `DRAKEN_BOOL`,
`DRAKEN_DECIMAL`, `DRAKEN_DATE32`, `DRAKEN_TIMESTAMP64`, `DRAKEN_NULL`,
`DRAKEN_VECTOR_FP16`, etc. — all the values in `buffers.h`. If a
needed value is missing from the pxd, surface it (most recent example:
`DRAKEN_DECIMAL`, `DRAKEN_NULL`, `DRAKEN_VECTOR_FP16` were added on
2026-05-25 when a per-row dispatch needed them).

### 7.3 `_key_store.pxi` has cascading errors

It's broken at compile time for two independent reasons (DRAKEN_STRING
+ str_init_extern). Don't be surprised by error #2 after fixing
error #1. The fix is mechanical but takes both passes.

### 7.4 ParquetRead's ILIKE pass-1/pass-2 merge

Eval-PM surfaced this; you inherit it. The IndexError is likely a
selection-vector bound check that doesn't account for the merge
re-ordering. Treat as its own focused investigation when you reach
parquet operators.

### 7.5 The scheduler is its own initiative

Phases 2+ of `00_operators_and_parallelism.md` are a separate project
on the scale of the draken rebuild itself. Don't conflate them with
the per-file migration work of Phase 1. Phase 1 is "make every
operator §3.1-compliant"; Phase 2+ is "build a scheduler."

## 8. The protocol with draken-PM

When you hit a missing draken-side primitive or a draken-side bug:

1. **Stop.** Don't extend `draken_native.cpp`, `draken/core/`,
   `draken/ops/`, or `draken_bridge.h` yourself.
2. **Surface to draken-PM** (me) with a clear description of:
   - What you're trying to do
   - The missing primitive / observed bug
   - The smallest viable shape (signature, semantics)
3. **Draken-PM adds the primitive directly** in fast turnaround.
   Recent examples: `vector_uint64_eq_scalar`, `vector_fp16_zeros`,
   `vector_fp16_with_nulls`. The pattern is "small kernel ticket,
   ~80 lines of nanobind C++, smoke-tested, done in under an hour."

The protocol exists because the alternative — agents extending
draken-side code themselves — has produced the worst drift incidents
(E.24, E.32, the recent decimal subclass attempt). Agents who follow
the protocol get fast turnaround; agents who don't get their work
reverted.

**Exception observed:** the eval-PM agent made three small draken-side
fixes (TIMESTAMP64 logical-type attachment, `compare_scalar` int
acceptance, `take_child` signature fix) during their migration and
surfaced them explicitly. The fixes were accepted because each was
correct, well-commented, and surfaced. But the protocol still
stands — that exception worked because the agent's instincts were
good, not because the protocol is loose. Next time, surface first.

## 9. Suggested first three tickets

A starting punch list — adjust as you learn the code:

### Ticket 1 — The one-line `serial_engine.py` fix

Change `from draken import Morsel` to `from draken.morsels.morsel
import Morsel` at `opteryx/managers/execution/serial_engine.py:28`.
Verify `make q` rises off 0/133. This is the smallest single change
with the highest payoff: unblocks the entire engine integration
substrate that eval-PM just finished.

Scope: one file, one line. Acceptance: `make q` reports a positive
pass count.

### Ticket 2 — `bloom_filter.pyx` + `_factory.pxi` typed-cimport migration

Migrate both off `Integer64Vector`, `DecimalVector`, etc. cimports to
uniform `Vector` cimport from the shim. Runtime type discrimination
via `vec.type == DRAKEN_INT64`, etc. The pattern matches eval-PM's
migration of `arithmetic.pyx` — see what they did for a worked
example.

Scope: two files. Acceptance: both compile, `make q` doesn't regress.

### Ticket 3 — `_key_store.pxi` (the cascading-error one)

Two-pass fix: replace `DRAKEN_STRING` with `DRAKEN_VARCHAR`; update
`str_init_extern` call to pass `hash32` as the 4th argument
(compute via XXH3 inline, or look at how the live nanobind callers in
`draken_native.cpp` pass it).

Scope: one file. Acceptance: compiles, `make q` doesn't regress.

After these three, your picture of the remaining operator work
clarifies. Phase 0 of `00_operators_and_parallelism.md` (the audit
ticket) is then the right next phase boundary.

## 10. Cadence

- **Small tickets, often.** Hours, not days. A ticket that takes >1
  agent work session is probably two tickets in a trenchcoat.
- **Verify every completion claim.** No exceptions. Read the diff.
- **`make q` is the gate.** Each ticket should not regress it.
- **Surface to architect early.** §12 closures, anything genuinely
  architectural. Don't pick design questions yourself.
- **Memory files are durable.** When the architect closes a decision
  during your work, capture it in a memory file before moving on. The
  ones from the rebuild ([[draken-rebuild-delivery-plan]],
  [[draken-encoding-shape-is-layout-not-type]],
  [[draken-consumer-edge-pattern]], [[feedback-no-false-green-clean-break]]
  et al.) are all examples — bind decisions in writing.

## 11. Who to ask

- **Architect** — design forks, type semantics, parallelism §12
  closures, any "should this exist?" question.
- **Outgoing draken-PM (me)** — questions about the substrate,
  missing primitives (I add them on request), why a draken-side
  decision went the way it did.
- **Eval-PM (outgoing)** — questions about how the evaluator works
  post-migration. Their briefing and course-correction docs are at
  `opteryx/expression/evaluator/docs/design/`.

## 12. What "done" looks like

Your initiative ends when:

1. All 21 operator files migrated off typed-Vector cimports to
   uniform Vector + DrakenType dispatch.
2. `_key_store.pxi`, `bloom_filter.pyx`, `_factory.pxi` (and friends)
   compile cleanly.
3. `serial_engine.py` import fixed; eval-engine reachable through
   `make q`.
4. `parquet_read.pyx` ILIKE IndexError resolved.
5. `case_helpers.pyx`'s `assemble_flat_string` migrated once the
   producer-surface design closes (may be your work or draken-PM's).
6. §12 forks closed.
7. The scheduler/parallelism work tracked in
   `00_operators_and_parallelism.md` (Phases 0–6) — that's its own
   multi-week project, scoped separately. Probably becomes the next
   PM initiative after the migration is clean.
8. `make q` materially above 0/133 — operator-PM is what closes most
   of the remaining test failures.
9. You write a handover doc capturing what's done, what isn't, and
   what the scheduler-PM (if that's a separate initiative) needs to
   know.

---

**Closing note.** The draken rebuild and the eval-engine migration
have produced a substrate that's *finally* clean — no zombies, no
shape-as-type, no fake-green compat shims, no Python loops in hot
paths. That cleanliness was bought with a lot of revert cycles and
architect anger. Don't waste it. The operator migration is the
piece where most of the engine becomes runnable; the discipline of
small, scoped tickets with concrete acceptance criteria is what gets
you across the finish line without re-introducing the patterns we
just deleted.

The conch is yours.
