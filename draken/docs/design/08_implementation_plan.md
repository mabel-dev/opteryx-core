# Draken Rebuild — Implementation Plan (DRAFT)

> Status: DRAFT. The execution roadmap for the C++-first rebuild. Design is in
> `00`–`07`; this doc is *sequence, gates, and risks*. Strategy (from `03`/`04`): new
> `draken` is built behind a **per-type fallthrough shim to `draken_old`**, so the
> engine stays green throughout; types come off the shim one at a time, gated on
> parity + benchmark + `make q`. Contract is **function, not signature** (`02`/`03`).

<!--
/opus/ FINAL-REVIEW SUMMARY (this plan)
Strong and directly responsive to the design review — the ABI freeze + static_assert,
the extern-pxd path for the compiled consumers, naming mimalloc, out-of-band
logical-type/stats, arithmetic folded into the pilot op set, and the consumer
enumeration in 07 all close gaps I'd raised. The phasing (scaffold → foundations →
one pilot proves the template → gated type-by-type → de-shim) is the right shape and
the gates are real. I'd approve starting Phase 0/1 on this.

Status of the five (all now closed at the design level):
 A. CLOSED — int64×float64 compare = EXACT; CarcharSet = hash-only accepted (named §1
    exception). Both gated into Phase 2. See "Resolved since review" + `02`.
 B. CLOSED (process) — de-shim is signature-preserving; interface redesign is a
    separate later pass. Baked into Phase 2 + the cross-cutting re-homing track.
 C. CLOSED (process) — parity harness negative control added to Phase 0 gate.
 D. CLOSED — STRUCT/MAP extract-only; whole-value ops raise "unsupported" at bind time
    (`06`).
 E. CLOSED (process) — Phase-2 bench must exercise mimalloc CROSS-THREAD free, not just
    same-thread.

All architect forks from the review are resolved (`00`–`08` updated). What remains is
execution discipline (B/C/E are process gates, not open design questions) and the one
pending validation: mimalloc under clickbench (Phase 2), with the per-morsel arena as
the named escape hatch.
-->


## Invariants that hold for the whole project
- **`buffers.h` struct ABI is FROZEN** from Phase 1 to the end (`00`/`03`): new and
  `draken_old` vectors are byte-identical, or compiled consumers (369 cimport sites)
  and mixed old/new morsels segfault. Logical-type + stats stay **out-of-band**.
- **`draken_old` stays buildable** until the shim empties — it's the parity oracle.
- **`make q` is green after every type de-shim.** Never merge a red gate.
- No `object` in compiled paths; no `import opteryx` from draken core (`03`).

## Phase 0 — Scaffolding (no behaviour change; engine 100% on `draken_old`)
Goal: the new layer can be built and tested empty, with the shim routing everything
to `draken_old`.
- Vendor **mimalloc** (§4) as the allocator; wire into the build.
  <!-- /opus/ mimalloc resolves the allocator-concurrency concern I raised on 01
  (per-thread heaps, thread-safe by design) — good. But state the access pattern
  explicitly: vectors are often ALLOCATED on a rugo decode-pool thread and FREED later
  on an execution thread. That's mimalloc's *cross-thread free* path (deferred free
  list), which is correct but slower than same-thread free, and is exactly the churn
  pattern the per-vector ownership decision (01) maximises. Validate THIS pattern in
  the Phase-2 bench, not just single-threaded alloc/free, or the bench will look
  rosier than production. -->

- New `draken/` skeleton: `core/buffers.h` (the frozen struct + `DRAKEN_*` tags +
  inline helpers — copied byte-for-byte from `draken_old`, the only change being the
  `flags` byte in tail padding), and a hand-written `core/buffers.pxd`
  (`cdef extern from "buffers.h"`) so the 95 `core.buffers` + 369 cimport consumers
  compile unchanged against the C++-defined struct.
- **nanobind** module skeleton (one module, `03`); a `Vector` handle that, for every
  op/type, **delegates to `draken_old`** (the shim).
- Build: compile new `draken` + `draken_old` (under its own name) + mimalloc.
- Test harness skeleton (`04`): native unit-test rig + the **parity harness**
  (randomised inputs, new vs `draken_old`, hypothesis at the Python edge) + a
  per-op microbench rig.
  <!-- /opus/ The parity harness is the safety net the entire bring-up leans on, so
  prove it can FAIL before you trust it to pass. At the end of Phase 0 (or start of
  Phase 2) inject a deliberate divergence into one shimmed op and confirm the harness
  flags it — a negative control. A parity harness that silently always passes (e.g.
  because both paths route to draken_old, or the comparator is too loose on NaN/-0.0/
  null) is worse than none: it manufactures false confidence to de-shim on. -->
- Capture the **clickbench baseline** here, on BOTH target platforms (ARM dev / x86
  prod, `04`/`06`), tagged to the Phase-0 commit — Phase 4 compares against it.
  <!-- /opus/ Make the baseline per-platform and per-commit. A single dev-ARM number
  compared later against a different machine/commit isn't a regression gate, it's
  noise. This is the apples-to-apples anchor for the "≥ baseline" definition of done. -->
- **Gate:** `make c` clean; `make q` 100% (running entirely on `draken_old`).

## Phase 1 — Foundations (still all behaviour on `draken_old`)
Goal: the pieces every type needs, with the shim still covering all types.
- `core` allocation + RAII buffer ownership (`01`): owning buffer handle vs non-owning
  span; `unique_ptr` + stateless deleter to mimalloc; the shared-global
  identity/zero selection + `draken_zero_validity`.
- Base **vector** surface and the **dumb `Morsel`** container (`01`/`03`).
- The **logical-type descriptor** + **stats side-channel**, both out-of-band keyed by
  column (`00`/`05`/`06`) — interfaces only; populated as types need them.
- Ingestion primitives: `from_decoded` (ownership transfer) + own-and-copy
  `from_sequence`; `scalar_constructors.from_scalar` (literal → constant vector).
- **Gate:** `make c` + `make q` green; parity harness runs against `draken_old`.

## Phase 2 — Pilot type: `int64` end-to-end
Goal: prove the whole pattern on the most representative type, then de-shim it.
- Implement the full int64 op set (`02` catalog), `hash` first (joins/distinct/
  group-by depend on it), then `compare_*`, `between`, `in_list` (CarcharSet probe),
  `sum/min/max`, `arithmetic`, `take`, `materialize`, `compress`, element access.
  Dispatch via the **table** (`02`); SIMD by hand for the integer kernels.
- Wire `int64` results through the logical-type/stats out-of-band path where relevant.
- **Implement & assert the two (now-resolved) correctness rules HERE** — the pilot hits
  both, so they are gated, not deferred:
   1. int64 × float64 COMPARE/IN = **EXACT** (range-check then integer compare; no
      double promotion). Arithmetic→double is the defined SQL result type. Per the `02`
      precision table. Unit test must include values straddling 2^53.
   2. CarcharSet membership = **hash-only, accepted** (named §1 exception, `02`). No
      verify path. Document the bound at the call site; no test can assert "no
      collision," so the gate is just: the exception is recorded and the path is the
      single shared hash path (no accidental raw-key branch that drifts).
  <!-- /opus/ Both decided by the architect post-review; this phase is where they go
  from decision to enforced code. The exact-compare rule is the one with a real unit
  test (2^53 boundary); the hash-only rule is a documented acceptance, not a testable
  invariant. CLOSED at the design level. -->
- **De-shim is signature-preserving.** The shim presents int64's EXISTING Python
  signature; the implementation swaps underneath. Interface redesign ("function not
  signature", `03`) is a SEPARATE later pass, not bundled into the de-shim.
  <!-- /opus/ This is the sequencing safeguard. If you redesign int64's API AND swap
  its implementation in one move, a make-q failure can't tell you which broke it, and
  you've coupled ~72 call-site edits to a risky swap. Keep de-shim a pure
  implementation substitution (same signature) so make q isolates it; redesign
  interfaces in a dedicated pass once all types are native (Phase 4 territory). The
  cross-cutting "consumer re-homing alongside" track (below) should be read this way. -->
- **Gate (per `04`, full matrix):** native unit tests (type×op×shape×null×size+edges);
  **parity vs `draken_old`** over randomised inputs; **benchmark ≥ `draken_old`**;
  the two correctness decisions above resolved + asserted; remove `int64` from the
  shim; `make q` 100%.
- Exit criterion = the template for every remaining type.

## Phase 3 — Remaining types, one at a time (each fully gated)
Order (high-traffic first, per `07`): **bool → string → float64 → timestamp →
date32**, then the long tail **time → decimal → interval → vector(fp16) → array →
null**. Per type, all its ops, then the Phase-2 gate, then de-shim, then `make q`.
Type-specific notes (from `06`):
- **bool** — bit-packed; bit-addressed accessor (same family as validity).
- **string** — German-string slots + arena (`string_slot.h`, `string_arena`); arena
  travels with `data` (`01`); compares short-circuit on `len||prefix`.
  <!-- /opus/ Treat string as a SECOND pilot, not a routine port. It's the first type
  to introduce owned variable-length storage (arena), dict-encoded data with owned
  codes + unique slots, and the arena-travels-with-data ownership rule — i.e. it's the
  RAII-subtree template the same way int64 was the dispatch/SIMD template. Give it the
  extra scrutiny risk #3 implies, and prove arena ownership here before array/nested
  builds on it. -->
- **STRUCT/MAP** — string-backed JSON, **extract-only**. Explicitly SCOPE OUT struct
  equality / GROUP BY / DISTINCT / JOIN on whole struct values in v1.
  <!-- /opus/ "extract-only" is the right move and it's also what makes the JSON
  approach safe: it sidesteps the canonicalization/lossiness correctness hole flagged
  in 06 (two logically-equal structs hashing differently via JSON text). But that
  safety only holds if equality/grouping/join on whole structs is genuinely
  unsupported — so SAY it, and make those ops raise a clear "unsupported" rather than
  silently hashing JSON bytes and returning wrong groups. If whole-struct equality is
  ever needed, that's the trigger to promote to parallel child vectors (the 06 plan),
  not to start canonicalizing JSON. -->

- **decimal** — logical `DECIMAL(p,s)` → physical int64 (p≤18) **and** int128 (p≤38).
- **timestamp** — int64 + logical unit/tz; **CCTZ** (or C++20 chrono later).
- **vector(fp16)** — dimension is a logical-type param; native `from_float_pylist`.
- **array** — offsets + child vector (recursive); parent owns child (RAII chains).
  STRUCT/MAP are **string-backed JSON** (extract-only via `yyjson`), not a new shape.

## Phase 4 — De-shim, harden, delete `draken_old`
- When the shim is empty: drop `draken_old` from the build; full regression +
  **`make clickbench`** (must match/beat the `draken_old` baseline captured in Phase 0).
- **C++ consumer ABI cleanup:** migrate C++ consumers (rugo, C++ ops) from `cimport`
  to `#include "draken/core/buffers.h"` where it's cleaner; keep the `extern-pxd` for
  the Cython consumers that stay Cython.
- Internalize/rename `var_vector` and re-home its one caller (`07`).
- Delete `draken_old`; final `make q` + clickbench.

## Cross-cutting tracks (run alongside, not separate phases)
- **Consumer re-homing (`07`):** ~161 files, ~22 module paths. The struct ABI is
  frozen so cimports keep working; Python-import call sites get updated as interfaces
  are redesigned ("function not signature"). Drop confirmed-dead surface (arrow).
  <!-- /opus/ Reconcile this with the "de-shim is signature-preserving" note in Phase
  2: don't redesign-and-re-home a type's Python interface in the SAME step that swaps
  its implementation off the shim. Sequence it: (1) de-shim with the existing
  signature (make q isolates the swap) → (2) once native, redesign + re-home call
  sites as a distinct change. Running interface churn "alongside" the swaps is where a
  green-looking de-shim hides an API regression. The re-homing is real work but it's
  downstream of the swap, not concurrent with it. -->
- **Re-shim = rollback.** Because `draken_old` stays buildable to the end, removing a
  type from the shim is reversible: a post-ship regression can be re-shimmed in one
  line while it's diagnosed. This is the built-in safety valve — note it as the
  rollback story rather than leaving it implicit.
- **Parity oracle:** run `draken_old` **live** in parity tests (not golden files).
- **Benchmarks:** per-op micro + clickbench macro; a type can't de-shim if it regresses.

## Top risks / de-risk early
1. **ABI drift** — the single biggest hazard (`00`/`03`). Add a build-time
   `static_assert(sizeof(DrakenVector)==40)` + offset asserts mirrored in new + old, so
   any accidental field addition fails the build instead of segfaulting a consumer.
   <!-- /opus/ Right mitigation, cheap, do it Phase 0. Make the asserts cover every
   shared field's OFFSET, not just sizeof — `flags` must land in the old struct's tail
   padding without shifting any prior field, and sizeof==40 alone wouldn't catch a
   reordering that kept the total size. Also pin the DrakenType enum's underlying type
   and tag VALUES (static_assert on a couple of representative values) — a mixed
   old/new morsel dispatches on the tag, so a tag renumber is as fatal as a layout
   shift and far easier to do by accident. The extern-pxd must be regenerated/checked
   against the header too (a stale pxd is silent ABI drift on the Cython side). -->

2. **mimalloc under churn** — the per-vector-free bet (`01`). Validate in Phase 2's
   benchmark; fallback is per-morsel arena (rejected, but the escape hatch).
3. **String/array/nested ownership** — the RAII subtree (`01`/`06`); get it right on
   the string pilot before array.
4. **Pilot scope creep** — keep Phase 2 to `int64` only; resist starting other types
   until the gate template is proven.

## Sign-offs (§4)
- ~~Vendor IANA tzdata (CCTZ)~~ **RESOLVED: fixed-offset only, no tzdata dependency**
  (`06`, /JJ/ "store as offsets"). Named-zone/DST is out of v1 scope. No §4 dependency
  added here.
- Confirm **mimalloc** post-clickbench (locked pending Phase-2 validation; fallback =
  per-morsel arena).

## Resolved since review (architect decisions — now baked into the design docs)
- **Canonical struct frozen at 40 bytes**; logical-type + stats are out-of-band, keyed
  by column, never struct fields (`00`/`05`/`06`). Guarded by Phase-0 size+offset+enum
  asserts in new *and* `draken_old`.
- **int64×float64 compare is EXACT** (no double promotion); arithmetic→double is the
  defined SQL semantic. Precision table in `02`. Pilot (`int64`) must implement and
  assert this.
- **CarcharSet hash-only accepted engine-wide** as a named, quantified §1 exception
  (`02`, /JJ/). No key-verify path in v1.
- **STRUCT/MAP extract-only** in v1; whole-value `=`/GROUP/DISTINCT/JOIN/ORDER raise
  "unsupported" at bind time (`06`). No JSON canonicalization needed.
- **Timestamp = fixed-offset** (`06`).
- **Logical-type descriptor interned/immutable/shared**, borrowed pointer, out of RAII
  churn (`06`). **Decimal = int64 + int128**, promotion at the dispatch layer (`02`/`06`).
- **Nested ownership:** arena travels with `data`; array parent owns child, RAII chains
  (`01`).

## Definition of done
Shim empty; `draken_old` deleted; `make q` 100%; `make clickbench` ≥ baseline; no
`object` in compiled paths; no draken→opteryx deps; `buffers.h` is the single ABI
source of truth with size/offset asserts.
