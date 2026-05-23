# Draken Rebuild — Delivery Plan (DRAFT)

> Status: DRAFT. The operational companion to `08_implementation_plan.md`. Doc `08`
> is *design sequence*; this doc is *how we actually deliver it given the current repo
> state* — milestones, gates, the consumer-rewrite tracker, and the first concrete step.

## 0. Supersedes (architect decision, 2026-05-21)

**No shim. No fallthrough to `draken_old`. No bridging tricks to restore green.**

Doc `08` is built around a *per-type fallthrough shim to `draken_old`* that keeps the
engine green throughout, gating each type de-shim on `make q`. **That strategy is
withdrawn.** The architect deliberately gutted `draken/` (old impl moved to
`draken_old/`) to hold a *clear position*: it must always be unambiguous what is
genuinely rebuilt versus not, and nothing may "lazily claim completion" by routing
through the old code. A shim that turned `make q` green while the native work was
undone would **lie about state** — the §1 violation the engineering contract exists to
prevent.

**Consequence, accepted:** the engine build is **red for the duration of the rebuild**
and returns to green only when the work is *actually* delivered (new `draken` complete
+ consumers rewritten). Broken-but-honest is the chosen state.

What this changes vs `08`:
- The "per-type fallthrough shim" mechanism (`03`/`08`) is **deleted**.
- "Re-shim = rollback" safety valve (`08`) **does not exist**.
- "`make q` green after every type de-shim" gate (`08`/`04`) is **replaced**: whole-
  engine green (`make q`, `make clickbench`) is a single **end-of-rebuild milestone**
  (E below). Per-type gates become *engine-independent*: native unit tests + **live**
  parity vs `draken_old` + microbench.
- `draken_old` is retained **only** as the parity/reference oracle — built in
  isolation for tests, **never** imported by the engine or by new `draken`. Deleted at E.

Everything else in `00`–`08` (the data model, ownership, dispatch, encoding, the
correctness rules, the ABI freeze) stands unchanged.

## 1. Starting state (verified 2026-05-21, not assumed)

- `draken/` is **gutted** — only `draken/docs/` remains; all old `.pyx/.pxd/.hpp/.h`
  are deleted in the working tree (uncommitted).
- `draken_old/` is **intact** on disk — the parity oracle is real.
- **The build is red.** `setup.py` still lists `draken/...` sources that no longer
  exist and never references `draken_old`; ~516 consumer sites still import `draken.*`.
- Vendored deps present: `nanobind`, `yyjson`, `third_party/mabel/carchar`.
  **`mimalloc` is NOT vendored** — Milestone-A task.

### The dominant cost: compiled consumer rewrite (not the kernels)

516 consumption sites (per `07`), split by what they bind:

| Binding | sites | bridgeable? |
|---|--:|---|
| `draken.core.buffers` — the **struct** | 95 | Yes, via hand-written `buffers.pxd` (ABI frozen). Survives the rebuild. |
| per-type **cdef classes** (`StringVector`, `Integer64Vector`, `BoolVector`, `Vector`, `Morsel`, …) | **274** | **No.** nanobind emits no cimportable `cdef class`. Every site must be **rewritten** to the new binding surface. |
| Python-level `import draken.*` (the edge) | ~147 | Rewritten as interfaces are redesigned ("function not signature"). |

The **274 compiled cdef-class cimports are the critical path** — more work than the
C++ kernels. "Bring a type native" *is* "rewrite that type's compiled consumers." The
breakdown that drives ordering (cimport statements, non-`draken_old`):

| Module (owning type) | sites | | Module | sites |
|---|--:|---|---|--:|
| `core.buffers` (ABI) | 95 | | `morsels.morsel` | 13 |
| `vectors.string_vector` | 57 | | `vectors.timestamp_vector` | 10 |
| `vectors.integer64_vector` | 45 | | `vectors.integer{8,16,32}` | 18 |
| `vectors.bool_vector` | 38 | | `vectors.date32_vector` | 6 |
| `vectors.vector` (base) | 25 | | `interop.vector_sequence` | 6 |
| `vectors.array_vector` | 19 | | `vectors.float32_vector` | 5 |
| `vectors.float64_vector` | 18 | | `morsels.align` | 5 |
| | | | tail (vector_vector, scalar_constructors, time, null, var_vector) | ~9 |

## 2. Invariants that hold for the whole project (from `08`, still binding)

- **`buffers.h` struct ABI is FROZEN** at 40 bytes (LP64). New `draken` and
  `draken_old` structs must be **byte-identical** so parity tests and any mixed
  handling stay safe. Logical-type (`06`) and stats (`05`) stay **out-of-band**.
- **`draken_old` stays buildable** (isolated test target) until E — the parity oracle.
- **No `object` in compiled paths; no `import opteryx` from draken core** (`03`).
- ABI guard (risk #1): build-time `static_assert(sizeof(DrakenVector)==40)` + **per-
  field offset asserts** + a **pinned `DrakenType` underlying-type + representative
  tag-value asserts**, mirrored in **both** new and `draken_old`. A stale `buffers.pxd`
  is silent ABI drift — regenerate/check it against the header.

## 3. Gates (no-shim form)

**Per-type gate** (engine-independent — does not need `make q`):
- [ ] Native unit tests pass — full matrix: type × op × shape (dense/constant/dict) ×
      nullability (none/some/all) × size (0,1,<8 tail,large) + per-type edge values.
- [ ] **Live** parity vs `draken_old` over randomized inputs (hypothesis at the edge).
      Parity certifies *agreement*, not correctness — layer-1 native tests must assert
      the *correct* answer independently so we don't inherit `draken_old` bugs.
- [ ] Microbench ≥ `draken_old` **or** a recorded, time-boxed perf-debt waiver signed
      by the architect (a correct kernel may trail before its SIMD path lands).
- [ ] No `object` in compiled paths; no upward (`import opteryx`) deps.

**Whole-engine gate** (Milestone E only):
- [ ] All 369 cimports + ~147 Python imports rewritten; the engine compiles.
- [ ] `make q` 100%.
- [ ] `make clickbench` ≥ the per-platform Phase-0 baseline.

## 4. The central trade-off (name it, navigate it)

No-shim means a **long red period** and a **single big-bang re-green at E**. We do not
relitigate this — it is the architect's chosen, honest model. We *navigate* it:

1. **Per-type confidence without green:** native + live-parity + microbench prove each
   type correct and fast in isolation, no running engine required.
2. **"What's done" stays unambiguous:** the §2 tracker (per-type ops + per-type
   consumer-rewrite counts) is the single source of truth; a type is "done" only when
   both its native gate passes *and* its consumers are rewritten.
3. **De-risk integration before E (optional, recommended):** a thin end-to-end harness
   that drives new `draken` directly (build vectors → run ops → read back), bypassing
   the full engine, so integration bugs surface per-type instead of all at E.

## 5. Milestones

### A — Scaffolding & oracle  *(engine red; expected)*
- Retain `draken_old` as an **isolated parity-oracle build target** (its own name);
  importable by tests only, never by the engine or new `draken`.
- **Vendor mimalloc** (§4); wire into the build (per-thread heaps; one source, not one
  lock).
- New `draken/` skeleton: `core/buffers.h` — the frozen struct copied byte-for-byte
  from `draken_old`, the only change the `flags` byte in tail padding — plus a hand-
  written `core/buffers.pxd` (`cdef extern from "buffers.h"`) so the 95 struct cimports
  bind the C++-defined layout.
- **ABI asserts** (sizeof + per-field offset + `DrakenType` underlying-type/values),
  mirrored in new **and** `draken_old`.
- **nanobind** module skeleton (one module, `03`): a `Vector` handle + the dumb
  `Morsel`. **No shim, no fallthrough.**
- Test harness: native unit-test rig + **parity harness** (live vs `draken_old`) +
  per-op microbench rig. **Negative control:** inject a deliberate divergence into one
  op and confirm the harness *fails* — prove it can fail before trusting it to pass.
- **Capture the clickbench baseline** from the **last green commit** (the commit prior
  to "removing python from draken" — verify with `make q`), **per-platform** (ARM dev /
  x86 prod), tagged. Current HEAD is red, so the baseline cannot come from HEAD.
- **Gate:** new `draken` skeleton **and** `draken_old` each compile independently;
  parity harness proven to fail on injected divergence; baseline recorded. (*Not*
  `make q`.)

### B — Foundations  *(engine red)*
- `core` allocation + **RAII ownership** (`01`): owning buffer handle vs non-owning
  span; `unique_ptr` + **stateless** deleter → mimalloc; shared identity/zero selection
  + `draken_zero_validity`.
- Base **vector** surface + the **dumb `Morsel`** container.
- **Logical-type descriptor** (interned/immutable/shared, borrowed ptr, **mandatory**
  for parameterized types) + **stats side-channel** (optional, "absent = don't know"),
  both **out-of-band keyed by column** — interfaces only.
- Ingestion primitives: `from_decoded` (ownership transfer), `from_sequence` (own-and-
  copy at the Python boundary), `from_scalar` (literal → constant vector).
- **Gate:** foundations compile; parity harness exercises them where applicable.

### C — Pilot type `int64` end-to-end  *(the template)*
- Full int64 op set, in dependency order: **`hash`** (joins/distinct/group-by depend
  on it) → `compare_*` → `between` → `in_list` (CarcharSet probe) → `sum/min/max` →
  `arithmetic` → `take` → `materialize` → `compress` → element access. Dispatch via the
  **table** (`02`); hand-written SIMD for the integer kernels (NEON/AVX2).
- Wire int64 through the logical-type/stats out-of-band path where relevant.
- **Implement + assert the two correctness rules HERE** (the pilot hits both):
  1. int64 × float64 **COMPARE = EXACT** (range-check then integer compare; no double
     promotion). Unit test must straddle 2^53. Arithmetic → float64 is the defined SQL
     result type.
  2. CarcharSet membership = **hash-only, accepted** (named §1 exception). Single shared
     hash path; no raw-key branch that can drift. Documented acceptance, not a testable
     invariant.
- **mimalloc cross-thread bench** (risk #2): allocate on one thread, free on another —
  the rugo-decode-then-execution-free pattern this ownership model maximises.
- **Per-type gate** (§3) passes for int64.
- **Then rewrite int64's compiled consumers** — the 45 `integer64_vector` cimports +
  its share of `core.buffers`/`vector`/`morsel` + Python imports — to the new surface
  ("function not signature"). Reviewed by diff + isolated compile; the engine still
  won't link until D completes, by design.
- **Exit = the proven template** for every remaining type. Resist starting other types
  until this gate template is real (risk #4).

### D — Remaining types, one at a time  *(engine red; each fully gated)*
Order (consumer traffic `07` + `08`): **string → bool → float64 → timestamp →
date32**, then the long tail **time → decimal → interval → vector(fp16) → array →
null**. Per type: all ops → per-type gate (§3) → rewrite that type's consumers.

Type-specific (from `06`):
- **string** — *second pilot, not a routine port*: first owned variable-length storage
  (German-string slots + arena, arena **travels with `data`**), dict codes + unique
  slots. Prove the RAII subtree here before `array` builds on it.
- **bool** — bit-packed (1 bit/value); bit-addressed accessor (validity's family).
- **decimal** — logical `DECIMAL(p,s)` → physical **int64** (p≤18) **and int128**
  (p≤38); int128 is scalar (no SIMD on NEON/AVX2); overflow promotion int64→int128 at
  the dispatch/promotion layer, not in the kernel.
- **timestamp** — int64 + logical unit/**fixed-offset** tz (no tzdata/CCTZ; named
  zones + DST out of v1).
- **array** — int32 offsets + child `DrakenVector`; parent owns child, RAII chains.
- **STRUCT/MAP** — string-backed JSON (`yyjson`), **extract-only**. Whole-value `=` /
  GROUP BY / DISTINCT / JOIN / ORDER BY raise **"unsupported" at bind time** (`06`) —
  must fail loudly, never silently hash JSON bytes.

### E — Re-green, harden, delete `draken_old`  *(big-bang integration)*
- All types native + all consumers rewritten ⇒ **the engine compiles for the first
  time.** Whole-engine gate (§3): `make q` 100%; `make clickbench` ≥ baseline.
- **C++ consumer ABI cleanup:** migrate C++ consumers (rugo, C++ ops) from `cimport` to
  `#include "draken/core/buffers.h"` where cleaner; keep the extern-`pxd` for remaining
  Cython consumers.
- Internalize/rename `var_vector`; re-home its one caller (`07`).
- **Delete `draken_old`**; final `make q` + clickbench.

## 6. Top risks / de-risk early (from `08`)

1. **ABI drift** — biggest hazard. Offset + sizeof + enum asserts in new *and* old,
   Phase A. A stale `buffers.pxd` is silent drift.
2. **mimalloc under cross-thread churn** — validate in C with the alloc-here/free-there
   bench; fallback (rejected) is the per-morsel arena.
3. **String/array/nested ownership** — the RAII subtree; get it right on the string
   pilot before array.
4. **Pilot scope creep** — keep C to int64 only until the gate template is proven.
5. **(new) Big-bang re-green at E** — long red period hides integration breakage. Use
   the §4 end-to-end harness to surface it per-type.

## 7. Definition of done (from `08`)
Shim concept never existed; `draken_old` deleted; `make q` 100%; `make clickbench` ≥
baseline; no `object` in compiled paths; no draken→opteryx deps; `buffers.h` is the
single ABI source of truth with size/offset/enum asserts.

## 8. Open items for the architect
- [ ] `08` still documents the withdrawn shim throughout. Annotate `08` with a
      "superseded by `09` §0" banner, or edit out the shim sections? (Not done without
      your call — `08` is part of the reviewed set.)
- [ ] Baseline capture on **x86** likely needs you (prod platform) — confirm who runs
      the last-green-commit clickbench on x86.
- [ ] C++ test framework (`04` open q): gtest / doctest / match rugo+carchar?
