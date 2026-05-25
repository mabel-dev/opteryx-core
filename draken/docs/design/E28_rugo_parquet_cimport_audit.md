# E.28 — Rugo `parquet_reader.pyx` cimport audit + minimal migration (rugo cluster pilot)

> **Status:** TODO.
>
> **Goal:** unblock `rugo/src/parquet/parquet_reader.pyx`'s typed-Vector
> cimports the same way `_impl.so` was unblocked — by routing through the
> E.24 shim layer (`Vector`/`BoolVector`/`Morsel`) — AND produce an
> **honest gap list** of producer-side draken helpers that rugo depends on
> but new draken does not yet expose (`StringVectorBuilder`,
> `bool_vector_from_bits`, `array_vector_from_parts`, possibly others).
>
> This is a **pilot-as-survey**, not a full port. The goal is to surface
> the rugo-specific gaps cleanly so each can be its own scoped follow-up
> ticket. Surface, don't fill.
>
> **Why rugo is different from eval-PM's migration:** rugo is on the
> **producer** side of the vector ABI (it builds vectors from parquet
> bytes and feeds them into the engine), whereas the evaluator is on the
> **consumer** side. The producer surface uses helpers (string-arena
> builders, bitmap-to-vector wrappers, array constructors) that haven't
> been ported because no consumer-rewrite phase exercised them.

---

## 1. The 3-file rugo cluster

For context — not in scope for this ticket:

| File | Status |
|---|---|
| `rugo/src/parquet/parquet_reader.pyx` | **This ticket.** Largest, most representative. Pilot for the cluster. |
| `rugo/src/jsonl/jsonl_reader.pyx` | Follow-up ticket. Will follow this one's pattern. |
| `rugo/src/_jsonl/_jsonl_reader.pyx` | Follow-up. Underscore-private variant of jsonl_reader. |

After this ticket lands, those two get their own tickets, each modelled on
the pattern you establish.

## 2. What's being delivered

### 2.1 Setup-side change

Add `rugo.*` to the `DRAKEN_BUILD=1` extension allowlist in `setup.py`
(around L2252–2254, alongside `draken.*`, `opteryx.compiled.nanobind.*`,
and `opteryx.expression.evaluator.*`). This makes the rugo migration
testable in isolation, the same way the evaluator now is.

Pre-existing rugo build flags / vendor paths (zstd, lz4) must continue to
work under `DRAKEN_BUILD=1`. If they break, the right fix is conditional
inclusion at the setup.py level — surface and stop, don't paper over.

### 2.2 Migration-side change to `parquet_reader.pyx`

Replace the typed-Vector cimports:

```cython
from draken.vectors.integer64_vector cimport (...)
from draken.vectors.float64_vector cimport (...)
from draken.vectors.string_vector cimport (StringVector, StringVectorBuilder, ...)
from draken.vectors.bool_vector cimport BoolVector, bool_vector_from_bits
from draken.vectors.array_vector cimport ArrayVector, array_vector_from_parts
from draken.vectors.vector cimport Vector
```

with the uniform single import:

```cython
from draken.vectors.vector cimport Vector
from draken.morsels.morsel cimport Morsel
```

For each typed-Vector reference inside the file:

- **Variable/parameter typing:** `cdef Integer64Vector v` → `cdef Vector v`.
  Runtime type discrimination via `vec.type == DRAKEN_INT64`
  (`DrakenType` is cimported from `draken.core.buffers`).
- **Producer-side helper calls** (`StringVectorBuilder(...)`,
  `bool_vector_from_bits(...)`, `array_vector_from_parts(...)`,
  `string_vector_module.StringVectorBuilder.with_estimate(...)`, etc.) —
  see §2.3.

### 2.3 Producer-side helper handling — the heart of this ticket

For each producer-side helper rugo uses, do the following audit:

1. **Search the post-E.25 draken surface** (`draken/draken_native.cpp`,
   `draken/core/draken_bridge.h`, `opteryx/compiled/nanobind/`) for a
   functional equivalent.
2. If an equivalent exists, migrate the call. (Likely candidates:
   `bool_vector_from_bits` → `draken_vector_own_raw` with `DRAKEN_BOOL`;
   the bridge function already takes the right shape.)
3. If no equivalent exists, **stub the call site** with:
   ```cython
   raise NotImplementedError(
       "rugo migration gap: <helper_name> has no new-draken equivalent; "
       "tracked as E.28-gap-N."
   )
   ```
   and record the gap in §6 reporting.

**You do not implement the missing primitives.** That is a separate
ticket per gap, scoped after you surface the list.

## 3. What is explicitly NOT in scope

- The other two rugo files (`jsonl_reader.pyx`, `_jsonl_reader.pyx`).
- Implementing any new producer-side primitive in draken
  (`draken_native.cpp`, `draken/core/draken_bridge.h`, `draken/ops/`). The
  C++ side is closed for this ticket.
- Extending the E.24 shim with new methods. The shim is `cdef class Vector`
  with a fixed surface; if your migration needs something the shim
  doesn't expose, surface it as a gap.
- Refactoring rugo's parquet decoding logic. The migration is import-shape
  + helper-call-shape only. Don't optimise, don't reorder, don't
  "while you're here" anything.
- Touching the generated `parquet_reader.cpp` (Cython output). It will
  regenerate when the `.pyx` is recompiled.
- The non-`.pyx` parquet code (vendored zstd / lz4 / parquet-thrift). No
  reason to change them.

## 4. STOP conditions

Trip any of these → stop, surface, do not fix.

- File count >4: `setup.py` (the allowlist line), `parquet_reader.pyx`
  (the migration), the gap-list note (likely in §6 of this doc or a
  scratch file), and at most one orphan-cleanup. Past 4 → drifting.
- You find yourself editing `draken/draken_native.cpp` or
  `draken/core/draken_bridge.h` or anything in `draken/ops/`. **STOP.** The
  whole point of this ticket is to surface that draken doesn't have these
  primitives. Adding them is not your call.
- You find yourself editing `opteryx/compiled/nanobind/*.cpp`. Same
  reasoning. The rugo migration uses what's there; what's missing is a
  follow-up ticket each.
- The `DRAKEN_BUILD=1` allowlist change breaks `make draken`. The
  pre-rugo-add state of `make draken` must continue to work. If adding
  rugo breaks it, surface and stop.
- `make dt` regresses below 2792.
- More than 5 distinct producer-side gaps surface. That's not a stop
  condition for completion, but it's a signal to surface to the architect
  before continuing — the rugo migration may be larger than this ticket
  framing assumes.

## 5. Discipline reminders

- **Pilot-as-survey ≠ implementation.** Your job is to surface the gaps,
  not fill them. Adding `bool_vector_from_bits` back to `draken_native.cpp`
  to "make it compile" is the E.24 anti-pattern (the architect-reverted
  function was exactly this).
- **No fake-green.** If the migration leaves the file with 4
  `NotImplementedError` raises, that is the correct outcome. The file
  compiles cleanly; the runtime gaps are explicit and addressable. Do
  NOT add a fallback that returns an empty vector or a wrong-shape
  vector to keep something passing.
- **No `object` parameters/returns.** Per CLAUDE.md §3. The migration is
  to uniform `Vector` typed at C level.
- **No git commands.**

## 6. Acceptance criteria

Run and report verbatim:

1. `grep -E "rugo|nanobind|draken|evaluator" setup.py | grep startswith` —
   shows `rugo.` in the allowlist alongside the others.
2. `DRAKEN_BUILD=1 python setup.py build_ext --inplace -j 4 2>&1 | tail -10` —
   build completes; `rugo/parquet_reader.cpython-313-darwin.so` appears
   in the copying lines.
3. `python -c "from rugo.parquet_reader import *"` — imports cleanly.
4. `make dt 2>&1 | tail -3` — still ≥2792 passing.
5. `grep -c "from draken.vectors.integer64_vector\|from
   draken.vectors.float64_vector\|from draken.vectors.string_vector\|
   from draken.vectors.array_vector" rugo/src/parquet/parquet_reader.pyx` —
   should be 0 (no typed-Vector cimports remain).
6. `grep -c "NotImplementedError.*rugo migration gap" rugo/src/parquet/parquet_reader.pyx` —
   reports the count of surfaced gaps.

## 7. Reporting back

The acceptance outputs above, plus a **gap list** in this exact shape:

```
| # | Old helper used by rugo | New draken equivalent (if any) | Recommendation |
|---|------------------------|--------------------------------|----------------|
| 1 | StringVectorBuilder.with_estimate(...) | <findings> | <new draken primitive / rugo rewrite / out-of-scope-feature> |
| 2 | bool_vector_from_bits(bits, nulls, n) | draken_vector_own_raw(..., DRAKEN_BOOL) | migrate inline |
| 3 | array_vector_from_parts(...) | <findings> | ... |
| ... | | | |
```

Plus:

- A note on whether the `DRAKEN_BUILD=1` allowlist addition for rugo
  exposed any unexpected interactions with the vendored zstd/lz4 paths.
- Confirmation that no new code was added to `draken/` C++ or the
  bridge.
- Recommendation: which gaps look like draken-side primitives the
  draken-PM should fill, and which look like rugo-side rewrites
  (e.g. "rugo could build strings via the existing
  `draken_vector_own_string` bridge by writing into the arena
  directly — no new primitive needed").

This gap list is the deliverable that defines the next 1–3 tickets after
this one. Make it precise.

## 8. After this lands

Each surfaced gap becomes its own micro-ticket. Most should be ~1 hour
of agent work each. The rugo cluster is then ready for the two follow-up
files (`jsonl_reader.pyx`, `_jsonl_reader.pyx`) to follow the same
pattern.
