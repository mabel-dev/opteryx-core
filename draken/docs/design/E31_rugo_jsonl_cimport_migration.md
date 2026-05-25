# E.31 — Rugo `jsonl_reader.pyx` + `_jsonl_reader.pyx` cimport migration

> **Status:** TODO.
>
> **Goal:** finish the rugo migration that E.28 started on
> `parquet_reader.pyx`. Migrate the two remaining rugo files
> (`jsonl_reader.pyx`, `_jsonl_reader.pyx`) off old-draken's typed-Vector
> subclass cimports to uniform `Vector` + `DrakenType` dispatch, using the
> same shape and stub-gap pattern E.28 established. Also fix E.28's
> outstanding `setup.py` allowlist gap.
>
> **Predecessor:** E.28 (parquet pilot — complete, 14 gap-stubs in place).
> Pattern is established; this ticket replicates it on two files.
>
> **Why this is small:** mechanical. E.28's parquet_reader migration is
> the worked example; jsonl files follow the same shape. The producer
> surface remains absent — gaps get stubbed with `NotImplementedError`
> and tagged with E.28-gap-N labels matching parquet's existing gap list.
> This ticket does not build the producer surface, does not add anything
> to draken, does not extend the bridge.

---

## 1. What's being delivered

1. **`setup.py` allowlist fix.** E.28 said it would add `rugo.*` to the
   `DRAKEN_BUILD=1` extension allowlist alongside `draken.*`,
   `opteryx.compiled.nanobind.*`, and `opteryx.expression.evaluator.*`.
   Today the line at setup.py:2252-2254 still doesn't include rugo. Add
   it.

2. **`rugo/src/jsonl/jsonl_reader.pyx` migration.** Replace these
   cimports:
   ```cython
   from draken.vectors.integer64_vector cimport Integer64Vector
   from draken.vectors.float64_vector cimport Float64Vector
   from draken.vectors.bool_vector cimport BoolVector
   from draken.vectors.string_vector cimport StringVector, StringVectorBuilder
   from draken.vectors.array_vector cimport ArrayVector, from_sequence as array_from_sequence
   ```
   with:
   ```cython
   from draken.vectors.vector cimport Vector
   from draken.morsels.morsel cimport Morsel
   ```
   Plus `from draken.core.buffers cimport DrakenType` (and the per-type
   enum members) if runtime type checks are needed.

3. **`rugo/src/_jsonl/_jsonl_reader.pyx` migration.** Same as #2.

4. **Producer-side helper handling — same as E.28.** For each
   producer-side helper this file uses (`StringVectorBuilder`,
   `bool_vector_from_bits`, `array_from_sequence`, `array_vector_from_parts`,
   `*_from_decoded`, etc.):
   - If an existing draken bridge function covers it
     (`draken_vector_unwrap`, `draken_vector_own_raw` with `DRAKEN_BOOL`
     etc.), migrate the call inline.
   - If no equivalent exists, **stub the call site** with:
     ```cython
     raise NotImplementedError(
         "rugo migration gap: <helper_name> has no new-draken equivalent; "
         "tracked as E.28-gap-N."
     )
     ```
     Reuse the E.28 gap numbering where the same helper appears in
     parquet_reader (e.g. `StringVectorBuilder` is E.28-gap-3,
     `bool_vector_from_bits` is E.28-gap-9). If a helper appears in jsonl
     that didn't appear in parquet, assign a new gap number (E.31-gap-1,
     E.31-gap-2, …) and document in §6 reporting.

## 2. What is explicitly NOT in scope

- Filling any of the E.28 gaps. Producer-surface primitives are a
  separate decision (E.29 / E.30 territory) — not this ticket.
- Adding anything to `draken/`. No `.h`, no `.cpp`, no extension to
  `draken_bridge.h`, no new function in `draken_native.cpp`. The C++
  side is closed for this ticket.
- Refactoring rugo's JSONL decoding logic. The migration is
  import-shape + helper-call-shape only — same as E.28 did for parquet.
- The collector-internals restructure (`_collectors_*.pxi`) — that's
  E.30b / operator-PM work.
- Anything touching `opteryx/`. If a caller of rugo breaks, surface
  it; don't fix it here.

## 3. STOP conditions

- File count >4: `setup.py` (the allowlist line), `jsonl_reader.pyx`,
  `_jsonl_reader.pyx`, and at most one cleanup. Past 4 → drifting.
- You find yourself editing `draken/draken_native.cpp`,
  `draken/core/draken_bridge.h`, anything in `draken/ops/`, or
  `draken/core/buffers.{h,pxd}`. **STOP.** Surface as a follow-up.
  The lesson from E.24 is *do not add producer-side primitives mid-migration*.
- The `DRAKEN_BUILD=1` allowlist change breaks `make draken` for an
  unrelated reason. Surface and stop.
- `make dt` regresses below 2801 passing.
- More than ~12 distinct gap stubs across the two jsonl files. That's a
  signal to surface to the architect — the producer-surface absence may
  have a different shape on JSONL than on parquet.

## 4. Discipline reminders

- **Pilot-as-survey ≠ implementation.** Stub gaps, do not fill them.
  E.24 was reverted for filling gaps mid-migration. Same rule.
- **No fake-green.** If a fix doesn't work, the build is allowed to be
  red at the call site (it'll raise `NotImplementedError` at runtime).
  Don't add a fallback that returns wrong data to keep something
  passing.
- **No `object` parameters/returns in compiled Cython.** Per CLAUDE.md §3.
  If a producer-side helper had a Python-shape signature in old draken
  and the migration suggests typing it as `object` to fit, **stop and
  surface** — same anti-pattern as `vector_lowercase.pyx`'s
  `cdef object builder = ...`.
- **No git commands.**
- **Encoding shape is NOT type.** If you see code branching on
  `vec.type == DRAKEN_DICTIONARY` or `DRAKEN_CONSTANT`, those values
  no longer exist (E.30c). Replace with layout-based shape inference
  (`vec.data_length < vec.length`, `vec.data_length == 1`, etc.) — read
  `draken-encoding-shape-is-layout-not-type` memory file if unclear.

## 5. Acceptance criteria

Run and report verbatim:

1. `grep -E "rugo\." setup.py | grep startswith` — shows the rugo line
   in the `DRAKEN_BUILD=1` allowlist.
2. `make draken 2>&1 | tail -10` — build completes; both
   `rugo/jsonl_reader.cpython-313-darwin.so` and
   `rugo/_jsonl/_jsonl_reader.cpython-313-darwin.so` appear in the
   copying lines.
3. `python -c "from rugo.jsonl_reader import *; from rugo._jsonl._jsonl_reader import *"` — imports cleanly (the
   NotImplementedError stubs only fire at call time, not at import).
4. `make dt 2>&1 | tail -3` — still ≥2801 passing.
5. `grep -c "from draken.vectors.integer64_vector\|from draken.vectors.float64_vector\|from draken.vectors.string_vector\|from draken.vectors.array_vector\|from draken.vectors.bool_vector cimport" rugo/src/jsonl/jsonl_reader.pyx rugo/src/_jsonl/_jsonl_reader.pyx` — both files: 0 (no typed-Vector cimports remain).
6. `grep -c "NotImplementedError.*rugo migration gap" rugo/src/jsonl/jsonl_reader.pyx rugo/src/_jsonl/_jsonl_reader.pyx` — reports stub counts for each file.
7. `git diff --stat HEAD` — files changed ≤4.

## 6. Reporting back

- The seven acceptance outputs.
- A list of any new gaps surfaced that don't already have an E.28 gap
  number — assign E.31-gap-N and describe the helper that's missing.
- Confirmation that no new code was added to `draken/` C++ or the
  bridge.
- Confirmation that no zombie `.so` files were touched. (After the
  zombie sweep, only 6 sanctioned `.so` files should exist in `draken/`:
  `_abi_guard`, `_mimalloc_smoke`, `draken_native`, `vectors/vector`,
  `vectors/bool_vector`, `morsels/morsel`. Verify post-build.)
- One paragraph: what's the runtime state of rugo after this ticket?
  Imports clean, files build, but reading any actual file will raise
  `NotImplementedError` until producer-surface primitives are in. That's
  the expected state — surface it explicitly so the next PM knows.

## 7. After this lands

The three rugo files are migrated to uniform Vector at the
cimport-shape level. Rugo is no longer compile-broken. Reading actual
data files still doesn't work — that requires producer-surface
primitives (E.28 gap-1 through -9, plus any E.31 gaps). Those are
follow-up draken-PM work *if* the architect chooses to do them; per the
broader project state, they may fold into eval-PM's producer-surface
ownership instead.

This ticket's deliverable is: **rugo compiles cleanly with the rest of
draken, and the producer-surface gap is documented and stubbed
consistently across all three rugo files.**
