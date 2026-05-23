# Draken Testing & Bring-up (DRAFT)

> Status: DRAFT. The data layer is the foundation — it gets its own isolated tests
> and benchmarks, not just coverage-by-accident through SQL. Note: the old draken
> unit suite was heavily PyArrow-fixture-based and was largely deleted; the new
> suite is native from day one.

## Test layers

1. **Native unit tests (no PyArrow).** Build vectors from typed sequences / raw
   buffers (`vector_from_sequence`, `from_decoded`-style), never `from_arrow`.
   Cover the matrix:
   - **type** × **op** × **shape** (dense / constant / dict) × **nullability**
     (no nulls / some nulls / all null) × **size** (0, 1, <8 tail, large).
   - Edge values per type (INT64_MIN, NaN/-0.0 for float, empty string, etc.).
2. **C++-level tests** for the kernels/dispatch directly (gtest or the existing
   C++ test pattern), so correctness isn't gated on the binding.
3. **Parity tests (the safety net during rebuild).** For each ported op, assert
   new `draken` == `draken_old` over randomized inputs across the matrix above.
   This is how we trust the inversion without re-deriving every edge case by hand.
   <!-- /opus/ Confirmed draken_old is present on disk, so this oracle is real and
   this is the right strategy — the strongest part of the bring-up plan. Two caveats:
   (1) parity can only certify behaviour draken_old ITSELF gets right; known/latent
   draken_old bugs (e.g. the all-null 1-byte-sentinel bug 00 mentions, the dict
   fast-path skips in the memory notes) will be "matched" and carried forward unless
   the native unit tests in layer 1 independently assert the CORRECT answer. Don't let
   parity become bug-compatibility. (2) Parity must run LIVE, not against snapshots,
   for the randomized inputs to mean anything (see open question below — answer: live). -->

4. **SQL regression = the integration oracle.** `make q` (and the broader query
   tests) must stay 100% as ops are ported behind the shim. This is the
   end-to-end proof the engine still behaves.

## Benchmarks

Per-op microbenchmarks (hash, compare, reductions, in_list, take) on the dense /
dict / constant shapes, both nullable and not — so a port is accepted only if it
matches or beats `draken_old`. Wire into the existing `make clickbench` for the
macro view. Target platforms: ARM/NEON (dev), x86/AVX2 (prod).

## Bring-up sequence (gated, **per type** — see `03_binding.md`)

The shim is per **vector type**: a type is either fully on the new layer or fully on
`draken_old`. So we bring up **one type at a time, all its ops**, not one op across
all types.

1. Stand up the data model + ownership + the nanobind handle + the `draken_old`
   fallthrough shim. Engine runs entirely on `draken_old`.
2. **Pilot type: `int64`** (most representative op coverage). Implement its full op
   set — `hash` first within the type (joins/distinct/group-by depend on it), then
   `compare_*`, `reductions`, `in_list`, `take`/`materialize`, `compress`, element
   access. A/B each op vs `draken_old` across the type×op×shape×null matrix;
   benchmark; then remove **`int64`** from the shim. `make q` green.
3. Bring up the remaining types one at a time (e.g. `bool`, `string`, `float64`,
   the temporals, `decimal`, …), same full-op gate each, de-shimming the type when
   it passes. `make q` green after each type.
4. When the shim is empty: drop `draken_old` from the build, run full regression +
   clickbench, then delete `draken_old`.

Note: because `Morsel` is a dumb container and the shim is per type, a mixed-type
morsel can have some columns on the new layer and some on `draken_old`
simultaneously — each vector dispatches by its own type. That's what makes per-type
bring-up safe.

## Gates (every ported **type**, before it leaves the shim)

- [ ] Native unit tests pass (full matrix).
- [ ] Parity vs `draken_old` over randomized inputs.
- [ ] Benchmark ≥ `draken_old`.
- [ ] `make q` 100%.
- [ ] No `object` in compiled paths; no upward (`import opteryx`) deps.

<!--
/opus/ Two additions to the gate list, both prerequisites this set of docs implies but
the gate omits:
 - [ ] Compiled consumers re-bind to the new draken (header/extern-pxd) and the full
       build links — a type isn't really "off the shim" until the 99 cimport sites
       compile against it (see 03 /opus/). The Python A/B can pass while the C-level
       integration is still pointing at draken_old.
 - [ ] Struct ABI unchanged vs the frozen baseline (or, if changed, ALL consumers +
       draken_old rebuilt together). Guards the mixed-morsel safety property.
Also: "Benchmark ≥ draken_old" as a HARD gate can stall bring-up — a correct new kernel
may be temporarily 5% slower before its SIMD path lands. Suggest "≥ draken_old OR a
recorded, time-boxed perf-debt waiver signed by the architect," so correctness can ship
and perf is tracked, rather than blocking a green type on a micro-regression.
-->


## Open questions

- [ ] C++ test framework: gtest, doctest, or match whatever rugo/carchar use?
- [ ] Property-based/randomized harness for parity — home-grown or hypothesis at
      the Python edge?
- [ ] Do we snapshot `draken_old` outputs as golden files, or run it live in parity
      tests (live is safer but slower)?
- [ ] Coverage bar before a type is allowed off the shim.
