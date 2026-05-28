# Ticket: Zero-Python Phase 9a-completion-4 — parity-test every registered kernel

> Fourth and final 9a ticket. Round 3 got `make kernel-parity` to link,
> run, and pass — the C ABI mechanism is **proven** (registry lookup,
> context passing, error sentinel, VecResult all green). But the test
> exercises only **4 of 48 registered kernels**. `draken_add` itself
> aborted until round 3, so "compiles but broken" is a demonstrated
> failure mode for the 44 untested kernels — not hypothetical.
>
> Architect decision (2026-05-28): **expand parity coverage to all
> registered kernels before 9b starts.** The harness works now; this is
> mechanical.

## What's accepted (do not redo)

- The C ABI mechanism is done and verified. The registry, `lookup_kernel`,
  context allocation, `DRAKEN_KERNEL_TRY` error path, and `VecResult`
  handling all work — round 3's green parity run proves it.
- The `kernel-parity` Makefile target links (mimalloc wired) and runs.
- cast / binary_op / extraction kernels are registered (48 entries).

**This ticket does not touch the mechanism.** It only widens test
coverage. Function kernels remain carved out to 9a-fn.

## The gap (verified 2026-05-28)

- `grep -c "kernel_fn_t)&" kernel_registry.cpp` → **48** registered.
- `c_abi_test.cpp` exercises: `draken_add`, `draken_multiply`,
  `draken_cast_identity`, `draken_cast_to_float` (+ error/context
  infra). **~4 real kernels.**
- 31 cast kernels registered; ~2 tested. Bitwise/shift ops, extraction
  kernels: essentially untested.
- `draken_add` aborted (SIGABRT) until round 3 — a registered,
  "complete" kernel that didn't work. The 44 untested kernels are in
  exactly that unverified state now.

## Goal

`make kernel-parity` exercises **every registered kernel** (all 48
in-scope cast / binary_op / extraction entries) with a parity
assertion — C-function output vs the kernel's nanobind binding output
on representative inputs — plus error and nullable-input cases. All
PASS. Output pasted in PR, listing each kernel covered.

## Scope

**In scope**
1. **A parity case for every registry entry.** For each of the 48
   `{"name", &fn}` entries in `kernel_registry.cpp`, add a
   `c_abi_test.cpp` case that:
   - Builds representative input `DrakenVector`(s) for the kernel's
     input type(s).
   - Calls the C kernel via the registered function pointer (or
     directly).
   - Calls the corresponding **nanobind binding** with equivalent
     input.
   - Asserts the two outputs match (data + validity + type + length).
   - For parameterised kernels (cast-timestamp unit, binary op_code),
     allocate the context via `kernel_alloc_*_ctx` and pass it.
2. **Coverage enforcement.** Add a mechanism so the test can't silently
   drift below 100% of registered kernels. Options (pick one, surface
   in PR):
   - The test iterates the registry and asserts every entry has a
     corresponding executed case (fail if a registered kernel has no
     test).
   - Or a static assertion / count check: `tested_count == registry_size`.
3. **Shape coverage per §11.** For at least the cast and binary_op
   kernels, test **dense AND constant-encoded** inputs (the uniform
   `data[selection[i]]` access must be correct for both). `draken_add`'s
   abort was the kind of shape/selection bug this catches.
4. **Nullable inputs.** Round 3 flagged `draken_add`'s
   `// TODO: merge validity bitmaps` (`result.validity = nullptr`).
   Add nullable-input cases for arithmetic/bitwise kernels and assert
   the output validity is the correct merge of input validities. If a
   kernel drops nulls, that's a bug to fix here (it's a correctness
   defect, in scope).

**Out of scope**
- Function kernels — 9a-fn.
- The C ABI mechanism — accepted, untouched.
- 9b/9c — unblocked when this lands.

## Verification — the gate

```
make kernel-parity
```
- links, runs to exit 0,
- prints PASS for **every registered kernel** (48), plus error +
  nullable + shape cases,
- the coverage-enforcement check confirms `tested == registered`.

**Paste the full output in the PR**, and a checklist mapping each of
the 48 registry entries to its passing test case. A run that covers
fewer than all 48 is not acceptance — that's the entire point of this
ticket.

Also: `make c` clean; `make q` 100/100; `make clickbench` non-regressing.

## If a kernel fails parity

Expected — that's why this ticket exists. When a kernel's C output
differs from its nanobind binding (or it aborts/crashes):
- Root-cause: kernel bug, or test building bad input.
- Fix the actual cause (a kernel that returns wrong data is a
  correctness defect, fix it here).
- Report every kernel that failed and what the bug was, in the PR.
  That list is the value of this ticket — it's the set of broken
  kernels the prior "complete" reports shipped.

## Constraints (CLAUDE.md)

- **Broken but honest beats green but fake.** Four rounds in; the only
  acceptable completion is pasted output showing all 48 kernels PASS.
  If some can't be made to pass, report which and why — do not trim
  the test to make it green.
- **§11 uniform access** — kernels correct for dense + constant shapes.
- **Fail fast** — error-sentinel + nullable paths tested, not assumed.
- **`make c` clean before done.**
- **Do not commit.**

## Pre-flight reading

1. This ticket + completion tickets 1–3.
2. `draken/ops/kernels/kernel_registry.cpp` — the 48 entries to cover.
3. `draken/ops/kernels/c_abi_test.cpp` — the existing 4-kernel harness
   to extend.
4. The nanobind bindings for each kernel family
   (`opteryx/compiled/nanobind/vector_casts.cpp`, `vector_bitwise.cpp`,
   `vector_json.cpp`, `vector_special.cpp`) — the parity reference.
5. `draken/core/buffers.h` §11 — DrakenVector shapes for building
   dense + constant test inputs.

## Definition of done

- Every registered kernel (48) has a parity case in `c_abi_test.cpp`.
- Coverage-enforcement check confirms `tested == registered`.
- Dense + constant shape cases for cast/binary_op kernels.
- Nullable-input cases; validity-merge bugs fixed.
- `make kernel-parity` green for all 48 + infra cases — full output +
  per-kernel checklist in PR.
- PR lists every kernel that failed parity during development and the
  bug found (the audit value of this ticket).
- `make c` clean; `make q` 100/100; `make clickbench` non-regressing.

## After this lands

9a is genuinely complete — every registered kernel proven correct
against its nanobind reference. 9b (BytecodeInstr struct + bind-time
wiring) is unblocked. The key-naming scheme documented in
completion-2 is the contract 9b's `lookup_kernel` calls must match.

9a-fn (the 9 function kernel files) remains queued, must land before
9e.
