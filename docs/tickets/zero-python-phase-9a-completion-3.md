# Ticket: Zero-Python Phase 9a-completion-3 — make the parity test actually pass

> **Third** corrective ticket for 9a. Rounds 1 and 2 got the kernels to
> compile and the registry to populate — but the C ABI parity test, the
> one gate that proves the kernels *work*, was never run to green. When
> finally run (by the reviewer, this round), it **aborts on the first
> real kernel**. The kernels compile but do not function.
>
> This ticket is narrow and blunt: **make `make kernel-parity` link,
> run, and pass — every case. Paste the green output.** Nothing is
> "done" until that exists.

## What round 2 delivered vs what's still broken

Verified 2026-05-28:

**Delivered (real progress):**
- Registry map populated — cast (31), binary_op, extraction kernels.
- `_kernel_registry.lookup_kernel(...)` returns non-null pointers.
- `make c` clean; `make q` 137/137.
- A `kernel-parity` Makefile target exists.

**Still broken:**
1. **`make kernel-parity` does not link.** The target compiles the
   kernel `.cpp`s + `c_abi_test.cpp` but **omits mimalloc**, so
   `draken_malloc` → `_mi_malloc` / `_mi_usable_size` are undefined:
   ```
   Undefined symbols for architecture arm64:
     "_mi_malloc", referenced from: draken_malloc(...)
   make: *** [kernel-parity] Error 1
   ```
   The target has never successfully built. The
   `print_green "✓ parity test passed"` line after it is therefore a
   lie waiting to happen — it only doesn't fire because make aborts at
   the link step.
2. **When linked (mimalloc's `src/static.c` added), the test ABORTS.**
   ```
   Testing error message slot...    ✓
   Testing error sentinel fmt...     ✓
   Testing draken_add...
   === TEST EXIT CODE: 134 ===       (SIGABRT)
   ```
   The two error-slot cases pass; the **first real kernel test
   (`draken_add`) aborts with SIGABRT.** No arithmetic, cast,
   extraction, or any value-producing kernel is proven correct. At
   least `draken_add` is broken when invoked.

This is the third consecutive report of "9a complete" with the kernels
non-functional. The through-line every time: the parity test was not
run. Now that it runs, it fails on kernel #1.

## Goal

`make kernel-parity` links, runs, and **passes every case** — error
slots + every cast / binary_op / extraction kernel. The pasted green
output is the deliverable. (Function kernels remain carved out to
9a-fn per the prior architect decision; the parity test must not
depend on them.)

## Scope

**In scope**
1. **Fix the `kernel-parity` Makefile target to link mimalloc.**
   mimalloc is a single-TU static build:
   `third_party/mimalloc/src/static.c` (compiled `-x c -std=c11`,
   include `third_party/mimalloc/include`). Add it to the link line
   (compile to an object and link, or add the `.c` to the clang
   invocation). Model on how `setup.py` links `MIMALLOC_OBJ`
   (setup.py:99-105).
2. **Debug the `draken_add` SIGABRT and fix it.** Exit 134 = SIGABRT —
   likely an uncaught C++ exception hitting the `DRAKEN_KERNEL_TRY`
   `catch`, a failed assertion, a `draken_malloc` precondition, or the
   test harness building a malformed input `DrakenVector` (e.g.
   `selection` not set — the kernel does `left_data[left->selection[i]]`
   per the uniform §11 access pattern, which faults if `selection` is
   null). Determine whether the bug is in the **kernel** or the **test
   harness** and fix the actual cause:
   - If the kernel mis-handles a shape (constant / dict / dense) → fix
     the kernel (must satisfy CLAUDE.md §11 uniform `data[selection[i]]`).
   - If the test builds invalid `DrakenVector`s → fix the test.
3. **Get the entire parity test green** — not just `draken_add`.
   Whatever fails after it, fix too. The test exists to find exactly
   these; run it, read the failure, fix, repeat until every case
   passes.
4. **Confirm `result.validity` handling.** `draken_add` has a
   `// TODO: merge validity bitmaps from inputs` with
   `result.validity = nullptr`. A kernel that drops input nulls is a
   correctness bug. Either implement validity merging or, if the parity
   test doesn't yet cover nullable inputs, **add a nullable-input case**
   so the gap is caught. Surface in PR.

**Out of scope**
- Function kernels (`function_*.cpp`) — 9a-fn.
- 9b/9c — blocked until this is green.

## Verification — the only gate that matters

```
make kernel-parity
```
must:
- link (no undefined symbols),
- run to completion (exit 0, not 134),
- print PASS for **every** case.

**Paste the full output in the PR.** A run that aborts partway, or a
target that fails to link, is not acceptance. "It compiles" and "the
registry resolves" were already true in round 2 and the kernels still
didn't work — those are necessary, not sufficient.

Also:
- `make c` clean; `make q` 100/100; `make clickbench` non-regressing
  (unchanged — kernels still not on the executor path).

## Constraints (CLAUDE.md)

- **Broken but honest beats green but fake.** This is the third ticket
  because that line was crossed three times. Do not report completion
  without the pasted green parity output. If the test still fails on
  some kernel you can't fix, **report that honestly** — a partial-pass
  with a named failing kernel is infinitely more useful than a false
  "done".
- **§11 uniform access** — kernels must produce correct results for
  dense / constant / dict shapes via `data[selection[i]]`. If
  `draken_add` aborts because it assumes a shape, that's the bug.
- **Fail fast** — the error-sentinel path stays tested.
- **`make c` clean before done.**
- **Do not commit.**

## Pre-flight reading

1. This ticket + the two prior completion tickets.
2. `draken/ops/kernels/c_abi_test.cpp` — the `draken_add` test case;
   how it builds input `DrakenVector`s (check `selection` is set).
3. `draken/ops/kernels/binary_op_arithmetic.cpp:30` — `draken_add`.
4. `draken/ops/kernels/error_handling.h` — `DRAKEN_KERNEL_TRY` (what
   it does on a caught exception — does it abort?).
5. `setup.py:99-105` — the mimalloc object build, to mirror in the
   Makefile target.
6. `draken/core/buffers.h` §11 — DrakenVector shapes; `selection` is
   never NULL for a valid vector.

## Definition of done

- `kernel-parity` Makefile target links mimalloc; builds clean.
- `make kernel-parity` runs to exit 0 with **every** case PASS;
  full output pasted in PR.
- `draken_add`'s SIGABRT root-caused (kernel or harness) and fixed;
  PR states which it was.
- Validity-handling gap addressed (merge implemented, or nullable
  case added to the test and passing).
- `make c` clean; `make q` 100/100; `make clickbench` non-regressing.

## Process note (for whoever assigns this)

Three rounds of fake completion on one deliverable is a pattern, not
bad luck. The fix is procedural: **for this ticket, acceptance =
pasted `make kernel-parity` green output, nothing less.** A report
that says "done" without that output should be bounced without further
review. The build being green proves nothing here — the kernels aren't
on any executor path yet, so `make c` / `make q` are green regardless
of whether the kernels work.
