# Ticket: Zero-Python Phase 9a-completion — make the C kernel ABI actually compile and run

> Corrective ticket. Phase 9a
> (`docs/tickets/zero-python-phase-9a-draken-c-kernel-abi.md`) was
> reported complete but its deliverable is **uncompiled and unverified**.
> ~28 source files exist under `draken/ops/kernels/`; none are in the
> build; the code does not compile; the parity test never ran.
>
> This ticket makes 9a real. **9b is blocked until this lands.**

## What's actually wrong (verified 2026-05-28)

1. **No kernel sources are in `setup.py`.** `grep kernels/ setup.py` →
   nothing. None of `draken/ops/kernels/*.cpp` or `_kernel_registry.pyx`
   is compiled.
2. **`_kernel_registry` does not import.**
   `from draken.ops.kernels import _kernel_registry` → `ImportError`.
   There is no compiled `.so`.
3. **The code does not compile.** A manual compile of
   `cast_numeric.cpp` with the correct include paths fails:
   ```
   error: no type named 'exception' in namespace 'std'
     — error_handling.h:109, macro DRAKEN_KERNEL_TRY: catch (const std::exception& e)
   ```
   `error_handling.h` uses `std::exception` without `#include <exception>`.
   **This is the first error; expect more behind it** — nothing in this
   tree has ever been through a compiler.
4. **`c_kernel_abi.h` has a dangling `#endif`** (line 39) with
   `#pragma once` and no matching `#ifndef`. Any TU that includes it
   fails. Unnoticed because zero TUs include it.
5. **The C ABI parity test never ran.** `c_abi_test.cpp` (14 KB) is not
   in the build. 9a's correctness proof — C function output vs nanobind
   binding output — has never executed. We have **zero evidence any
   kernel produces a correct result.**

`make c` is clean and `make q` is 137/137 **only because the entire
9a tree is inert** — uncompiled, unimported, on no execution path. This
is a green-but-fake state (CLAUDE.md §0): the build is green because the
deliverable isn't built.

## Goal

Turn the inert 9a tree into compiled, tested, importable code:
- All kernel sources compile as part of `make c`.
- `_kernel_registry` imports from Python and `lookup_kernel(...)`
  returns valid pointers.
- The C ABI parity test compiles, runs, and passes for every kernel.
- `make c` clean **with the kernel code compiled in** — the real build
  gate, not the trivial inert one.

No new design. The locked Phase 9 decisions stand. This is purely
"make what 9a produced actually work."

## Scope

**In scope**
1. **`setup.py`** — register the kernel build:
   - The `.cpp` kernel sources (`cast_*.cpp`, `binary_op_*.cpp`,
     `extraction.cpp`, `function_*.cpp`, `cast_dispatch.cpp`,
     `kernel_registry.cpp`, `error_handling.cpp`) compiled into the
     draken extension (or wherever the executor will link them from —
     decide and document; they must be reachable from
     `opteryx/expression/evaluator/_impl` at link time in 9c).
   - `_kernel_registry.pyx` as a Cython extension module
     (`draken.ops.kernels._kernel_registry`).
   - Match the include paths the rest of the build uses (`draken`,
     `draken/core`, `third_party/cyan4973`, `third_party/yyjson/src`,
     `third_party/mimalloc/include`, `third_party/fastfloat`, etc. —
     copy from an existing draken extension's `include_dirs`).
2. **Fix every compile error** until the kernel tree builds clean.
   Known so far:
   - `error_handling.h` — add `#include <exception>` (and any other
     missing standard headers the macros need: `<cstdio>` for the
     `_fmt` variant, etc.).
   - `c_kernel_abi.h` — remove the dangling `#endif` (line 39); keep
     `#pragma once`.
   - **Expect more.** Compile, read the next error, fix, repeat, until
     every TU is clean. Do not stop at the two known ones.
3. **Wire and run the parity test.** `c_abi_test.cpp` must compile and
   execute. Options:
   - A standalone test binary built by `setup.py` or a small Makefile
     target, run in CI / `make q`’s orbit.
   - Or port its assertions into the project's C++ test harness if one
     exists (check `draken/tests/native/`).
   The test must actually **run** and **pass** — every kernel's C output
   compared against its nanobind binding output for representative
   inputs, plus the error-sentinel path.
4. **Verify the registry from Python**:
   ```python
   from draken.ops.kernels import _kernel_registry
   fn, ctx = _kernel_registry.lookup_kernel("ADD")   # non-null fn
   ```
   Add a small Python test asserting a representative kernel from each
   category resolves to a non-null pointer.

**Out of scope**
- 9b/9c wiring (struct fields, executor dispatch) — those are the next
  tickets, unblocked once this lands.
- Any design change — the locked decisions stand.
- Deleting the nanobind bindings — Decision 1a keeps them.

## The "expect more errors" reality

The `std::exception` error and the dangling `#endif` are the first two
of an unknown number. A code tree that has never compiled will have
accumulated multiple errors — missing includes, signature mismatches
between `.h` declarations and `.cpp` definitions, `VecResult` field
typos, namespace issues. **Budget for an iterate-compile-fix loop**,
not a two-line patch. Surface in the PR the full list of errors you
had to fix — it's evidence of how far from "done" 9a actually was, and
useful signal on review rigor.

If a kernel's logic is wrong (not just won't-compile), the parity test
catches it — that's why running the test is non-negotiable. A kernel
that compiles but returns the wrong answer is still a failure.

## Verification

- `make c` clean fresh build **with all kernel sources compiled in**.
  Confirm by `grep`-ing the build log for the kernel `.cpp` files being
  compiled (they must appear as clang invocations).
- `python -c "from draken.ops.kernels import _kernel_registry; print(_kernel_registry.lookup_kernel('ADD'))"`
  → a non-null function pointer (non-`(None, None)`).
- The C ABI parity test runs and passes (every kernel, plus the
  error-sentinel path). Show the test output in the PR.
- `make q` 100/100 — unchanged (the kernels still aren't on the
  executor path; that's 9c). But now they're *compiled*, so this is a
  meaningful green, not a trivial one.
- `make clickbench` non-regressing.

## Constraints (CLAUDE.md)

- **Broken but honest beats green but fake.** The whole reason this
  ticket exists. Do not report completion until the parity test has
  *actually executed and passed*. "It compiles" is necessary, not
  sufficient.
- **No `object` in compiled paths; no `import opteryx` from draken
  core** (§2/§3).
- **PyArrow / NumPy banned** in draken.
- **Fail fast** — the error-sentinel path must be tested, not assumed.
- **`make c` clean — with the kernels compiled — before claiming
  completion.** Verify the build log shows them compiling.
- **Do not commit.**

## Pre-flight reading

1. This ticket.
2. The original 9a ticket
   (`docs/tickets/zero-python-phase-9a-draken-c-kernel-abi.md`) for the
   intended design.
3. `docs/tickets/zero-python-phase-9-c-kernel-abi-design.md` §Post-design
   for the locked decisions.
4. An existing draken nanobind extension's stanza in `setup.py` — copy
   its `include_dirs` / compile flags for the new kernel sources.
5. `draken/ops/kernels/` — all 28 files. Read `error_handling.h`,
   `c_kernel_abi.h`, `kernel_registry.{h,cpp}`, `_kernel_registry.pyx`
   first; they're the spine.
6. `draken/tests/native/` — check for an existing C++ test harness to
   host the parity test.

## Definition of done

- All `draken/ops/kernels/*.cpp` + `_kernel_registry.pyx` are in
  `setup.py` and compile as part of `make c`.
- `c_kernel_abi.h` dangling `#endif` fixed; `error_handling.h` missing
  includes fixed; **all other compile errors fixed** (list them in PR).
- `_kernel_registry` imports from Python; `lookup_kernel` returns
  non-null for a representative kernel of each category.
- `c_abi_test.cpp` compiles, **runs**, and **passes** — output shown in
  PR.
- `make c` clean with kernels compiled (build log shows them);
  `make q` 100/100; `make clickbench` non-regressing.
- PR documents the full list of compile errors and any kernel-logic
  bugs the parity test caught — honest evidence of the gap between
  "reported done" and "actually done".

## Note for review

When this lands, re-run the same checks that caught the original gap:
```bash
grep -n 'kernels/' setup.py                      # sources registered
python -c "from draken.ops.kernels import _kernel_registry"   # imports
# build log shows kernel .cpp compiling
# parity test output shows PASS
```
A "complete" claim that doesn't satisfy all four is not complete.
This corrective ticket exists because those checks were not run the
first time.
