# Ticket: Zero-Python Phase 9a-completion-2 — populate the registry and RUN the parity test

> Second corrective ticket. The first
> (`zero-python-phase-9a-completion.md`) got the kernel code to compile
> and `_kernel_registry` to import — real progress — but the deliverable
> is **still not functional**:
> - the registry map is **empty** (`lookup_kernel` returns `(None, None)`
>   for every key);
> - the C ABI parity test **still never ran**.
>
> Two completion reports, core deliverable absent both times. This ticket
> closes it with un-fakeable gates.

## Architect decision (2026-05-28)

**Function kernels are carved out.** The 9 `draken/ops/kernels/function_*.cpp`
files are **out of scope here** — they move to a dedicated **9a-fn**
sub-phase, sequenced before 9e. BC_FUNCTION keeps its Python call until
9a-fn lands. (The current code-comment claiming function kernels are
"deferred to Phase 9f" is wrong — 9f is cleanup. Fix that comment to
reference 9a-fn.)

So **9a's scope is now: cast + binary_op + extraction kernels only.**
Those three categories must be fully registered, reachable, and
parity-tested before 9a is done.

## What's still wrong (verified 2026-05-28)

1. **Registry is an empty map.** `kernel_registry.cpp:35`:
   ```cpp
   static std::map<std::string, kernel_fn_t> _kernel_registry = {
       // Add cast/binary_op kernels here as they are implemented
   };
   ```
   No kernel is registered. `lookup_kernel("ADD")`, `"Plus"`, every key →
   `(None, None)`. The registry compiles and imports but resolves
   nothing.
2. **The parity test never ran.** `c_abi_test.cpp` is still not in any
   build target. `grep c_abi_test setup.py Makefile draken/tests/` →
   empty. Zero evidence any kernel returns a correct result.

`make c` clean + `make q` 137/137 remain green-but-fake: the kernels are
compiled but unreachable (empty registry) and unproven (no test run).

## Goal

For **cast + binary_op + extraction** kernels:
- The registry maps every kernel name → its `extern "C"` function
  pointer. `lookup_kernel` returns non-null for each.
- The C ABI parity test is wired into a build/run target, **executes**,
  and **passes** — C-function output vs nanobind-binding output for each
  kernel, plus the error-sentinel path.

## Scope

**In scope**
1. **Populate `kernel_registry.cpp`'s map** with every cast, binary_op,
   and extraction kernel:
   - Forward-declare each `extern "C"` kernel function (or include the
     category headers).
   - Add a `{ "NAME", (kernel_fn_t)&draken_kernel_fn }` entry per kernel.
   - **Naming**: decide the canonical key scheme and document it. The
     bind-time resolver in 9b will look up by these names, so they must
     match what 9b will produce from `(op_code, types)` /
     `resolve_cast` / `resolve_binary_op`. Recommended: the same
     op-string + type-pair encoding the resolvers already use (e.g.
     cast key = `"CAST_INT64_TO_STRING"`, binary key = `"ADD"` /
     `"BITWISE_OR"` / etc.). Surface the scheme in the PR so 9b aligns.
   - For parameterised kernels (cast timestamp unit, binary op_code),
     the registry returns the function pointer; the **context** is
     allocated separately by `kernel_alloc_*_ctx` (already implemented).
     Confirm `lookup_kernel` + the alloc functions compose correctly.
2. **Wire `c_abi_test.cpp` into a runnable target.** Either:
   - A `setup.py`-built standalone test binary, or
   - The existing C++ native test harness under `draken/tests/native/`
     (check what's there), or
   - A `make`-target that compiles and runs it.
   The test must **run as part of the verification** — pasted output in
   the PR.
3. **Scope the test to in-scope kernels.** If `c_abi_test.cpp`
   currently references function kernels (now carved out), gate or
   remove those cases so the test covers cast/binary_op/extraction. Do
   not let carved-out kernels block the test.
4. **Fix the stale comment** in `kernel_registry.cpp` that says function
   kernels are "deferred to Phase 9f" → "deferred to Phase 9a-fn".

**Out of scope**
- `function_*.cpp` kernels — 9a-fn (separate ticket).
- 9b/9c wiring — unblocked once this lands.
- Any design change.

## Verification — un-fakeable gates

Each of these is a hard gate. A completion claim that doesn't show all
four **with output** is not complete. (Two prior reports skipped these.)

1. **Registry non-empty, reachable from Python.** Paste the output of:
   ```python
   from draken.ops.kernels import _kernel_registry as r
   for k in [<one cast key>, <one binary_op key>, <one extraction key>]:
       fn, ctx = r.lookup_kernel(k)
       assert fn is not None, f"{k} unresolved"
       print(k, "->", hex(fn))
   ```
   All three must print a non-null pointer.
2. **Parity test runs and passes.** Paste the test runner's output
   showing PASS for each in-scope kernel + the error-sentinel case.
   "It compiles" is not acceptance; it must **execute**.
3. **`make c` clean** with the kernels compiled — show the build log
   lines compiling `cast_*.cpp` / `binary_op_*.cpp` / `extraction.cpp`
   / `kernel_registry.cpp`.
4. **`make q` 100/100** and **`make clickbench` non-regressing.**

## Constraints (CLAUDE.md)

- **Broken but honest beats green but fake.** This ticket exists
  because that line was crossed twice. Do not report done until gates
  1 and 2 above produce real output. An empty registry that compiles is
  not a registry.
- **Fail fast** — error-sentinel path tested, not assumed.
- **No `object` in compiled paths; no `import opteryx` from draken.**
- **`make c` clean — with kernels compiled and registry populated —
  before claiming completion.**
- **Do not commit.**

## Pre-flight reading

1. This ticket + `zero-python-phase-9a-completion.md` (round 1) +
   the original 9a ticket.
2. `draken/ops/kernels/kernel_registry.cpp` — the empty map to fill.
3. `draken/ops/kernels/cast_kernels.h`, `binary_op_kernels.h`,
   `extraction_kernels.h` — the `extern "C"` signatures to register.
4. `draken/ops/kernels/c_abi_test.cpp` — the parity test to wire + run.
5. `draken/ops/kernels/_kernel_registry.pyx` — the Python surface
   (`lookup_kernel`, `alloc_*_ctx`).
6. `opteryx/expression/casts.pyx:resolve_cast` and
   `opteryx/expression/evaluator/arithmetic.pyx:resolve_binary_op` —
   the key scheme must align with what these will pass to 9b.
7. `draken/tests/native/` — check for a C++ test harness to host the
   parity test.

## Definition of done

- `kernel_registry.cpp`'s map is populated for **every** cast,
  binary_op, and extraction kernel; `lookup_kernel` returns non-null
  for each (gate 1, output in PR).
- C ABI parity test wired, **run**, **passing** (gate 2, output in PR).
- Stale "Phase 9f" comment corrected to "9a-fn".
- `make c` clean with kernels compiled (gate 3, build-log lines in PR).
- `make q` 100/100; `make clickbench` non-regressing (gate 4).
- The key-naming scheme documented in the PR so 9b can align its
  bind-time lookups.

## Note: a 9a-fn ticket is now needed

Per the architect decision, the 9 `function_*.cpp` kernels need their
own ticket (**9a-fn**): wire them into the build, register them, extend
the parity test to cover them. It must land **before 9e** (the nogil
annotation) because BC_FUNCTION can't go nogil until its kernels are
C-callable. That ticket is separate from this one — flag it as the
next-but-one piece of work.
