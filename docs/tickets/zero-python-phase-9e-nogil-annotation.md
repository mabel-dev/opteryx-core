# Ticket: Zero-Python Phase 9e — nogil annotation + thread-safety stress test

> Implementation sub-ticket of the locked Phase 9 design
> (`docs/tickets/zero-python-phase-9-c-kernel-abi-design.md` §Post-design).
> Implements Decision 7's end state (GIL release boundaries).
> **Depends on 9c (C-pointer dispatch) and 9d (Morsel nogil surface).**
> This is the ticket that delivers the architect's headline goal:
> `execute_bytecode` runs with the GIL released.

## Goal

Annotate the `execute_bytecode` inner loop `nogil`. Release the GIL on
entry; re-acquire only at the exit boundary (to construct the Python
Vector wrapper around the final result) and at any kernel-error point.
BC_CASE re-entry (`execute_bytecode_c`) stays nogil end-to-end.

After 9e, the Cython compiler is the proof: `with nogil:` over the
dispatch loop refuses to compile if any Python op remains. If it
compiles, the execute-time path is Python-free.

## Locked decision

**Decision 7 / GIL boundaries**:
- `execute_bytecode` entry: acquire morsel handle, then `with nogil:`
  the dispatch loop.
- Inner loop: nogil. All 5 opcode kernels are C function pointers
  (9c). LOAD_COL / COMPARE / BINARY_OP DV paths / etc. use the nogil
  Morsel surface (9d).
- Exit: re-acquire GIL, build the Python `Vector` wrapper from the
  final `dv_stack[0]` via `_slot_to_pyobj`.
- Kernel error: set a flag in the nogil region, break, re-acquire GIL,
  raise.
- BC_CASE recursion: `execute_bytecode_c` is itself nogil.

## Approach

1. Split `execute_bytecode` into:
   - A thin cpdef shell (holds GIL): unwrap morsel, set up arena,
     `with nogil:` call the inner loop, re-acquire, wrap result, raise
     on error.
   - `cdef ... execute_bytecode_c(...) nogil` (from 9c): the dispatch
     loop. Every branch must be nogil-clean.
2. Audit every opcode branch for stray Python ops. The compiler
   enforces this — any `object` access inside `with nogil:` is a hard
   error. Fix each by routing through the 9d C surface or the 9a C
   kernels.
3. Error path: a `cdef int err_code` + `cdef const char* err_msg` set
   inside nogil; checked after the loop; raised under GIL.
4. Stress test: run a CASE-and-function-heavy query mix across N
   concurrent threads (Python `threading` — real parallelism now that
   the GIL is released in the hot loop). Assert correctness + no
   crashes + no data races.

## Scope

**In scope**
- `opteryx/expression/evaluator/evaluation.pyx`:
  - `with nogil:` around the dispatch loop.
  - cpdef shell / cdef nogil core split (may already be done in 9c;
    finalise here).
  - nogil-clean every opcode branch.
  - Error-flag-and-raise plumbing.
- A thread-safety stress test (new test file under `tests/`).

**Out of scope**
- Deleting dead flags / resolver closures — 9f.
- Any new opcode or kernel.
- Operator-level parallelism that nogil now *enables* — that's a
  separate future opportunity, not this ticket. 9e only makes the
  executor nogil-safe; it doesn't parallelise operators.

## The compiler is the gate

The point of this ticket: if `make c` succeeds with `with nogil:`
over the loop, there is provably no Python op in the execute path.
A compile error inside the nogil block names the exact remaining
Python op — fix it (route through 9a/9d C surfaces) and recompile.

## Verification

- `make c` clean fresh build — **with the `with nogil:` block in
  place**. This is the proof.
- `make q` 100/100.
- Thread-safety stress test: e.g. 8 threads each running a loop of
  mixed CASE / function / cast / arithmetic queries for a few seconds;
  assert every result matches the single-threaded answer; no segfault,
  no ASAN/TSAN complaint if available.
- Value-checked spot tests (carry from 9c) still pass.
- `make clickbench` — report deltas; nogil shouldn't regress single-
  thread and may improve if the runtime was contending.

## Constraints (CLAUDE.md)

- **Release the GIL as early as possible** (§2) — this ticket is that
  principle realised for the expression engine.
- **No Python in the nogil region** — the compiler enforces; don't
  silence it with `with gil:` escape hatches except the one
  documented exit-wrap and error-raise points.
- **Fail fast** — kernel errors propagate via the error flag → raise.
- **`make c` clean before done.**
- **Do not commit.**

## Pre-flight reading

1. Phase 9 design §Post-design (Decision 7, Risk 3 Cython/3.13 nogil).
2. 9c + 9d tickets.
3. `evaluation.pyx` — search for the existing `c_execute_bytecode_inner`
   (the bitmap-only nogil path mentioned in the original audit) — it's
   the precedent for how nogil is already done for a subset; generalise
   that pattern.
4. CLAUDE.md §2 (execution model — release GIL early).

## Definition of done

- `execute_bytecode`'s dispatch loop runs under `with nogil:`; `make c`
  compiles it clean (the proof of zero-Python execute path).
- GIL re-acquired only at result-wrap exit + error-raise.
- BC_CASE recursion nogil end-to-end (or documented bounded GIL
  reacquire per 9d path (b)).
- Thread-safety stress test green.
- `make q` 100/100; `make clickbench` reported (no single-thread
  regression).
