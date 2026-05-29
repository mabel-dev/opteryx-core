# Ticket: Restore the binary-op path to one correct path + targeted test gate

> Architect direction (2026-05-28), post Phase-9 step-back:
> - **Correctness before performance** — Phase 9 (a perf initiative) is
>   paused; fix the correctness regressions it introduced first.
> - **One path, complete or fails loud — no fallback.** The current
>   half-converted binop path (C-native for some cases, Python for
>   others, SIGBUS for null/bitwise/concat) is precisely the
>   incomplete-path-with-implicit-fallback that is not wanted.
> - **Undo what isn't right.** Reverting the not-yet-correct binop
>   C-native dispatch is sanctioned.
> - **Targeted tests per change** (the `make dt` model), not a `make q`
>   expansion — `make q` is intentionally fast/broad.

## Goal

The binary-op execution path is **one correct path** again — no
crashes, correct null propagation — and is guarded by a **targeted,
`make`-runnable value-checked test**. Phase-9's C-native conversion of
the binop path is explicitly deferred until it can *replace* this path
completely (all cases, null-correct), not run beside it.

## Current broken state (working tree, verified 2026-05-28)

Three live SIGBUS regressions on the binop C-native path, introduced
across the 9c rounds (all worked pre-9c, all invisible to `make q`):

| Query | State |
|---|---|
| `SELECT id \| 2 FROM $planets` | **SIGBUS** (worked in 9c) |
| `SELECT name \|\| '!' FROM $planets` | **SIGBUS** (worked in 9c) |
| `SELECT CASE WHEN id>4 THEN NULL ELSE id END + 10 FROM $planets` | **SIGBUS** (partial-null per-row merge never implemented) |

Working and to be **preserved**: `id+1`, `NULL+1`, `CAST(...)`,
`missions[0]`, CASE, constant fold.

## Approach — revert binop C-native dispatch to the single working path

The binop C-native dispatch (BC_BINARY_OP `kernel_fn` path added in 9c
+ the null-handling change in 9c-completion-2) is the source of all
three crashes. It is a *partial* conversion — exactly what "one path,
complete or fails loud" forbids.

**Revert the BC_BINARY_OP executor branch to the pre-9c single path**:
the DV fast path (`draken_arithmetic_dv`, already pure C, handles
common arithmetic) + the `resolve_binary_op` path for everything else.
That path was correct for all cases (null, bitwise, concat,
arithmetic) before 9c touched it. This is not "restoring a fallback" —
it is restoring **the** binop path to its last-correct, single-path
state.

Specifically:
- In `evaluation.pyx` BC_BINARY_OP: remove the `BC_INSTR_C_NATIVE`
  C-kernel dispatch branch for binary ops; the branch reverts to the
  pre-9c dispatch (DV fast path, then the resolved binary-op path).
- In `compiled_expression.pyx` `_NT_BINARY_OPERATOR`: stop populating
  `slot.kernel_fn`/`ctx_ptr`/`BC_INSTR_C_NATIVE` for binary ops. (Leave
  the struct fields — CAST and EXTRACTION still use them and work.)
- Leave CAST and EXTRACTION C-native dispatch **in place** — they are
  verified working and complete (`CAST(id AS VARCHAR)` → `['1','2']`,
  `missions[0]` → correct). Undo only the binop part that isn't right.

After revert, all three repros must work:
- `id | 2` → `[3, 2, 3]`
- `name || '!'` → `['Mercury!', ...]`
- `CASE WHEN id>4 THEN NULL ELSE id END + 10` → null-propagated
- `NULL + 1` → `[None]` (the pre-9c path handles this correctly too —
  verify)

## Targeted test gate (the `make dt` analogue)

Add a `make`-runnable targeted suite — propose `make et`
("expression tests") or fold into an existing fast target — that
value-checks the expression engine where `make q` is blind. Minimum
matrix:

- **Binary ops** × {non-null, all-null constant, partial-null column}:
  `+ - * / %`, `| & ^ << >>`, `||`. Assert values AND null
  propagation.
- **Cast**: int/float/string/bool/date/timestamp pairs, incl. null
  inputs. Assert values.
- **Extraction**: `arr[0]`, `arr[-1]`, OOB→NULL, string subscript.
- **CASE**: with/without ELSE, fixed + string + bool results,
  multi-branch, all-null branch.
- **The two standing correctness-bug repros** (so they're guarded once
  fixed): `COUNT(*) ... WHERE` value; `CASE WHEN x THEN int END` no-ELSE.

This suite is the gate the eight Phase-9 rounds lacked. It must run
fast (seconds), like `make dt`, and be the standard check for any
expression-engine change going forward. Wire it so it's easy to run
targeted during development.

## Scope

**In scope**
- Revert binop C-native dispatch (executor + bind-time) to the
  single working path.
- Verify the three crash repros + `NULL+1` all pass.
- Add the targeted value-checked `make` test suite (binop matrix +
  cast + extraction + CASE + standing-bug repros).
- `make q` 100/100; new suite green.

**Out of scope**
- Re-attempting binop C-native (Phase 9, paused — resumes only when it
  can replace the path completely, gated on the new suite).
- The two standing correctness bugs themselves (COUNT(*)-WHERE,
  assemble_fixed) — separate tickets exist; this ticket only adds
  their repros to the test suite so they're guarded when fixed.
- CAST/EXTRACTION/CASE C-native — working; leave them.

## Verification

- `make c` clean.
- The three regression repros + `NULL+1` pasted, all correct.
- New targeted suite green — full output pasted.
- `make q` 100/100.
- `make kernel-parity` still 48/48 (kernels unchanged; only binop
  *dispatch* reverts).
- `make clickbench` non-regressing (binop reverts to its prior path;
  no perf change expected vs pre-9c).

## Constraints (CLAUDE.md)

- **Correctness non-negotiable; fast-but-wrong is worthless.**
- **One path, complete or fails loud — no fallback.** The reverted
  binop path is the single path, not a fallback beneath a C path.
  Do not leave a half-C-native binop dispatch in place.
- **Fail fast** — no silent degradation; null input → null result.
- **`make c` clean before done.**
- **Do not commit.**

## Files (verify before editing)

- `opteryx/expression/evaluator/evaluation.pyx` — BC_BINARY_OP branch:
  remove the C-native binop dispatch, restore prior path.
- `opteryx/compiled/expression/compiled_expression.pyx` —
  `_NT_BINARY_OPERATOR`: stop setting `kernel_fn`/`ctx_ptr`/
  `BC_INSTR_C_NATIVE` for binops.
- `draken/ops/kernels/binary_op_*.cpp` — unchanged (kernels stay
  registered + parity-tested; just not dispatched from the executor).
- New test file + Makefile target for the targeted suite.

## Definition of done

- `id | 2`, `name || '!'`, partial-null arithmetic, `NULL + 1` all
  correct — no SIGBUS. Repros pasted.
- Binop is a single path (DV fast + resolved binary-op); no
  `BC_INSTR_C_NATIVE` binop dispatch remains.
- CAST/EXTRACTION/CASE C-native paths untouched and still working.
- Targeted value-checked `make` suite added, green, fast; covers the
  binop matrix + cast + extraction + CASE + standing-bug repros.
- `make c` clean; `make q` 100/100; `make kernel-parity` 48/48;
  `make clickbench` non-regressing.

## Note on Phase 9 resumption

When performance work resumes, binop C-native conversion is redone as
**one complete path**: the C kernels handle every case (null, bitwise,
concat, mixed-type, all shapes) correctly, the new targeted suite
gates it, and the old dispatch is **deleted** (not kept as a fallback)
the moment the C path is complete. The `BytecodeInstr` fields and the
48-kernel registry (9a/9b, verified good) remain in place for that.
