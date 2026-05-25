# E.27 — Stub `MATCH AGAINST` as NotImplementedError

> **Status:** TODO.
>
> **Architect call (2026-05-25):** "match_against() should be not supported
> and TODO — let's not get dragged into a machine learning hole right now."
>
> **Goal:** retire the existing `vector_match_against` C kernel and its
> embedding-provider plumbing. SQL `MATCH AGAINST (...)` continues to parse
> (so existing queries don't blow up at parse time), but execution raises
> `NotImplementedError` with a clear message. No fallback, no degraded
> mode — fail fast and loud per CLAUDE.md §1.
>
> **Why:** the current implementation drags in an embedding-provider
> abstraction (ML/vector-search territory) that we are not investing in now.
> Carrying the dead code through the rest of the draken rebuild and into
> the eval/operator rewrites costs effort and clouds the migration shape.

---

## 1. What's being delivered

1. **Delete** `opteryx/compiled/vector_ops/vector_match_against.pyx`.
2. **Remove** the textual include from `opteryx/compiled/vector_ops/vector_ops.pyx`
   (line 9: `include "vector_match_against.pyx"`).
3. **Edit** `opteryx/expression/functions/implementations/text.pyx`:
   - Remove the import `from opteryx.compiled.vector_ops import vector_match_against`.
   - Remove the `get_embedding_provider()` import / reference if it's not
     used elsewhere in the file (check; only delete if it goes to zero
     uses).
   - Replace the body of `def match_against(arr, val):` with a single
     `raise NotImplementedError("MATCH AGAINST is not currently supported.")`
     line. Keep the function signature so the registrar wiring stays
     intact — only the body changes.
4. **Verify** `opteryx/planner/logical_planner/logical_planner_builders.py`
   doesn't need changes. The parser-side `match_against` builder
   (L1091, L1331) builds the AST and is independent of execution. It stays.
5. **Verify** `opteryx/expression/functions/registrar/text.pyx`
   L699 (`callable_ref=string_functions.match_against`) still resolves —
   the function still exists at that name, just raises. The registrar
   doesn't need a change. (Confirm by inspection; no change unless
   compilation fails.)

## 2. What is explicitly NOT in scope

- The `vector_match_against.pyx`'s typed-Vector cimports (BoolVector,
  Float32Vector, StringVector, VectorVector) — they go away with the file.
  Don't migrate them; delete.
- The `get_embedding_provider()` abstraction and any embedding-related
  imports. If they're used solely by `match_against`, delete the imports
  too. If they have other consumers, leave them; surface the consumers as
  a follow-up.
- The SQL grammar / parser — `MATCH AGAINST` continues to parse.
- Any opteryx test that exercises `MATCH AGAINST` — those will now raise
  `NotImplementedError`. That's the intended failure mode. **Do not edit
  tests to silence the new exception.** If a test in `tests/` is now
  failing because of this stub, that's the *correct* state for those
  tests until the feature is re-implemented. Report them in §6 reporting
  back; do not "fix" them.
- The matching `text.py` (non-Cython) sibling. Check whether one exists;
  if there's a pure-Python `match_against` somewhere, it follows the same
  pattern (raise `NotImplementedError`). If there isn't, skip.

## 3. STOP conditions

- File count >5 (the .pyx delete, vector_ops.pyx include removal,
  text.pyx body change, possibly one cleanup of an orphan import, possibly
  a memory note). If you're past 5 you've drifted.
- `make draken` regresses. `vector_match_against` removal must not break
  the draken side — and it shouldn't, because nothing in `draken/` calls
  it.
- A test outside `tests/` (e.g. a doctest, a sample script in
  `dev/` or `scratch/`) imports `vector_match_against` directly. Surface,
  don't silently delete the importer.
- You find yourself rewriting the embedding-provider abstraction "to
  prepare for re-enablement later." Stop. The architect explicitly said
  don't get dragged into the ML hole; preparation work counts.

## 4. Discipline reminders

- **No fallback.** A function that's "not currently supported" raises.
  It does NOT return an empty BoolVector, a `False` literal, or `None`.
  Per CLAUDE.md §1: "Fail fast, fail clean. Never silently degrade
  behaviour."
- **No `try/except`.** Per CLAUDE.md §9. The `match_against` Python
  function's body is one line: `raise NotImplementedError(...)`.
- **No git commands.**

## 5. Acceptance criteria

Run and report verbatim:

1. `ls opteryx/compiled/vector_ops/vector_match_against.pyx 2>&1` —
   should be "No such file or directory".
2. `grep -c "vector_match_against" opteryx/compiled/vector_ops/vector_ops.pyx` —
   should be 0.
3. `grep -n "match_against" opteryx/expression/functions/implementations/text.pyx` —
   should show only the new stubbed function definition (and no import).
4. `make draken 2>&1 | tail -3` — succeeds.
5. `make dt 2>&1 | tail -3` — still ≥2792 passing.
6. `python -c "from opteryx.expression.functions.implementations.text
   import match_against; match_against([], ['anything'])"` — raises
   `NotImplementedError` with the expected message. (Or whatever scalar
   shape the function takes; adjust the call to satisfy the signature.)
7. `git diff --stat HEAD` shows ≤5 files changed.

## 6. Reporting back

- The seven acceptance outputs above.
- A list of any `tests/` files that now fail because they exercise
  `MATCH AGAINST`. Filenames only, do not edit them. These become a
  follow-up tracking item; they are not regressions of this ticket.
- Any orphan imports/symbols cleaned up (one line each, why).
- Confirmation that no embedding-provider preparation work was kept "for
  later" — the goal is genuine removal, not a parked stub.
