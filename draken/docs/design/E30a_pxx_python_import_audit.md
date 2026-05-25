# E.30a — Audit: Python imports inside `cdef` / `cpdef` bodies in `.pyx` / `.pxi`

> **Status:** TODO.
>
> **Goal:** produce a complete, classified inventory of every Python
> `import` / `from … import` statement that appears **inside** a `cdef`
> or `cpdef` function/method body across `opteryx/` and `rugo/`. No code
> changes. Output is one structured doc.
>
> **Why:** Survey E.29 §9.4 found multiple instances of this pattern in
> `opteryx/operators/grouped_aggregate_hashed/_collectors_*.pxi` where a
> cdef method does `from draken.interop.arrow import vector_from_sequence`
> on every call. Architect call: "Sounds like a tyre-fire we need to put
> out before it spreads." This ticket maps the fire before E.30b puts
> it out.
>
> **Predecessor:** E.29 (producer-surface survey, complete). E.30b (the
> fix) is blocked on this audit.

---

## 1. What's being delivered

A single output document at
`draken/docs/design/E30a_pxx_python_import_audit_report.md` containing
one table:

```
| # | File | Line | Containing function/method | Import statement (verbatim) | Category | Notes |
```

Plus a short summary at the bottom counting category totals.

**No code changes.** Not one `.pyx` or `.pxi` is touched. Not setup.py.
Not anything. The deliverable is the table.

## 2. Scope — what to grep, where

Scan:
- `opteryx/**/*.pyx`
- `opteryx/**/*.pxi`
- `rugo/**/*.pyx`
- `rugo/**/*.pxi`

Exclude:
- `tests/`, `scratch/`, `dev/` — non-production trees.
- Generated `.cpp` / `.c` files — they reflect the `.pyx` source.
- `draken/` — `draken/` is C++-first; if a Python import appears in
  draken's own `.pyx` (there shouldn't be any) report it but in a
  separate section.

The pattern to find: any `import` or `from … import` statement whose
**syntactic enclosing scope** is the body of a `cdef` or `cpdef`
function/method, **not** module top-level.

This is not a pure regex problem — module-level imports inside Cython's
`include` blocks resolve as module-top, not function-inner. The audit
must distinguish. The cheapest way to be accurate is: for each `.pyx` /
`.pxi`, walk top-down tracking indent depth + most recent `cdef`/`cpdef`
def line; if an `import` is reached while inside a function body
(deeper indent than the `cdef`/`cpdef` def line), flag it.

A small Python script (in `dev/`, or one-shot via the agent) is the
right tool — `dev/audit_pxx_imports.py` if you want it durable. Either
way, do not "approximate" via flat grep; the misclassification rate is
real (`include` blocks, decorated functions, nested classes).

## 3. Classification rules

For each finding, assign exactly one category:

- **(A) Hot-path** — the enclosing function is `cdef inline`, has
  `noexcept`, is `nogil`, or is named in a way that suggests
  per-row/per-morsel execution (e.g. `_push_impl`, `compare_*`, anything
  on a `BasePlanNode` subclass's hot methods, anything in a
  `_collectors_*.pxi`'s `finalise`/`collect` methods, anything in a
  vector op `vector_*` cdef). These are §2/§3 violations to be fixed.

- **(B) Init-time / once-per-query** — the enclosing function is a
  `__cinit__`, `__init__`, `bind()`, `compile()`, a constructor, or
  similar. Called once at object/pipeline construction; Python imports
  here are still wasteful but not a hot-path violation. Fix-recommended,
  not fix-required.

- **(C) Defensible deferred** — the import is explicitly deferred to
  avoid a documented circular import, and there's a comment or commit
  message saying so. Rare; if you cannot find evidence of intent, do NOT
  classify here — use (A) or (B).

If a finding is genuinely ambiguous, mark **(?)** and surface in §6
reporting — don't guess.

## 4. STOP conditions

- The audit script touches a single `.pyx` / `.pxi` file. **STOP.** This
  ticket is read-only.
- You find yourself "fixing one while you're there." **STOP.** Fixes are
  E.30b, not E.30a.
- More than 5 distinct files touched in `dev/` for tooling. **STOP** —
  the audit script should be one file.
- The grep / classifier finds more than ~50 distinct hits AND you start
  consolidating them into "this is probably fine, skip." **STOP.** All
  findings get listed; consolidation is the architect's call.

## 5. Acceptance criteria

1. `ls draken/docs/design/E30a_pxx_python_import_audit_report.md` — the
   report exists.
2. The report's table is non-empty and contains at minimum the 5+ hits
   already identified in E.29 §9.4 (4 hits in `_collectors_*.pxi`, plus
   any additional ones the structured scan finds).
3. Every row has all six columns populated. No "TODO", no "tbd".
4. The summary at the bottom of the report counts: total findings, count
   by category, count of (?) ambiguous.
5. `make draken && make dt 2>&1 | tail -3` — still passes. (Sanity:
   confirms the audit didn't accidentally change anything compiled.)
6. `git diff --stat HEAD` — shows ≤2 new files: the report itself, and
   at most one audit script under `dev/`. Zero modifications to
   existing `.pyx` / `.pxi` files. Zero modifications to `setup.py`.

## 6. Reporting back

In addition to the report file, provide:

- The summary numbers from §5 acceptance #4.
- The top three (A)-category findings by likely call frequency, with a
  one-line note each on why you ranked them so. (This feeds E.30b's
  prioritisation.)
- Any (?) ambiguous findings called out separately with a note on what
  made them ambiguous.
- A note on whether the structured scan found any pattern not yet
  contemplated by E.29 §9.4 (e.g. wildcard imports, conditional imports,
  `__import__` calls, late-bound module attribute access via
  `importlib`). If you find any, surface them — they may be the same
  fire wearing a different mask.

## 7. After this lands

**E.30b — fix the hot-path Python imports.** Scoped from the (A)
findings in this audit. Likely shape: per-file or per-cluster
micro-tickets each removing one import-pattern. (B) findings are
fix-recommended and follow at a relaxed pace; (C) findings stay where
they are, possibly with a clarifying comment.

Then **E.31 onward** (producer surface design + implementation +
migration), with the producer-surface migration in E.33 no longer
risking entrenching the import-inside-cdef pattern.

## 8. Discipline reminders

- **Read-only.** Audits don't fix. If you can't help yourself, stop
  doing audits.
- **No `hasattr`.** Per CLAUDE.md §9. The classifier shouldn't need it
  anyway.
- **No git commands.**
- **No grep-only "approximation"** for the classifier — you'll get the
  count wrong by 30%+ and the architect will catch it.
