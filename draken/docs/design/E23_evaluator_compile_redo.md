# E.23 — Evaluator Compile Redo (Phase 20 finish)

> **Goal:** produce `opteryx/expression/evaluator/_impl.cpython-*.so` from a full
> `setup.py build_ext --inplace` (no `DRAKEN_BUILD=1`).
>
> **Predecessors:** E.20 survey (compile blockers identified), Phase 20/20a (3 `.pxd`
> stubs added: `morsel.pxd`, `bool_vector.pxd`, `vector.pxd`), E.22 (build isolation
> via `make draken` / `DRAKEN_BUILD=1` — so failing here does NOT wipe `draken_native.so`
> anymore).
>
> **Why this is now small:** every prior attempt at this collapsed because a single
> Cython error in one unrelated `.pyx` aborted the whole `cythonize()` batch, wiping
> the draken extensions. With E.22 in place, the draken side is permanently safe; this
> ticket is purely about getting one more `.so` over the line.
>
> **Status:** TODO.

---

## 1. Known starting point

A fresh `python setup.py build_ext --inplace -j 4` (no `DRAKEN_BUILD`) currently fails
with exactly one Cython error:

```
opteryx/compiled/vector_ops/vector_dfa_extract.pyx:228:12: 'uint32_t' is not a type identifier
opteryx/compiled/vector_ops/vector_dfa_extract.pyx:308:38: 'uint32_t' is not a type identifier
```

`vector_dfa_extract.pyx` line 73 has `from libc.stdint cimport int32_t, uint8_t` —
it forgot `uint32_t`. Trivially fixable.

There is also a non-blocking warning to clean up (placement, not breakage):
```
draken/vectors/vector.pxd:31:75: The keyword 'nogil' should appear at the end of the
function signature line. Placing it before 'except' or 'noexcept' will be disallowed
in a future version of Cython.
```

After fixing the `vector_dfa_extract` cimport there may be more errors hiding behind it
(cythonize stops on first batch failure). Walk them down one at a time. Do **not** add
silencing pragmas, blanket try/except, or stub out functions to "make it compile" —
each error is either a one-line fix (missing cimport, wrong type, stale import path of
something the rebuild moved) or a genuine integration gap that must be surfaced. If a
class of error needs broader judgement (e.g. an import points at something genuinely
gone, requiring a real C′ port), STOP and report — that's a new ticket, not this one.

---

## 2. Acceptance criteria (hard)

After this ticket:

1. `python setup.py build_ext --inplace -j 4` (no `DRAKEN_BUILD`) runs to completion
   without a Cython compile error.
2. `opteryx/expression/evaluator/_impl.cpython-313-darwin.so` exists on disk.
3. `python -c "from opteryx.expression.evaluator import _impl"` imports cleanly.
4. `make draken` still works (E.22 must not regress).
5. `make dt` still passes (2792+ draken tests).
6. Report the state of `make q` afterwards. It is **not** required to be green here —
   downstream operator rewrites are still pending — but record the failure mode so we
   know what's next. If a small number of failures are caused by trivial issues the
   evaluator change introduced, note them; do not attempt broad operator fixes.

## 3. Non-goals (explicitly out of scope)

- Operator file rewrites in `opteryx/operators/` — separate phase (~21 files).
- The bitmap-VM rewrite (E.20 §5.1 Option B) — already deferred.
- UTF-8 cluster, regex cluster, heavy specials — pending architect decisions.
- Touching `draken/` C++ or `.h` files. The draken side is closed and ABI-frozen for
  this ticket; if you think you need to change it, you've gone out of scope — stop.
- Mass-rewriting `vector_ops` `.pyx` files into nanobind C′. The instruction is fix
  what blocks compile, no more. Per-op C′ migrations are their own tickets.

## 4. Discipline reminders (from prior ticket failures)

- **No `object` parameters or returns in compiled Cython.** §3 violation, terminated
  twice already. If you find one already in the tree, leave it; do not introduce new
  ones.
- **No silencing.** No `# noqa`, no `try: from X import Y\nexcept: Y = None`, no
  blank-except. Errors must be visible.
- **No drift.** The Phase 20a postmortem was "agent drifted into 57 files, chose stubs
  over migration". Stay in the named files. If a fix needs more than ~5 files, stop
  and ask.
- **Don't touch git.** Per CLAUDE.md §0 you are not trusted with git commands.

## 5. Reporting back

On completion, report:
- The exact list of files you changed (paths + 1-line reason each).
- Output of `ls -la opteryx/expression/evaluator/_impl*.so`.
- Output of `make dt | tail -3`.
- A one-paragraph note on `make q` state (pass count, what fails, whether failures are
  evaluator-related or pre-existing operator-rewrite gaps).
