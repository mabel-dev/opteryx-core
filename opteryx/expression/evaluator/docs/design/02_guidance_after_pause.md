# 02 — Guidance after pause

> Issued mid-flight to course-correct, not as a fresh ticket. You stopped
> in the middle of broad eval-engine migration after surfacing the
> `vector_math.pyx` / `VectorVector` blocker. The blocker is now resolved
> (`vector_fp16_zeros` / `vector_fp16_with_nulls` exist in
> `draken.draken_native`). This document tells you what to keep, what to
> fix, and what's outside your scope before you resume.

---

## What's good — keep

You correctly identified that several adjacent eval-engine files needed
migration alongside `vector_math.pyx`. The work you did on these is
broadly correct:

- Killing the inline `from draken.interop.arrow import vector_from_sequence`
  pattern inside `cdef`/`cpdef` method bodies (one of E.30a's tyre-fire
  findings). Replacing with module-level `import draken.draken_native as
  _draken_native` and calling `_draken_native.vector_from_sequence(...)`
  is the right move.
- Wrapping nanobind handles in the `Vector` cdef-class shim at consumer
  boundaries (`Vector(_draken_native.vector_from_sequence(...))`).
- Updating evaluator-side imports off the deleted typed-Vector subclasses
  (`Integer64Vector`, `StringVector`, etc.) onto uniform `Vector`.

These are legitimate eval-PM migrations. Don't undo them. They will be
re-applied with corrections per the next section.

## What needs correcting — §3 violations

Three new `cdef object` instances introduced in compiled paths. All
should be `cdef Vector` (the shim type), not `cdef object`:

1. `cdef object _scalar_to_vector(object value)` — the parameter `object
   value` is legitimate (Python scalar input), but the **return type**
   must be `cdef Vector` since the function returns a Vector handle.
   Signature: `cdef Vector _scalar_to_vector(object value)`.

2. `cdef object nb_result` (local in the type-coercion path) — should be
   `cdef Vector nb_result` if it holds a Vector. If it holds something
   else, type it concretely. Never `object` in a compiled path.

3. `cdef object result = Vector(_draken_native.vector_from_sequence(...))`
   in the collector finalize methods — should be `cdef Vector result =
   Vector(_draken_native.vector_from_sequence(...))`. The
   `from-the-shim` cimport (`from draken.vectors.vector cimport Vector`)
   needs to be present at the top of each file that uses this pattern.

These are mechanical edits. Make the same change everywhere the pattern
appears (likely 3–10 sites across the files you've touched).

**Why this matters:** `cdef object` in compiled Cython is a CLAUDE.md §3
violation. It's also the exact smuggling pattern that produced the
`vector_lowercase.pyx`'s `cdef object builder = ...` issue and the
E.32 agent's `DecimalVector` cdef-class drift. The architect is allergic
to this and rightly so. When typing as `Vector` is possible, use
`Vector`. Only legitimate `object` is at the `def` Python edge (and
even there, type as concretely as the input allows).

## What's outside your scope — revert or leave alone

You've touched files that belong to the operator-PM, not the eval-PM.
Your scope per `00_pm_briefing.md` §2 is `opteryx/expression/evaluator/`
plus the four `vector_ops/*.pyx` files the evaluator directly imports
(`vector_like`, `vector_rlike`, `vector_bitwise_not`, `case_helpers`).
Plus `vector_math.pyx` because the eval-engine chain depends on it (you
correctly surfaced this).

**Files outside that boundary** that you've modified — revert these:

- `opteryx/operators/**/*.pyx` and `*.pxi` — all of them. Operators are
  operator-PM's lane (separate initiative per
  `opteryx/operators/docs/design/00_operators_and_parallelism.md`).
- `opteryx/managers/execution/serial_engine.py` — execution engine,
  operator-PM-adjacent.
- `opteryx/managers/virtual_datasets/*.py` — out of scope for any
  current PM.
- `opteryx/connectors/base/base_connector.py` — connector layer, out.
- `opteryx/models/manifest.py` — out.
- `opteryx/utils/__init__.py` — out.

`git checkout HEAD -- <path>` each one. The legitimate migration moves
you made in operator files (killing inline imports, wrapping in shim)
are real work, but they're not your work — they belong to the
operator-PM's tickets so the operator-PM owns their correctness and the
operator-PM's testing exercises them. If you leave them in, you've
done the operator-PM's job for them with violations they'll inherit.

**In-scope files where the legitimate moves stay** (after §2 corrections):

- `opteryx/expression/**/*.pyx`
- `opteryx/expression/evaluator/**/*.pyx`
- `opteryx/compiled/vector_ops/{vector_like, vector_rlike,
  vector_bitwise_not, case_helpers}.pyx` (the four the evaluator
  imports)
- `opteryx/vectors/vector_math.pyx` (the original ticket)
- `opteryx/vectors/embeddings.py` (vector_math's caller)

## How to resume

1. **Verify `vector_math.pyx` first.** That's your primary ticket. The
   `vector_fp16_zeros(length, dimension)` and
   `vector_fp16_with_nulls(length, dimension)` primitives exist in
   `draken.draken_native` and are the constructors for `new_matrix` /
   `new_matrix_with_nulls`. The Vector shim's `vec.unified()` returns a
   non-const `DrakenVector*`, so the in-place mutation pattern
   (`mark_present`, `pack_fp32_row`, `write_row_bytes`) keeps working
   — write into `vec.unified().data` (cast to `uint16_t*`) and
   `vec.unified().validity` (Arrow convention: bit=1 valid, bit=0 null,
   inverted from the old VectorVector's `_null_bitmap` if that used the
   IS_NULL=1 convention).

   For accessing the dimension from cdef code: pass it as a function
   argument where possible. If a function genuinely needs to read the
   dimension from the Vector itself, surface the gap — there isn't yet
   a cdef-accessible "get dimension from logical-type descriptor"
   accessor. Don't invent one in your own code.

2. **Revert the out-of-scope files** per §3 above.

3. **Fix the three `cdef object` instances** per §2 above. Re-apply the
   legitimate migration moves to the in-scope files with `cdef Vector`
   typing.

4. **Verify the build.** `make draken` (or `make compile` if that
   subset works) should still build draken-side cleanly; `_impl.so`
   should compile and import; relevant tests should run. If something
   breaks that's clearly downstream of operator-PM-lane work you've
   reverted, surface that as a follow-up rather than re-applying the
   operator-side changes here.

5. **Report back** with: the final in-scope file list, the diff stat,
   confirmation that no `cdef object` remains in compiled paths, and
   any genuinely-architectural gap you surfaced during the rewrite.

## STOP conditions going forward

Trip any of these → stop and surface, do not work around:

- The migration of a file requires a new `cdef object` instance in a
  compiled path. **STOP.** That's a missing primitive or wrong shape;
  the draken-PM provides primitives, not the eval-PM.
- The migration requires a function in `draken/draken_native.cpp` or
  `draken/ops/` or `draken/core/` that doesn't exist. **STOP and
  surface.** The draken-PM adds the primitive directly (recent
  examples: `vector_uint64_eq_scalar`, `vector_fp16_zeros`).
- The migration requires touching a file outside the in-scope list
  in §3. **STOP and surface.** That belongs to another PM.
- The migration would introduce a Python loop over per-row data in a
  compiled path "as a temporary fix to be optimised later." **STOP.**
  The architect's words: "I'll add a comment flagging the perf
  optimisation and move on" is the no-false-green pattern. The
  primitive doesn't exist yet → surface the gap, don't paper over.
- The migration requires a compatibility shim, a typedef alias for a
  deleted type, or restoring a deleted helper as a stub. **STOP.**
  Whole rebuild's clean-break charter forbids these.

## Anti-pattern recognition (calibration)

If you find yourself writing any of the following, recognise it and stop:

- `cdef object foo = something_that_returns_a_Vector(...)` — type as
  `cdef Vector foo`.
- `cdef ... helper(object x) -> object` — at minimum the return should
  be typed.
- `"""TODO: long-term fix is X, for now Y"""` — Y is fake-green if X
  is the correct path. Surface the gap.
- `from <module> import foo` inside a `cdef`/`cpdef` body — hoist to
  module level (unless explicitly documented circular-import in
  source, per E.30a category C).
- `cdef class FooVector(Vector)` — typed-Vector subclass hierarchy is
  the explicit anti-pattern; uniform Vector + DrakenType.

## Why this matters

The eval-engine migration is a real, substantial piece of work. You're
doing it. The risk isn't the work itself — it's the pattern where an
agent in mid-migration accumulates small concessions (one `cdef object`
here, one out-of-scope file there) and the result becomes too tangled
to verify cleanly. The remedy is small-and-scoped, with the discipline
that *the substrate is honest at every checkpoint*.

You stopped at the right moment when you hit the `VectorVector`
blocker — that's the model. Keep doing it. Surface, don't paper over.
