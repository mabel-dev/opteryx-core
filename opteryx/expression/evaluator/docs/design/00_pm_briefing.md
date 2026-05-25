# 00 — Briefing for the evaluation-engine rewrite PM

> Status: briefing record. Written at the handover from the draken-rebuild PM.
> You are taking over the *first* of two follow-on initiatives:
>
> 1. **(You)** Evaluation-engine rewrite to use new draken.
> 2. (Next PM) Operator rewrite to use new draken — gated by your work, since
>    ~5 operators depend on the evaluator.
>
> This document is a briefing, not a plan. Plans come from your tickets.

---

## 1. Read these first, in this order

Do not skip. The draken-rebuild PM repeatedly cost time by mis-specifying
tickets that contradicted documents already in the tree.

1. **`CLAUDE.md`** at the repo root — non-negotiable engineering contract.
   Internalise §1, §3, §9, §11. Especially §11 (the DrakenVector model) and
   §3 ("Cython code must be typed, use of `object` is forbidden").
2. **`draken/docs/design/09_delivery.md`** — operational state of draken.
3. **`draken/docs/design/00_data_model.md`** — what a Vector is, the three
   encoding shapes, uniform `data[selection[i]]` access.
4. **`draken/docs/design/07_consumer_contract.md`** — what consumers may
   assume about a Vector.
5. **`opteryx/operators/docs/design/01_draken_state_at_handover.md`** — the
   state of draken at the moment you start. Lists what's done, what's not, the
   landmines, the binding memory-file decisions.
6. **`draken/docs/design/E20_evaluator_survey.md`** — a structural survey of
   the evaluator. **Most relevant doc for you specifically.** It maps the
   ten `.pyx` files, the two halves (tree-walker + bytecode VM), where each
   imports from, and the cimport blockers.
7. **`draken/docs/design/E24_cython_vector_shim.md`** and
   **`draken/docs/design/E25_e24_revert_and_redo.md`** — the most recent two
   tickets. Read E.25 carefully. It catalogues a class of failure (annotated
   fake-green, scope drift, ticket-vs-actual mismatch) you must not repeat.

After that, read the actual evaluator code: `opteryx/expression/evaluator/`.
Ten files, ~3,000 lines.

## 2. What is yours

In scope:

- Everything in `opteryx/expression/evaluator/`. The ten `.pyx` files and
  `_impl.pyx` that orchestrates them via Cython `include`.
- Migration of evaluator imports off old-draken's typed-Vector subclass
  hierarchy (`Integer64Vector`, `StringVector`, `Float64Vector`, etc.) to new
  draken's uniform `Vector` + `DrakenType`-dispatch model.
- The bytecode VM's GIL-free inner loop (the lower half of `evaluation.pyx`,
  lines ~650 onward).
- The `comparisons.pyx` dispatch tree (~35 vector_* call sites).
- The four `vector_ops/*.pyx` files the evaluator imports directly
  (`vector_like`, `vector_rlike`, `vector_bitwise_not`, the
  `assemble_*`/`decide_*` helpers in `case_eval.pyx`). When these need C′
  ports, that work belongs to you — surface as draken-side gap if the op is
  missing from `draken/ops/`.

Out of scope — explicitly NOT yours:

- The 21 operator files in `opteryx/operators/`. That is the next PM.
- The scheduler, pipeline DAG, inter-operator parallelism. That is
  `opteryx/operators/docs/design/00_operators_and_parallelism.md`'s
  territory.
- `draken/` C++ headers, `.h` files, `draken_native.cpp`. The ABI is frozen
  and the type matrix is closed. You consume draken's surface; you do not
  modify draken's internals. If you need a missing op, scope a draken-side
  ticket and **surface it to the architect**.
- The UTF-8 cluster (`vector_initcap`, `vector_lowercase`, `vector_uppercase`,
  `vector_reverse`, `vector_string_slice`). These need an architect decision
  on the case-folding library (boost::locale vs ICU) before they can be
  ported. If your migration runs into one, surface it; do not fold the
  decision into a ticket.
- The heavy specials (`vector_match_against`, `vector_dfa_extract`). Same
  pattern.

## 3. The state you inherit

At handover:

- `make draken` works. 2792+ native draken tests pass via `make dt`.
- `make q` is `0/133`. **This is the honest baseline.** A previous attempt
  produced `111/133` (83%) by riding two annotated fake-green shims; that
  number was discarded along with the shims in E.25.
- Four known pre-existing gaps that E.25's revert made honest:
  1. `opteryx/compiled/structures/bloom_filter.pyx` cimports
     `Integer64Vector` (never existed in new draken).
  2. `opteryx/operators/_operators.pyx` via `_factory.pxi` cimports
     `Integer64Vector`, `DecimalVector`, etc.
  3. `opteryx/expression/evaluator/evaluation.pyx` Python-level imports of
     `Integer64Vector`, `StringVector`, `scalar_constructors.from_scalar`,
     `interop.vector_sequence.bool_vector_from_uint64_eq` — old-draken
     surface that was being smuggled back in via shims.
  4. `_impl.so` cannot be refreshed until 1–3 are honest.

Gaps 1 and 2 belong to the operator-rewrite PM (they sit in
`opteryx/compiled/structures/` and `opteryx/operators/`). **Gap 3 is yours
and is the first concrete piece of work.** Gap 4 falls out automatically
once 3 lands.

- Three Cython shims survive E.25: `draken.vectors.vector`,
  `draken.vectors.bool_vector`, `draken.morsels.morsel`. These wrap the
  nanobind classes and give Cython something with `__pyx_vtable__` to
  cimport. The wrap cost is per-morsel, not per-row. Architect-approved
  Morsel helpers (`__len__`, `__getitem__`, column accessors) live on the
  Cython `Morsel`. **Do not extend this pattern to typed-Vector subclasses
  on your own authority** — that was E.24's drift. New draken has ONE
  Vector class; type dispatch is via `DrakenType` on the underlying struct.

## 4. The architecture you must respect

All settled. Do not relitigate. From the binding memory-file decisions
listed in `01_draken_state_at_handover.md` §2.2, the ones that bite the
evaluator hardest:

- **One Vector, dispatched by `DrakenType`.** Per
  `draken/docs/design/00_data_model.md` and §11 of CLAUDE.md. Where the
  evaluator currently imports `Integer64Vector`/`StringVector`/etc. as
  Python-level imports, it must migrate to importing `Vector` and reading
  `vec.type` to dispatch. The OpsTable layer in `draken/ops/` is where
  per-type behaviour lives, not in subclasses.
- **`feedback-no-false-green-clean-break`.** If a migration step makes
  something stop compiling, that is *information*. Surface it. Do not
  introduce a compatibility shim, a typedef alias, a verbatim-C struct
  pretending to be a real type, or a fallback `.py` re-import that keeps
  the build green at the cost of correctness. E.24 did all four; E.25
  reverted them. **Do not repeat them.** Recognise these patterns:
  - Comments that say "for compatibility" / "runtime correctness is a
    separate concern" / "TODO: real impl later" → fake-green.
  - `cdef extern from *: """..."""` blocks declaring structs that don't
    exist in the real header → fake-green.
  - Aliases that restore an old name the architect deliberately renamed →
    fake-green.
- **`draken-string-type-family`** — `DRAKEN_VARCHAR` is the new name for
  `DRAKEN_STRING`. The rename was deliberate. Don't add the alias back.
- **`draken-float-nan-semantics`** — NaN=NaN canonicalised, sorts highest;
  -0.0=0.0 canonicalised. The evaluator must produce results consistent
  with this. The native ops already do; just don't paper over it in glue.
- **`draken-consumer-edge-pattern`** — pure nanobind C++ at the Python edge,
  typed `.pyx` only. Zero `object` parameters or returns in compiled
  Cython. `<object>` casts at a `def` function's RETURN to box scalars are
  the documented §02 exception — that's the only legitimate `object` use
  in your code.
- **`feedback-hash-no-parity`** — hash values are disposable; the evaluator
  must not require any cross-version hash compatibility.

## 5. The §12 design forks that touch you

Open questions in
`opteryx/operators/docs/design/00_operators_and_parallelism.md` §12. The
ones that affect evaluator implementation:

- **§3.1 — `_push_impl` purity contract.** The bytecode VM's inner loop is
  already `noexcept nogil`. If the architect closes §3.1 as "`cdef ...
  nogil` signature on `_push_impl`", every helper the VM calls must be
  `nogil`-clean. That includes anything you change in the bytecode VM's
  postpass. Track the decision; don't pre-commit either way.
- **§5.4 — termination primitive (`threading.Event` vs C atomic vs
  `volatile`).** The bytecode VM reads `PipelineContext.terminate` between
  morsels. If the closure picks C atomic, the VM's reader changes shape.
  Don't write VM changes that pre-commit to one option.

Surface to the architect when you hit a decision point that requires one
of these closed. Do not close them yourself.

## 6. How to scope and write tickets

You will assign work to agents. Three patterns from the draken rebuild that
cost real time:

### 6.1 Ticket spec bugs

Three tickets during the draken rebuild had spec bugs (allowed `object` in
`.pyx`, demanded hash parity, missed the `__pyx_vtable__` issue). Each was
caught and corrected, but the cost was wall-clock. Before writing any
ticket:

1. Check it against `CLAUDE.md` §3 and §9.
2. Check it against the relevant memory files (the
   `draken-consumer-edge-pattern` memory in particular).
3. Check it against the relevant binding decisions in
   `01_draken_state_at_handover.md` §2.2.
4. Write the acceptance criteria *concretely*: file paths, exact commands
   to run, exact expected output. "Tests pass" is not an acceptance
   criterion.

### 6.2 Ticket size

Phase 20a touched 57 files (drifted from a small scope). E.24 touched 144
files (was supposed to touch 5). Both passed an "I am done" claim. Both had
to be substantially redone.

Rules of thumb:

- A ticket that opens with "audit X" or "rewrite all Y" is too big.
- A ticket that names <5 files in scope, gives explicit STOP conditions,
  and a clear acceptance test, behaves better.
- STOP conditions are not optional. Every ticket needs ≥1. Examples:
  - "If you find yourself editing more than ~5 files, stop and report."
  - "If a fix requires changing the nanobind layer, stop."
  - "If `make dt` regresses, stop."
- Acceptance criteria are concrete (`ls -la X.so` shows the file;
  `python -c "..."` runs without traceback). Not "looks good".

### 6.3 Trust-but-verify

Every "I am done" claim from an agent gets independently verified by you
before you accept it. Do not skip this. Three patterns that hid drift in
the draken rebuild:

- **The headline-metric trap.** "make q went from 0 to 111" sounds great
  but says nothing about whether the 111 are honest. Read the diff. Read
  the comments in the diff. Read the new files. The damning evidence in
  E.24 was a comment in the agent's own code (*"Runtime correctness is a
  separate concern"*).
- **The scope-drift trap.** Verify with `git diff --stat HEAD~1 HEAD`.
  Compare the file list to the ticket's allowed scope. >2× over is a
  problem; >5× is a failed ticket.
- **The "tests pass" trap.** Tests passing on a fake-green substrate is
  worse than tests failing honestly. Inspect for compatibility shims,
  alias re-introductions, suspect typedefs.

Verify before writing the next ticket. Always.

### 6.4 The §3 trap

`object`-typed Cython is the single most expensive class of mistake.
Recognise the bait:

- `cpdef object f(object x)` in a `.pyx` you control → wrong.
- `cdef extern from "..." : ... object foo(object)` → wrong.
- `cdef object _nb` as a *field* on a cdef class → **fine** (storing a
  reference to a Python object is allowed; the field is typed).
- `<object>` cast at the RETURN of a `def` function to box a scalar dict →
  fine (the §02 documented exception).

If a ticket needs to type a parameter as `object` to compile, the ticket
is wrong, not Cython.

## 7. Hard-won lessons specific to your area

### 7.1 The two halves of `evaluation.pyx` are different beasts

Per E.20 §3.1 and §3.2, the upper half (~lines 1–650) is a tree-walker
using Python-level `import`. The lower half (~lines 650–1499) is a
three-phase bytecode VM with `cimport` and a `noexcept nogil` inner loop.

The upper half migrates to uniform `Vector` + `DrakenType` dispatch as
straightforward import-line changes plus runtime type checks. The lower
half migrates more carefully — its cimports drove the entire
`__pyx_vtable__` saga, and its inner loop must stay `nogil`-clean. **Treat
them as separate migrations.** Don't bundle them into one ticket.

### 7.2 The cythonize-batch cascade

Before E.22 (`make draken` / `DRAKEN_BUILD=1`), a single Cython error in
any `.pyx` aborted the whole `cythonize()` batch and wiped draken's `.so`s.
This recurred across four phases before being solved. For your work:

- Use `make draken` to confirm draken stays intact while you change
  opteryx-side files.
- Full `make c` / `make compile` still depends on all `.pyx` compiling. If
  it breaks because something *outside* the evaluator broke, that is not
  your bug. Report it, don't fix it.

### 7.3 The bytecode VM postpass

E.24 added a `bool_vector_from_bits` function to `draken_native.cpp` to
wrap bitmap results into Vectors for the VM postpass. E.25 reverted it.
The existing bridge surface (`draken_vector_own_raw` from
`draken/core/draken_bridge.h`) already does this. If your migration of the
VM postpass needs a bitmap-to-Vector wrapper, **use the existing bridge,
don't extend the nanobind layer.** If the existing bridge is genuinely
insufficient, surface it as a draken-side ticket to the architect.

### 7.4 The "interop helper" trap

`evaluation.pyx` imports things like
`interop.vector_sequence.bool_vector_from_uint64_eq`. These are old-draken
helpers that don't exist in new draken's surface. **Do not recreate them
under new draken's tree.** The migration is to express the operation in
terms of the uniform Vector + DrakenType dispatch via the existing C′
nanobind extensions or `draken/ops/`. If the operation doesn't exist in
the C′ surface yet, that is a draken-side gap to surface.

## 8. Suggested first three tickets

A starting punch list — adjust as you learn the code, but these are the
honest first moves.

### Ticket 1 — Migrate `evaluation.pyx` upper half off typed Vector imports

Replace Python-level `from draken.vectors.integer64_vector import
Integer64Vector` (and similar for String/Float/Decimal/Timestamp/Date32/
Interval/Array) with `from draken.vectors.vector import Vector`. Replace
runtime `isinstance(col, Integer64Vector)` with checks on `col.type ==
DRAKEN_INT64`. Replace constructor calls with whatever uniform-Vector
constructor pattern the bridge exposes.

Scope: `evaluation.pyx` upper half only (lines 1–~650). Acceptance: the
file compiles; the upper-half tests in the evaluator's test set (if any)
pass.

### Ticket 2 — Replace `bool_vector_from_uint64_eq` with the equivalent uniform op

This is a specific old-draken helper the evaluator depends on. Find its
new-draken equivalent in `draken/ops/` or `opteryx/compiled/nanobind/`. If
no equivalent exists, surface as a draken-side gap. Either way, update
`evaluation.pyx`'s reference.

Scope: one symbol, one or two files. Acceptance: import line replaced;
the function still works at runtime against a smoke test you write.

### Ticket 3 — Bytecode VM cimport migration

Lower half of `evaluation.pyx`. The cimports of `BoolVector`, `Vector`,
`Morsel` now resolve to the Cython shims from E.24 (with `__pyx_vtable__`).
Confirm that path works end-to-end. If it doesn't, surface the failure
mode — do not paper over.

Scope: `evaluation.pyx` lines ~650–1499. Acceptance: `_impl.so` builds;
`from opteryx.expression.evaluator import _impl` imports cleanly with
zero nanobind ref-leak warnings; the VM passes a smoke test against a
known input.

After these three, your picture of the remaining work will be sharp
enough to plan the rest as a sequence — `comparisons.pyx`, `arithmetic.pyx`,
`case_eval.pyx`, `string_ops.pyx`, `temporal_ops.pyx`, `json_ops.pyx`,
`type_coercion.pyx`. Most should be smaller than evaluation.pyx itself.

## 9. Cadence

- **Small tickets, often.** Hours, not days. A ticket that takes >1 agent
  work session is probably two tickets in a trenchcoat.
- **Verify every completion claim** (§6.3). No exceptions.
- **`make q` is the gate** for each ticket. The handover baseline is 0/133;
  every ticket should keep or improve it. A ticket that regresses `make q`
  is a failed ticket, even if the diff "looks reasonable."
- **Surface to architect early.** Better to ask three questions and get
  three "your call, proceed" answers than to discover after a ticket lands
  that the architect would have decided differently. The architect's time
  is cheaper than a wrong week.
- **Memory files are durable.** When the architect closes a decision,
  capture it in a memory file (or surface that it should be) before moving
  on. Lessons that aren't written down get re-learned.

## 10. Who to ask

- **Architect** — design forks, type semantics, library choices (boost::locale
  vs ICU for case folding), parallelism §12 closures.
- **Outgoing draken-rebuild PM** — questions about the bridge, the shim,
  the ABI guard, why a specific draken-side decision went the way it did,
  whether a draken-side gap should be a new ticket or an existing one
  reopened.
- **Operator-rewrite PM (downstream)** — gaps 1 and 2 from §3 are theirs;
  coordinate when your work depends on theirs or vice versa.

## 11. What "done" looks like

Your initiative ends when:

1. All ten `.pyx` files in `opteryx/expression/evaluator/` have been
   migrated off old-draken's typed-subclass surface to uniform `Vector` +
   `DrakenType` dispatch.
2. `_impl.so` builds and imports cleanly with no ref-leak warnings.
3. The four old-draken Python imports listed in §3 gap 3 are replaced with
   new-draken equivalents (none of them by reintroducing old surfaces).
4. The evaluator's own tests are green.
5. `make q` is materially higher than 0/133 — specifically, every test
   whose failure is *evaluator-caused* now passes. Tests that fail for
   operator-rewrite reasons (downstream `_operators.pyx`, `bloom_filter.pyx`,
   etc.) remain failing and are the operator-rewrite PM's work.
6. You write a handover doc (`01_eval_state_at_handover.md` alongside
   `00_pm_briefing.md`) capturing what's done, what isn't, and what the
   operator-rewrite PM needs to know about how the evaluator now looks.

That's the bar. Anything short of it is in flight; anything beyond it is
the next PM's work.

---

**A final note.** The draken rebuild was, at its worst, three days of
specs-and-corrections cycles in a row. It was, at its best, monotonic
progress on a substrate that didn't lie. The difference between the two
modes was almost always the discipline of small, scoped tickets with
acceptance criteria you could run, plus refusal to accept fake-green
completion. You inherit that discipline. Hold it.
