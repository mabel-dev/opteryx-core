# 04 — Refocus

> A short re-anchor, not a re-onboard. Read `00_pm_briefing.md` once for
> the rules; read `03_native_eval_engine_design.md` for the plan. This
> document is here because the work drifted. It tells you the one thing
> to do next and what to stop doing.

---

## Where you actually are (corrected 2026-05-28 — this doc had gone stale)

This file previously said "do the DV* stack rewrite next." That was
already done in the tree. Verified against `evaluation.pyx`:

| Piece | State |
|---|---|
| Phase 1 — instruction-word PyObject* → C ints | ✅ done |
| Phase 2 — BC_LEGACY → BC_CASE / BC_EXTRACTION | ✅ done |
| Phase 4 — DrakenVector* stack | ✅ done — `dv_stack[64]`+`dv_store[64]`+anchor list; arena create/destroy; `draken_compare_dv` (BC_COMPARE) and `draken_arithmetic_dv` (BC_BINARY_OP) wired; no `cdef list stack` remains |
| Phase 3 — BC_FUNCTION dispatch | 🟡 in-scope part done; native table gated out-of-scope (below) |

Matches doc 03's own status line (Phase 4 Stage B+C complete). The
framework refactor — instruction word, no BC_LEGACY, DV* stack, arena,
native compare/arith/boolean — is **complete**.

## Phase 3 — the honest state

Doc 03 lists five per-call overheads for BC_FUNCTION. Four are already
removed (the "incremental Phase 3" doc 03 records): bind-time `nb_func`
flag (no runtime type check), typed `._nb` access (no `getattr`), direct
per-arity dispatch for arity 1–3 (no per-call list, no `*args`). Only
arity>3 still builds a list.

The fifth — a `function_id` → C `eval_fn_t` indexed table — is the only
item left, and **it cannot be done in scope.** An `eval_fn_t` is a C
function pointer; every function target is a Python/nanobind callable.
A `function_id` table whose entries all just call Python is pure
indirection with zero benefit — inert scaffolding. Making it real needs
**native, C-callable function kernels**, which live outside
`opteryx/expression/` (draken / `opteryx/compiled/nanobind`). Same
applies to CAST / EXTRACTION / UNARY / CASE, which doc 03 explicitly
keeps Python-mediated until their native kernels exist.

This is the *same* out-of-scope native-kernel dependency the Phase 9
detour was (badly) trying to satisfy. It is a different component's
deliverable, requested via the PM protocol — not engineered around here.

## The decision in front of the architect

The eval-engine framework refactor is done. The remaining work (native
per-kernel dispatch) is gated on native kernels owned elsewhere. Choose:

1. **Call it done.** Accept function/cast/extraction/case as
   Python-mediated (correct, just not GIL-free). The framework is in
   place to wire native kernels later with no re-architecture.
2. **Request the native kernels** (function/cast) from the owning PM,
   then Phase 3 closes as the trivial indexed-dispatch reshape doc 03
   describes. Surface the request; do not reach across the boundary.

Do **not** build the dispatch table against Python callables — that's
the inert-scaffolding anti-pattern the kernel audit already found.

## Stop doing

- **Stop touching `draken/`.** The kernels you need are delivered. If
  one is missing a type, that's a draken-PM request (surface it; don't
  reach into `draken/ops/` or `draken_native.cpp` yourself). The
  protocol stands even though one batch of your small fixes was
  accepted — that worked because they were correct, not because the
  protocol is loose.
- **Stop expanding scope beyond `opteryx/expression/`.** Operators,
  managers, connectors are not yours. If the stack rewrite seems to
  need a change there, surface it — it's the operator-PM's, and
  doing it for them hands them a change they didn't write and can't
  test.
- **Stop adding `cdef object`.** Every time it appears it's either a
  missing typed surface (surface it) or laziness (type it). There is
  no third reason.
- **Stop chasing the descriptor problem in code.** It's a design
  decision (α/β/γ), not something to engineer around with a workaround.
  Pick one, tell draken-PM, move on.

## The gate

`make q` after every change. It does not regress. A change that drops
the pass count is a failed change, even if the diff looks clean. If
`make q` can't run because the engine won't build, the build break is
the only thing that matters until it's fixed.

## If you're unsure what to do

Re-read doc 03's phase table. Do the next undone phase, in order. Don't
invent work that isn't on it. The plan is good; the failure mode is
wandering off it, not the plan being wrong.
