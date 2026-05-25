# Operators & Parallelism — Specification (DRAFT)

> Status: DRAFT for review/edit. Anchors the relational-operator rewrite that
> follows the draken C++-first rebuild. Two coupled goals: (1) finish the
> Cython-isation of the operator layer so every hot path is typed and
> `nogil`-clean; (2) turn on inter-operator pipeline parallelism on that
> substrate. This document is design, sequence, and gates — not code.

<!--
Authoring notes
- Style mirrors draken/docs/design/*. Frozen invariants live in fenced "RESOLVED"
  blocks. Open questions live in "UNRESOLVED" blocks with the option set and the
  cost of each. Do not "soft-resolve" by picking in prose.
- Source of truth for what an operator *is* today: opteryx/operators/_operators.pyx
  (BasePlanNode, PipelineContext, push/emit, drive_scan) and
  opteryx/managers/execution/serial_engine.py (the driver).
- Source of truth for the data flowing through: draken/src/core/buffers.h
  (DrakenVector) and Morsel. CLAUDE.md §11 governs.
-->

## 0. Why now

The draken rebuild has pushed vectors, kernels, hashing, and the dispatch table
into a C++-first layer with nanobind edges and typed `.pyx` glue. The next
bottleneck is no longer per-row kernel cost — it is:

1. **Serial pipeline.** `serial_engine.execute` is single-threaded
   morsel-at-a-time. The GIL is released inside kernels, but the pipeline as a
   whole runs on one core. Star-schema fact-table probes and wide-table scans
   that survive projection both have abundant inter-operator parallelism we are
   leaving on the floor.
2. **Mixed-purity operators.** `BasePlanNode` and many operators are already
   `cdef class` and typed, but several still hold Python state, take `object`
   parameters in places, or call into Python helpers on the hot path. Turning
   on a thread pool over operators that are not `nogil`-clean turns latent
   Python access into real serialisation.

The two problems are entangled: parallelism only pays once the operators are
clean, and the discipline of "clean enough to release the GIL across the
pipeline" is what justifies finishing the Cython migration. This spec
addresses them together.

Scope explicitly **included**:
- `opteryx/operators/` — every operator in the catalog, push-pipeline and
  off-pipeline alike (per the /JJ/ note in §1.3, off-pipeline DDL/DML ops
  also Cython-ise, even if "mostly Python in Cython").
- `opteryx/expression/evaluator/` — the residual evaluator surface and the
  `vector_ops/*.pyx` files imported by it. These migrate in-place per
  operator, **not as a separate sweep** (handover doc §9).
- The scheduler / executor (`opteryx/managers/execution/`).

Scope explicitly **excluded**:
- Intra-operator parallelism (partitioned hash-joins, parallel sort merge,
  parallel group-by). Out of scope for v1; revisit after inter-op ships and
  measurements identify the operators that still dominate.
- Distributed / multi-machine execution.
- Planner changes beyond what the scheduler contract requires.
- Replacing the morsel abstraction.
- Draken-side work (UTF-8 cluster, regex cluster, heavy specials, decimal
  pt2, NVARCHAR ops). These are listed in the handover doc §4.2 and
  remain draken-team responsibility. **If an operator hits a missing op,
  surface a draken-side ticket — do not paper over with a `.pyx` loop
  (handover doc §7 landmine #4).**

## 1. Current state (as of 2026-05-24)

> **Handover input.** The draken rebuild's hand-off record lives at
> [`01_draken_state_at_handover.md`](01_draken_state_at_handover.md). That
> document is the operator rewrite's starting line and is binding. Read it
> before Phase 0 begins. The post-E.24 `make q` baseline is captured in
> `02_make_q_baseline_at_handover.md` (sibling, produced when E.24 lands) —
> that is where the operator rewrite picks up.

### 1.1 Pipeline shape
Push pipeline, compiled at plan time:
- `BasePlanNode` (in `opteryx/operators/_operators.pyx`) — `cdef class` with
  typed `push()` / `_push_impl()` / `emit()` and a C-level downstream pointer.
  Telemetry counters are typed `uint64_t`.
- `PipelineContext` — per-query shared state. Currently carries termination
  (a `bint`) for LIMIT short-circuit. Comment in the source already
  acknowledges it will need a real synchronisation primitive ("today: bint;
  later: threading.Event").
- `serial_engine.execute` — iterates scans, pushes morsels into the chain head,
  drains the terminal `ExitNode`'s pending queue. One thread.
- `drive_scan` — Cython function in `_operators.pyx` that walks one scan
  iterator.

### 1.2 Operator catalog
`opteryx/operators/catalog.py` already declares `ParallelStrategy`
(`SINGLE_THREAD` / `MULTI_THREAD` / `ASYNC`) and `is_pipeline_breaking` per
operator. **Today these fields are advisory only — nothing reads them at
execution time.** This is good news: the contract surface exists, it just is
not wired.

### 1.3 Cython-ness of operators
Mixed. Survey (counts approximate, to be re-taken at Phase 0):
- `_operators.pyx` (umbrella, foundation classes) — 757 lines, typed.
- `filter/filter.pyx` — typed.
- `hashed_inner_join/hashed_inner_join.pyx` — 845 lines, typed.
- `aggregate/` — multiple `.pyx` per aggregation kernel, typed.
- Several operators (`explain`, `set_variable`, `show_*`, DDL/DML, view/table
  management) are off the push pipeline by design and stay Python. /JJ/ these should be cythonized, even if they are mostly python in cython.

A full audit of which operators have `object` parameters, Python attribute
writes per morsel, or Python helper calls inside `_push_impl` is **Phase 0
gate work** (see §9). Do not assume cleanliness; verify.

### 1.4 Morsel & vector contract
Established by draken (CLAUDE.md §11 + `draken/src/core/buffers.h`):
- `DrakenVector` is 40 bytes, ABI-frozen (guarded at build time by
  `draken/core/_abi_guard`), accessed uniformly as `data[selection[i]]`.
- `Morsel` owns a set of vectors plus column metadata.

This contract is the **only** data contract between operators. Operators see
morsels of `DrakenVector`s; they do not see PyArrow, numpy, or Python
sequences on the hot path. This is non-negotiable.

### 1.5 The bridge & the shim (what operators call into)

Per handover doc §3:

- **`draken/core/draken_bridge.h`** — the C-callable surface. Hot path uses
  `draken_vector_unwrap(PyObject*) -> const DrakenVector*` (type-checked,
  fail-fast). Wrap newly built vectors with `draken_vector_own*`. This is
  the **strategic** consumer pattern for the operator rewrite.
- **Cython shim** (`draken/vectors/vector.pxd`, `bool_vector.pxd`,
  `morsels/morsel.pxd`) — wraps a nanobind handle plus its unwrapped
  `DrakenVector*` as a `cdef class`. Lets a `.pyx` `cimport` the type and
  call typed methods. **Transitional.** The architect's "A then B" call
  (handover §3.4) is: operator-side callers move to `draken_vector_unwrap`
  directly, and the shim eventually goes away.

For the operator rewrite this means: a new `.pyx` operator on the hot path
SHOULD prefer the bridge over the shim where it has a choice. Existing
operators on the shim are not blocked from running; the migration is
opportunistic, per operator. **Do not re-wrap inside per-morsel loops** —
the wrap cost is per-morsel, not per-row, and re-wrapping inside `_push_impl`
re-introduces Python work the lift was meant to eliminate.

### 1.6 Binding decisions inherited from the draken rebuild

The handover doc §2.2 enumerates architect calls captured in user-memory
that bind this rewrite. The operationally relevant ones:

- **`feedback-no-false-green-clean-break`** — no shim/bridging in the
  rewrite work itself; broken-until-rewritten is acceptable. Never fake
  green.
- **`draken-consumer-edge-pattern`** — Python edge lives in nanobind C++;
  `.pyx` is **typed-only, zero `object` params/returns**. The two-layer
  `.pyx` cdef-kernel + nanobind glue pattern was dropped; pattern is pure
  nanobind C++. This is the rule for every operator hot path.
- **`draken-german-string-format`** + **`draken-float-nan-semantics`** +
  **`draken-string-type-family`** — value semantics operators must honour.
- **`feedback-hash-no-parity`** — hash values are disposable across
  versions; operators must not assume hash stability.

These are **not** open for relitigation by the operator rewrite.

## 2. Target architecture (high-level)

Morsel-driven pipeline parallelism, in the style of Hyper/DuckDB but scoped
to inter-operator only for v1.

- **Operators are stateful `cdef class` objects.** Each operator instance
  belongs to exactly one pipeline. Operators expose a single hot entry point
  (`push(Morsel)`), typed and `nogil`-callable.
- **The scheduler owns the threads.** Operators do not spawn threads. The
  scheduler dispatches morsels to operator instances via a bounded work queue.
- **A "pipeline" is a chain of non-blocking operators terminated by a
  pipeline breaker or a sink.** Pipeline breakers (hash-join build, group-by
  build, sort, distinct, window) buffer input until their input stream is
  closed, then begin producing output as the head of the next pipeline.
- **Morsels flow downstream; control flows upstream.** Termination
  (`PipelineContext.terminate`) and backpressure both propagate from sinks
  back toward scans.
- **Order is not preserved across morsels** except where an operator
  explicitly enforces it (ORDER BY, LIMIT after ORDER BY, windowed ops).
  Within a morsel, row order is preserved.

Everything else in this document is the detail behind that picture, plus the
open decisions that determine which variant of it we build.

## 3. Operator contract (v2)

This section defines the contract every operator MUST satisfy to run on the
parallel scheduler. Operators that cannot yet satisfy it run on a
single-threaded fallback pipeline (see §9.2) — they are not deleted, they are
gated.

### 3.1 Hot path
- `cdef class` deriving from `BasePlanNode`.
- `_push_impl(Morsel)` must be `nogil`-clean: no Python object access, no
  Python attribute writes, no calls into pure-Python helpers. Per-morsel
  state lives in typed `cdef` fields.
- **Zero `object` params or returns on hot-path `cdef` functions**
  (consumer-edge-pattern, §1.6). The only sanctioned `<object>` cast is at
  the `def` boundary to box a final result — the CLAUDE.md §02 exception.
  Operator-rewrite tickets must call this out explicitly; it was the
  single biggest cause of failed tickets in the draken rebuild (handover
  §7 landmine #1).
- **No `.pyx` loops to fill draken op gaps** (handover §7 landmine #4).
  Math, comparison, hash, string-search etc. belong in `draken/ops/*.h`
  dispatched by `DrakenType`. If an op is missing, surface a draken-side
  ticket — do not inline a Cython loop.
- `emit(result)` is the only legal way to forward downstream. It MUST NOT be
  called with `EMPTY` or `None`. EOS is passed through `emit(EOS)` exactly
  once per pipeline.
- The operator MUST NOT retain a reference to an incoming morsel past its
  return from `_push_impl`, except where the operator is a pipeline breaker
  whose semantics require buffering (see §4).

### 3.2 State ownership
- **Per-instance state** (hash table, sort run, aggregator) lives in typed
  fields on the operator instance.
- **Per-query state** (termination, LIMIT counters, telemetry rollups) lives
  on `PipelineContext`.
- **No global mutable state.** No module-level dicts of caches. Caches live
  on the session.

### 3.3 Thread-safety obligations
- **Stateless operators** (Filter, Projection, Limit-passthrough,
  CrossJoin-probe, StringFunctions) are safe to run on multiple threads
  concurrently against multiple morsels from the same logical operator
  position **if** their state is read-only after construction. The scheduler
  may exploit this (§5.2).
- **Stateful non-breaking operators** (LIMIT with a counter,
  per-partition probes) need either a lock-free counter (atomic) or one
  instance per worker. Default: one instance per worker, results merged at
  the next breaker.
- **Pipeline breakers** are accessed by exactly one worker at a time during
  the build phase, or are partitioned by hash. v1 uses **one instance, one
  worker for build**; partitioned build is intra-operator parallelism, out of
  scope.

### 3.4 Lifecycle
1. **Construct** — at plan compile time, on the planner thread. Allowed to
   touch Python freely.
2. **Bind** — `pipeline_compiler.compile_pipeline` wires `_downstream` and
   `_ctx`. Still on the planner thread, still Python-allowed.
3. **Run** — `push()` calls arrive on scheduler threads. Hot path MUST be
   `nogil`-clean.
4. **Finalise** — on EOS, breakers emit accumulated state and call
   `emit(EOS)` downstream. Stateless operators just forward EOS.
5. **Teardown** — operator instances are dropped after the query completes.
   No `__del__` hooks on the hot path.

> ## UNRESOLVED — must `_push_impl` itself be `nogil`, or just `nogil`-clean?
>
> "`nogil`-clean" means the body could run without the GIL but the method
> signature is plain `cdef`. "`nogil`" means the signature is `cdef ... nogil`
> and the compiler enforces purity. The latter is stronger but bans even
> incidental Python access (e.g. raising a Python exception, calling a
> CarcharSet method that isn't itself `nogil`).
>
> Options:
> - **A. Plain `cdef`, lint-enforced clean.** Maximum flexibility, weakest
>   guarantee. Risk: silent regressions where someone reintroduces a Python
>   call.
> - **B. `cdef ... nogil` on `_push_impl`.** Compiler-enforced. Forces every
>   downstream call (CarcharSet, PerfectHashSet, vector ops) to also be
>   `nogil`. Some of those are not today.
> - **C. Hybrid: `with nogil:` block inside `_push_impl`.** GIL is released
>   around the kernel core but the operator can still raise Python errors at
>   the edges.
>
> Recommendation pending architect: **B** is the only one that survives a
> year of code churn. The cost is auditing every `cimport`ed primitive and
> marking it `nogil` or wrapping it. Decide before Phase 1.

## 4. Pipeline breakers

A pipeline breaker is an operator that cannot emit any output until it has
seen all of its input. Breakers split a query into stages.

Confirmed breakers (v1):
- **Hash-join — build side.** Probe is non-breaking; build is.
- **Hashed group-by — build side.** Output begins when input EOS arrives.
- **Sort / TopK heap.** Sort is fully breaking; heap-based TopK can stream a
  bounded result on EOS.
- **Distinct (hashed).** Same shape as group-by.
- **Set operations (UNION/INTERSECT/EXCEPT) where dedup is required.**
- **Window functions over a frame that requires the full partition.** (v1
  may not support these in parallel mode; see §9.2.)

**Non-breakers** (streaming):
- Filter, Projection, Limit (with short-circuit), CrossJoin probe,
  HashJoin probe, AsofJoin probe (build is the breaker), most scalar
  function ops, UNION ALL, Unnest.

> ## UNRESOLVED — does LIMIT serialise the pipeline?
>
> LIMIT today calls `ctx.terminate()` when its quota is reached. In a parallel
> pipeline, multiple workers may be in `_push_impl` simultaneously when
> termination fires. Two questions:
>
> 1. **Counter race.** A naive `cdef int counter` overcounts under
>    concurrency. Options: (a) atomic counter; (b) per-worker counter +
>    coordinator that decides when total ≥ N; (c) serialise LIMIT (run it on
>    a dedicated single-threaded stage). (c) is simplest but caps throughput
>    at LIMIT's single-thread rate.
> 2. **Determinism.** Without ORDER BY, LIMIT is allowed to return any N
>    rows. With parallel execution this becomes *more* non-deterministic
>    (depends on worker scheduling, not just scan order). Is that acceptable?
>    Most engines say yes; users sometimes complain.
>
> Recommendation pending architect: (b) for the counter, accept the
> non-determinism, document it. But this is a UX call as much as a
> technical one.

## 5. Scheduler

### 5.1 Shape
**Morsel-driven, work-stealing, fixed thread pool sized to physical cores.**

- One global pool per session (or per query — see UNRESOLVED below).
- Workers pull morsel-tasks from a deque. Stealing happens across workers.
- A "task" is the tuple `(operator_instance, morsel)`. Executing a task means
  calling `operator.push(morsel)`; whatever the operator emits becomes one
  or more new tasks for its downstream.
- The scan layer is the *source* of tasks: each scan worker reads one morsel,
  enqueues a task for the chain head, and pulls the next morsel.

### 5.2 Operator multiplicity
Three patterns, picked per operator from the catalog:

| Pattern              | Instances | Used for                                    |
|----------------------|-----------|---------------------------------------------|
| **shared-stateless** | 1         | Filter, Projection, scalar functions        |
| **per-worker**       | N         | LIMIT (per-worker counter), per-thread temp |
| **single-instance**  | 1         | All breakers' build side; ORDER BY          |

Default is shared-stateless. The catalog already has
`ParallelStrategy.SINGLE_THREAD` — we extend it to express these three.

### 5.3 Backpressure
Bounded work queues. When the queue between two stages reaches its high-water
mark, the scan layer pauses morsel production (does not pull the next morsel
from the IO layer) until the queue drains below the low-water mark.

This is **the** lever that prevents a fast scan + slow aggregate from OOMing.
It must be present from v1. Suggested defaults: queue depth 4× worker count,
high-water = depth, low-water = depth/2. Tune empirically.

### 5.4 Termination
`PipelineContext` becomes the broadcast channel. `terminate()` sets a flag
that scan workers and breakers check between morsels. Workers in-flight in
`_push_impl` continue to completion (cannot interrupt without unsafe state).
The flag's type changes from `bint` to an atomic-readable equivalent;
exact primitive is an UNRESOLVED below.

> ## UNRESOLVED — thread pool: per-session or per-query?
>
> Per-session: one pool reused across queries. Lower overhead, better cache
> warmth for repeated workloads (e.g. dashboards). Concurrent queries share
> threads — long-running ones starve short ones unless we add fair
> scheduling.
>
> Per-query: one pool per `session.execute`. Simpler isolation, no fairness
> problem. Overhead per query (~ms to spin up threads), wasted if many
> small queries hit the engine.
>
> Recommendation pending architect: **per-session, fixed size**, with a
> simple priority hint (short queries jump the queue). Defer fair scheduling.
> But this depends on the deployment model — embedded library vs.
> long-running server matters here.

> ## UNRESOLVED — what primitive backs PipelineContext termination?
>
> Source comment says "today: bint; later: threading.Event". Options:
> - **A. `threading.Event`.** Cross-thread visible, but reading it requires
>   the GIL — breaks the `nogil` rule for the operator hot path.
> - **B. C-level `atomic_bool`.** `nogil`-readable, but Cython's stdatomic
>   support is awkward; may need a tiny C shim.
> - **C. `volatile bint`.** Works on x86 and ARM in practice for this
>   single-writer-many-reader pattern. Technically UB by the C standard but
>   universally accepted. Many engines do this.
>
> Recommendation pending architect: **B** via a small `atomic.h` shim in
> `opteryx/compiled/structures/`. Decide before Phase 2.

## 6. Morsel sizing & flow

Star-schema and wide-table workloads pull in different directions on morsel
shape. The scheduler must serve both, not optimise one.

- **Star schema fact probes:** want **tall and narrow** morsels — many rows,
  few columns surviving projection. Probe throughput dominates; we want as
  much data in cache per probe as possible.
- **Wide tables:** want **narrow morsels in column count** — the win is not
  reading the 200 columns we don't need. After projection pushdown, the
  morsel is naturally narrow.

Resolution: **morsel size is bounded by row count, not byte count, at the
scan boundary** (today's behaviour — keep it). Projection pushdown is the
mechanism that handles wide-table column reduction; it happens before the
morsel exists.

Open question: should the scheduler resize morsels mid-pipeline (e.g. after
a highly selective filter, coalesce into fuller morsels)? Today it does not.
Coalescing reduces per-morsel overhead downstream but adds a copy.

> ## UNRESOLVED — coalesce after selective filters?
>
> Options:
> - **A. Never coalesce.** Simplest. Highly selective filters produce small
>   morsels that under-utilise downstream ops.
> - **B. Coalesce when output morsel is below a threshold (e.g. 25% of
>   target).** Adds a coalescing stage with its own buffer; non-trivial
>   memory ownership.
> - **C. Push selection vectors instead of materialising filter output.** The
>   filter emits an unchanged morsel + a selection bitmap; downstream ops
>   read through it. This is the draken-native model (selection vectors
>   already exist on every vector). Most elegant; requires every operator to
>   handle non-identity selection vectors correctly, which §1.4 already
>   mandates.
>
> Recommendation pending architect: **C** — it composes with what draken
> already is. Cost: every operator audit must confirm selection-vector
> correctness, not just `nogil`-cleanness. This is a real lift.

## 7. Per-operator notes

Notes here are scoping signals, not implementation. Read §3 for the contract
every operator must meet; this section calls out where each operator
*differs* from the default shared-stateless pattern.

| Operator                  | Pattern              | Breaker? | Notes |
|---------------------------|----------------------|----------|-------|
| Reader / Scan             | per-worker (per file)| no       | One worker per scan partition; today's IO concurrency already exists at this layer, integrate not replace |
| Filter                    | shared-stateless     | no       | Selection-vector emission if §6 resolves to C |
| Projection                | shared-stateless     | no       | Pure compute, easy first target |
| Hashed Inner Join         | single-instance      | build    | Build side serial in v1; probe is shared-stateless against the built table |
| Outer Join                | single-instance      | build    | Same as inner; null-fill on probe side |
| Filter Join (semi/anti)   | single-instance      | build    | Same |
| Asof Join                 | single-instance      | build    | Same; probe is stateful per-key, may need per-worker probe state |
| Non-equi Join             | single-instance      | build    | Likely stays serial in v1 |
| Nested Loop Join          | single-instance      | yes      | Serial; perf hazard, leave alone |
| Cross Join                | shared-stateless     | no       | Right side already materialised |
| Unnest Join               | shared-stateless     | no       | |
| Distinct                  | single-instance      | yes      | Hashed dedup |
| Grouped Agg Hashed        | single-instance      | yes      | Build serial in v1; intra-op parallelism is the obvious next target after v1 ships |
| Aggregate (ungrouped)     | shared-stateless+merge | yes    | Per-worker partial aggregate, merged on EOS. Easy parallel win |
| Sort / Heap Sort          | single-instance      | yes      | Serial in v1 |
| Limit                     | per-worker           | no       | See §4 UNRESOLVED |
| Union                     | shared-stateless     | only if DEDUP | UNION ALL is trivial; UNION needs Distinct |
| Parquet Read              | per-worker           | no       | Scan-side, see Reader |
| Null Reader               | shared-stateless     | no       | |
| Function Dataset          | shared-stateless     | no       | |
| Explain / Show* / DDL     | off-pipeline, Cython | n/a      | Off the push pipeline by design, but still ported to `cdef class` per /JJ/ §1.3. Hot path is irrelevant here; the Cython lift is for consistency and to keep the build-time `object`-ban check meaningful across the whole catalog |
| Insert                    | sink, single-instance| yes      | Stays as today |

**Ungrouped aggregate** is the cheapest big parallel win: every worker keeps
a tiny partial-aggregate struct, on EOS the per-worker partials are merged.
Worth landing first as the proof point.

## 8. Planner / compile-time contract

The planner does **not** decide degree of parallelism. The scheduler does.
The planner's only obligation is to keep the catalog accurate
(`ParallelStrategy`, `is_pipeline_breaking`, multiplicity pattern).

`pipeline_compiler.compile_pipeline`:
- Today: builds a single push chain by wiring `_downstream` pointers.
- Target: builds a **pipeline DAG** where edges that cross pipeline breakers
  are marked. Within a pipeline segment, the chain is unchanged. The
  scheduler drives one segment at a time per source morsel.
- No change to the planner above this layer. Physical plan → operator
  instances mapping is preserved.

> ## UNRESOLVED — does an operator declare its own multiplicity, or does the catalog?
>
> The catalog (`opteryx/operators/catalog.py`) is the design source of truth
> per CLAUDE.md §5 (operator classes stay clean). But some operators'
> multiplicity depends on query shape (e.g. an aggregate with no group keys
> is shared-stateless+merge; with group keys it is single-instance for v1).
>
> Options:
> - **A. Catalog declares; operator overrides at construct-time** by setting
>   a flag.
> - **B. Catalog declares the *capability set*; planner picks the actual
>   pattern** based on the operator's configuration.
>
> Recommendation pending architect: **A** — catalog is the contract,
> operator self-classifies on `__init__` if it has reason to. Decide before
> Phase 1.

## 9. Sequencing & gates

Phased like the draken rebuild: each phase is shippable on its own and
gated by `make q` green plus a behavioural check.

### Phase 0 — Audit & instrument (no behaviour change)
Goal: know exactly which operators meet the §3 contract today and which
don't, so the migration is finite and visible.
- **Start with Filter as a worked example** (handover doc §8). Filter is
  small, stateless, hot-path on essentially every query, and exercises the
  Cython↔nanobind seam against a real operator. The Filter audit is the
  template; generalise only after it is reviewed and accepted.
- Static audit of every `.pyx` operator: list `object`-typed parameters,
  Python attribute accesses inside `_push_impl`, calls to non-`nogil`
  helpers, and selection-vector handling (the latter only if §6 resolves
  to option C). Produce a per-operator report in
  `opteryx/operators/docs/audits/`.
- Add a build-time check (analogous to the PyArrow check) that flags
  `object` parameters on `_push_impl` and forbids new ones. Existing
  violations are whitelisted, not silently passed.
- Add per-operator telemetry — already partly there. Make sure every
  operator increments `records_in`/`records_out`.
- **Starting line:** `02_make_q_baseline_at_handover.md` (the post-E.24
  `make q` state). Per-failure categorisation comes from that document;
  this phase does not try to *close* failures, only to record what they
  cost the parallel rewrite.
- **Gate:** Filter audit reviewed; report exists for every other operator;
  build-time check passes on current code; whitelist is exhaustive.

### Phase 1 — Operator cleanup (still single-threaded)
Goal: every operator in the catalog (excluding the off-pipeline DDL/DML set)
is `nogil`-clean per §3.1. No scheduler changes.
- One operator at a time. Each lift is its own PR.
- Whitelist shrinks; build-time check tightens.
- **Gate:** `make q` green after each operator; whitelist empty at phase
  end.

### Phase 2 — Scheduler skeleton (single thread, but on the new shape)
Goal: replace `serial_engine.execute` with a scheduler that *could* run
multiple threads, configured to one. Pipeline DAG, work queue, bounded
backpressure, all in place.
- One worker, one scan thread (or even same thread). Functional change is
  zero; structural change is large.
- `PipelineContext` termination primitive migrates to atomic (§5.4
  UNRESOLVED).
- **Gate:** `make q` green; `make clickbench` no regression > 3%.

### Phase 3 — Turn on parallelism for shared-stateless operators
Goal: scans, filters, projections, scalar functions run on N workers.
Breakers still single-instance.
- The cheapest, lowest-risk parallel win. Most workloads benefit
  immediately.
- **Gate:** `make q` green; ClickBench wide-scan queries (Q1–Q4 territory)
  improve meaningfully; no regression on join-heavy queries.

### Phase 4 — Parallel ungrouped aggregate (partial+merge)
Goal: prove the per-worker+merge pattern on the simplest operator that
benefits from it.
- **Gate:** `make q` green; aggregate-heavy queries improve; result
  numerical stability verified against serial.

### Phase 5 — Parallel hash-join probe
Goal: probe runs on N workers against a single shared (immutable post-build)
hash table.
- This is where star-schema fact-table queries should see the headline
  improvement.
- **Gate:** `make q` green; ClickBench join queries improve; JOB-shape
  workloads improve.

### Phase 6 — Catalog hygiene & lock-down
Goal: the catalog and the scheduler are the only places that know about
parallelism. The build-time check forbids regressions. Document the
contract.

### 9.1 What we do NOT do in v1
- Partitioned hash-join build (intra-op).
- Parallel sort (intra-op).
- Parallel group-by build (intra-op).
- Adaptive degree-of-parallelism per query.

These are the v2 work. Their absence is **deliberate** — v1 is about turning
the engine into a parallel substrate without rewriting every operator.

### 9.2 Fallback path for non-conformant operators
Any operator that cannot meet §3 by its phase gate runs on a
**single-threaded fallback driver** that looks like today's `serial_engine`.
The query containing it loses inter-op parallelism but still runs. This is
the "fail fast, fail clean" mode — we never silently degrade *correctness*,
but we visibly degrade *parallelism* with a logged note, until the operator
is lifted.

## 10. Invariants that hold for the whole project

- **Morsel/vector ABI is the draken ABI.** No new vector layouts. No new
  morsel field that an operator reads on the hot path without going through
  the existing accessors.
- **Operator hot paths are `nogil`-clean** (see §3.1 + UNRESOLVED). Once an
  operator is lifted, it stays lifted — the build-time check enforces it.
- **The catalog is the single source of truth for parallelism strategy.**
  Operators do not store parallelism flags in arbitrary locations.
- **`make q` is green after every merged change.** No exceptions.
- **No `import opteryx` from inside a `_push_impl` hot path.** Sentinels
  (`EOS`, `EMPTY`) are resolved once at module init, already done in
  `_operators.pyx`.
- **No `try/except` for flow control on the hot path** (CLAUDE.md §9).
- **Selection vectors are honoured by every operator** (CLAUDE.md §11). The
  scheduler's correctness assumes this.
- **No `.pyx` loops to fill draken op gaps.** Missing ops are draken-side
  tickets, never operator-side workarounds (handover §7 landmine #4).
- **`object` params/returns are banned on `cdef` hot-path functions.** The
  only sanctioned `<object>` cast is at the `def` boundary to box a final
  result. The build-time check (Phase 0) enforces this.
- **Prefer the bridge over the shim.** New hot-path code goes through
  `draken_vector_unwrap` / `draken_vector_own*`. Existing shim consumers
  are migrated opportunistically per operator, not in a separate sweep.

## 11. Risks

| # | Risk                                                                 | Mitigation |
|---|----------------------------------------------------------------------|------------|
| 1 | An operator looks `nogil`-clean but a transitive Cython call reacquires the GIL silently, serialising the pipeline | Phase 0 audit + compiler-enforced `nogil` signatures (§3.1 option B) |
| 2 | Backpressure tuning wrong → OOM on fast-scan/slow-aggregate query    | Bounded queues from v1; bench during Phase 3 with adversarial workload |
| 3 | LIMIT semantics changes (more non-determinism)                       | Document; surface in release notes; offer `--ordered-limit` if users push back |
| 4 | Hash-join build remains the bottleneck                               | Acknowledged; v2 work; ungrouped agg + probe parallelism are the v1 wins |
| 5 | Selection-vector handling has gaps in some operators                  | Phase 0 audit includes a selection-vector correctness pass per operator |
| 6 | Per-session pool starves under concurrent queries                    | Defer fair scheduling to v2; document the limit; offer per-query pool flag as escape hatch |
| 7 | Coalescing decision (§6 UNRESOLVED) made wrong way → wasted lift     | Resolve before Phase 1 — operator audits depend on it |
| 8 | `_push_impl` `nogil` decision (§3.1 UNRESOLVED) made wrong way       | Resolve before Phase 1 — every operator lift depends on it |
| 9 | Cython↔nanobind seam surprises during operator audits (handover §7 landmine #6 — every late-rebuild surprise came from this seam; not every operator has exercised it yet) | Filter as worked-example audit (Phase 0); seam issues surfaced there before generalising; expect 1–2 discoveries |
| 10 | A ticket grows past its STOP condition (handover §7 landmine #3 — Phase-20a-style drift, 57 files in one ticket → agents pick stubs over migration) | Every operator-rewrite ticket carries an explicit STOP condition (file count + scope) like the E.24 ticket |
| 11 | An operator quietly inlines a `.pyx` loop to work around a missing draken op (landmine #4) | §3.1 + §10 invariant + reviewer discipline; reviewer rejects the loop and the gap becomes a draken-side ticket |

## 12. Open questions for architect review (consolidated)

The UNRESOLVED blocks above, listed here for ease of review:

1. **§3.1** — Plain `cdef`, `cdef nogil`, or `with nogil:` block for
   `_push_impl`? (recommendation: `nogil` signature.)
2. **§4** — LIMIT counter: atomic, per-worker+merge, or serialised stage?
   And is the extra non-determinism acceptable? (recommendation:
   per-worker+merge, accept and document.)
3. **§5.4** — Thread pool: per-session or per-query? (recommendation:
   per-session, fixed, no fair scheduling in v1.)
4. **§5.4** — Termination primitive: `threading.Event`, C atomic, or
   `volatile bint`? (recommendation: C atomic via small shim.)
5. **§6** — Coalesce after selective filters: never, by threshold, or via
   selection vectors? (recommendation: selection vectors — the
   draken-native path.)
6. **§8** — Multiplicity pattern declared by catalog, or by operator
   `__init__`? (recommendation: catalog declares default, operator may
   override at construct-time.)

Each of these is a design fork, not an implementation detail. Nothing
should be coded until they are closed.

---

> ## Meta — what this document is NOT
>
> - **A plan.** A plan implies the questions are answered. They are not.
> - **A claim that any of this is built.** None of it is. The infrastructure
>   (catalog, `BasePlanNode`, `PipelineContext`) is in place; nothing reads
>   it for parallelism yet.
> - **A claim that performance will improve by X%.** Numbers come from
>   measurement, not specs. The hypotheses in §7 are hypotheses.
