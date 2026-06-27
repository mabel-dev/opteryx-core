# Native Execution Scheduler — Design

Status: **DRAFT for architect ratification.** No code until sign-off.

## 0. Goal

Remove the last Python island on the execution path. Today the operator
pipeline is native (`cdef class` operators, a `nogil` `push(shared_ptr[CxxMorsel],
ErrCtx*)` drive surface), but the thing that *schedules and drives* it —
`opteryx/managers/execution/scheduler_engine.py` and the per-shape handlers in
`parallel_engine.py` — is Python: `queue.Queue` hand-off, `threading.Lock`/
`Event`, a Python `Event`/`Executor` DAG, and per-morsel `push_one`/`pull_one`
shim calls that bounce the GIL once per morsel.

This is a **ground-up rewrite as a Cython executable**: declared `cdef` types,
explicit C++ ownership and lifetimes, native synchronisation. Not a `.py`→`.pyx`
recompile.

Per CLAUDE.md §1/§2 the line is by PHASE. `identify_segments` (cutting the plan
into pipeline segments) stays on the **planning** side. Everything past it —
DAG, drive loop, hand-off, fan-out — becomes native.

## 1. Two sanctioned boundaries (the only Python crossings)

1. **Plan in.** Python planning hands a compiled segment list to the Cython
   `execute(plan, segments, dop, telemetry)`. One crossing, at query start.
2. **Result out.** The cursor materialises the final morsel to PyObject columns
   via `Morsel.materialize()` — the *existing* single sanctioned shim point
   (`_morsel_shim.pyx`). The engine-internal hand-off never materialises.

Nothing else crosses. No `cdef class Morsel` (the shim) enters the queue or the
drive loop; the carrier is `shared_ptr[CxxMorsel]` end to end.

## 2. What already exists (do not rebuild)

- `shared_ptr[CxxMorsel]` carrier; `CxxMorsel` columns are `DrakenVector` views,
  `nogil`-accessible (`_col_view`).
- `BasePlanNode.push / push_left / push_right (shared_ptr[CxxMorsel], ErrCtx*)
  noexcept nogil` and `next_morsel() -> shared_ptr[CxxMorsel]`.
- EOS as `cxx_morsel_new_eos()` / `MorselState.END_OF_STREAM`.
- `ErrCtx{code,msg}` + `_stash_exc`/`_take_exc` for surfacing a Python exception
  out of a `nogil` region at the boundary.
- `CppThreadPool` (BS::thread_pool).

The rewrite *consumes* this surface directly. `push_one`/`pull_one` (the
per-morsel GIL-bouncing Python shims) are **bypassed**, not reused.

## 3. Type model (all declared, no `object` on the hot path)

| Today (Python)              | Native                                                            |
|-----------------------------|-------------------------------------------------------------------|
| `Segment` (Python class)    | `cdef class Segment` — `cdef` typed fields (node ids, flags)       |
| `Event` (Python class)      | `cdef class Event` — `cdef int` counters, typed parent list        |
| `Executor` (Python class)   | `cdef class Executor` — owns events, native sync                   |
| `queue.Queue` hand-off      | `MorselQueue` — C++ bounded ring of `shared_ptr[CxxMorsel]`        |
| `threading.Lock`/`Event`    | `std::mutex` / `std::condition_variable` (cdef RAII guard)         |
| `_DONE = object()` sentinel | EOS `CxxMorsel` (`MorselState.END_OF_STREAM`)                      |

## 4. The hand-off queue — ownership (the decision #1 we settled)

`MorselQueue`: a bounded queue holding `shared_ptr[CxxMorsel]` by value, built on
**vendored moodycamel**. No Python object ever sits on a native queue — the
carrier is the C++ `shared_ptr[CxxMorsel]`, period.

- **Carrier ownership is C++ shared ownership.** Enqueue moves a `shared_ptr`
  in; dequeue moves it out. No `Py_INCREF`. No shim crossing the queue. When the
  last `shared_ptr` drops, the `CxxMorsel` frees deterministically.
- **Bounded depth** (`_QUEUE_DEPTH`, default 8) → backpressure (blocking
  enqueue/dequeue). Matches the serial streaming memory profile.
- **Drain-at-close.** On early consumer abandonment (LIMIT), `close()` drops all
  queued `shared_ptr`s (freeing their morsels) and wakes a blocked producer.
  Mirrors `MorselRef`'s drain-on-destruct discipline.
- `nogil` `put`/`get` — the carrier is C++, the queue is C++; no GIL needed to
  move a morsel across the boundary.

### 4.1 Queue shape — RESOLVED (A: MPMC, vendor `concurrentqueue.h`)

The per-shape fan-out (`out_q` in `_stateless_stream`, `_join_probe_stream`, …)
is **N workers → 1 consumer = MPSC**. moodycamel's vendored variants
(`readerwriterqueue.h`, `readerwritercircularbuffer.h`) are SPSC only, so we
**vendor `concurrentqueue.h`** (MPMC) — architect-approved (§4). One `MorselQueue`
type serves both the terminal hand-off and the fan-out. (Per-worker SPSC was the
no-new-dependency alternative; rejected — one MPMC queue is simpler than N SPSC
queues + round-robin drain.)

### 4.2 Dead code removed

`opteryx/compiled/pyobject_queue.{pyx,cpp,so}` and `src/cpp/pyobject_queue.hpp`
(a `ReaderWriterQueue<PyObject*>` doing INCREF/DECREF) had **zero consumers** —
built but never imported. It is exactly the Python-object-on-a-native-queue
pattern this rewrite forbids. **Deleted** (files + `setup.py` extension entry) as
part of slice 1.

## 5. The drive loop — `nogil`, no per-morsel GIL bounce

Per worker, once: build the cloned operator chain (typed `cdef`-class
construction), then:

```
while True:
    cdef shared_ptr[CxxMorsel] m = scan.next_morsel()   # nogil
    if m.get() == NULL: break
    head.push(m, &err)                                  # nogil, whole chain
    if err.code: break
# push EOS carrier, then drain pending to MorselQueue
```

The entire loop is `nogil`. Contrast today: `push_one` re-acquires the GIL and
runs `cxx_to_morsel` for **every** morsel. Even in free-threaded 3.14t the
GIL-state transition and the shim alloc are per-morsel cost this removes.

## 6. DAG ownership & lifetimes

- The Cython `execute()` generator owns one `Executor` for the query's lifetime.
- `Executor` is the **sole owner** of its `Event`s. `Event.parents` is a
  **borrowed** typed list (no back-owning reference) — breaks the current
  `event ↔ executor` cycle by construction.
- Deterministic teardown order on generator close: drain/close `MorselQueue` →
  join worker futures → `Executor` releases `Event`s → `pool.shutdown(wait=True)`.
- Dependency edges (build-before-probe, breaker ordering) stay as the proven
  two-counter model (DuckDB §6), now with `cdef int` counters under a `std::mutex`.

## 7. Synchronisation

Every `threading` primitive is replaced 1:1:

- `Executor.lock` → `std::mutex` (cdef RAII guard for the counter updates).
- `Executor._all_done` / completion signalling → `std::condition_variable`.
- per-worker `pull_lock` (shared scan pull) → `std::mutex`.

No `import threading`, no `import queue` survives in the execution package.

## 8. Errors & cancellation

- In-loop errors surface via `ErrCtx`; the worker stops, stashes via `_take_exc`,
  and the `Executor` keeps the first error (idempotent), re-raised on the consumer
  thread after the EOS carrier wakes it. Same semantics as today's `stash_error`.
- LIMIT / early close: consumer `close()` → `MorselQueue.close()` → workers see
  the closed queue on next `put` and unwind, running operator `finally` (ctx
  terminate, source close).

## 9. Staging (each slice independently compiles + passes `make q`)

1. **`MorselQueue`** on vendored moodycamel SPSC, carrying `shared_ptr[CxxMorsel]`,
   drain-on-close. Unit-tested in isolation. No scheduler wiring yet.
2. **Worker plan-distribution** (§9.1) — replace `_clone_op`'s Python fork. Shape
   **pending architect decision** (spec/worker-state split, not a native deep-copy
   clone). Cross-operator change, parity-tested before anything depends on it.
3. **`cdef class Executor`/`Event`/`Segment`** + native sync, driving the
   **existing** single terminal pipeline through the `nogil` `push` surface
   (replaces `scheduler_engine.py`). Single-segment plans first.
4. **Per-shape handlers** (`_stateless_stream`, `_join_probe_stream`,
   `_run_breaker_segment`, multi-breaker chain) ported onto the native queue +
   `nogil` drive + the slice-2 distribution mechanism (replaces
   `parallel_engine.py`).
5. **Delete** `scheduler_engine.py`, `parallel_engine.py`, and the now-unused
   `push_one`/`pull_one`/`push_left_one`/`push_right_one` Python shims.

### 9.1 Worker plan-distribution — kill the Python fork (Q2, OPEN)

Two things are bundled in today's "forking" and must not be conflated:

- **The strategy (KEEP).** Partition data by `hash(key) % radix`; each worker
  holds *private* operator state over a disjoint partition; CONCAT / scalar-merge
  at the end. No shared mutable state. This is the strategy that **won** — shared-
  mutable intra-op parallelism was tried and lost; partition+private-state is the
  central-scheduler direction.
- **The mechanism (KILL).** How a worker gets its private state: today
  `type(op)(properties=…, **op.parameters)` — re-running the operator's Python
  `__init__` to deep-copy a fully-built operator. This Python fork is the hack.

A `cdef clone(self)` that deep-copies a built operator natively would just
**re-skin the fork** — wrong target. The intended replacement is a **spec/state
split**: each operator separates an *immutable spec* (built once from the physical-
plan node, shared read-only across all workers) from *worker-local mutable state*
(instantiated per worker from the spec). The scheduler hands every worker the one
shared spec and spins up only the per-worker state — no operator copy, no
`__init__` round-trip, no Python. The join path already half-does this (workers
"rebuild their OWN private join engine from the shared read-only `left_morsel`").

Stateless ops (filter/project) holding no per-morsel mutable state may share a
single instance across workers; only breakers (agg, sort, distinct, join build)
need per-worker state — an optimisation to confirm, not assume.

**OPEN before slice 2 is designed:** (a) confirm keep-strategy / kill-mechanism-
via-spec-state-split; (b) whether to first audit how cleanly each operator family
separates immutable spec from mutable state, and report before committing the
slice-2 design.

DOP=1 must stay byte-identical to the serial path at every slice (the prime
constraint). No flags, no shadow path, no fallback (§ contract).

## 10. Decisions

- **Q1 — RESOLVED.** `MorselQueue` on **vendored moodycamel `concurrentqueue.h`**
  (MPMC, architect-approved), carrying `shared_ptr[CxxMorsel]` only — no
  `PyObject*` on any native queue. The dead, unreferenced `pyobject_queue.*`
  (PyObject-on-queue hack) is **deleted** (§4.2).
- **Q2 — OPEN.** Strategy (partition + private state + concat) is kept; the Python
  fork mechanism (`_clone_op`) is killed and replaced by a **spec/worker-state
  split**, not a native deep-copy clone (§9.1). Awaiting architect confirmation +
  decision on the operator spec/state audit.
- **Q3 — RESOLVED.** `identify_segments` stays planning-phase; may be pythonic
  Cython or pure Python (planning-phase latitude).
