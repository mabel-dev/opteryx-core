# Execution Thread-Safety Contract

**Status:** specification (feeds WP-7 / M4-prep). **Date:** 2026-06-15.

## Why this exists

The draken kernels now release the GIL ([WP-6](WP6_GIL_PROFILE.md) sweep:
~56 `nb::gil_scoped_release` sites), and they scale 5–7× on real OS threads.
The execution **coordination layer** (scheduler / `drive_scan` /
`serial_engine` / `PipelineContext`) will be replaced by a parallel engine
(M4). The **operators and the draken Morsel/Vector model are kept and
refactored**, not rewritten — they encode years of hard-won correctness.

Releasing the GIL removed the implicit lock that was silently serialising every
operator's shared state. This document is the contract a parallel engine must
honour when it drives the reused operators on multiple threads. It is *not* an
audit of the doomed coordination layer.

## Decision frame

- **Kept + refactored:** draken `Morsel`/`Vector`/kernels; operator bodies
  (filter/join/agg/sort/distinct kernels); the `merge()` contract (WP-7).
- **Replaced:** the scheduler, `drive_scan`, `serial_engine.stream()`,
  `PipelineContext`, the single-output push topology.

The single fork that shrinks the audit: **stateful operators are cloned per
worker and merged**, not shared across threads. Under that model most
per-instance-state races are moot by construction; what remains is (a) the
Morsel memory model, (b) in-place mutation, (c) genuinely shared module/global
state and singletons.

## Cross-cutting rules (the contract)

1. **Exclusive morsel ownership.** A morsel pushed into an operator is owned
   exclusively by that operator. Once `emit(morsel)` is called the sender MUST
   NOT retain, read, or mutate it. No morsel is shared between two operators or
   two worker threads. This makes the GIL-protected `Py_INCREF/DECREF` on
   `Morsel` columns (`_morsel_shim.pyx` `_set_column`) safe without per-morsel
   locks, and is the precondition for rule 2.

2. **In-place mutation is for exclusive owners only.** Operators that mutate a
   received morsel in place are correct *only* under rule 1. Known sites:
   - `Distinct._dispatch_push` — `morsel._empty_inplace()` / `_take_inplace()`
     (`distinct.pyx:133,135`). Mutates the **received** morsel. ⚠️ highest-risk:
     a data race the instant a morsel is shared. Must stay exclusive-owned.
   - `Filter` — `_set_column` inside `_apply_constant_replacements`
     (`filter.pyx:167`). Mutates a freshly-produced morsel; safe.
   - `CrossJoin` — `res._empty_inplace()` on a `left_morsel.copy()`
     (`cross_join.pyx:152`); operates on a local copy, safe.
   The parallel engine must guarantee rule 1 or these become corruption.

3. **Stateful operators are cloned per worker and merged.** Each worker gets
   its own instance; per-instance accumulators never cross threads; a merge
   phase combines partials. Applies to: grouped/ungrouped aggregates
   (`GroupHashEngine`, `_LiteralAggState`), distinct (`_hash_set`,
   `_promoted`), sort/heap_sort (`_morsels` buffer), all joins (build buffers +
   hash tables), limit (`remaining_rows`). This is exactly the WP-7 `merge()`
   surface. Joins already hold `self.lock` during build (`hashed_inner_join`
   line ~172) — under clone-per-worker that lock becomes redundant, not load-
   bearing.

4. **Singleton multi-input operators need synchronised close-counting.** An
   operator that joins N input chains cannot be cloned (it *is* the join point).
   `Union` counts EOS via `BasePlanNode._seen_input_closes`
   (`_operators.pyx`); if N workers push EOS concurrently the increment +
   compare races. Either serialise EOS handling at the join point or make the
   counter atomic. Same applies to the terminal `ExitNode._pending` deque —
   either a singleton with synchronised append, or per-worker exits merged
   above.

5. **Early-termination flag must tolerate concurrent writers.**
   `PipelineContext._terminated` (a `bint`) is written by `Limit` via
   `ctx.terminate()` and read by every `push()`. Concurrent `terminate()` calls
   race; benign in practice (idempotent set-to-true) but the new engine's
   primitive should be an atomic/event, not a bare `bint`. The existing API
   already hides this behind `is_terminated()`/`terminate()` so the primitive
   can change without touching call sites.

6. **No module-level mutable state reachable from execution.** Read-only
   tables (`_OP_CODE`, `_DRAKEN_CMP_OP`, `_NULL_CONSTRUCTORS`) are fine. Mutable
   module globals are not — they are GIL-protected today and race once the GIL
   is released. See the residue list below.

7. **Borrowed `DrakenVector*` lifetime.** The resolve-under-GIL →
   use-in-one-nogil-section → free discipline holds today. When a morsel is
   queued across a thread boundary, the queue/edge MUST hold a **strong**
   reference to the morsel (not a borrowed pointer); borrowed `DrakenVector*`
   must never outlive the call that resolved it.

## What is already safe (do not re-litigate)

- **Vectors are immutable post-construction**; slice/take/concat copy, they do
  not alias. Concurrent *reads* of a shared build-side table are safe.
- **CPython refcounts are atomic**; freeing a morsel on a different thread than
  it was created is safe *given* exclusive ownership (rule 1).
- **Shared selection/validity pools** (`vector_alloc.cpp`
  `draken_identity_sel`/`zero_sel`/`zero_validity`) — lock-free atomic read +
  mutex-guarded grow, old buffers leaked so live pointers stay valid.
- **`logical_type_intern`** (`logical_type.h:106`) — now mutex-guarded with a
  `std::deque` (stable addresses); covered by
  `tests/draken/test_gil_release_concurrency.py`.
- **`g_ops_table`, `_kernel_registry`, `draken_malloc/free`** — read-only after
  init / thread-safe libc.

## Concrete current-code residue (surviving the rewrite)

| item | file:line | severity | disposition |
|------|-----------|----------|-------------|
| `BLOOM_FASTPATH_COUNTER` module global (telemetry) | `outer_join.pyx:49,549-550` | real lost-update race | **FIX NOW** — same WP-3 pattern; move to `self.readings` (instance state). Done in this change. |
| `_WP13_KPROBE_MIN_RATIO` | `hashed_inner_join.pyx:1003-1008` | low | startup/test-only tuning knob, read-only during execution; document, leave. Concurrent reads of a `double` are safe. |
| `_WP13_FILTER_KPROBE` | `filter_join.pyx:76-78` | low | as above (`bint`). |
| `_FOOTER_CACHE` singleton | `parquet_read.pyx:73` | documents safety | `ParquetFooterBytesCache` declares itself thread-safe via internal RLock on its MemoryPool + LRU_K (`footer_cache.pyx:34`). Not a bug today (single-threaded). M4 ENFORCEMENT: stress-verify under concurrent scan when M4 parallelises the scan operator (it hasn't yet — the IO pipeline below the scan is already C++-parallel and independent of this Python cache). |
| `PipelineContext._terminated` | `_operators.pyx` | benign | set-once, monotonic false→true `bint`; concurrent `terminate()` is idempotent and reads tolerate either value. M4 ENFORCEMENT: if M4 needs richer cancellation (reason, per-worker), give it a real primitive then — don't harden the orchestration layer M4 replaces. |
| `Union._seen_input_closes` | `union.pyx` / `_operators.pyx` | M4-coupled | SINGLETON operator (one instance joins N input chains); the EOS countdown is single-threaded today. M4 ENFORCEMENT: when N workers each close an input, make the countdown atomic at the join point. |

## Per-operator classification (for the WP-7 catalog)

- **Stateless / shareable** (no per-morsel writes to instance state): Filter,
  Projection, NullReader. Scans (Read, ParquetRead) produce fresh morsels —
  shareable except for the `_FOOTER_CACHE` caveat.
- **Stateful → clone-per-worker + `merge()`**: ungrouped aggregate, grouped
  aggregate, distinct, sort, heap_sort, limit, and all joins
  (inner/outer/cross/filter/nested-loop/non-equi/asof/unnest).
- **Singleton multi-input** (cannot clone; needs synchronised close-counting):
  Union, ExitNode.

## Validation hooks

- `tests/draken/test_gil_release_concurrency.py` — stress-tests the draken
  *kernel* layer (16 threads, results vs single-threaded reference).
- `tests/unit/operators/test_grouped_engine_concurrency.py` — **DELIVERED**
  (M4-prereq): the *operator*-layer analog. 8 OS threads each ingest a disjoint
  partition into their own GroupHashEngine concurrently (barrier-synced to
  maximise contended re-entry into the shared kernels), then the partials are
  merged and asserted byte-identical to a single-threaded reference. Validates
  the clone-per-worker model end-to-end under real threads.
- `tests/unit/operators/test_{grouped,ungrouped}_merge_equivalence.py` — the
  WP-7 merge property tests (correctness of the combine, serial).

## M4 enforcement checklist (carried forward)

The items above marked **M4 ENFORCEMENT** are the shared-state obligations a
parallel engine must satisfy; they are intentionally NOT changed now because
they are benign single-threaded or live in the orchestration layer M4 replaces:

1. `_FOOTER_CACHE` — stress-verify (or make thread-local) when the scan operator
   is parallelised.
2. `PipelineContext` termination — replace the bare `bint` with a real primitive
   only if/when M4 needs richer cancellation.
3. `Union` / `ExitNode` (SINGLETON) — synchronise EOS close-counting / result
   append once N workers feed them.
4. Exclusive morsel ownership (rule 1) — the engine must guarantee it; in-place
   mutators (Distinct) depend on it.
