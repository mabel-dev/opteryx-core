# M4 Iteration 1 — Parallel Grouped Aggregation (Design)

**Status:** BUILT & validated (2026-06-15). `GROUP_AGG_WORKERS` knob (default 1 =
serial). `make q` 182/182 and `make tpch` 22/22 give identical answers across
W ∈ {1,4,8}; real end-to-end **2.6× on an agg-dominant query** (RegionID
GROUP BY, 247→96ms at W=8), neutral on high-cardinality / scan-dominated queries
(as designed). Differential + concurrency + merge tests green (158).
Implementation: `opteryx/operators/grouped_aggregate_hashed/_node.pxi`. **Date:** 2026-06-15.
Backed by [`M4_PARALLEL_AGG_PROTOTYPE.md`](M4_PARALLEL_AGG_PROTOTYPE.md) (6.1×@8
threads low-card; high-card neutral at ≤8 workers),
[`WP6_GIL_PROFILE.md`](WP6_GIL_PROFILE.md) (grouped-agg ~74% GIL-released),
[`EXECUTION_THREAD_SAFETY_CONTRACT.md`](EXECUTION_THREAD_SAFETY_CONTRACT.md), and
the WP-7 `merge()` + the operator concurrency test
(`tests/unit/operators/test_grouped_engine_concurrency.py`).

## Goal & explicit scope

Make `GroupedAggregateHashedNode` ingest morsels across **W worker threads, each
into its own `GroupHashEngine` clone**, then `merge()` the partials and finalize.
Capture the proven common-case win (low/medium cardinality). **Partial benefit is
the goal** — we deliberately do NOT solve high-cardinality optimally in v1.

In scope: round-robin streaming dispatch + per-worker engines + merge on EOS,
operator-internal, behind one config knob.
Out of scope (iteration 2+): hash-partition-by-key (the high-card fix), NDV-gated
strategy selection, a shared/central worker pool, parallel DISTINCT, parallel
expression-prep, tree merge.

## Why no NDV gate in v1 (the simplification)

Prototype high-cardinality (500k groups): 4 thr 1.66×, 8 thr 1.68×, **16 thr
1.39× (regresses)**. The regression only appears past 8 workers. So **capping W ≤
8 makes parallel agg a win-or-neutral across all cardinalities** — never a loss.
That removes the need to estimate cardinality or branch strategies in v1. (v2's
hash-partition turns the high-card 1.7× into a real win; that's when NDV gating
earns its place.)

## Architecture — streaming round-robin dispatch (Model A)

The operator becomes a self-contained mini-scheduler. It stays a single
push/EOS operator to the rest of the pipeline; nothing upstream/downstream
changes.

```
__init__ (when W > 1):
  engines   = [GroupHashEngine(...) for _ in range(W)]   # W clones
  queues    = [BoundedQueue(maxsize=Q) for _ in range(W)] # backpressure
  workers   = [Thread(target=run, args=(k,)) for k in range(W)]   # started
  rr        = 0
  worker_err = [None]*W

  def run(k):                      # worker loop
      try:
          while True:
              m = queues[k].get()
              if m is EOS: break
              engines[k].ingest(m)         # GIL released inside kernels → real parallelism
      except Exception as e:
          worker_err[k] = e
          # keep draining so main's put() never blocks on a dead worker
          while queues[k].get() is not EOS: pass

_push_impl(morsel):
  if morsel is EOS:
      for k: queues[k].put(EOS)
      for w: w.join()
      if any(worker_err): raise the first             # surface on main thread
      for k in 1..W-1: engines[0].merge(engines[k])    # serial merge (WP-7)
      for chunk in engines[0].finalize(): emit(chunk)
      emit(EOS); return
  if morsel.num_rows == 0: return
  morsel = prepare/select(morsel)        # expression-eval + column select, MAIN thread (v1)
  queues[rr % W].put(morsel)             # blocks if full → backpressure to drive_scan
  rr += 1
```

W == 1 ⇒ the existing serial path, untouched.

## Concurrency safety (why this is correct)

- **Per-worker engines** — no engine state shared between threads. This is
  exactly what `test_grouped_engine_concurrency.py` validates (8 threads, == serial).
- **Exclusive morsel ownership** — each morsel is dispatched to exactly one
  worker (contract rule 1); in-place safe.
- **Kernels are concurrency-safe** — the GIL-release sweep + `logical_type_intern`
  mutex + the draken concurrency test cover the kernels workers call concurrently.
- **Merge / finalize / emit happen on the main thread** after all workers join —
  no concurrent merge, no concurrent downstream emit.

## Backpressure, memory, errors, cancellation

- **Backpressure:** bounded queues. A full queue blocks `put()` → blocks
  `_push_impl` → blocks `drive_scan` → throttles the scan. Q tunable (start ~4).
- **Memory:** W partial hash tables during ingest. Low-card: W small tables, fine.
  High-card: ~W× the serial table — accepted in v1 (bounded by W≤8; v2
  hash-partition removes it).
- **Errors:** a worker that throws records its exception and then drains-to-EOS
  so the main thread's `put()` can never deadlock on a stalled worker; the main
  thread re-raises after join. Scan-side cleanup is already handled by the WP-1
  `drive_scan` finally.
- **Cancellation:** grouped agg is a pipeline breaker (emits only at EOS), so
  downstream LIMIT cannot short-circuit it mid-stream anyway. On
  `ctx.is_terminated()` the operator stops dispatching, sends EOS to workers,
  joins, and discards without finalizing.

## Config

One knob: `GROUP_AGG_WORKERS` (int, env/config). **Default 1 = current serial
path** (safe rollout). `> 1` enables parallel with that many workers; recommend a
launch default of `min(8, cpu_count - 2)` once the gate below passes. W is capped
at 8 regardless (the regression boundary).

## Code touch points

- `opteryx/operators/grouped_aggregate_hashed/_node.pxi` — `__init__` (build W
  engines/queues/workers when W>1), `_push_impl` (dispatch + EOS drain/merge/
  finalize), `__dealloc__`/teardown (join workers, drain queues on abnormal exit).
- `opteryx/config.py` — `GROUP_AGG_WORKERS`.
- Engine / `merge()` / collectors: **unchanged** (already built + tested).

## Wider benchmark result (2026-06-15) — DEFAULT STAYS SERIAL

Ran the full ClickBench GROUP BY battery (29 queries) W=1 vs W=8 warm
([`dev/bench_parallel_agg.py`](../dev/bench_parallel_agg.py)). **Verdict: do NOT
flip the default.**

- Geomean speedup on the 17 queries where parallel actually engaged: **0.94×**
  (a net *slowdown*). Geomean over all 29: 0.96×.
- Only 2 queries won (q36 1.54×, q16 1.08×); the rest were neutral-to-worse,
  with real regressions (q19 0.73×, q34 0.78×, q18 0.80×, q35 0.82×, q15 0.85×).
- Why: the ClickBench GROUP BYs are dominated by **high cardinality** (UserID,
  SearchPhrase, URL, WatchID). There each worker builds a near-full hash table
  (partitioning barely shrinks the group set), the serial `merge()` is the
  Amdahl bottleneck (prototype predicted this), and 8× tables + the downstream
  sort make it actively *worse* than serial, not just neutral. Small-input
  filtered queries (q37/38/42) also regress on thread/queue overhead.

The earlier 2.6× (`GROUP BY RegionID`) was a *favorable* medium-cardinality,
agg-dominant query — not representative. **This is exactly why we benched wide.**

Consequence: ship the operator as **opt-in only** (`GROUP_AGG_WORKERS=1`
default). Unconditional default-on would regress the real workload. To earn
default-on, iteration 2 needs **one of**:
1. **NDV-gated engagement** — only go parallel when the estimated group count is
   low relative to rows (optimizer already has NDV); falls back to serial for the
   high-card majority. Captures the wins, avoids the losses.
2. **Hash-partition by key** (no merge) — removes the Amdahl bottleneck that
   sinks high cardinality, making it a win everywhere. The real fix, more work.

## Test & benchmark gate (must pass before default-on)

- **Correctness:** existing `make q` (182) + `make tpch` (22) with
  `GROUP_AGG_WORKERS` ∈ {1, 4, 8} — results identical across W (this is the real
  gate: the same SQL battery must produce the same answers serial and parallel).
- **Unit:** a flag-on/off differential test driving a grouped-agg query both ways
  and asserting identical output; reuse the concurrency-test data shapes (incl.
  NULL keys, parvi→carchar promotion, empty input, HAVING).
- **Benchmark:** `make clickbench` group-by queries (q08–q15 are GROUP BY) at
  W=1 vs W=8 — expect a clear speedup on the group-heavy ones, no regression
  elsewhere. ClickBench has both low- and high-card group-bys, so it exercises
  the neutral-at-high-card claim end-to-end (with real scan/decode overlap, not
  just the synthetic prototype).

## Risks / open questions for the architect

1. **Operator-internal threads vs a shared pool.** v1 spawns W threads inside the
   operator. Two parallel grouped-aggs in one query ⇒ 2 pools ⇒ oversubscription.
   Rare today; the long-term central-scheduler vision replaces this. Accept for
   v1, or introduce a query-scoped shared pool now? (Recommend: accept; the
   per-worker-engine + merge model is what carries forward regardless of who owns
   the threads — only thread-ownership is throwaway.)
2. **Default-on vs flag-gated.** Recommend ship default-1 (off), flip to
   `min(8,cpu-2)` after the bench gate.
3. **Prep on main thread serializes expression-eval** for computed group keys.
   Fine for simple keys; v2 moves prep to workers once verified morsel-local.
4. **Handoff overhead** (GIL-bound queue put/get) — negligible because morsels
   are row-group-coarse (the prototype confirmed the speedup with this model),
   but worth a Cython/C++ queue if a future bench shows it dominating.

## What carries forward to the eventual central scheduler

The reusable, non-throwaway parts: the cloned per-worker engines, `merge()`, the
exclusive-ownership dispatch, and the concurrency-safety proof. The scheduler
later owns the threads and the partition assignment instead of the operator —
the operator's per-worker engines become the scheduler's partition tasks.
