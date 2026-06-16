# M4 Iteration 2 — Hash-Partition (Shuffle) Parallel Grouped Aggregation (Design)

**Status:** BUILT, then MEASURED — **does not pay as built; default stays
serial.** **Date:** 2026-06-15.

> **Result (the bench is the verdict).** Hash-partition is *worse* than
> round-robin on the ClickBench GROUP BY battery: geomean **0.72×** (main-thread
> gather) and **0.62×** (worker-side gather) vs round-robin's 0.94× — all net
> slowdowns; worst q09 (COUNT DISTINCT) **0.27×**. Root cause = the design's own
> flagged risk: the **scatter is the serial bottleneck**. The per-row bucketing
> runs in Python on the main thread (slow AND GIL-heavy, so it starves the
> workers), and the approach copies every row (the gather) — pure overhead vs
> serial. Correctness is perfect (make q 182, tpch 22, differential incl. the
> now-eligible COUNT DISTINCT / string MIN-MAX), but the speed is not there.
> **The naive (Python-level) scatter is fundamentally too expensive.** Making it
> pay would need a fully-native off-GIL scatter kernel (Cython hash + int32
> bucketing + native take), and even then the full-dataset row-copy is uncertain
> to beat serial on high-cardinality queries. Recorded honestly; the operator
> currently carries this path behind the default-off flag (no harm), pending a
> direction call: native-scatter investment, NDV-gated round-robin, or shelve.

---

**(Original design below — the mechanism is sound; the implementation cost is
what killed it.)**

**Date:** 2026-06-15.
Supersedes the round-robin strategy of iteration 1
([`M4_PARALLEL_GROUP_AGG_DESIGN.md`](M4_PARALLEL_GROUP_AGG_DESIGN.md)) for the
high-cardinality case that blocked default-on. Backed by the wider bench
(round-robin geomean **0.94×** on ClickBench GROUP BY — a net slowdown driven by
the serial `merge()` at high cardinality) and the prototype
([`M4_PARALLEL_AGG_PROTOTYPE.md`](M4_PARALLEL_AGG_PROTOTYPE.md)).

## The problem this fixes

Iteration 1 (round-robin morsels + `merge()`) loses at high cardinality because
the merge is **serial and O(groups × partials)** — the Amdahl bottleneck — and
each worker builds a near-full hash table (partitioning rows doesn't shrink the
group set). ClickBench GROUP BYs are mostly high-cardinality (UserID, URL,
SearchPhrase), so round-robin regressed the suite.

## Mechanism — route rows by key hash, so there is NO merge

Partition each morsel's **rows** by `hash(group_key) % W`, so worker *w* receives
only the rows whose group key routes to *w*. Identical keys always route to the
same worker ⇒ **each group lives on exactly one worker** ⇒ the workers' group
sets are disjoint ⇒ **no merge**. On EOS each worker finalizes its own groups and
the W finalized outputs simply concatenate.

```
per data morsel (main thread):
  prep (expr-eval + select)                         # as it.1
  h    = morsel.hash_keys(group_columns)            # uint64 per row, deterministic per key
  subs = partition_by_key_hash(morsel, h, W)        # W sub-morsels, rows routed by h % W
  for w: queues[w].put(subs[w])                     # exclusive ownership; bounded → backpressure

worker w:  loop: ingest(sub) into its OWN engine    # disjoint keys, GIL released in kernels

EOS (main thread):
  drain + join workers
  for w: emit each chunk of engines[w].finalize()   # concatenate — NO merge
  emit EOS
```

### Why this beats round-robin at high cardinality

- **No serial merge** — the Amdahl term that sank it.1 is gone.
- **Each worker holds ~1/W of the groups**, not the full set — memory is ~total
  split across workers (vs it.1's W× near-full tables), and each worker does 1/W
  of the grouping work, so it scales even when total cardinality is huge.

## The new primitive

`partition_by_key_hash(morsel, h, W) -> list[Morsel]` (W sub-morsels). One pass
over `h` buckets each row index by `h[i] % W` (a radix/counting partition), then
`morsel.take(indices_w)` per bucket (gather, already GIL-released). Reuses the
existing `Morsel.hash_keys` (routing) and `Morsel.take`; only the index-bucketing
is new — a small Cython kernel in the grouped-agg package
(`opteryx/operators/grouped_aggregate_hashed/`), promoted to draken if reused.

### Routing correctness

- `hash_keys` is **deterministic per key value** (same key → same hash → same
  bucket → same worker), across all morsels. Verified property the engine already
  relies on for grouping.
- **NULL group keys** hash to some value and route consistently → the NULL group
  lands wholly on one worker → one correct group.
- **Hash collisions** (different keys, same `h % W`) route to the same worker;
  the worker's engine still distinguishes them by the full key. Harmless.
- **Multi-column keys** — `hash_keys(group_columns)` already combines all group
  columns into one hash; routing on it is correct.

## The new cost — the scatter is the serial fraction (key risk)

Per morsel the main thread now does `hash_keys` + bucket + W `take`s — all
**O(rows)**, much less than the **O(rows × agg-work)** it parallelises, so the
scatter should be a small serial term. But it is non-zero, and at **low
cardinality** (where it.1's round-robin had ~zero serial cost) the scatter may
make hash-partition slightly slower than round-robin — though it should still
beat serial. **This is the measurement that decides the final shape** (below).

Note a redundancy: routing computes `hash_keys` and the worker engine recomputes
it on ingest. Acceptable for v1; a later optimization can thread the routing hash
into `ingest` to hash once.

## Strategy: replace it.1, or coexist? (decide by bench)

Two outcomes, picked by measuring the scatter overhead on the ClickBench battery:

- **If hash-partition is neutral-or-better than serial at LOW cardinality too**
  (scatter cheap) → it becomes the **single universal parallel strategy**;
  round-robin + the engine `merge()` path are retired from the operator (merge
  stays for any non-operator use, but the operator no longer calls it). Simplest.
- **If scatter materially hurts low-cardinality** → keep both and pick by an NDV
  estimate (the optimizer already has NDV): low card → round-robin + merge (it.1),
  high card → hash-partition. More complex; only if the bench forces it.

**Default stays serial (`GROUP_AGG_WORKERS=1`)** until the *same wider bench*
(`dev/bench_parallel_agg.py`) shows hash-partition is **neutral-or-better on every
ClickBench GROUP BY query** — especially the it.1 regressors (q18 0.80×, q19
0.73×, q34 0.78×). Only then does default-on get proposed.

## Min-input gate (carry over from the it.1 finding)

it.1 also regressed tiny filtered inputs (q37/38/42) on thread/queue overhead.
Add a simple **row-count floor**: stay serial when the input is small (e.g. below
a few morsels / N rows). A row-count floor is cheap and independent of the
cardinality question.

## Concurrency safety

Strictly simpler than it.1: per-worker engines (validated by
`test_grouped_engine_concurrency.py`), **exclusive ownership of each sub-morsel**
(`take` produces a fresh morsel handed to exactly one worker), concurrency-safe
kernels (the GIL-release sweep + draken concurrency test), and **no cross-engine
merge at all** — each worker finalizes independently on the main thread after
join. The only shared read is the input morsel during the main-thread scatter
(single-threaded). Worker error handling = it.1 (record + drain-to-stop +
re-raise on join).

## Memory & backpressure

- **Memory:** ~total group state split across W workers (each ~1/W), plus
  transient sub-morsels in flight — a clear improvement over it.1's W× near-full
  tables.
- **Backpressure:** bounded per-worker queues, as it.1.

## Code touch points

- `opteryx/operators/grouped_aggregate_hashed/_node.pxi` — replace the
  round-robin dispatch with scatter dispatch; on EOS, per-worker finalize +
  concatenate (drop the `_drain_and_merge` for this path). Keep the W=1 serial
  path untouched. Reuse the lazy worker spin-up, bounded queues, error handling.
- New partition helper (grouped-agg package) — `partition_by_key_hash`.
- `is_mergeable()` gate still applies: hash-partition needs no merge, but
  per-worker *finalize* of every aggregate must be correct independently — which
  it already is (each engine finalizes its own groups today). So hash-partition
  can engage for ANY aggregate that the engine can finalize, including COUNT
  DISTINCT and string MIN/MAX (which it.1 had to exclude because they couldn't
  merge). **This is a second win: hash-partition parallelises strictly more
  aggregates than round-robin** (no merge requirement). Confirm per aggregate.

## Test & benchmark gate

- **Correctness:** `make q` (182) + `make tpch` (22) identical across
  `GROUP_AGG_WORKERS` ∈ {1,4,8}; the differential test extended; a new property
  test that hash-partition output (concatenated, unordered) == serial, incl. NULL
  keys, multi-column keys, high cardinality, COUNT DISTINCT (now eligible),
  string MIN/MAX, empty/tiny input.
- **Benchmark (the real gate):** rerun `dev/bench_parallel_agg.py` (the full
  ClickBench GROUP BY battery, W=1 vs W=8). Target: **geomean ≥ ~1.2× on engaged
  queries and worst-case ≥ ~0.97×** (no material regression) — specifically the
  it.1 regressors must recover to ≥ neutral. That result is what justifies
  flipping the default.

## Risks / open questions for the architect

1. **Scatter overhead** could itself bottleneck (it's the new serial fraction).
   Mitigation if it bites: parallelise the scatter (a Cython kernel doing the
   bucket+gather off-GIL), or thread the routing hash into `ingest` to avoid the
   double-hash. Measure first.
2. **Replace vs coexist** (above) — the bench decides. Recommend aiming to
   replace (single strategy) for simplicity; fall back to NDV-gated coexistence
   only if low-card regresses.
3. **Min-input floor** value — tune from the bench (q37/38/42).
4. **COUNT DISTINCT / string MIN-MAX now eligible** under hash-partition — worth
   confirming each finalizes correctly per-worker (they do today in serial; the
   only change is each worker sees a disjoint key subset).
5. **Partition kernel placement** — operator-local Cython helper first; promote
   to a draken primitive if another operator (e.g. parallel joins) wants the same
   shuffle.

## What carries forward

The per-worker-engine model, the lazy worker pool, bounded-queue backpressure,
and the concurrency-safety proof are reused from it.1. The merge() built in WP-7
is *not* on this path (its value was the round-robin strategy and any future
non-operator use); hash-partition's no-merge property is the whole point.
