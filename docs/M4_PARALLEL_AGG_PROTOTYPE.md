# M4 Pilot — Parallel Grouped-Aggregation Prototype Findings

**Status:** evidence (M4 design input). **Date:** 2026-06-15. **Tool:**
[`dev/parallel_agg_prototype.py`](../dev/parallel_agg_prototype.py).

## What was measured

The full *operator path* of partition-parallel grouped aggregation — not just the
kernels ([`dev/parallel_scaling.py`](../dev/parallel_scaling.py) already showed
those scale 5–7×). Model = exactly what an M4 parallel grouped-agg operator does:

- **serial:** one `GroupHashEngine` ingests every morsel, then `finalize`.
- **parallel (round-robin + merge):** split morsels round-robin across N workers;
  each worker thread ingests its share into its OWN engine concurrently
  (exclusive morsel ownership — the thread-safety contract); merge the N partials
  into one (`engine.merge`, the WP-7 work); finalize once.

Correctness gated first (parallel == serial) before any timing. 15.7M rows, 4
aggregates, on an 18-core M-series.

## Result — it works, but cardinality decides everything

**Low/medium cardinality (1,000 groups):**

| threads | speedup | efficiency |
|--------:|--------:|-----------:|
| 2 | 1.93× | 97% |
| 4 | 3.73× | 93% |
| 8 | 6.13× | 77% |
| 16 | 8.68× | 54% |

Near-linear to 4 threads, 6× at 8. The merge + finalize + dispatch tax is
negligible because merging 1,000-group tables is cheap. **This is the common
case and it parallelises beautifully.**

**High cardinality (500,000 groups):**

| threads | speedup | efficiency |
|--------:|--------:|-----------:|
| 2 | 1.37× | 68% |
| 4 | 1.66× | 41% |
| 8 | 1.68× | 21% |
| 16 | 1.39× | 9% (slower than 8) |

The speedup plateaus at ~1.7× and then **regresses** past 8 threads.

## The finding

`merge()` is **serial and O(total groups × partials)**. At high cardinality each
worker builds a near-full hash table (partitioning the rows barely shrinks the
group set each worker sees), and then the single-threaded merge of N large
partial tables dominates wall time — more workers means more partials to merge,
so 16 threads is worse than 8. This is textbook Amdahl: the serial merge caps the
parallel section.

## Consequence for the M4 parallel-agg operator

There are two parallel-aggregation strategies; the operator must pick by
cardinality (or estimate it):

1. **Round-robin partition + merge** (this prototype) — workers ingest arbitrary
   morsels, partials are merged. **Cheap merge ⇒ great for low/medium
   cardinality.** Use when estimated NDV(group keys) is small relative to row
   count. The mergeable engines + `merge()` (WP-7) already support this exactly.

2. **Hash-partition by group key** — repartition rows so each worker owns a
   disjoint key range (`hash(key) % N`); each worker's groups are unique across
   workers, so there is **no merge** — finalized outputs concatenate. Pays a
   per-morsel scatter (a shuffle) instead. **Use for high cardinality**, where
   the no-merge property removes the Amdahl bottleneck. Needs a Cython
   row-scatter-by-key-hash kernel to be measured fairly (a Python scatter would
   misrepresent it), so it is **not yet prototyped** — but it avoids the merge by
   construction.

**Recommendation:** the M4 grouped-agg operator should default to strategy 1
(round-robin + merge) — it is built and validated today and wins the common case
— and switch to strategy 2 (hash-partition, no merge) when a cardinality estimate
(NDV statistics already exist in the optimizer) crosses a threshold to be tuned.
Distinct (COUNT DISTINCT) will have the same split once its merge lands.

## Caveats

- Synthetic morsels isolate aggregation from scan/IO (correct for measuring the
  agg path; a real query also overlaps scan/decode, which is already C++-parallel
  below the operator).
- The round-robin merge cost is the *serial* tail; a tree/pairwise merge across
  workers would parallelise part of it but does not change the asymptotic
  conclusion for very high cardinality — hash-partition is the real fix there.
- 16-thread efficiency drop at low cardinality (54%) is partly DRAM bandwidth and
  the serial finalize, consistent with `parallel_scaling.py`.
