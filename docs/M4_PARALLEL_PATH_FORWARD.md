# M4 Parallel Execution — Path Forward (Options)

Status: **decision document for the architect.** Written 2026-06-18 after the
free-threading move and the parallel-GROUP-BY investigation. No code decision is
implied by this doc — it lays out what we have proven and the options, with a
recommendation.

---

## 1. What we have empirically established this cycle

All measured on free-threaded CPython 3.14t (no GIL), Apple Silicon (18 logical
cores, **only 6 performance cores**), full ClickBench `hits` (~92M rows), warm.

| Finding | Evidence | Confidence |
|---|---|---|
| Free-threading works end-to-end | all extension modules declare FT-safe (nanobind `NB_FREE_THREADED`, Cython `freethreading_compatible`, pyo3 `gil_used=false`); `import opteryx` keeps GIL off; `make q` 190/0, `make tpch` 22/0 | high |
| Stateless parallel (filter/projection) wins | filter 4.48×, selection 3.4× (prior runs) | high |
| Ungrouped aggregate parallel wins | ~4.5× | high |
| Parquet **decode is already parallel + lock-free** | 8-thread C++ pool; decode runs entirely outside every mutex (`io_pipeline.hpp:727-747`) | high |
| Parallel **ingest/keying** scales | Q16 1100ms → 262ms @ W=8 (~2.5×) | high |
| **Grouped-agg via round-robin + merge does NOT win** | serial merge peaks **1.10×** (Q16) / 1.12× (Q13); declines past W≈4–8 | high |
| **Parallelising the merge makes it WORSE** | tree-reduction over disjoint pairs: Q16 1.10×→**0.74×**, degrades with W | high |
| The merge is the wall | 73% of Q16 wall (916ms @ W=8), serial, **grows** with W (629→798→916) | high |
| The 6.4× merge microbench was option B | it merged **disjoint radix bins** (no `find_or_insert`), never the live round-robin path | high |
| Hardware ceiling is ~6 cores | sweet spot W=4–8, declines after (E-core spillover) | high |
| Net effect on ClickBench today | **regression 92s → 106s** with parallel default on | high |
| ClickBench time mix | GROUP BY ~64%, COUNT(DISTINCT) ~17%, string-MIN ~8%, ungrouped ~1% | high |

### The one-line conclusion

**Merge-based recombination is a dead end.** Serial merge caps at ~1.1×;
parallel merge regresses. The cost is *fundamental re-keying work* (re-hash +
`find_or_insert` every group, W times) — not GIL or serialization — so adding
threads only adds DRAM-bandwidth and free-threaded refcount contention. The win
requires **not merging**: partition the key space so workers own **disjoint**
groups and recombination becomes a trivial concatenation.

---

## 2. Why "no merge" is the crux

Round-robin gives each worker a random subset of *morsels*, so every worker sees
the *whole* key space → W overlapping hash tables → a merge whose cost scales
with `W × groups`. If instead we route each row by `hash(key)` to an owning
worker, each worker sees a **disjoint** slice of the key space → its table shares
no keys with any other → finalize is `concat`, cost ~0.

The trade we are making: pay an **O(rows) scatter** (route each row to its bin)
to delete an **O(W × groups) merge**. For high-cardinality GROUP BY (groups ≈
rows, the expensive case) this is strongly favourable. For low-cardinality the
query is scan-bound and parallel width is irrelevant either way.

This is the DuckDB / ClickHouse model, and it is the "give me bin 0" shape the
architect has been pointing at since the start.

---

## 3. Options

### Option A — Key-partitioned exchange (the "no merge" model) — RECOMMENDED

Route rows by `hash(group_key)` into W disjoint partitions; each worker ingests
only its partition; finalize concatenates. No cross-worker merge.

Two implementable variants:

**A1 — Morsel-level scatter (eager partition).**
A router pulls each scan morsel and splits it into W sub-morsels by key-hash bin,
handing each bin to its owning worker. Workers ingest as today but never see
foreign keys.
- *Pros:* simplest mental model; reuses the existing per-worker ingest; the
  scatter is a single O(rows) C++ pass, gil-free.
- *Cons:* the scatter is a copy (the "per-morsel K-way copy" that hurt the prior
  GIL'd exchange — but that attempt also had a full-table barrier and was GIL'd;
  gil-free + streaming should be far cheaper). Router can become a serial
  bottleneck if scatter isn't cheap enough.

**A2 — Two-level radix (ClickHouse-style, partition-on-collect).**
Each worker ingests its round-robin morsels into *many* small fixed buckets by
`hash(key) mod N` (N ≫ W, e.g. 256). Because bucket assignment is by key, bucket
*b* across all workers holds the same disjoint key slice. Finalize: hand whole
bucket *b* to one thread, which combines the W copies of bucket *b* — but those
are small and, critically, can be combined per-bucket in parallel across buckets.
- *Pros:* no up-front router/scatter bottleneck (workers still pull round-robin);
  the per-bucket combine is embarrassingly parallel and each bucket is small;
  this is the shape the 6.4× microbench actually measured.
- *Cons:* still a combine step (but per-bucket, parallel, disjoint — not the
  global re-keying merge); more bookkeeping; hash table must support cheap
  bucketed layout.

> A2 is closer to what we already proved works (the microbench) and avoids a
> serial router. A1 is simpler to build first. A spike of both on Q16 would settle
> it; A2 is the likely end state.

**Also unlocks COUNT(DISTINCT) (~17% of ClickBench).** Partition by
`hash(distinct_col)` → each worker counts distinct in its disjoint slice → sum
the counts. Same exchange machinery; today this is forced serial.

**Estimated ceiling:** ingest already scales ~2.5×; deleting the merge removes
the 73% serial fraction → Amdahl ceiling ~**4×** on high-card GROUP BY, with the
real number bounded by the 6 performance cores.

> **⚠️ MEASURED 2026-06-18 — this option is now downgraded.** A faithful prototype
> (`dev/parallel_groupby_proto.py`) of the shuffle/exchange showed the **distribute
> step costs ≈ the entire serial aggregation** (≈1950–2060ms for the Q16/Q13/Q30g
> classes): it must hash the key and copy every row into per-worker bins. Net
> speedup at W=8 was **0.58–0.87× (a regression)** on every query, including the
> accumulate-heavy one — even though the parallel accumulate phase itself scaled
> 3.3–6.1×. The shuffle pays the same data-movement tax as the merge; it does not
> escape it. **The generalisable result: any grouped-agg scheme that physically
> moves data — partial results (merge) OR rows (shuffle) — pays a copy ≈ the work
> it parallelises.** Option A is therefore not the lever for high-/medium-card
> GROUP BY. The only untaxed corner is accumulate-heavy + LOW cardinality (tiny
> merge), which overlaps with already-parallel (ungrouped) or scan-bound cases.

---

### Option B — Inter-query / pipeline parallelism only

Drop intra-query parallelism; use free-threading to serve **concurrent queries**
(prod is GCP Cloud Run, request-concurrent) and/or run independent pipeline
segments on separate threads.
- *Pros:* free-threading + thread-safety work already done is the whole
  deliverable; no risky exchange to build; helps real multi-tenant throughput.
- *Cons:* does nothing for single-query latency (the ClickBench metric); most
  ClickBench queries are a single pipeline so pipeline-level gives little.

---

### Option C — Faster serial (reduce work, don't add cores)

Leave execution serial; attack the GROUP BY cost directly: better hash table,
SIMD key hashing, fewer passes, cheaper finalize.
- *Pros:* benefits *every* deployment (no core count needed); compounds with
  Option B's concurrency; no recombination problem to solve.
- *Cons:* incremental, not transformative; doesn't use the free-threading we just
  built; ceiling is single-core throughput.

---

### Option D — Park intra-query parallelism; bank free-threading

Set the default worker count to 1 (serial), keep all the free-threading +
thread-safety infrastructure, and revisit intra-query parallelism later.
- *Pros:* removes the current 106s regression immediately; nothing is lost (infra
  stays); honest about intra-query parallelism not being ready.
- *Cons:* no latency win now. **Note:** this is *not* "gating to where it wins" —
  it is acknowledging the feature is not done and not shipping a regression. It is
  a holding position, not a solution.

---

## 4. Cross-cutting items (independent of which option)

- **Default `MAX_EXECUTION_WORKERS` ships a regression today** (defaults to CPU
  count → 8 → 106s). Whatever we choose, the default cannot be "slower out of the
  box." If Option A is pursued, default-on follows once it wins; until then the
  default should be 1.
- **6 performance cores cap everything.** No grouped-agg shape will exceed ~4–6×
  on this machine regardless of model. Prod (x86 Cloud Run) core counts differ —
  validate there before tuning the cap.
- **`Morsel.__getitem__` segfaults under free-threading** (serial, independent of
  the scheduler) — a latent FT-safety bug already spawned as a task. Free-threading
  *exposes* such bugs; a broader FT-safety audit is owed before default-on.
- **`make q` runs on tiny data** and is weak at catching concurrency races. A
  dedicated concurrency/stress gate is needed before trusting "green" under FT.

---

## 5. Recommendation (revised 2026-06-19 — THIRD revision, authoritative)

### ⭐ The reversal: parallel KEYING beats serial keying ~7× (this changes everything)
Earlier revisions concluded "COUNT has no parallel win." **That was wrong** — it was
an artifact of only ever testing *serial-keying* models (the central-key model keeps
keying serial; merge re-keys serially). When rows are routed by `hash(key)%W` so each
worker keys its **disjoint slice in parallel**, keying scales **~7× key-only @ W8**
(measured, approximation-free) for COUNT and int COUNT(DISTINCT), still climbing at
W8. COUNT(DISTINCT) is confirmed keying-bound (dedup = set-insert = a probe) and
parallelizes identically.

- Q16 COUNT (int key): **~7× key-only**, ~4.5× total (see caveat).
- Q09 COUNT(DISTINCT UserID) GROUP BY RegionID: **~7× key-only** — partition by the
  distinct value's hash → per-group counts summable, no double-count.
- Q06 COUNT(DISTINCT SearchPhrase) (string key): **~1.4×, walls at W4** —
  string-key construction + W high-card string sets is memory-bandwidth bound.
  **Fixed-width/int keys scale; string keys do not.**

### ⚠️ The load-bearing dependency: a minimal native scatter must be BUILT
The ~7× is the *keying* phase. End-to-end ~4.5× **assumes a 130ms-class minimal
columnar scatter** (measured in an earlier prototype kernel, so grounded — but never
run end-to-end with the parallel keying). The only scatter currently in the tree,
`partition_by_hash`, is **2.9–4.0s @ W8 → catastrophic**. So the realizable win is
**contingent on building a minimal native fixed-width columnar scatter** (route by
`hash(key)%W`, single pass, append narrow fixed-width payload into W buffers, nogil).
That primitive is the keystone of the whole parallel-agg story.

### What's settled
- **Merge-based recombination is dead** (serial merge 1.1× ceiling; tree-reduction
  regressed). Do not revisit.
- **Serial-keying models cap near 1×** (central-key 1.16× on Q16). The win requires
  *parallel* keying via row-routing, which requires the minimal scatter.
- **String keys don't parallelize** (bandwidth wall ~1.4×); faster *serial* keying
  (Option C) remains their only lever.

### The model that wins: row-routing shuffle with PARALLEL keying (fixed-width keys)
**scatter rows by `hash(key)%W` → each worker keys+aggregates its DISJOINT slice in
parallel → concat.** Disjoint key slices ⇒ no merge. Measured: keying ~7× @ W8 for
COUNT and int COUNT(DISTINCT); ~4.5× total assuming the minimal scatter (see caveat).
The accumulate-heavy case folds in here too (its earlier 1.92× was the same family
with keying kept serial — parallel keying lifts it further).

**The governing variables are (1) does the minimal native scatter exist, and
(2) key type:**
- fixed-width / int keys → keying parallelizes ~7× → ~4.5× total once scatter exists;
- string keys → memory-bandwidth wall at W4 (~1.4×) → not worth parallelizing.

(Superseded: the earlier "governing variable is keying fraction" framing — true only
for *serial*-keying models. Parallel keying wins even on keying-bound COUNT.)

### Recommendation
1. **Default worker count → 1 (DONE).** Stops the 106s regression.
2. **Build the keystone: a minimal native fixed-width columnar scatter** (route by
   `hash(key)%W`, single pass, append narrow fixed-width payload to W buffers, nogil).
   The only existing router (`partition_by_hash`) is 2.9–4.0s and unusable; a prior
   prototype kernel hit ~130ms, so this is feasible. **Everything below depends on it.**
3. **Then wire row-routing parallel keying** and confirm the ~4.5× end-to-end (the one
   combination not yet run together: real scatter + parallel keying). Gate engagement
   on a cost model: **fixed-width key + enough rows**; route string-keyed and tiny
   inputs to serial.
4. **Faster serial keying (Option C)** is the lever for string-keyed GROUP BY (which
   won't parallelize) and small inputs — tax-free, benefits every deployment.
5. **Bank existing wins:** stateless ~4×, ungrouped aggregate ~4× (covers the
   heavy-SUM *no-GROUP-BY* class like Q30), request-concurrency via free-threading.

### ClickBench impact (revised — now plausibly material)
ClickBench is dominated by COUNT / COUNT-DISTINCT GROUP BY, which we now know
parallelize ~7× on the *keying* — **so this CAN move ClickBench**, unlike the earlier
(serial-keying) conclusion. The mixed reality: int-keyed group queries (RegionID,
UserID, IDs) win; **string-keyed ones (SearchPhrase, URL, Title) hit the bandwidth
wall and won't** — so the realized ClickBench gain depends on the int-vs-string key
mix, and on the scatter primitive landing near its prototyped ~130ms.

Do **not** invest further in merge-based recombination — it is empirically closed.

---

## 6. Open questions for the architect

1. Approve Option A as the direction? A1-first or straight to A2?
2. Interim default worker count → 1 now? (removes the 106s regression)
3. Is single-query ClickBench latency the target, or is prod multi-tenant
   throughput (which Option B already serves) equally weighted?
4. How much FT-safety hardening is required before any parallel path is default-on
   (the `Morsel.__getitem__` class of latent bug)?
