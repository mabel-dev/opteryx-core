# Parallel Grouped Aggregation — Approaches, Costs, and the Chosen Design

Status: **design reference**, written 2026-06-19 from the M4 free-threading
investigation. Companion to `M4_PARALLEL_PATH_FORWARD.md` (which carries the
decision/recommendation); this document *describes the approaches* and the
evidence behind them so the reasoning is recoverable without re-deriving it.

All measurements: free-threaded CPython 3.14t (GIL off), Apple Silicon (18
logical / **6 performance** cores), full ClickBench `hits` (~92M rows), scan
excluded unless stated, warm, min-of-3, W = worker count.

> **⭐ 2026-06-19 — riskiest unknowns now measured with a REAL native scatter
> kernel; this supersedes earlier estimates.** (1) The real minimal scatter costs
> **320–420ms @ W8, not the assumed 130ms** → true end-to-end for COUNT is
> **~2.4–2.9× @ W8, NOT 4.5×** (keying still ~6–7×, but the *serial* scatter is now
> the prelude bottleneck). (2) **Key skew is a hard killer** — degrades to 1.3× at
> 50% hot key and **0.7× (loses) at 90%**, crossing serial ~70% — **skew mitigation
> is mandatory before default-on.** (3) **The holistic-aggregate advantage is REAL
> and verified** — COUNT(DISTINCT) parallelizes **3.84× @ W8, exactly correct**;
> the merge model cannot do this at all. (4) Multi-col + NULL keys verified correct.
> **Reframe: the design's value is parallelizing holistic/non-mergeable aggregates
> (COUNT(DISTINCT) ≈ 17% of ClickBench, MEDIAN, PERCENTILE), NOT faster COUNT** (which
> at ~2.4× and skew-fragile is marginal). Two prerequisites: parallelize the scatter
> (it is ~47% of the COUNT(DISTINCT) total; would lift COUNT→~4×, DISTINCT→~6×) and
> skew mitigation.
>
> **⭐ Update — both prerequisites now MEASURED green (2026-06-19).** Parallel
> multi-producer scatter scales **5–6× (NOT bandwidth-bound)** → e2e **COUNT
> 2.34→3.31×, COUNT(DISTINCT) 3.74→4.78×** (short of the inferred 4×/6× only due to
> a *serial materialization tail* — `scatter_to_morsels` concat-memcpy + Morsel wrap
> — which producer-side output buffers / pipelining would recover; not a wall). **Skew
> handled:** salting rescues mergeable COUNT (90%-hot 0.97→**2.17×**, ~free on
> uniform, bins balanced); holistic COUNT(DISTINCT) is **naturally robust to
> group-key skew** (routes by value). **Design is de-risked and worth building.**

---

## 1. The pipeline and its stages

A grouped aggregation (`SELECT k, AGG(v) … GROUP BY k`) runs these stages. They
are **not mutually exclusive** — some of our measurements span several, and the
engine fuses keying and accumulate into one pass.

| # | Stage | What it does |
|---|-------|--------------|
| S1 | Scan-IO | fetch/mmap column chunks |
| S2 | Decode | decompress → column vectors |
| S3 | Scatter / distribute | route rows to workers by `hash(key) % W` *(parallel models only)* |
| S4 | Keying | hash the key, probe, `find_or_insert` → group slot |
| S5 | Accumulate | apply the value to the slot (`+=1`, `+=v`, set-insert for DISTINCT) |
| S6 | Merge / recombine | combine per-worker partials *(round-robin model only)* |
| S7 | Finalize | ORDER BY / LIMIT / emit |

Two empirical facts frame everything:
- **S2 (decode) is already parallel and lock-free** (8-thread C++ pool; decode
  runs entirely outside every mutex). It is not a bottleneck and not our lever.
- **S4 (keying) dominates the cost of COUNT-class queries** — 84–93% of ingest.
  S5 (accumulate) dominates only for many/heavy-aggregate queries (e.g. 16× SUM:
  keying drops to ~16%). COUNT(DISTINCT) is keying-bound too — its per-group
  set-insert *is* a probe.

---

## 2. The approaches we evaluated

### 2.1 Round-robin + merge (DEAD)
Each worker pulls round-robin morsels (so it sees the **whole** key space), keys
+ accumulates into its own table, then the W partial tables are merged.

- **Serial merge:** total **1.10×** (Q16); the merge re-keys every group and
  **grows with W** (629→798→916ms) — it is the wall.
- **Parallel merge (tree-reduction of disjoint pairs):** **0.74×** — *worse*; the
  merge is fundamental re-keying work, and parallelizing it adds DRAM-bandwidth
  and refcount contention.

**Verdict: dead, both directions.** Round-robin forces every worker to key the
full key space → W overlapping tables → an `O(W × groups)` merge that costs ≈ the
aggregation itself.

### 2.2 Central-key + parallel dense-accumulate (wins only when accumulate is heavy)
Key **once, serially** (S4) to assign every row a dense `group_id`; scatter the
narrow `(group_id[, value])` (S3); workers do **hash-free** dense accumulate
`acc[gid] += v` (S5) over disjoint group ids; concat.

- COUNT (Q16): **1.16×** — capped by the serial keying (Amdahl predicted 1.18×).
- 16× SUM, low card (Q30g): **1.92×** — accumulate is 83% here and it
  parallelizes; the serial keying is only 16%.
- Low-card light COUNT (Q08): **0.79×** — overhead > work.

**Verdict: real, but narrow.** It only wins when keying is a small fraction.
It is strictly **dominated by 2.3** (which parallelizes keying too).

### 2.3 Row-routing shuffle + parallel keying (CHOSEN)
Scatter rows by `hash(key) % W` (S3) so each worker owns a **disjoint** key
slice; each worker keys **and** accumulates its slice **in parallel** (S4+S5);
concat (S7). Disjoint slices ⇒ **no merge**.

- **Keying (S4) alone: ~7× @ W8** for COUNT and int COUNT(DISTINCT), still
  climbing at W8. This is the real, approximation-free number.
- **End-to-end: ~4.5×** once S3 + S7 are included (see §4 for why 7 → 4.5).
- String keys: **~1.4×, walls at W4** (bandwidth-bound).

**Verdict: the design.** It parallelizes the *dominant* stage (keying) instead of
working around it, and it subsumes 2.2 (it parallelizes accumulate as well).

---

## 3. Cost matrix

Performance = **speed-up vs serial-int = 1.0** for that stage; parallel = W=8
unless noted; `?` = not measured; `<1` = regression.

### Per stage

| Stage | Mode | Data | Perf | Notes |
|---|---|---|---|---|
| S1 Scan-IO | serial / parallel | int/str | 1 / ? | local mmap near-free; parallel folded into decode pool |
| S2 Decode | parallel | int/str | ? | **already parallel, lock-free** 8-thread C++ pool; ratio never isolated |
| S3 Scatter (minimal) | parallel | int | cost ≈ 0.09× of serial total | 51ms@W1 → 130ms@W8 (92M); **prototyped kernel, not in tree** |
| S3 Scatter (`partition_by_hash`) | parallel | int | **catastrophic** | 2.9–4.0s@W8 — the only router in tree; unusable |
| S3 Scatter | parallel | str | ? | variable-length; not built/measured; expected costlier |
| S4 Keying | serial | int | 1 | baseline (~1070ms on Q16) |
| S4 Keying | parallel | int | **~7×** | row-routing, disjoint slices; still climbing at W8 |
| S4 Keying | serial | str | 1 | own baseline (absolutely slower than int; ratio not measured) |
| S4 Keying | parallel | str | **~1.4×, walls W4** | bandwidth-bound; do not parallelize |
| S5 Accumulate | serial | int | 1 | tiny for COUNT; large for many-SUM |
| S5 Accumulate | parallel | int | ~2.7–3.6× | dense hash-free `acc[gid]+=v`; matters only when aggregates heavy |
| S5 Accumulate | parallel | str | ? | not isolated |
| S6 Merge | serial | int | caps total ~1.1× | **grows with W** (629→916ms) — the round-robin wall |
| S6 Merge | parallel | int | **0.74×** | tree-reduction; DEAD |
| S7 Finalize | serial | int/str | 1 | ~2.5% of wall; left serial (negligible) |

### End-to-end models

| Model (stages) | Data | Perf @ W8 | Notes |
|---|---|---|---|
| Round-robin + serial merge (S4+S5 ‖, S6 serial) | int | 1.10 / 1.12 / 1.00 | shipped-then-reverted default; net ClickBench regression |
| Round-robin + parallel merge (S6 tree) | int | **0.74×** | DEAD |
| Central-key + parallel accumulate (S4 serial, S3, S5 ‖) | int | Q16 1.16 / **Q30g 1.92** / Q08 0.79 | wins only on heavy accumulate |
| **Row-routing parallel keying, parallel scatter (S3‖+S4+S5‖)** | int | **3.31× total @ W8**; ~7× key-only | CHOSEN; headroom to ~4× by removing the serial materialization tail |
| Row-routing parallel keying | str | ~1.3–1.4× | bandwidth wall |
| **COUNT(DISTINCT) grouped (holistic, non-mergeable)** | int | **4.78× total @ W8, correct** | the real prize — merge CANNOT parallelize this; headroom to ~6× |
| COUNT(DISTINCT) | str | ~1.4× walls W4 | Q06 |
| Row-routing under **key skew, SALTED** (mergeable) | int | uniform 3.38× → 90% hot **2.17×** (rescued) | salting ~free on uniform; bins balanced |
| Row-routing under group-key skew (holistic, route-by-value) | int | maxbin unchanged — **naturally robust** | no mitigation needed for group-key skew |

### Context (non-grouped operators)

| Operator | Data | Perf | Notes |
|---|---|---|---|
| Stateless filter / projection | int | ~4× (4.48 / 3.4) | per-morsel independent, no merge |
| Ungrouped aggregate | int | ~4.5× | scalar merge trivial |

---

## 4. The chosen design in detail

```
            ┌──────────────── producers (parallel) ────────────────┐
 scan ─▶ morsel ─▶ for each row: bin = hash(key) % W ─▶ append (key[,vals]) to buffer[bin]
            └───────────────────────── S3 scatter ─────────────────┘
                                   │  (disjoint by key)
        ┌──────────┬──────────┬────┴─────┬──────────┐
      worker0    worker1    worker2    …  workerW-1        ← S4+S5, parallel
   key+probe+    (its own   disjoint                       each builds its OWN table
   accumulate     slice)    keys                           over its key slice
        └──────────┴──────────┴──────────┴──────────┘
                                   │
                              concat (S7)               ← no merge: slices disjoint
```

**Why no merge:** `hash(key) % W` is a pure function, so every occurrence of a
key routes to the same worker. Each key lives in exactly one worker's table →
the tables share no keys → finalize is a concatenation, not a reconciliation.

**The 7× vs 4.5× relationship.** They are the *same design at two scopes*:
- 7× = S4 (keying) in isolation: `1708ms serial ÷ 243ms parallel`.
- 4.5× = end-to-end: `scatter 130 + keying 243 + finalize 13 = 386ms` vs 1772.

The scatter (~34% of the parallel total) is the entire 7 → 4.5 dilution. Levers
to push 4.5 toward 7: a cheaper scatter, **parallelizing the scatter** across
producer threads, or **pipelining** scatter with keying so it overlaps instead of
being a serial prelude.

**The keystone — a minimal native fixed-width scatter (S3).** Everything above
depends on a scatter that routes by `hash(key)%W` in a single pass, appending a
narrow fixed-width payload into W buffers, nogil. The only router in the tree
(`partition_by_hash`) is 2.9–4.0s and unusable; a prototype kernel hit ~130ms, so
this is feasible — **but it must be built, and scatter+keying have never been run
end-to-end together.** That is the one unverified link in the 4.5×.

**COUNT(DISTINCT) is the same design.** Partition by the *distinct value's* hash
→ each worker sees disjoint values → per-group distinct counts are summable with
no double-count. Dedup is a set-insert = a probe, so it parallelizes exactly like
keying (~7× int).

### 4.1 The decisive property: holistic aggregates parallelize

Because `hash(key) % W` sends **every** row of a group to **one** worker, that
worker sees the group's **entire** input and produces its **complete, final**
result — there is no partial state to combine. This removes the `is_mergeable`
gate entirely: **all aggregates qualify, including the holistic ones the merge
model can never parallelize** — MEDIAN, PERCENTILE, COUNT(DISTINCT), MODE,
collect-to-list, etc. The merge model is fundamentally blocked on these (you
cannot merge two partial medians); row-routing is not. *This is the strongest
single argument for the design.* (Verified only for COUNT/COUNT(DISTINCT) so far;
MEDIAN/percentile end-to-end is unproven — §6/§8.)

### 4.2 Mechanics (PROPOSED — open decisions flagged ⚑ for the architect)

**Scatter buffers & handoff.** Proposed: **per-`(producer × worker)` append
buffers** in fixed chunks (e.g. 64 KB), so producers never contend on a shared
buffer and a consumer drains whole chunks. ⚑ *push* (producer enqueues a full
chunk to the worker) vs *pull* (worker owns its bins, producers append under a
per-bin lock) — recommend push + per-(producer,worker) buffers = lock-free hot
path.

**Threading & overlap.** ⚑ Phase-1 ship **two-phase** (scatter all → then key
all) — simplest, matches what we measured. **Pipelined** (workers key chunks as
they fill) closes the 7→4.5 gap but is harder; defer. ⚑ One `CppThreadPool` reused
for producers then consumers, or two pools.

**Worker count.** `resolve_worker_count` (cap currently 8); ⚑ revisit the cap for
this design — keying was still climbing at W8 on 6 perf cores; prod core counts
differ.

**Memory, ownership & FT-safety.** Each worker **owns** its hash table + dense
accumulators → no sharing, no hot-path locks (the FT-safety argument). Scatter
buffers are producer-owned and **moved** to the worker on handoff (RAII). The scan
source is already `std::mutex`-guarded. **Telemetry must be per-worker then
summed** — no shared counter increments (an FT race otherwise).

**Error & cancellation.** Reuse the existing `ErrCtx` / `PipelineContext._exc`
pattern: a worker records its first exception, the main thread re-raises after the
barrier; a terminate flag is checked at chunk boundaries.

### 4.3 Semantics (must hold; verify against existing engine behaviour)

- **Multi-column keys:** route by the engine's **composite-key hash** (already
  computed for keying); disjointness holds on the tuple. ⚑ confirm the composite
  hash is obtainable pre-keying for routing.
- **NULL keys:** a NULL key routes to a bin like any value; since all NULLs hash
  identically they land in one worker → the canonical NULL group is not split →
  correct. Must verify against the engine's existing null-key semantics (a
  repeated source of bugs).
- **Payload composition:** for COUNT, move the key only; for SUM/AVG/MIN/MAX move
  key + the (fixed-width) agg input column(s); for COUNT(DISTINCT) move key +
  distinct column. Width = key + Σ agg-input columns, all fixed-width or the query
  routes to serial.

---

## 5. Optimal and pathological scenarios

**Optimal:**
- Fixed-width / int keys (cheap hash+probe, narrow fixed-width scatter).
- High cardinality — keying dominates, so parallelizing it captures the most.
- Even hash distribution — balanced bins, all workers equally loaded.
- Rows well above the floor (amortizes scatter + thread setup).
- Memory-bandwidth headroom (keying is latency-bound).

**Pathological:**
- **Key skew — the headline risk, and UNMEASURED.** Our benches used near-uniform
  UserID. If a few hot keys/values dominate, they hash to one bin → one worker
  does most of the work → collapse toward serial. *Must be benched on a skewed
  key before the 7× is trusted.* Mitigations if it bites: salting hot keys, a
  two-level hash, or work-stealing on bin drain.
- **String / variable-length keys** — bandwidth wall (~1.4×, stalls at W4); the
  scatter is costlier too. Route to serial.
- **Low cardinality + light aggregate** (e.g. Q08) — keying is trivial; scatter +
  thread overhead exceed the work → regression (~0.79× observed in the sibling
  model). Keep serial.
- **Tiny inputs** (below the row floor) — setup + scatter > total work.
- **Wide/heavy scatter payload** — many/wide agg columns to move saturate
  bandwidth and compete with keying.

---

## 6. Measured vs assumed vs unmeasured (honesty ledger)

| Claim | Status |
|---|---|
| Keying parallelizes ~7× (int, uniform key) | **measured**, approximation-free |
| COUNT(DISTINCT int) parallelizes ~7× | **measured** |
| String keys wall at ~1.4× / W4 | **measured** |
| Real native scatter (raw int key) = **320–420ms@W8** | **measured** (supersedes the 130ms group_id-scatter estimate) |
| End-to-end COUNT = **~2.4–2.9×@W8** (NOT 4.5×) | **measured** with real scatter |
| Key skew: 1.3×@50% hot, **0.7×@90%** (loses), crosses ~70% | **measured** — mitigation mandatory |
| Holistic aggregate (COUNT DISTINCT) parallelizes **3.84×**, correct | **measured** — the decisive advantage, confirmed |
| Multi-column + NULL-key routing correctness | **measured** correct == serial |
| Parallel multi-producer scatter scales 5–6× (not bandwidth-bound) | **measured** |
| Parallel scatter e2e: COUNT **3.31×**, COUNT(DISTINCT) **4.78×** | **measured** (headroom to 4×/6× via removing serial materialization tail) |
| Salting rescues skewed mergeable COUNT (90%-hot 0.97→2.17×) | **measured**, ~free on uniform |
| Holistic (route-by-value) naturally robust to group-key skew | **measured** |
| Removing the serial materialization tail recovers toward 4×/6× | **inferred** (not yet built) |
| Decode parallel & lock-free | **measured** (static + timing) |
| Merge dead (serial 1.1× ceiling, parallel 0.74×) | **measured** |

---

## 7. Cost-model gate (when to engage)

The design is not universally a win; engage it via a cost model, not a flag:

- **Engage** when ALL hold: (1) key is **fixed-width** (int/temporal/decimal, not
  string/varbinary); (2) input rows ≥ floor (start at the existing
  `PARALLEL_MIN_ROWS = 262_144`); (3) keying+accumulate is expected to dominate —
  proxy: estimated `groups × per-group-agg-cost` above a threshold, OR a holistic
  aggregate is present (always worth it since serial has no alternative).
- **Stay serial** when: **string/var-width** key (bandwidth wall), input below the
  floor, or low cardinality + light aggregate (e.g. Q08-class: ~tens of groups,
  COUNT only → overhead > work, measured 0.79×).
- ⚑ The cardinality/agg-weight threshold needs **calibration from the e2e bench**
  (§8) — the crossover point where scatter+thread overhead is repaid. Until
  calibrated, a conservative high floor is safer than engaging marginal queries.
- This is legitimate algorithm selection by a cost model — *not* gating-to-where-
  it-wins: the design genuinely cannot win on the excluded shapes.

---

## 8. Open work

### Done (measured)
- ✅ Minimal native scatter built; real cost 320–420ms@W8 (`dev/_native_scatter.pyx`).
- ✅ Row-routing parallel keying wired e2e — COUNT ~2.4–2.9×, COUNT(DISTINCT) 3.84×.
- ✅ Skew bench — degrades to 0.7× at 90% hot (mitigation mandatory).
- ✅ Multi-col + NULL correctness verified.

### Both make-or-break risks RESOLVED (measured green, 2026-06-19)
- ✅ **Parallel scatter recovers** — scales 5–6× (not bandwidth-bound); e2e COUNT
  3.31×, COUNT(DISTINCT) 4.78×. `dev/_native_scatter_parallel.pyx`.
- ✅ **Skew handled** — salting rescues mergeable COUNT (90%-hot → 2.17×, ~free on
  uniform); holistic route-by-value is naturally robust to group-key skew.
  `dev/_native_scatter_salt.pyx`.

### Remaining work (no known blockers — this is a build/optimize phase)
1. **Kill the serial materialization tail** — build per-worker output buffers in the
   producer phase (or pipeline scatter↔keying) to recover COUNT→~4×, DISTINCT→~6×.
   The remaining headroom, an optimization not a risk.
2. **Production build** — wire row-routing + parallel scatter + salting into the
   engine (parallel_engine / GroupedAggregateHashedNode), behind the cost-model gate.
   Surface the §4.2 ⚑ decisions (push/pull buffers, one pool vs two, two-phase vs
   pipelined) to the architect before building.
3. **Cost-model gate** (§7), calibrated from the benches; include the route-by-value
   low-cardinality bin-imbalance note for holistic aggregates.
4. **Online hot-key detector** for salting (the bench used an exact pre-pass; prod
   needs a sampled/online detector).
5. **String-key keying** stays on the serial-keying track (Option C in
   `M4_PARALLEL_PATH_FORWARD.md`) — the only lever where parallelism doesn't pay.
