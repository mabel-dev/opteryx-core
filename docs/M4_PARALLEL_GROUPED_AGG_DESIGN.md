# Parallel Grouped Aggregation — Approaches, Costs, and the Chosen Design

Status: **design reference**, written 2026-06-19 from the M4 free-threading
investigation. Companion to `M4_PARALLEL_PATH_FORWARD.md` (which carries the
decision/recommendation); this document *describes the approaches* and the
evidence behind them so the reasoning is recoverable without re-deriving it.

All measurements: free-threaded CPython 3.14t (GIL off), Apple Silicon (18
logical / **6 performance** cores), full ClickBench `hits` (~92M rows), scan
excluded unless stated, warm, min-of-3, W = worker count.

> **⭐ 2026-06-19 — riskiest unknowns measured with a REAL native scatter
> kernel.** This block is the current truth; older inline figures in §3/§4 have
> been reconciled to it.
>
> 1. **Scatter cost.** The real minimal fixed-width scatter costs **320–420ms @ W8**
>    serial. With a **parallel multi-producer scatter** (scales **5–6×, NOT
>    bandwidth-bound**) the end-to-end is **COUNT 3.31× @ W8** and **grouped
>    COUNT(DISTINCT) 4.78× @ W8, exactly correct**. Both fall short of the
>    keying-only ~7× / ~6× *only* because of a **serial materialization tail**
>    (`scatter_to_morsels` concat-memcpy + Morsel wrap); producer-side output
>    buffers / pipelining recover it — an optimization, not a wall.
> 2. **Holistic aggregates parallelize for free, via the *same* route.** Because
>    `hash(key)%W` sends every row of a group to one worker, that worker computes
>    the group's *final* result with no cross-worker combine. This needs **no
>    special routing** — COUNT(DISTINCT)/MEDIAN/PERCENTILE just work under
>    route-by-group-key. This is the decisive advantage: the merge model cannot
>    parallelize these at all. (The earlier "route by the distinct value's hash"
>    special case is dropped as unwarranted complexity.)
> 3. **Key skew is the one residual risk.** A single dominant key routes wholly to
>    one bin (hash mixing distributes *distinct* keys, not occurrences of one key)
>    → degrades toward serial (measured **0.7× @ 90%-hot**, crossing serial ~70%).
>    **Salting is rejected:** it splits a key across bins and so reintroduces a
>    merge, destroying the disjoint-slice→concat invariant that is the design's
>    clearest win. Skew is handled by **algorithm selection** — the >floor scan
>    sample (§7) estimates skew and routes dominant-hot-key shapes to serial. ⚑
>    serial-cutover threshold needs calibration.
> 4. **Multi-col + NULL keys verified correct == serial.**

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
- **End-to-end: COUNT 3.31×, grouped COUNT(DISTINCT) 4.78× @ W8** with a parallel
  scatter (see §4 for the 7 → 3.31 dilution and the recoverable materialization tail).
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
| S3 Scatter (minimal, serial) | — | int | 320–420ms@W8 (~0.2× of serial total) | measured on a throwaway prototype (now deleted); production kernel to be built in `opteryx/compiled/` |
| S3 Scatter (minimal, parallel) | parallel | int | **5–6×** over serial scatter | multi-producer, **not bandwidth-bound**; prototype measured then deleted |
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
| Row-routing under key skew (single hot key) | int | uniform 3.31× → 90% hot **0.7×** | one hot key → one bin; salting **rejected** (breaks concat); cost-model routes skewed shapes to serial |

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

**The 7× vs 3.31× relationship.** They are the *same design at two scopes*:
- 7× = S4 (keying) in isolation: `1708ms serial ÷ 243ms parallel`.
- 3.31× = end-to-end COUNT once S3 (parallel scatter) + S7 are included.

Two things dilute 7 → 3.31: the scatter, and a **serial materialization tail**
(`scatter_to_morsels` concat-memcpy + Morsel wrap) that runs single-threaded after
the parallel keying. Levers to push back toward 7: **parallelizing the scatter**
across producer threads (done — 5–6×), **producer-side output buffers** to kill the
materialization tail, or **pipelining** scatter with keying so it overlaps instead
of being a serial prelude.

**The keystone — a minimal native fixed-width scatter (S3).** Everything above
depends on a scatter that routes by `hash(key)%W` in a single pass, appending a
narrow fixed-width payload into W buffers, nogil. The only router that was in the
tree (`partition_by_hash`) is 2.9–4.0s and unusable; the purpose-built kernel is
320–420ms serial and **5–6× parallel**, and scatter+keying have now been run
end-to-end together (COUNT 3.31×, COUNT(DISTINCT) 4.78×).

**COUNT(DISTINCT) needs no special case.** Under route-by-group-key, every row of
a group lands on one worker, so that worker holds the group's *entire* input and
computes the **exact** distinct count locally — there is nothing to sum across
workers. Dedup is a set-insert = a probe, so it parallelizes exactly like keying
(~7× int). (A "route by the distinct value's hash" scheme — needed only for a
*global*/ungrouped `COUNT(DISTINCT)`, a separate operator — is out of scope here
and is **not** used for the grouped path.)

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

**Worker count.** **Softcode** the default — derive it from the CPU count — and
allow an **environment-variable override** (e.g. `MAX_EXECUTION_WORKERS`) on top,
so it can be tuned by trial-and-error on Cloud Run (x86) without a rebuild. Keying
was still climbing at W8 on 6 ARM perf cores and prod core counts differ, so the
default must track the machine and the optimum be found in the target environment,
not fixed at a literal.

Worker count is **degree-of-parallelism only — it MUST NOT select a code path.**
`W=1` runs the row-routing engine with a single worker (scatter → one bin → key →
concat), the *same* path as `W=8`, not a divert to the serial grouped-agg node.
There is one parallel implementation; the worker count only sets how many slices
it splits into. (A divergent `if W==1: serial` path is forbidden — §2.)

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
- **Key skew — the one residual risk, MEASURED.** Because `hash(key)%W` routes
  every occurrence of a key to one bin, a single dominant key sends most rows to
  one worker → collapse toward serial (measured **0.7× @ 90%-hot**, crossing serial
  ~70%). Hash mixing distributes *distinct* keys well but cannot split one hot key.
  **Salting is rejected** — it splits a key across bins and so reintroduces a merge,
  destroying the disjoint-slice→concat invariant that is the design's clearest win.
  The sanctioned mitigation is **algorithm selection**: the >floor scan sample (§7)
  estimates skew, and dominant-hot-key shapes route to serial rather than degrade.
  ⚑ skew threshold for the serial cutover needs calibration.
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
| Key skew: 1.3×@50% hot, **0.7×@90%** (loses), crosses ~70% | **measured** — handled by serial cutover, NOT salting |
| Holistic aggregate (COUNT DISTINCT) parallelizes **3.84×** (serial scatter), correct | **measured** — the decisive advantage, confirmed |
| Multi-column + NULL-key routing correctness | **measured** correct == serial |
| Parallel multi-producer scatter scales 5–6× (not bandwidth-bound) | **measured** |
| Parallel scatter e2e: COUNT **3.31×**, COUNT(DISTINCT) **4.78×** | **measured** (headroom to 4×/6× via removing serial materialization tail) |
| Salting would rescue skewed mergeable COUNT (90%-hot 0.97→2.17×) | **measured but REJECTED** — breaks disjoint-concat invariant (§4/§5) |
| Removing the serial materialization tail recovers toward 4×/6× | **inferred** (not yet built) |
| Decode parallel & lock-free | **measured** (static + timing) |
| Merge dead (serial 1.1× ceiling, parallel 0.74×) | **measured** |

---

## 7. Cost-model gate (when to engage)

The design is not universally a win; engage it via a cost model, not a flag:

- **The floor does double duty.** Below `PARALLEL_MIN_ROWS` (≈250k, start at the
  existing `262_144`, exact value TBC) the input is too small to bother — **stay
  serial, no estimation**. At/above the floor, the scanned rows are large enough to
  serve as a **sample for estimating NDV and skew** — accepted as a sample, subject
  to error — so the gate does **not** depend on precomputed sidecar statistics.
- **Engage** when ALL hold: (1) key is **fixed-width** (int/temporal/decimal, not
  string/varbinary); (2) input rows ≥ floor; (3) no dominant hot key (from the
  sample skew estimate); (4) keying+accumulate is expected to dominate — proxy:
  sampled `groups × per-group-agg-cost` above a threshold, OR a holistic aggregate
  is present (always worth it since serial has no alternative).
- **Stay serial** when: **string/var-width** key (bandwidth wall), input below the
  floor, **dominant hot key** (skew cutover), or low cardinality + light aggregate
  (e.g. Q08-class: ~tens of groups, COUNT only → overhead > work, measured 0.79×).
- ⚑ The cardinality/agg-weight threshold **and the skew cutover point** need
  **calibration from the e2e bench** (§8).
- **The floor is the on/off switch, not the worker count.** Until calibrated, ship
  `PARALLEL_MIN_ROWS` at an effectively-infinite value (e.g. `1_000_000_000_000`) so
  the gate never selects row-routing in production; lower it (toward ≈250k) for
  calibration runs and once the crossover is known. Disabling the feature is "raise
  the floor," never "set workers to 1" — worker count is degree-only (§4.2). Note
  the distinction from below-floor inputs, which genuinely run the **serial
  grouped-agg node** (legitimate algorithm selection for small inputs); that serial
  node is *not* what `W=1` of the parallel path runs.
- This is legitimate algorithm selection by a cost model — *not* gating-to-where-
  it-wins: the design genuinely cannot win on the excluded shapes.

---

## 8. Open work

### Done (measured on throwaway prototypes, since deleted)
- ✅ Minimal native scatter measured; real cost 320–420ms@W8.
- ✅ Row-routing parallel keying wired e2e — COUNT ~2.4–2.9×, COUNT(DISTINCT) 3.84×.
- ✅ Skew bench — degrades to 0.7× at 90% hot (handled by serial cutover, §5/§7).
- ✅ Multi-col + NULL correctness verified.

*All prototype kernels were experimental and have been deleted; the production
kernel is built fresh under `opteryx/compiled/` (see §8 remaining work).*

### Both make-or-break risks RESOLVED (2026-06-19)
- ✅ **Parallel scatter recovers** — scales 5–6× (not bandwidth-bound); e2e COUNT
  3.31×, COUNT(DISTINCT) 4.78× (prototype, measured then deleted).
- ✅ **Skew decided** — disjoint-slice→concat is the invariant; salting is
  **rejected** (it reintroduces a merge). Skew is handled by **algorithm
  selection** — the >floor sample estimates a dominant hot key and routes that
  shape to serial. (Salting was measured at 2.17× on a prototype, now deleted; it
  is rejected evidence, not a build target.)

### Remaining work (no known blockers — this is a build/optimize phase)
1. **Kill the serial materialization tail** — build per-worker output buffers in the
   producer phase (or pipeline scatter↔keying) to recover COUNT→~4×, DISTINCT→~6×.
   The remaining headroom, an optimization not a risk.
2. **Production build** — wire row-routing + parallel scatter into the engine
   (parallel_engine / GroupedAggregateHashedNode), behind the cost-model gate.
   Surface the §4.2 ⚑ decisions (push/pull buffers, one pool vs two, two-phase vs
   pipelined) to the architect before building.
3. **Cost-model gate** (§7), calibrated from the benches — the engage/serial
   crossover and the skew cutover threshold.
4. **Sampled skew estimate from the >floor scan** feeding the serial cutover (the
   skew bench used an exact pre-pass; prod reads it off the sample, §7).
5. **String-key keying** stays on the serial-keying track (Option C in
   `M4_PARALLEL_PATH_FORWARD.md`) — the only lever where parallelism doesn't pay.
