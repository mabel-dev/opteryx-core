# Opteryx Parallel Execution Engine — Design (Chief-Architect Ratified)

> **SUPERSEDED (2026-06-25) for *how* parallelism is structured** — see
> [`GENERIC_PIPELINE_PARALLELISM_DESIGN.md`](GENERIC_PIPELINE_PARALLELISM_DESIGN.md).
> The five bespoke per-shape strategies this doc describes (`_grouped_agg_route`,
> `_join_probe_stream`, `_stateless_stream`, `_distinct_stream`, `_ungrouped_agg_stream`)
> were **retired into one executor** (the scheduler Event-DAG) and **deleted** — they no
> longer exist in the tree. This doc is retained for its still-valid *rationale and
> measurements*: the route-agg design, the tdoms join build-side work, and the empirical
> `scratch.hits` numbers. For the current execution architecture, read the generic design.

> Authored 2026-06-25. Reviewed adversarially (36-agent review: empirical gap-fill on the
> full 14 GB `scratch.hits` + 7 grounded dimensions, each finding independently refuted).
> This revision folds in every held-up finding and scopes every number to what was actually
> observed. It supersedes `M4_SEGMENT_SCHEDULER_SHUFFLE_DESIGN.md`.
>
> **Provenance discipline (non-negotiable):** a number in this document is one of three
> kinds, always labelled: **[M-now]** measured in the 2026-06-25 review run on full
> `scratch.hits`; **[M-prior]** measured in an earlier full-`scratch.hits` pass but not
> re-confirmed in the review run (must be re-gated by Stage A before it's load-bearing);
> **[proj]** a projection from a mechanism, not yet measured. Synthetic-prototype numbers
> are never targets.

---

## 0. Chief-architect verdict

**GO on the aggregate win; sequence the rest honestly; cut the padding.**

What the review *proved* (not projected):
- **Route-on-abandon is correct for every aggregate shape tested — byte-identical to the
  serial path.** [M-now] int key, wide temporal payload (`SUM`+`MIN`/`MAX` over `TIMESTAMP`),
  high-card string keys (`URL`, `SearchPhrase`), and multi-column keys all produced
  identical row counts + identical sorted-tuple checksums route-OFF vs route-ON, zero
  errors. The historically-corrupting case (grouped temporal `MIN`/`MAX`) is clean. This is
  the catastrophic-bug surface and it survived.
- **Route scales where decode is cheap** [M-now]: high-card `COUNT` 2.58×→**4.51×**,
  wide-payload 2.52×→**4.29×**, multi-col 1.55×→**5.12×** (best), all at DOP=8.
- **Route is decode-capped where decode is expensive** [M-now]: string keys 1.0×→1.78×
  (`URL`), 1.03×→1.90× (`SearchPhrase`). Route-OFF is ~1.0× there, so execution DOP is inert
  — the rugo decode pool already owns the cores. This is the §4.2 thread-budget problem, not
  a route defect.

What changed from the first draft after review:
1. **Targets are now scoped by decode cost and capped at the measured ceiling** (no uniform
   "5–6×"; high-card GROUP BY target is **~5–5.5×**, 6× a labelled stretch).
2. **W1.1 carves out holistic aggregates** (they cannot pre-aggregate).
3. **W1.2's real lever is the double-hash/double-key pass, not "column slices"** — the
   payload gather is irreducible.
4. **§4.2 (thread budget) is downgraded from "the signal exists" to "build a concrete
   sizing rule"** — it was the weakest workstream and is now the most explicitly scoped.
5. **The join workstream gains an explicit prerequisite stage**: the multi-input
   breaker-split scheduler DAG, which does **not** exist today beyond a single-task skeleton.
6. **Unanchored projections (scan→14 cores, join→5×) are demoted to measure-first.**

The aggregate win (Stage B) exists in the tree behind `M4_ROUTE_AGG`. Everything else is
sequenced behind it and gated on real-data sweeps.

---

## 1. Observations (the empirical bedrock)

Method: `cores-used = (ru_utime+ru_stime)/wall` via `getrusage`; `speedup =
wall(DOP=1)/wall(DOP=8)`; full `scratch.hits`, `PYENV_VERSION=3.14.5t PYTHON_GIL=0`,
warm-then-best-of-2.

### 1.1 Aggregates — route correctness and scaling [M-now]

| GROUP BY shape | groups | route-OFF DOP8 | route-ON DOP8 | correctness |
|---|---|---|---|---|
| int key, `COUNT(*)` | 17.6 M | 2.58× / 3.84 cores | **4.51× / 8.19 cores** | identical |
| int key, wide payload `COUNT+SUM+MIN/MAX(ts)` | 17.6 M | 2.52× / 4.45 | **4.29× / 9.61** | identical |
| string key `URL` | 18.3 M | 1.01× / 3.11 | **1.78× / 7.35** | identical |
| string key `SearchPhrase` | 6.0 M | 1.03× / 2.27 | **1.90× / 5.25** | identical |
| multi-col `(CounterID,RegionID)` | 0.63 M | 1.55× / 2.25 | **5.12× / 9.29** | identical |

Reading: route is **correct everywhere** and **scales for cheap-decode keys** (int,
multi-col). String keys are limited by the decode-bound scan (route-OFF ≈ 1.0× → DOP is
inert), not by route. Multi-col scaling is best because int decode is cheap, leaving cores
for execution DOP.

### 1.2 Scans already saturate the machine [M-prior — re-gate in Stage A]
- `COUNT(*) … WHERE "URL" LIKE …` → **9.7 cores at DOP=1**, flat across DOP.
- `SELECT 3 cols … WHERE "CounterID">0` (100 M rows out) → **5.8 cores at DOP=1**, flat.
- Cause: the rugo decode pool (`PARQUET_LOCAL_IO_WORKERS = 16`, `in_flight = 18`) decodes row
  groups on its own GIL-released C++ pool from one puller. These numbers were measured in the
  prior pass, **not** re-confirmed in the review run; Stage A re-gates them before they're
  load-bearing. The *mechanism* (decode-pool parallelism) is verified in code.
- Consequence: **do not add execution workers to scan/filter/project pipelines** — for pure
  scan/filter (no aggregation) it oversubscribes against the decode pool and cannot help.

### 1.3 DISTINCT already scales [M-prior — re-gate in Stage A]
The fused distinct path (`_distinct_stream`, `parallel_engine.py:790-838`) measured 4.15× /
8.7 cores in the prior pass; not re-confirmed in the review run. It is the Cxx-backed
streaming template the aggregate read-out should match.

### 1.4 The serial sandwich (why default agg is capped) — verified in code
Default GROUP BY (`_grouped_agg_stream`) is **serial-scatter → parallel-key → serial-read-out**:
- *Serial scatter-in*: one producer pushes every morsel through `_ScatterCollectEngine` on
  the main thread (`parallel_engine.py:946-958`).
- *Parallel keying*: W workers each key a disjoint hash-bin into a private `GroupHashEngine`
  (`:965-981`) — lock-free, results stay in C++.
- *Serial read-out*: `_finalize` iterates the W engines **one at a time on the main thread**,
  reconstructing every column and finalizing every aggregate per engine
  (`grouped_aggregate_hashed/_node.pxi:293-300` → `_engine.pxi:769,801,819-824,833`, which
  builds **PyObject** morsels via `from_vectors`).

Route-on-abandon parallelizes **both** serial ends. That is the entire mechanism.

### 1.5 The two-pool reality (corrected after review)
There are two pools — the rugo decode pool (≤16) and the execution `CppThreadPool` (≤DOP).
**Contention is bottleneck-dependent, not steady-state** (the decode pool's threads sleep on
a condition variable when idle; it is prefetch-bounded to `in_flight = decode_workers+2`):
- Cheap-decode pipelines (int/multi-col agg): decode drains fast and idles → execution DOP
  has free cores → route scales (4.3–5.1×). No coordination needed.
- Decode-bound pipelines (string scan): the decode pool saturates the cores → execution DOP
  is inert for the scan portion (route-OFF ≈ 1.0×). Route still nearly doubles the *aggregate*
  portion (1.0→1.8×) but cannot reach the cheap-decode band until decode and execution share
  one budget (§4.2) **or** the decode path itself goes faster/wider.

---

## 2. The model we translate (DuckDB morsel-driven), and Opteryx's one divergence

From `DUCKDB_PARALLELISM_REFERENCE.md` and the prototype (`scratch/ddb_proto/`):

- **Pipeline** = source → streaming operators → one sink. Cut the plan at *breakers* (sinks
  that fully consume input: aggregate, join-build, sort, distinct). A breaker ends one
  pipeline and becomes the source of the next.
- **Parallelism is a property of the pipeline.** N identical tasks race the same operator
  chain on disjoint morsels. No operator spawns threads.
- **Partitioning folds into the sink — no exchange operator.** `Sink` (thread-local,
  radix-partitioned) → `Combine` (partition-aligned pointer hand-off, O(partitions), short
  lock) → `Finalize` (event-gated).
- **★ The read-out is itself parallel — no single-threaded consumer.** A group's hash lands
  in exactly one partition, so partitions are read out by N tasks claiming partitions via an
  atomic counter. *This is the one thing the default Opteryx path lacks and route supplies.*
- **Ordering between pipelines is an event dependency**, not a lock (two-counter events).

**Opteryx's hard divergence — the Prime Constraint (locked):** we are ~2× DuckDB per core
[M-prior, inherited — not re-measured]; the gap is the serial ceiling, not single-thread
efficiency. **DOP=1 must be byte-identical to today's serial engine** (1 partition, no
Combine, no merge). DuckDB pays radix-partitioning even single-threaded; we must not. A
single-thread regression fails the design.

**v1 cuts (deliberate):** no cooperative `BLOCKED`/`InterruptState` async machine (tasks run
to completion); no spill; keyless joins serial.

---

## 3. Why the prototype mispredicted — and the rule it gives us

The prototype hit 4.8–6.2× because it was pure C++ threads over a synthetic in-memory source
(`CScanSource`), with no parquet decode, no planning, and — critically — **no Python
consumer** (results accumulated thread-local in C++, merged by an atomic-counter parallel
read-out). It proved the *shape* scales; it abstracted away the decode pool (so it couldn't
show scans are already parallel), per-query overhead, and the Python consumer funnel.

**The rule it gives us:** *keep the morsel in C++ and the result thread-local; let the
read-out be N tasks over disjoint partitions; let Python touch only orchestration and the
final materialization.* Route already obeys this for aggregates. The remaining work is to
obey it where the main thread still funnels (the read-out finalize, the cursor, the join
probe), and to do it without ever quoting a synthetic number as a target again.

---

## 4. The design

Four workstreams + one prerequisite stage, ordered by measured payoff-per-risk. Each gated
on a **real-data** `scratch.hits` DOP-sweep (cores-used + speedup + result-identity),
DOP=1 within noise of serial.

### 4.1 Aggregate → route-on-abandon as the default (the proven win)

**Status: built (`_grouped_agg_route`), [M-now] correct + 4.3–5.1× for cheap-decode aggs,
gated off. Promote it.**

- **W1.1 — Bounded-adaptive sink, gated to MERGEABLE aggregates (correctness blocker
  resolved).** Today's `_grouped_agg_route` is *always-route*. Add the `Abandon()` switch
  (validated in `demo_agg_adaptive.py`): a task pre-aggregates into a thread-local table
  until overflow, then flushes **by `part(key)` into the same partition** the raw rows route
  to (so co-location is preserved) and switches to route-raw. **Carve-out (mandatory):** when
  *any* aggregate in the breaker is **holistic** — `MEDIAN`, `COUNT(DISTINCT)`,
  `APPROX_*`, `ARRAY_AGG`/list — the sink **must stay pure-route (no local pre-agg table)**,
  because holistic aggregates have no mergeable partial state. The grouped finder has no
  `is_mergeable` gate today (`parallel_engine.py:218-225`); add the holistic check here.
  *Gate:* DOP=1 byte-identical to serial finalize; low-card DOP=1 within noise of today.
- **W1.2 — Collapse the double key-pass (the real overhead, reordered).** Route's +~38% CPU
  at DOP=8 (cpu 2.3→3.2 s on high-card) is **redundant work**, decomposed into four named,
  separately-measurable costs:
  1. **Double keying hash (primary lever).** `cxx_scatter` computes the key hash to route
     (`draken_native.cpp:5641`) and then **discards it**; the read-out's `engine.ingest`
     recomputes it (`cxx_hash_c`). Forward the scatter's hash vector to the read-out, OR sink
     directly into the read-out's hash-table layout so read-out is a **fold, not a
     re-ingest** — eliminating the second hash+key pass. This is the bulk of the +38%.
  2. **Payload gather (irreducible).** Routing a payload column to its partition is a real
     materializing gather — `cxx_take`/`cxx_take_c` copies (`draken_native.cpp:5386,5204`),
     **not** a view. The first draft's "payload travels as column slices, no materialization"
     was wrong; delete it. This cost cannot be removed, only not-doubled.
  3. **PyObject morsel churn.** The scatter wraps each of W bins in a `Morsel.from_cxx`
     PyObject (`:567`); keep the bins as `shared_ptr[CxxMorsel]`, materialize once.
  4. **GIL-held scatter.** The scatter currently crosses the GIL; the `cxx_scatter` C-ABI is
     nogil-capable — drive it GIL-off.
  *Gate:* W1.2 ships only if the real-data sweep shows the predicted redundant-CPU reduction.
- **W1.3 — Replace the dispatch.** Once W1.1/W1.2 hold the Prime Constraint, make route the
  grouped-agg path; retire `M4_ROUTE_AGG` and `_grouped_agg_stream`'s serial scatter/finalize.
  DISTINCT keeps its fused path (do **not** route it).

**Target [proj, gated by sweep]:** high-card GROUP BY 4.5 → **~5–5.5×** (recovering the
double-hash + PyObject churn + GIL-scatter; **not** the irreducible payload gather). 6× only
if W3.1 keeps the read-out Cxx-backed *and* §4.2 lifts the parallel region above ~8 cores —
labelled a stretch, not the target. Multi-col already at 5.1× → ~5.5×. **String GROUP BY
stays ~1.8–1.9× until §4.2** (decode-bound; out of Stage B's reach by construction).

### 4.2 Size the core budget (the weakest workstream — now concrete, not "the signal exists")

**Correction (held-up review finding): the live sizing signal does NOT exist today.**
`is_concurrent_pull_safe()` is a boolean correctness gate (`_scan_mode==_SCAN_SINGLE`); the
decode pool's occupancy accessors exist (`pending_work_count`, `queue_high_watermark`,
`io_pipeline.hpp:1236,1252`) but only as a **post-run telemetry snapshot**, not a
live-at-sizing meter. So §4.2 is **build work**, scoped two ways — pick one by measurement,
do not hand-wave:

- **Option A (preferred, concrete, no new live meter): static budget split from plan-time
  facts.** `execution_DOP = max(1, B − expected_decode_threads)`, where `B` = scheduler
  threads and `expected_decode_threads` is derived from the scan's **post-pruning work-item
  count** `src.n_items` (`pool_reader.pyx:1207-1208`, the plan-time proxy that *replaces*
  `PARALLEL_MIN_ROWS` and the vague "source morsels available"). A decode-bound scan (many
  large row groups, expensive columns) reserves most of `B` for decode; a cheap-decode agg
  reserves little, so execution DOP fills the budget. Deterministic, plan-time, no runtime
  feedback loop.
- **Option B (only if A under-delivers): build a live decode-occupancy counter** — a runtime
  atomic the decode pool publishes and `resolve_worker_count` reads to throttle execution DOP
  when decode is saturating. This is explicit new work in Stage D, not an existing capability.

**Why it matters:** this is what makes string GROUP BY (decode-bound) stop oversubscribing
and is the only path to lifting it past ~1.9×. It is the highest-uncertainty workstream;
Stage D is explicitly gated on a measured mixed-workload win or it is cut.

### 4.3 Shave the residual main-thread tail (re-scoped — these are not one-liners)

- **W3.1 — Make the route read-out Cxx-backed (the real, small win).** *Correction:*
  per-partition **aggregation (ingest) is already parallel**, but per-partition **finalize +
  materialize is still a serial main-thread loop** (`_node.pxi:297-298` →
  `_engine.pxi:833` builds PyObject morsels via `from_vectors`). W3.1 flips that to
  `from_cxx_vectors` so the read-out emits Cxx-backed morsels. The whole downstream tail
  (Exit select/rename → cursor slice/combine) is already representation-agnostic, so this one
  change collapses the tail. Small and correct; do it first.
- **W3.2 — Cheap split once Cxx-backed.** With W3.1 done, `_split_morsel`'s per-column
  nanobind `slice`/`combine` (`query_session.py:541-585`) become C++ views + one final concat
  (`_morsel_shim.pyx:1026-1028`). Falls out of W3.1.
- **W3.3 — GIL-off source pull = a multi-step native-scan rewrite, NOT "convert pull to
  nogil".** Today `IpcRowGroupSource.next` returns a Python tuple (`parquet_read.pyx:574`,
  assembled at `:1318`). The real work: (1) give the source a typed `cdef`/C++ `next`
  returning C++ vectors; (2) rewrite `_single_pass_next` to assemble a `CxxMorsel` with zero
  Python objects; (3) *only then* can `pull_one`/`next_morsel` be nogil. Re-scoped as its own
  multi-step item; this is the lever for decode-bound scans and is non-trivial.
- **W3.4 — Nogil projection = two distinct GIL barriers.** Converting projection's
  `_dispatch_push` to a nogil body removes only the **operator-wrapper** GIL. The
  **expression body** is GIL-off only for `is_all_c_native` bytecode (`evaluation.pyx:1928`);
  mixed/string-result/cast-to-decimal-or-date/`IN`/`LIKE`/function/legacy expressions still
  take the GIL (`:2348`). Full GIL-off projection requires a nogil `execute_and_append`
  orchestration *and* nogil kernels for the remaining expression classes. State both barriers;
  do not present W3.4 as a single conversion.

### 4.4 Parallel equi-join (largest unbuilt opportunity — with its true prerequisite)

The join engine already has the right shape — serial-accumulate-left → one sealed immutable
Carchar table at left-EOS → streaming read-only per-right-morsel probe. So "stream-the-probe"
is structurally sound. **But the substrate it needs does not exist** and is the real first
deliverable (see Stage **D-DAG** below). Scope:

- **Build side:** *match the real code* — the left side accumulates serially into one sealed
  immutable table. v1: **parallelize the probe only** (the build is the small side); keep the
  single sealed-table build. A thread-local parallel build is a *later* option, not v1.
- **Probe side:** the probe operator becomes streaming over a read-only sealed table; probe
  parallelism inherited from the probe scan's morsels. v1 must (i) drop the probe-phase node
  lock once the table is sealed-immutable and (ii) make probe emission Cxx-backed.
- **Outer joins:** per-build-row matched flag via `atomic_ref`, then a parallel range-claimed
  pass for unmatched.
- **Keyless (CROSS/non-equi): serial fallback** (locked cut).

**Target: UNMEASURED — Stage-E real-data sweep TBD.** The prototype's 6.19× is synthetic
(§3) and is used **only** to select the stream-the-probe shape, never as the target. The §5
join row carries no empirical anchor and must not be read as a peer of the measured agg rows.

---

## 5. Performance model & targets (scoped, labelled, gated)

| shape | today | after design | how | provenance |
|---|---|---|---|---|
| GROUP BY high-card (int) | 2.58× [M-now] | **~5–5.5×** (6× stretch) | W1.1–1.3 route default + W1.2 double-hash cut | [proj] gated |
| GROUP BY wide-payload (int) | 2.52× [M-now] | **~5×** | same; payload gather irreducible | [proj] gated |
| GROUP BY multi-col | 1.55× [M-now] | **~5.5×** (already 5.1×) | route default | [proj] gated |
| GROUP BY string key | 1.0× [M-now] | **~1.9× until §4.2**, then TBD | route lifts agg end; scan decode-capped | [M-now] + gated |
| DISTINCT | 4.15× [M-prior] | hold ~4× | already Cxx-backed fused path | re-gate Stage A |
| filter/scan | 9.7 cores [M-prior] | **measure-first** | W3.3 removes pull-GIL serialization; no core target until the sweep shows pull-GIL is binding | re-gate + [proj] |
| heavy projection | 5.8 cores [M-prior] | **measure-first** | W3.4 removes wrapper-GIL only (not the expression body); bounded | re-gate + [proj] |
| equi-join | 1× (serial) | **UNMEASURED — Stage-E sweep TBD** | W4 stream-the-probe (proto 6.19× synthetic, not a target) | unanchored [proj] |

**No uniform "5–6×."** Cheap-decode aggregates reach ~5–5.5× via Stage B. Decode-bound string
aggregates stay ~1.9× until Stage D. Scan/projection/join targets are measure-first — a number
goes in this table only after its sweep produces it.

---

## 6. Delivery plan (staged, each gated on real-data sweep, DOP=1 byte-identical)

1. **Stage A — Real-data cores-used sweep as a first-class gate.** Promote the `getrusage`
   sweep into `make m4-sweep` on `scratch.hits` (not `clickbench_tiny` — it is overhead-bound
   and *lies*; this is the failure that parked a 4.6× win as "neutral"). Report cores +
   speedup + result-identity per shape. **Extend the oracle**: add genuine NULL-bearing group
   keys, `MEDIAN`, `COUNT(DISTINCT)`, and `AVG` GROUP BY cases (the holistic + null cases the
   review flagged as unverified). *No stage certifies without it; it re-gates every [M-prior]
   number.*
2. **Stage B — Aggregate route as default (W1.1 → W1.2 → W1.3).** Bounded-adaptive with the
   holistic carve-out first (Prime Constraint), then the double-hash collapse, then flip the
   dispatch + delete the serial path. *First shipped win: cheap-decode GROUP BY 2.5 → ~5×.*
3. **Stage C — Residual tail (W3.1 → W3.2), then the GIL-off rewrites (W3.3, W3.4).** W3.1
   (Cxx-backed read-out) before W3.2; then the native-scan nogil rewrite (W3.3) and the
   projection nogil work (W3.4), each measured before its core-count target is published.
4. **Stage D-DAG — Build the multi-input pipeline DAG (NEW, prerequisite for joins).** Turn
   `scheduler_engine.py`'s single-task no-op skeleton into a real breaker-split DAG with
   cross-pipeline event-dependency wiring (build-pipeline → Finalize event → probe-pipeline).
   This does not exist today and is the true first cost of parallel joins.
5. **Stage D-Budget — Size the core budget (§4.2, Option A).** Static plan-time split from
   `src.n_items`; retire `PARALLEL_MIN_ROWS`. Validate on mixed scan+agg queries; only build
   Option B's live meter if A under-delivers. Gated on a measured mixed-workload win or cut.
6. **Stage E — Parallel equi-join (W4).** On top of Stage D-DAG: sealed-table build →
   streaming Cxx-backed probe. Real-data join sweep (int + string keys) sets the target.

**Critical path:** **A → B** is the standalone aggregate win (recover what already exists). **C**
is independent and broadly useful. **E depends on D-DAG and on C's GIL-off pull + Cxx output**
— it is *not* "parallelizable after A" except for throwaway prototyping. D-Budget is
independent and gates the string-aggregate ceiling.

---

## 7. Prime constraint, verification, and the discipline that was missing

- **DOP=1 byte-identical (the gate that fails the design if broken).** W1.1's bounded-adaptive
  is what collapses route to today's serial finalize at one partition. Tested first.
- **Verification claim, stated honestly (corrected).** Route read-out is **verified
  row-identical against the serial Opteryx path** (route-OFF vs route-ON, matching row counts
  + sorted-tuple checksums) on full `scratch.hits` for `COUNT`/`SUM`/`MIN`/`MAX` over int,
  temporal, string, and multi-col keys [M-now]. It is **not yet** verified for `MEDIAN`,
  `COUNT(DISTINCT)`, `AVG`, or genuine NULL-bearing keys, nor against an external DuckDB
  oracle — Stage A adds those cases, and the default flip (W1.3) is gated behind them.
- **Correctness over cores.** The grouped-agg engine is the catastrophic-bug operator; the
  temporal `MIN`/`MAX` payload (the historical corruption) is [M-now] clean under route, but
  the holistic carve-out (W1.1) and the extended oracle (Stage A) are prerequisites to the flip.
- **The process failure this design institutionalizes against:** the previous pass benchmarked
  the parallel aggregate on a 40 ms / 16-morsel dataset, saw noise, and parked a 4.6× win as
  "exhausted." **Synthetic prototypes may propose; only full-`scratch.hits` cores-used sweeps
  may certify.** Stage A is that rule made executable.
- **Entropy discipline.** Stage B's final step **deletes** `_grouped_agg_stream`'s serial
  scatter/finalize and the `M4_ROUTE_AGG` flag — the design *reduces* operator-path surface,
  it does not accrete a flagged second path. Any workstream whose real-data gate fails reverts;
  the tree never carries an unproven parallel path behind a flag again.

---

## Appendix — key code anchors (audited against the tree)

- Default agg serial sandwich: `parallel_engine.py:946-958` (scatter-in), `:965-981` (parallel
  key), `:1003-1009` + `grouped_aggregate_hashed/_node.pxi:293-300` +
  `_engine.pxi:769,801,819-824,833` (serial PyObject read-out).
- Route-on-abandon (the win): `parallel_engine.py:_grouped_agg_route` (~`:1017-1196`), dispatch
  `:390`, gated `M4_ROUTE_AGG`; double-hash at `_ScatterCollectEngine.scatter` (`:564`) →
  `cxx_scatter` (`draken_native.cpp:5634-5655`, hash computed+discarded `:5641`) then re-hashed
  in read-out `engine.ingest`; payload gather `cxx_take` (`:5386,5204`, real copy).
- §4.2 facts: occupancy accessors `io_pipeline.hpp:1236,1252` (post-run only); plan-time
  work-item count `src.n_items` `pool_reader.pyx:1207-1208`; `is_concurrent_pull_safe`
  (boolean gate) `_operators.pyx:546-559`.
- Nogil substrate: carrier `draken/morsels/cxx_morsel.h:47-63`; nogil push/emit
  `_operators.pyx:380-480`; nogil filter `filter/filter.pyx:308-357`; nogil agg ingest
  `_engine.pxi:361-405`; GIL-held pull `_operators.pyx:908-922`; projection GIL body
  `projection/projection.pyx:106-118`; VM nogil only for c-native `evaluation.pyx:1928`, GIL
  fallback `:2348`.
- Tail: cursor consume + split `query_session.py:541-585`; materialize `_morsel_shim.pyx:189-202`.
- Join + scheduler: join engine (serial build → sealed table → streaming probe);
  `scheduler_engine.py` single-task no-op skeleton (DOP pinned to 1) — the Stage D-DAG target.
- Model + prototype (shape only, never a target): `DUCKDB_PARALLELISM_REFERENCE.md`;
  `scratch/ddb_proto/_cops.cpp` (`CAggRoute`, `CAggAdaptive`, `CStreamJoin`).
