# Remote scan: decoupling FETCH from DECODE

**Status:** P0 LANDED 2026-09-12 (see §0); P1–P4 remain proposals awaiting the
rulings in §5. The env-var POC (`RUGO_FETCH_AHEAD`) is gone — the instrument is
now the session variable `parquet_io_fetch_ahead` (config `PARQUET_IO_FETCH_AHEAD`).
**Default 64** (architect, 2026-09-13, from the production sweep: 48-64 optimal, 128
and 256 slower than off; the initial 128 default of 2026-09-12 was a regression);
0 = off = the coupled path byte-for-byte.

---

## 0. Review outcome and what landed (2026-09-12)

**Landed (P0 — the production measurement instrument):**

- `ParquetIOPipeline::set_fetch_ahead(depth)` replaces the `getenv` channel: an
  explicit plan-time setter, no hidden configuration inside a C++ constructor.
- `parquet_io_fetch_ahead` (USER/RESTRICTED, like `parquet_io_in_flight_limit`),
  default 64, reaches BOTH scan paths — the trampoline (`parquet_read.pyx`) and the native
  compiler path (`compiler.py` → `open_native_scan_plan`). Every pipeline
  reports the depth it actually runs as `fetch_ahead_depth` in
  `io_scan_diagnostics`, so a production run can prove the knob bound before
  a number is believed (the [KNOB] rule).
- The two silently-inert combinations of §4.6 are **rejected at plan time**
  (`ValueError`): depth ≤ decode workers, and an explicit window smaller than
  the depth. With the window on auto it widens to `max(workers, depth) + 2`.
- Fetch-stage failure is **carried as an exception and rethrown by decode**
  through the one existing error path. The POC's "publish undecoded and let
  decode re-fetch" was a hidden second retry round on top of HttpClient's own
  budget — a fallback, removed.
- Local paths never enter the fetch stage, and a local-only scan does not arm
  the pool at all (no idle threads for nothing to do).
- `prefetch_discarded_bytes` counts bytes bought by fetch-ahead that a cancel
  then threw away (§4.4's waste counter), reported beside `cancelled_skips`.
- Tests: `tests/unit/connectors/parquet_io/test_fetch_ahead.py`. Rig A/B after
  the change (strings, workers=4, window=48, 5 interleaved rounds): coupled
  4.10s, depth 32 0.76s, bytes and rows identical — the pre-edit numbers were
  4.09s / 0.74s, so the default path did not move and the win is intact.

**Challenged — findings that change the plan below:**

1. **§4.6's "hand-off cost" attribution is wrong.** `wait_and_get_result`'s
   help-loop lets the CONSUMER thread claim and decode (hence fetch) a pending
   item, so the coupled path at workers=4 runs up to 5 concurrent fetches. Fetch-
   ahead=4 gives exactly 4. 4.46s × 5/4 ≈ 5.6s ≈ the 5.42s measured. The rule
   "depth must exceed workers" stands, but the mechanism is a lost fetcher, not
   a hand-off — do not go looking for hand-off overhead to shave.
2. **§2.1 (M2, persistent connections) cannot be measured on either rig.** The
   dev rig's throttle applies RTT to the first response byte only; a fresh TCP
   connect to localhost costs ~0, so handshake removal shows nothing there. In
   production the fetch is bandwidth-dominated: ~631 ms per 2 MB row group vs a
   same-region TCP+TLS handshake of a few ms (~1%), below the ±13% in-session
   noise floor. M2 is correct hygiene but cannot be a "phase with its own
   number" — and it must be a **persistent per-thread `CURLM`**, not
   `CURLOPT_SHARE`: libcurl documents connection sharing across threads via
   CURLSH as unsupported (the deadlock the code comment at `get_many()` warns
   about). Recommendation: drop R2's "measure M2 first"; do it as hygiene if at
   all, unbundled. Note the header comments in `http_client.cpp` (lines ~10 and
   ~508) still claim `CURLOPT_SHARE` IS set on `get_many()` handles; the code
   deliberately does not. Stale docs, not fixed here.
3. **§3's thread-cost argument for M3 is overstated.** Fetch threads block in
   `curl_multi_wait`; blocked threads do not oversubscribe a 2-vCPU cgroup. The
   process-wide pool of §4.1 is still right (per-pipeline × latmat × joins
   multiplies threads), but an event loop to save a few dozen *sleeping* threads
   is complexity without a measured cost behind it. Recommendation: M1 with a
   process-wide pool ships if P0 pays; M3 stays unbuilt until a measurement
   says threads themselves cost something.
4. **The design's "session-settable, no redeploy" premise was broken on the
   production path.** `open_native_scan_plan` — what the compiler uses — took
   NO `in_flight_limit_override`, `http_tuning`, or `coalesce_tuning`; only
   the trampoline path resolved those variables. So `SET
   parquet_io_in_flight_limit`, `http_max_connections_per_host`, `http_pipewait`,
   `disable_http2` and both coalesce knobs are **inert on native scans** today.
   Any production sweep of them since the native path became the default
   measured nothing. Fetch-ahead is plumbed to both paths; the others are NOT
   touched here and need a ruling (they are the same one-line plumbing each).
5. **A process-wide fetch pool would latch its size at first use** — the
   [FROZENcfg] trap. A per-query sweep needs a pool that resizes (or is
   per-query). That is why P0 is per-pipeline; §4.1 must say how the shared
   facility takes a new size without a restart before P4.
6. **Unexplained:** the 2026-07-24 production traces show peak concurrent
   downloads of `workers + 6` in every cell. "Concurrent fetches == pool size"
   predicts at most `workers + 1` (the help-loop). Either the trace counts
   something else (footer batch, overlapping latmat pipelines) or another
   fetcher exists. Reconcile before quoting §1 as a production fact.
7. **Found in passing, not fixed:** `open_native_scan_plan`'s H5 local-footer
   pre-pass poisons the footer map on a single cold local file (empty entry
   from `try_get(path, &footer_map[path])`, batch fill skipped by the `> 1`
   guard) → zero row groups, silently. Direct callers only; the compiler's
   `native_scan_supported` gate warms the cache first. Strict xfail in
   `test_fetch_ahead.py`; `test_wp02_predicate_relocation::test_pruning_matches_direct_source_plan`
   fails the same way.

**Still needs the architect:** R3 (shared facility, FIFO), R4 (byte budget
source), R5 (depth under a pushed LIMIT), R6 (where the production A/B runs),
plus the two new questions above: plumb the other inert SET knobs to the native
path, and the H5 footer fix.

**Production target:** GCP — Cloud Run instances reading GCS. Everything below is
argued for that environment; the dev-rig numbers are labelled as such.

---

## 1. The defect (measured, not inferred)

`ParquetIOPipeline::decode_row_group` issues the range GET **and** decodes on the
same thread. `enqueue_pending` dispatches one ticket per row group onto
`decode_pool_`. Therefore:

> **concurrent fetches == decode pool size, by construction.**

A ticket beyond the pool size merely queues. This is structural, not a tuning
accident, and it explains the long-open attribution question in
`in_flight_window_vs_thread_count`: the submission window could never bind,
because the window does not create fetch concurrency — threads do.

Dev rig (`dev/throttle_server.py`, rtt=50ms, 100 Mbps/conn, 240 row groups,
79 MB; A/A noise floor B/A=0.9996, spread 6–8%):

| workers=4, window | 6 (auto) | 8 | 16 | 32 | 64 |
|---|---|---|---|---|---|
| wall | 4.70s | 4.54 | 4.45 | 4.49 | 4.45s |

Window 6→64 moves nothing. Threads 4→32 moves 4.70s→1.31s. Two knobs, one of
which was inert.

**Also measured:** decode is ~2% of wall on this shape (0.088s unthrottled vs
4.7s throttled). So DuckDB 2.0's stated mechanism — overlap CPU with network —
is worth ~2% to us. **Our win is concurrency, not overlap.** That distinction
drives the whole design: we are buying *requests in flight*, not *CPU/IO
overlap*, and the cheapest way to buy requests in flight is not more threads.

---

## 2. What is different on GCP (and why the rig number will not transfer intact)

Three production facts, each already recorded, each load-bearing here:

1. **A bandwidth ceiling exists.** `parquet_gcs_io_workers_production_sweep`:
   aggregate throughput never exceeded ~64 MB/s at ANY worker count (16 optimal,
   128 worst). `cloudrun_network_bandwidth_cap_hypothesis` puts Cloud Run's
   documented 600 Mbps/instance close enough to be the candidate cause,
   unconfirmed. The rig is latency-bound (16.8 MB/s achieved of ~50 available);
   production may be bandwidth-bound. **Fetch-ahead pays much less against a
   flat cap.**
2. **More connections can HURT.** `http2_multiplex_pipewait_missing` measured, in
   production, that capping connections to 1 BEAT 16 on wide projections
   (20-col: 25.69s vs 31.12s) — and that the effect VANISHED 90 minutes later
   when the link was slower. Depth interacts with
   `http_max_connections_per_host`, and the interaction is regime-dependent.
3. **CPU is cgroup-limited and we are blind to it.** `thread_pool_sizing_is_cgroup_blind`:
   every pool sizes from `os.cpu_count()`, which on Cloud Run reports the HOST's
   cores against a 1–4 vCPU allocation. **A new thread pool that inherits that
   sizing basis would be a new instance of a known bug.**

### 2.1 A finding from this work that may matter more than the decoupling

`src/cpp/http_client.cpp` `get_many()` builds a fresh `CURLM` per batch and sets
**no `CURLOPT_SHARE`** on its easy handles (`get()` does set it). Every row-group
fetch therefore pays a fresh **TCP + TLS handshake** to GCS — 396 row groups is
396 handshakes, none reused.

At GCS RTT that is plausibly a large fraction of per-fetch latency, and it is a
cost fetch-ahead only *hides* (by overlapping handshakes) rather than removes.
**Removing it may be worth more than decoupling, and is a smaller change.**

⚠️ UNMEASURED. Stated as a hypothesis with a clear mechanism, not a result. It is
proposed below as its own phase with its own A/B, explicitly NOT bundled into the
decoupling — bundling them would make both unattributable, which is the mistake
`in_flight_window_vs_thread_count` records.

---

## 3. Three mechanisms, deliberately separated

| # | Mechanism | Buys | Cost | Status |
|---|---|---|---|---|
| M1 | Fetch on a dedicated pool | requests in flight ⟂ thread count | threads | POC, 5.4x on rig |
| M2 | Persistent connections across fetches | removes per-RG handshake | none obvious | §2.1, unmeasured |
| M3 | Event-loop fetch (curl_multi, persistent) | M1 + M2 **without threads** | complexity | proposed target |

M1 and M2 are independent and must be measured independently. M3 subsumes both.

**M3 is the destination.** `get_many()` already drives `curl_multi`; the change is
to stop creating one per batch per thread and instead keep a small number of
long-lived multi handles driven by dedicated event-loop threads, with row-group
fetches submitted as easy handles and completions posted back. Then N concurrent
requests cost N easy handles, **not N threads** — which is the right shape for a
1–4 vCPU Cloud Run instance and sidesteps §2's point 3 entirely.

M1's thread cost is the reason it should not be the production default on Cloud
Run. Its value is that it exists, is correct, and can produce the production
measurement that tells us whether any of this pays.

---

## 4. Proposed design

### 4.1 Pool topology — process-wide, NOT per-pipeline

A `ParquetIOPipeline` is constructed **per scan plan** (`pool_reader.pyx:396`,
`:2424`), and the latmat path builds **two** per scan (`p1_pipeline`,
`p2_pipeline` — `_operators.pyx:379`). A 5-way join under latmat is ~10
pipelines. A per-pipeline fetch pool of depth 32 is then 320 threads on a 2-vCPU
instance.

⛔ Per-pipeline fetch pools are rejected on that ground alone.

**Design:** one process-wide fetch facility (pool in M1, event loop in M3),
created lazily on first remote scan. Per-pipeline configuration becomes a
**submission bound** (how many of this scan's row groups may be outstanding),
never a thread count. Precedent exists: the exec `CppThreadPool` is already
shared into the pipeline by handle.

⚠️ Consequence to accept deliberately: concurrent queries now contend for one
fetch facility. That is the correct trade (one bounded IO budget per instance
beats N uncoordinated ones), but it makes the facility a shared resource and a
noisy-neighbour surface between queries on the same instance. Fairness between
queries is **explicitly out of scope for v1** — FIFO submission — and should be
revisited if multi-tenant latency regressions appear.

### 4.2 Sizing basis — bandwidth-delay, NOT cores

⛔ The fetch facility must NOT size from `os.cpu_count()`. Fetch concurrency is a
bandwidth-delay-product question; core count is not an input to it. This is the
one pool in the tree that has a principled non-CPU sizing basis, and it should
use it rather than inherit the cgroup-blind idiom.

Default derives from measurement, not a formula, and ships as an absolute number
with the production sweep recorded next to it (the pattern
`parquet_gcs_io_workers` already follows in `config.py`).

### 4.3 Back-pressure — a BYTE budget, not a row-group count

Today the window counts row groups (`in_flight_limit`, default `decode_workers+2`)
and the memory pool is sized `est_rg * (in_flight_limit + 1)`. Fetch-ahead adds a
**new** allocation on top: each in-flight item holds its *compressed* row-group
bytes from fetch until decode.

Row-group counts are the wrong unit — row groups vary by orders of magnitude
across our datasets, so a count-based bound is a memory bound that does not
bound memory. `platform.pyx` already exposes `get_cgroup_memory_limit_bytes()`
(memory IS cgroup-readable even though CPU is not), so the budget can be derived
honestly on Cloud Run.

**Design:** fetch-ahead is bounded by outstanding *prefetched bytes*, and a fetch
is not issued when issuing it would exceed the budget. Depth becomes a
consequence of the budget and row-group size, not an independent knob.

### 4.4 Cancellation and wasted egress — a GCP cost, not just waste

Reading ahead means fetching bytes a query may never consume: a pushed LIMIT that
satisfies early, a dropped cursor, a `dict_all_filtered` skip. Today `cancel()`
bails before any IO. With fetch-ahead, up to the full depth is already in flight
or already bought — **billed GCS egress and Class B operations for rows nobody
reads**, on a platform where egress is already a governed concern.

**Design:** the fetch stage checks cancellation before issuing (the POC does),
and scans carrying a pushed LIMIT take a reduced depth. A new counter reports
prefetched-then-discarded bytes so the waste is visible rather than inferred —
if that number is large in production, the depth policy is wrong and we will
know from telemetry instead of from a bill.

⚠️ Interacts with `scan_pushed_limit_is_a_correctness_obligation`: reading ahead
must never change WHICH rows a limited scan returns, only how many bytes were
speculatively fetched. The limit remains enforced downstream, unchanged.

### 4.5 Telemetry — the fetch stage must not make the scan lie

The POC's first cut reported `read_ns == 0`, because the fetch left the decode
thread and nothing folded its time back. Fixed by carrying `prefetch_ns` on the
`WorkItem`. Any stage split must preserve attribution or the scan under-reports
its own IO.

New counters: fetch queue depth high-water, prefetched bytes high-water,
prefetched-then-discarded bytes, and fetch-stage wait separated from decode-stage
wait (per `execution_tracing_design`'s existing span vocabulary).

### 4.6 Configuration surface

`RUGO_FETCH_AHEAD` did not ship. The surface is `PARQUET_IO_FETCH_AHEAD` /
`parquet_io_fetch_ahead` in `config.py` + `variables.py` alongside
`parquet_io_in_flight_limit`, session-settable so production sweeps need no
redeploy (the method in `parquet_gcs_io_workers_production_sweep`) — and, unlike
its siblings, plumbed to the native compiler path (see §0 finding 4).

⛔ Two knobs that must not be settable into a silently inert combination: depth
below the submission window is a pure loss (rig: fetch_ahead=4 with workers=4
measured 5.42s, SLOWER than coupled 4.46s — same concurrency plus a hand-off).
Either derive one from the other or reject the combination loudly. Do not ship a
knob whose wrong setting is silently slower — that is how the last one cost a
month of attribution.

---

## 5. Decisions requiring the architect

**R1. Target mechanism.** M1 (thread pool) as the shipped design, or M1 as
measurement instrument only with M3 (event loop) as the shipped design?
*Recommendation: M3 ships, M1 measures.* M1's thread cost is real on 1–4 vCPU and
M3 subsumes it — but M3 is materially more work.

**R2. Does M2 (persistent connections) go first?** It is smaller, independent, and
may be worth more. *Recommendation: yes — measure M2 alone before building M3,
because if handshakes dominate, M3's design targets change.*

**R3. Shared fetch facility across queries** — accepted, with FIFO and no
fairness in v1? (§4.1)

**R4. Byte budget source.** Derive from `get_cgroup_memory_limit_bytes()`, or an
explicit configured budget? (§4.3)

**R5. Depth policy under a pushed LIMIT** — reduced depth, or opt out of
fetch-ahead entirely? (§4.4)

**R6. Where does the production A/B run?** The sweep method needs a real Cloud
Run instance; this is the one step that cannot be done on the workstation
(`bulk_data_jobs_run_on_xb500_for_egress`, `feedback_no_live_data_from_the_x86_box`).

---

## 6. Build order

Each phase lands with its own A/B and its own recorded number. No phase is
"obviously good" enough to skip measurement.

- **P0 — production measurement of the POC.** ✅ LANDED 2026-09-12 as
  `parquet_io_fetch_ahead`, default off (§0). The production run itself is
  still to do: it answers the only question that matters — does decoupling pay
  against a bandwidth cap? *If it shows no win on Cloud Run, P2/P3 do not
  proceed as written.* Check `fetch_ahead_depth` in `io_scan_diagnostics`
  before believing any number.
- **P1 — M2, connection reuse in `get_many()`.** Independent A/B. May reprice
  everything below it.
- **P2 — byte-budget back-pressure + cancellation accounting** (§4.3, §4.4).
  Required before any default-on, independent of mechanism.
- **P3 — M3 event-loop fetch**, if P0/P1 justify it.
- **P4 — default-on**, with the sweep table recorded in `config.py` next to the
  default, per existing convention.

---

## 7. What would make this not worth doing

Stated up front so it is checkable rather than rationalised later:

- P0 shows ≤10% on Cloud Run because the ~64 MB/s cap binds first. Then the win
  was always latency-regime-only, and the honest outcome is a documented
  negative result plus whatever P1 buys.
- Depth raises concurrent connections into the regime where
  `http2_multiplex_pipewait_missing` measured connections HURTING. Watch the wide
  projections specifically; they are where that effect appeared.
- Prefetched-then-discarded bytes turn out large on LIMIT-heavy workloads,
  making this a cost increase disguised as a latency win.

---

## 8. Out of scope

Local files (mmap path — untouched, and unchanged by the POC), the skene reader,
footer fetching (already batched separately), fairness between concurrent
queries, and any change to coalescing policy.
