# Process-wide fetch stream budget

**Status:** PROPOSAL — awaiting the rulings in §6. Nothing here is built.

**Predecessor:** `docs/FETCH_DECODE_DECOUPLING_DESIGN.md` (P0 landed 2026-09-12 as
`parquet_io_fetch_ahead`, default 128 by architect's decision). This document is
that design's §4.1 ("pool topology — process-wide, NOT per-pipeline") worked out
to the level where it can be built and, more importantly, where it can be
*proven* not to have made anything worse.

**The number 128 is not under discussion here.** This design changes what 128
*means*: today it is "128 fetch threads per pipeline"; after, it is "at most 128
range GETs in flight per process, however many pipelines are open."

---

## 1. The defect, in numbers

`ParquetIOPipeline::set_fetch_ahead(depth)` builds a private
`BS::thread_pool` of `depth` threads per pipeline. Nothing bounds the product.

| situation | fetch threads | max connections (cap 3/thread) |
|---|---|---|
| one narrow scan, depth 128 | 128 | 128 (one range per row group) |
| one wide scan, depth 128 | 128 | 384 |
| late-materialization scan (two pipelines built at plan time) | 256 | up to 768 |
| three remote scans open in a join | 384 | up to 1152 |

Measured, production, single narrow scan (architect, 2026-09-12):

| depth | wall |
|---|---|
| 0 | 2.715 |
| 48 | 2.636 |
| 64 | 2.636 |
| 128 | 2.800 |
| 256 | 3.077 (and an earlier 256 run died outright) |

So a *single* pipeline at 256 is already past the knee, and a multi-scan query at
the shipped default of 128 sits in that region by construction. The interior
optimum near 48–64 says the instance wants a bounded number of streams. Today no
knob expresses "per instance".

Three further facts this design must respect:

- **The submission window sizes the memory pool** (`est_rg * (window + 1)`,
  both scan paths). Window is `max(workers, depth) + 2 = 130` today. That is a
  memory budget in the wrong unit (row groups) — see §3.7; not fixed here, but
  the design must not make it worse.
- **A process-wide value latches at first use** — the [FROZENcfg] class of bug.
  A design that cannot be re-sized without a restart makes every future sweep
  void. §3.5 addresses this head-on rather than hoping.
- **The exec pool must never run fetches.** The no-deadlock argument for
  `wait_and_get_result`'s help-loop relies on the fetch stage never being blocked
  behind a consumer. §3.4 proves the shared fetch pool preserves that.

---

## 2. The invariant this buys

> At any instant, the process holds at most **B** concurrent range GETs against
> object storage, for any number of open pipelines and queries. B = 128.

Corollaries that follow *by construction*, not by tuning:

- process fetch threads = B, not B × pipelines;
- process connections for row-group fetches = B (once §3.3's pin lands), not
  B × pipelines × per-thread cap;
- a pipeline can still saturate B alone (its window is ≥ B), so the single-scan
  behaviour is unchanged — this is the equivalence §4.2 verifies.

What it does **not** buy: a byte budget (§3.7), fairness between queries (§3.4),
or anything past the instance bandwidth cap. Four knobs have now measured that
cap; this design stops the knobs from multiplying, it does not move the wall.

---

## 3. Design

All of it lives in `rugo/src/parquet/io_pipeline.hpp`. Rugo must run without
Python, so the facility is C++ and the Python side only passes numbers.

### 3.1 One facility per process

```cpp
// io_pipeline.hpp — file scope, rugo-owned
class FetchStreamBudget {
    // BS 5.1.0. `pause` is REQUIRED: with it, reset(n) waits only for RUNNING
    // tasks and keeps the queue; without it reset() drains queued tickets too,
    // which would stall every open pipeline for the whole backlog on a resize.
    using Pool = BS::thread_pool<BS::tp::priority | BS::tp::pause>;
    std::unique_ptr<Pool> pool_;     // never destroyed — see "lifetime" below
    int  budget_ = 0;                // 0 == not configured == fetch-ahead off
    long pid_    = 0;                // fork guard — see "fork" below
    std::mutex mu_;
public:
    static FetchStreamBudget& instance();          // Meyers singleton
    // First call sizes the pool. A later call with a DIFFERENT budget is an
    // error unless `resize` is set (the admin path, §3.5) — a silent latch is
    // exactly the FROZENcfg trap; a loud mismatch is not.
    void configure(int budget, bool resize);
    Pool* pool();                                  // nullptr when budget_ == 0
    int   budget() const;
    std::size_t running() const;                   // pool_->get_tasks_running()
    std::size_t queued()  const;                   // pool_->get_tasks_queued()
};
```

**Lifetime.** The pool is created lazily on the first `configure()` with a
positive budget and is **never destroyed**: it is an intentional leak. The
alternative — a static with a destructor — runs at exit after `thread_local
HttpClient` instances and `curl_global_cleanup` may already be gone, which is a
teardown crash for no benefit. Threads asleep in a pool at process exit cost
nothing.

**Fork.** `configure()` and `pool()` compare `getpid()` against `pid_`. A pool
whose threads were created in a parent does not exist in a forked child; a
ticket dispatched into it would hang silently. On a pid mismatch `pool()` throws
`std::logic_error("fetch pool created in another process")` — loud, not a hang.
Whether the deployment forks after import is **unknown to this repo** (§6 R4);
the guard costs one integer compare and turns an unknown into a diagnosable
failure either way.

### 3.2 Pipeline attachment

`ParquetIOPipeline::set_fetch_ahead(depth)` keeps its name and signature. Its
meaning changes from "build a private pool of `depth`" to:

- `fetch_pool_` becomes a **non-owning** `Pool*` from
  `FetchStreamBudget::instance().pool()`;
- `fetch_ahead_` (the read-back) becomes this pipeline's **submission bound**,
  i.e. how many of *its* row groups may be in the fetch stage — set to
  `min(depth, budget)`;
- the two constructors are unchanged; the standalone rugo wheel gets the same
  singleton, sized by whoever calls first.

`enqueue_pending` and `run_one_fetch` are unchanged: they already dispatch by
pointer and already carry every ticket through `tickets_inflight_`. The only
per-item change is a queued→started timestamp for `fetch_queue_wait_ns` (§3.6).

### 3.3 Derived controls (three knobs collapse into one)

- **Submission window.** Remote scans: `window = B + 2`. One pipeline can then
  keep the whole budget busy; more than that only holds prefetched bytes it
  cannot issue. (`parquet_io_in_flight_limit` remains an explicit override on
  the trampoline path, unchanged; it is inert on the native path today and this
  design does not touch that — §6 R5 of the predecessor.)
- **Connection total.** Fetch-stage `get_many()` calls pin
  `CURLMOPT_MAX_HOST_CONNECTIONS` to **1**, so streams == fetch threads == B
  exactly, for any projection width. Basis: the 2026-07-24 production run found
  a cap of 1 fastest on wide projections and neutral on narrow; coalescing
  already merges a row group's adjacent ranges into few GETs, so the per-batch
  fan-out being serialised on one connection is the case that measurement
  covered. This is a **ruling** (§6 R2) — it changes wide-projection behaviour,
  in the direction the only data point says is good.
- **The `depth > workers` validator is retired.** It encoded one rig measurement
  (fetch pool no wider than the decode pool = a lost help-loop fetcher) as a law.
  Under a shared budget the per-pipeline comparison it made no longer exists:
  decode threads are a per-pipeline CPU number, streams are a per-instance IO
  number, and neither bounds the other. The explicit-window check (`window <
  depth` is inert) stays, because that combination is still inert.

### 3.4 Shutdown, cancel, deadlock, starvation

**Shutdown.** `wait_shutdown()` today calls `fetch_pool_->wait()`, which is
wrong for a shared pool (it would wait on other pipelines' fetches, or forever).
It is replaced by the spin already used for the injected decode pool:

```
while (tickets_inflight_ != 0) yield();
```

This is sufficient because every fetch ticket is counted in
`tickets_inflight_`, and its successor decode ticket is counted **before** the
fetch ticket releases (`run_one_fetch` increments for the successor, then
dispatches, then the wrapper decrements). `tickets_inflight_ == 0` therefore
means no ticket of either stage will touch `this` again. The exclusive-decode
`decode_pool_->wait()` stays after the spin, as today.

**Cancel.** Unchanged: a fetch ticket checks `cancelled_` before its GET and
publishes without IO, so a cancelled pipeline's queued tickets drain at queue
speed and hold no bytes and no connections. A cancelled query cannot pin the
budget.

**Deadlock — the argument, not a hope.** A wait cycle through the shared pool
needs a fetch ticket that waits on something a consumer holds. Verified in the
current code: `run_one_fetch` takes `queue_mutex_` for one push, notifies, and
dispatches; it never waits on `queue_cv_` or on back-pressure (that wait lives in
the decode stage). Its only blocking call is the network. The exec pool never
runs fetches. Therefore no fetch ticket can be blocked behind a consumer, and
the help-loop's progress argument in `wait_and_get_result` holds unchanged.

**Starvation.** FIFO across pipelines within a priority: a scan that submits
first can occupy all B streams while a smaller query's tickets queue. This is
the accepted trade from the predecessor's R3, and it is made **visible** rather
than assumed: `fetch_queue_wait_ns` per pipeline (§3.6) is the number that
says whether it hurts in practice.

### 3.5 Changing B at runtime (the FROZENcfg trap, confronted)

B is a process property, so it is **SERVER-owned** — the same ruling that moved
the worker counts. `parquet_io_fetch_ahead` moves from USER to SERVER,
RESTRICTED. Config `PARQUET_IO_FETCH_AHEAD` (env) sizes the pool on first use.

For sweeps without a redeploy: a `platform_admin` SET calls
`configure(B', resize=true)` → `pool_->reset(B')`. With the `pause` flag that
waits only for the GETs currently running (bounded by one fetch latency each),
keeps every queued ticket, and continues at the new width. Two rules make this
honest rather than a latch:

- the read-back is the pool's **actual** thread count (`fetch_stream_budget`
  in telemetry), never the SET value, so a resize that did not happen is visible;
- a non-admin session that SETs a different value gets a loud error, not a
  silently ignored variable (the exact failure the predecessor recorded for the
  headroom knob).

Concurrent queries see the last writer's value. That is documented, and it is
the correct semantics for a per-instance budget.

### 3.6 Telemetry — the instrument the gates in §4 run on

Per scan, in `io_scan_diagnostics`:

| key | meaning |
|---|---|
| `fetch_ahead_depth` | this pipeline's submission bound (unchanged name; new meaning) |
| `fetch_stream_budget` | the process pool's actual thread count at scan open |
| `fetch_streams_high_water` | max `running()` sampled by this pipeline's tickets — the number that proves the bound |
| `fetch_queue_wait_ns` | sum of ticket queued→started time — starvation made visible |
| `prefetch_discarded_bytes`, `http_retries` | unchanged |

Sampling `running()` at ticket start is one relaxed atomic read of a BS
counter; no clock, no lock.

### 3.7 Explicitly out of scope, with the hook left in

- **Byte budget** (predecessor §4.3). The fetch ticket is the right place to
  gate on "bytes fetched-not-decoded across the process"; v1 adds the process
  counter (`prefetch_bytes_in_flight`, incremented on fetch, decremented on
  decode/discard) but does **not** gate on it. Gating needs the cgroup-derived
  limit and a ruling on what to do at the limit (block the ticket vs. refuse the
  query); both are a separate design. The window-sizes-the-memory-pool formula
  is likewise untouched.
- **Fairness** between queries (§3.4).
- **M2/M3** from the predecessor (persistent connections, event loop). A
  persistent pool makes M2 *possible* later — its threads and their
  `thread_local HttpClient`s now outlive a query — but nothing here reuses a
  connection.
- **Decode sizing from the cgroup CPU quota.** Independent; unchanged.

---

## 4. How this is proven not to be degenerative

"Degenerative" here means any of: a wrong answer, a hang, a single-scan
slowdown, a bound that does not bind, or a knob whose read-back lies. Each has a
gate, each gate has a measurement, and the baselines are taken **before the
first edit** (the [base1st] rule).

### 4.1 Invariants that must not change (tests, all fail-loud)

| invariant | how it is pinned |
|---|---|
| bytes and rows identical to the coupled path | existing `test_fetch_ahead.py` assertions, extended to run with **three pipelines open concurrently** against the throttle server |
| fetch failure surfaces with the original error, once | existing fault test, unchanged |
| local scans never touch the facility | existing test; plus assert `FetchStreamBudget::instance().budget()` is still 0 after a local-only process |
| depth 0 == coupled path byte-for-byte | existing test |
| cancel is clean and holds no bytes | existing abandonment test, plus: cancel pipeline A while B and C run; A's `tickets_inflight_` reaches 0 within a bounded time; B and C complete with full row counts |
| pool created at most once per pid; second `configure` with a different value without `resize` is a loud error | new unit test |
| shutdown cannot notify-after-free | 200 open/close cycles of pipelines sharing the pool under the throttle server with `rtt` set so tickets are queued at close; any crash or hang fails |

### 4.2 Single-scan equivalence (the "not slower" gate)

With one pipeline, a shared pool of 128 must behave exactly like a private pool
of 128. Rig protocol, `dev/throttle_server.py`, scratchpad `ab.py`:

1. **A/A first**, 5 interleaved rounds, to record today's floor (last measured
   spread 2–4%).
2. Baseline **before any edit**: coupled vs depth 32, strings / narrow / wide.
3. After the change: the same three cells.
4. **Pass:** each cell within the A/A floor of its pre-edit number. Last known
   values to reproduce: coupled 4.10s, depth 32 0.76s (strings).

Production: the architect's 128 cell (2.80s, single narrow scan) must reproduce
within the in-session noise floor, with `fetch_streams_high_water` reading ≤ 128
and `fetch_stream_budget` reading 128. In-session, interleaved; a cross-session
comparison is not evidence (±13% drift recorded).

### 4.3 The bound must be proven to bind (the [KNOB] rule)

A bound nobody has watched move is a hope. New rig test, three remote scans
driven concurrently from three threads, budget 8, per-pipeline depth 8:

- **before** the change (private pools) the process-wide high-water of running
  fetches reads up to 24;
- **after**, every pipeline's `fetch_streams_high_water` ≤ 8, and at least one
  reads ≥ 4 (the budget was actually used, not just never reached);
- process thread count ≤ decode threads + 8 + exec pool (read from
  `/proc/self` on Linux, `ps -M` on macOS in the harness);
- rows and bytes identical across all three scans to their single-scan runs.

Then the same shape with budget 128 and depth 128 to confirm the default.

### 4.4 No new blocking or starvation pathology

Stress test under the throttle server with `rtt=50ms` so tickets queue:

- four pipelines, one with a deliberately slow consumer (sleeps between pulls,
  so its decode stage back-pressures), one cancelled at 25%, two normal;
- all four reach shutdown; the normal two return full row counts; wall time
  bounded (fail at 10× the single-scan time);
- `fetch_queue_wait_ns` is recorded per pipeline so the FIFO cost is a number
  in the test log, not an inference.

If the sanitizer build exists in CI, the §4.1 and §4.4 tests run under
ThreadSanitizer; if it does not, that is stated in the PR rather than implied.

### 4.5 Runtime resize is real, not a latch

Test: configure 8 → open a pipeline and let it queue tickets → admin resize to
16 → `fetch_stream_budget` reads 16 on the next scan, and the queued tickets
were not lost (row count intact). Then resize down 16 → 4 mid-scan: completes,
high-water thereafter ≤ 4.

### 4.6 Kill criteria — what makes me revert rather than argue

- any byte or row divergence, anywhere;
- any hang in §4.1/§4.4, even once;
- a single-scan rig cell slower than pre-edit by more than the A/A floor;
- `fetch_streams_high_water` exceeding the budget in §4.3;
- production 128 cell slower than 2.80s by more than the in-session floor;
- `io_http_retries` non-zero on a query where it was zero before.

Any one of these and the change comes out; it does not get tuned in place.

### 4.7 Rollout shape

The change is **behaviour-neutral for one scan** (§4.2) and **strictly
bounding for many** (§4.3). Depth 0 stays the coupled path. Landing order:
baselines → tests that fail against today's code → implementation → rig A/B →
`make q` → production reproduction of the 128 cell.

---

## 5. Work plan and size

| piece | where | size |
|---|---|---|
| `FetchStreamBudget` singleton, pid guard, configure/resize | `io_pipeline.hpp` | ~80 lines |
| `set_fetch_ahead` → attach; `wait_shutdown` → spin; connection pin in the fetch stage; `fetch_queue_wait_ns` + high-water sampling | `io_pipeline.hpp` | ~60 lines |
| retire the `depth > workers` validator; window = B + 2; two new telemetry keys on both paths; `configure()` call at plan time | `pool_reader.pyx/.pxd` | ~40 lines |
| `parquet_io_fetch_ahead` USER → SERVER; admin resize path | `variables.py`, the SET handler | ~20 lines |
| tests in §4.1, §4.3, §4.4, §4.5 | `tests/unit/connectors/parquet_io/` | ~250 lines |
| multi-scan rig scenario | scratchpad `ab.py` | ~40 lines |

---

## 6. Rulings needed before a line is written

- **R1.** Go on the process-wide facility as designed, B = 128.
- **R2.** Pin per-batch host connections to 1 inside the fetch stage so
  streams == threads (§3.3). Changes wide-projection behaviour, in the measured
  direction.
- **R3.** `parquet_io_fetch_ahead` becomes SERVER-owned with an admin resize
  path; non-admin SET of a different value is a loud error (§3.5).
- **R4.** Does the deployment fork after import? Determines whether the pid
  guard is a safety net or the main path.
- **R5.** Byte budget stays a follow-on with only the counter landing now
  (§3.7), or is pulled into this change.
