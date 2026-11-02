# Remote read flow control: protect throughput without capping it

**Status:** RULED 2026-11-01 (§8). Steps 1 and 2 BUILT 2026-11-01, uncommitted: progress deadlines (§4.1, tests §6.1-6.3 and §6.6) and the process IO budget with the byte gate and per-scan budget deleted (§4.2-4.4, tests §6.4-6.5 in `test_memory_budget.py`). Owed: §6.7 performance on the fleet shapes.

**As built, where it differs from §4:**
- The IO budget lives in `draken/core/mem_account.{h,cpp}` (`draken_io_*`), the existing single-home translation unit, not a new `io_budget.h`.
- The per-scan read-backs keep their existing names: `memory_budget_bytes` (now the process budget the pipeline ran under), `memory_held_high_watermark`, `admission_waits`, `admission_blocked_ns`. New: `http_stalls`, `http_ceiling_timeouts`, `fetch_in_flight_peak`, `fetch_in_flight_avg`. `http_retries` is now per scan. Per statement: `io_budget_bytes`, `io_held_peak_bytes`.
- `IO_BUDGET_PERCENT` is shown in `SHOW VARIABLES` as `io_budget_percent` (server-owned, restricted).

**Replaces:** the adaptive bytes-in-flight gate (`ByteGate`, `HttpTuning::max_bytes_in_flight`,
`src/cpp/http_client.cpp`) and the size-derived request deadline it exists to protect.

**Builds on:** `docs/FETCH_STREAM_BUDGET_DESIGN.md` (process-wide fetch stream budget, also a
proposal). That document bounds *how many* requests are in flight per process; this one bounds
*how many bytes* and decides *when a request has failed*. The two are complementary and are
sequenced in §7.

---

## 1. The problem, measured

Fleet node `worker-us-e1-n488` (n4a-standard-4, 14.5 GiB cgroup), TPC-H SF100 Q8, one scan of
10 files, 2026-11-01:

| | |
|---|---|
| bytes downloaded | 1.71 GB |
| scan wall | ~10 s |
| peak IO throughput | 167 MB/s, a flat plateau for the whole scan |
| summed download time | 22 s ⇒ **~2.2 requests in flight on average** |
| per-request speed | ~77 MB/s |
| row groups waiting to be claimed (peak) | 72 |
| decode latency | 5 ms against 57 ms download |

The link from this VM does 760 MB/s over REST and 2.3 GB/s over DirectPath with 4 connections. The
scan used a fifth of the first and a fourteenth of the second. **Throughput is lost by
under-issuing, not over-issuing.**

## 2. Why the current mechanism is the wrong shape

Each request's deadline is `max(timeout_floor, bytes ÷ min_bandwidth)`, with 60 Mbps and 10 s by
default. Many large requests on one shared link can each be "on time" by that arithmetic while the
last bytes land late, so a busy link produces timeouts. `ByteGate` exists only to stop that: it
estimates the link and admits requests until their bytes would not drain within the deadline.
Every defect follows from having to estimate a link:

1. **It measures demand, not capacity.** The estimate is the windowed maximum of bytes *received*.
   With ~2 requests in flight it learns 167 MB/s and can never learn more. It does not throttle (its
   cap is 5 s of transfer at the estimate, ~835 MB here), but it cannot say anything about the
   link.
2. **There are three gates.** `http_client.cpp` is compiled into `_operators`, `pool_reader` and
   `compiled.http_client`, and each has its own static `ByteGate` and received-bytes counter. Each
   sees a slice of the traffic, and their caps do not add up to anything. DirectPath bytes are
   counted in `pool_reader`'s gate, while REST bytes from the native scan are counted in
   `_operators`'.
3. **Its constants describe Cloud Run.** 60 Mbps and 10 s were chosen where the worker measured
   ~15 MB/s. On a VM they are wrong by two orders of magnitude, and they still set every deadline.
4. **Samples are taken only when the gate is consulted.** Slices stretch over idle time.

A second, independent defect sits next to it. `PARQUET_IO_MEMORY_BUDGET_BYTES` auto-sizes to **half
the container per scan**. Q8 opens about six remote scans, so the "bound" is about 3× the
container. That is a per-scan ledger presented as a memory bound; it does not bound the process.

## 3. Principles

- **Bound what is finite and local.** Memory is finite and known exactly (the cgroup limit is now
  set on the fleet). Link capacity is neither, and TCP already shares it fairly among flows.
- **Fail on lack of progress, not on elapsed time.** A request that is moving bytes is healthy,
  however long it takes. A request that has stopped is dead, however small it is.
- **One instance per process means one translation unit.** Process-wide state lives in
  `draken_native`, as the memory account does (`draken/core/mem_account.h`), never in a source
  file compiled into several extensions.
- **No constant may describe a machine.** Defaults are derived from the container, or they are
  deadlines that do not depend on speed.

## 4. Design

### 4.1 Progress deadlines (replaces size-derived deadlines)

Every body transfer, REST or DirectPath, fails when **no bytes arrive for `stall_ms`**. It is also
bounded by a generous **absolute ceiling `ceiling_ms`**, which catches a trickle that never quite
stops.

| transport | mechanism |
|---|---|
| REST `get` / `get_many` / `head` / `head_many` | stall: a `CURLOPT_XFERINFOFUNCTION` watch (`StallWatch`) that aborts once nothing has moved for `stall_ms`, its clock starting when the request is sent (pretransfer), so a request queued for a connection is never a stall. `CURLOPT_TIMEOUT_MS = ceiling_ms`, `CURLOPT_CONNECTTIMEOUT_MS = connect_ms` (previously unset, so curl's 300 s applied) |
| REST `put` (uploads) | the stall and connect deadlines as above; the caller's own `timeout_ms` stays the ceiling, because an upload's size is the caller's and is not a read range |
| DirectPath `RangeStream` | a per-stream "last byte at" timestamp; the connection's I/O loop cancels a stream idle for `stall_ms` (RST_STREAM CANCEL) and reports it retryable. `read_many`'s whole-call deadline becomes `ceiling_ms`. |
| ALTS handshake, xDS, RLS | unchanged: these are control-plane round trips with their own fixed timeouts |

Not curl's `LOW_SPEED_LIMIT`/`LOW_SPEED_TIME`: curl averages speed over a ~5 s window, so a
transfer that went quiet after some bytes was caught 5+ s late (measured: 6.4 s for a 1 s stall;
the progress-callback watch fails at 1.09 s).

Retries are unchanged: a stall is a retryable `HttpError`, inside the existing `max_retries` and
backoff. Error text names which deadline fired (`stalled for N ms after M of K bytes` or
`exceeded the ceiling`), so a timeout says whether the link died or crawled.

Defaults (ruled): `stall_ms = 5 000`, `ceiling_ms = 30 000`, `connect_ms = 2 000`. Queries take
milliseconds to seconds on the VM deployment, so a request that has made no progress for 5 s is
dead, and one still running at 30 s has failed the query's purpose whether or not it is moving.

### 4.2 One process-wide byte budget (replaces the per-scan memory budget and the gate)

A new `draken/core/io_budget.h`, compiled into `draken_native` only and resolved at load like the
memory account:

```c
// Process-wide budget for bytes held by IO pipelines: compressed bytes from fetch
// admission until decode consumes them, plus decoded row groups until the consumer pops them.
void    draken_io_budget_configure(int64_t budget_bytes);    // 0 = off
// Blocks until `bytes` fit, FIFO; admits immediately when this caller holds nothing
// (so no scan can deadlock on its own budget). Returns ns waited.
int64_t draken_io_admit(int64_t bytes);
void    draken_io_release(int64_t bytes);
int64_t draken_io_held(void);
int64_t draken_io_held_peak(void);
```

The pipeline's existing ledger (`ledger_charge` / release, the footer-estimated charges) keeps its
accounting and its "always admit when nothing is releasable" rule. It now draws from this one pool,
not a per-scan budget. Fetch admission charges the compressed bytes of the row group before the GET
is issued. Decode moves the charge from compressed to decoded size. Pop releases it.

**Default (ruled):** `IO_BUDGET_PERCENT` (default 10) × container. The container is the cgroup limit when
set, else physical RAM. This is the same derivation the chunk cache uses, so the two are sized from
one number. Fleet node: 10% of 14.5 GiB ≈ 1.45 GiB, on the order of 12k requests in flight for TPC-H Q8
at SF100. That is far more than any link fills in a stall window, and it leaves the chunk cache
(≤ 0.6 × 14.5 − 4 = 4.7 GiB), slab retention (4 GiB) and query memory room inside the cgroup.

The bytes-in-flight question the gate answered becomes a memory question. "Can the link drain this
before the deadline" no longer exists, because the deadline is progress-based.

### 4.3 Deletions

- `ByteGate`, `ByteAdmission`, `http_link::Admission`, `note_received`, `gate_report`, and the
  `on_received` hook in the gRPC client.
- `HttpTuning::max_bytes_in_flight`, `min_bandwidth_bytes_per_s`, `timeout_floor_ms`;
  `timeout_for_bytes_ms`, `request_timeout_ms`.
- Config and variables: `HTTP_MAX_BYTES_IN_FLIGHT`, `HTTP_MIN_BANDWIDTH_MBPS`,
  `HTTP_REQUEST_TIMEOUT_FLOOR_MS`, and the per-scan `PARQUET_IO_MEMORY_BUDGET_BYTES`.
- Added: `HTTP_STALL_MS`, `HTTP_CEILING_MS`, `HTTP_CONNECT_MS`, `IO_BUDGET_PERCENT` (same
  ownership and visibility as today's HTTP variables).
- The `byte_gate_*` scan diagnostics (added 2026-11-01) and `HttpGateUse` go too. They are replaced
  by §4.4.

### 4.4 Telemetry

Per scan (`io_scan_diagnostics`):

- `io_budget_wait_ns`, `io_budget_waits`: did the process budget ever hold this scan back.
- `io_held_peak_bytes`: the most this scan held.
- `http_stalls`, `http_ceiling_timeouts`: per pipeline, not process-wide. Today's `http_retries` is
  process-cumulative and mislabelled per scan; it becomes per-pipeline in the same change.
- `fetch_in_flight_avg`, `fetch_in_flight_peak`: time-weighted requests in flight. This is the
  number that exposes §1's under-issuing directly. Today it has to be derived by hand from summed
  download time.
- `gcs_transport` stays.

Per statement: `io_budget_bytes`, `io_held_peak_bytes` (process), next to the chunk cache and slab
fields.

### 4.5 Concurrency: deliberately not adaptive (yet)

How many requests are in flight is `FETCH_STREAM_BUDGET_DESIGN.md`'s question (a process-wide
stream budget). No controller is added here. If, after §7 step 2, the fleet shows over-subscription
(429s, or latency rising while throughput is flat with `fetch_in_flight_avg` high), the follow-up is
a knee-finder in that facility: raise in-flight while delivered bytes per second keep rising, hold
when they stop, halve on 429 or stall. It would be one instance in `draken_native`. Without that
evidence it is a mechanism with nothing to act on.

## 5. Measurement caveat: §1 is not (yet) evidence of an IO defect

The first draft of this section called the ~2 requests in flight an "issuance defect". The same
panel argues against that. The operators were 82% busy, and workers recorded 683 queue stalls
(17 ms average, about 11.6 s in total). If those stalls are the pipeline's back-pressure
(`worker_blocked_ns`: workers parked because the consumer is behind), then Q8 on a 4-vCPU node was
**CPU-bound downstream of the scan**, and few requests were in flight because nothing was waiting
for more bytes. That is back-pressure working as designed, not a defect.

What follows for this design:

- §1's 167 MB/s does not show the gate or the deadlines costing throughput. The case for §4 rests
  on §2 (wrong mechanism, three gates, Cloud Run constants, no process bound), not on that number.
- Throughput gates (§6.7) must use queries that are IO-bound on the box they run on, and every run
  must report `worker_blocked_ns` next to `fetch_in_flight_avg`. A consumer-bound run says nothing
  about IO.
- Whether the stalls really are back-pressure is read from `worker_blocked_ns` in the scan's
  diagnostics, not inferred from the panel. If they are not, issuance becomes a separate
  investigation.

## 6. Proof that nothing got worse

All fail-loud tests, using `dev/throttle_server.py` unless noted.

1. **The case the gate was built for.** A wide string column, 24 ranges at ~180 MB in flight, on a
   100 Mbps shaped link. Today it completes because of the gate. After, it must complete with no
   timeout, at ≥ 90% of the shaped rate.
2. **A dead link fails promptly.** A server that sends headers and then nothing fails in
   `stall_ms ± 1 s`, as retryable, and the error names the stall.
3. **A crawling link hits the ceiling.** 1 byte per second fails at `ceiling_ms`, and says so.
4. **The budget binds.** Three concurrent scans whose combined demand exceeds `IO_BUDGET`:
   `draken_io_held_peak() ≤ budget + one row group`, and `io_budget_waits > 0` on at least one
   scan. That is the KNOB rule: prove the knob moves.
5. **No starvation.** A scan holding nothing is admitted immediately while the budget is full.
6. **DirectPath stall.** One stream pauses mid-body: it is cancelled after `stall_ms` and its range
   retried, while the connection's other streams complete. There is no gRPC test server today. The
   test needs a minimal h2c server (`PlainTransport` side) that speaks just enough ReadObject to
   stream ranges and pause on command. Built: `tests/unit/connectors/_grpc_read_object_server.py`
   (Python `h2`, test-only dependency, ruled 2026-11-01).
7. **Performance, on the fleet shape (n4a-standard-16 per the 16/64 ruling, and n4a-standard-4),
   interleaved A/B:** ClickBench cold 6-query ladder and TPC-H SF100 Q8, REST and gRPC. Kill
   criterion: any arm slower than 1.03×.

## 7. Order of work

1. §4.1 progress deadlines, REST and DirectPath. The gate stays for now; with progress deadlines it
   no longer protects anything, which test 1 shows.
2. §4.2 process budget and §4.3 deletions in one change: the gate and the per-scan budget go
   together, so there is never a window with neither bound.
3. `FETCH_STREAM_BUDGET_DESIGN.md` on its own rulings.
4. §4.5 only on evidence.

Steps 1 and 2 each pass `make q`, `make slt`, `make rt` (rugo builds without HTTP, so the deadline
code is `#ifdef RUGO_ENABLE_HTTP` like the rest of the remote path) and the §6 tests that apply.

## 8. Rulings (architect, 2026-11-01)

1. **Progress-based deadlines, adopted** with stall 5 s / ceiling 30 s / connect 2 s ("queries take
   ms"); the size-derived deadlines are deleted.
2. **`ByteGate` is purged** with its settings; it is not kept as an option.
3. **One process-wide `io_budget`** replaces the per-scan budget, default **10%** of the container
   (≈ 1.45 GiB on the 14.5 GiB fleet node).
4. **Slow links:** we are building for the VM deployment; a wide scan on a slow shared link may now
   finish slowly (or hit the 30 s ceiling) and that is accepted.
5. **Issuance:** not established as a defect (§5); the draft's "fix it first" step is withdrawn.
   The `fetch_in_flight_*` and `worker_blocked_ns` read-backs ship with step 2 and settle it.

## 9. Addendum 2026-11-02: process request cap (BUILT, uncommitted)

Fleet evidence on 0.9.185 (n488, TPC-H Q8 SF100): cold 26.2 s on REST against 15.9 s on 0.9.183.
The orders and part scans' requests went from mostly < 50 ms to 100–500 ms, with up to ~90 reads
in flight across the three scans. The byte gate's removal had left no limit on reads in flight.
Ruled: cap the number of reads.

- `HTTP_MAX_REQUESTS_IN_FLIGHT` (config, default 64, 0 = no cap; variable
  `http_max_requests_in_flight`, SERVER/RESTRICTED per the 2026-08-28 ruling that process-wide
  properties are not SET-able). Read at import, so a sweep is one restart per value.
- One count per process in draken (`draken_io_requests_*`, `core/mem_account.cpp`, single home in
  draken_native). `FetchTrack` takes n permits (clamped to the cap) before the request clock
  starts, so every remote read in the parquet pipeline (REST get / get_many, DirectPath) is
  covered on both the trampoline and the native scan.
- Telemetry: per scan `request_cap`, `request_cap_wait_ns`; per query `io_requests_cap`,
  `io_requests_peak`.
- Test: `test_the_process_request_cap_binds_and_is_reported` (uncapped peak > 2; capped at 2 the
  process peak is ≤ 2 and the scan waits).
- Sweep owed on the fleet after release: cold Q8 at 32 / 64 / 128 / 0, interleaved. Target: back
  near 15.9 s. If no value gets there, the cause is elsewhere and the cap comes out.
