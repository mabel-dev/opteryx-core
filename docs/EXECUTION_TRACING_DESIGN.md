# Execution Tracing — Design

Status: design, pending build. Author: agreed with architect 2026-07-22.

A trace, when enabled, produces a **waterfall/timeline** of a single query's
execution: when IO requests were issued, when their bytes returned, how long
data sat buffered before decode, how long decode took, how many row-group
gathers ran concurrently, and — on the same axis — when each operator ran and
for how long. The purpose is to find **stalls and bottlenecks** that aggregate
counters cannot show.

---

## 1. Why the old system died, and what that dictates

There are two instrumentation stacks in the tree today.

**Stack A — `opteryx/tracing/`** was purpose-built for this waterfall. Its event
schema (`TraceEvent`), `Session.trace()` plumbing, the `OPTERYX_TRACE` flag, and
a complete three-chart ECharts renderer (`dev/io_waterfall/`) are all intact.
But every semantic emitter — `trace_io_started/completed`, `trace_buffer_*`,
`trace_decode_*` — has **zero call sites**, and `reader.py` parses an event
vocabulary nothing emits. It is a renderer with no data source.

It died because it is **Python-first**: `record_event()` is a Python call. The
execution engine is native and GIL-free (see the Engineering Contract §2); you
cannot call into Python per-morsel or per-IO-request from the drive loop. So the
emitters were never wired, and the corpse misleads anyone who tries to wire them.

**Conclusion that governs this design: tracing must be recorded natively.** The
Python/native boundary is crossed exactly **once**, at query teardown, to hand
back a single blob. Any per-event Python emission on the execution path is the
mistake that killed Stack A and is forbidden here.

**Stack B — the live telemetry** is what actually runs and is well built:
`OpStats` (`src/cpp/engine/operator.hpp:36`, per-operator atomic `exec_ns` /
`cpu_ns`, always-on, identity-tagged, bracketed in `executor.hpp:96-133`);
`rugo_tel` (`rugo/src/parquet/telemetry.hpp`, decoder phase ns); `io_pipeline.hpp`
per-row-group `read_ns` / `decode_ns`; and `QueryTelemetry`
(`opteryx/models/query_telemetry.py`), the backbone that reaches every layer and
converts `time_*` keys ns→s in `as_dict()`.

**The gap.** Stack B records **sums**. A waterfall needs **spans** —
`(t_start, t_end)` pairs on a shared clock, tagged for correlation. "Decode took
4s total" cannot reveal that 3s of it was one row group sitting buffered waiting
for a free worker. This design adds the missing primitive: a **natively-recorded,
correlated, timestamped span stream**. It is *additive* to Stack B — the sums stay
as cheap always-on reporting; spans are the trace-on detail layer.

---

## 2. Constraints (from the Engineering Contract)

- **Native recording.** Probes are C++/Cython, GIL-free. One Python crossing, at teardown.
- **Runtime gate, env-driven.** `OPTERYX_TRACE=1` at the surface arms tracing.
  No rebuild required to trace a misbehaving query in a prod wheel.
- **Zero cost when off.** A trace-off query pays at most one perfectly-predicted
  branch per span site and reads no clock.
- **≤ 5% overhead when on.** Drives arena sizing and default probe depth.
- **One clock, one epoch.** Cross-subsystem spans are meaningless otherwise.
- **Truncate, don't fail.** Buffer exhaustion warns into telemetry and keeps
  going; it never aborts the query. (Divergence from the contract's usual
  fail-loud posture, agreed explicitly: a trace is a diagnostic, not a result —
  losing the tail of a trace must not lose the query. The truncation is a
  *reported fact*, not a silent degrade.)

---

## 3. Core model — spans on a shared clock

A trace is a flat stream of fixed-layout POD span records. No strings, no
allocation on the hot path.

```c
// src/cpp/engine/trace.hpp (new)
struct TraceSpan {              // 64 bytes, one cache line
    uint64_t t_start_ns;        // shared monotonic epoch (see §4)
    uint64_t t_end_ns;          // 0 == point event; else close timestamp
    uint32_t query_seq;         // per-query trace generation
    uint16_t category;          // TraceCategory, below
    uint16_t worker_id;         // engine worker or io_pipeline thread that emitted
    uint32_t node_id;           // interned plan-node identity (0 == none)
    uint32_t corr_id;           // correlation handle (§3.2)
    uint32_t rg_idx;            // row-group index; 0xFFFFFFFF == n/a
    uint32_t rows;              // rows in this unit of work
    uint32_t bytes;            // bytes moved
    uint32_t detail;           // category-specific (decode phase, morsel idx, column idx)
};

enum TraceCategory : uint16_t {
    TC_SOURCE_PULL = 1,   // worker pulling a morsel from a source
    TC_OP_EXEC,           // operator execute() self-time
    TC_SINK,              // sink() accumulate
    TC_COMBINE,           // per-worker combine into global
    TC_QUEUE_WAIT,        // drive/consumer waiting on the engine queue
    TC_IO_REQUEST,        // gather issued -> first byte (half-span, drive thread)
    TC_IO_WAIT,           // bytes-in-flight -> bytes resident
    TC_BUFFER_RESIDENT,   // bytes resident -> decode start  (the "sat buffered" gap)
    TC_DECODE,            // decode of a row group
    TC_DECODE_PHASE       // optional sub-span (decompress/rle/mask/...) via detail
};
```

### 3.1 One clock, one epoch — the linchpin

Every span, from any thread in either subsystem, timestamps with a single
source. `executor.hpp:37` already has `telem_now_ns()` reading `CLOCK_MONOTONIC`.
rugo's `telemetry.hpp` uses `std::chrono::steady_clock`. These must be unified:
one shared `trace_now_ns()` in `trace.hpp`, used by the engine and by rugo, with
the **epoch captured once at query start** and subtracted so spans are small
offsets from query-start rather than absolute wall counts. Without a shared
epoch, IO spans and operator spans do not lie on the same axis and the waterfall
is fiction. **This unification is Phase 1 and blocks everything else.**

### 3.2 Correlation — how the waterfall is reconstructed

Two tags turn an unordered span pile into a lifecycle timeline:

- **`node_id`** — the interned plan-node identity, reusing the existing
  `OpStats.identity` attribution. Ties operator/source/sink spans to the plan
  node the compiler lowered.
- **`corr_id`** — a monotonic handle minted when a **row-group gather is issued**
  (granularity agreed: per row group, not per column chunk). Every span in that
  gather's life — `TC_IO_REQUEST`, `TC_IO_WAIT`, `TC_BUFFER_RESIDENT`,
  `TC_DECODE` — carries the same `corr_id`, even though they run on different
  threads (issue on the drive thread, completion/decode on io_pipeline workers).

From these, the renderer draws each request's full lifecycle:
*issued at T → bytes back at T+n → sat buffered until T+m → decoded over
[T+m, T+k]*. **Concurrency needs no counter**: "how many gathers ran at once" is
just the count of overlapping `TC_IO_WAIT`/`TC_DECODE` spans at any instant.

---

## 4. Native architecture

### 4.1 Per-thread arenas, lock-free

Each engine worker and each io_pipeline thread owns a pre-allocated
`TraceSpan[]` arena (`thread_local`). Recording is a bump-index append: no
atomics, no lock, no contention — the same per-worker reasoning `OpStats`
already uses for its local state. Arenas are registered in a global intrusive
list at creation so the drain can walk them.

### 4.2 The gate

A `thread_local bool g_trace_on`, set at query start from `OPTERYX_TRACE`. Off
path:

```c
inline uint32_t trace_begin(uint16_t cat, uint32_t node_id, uint32_t corr_id) {
    if (!g_trace_on) return TRACE_NONE;   // one predicted branch, no clock read
    ...
}
```

Because most span sites (operator, source, decode) already read the clock to
feed the `exec_ns`/`decode_ns` sums, trace-on adds only the endpoint capture and
the arena append — comfortably inside the 5% budget. Trace-off adds the single
branch and nothing else.

### 4.3 Span open/close — two shapes

- **Scoped RAII** for operator/decode spans — `TraceScope s(TC_OP_EXEC, node, corr);`
  closes on destruction. Drops into the existing `executor.hpp` brackets where
  `exec_ns` is already accumulated (`:96-133`); we record the endpoints we
  currently discard.
- **Explicit begin/end handles** for IO, where issue and completion are on
  different threads. `trace_begin()` returns a slot id; `trace_end(id, rows,
  bytes)` closes it. The `corr_id` — not the slot — is what stitches the
  cross-thread half-spans together.

### 4.4 Drain — the single Python crossing

At `execute_native` teardown in `compiler.py` (alongside the existing
`collect_op_stats()` harvest, ~line 2134): one native call walks every
registered arena, concatenates into one contiguous buffer, and returns it to
Python as **one columnar blob** plus a small symbol table (`node_id → identity`,
category names). It lands on:

- `QueryTelemetry._reading["trace_spans"]` — the blob (memoryview over the
  native buffer, no per-span PyObjects).
- `QueryTelemetry._reading["trace_symbols"]` — the id→name maps.

One crossing, one allocation. Like `native_op_stats`, these keys are surfaced
via the trace access path (§6), never as top-level `as_dict()` keys — add them to
the `connector_only_keys` pop-list in `query_telemetry.py:84`.

### 4.5 Overflow — truncate and warn

Arena full → stop appending on that thread, set a per-thread `truncated` flag.
At drain, if any thread truncated, push a message via
`QueryTelemetry.add_message("trace truncated: worker N exceeded M spans;
timeline is incomplete")`. The query completes normally; the trace is honestly
labelled incomplete. Default arena size targets a normal query at ≤5% memory
overhead; size is set from an env knob (`OPTERYX_TRACE_ARENA_SPANS`, default
e.g. 1M spans/thread ≈ 64 MB/thread cap).

---

## 5. Where the probes go

| Span | Site | Notes |
|---|---|---|
| `TC_SOURCE_PULL` | `executor.hpp:131` | already timed; add endpoints |
| `TC_OP_EXEC` | `executor.hpp:111` | already timed (`exec_ns`) — the operator timeline |
| `TC_SINK` / `TC_COMBINE` | `executor.hpp:98`, combine | breaker cost |
| `TC_QUEUE_WAIT` | drive/consumer loop, `compiler.py` engine spans | `time_engine_*` totals become spans |
| `TC_IO_REQUEST` + `TC_IO_WAIT` | `io_pipeline.hpp:~1240` gather issue → completion | two-thread half-spans; `read_ns` already measured |
| `TC_BUFFER_RESIDENT` | gather-complete → decode-start | **new** — the "sat buffered" gap; the highest-value missing signal |
| `TC_DECODE` | `io_pipeline.hpp` decode | `decode_ns` already measured |
| `TC_DECODE_PHASE` | `rugo_tel` phase sites | optional; off by default (budget) |

Only `TC_BUFFER_RESIDENT` needs a genuinely new measurement: stamp a timestamp
when a gather's bytes go resident, read it again when decode begins. Everything
else captures endpoints the code already computes for its sum-counters.

---

## 6. Access and rendering

`Session.trace()` stays the access point; retarget it to yield the drained span
blob. `dev/io_waterfall/` already builds the three charts — IO waterfall (à la
the DevTools Network tab), operator execution timeline, per-operator profile.
Point `dev/io_waterfall/reader.py` at the blob's columnar layout instead of the
dead JSONL event vocabulary. The transport changes (native blob, not per-event
Python appends); the renderer barely changes. An `EXPLAIN ANALYZE`-style surface
can dump the blob for the tool.

---

## 7. Delete vs keep

**Delete** (dead and actively misleading — the Python-first corpse):
`trace_io_*` / `trace_buffer_*` / `trace_decode_*` helpers in
`opteryx/tracing/event_recorder.py`; `ring_buffer.py`; `trace_writer.py`;
`TraceConfig`; the `# TRACE:` comment-injection loader in `debugging.py` (zero
markers exist in the tree). Leaving them invites re-wiring the wrong thing.

**Keep**: `QueryTelemetry`, `OpStats`, `rugo_tel`, the `io_pipeline` timers
(spans are additive, the sums remain useful always-on), `Session.trace()`,
`OPTERYX_TRACE`, and the `dev/io_waterfall` renderer.

**Reuse the event schema idea, not the code**: the `TraceEvent` field set was
right; it is reborn as the native `TraceSpan` POD.

---

## 8. Phasing

1. **Clock + backbone.** Unify the monotonic epoch across engine and rugo; land
   `trace.hpp` (`TraceSpan`, arenas, gate, drain into `QueryTelemetry`). Prove
   with `TC_OP_EXEC` only — cheapest, sites already timed. Verify ≤5% on `make q`
   / clickbench with trace on, ~0% with it off.
2. **IO spans.** `TC_IO_REQUEST` / `TC_IO_WAIT` / `TC_BUFFER_RESIDENT` /
   `TC_DECODE` with `corr_id` threading. Where the waterfall's value lives.
3. **Renderer retarget.** Point `dev/io_waterfall/reader.py` at the blob; delete
   dead Stack A.
4. **Decode sub-phases** (optional). Fold `rugo_tel` phases into
   `TC_DECODE_PHASE`, off by default.

---

## 9. Implementation status (2026-07-22)

**Landed:**
- Phase 1 (operator spans): `draken/core/trace.hpp` (span/arena/gate primitive),
  `TC_OP_EXEC` wired into `executor.hpp`'s operator-execute bracket, drained via
  `NativePlan.collect_trace_symbols()` + `native_trace_drain()` into
  `QueryTelemetry._reading["trace_spans"/"trace_symbols"]`.
- Phase 2 (IO spans, partial): rugo's `io_pipeline.hpp::decode_row_group`
  reconstructs `TC_QUEUE_WAIT` (enqueue → a worker claims the item — the real
  "sat waiting" signal), `TC_IO_REQUEST`, and `TC_DECODE` spans from timestamps
  already computed for `total_read_ns`/`total_decode_ns`, correlated by a
  per-row-group `corr_id` minted at enqueue.
- `TC_BUFFER_RESIDENT` is deliberately **not emitted**: this implementation has
  no distinct "bytes arrived, decode not yet started" stage (fetch and decode
  happen column-by-column in the same loop) — emitting it would read ~0 always
  and add noise, not signal. It becomes meaningful only if a real buffering
  stage (e.g. a bounded pending-decode queue) is introduced later.
- rugo spans carry `node_id = 0` (untagged) — `ParquetIOPipeline::
  set_trace_node_id()` exists but is not yet called from the compiler/scan
  construction path, so IO spans aren't yet attributable to a specific scan
  plan node when a query has more than one. Follow-up work, not done here.

**Architectural correction discovered during Phase 2 (important for anyone
extending this):** §4.1/§4.4 as originally written assumed a single process
address space would give the engine and rugo one shared copy of the tracer's
global state. That's false. `rugo/src/parquet/io_pipeline.hpp` compiles into
**two separate `.so` files** — `pool_reader.so` (rugo's own scan path) and,
via `native_parquet_scan_source.hpp`, `_operators.so` (the engine) — and
header-only `inline`/`thread_local`/function-local `static` C++ state does
**not** merge across separately-linked shared libraries. This codebase already
hit exactly this trap with `BS::thread_pool` (see `src/cpp/bs_pool_bridge_c.h`)
and fixed it with "one compiled home + `extern "C"` bridge, loaded
`RTLD_GLOBAL`". The tracer needed the same treatment:

- The real state (arenas, registry, gate, clock) lives in
  `draken/core/trace.hpp`, but that header is now included from **exactly one**
  translation unit, `draken/core/trace_bridge.cpp`, compiled into
  `draken.draken_native` (`draken_native.so` — the same `.so`
  `draken/core/draken_bridge.h` already uses for this purpose, loaded
  `RTLD_GLOBAL` by `draken/__init__.py`).
- `draken/core/trace_bridge_c.h` is the `extern "C"` surface (span category
  vocabulary + record/gate/drain functions, POD `DrakenTraceSpanC`) that every
  *other* `.so` calls through.
- `src/cpp/engine/trace.hpp` no longer includes `draken/core/trace.hpp` at
  all — it's a thin C++ convenience wrapper (`TraceScope`, `TC_*` constants)
  over the bridge. `io_pipeline.hpp` calls the bridge functions directly.
- This also fixed the correlation story for free: `pool_reader.so` (rugo's
  Python-facing trampoline scan) and `_operators.so` (native scan) both
  resolve to the *same* `draken_native.so` state, so their spans share one
  `query_seq`/gate/registry regardless of which `.so` recorded them.
- Both wheels are covered: `rugo/__init__.py` already does `import draken`
  (which RTLD_GLOBAL-loads `draken_native.so`) before `rugo_native.so` needs
  its symbols, so the standalone `rugo` wheel resolves the bridge the same way
  `opteryx_core` does — no separate wiring was needed for it.

Verified: `make compile` clean; `make q` 197/197; a traced query shows
`TC_OP_EXEC` (engine) and `TC_QUEUE_WAIT`/`TC_IO_REQUEST`/`TC_DECODE` (rugo)
spans sharing one `query_seq` and correlated `corr_id`; an untraced query
leaves `trace_spans`/`trace_symbols` entirely absent from telemetry (verified
zero footprint, not just zero cost).

**Also landed, same session — full fidelity + node_id wiring + dev tool
retarget:**
- `ParquetIOPipeline::set_trace_node_id()` is now wired: `Engine::
  set_native_scan_source` (`engine.hpp`) tags the pipeline with the same
  `node_id` its `NativeParquetScanSource`'s `OpStats` gets, so IO spans
  attribute correctly to their scan when a query has more than one.
- `TraceSpan` gained a `file_id` field (repurposing a reserved slot — still
  64 bytes). `draken_trace_intern_file()`/`draken_trace_drain_file_symbols()`
  (draken/core/trace.hpp + trace_bridge_c.h/.cpp) intern the row group's file
  path once per unique path per query (reset at `trace_start_query()`, same
  per-query scope as `node_id`) and resolve it back at drain time — same
  shape as `node_id`→identity. `io_pipeline.hpp` interns `item.path` at
  enqueue and stamps `file_id` on all three emitted spans.
- `TC_DECODE` spans now carry a real row count (`column_stats[0].num_values`
  — stable, known pre-decode, shared by every column in the row group), not
  the placeholder 0 from the first landing.
- `dev/io_waterfall/` retargeted onto the new format: `span_reader.py`
  (`SpanTraceReader`, `dump_trace`/`load_trace` for a `.trace.json` sidecar)
  replaces the dead JSONL `reader.py` (deleted, along with its test and the
  orphaned `TraceReader`/`# TRACE:` machinery in `opteryx/tracing/` and
  `opteryx/debugging.py` — see §7). `generator.py`/`__main__.py` updated to
  match; the waterfall's middle lane (previously unused "buffer") now shows
  `TC_QUEUE_WAIT` (a real, measured gap), relabeled "Queue Wait" rather than
  mislabeled as buffering that doesn't exist in this implementation (§9's
  `TC_BUFFER_RESIDENT` note still applies — that phase is still not emitted).
- Verified with a real traced query end-to-end: `dump_trace` → CLI
  `trace`/`stats` → HTML with populated ECharts config, file path resolved,
  row/byte counts populated, `make q` still 197/197.

**Not yet done:** `TC_DECODE_PHASE` sub-spans from `rugo_tel`; footer-fetch
spans (footer fetches aren't span-recorded today — they land in
`telemetry["time_engine_footer_fetch"]` instead, so `dev/io_waterfall`'s
`footer_download_ops` stat is always 0).

## 9a. Architectural correction: trace data does not live in telemetry (2026-07-22)

§4.4 as originally implemented stored the drained span bundle *inside*
`QueryTelemetry._reading` (`trace_spans`/`trace_symbols`/`trace_file_symbols`),
alongside the aggregate counters. This was wrong, flagged by the architect:
**telemetry is aggregates that exist for every query** (bytes read, time
executing); **a trace is an event stream that exists only when
`OPTERYX_TRACE=1`**, produced by a different subsystem. The tell was that
`QueryTelemetry.as_dict()` had to explicitly pop those three keys before
returning — a field the object's own serializer has to exclude was never
really that object's field.

**Fixed:**
- New `opteryx.models.trace_bundle.TraceBundle` — a plain object (`blob`,
  `node_symbols`, `file_symbols`, `truncated`), owned directly by `Session`
  (`self._trace`), reset per query alongside (but independent of)
  `self._telemetry.reset()`. `execute_native`'s teardown now writes onto a
  `trace_sink: Optional[TraceBundle]` parameter threaded through
  `opteryx.managers.execution.execute()` → `execute_native()`, never onto
  `telemetry`. Tracing is armed only when `config.OPTERYX_TRACE` is set *and*
  a `trace_sink` was actually given — recording with nowhere to drain to
  would be wasted work.
- `QueryTelemetry.as_dict()`'s pop-list no longer has to name-exclude
  anything trace-related, because trace data is structurally incapable of
  reaching it.
- **The old coarse trace was deleted, not kept alongside the new one.** Prior
  to this session there was a second, pre-existing mechanism —
  `opteryx.tracing.event_recorder` (a global event list: `dataset_discovered`
  / `file_discovered` / `trace_session_end`, gated on the same `OPTERYX_TRACE`
  flag) — that `Session.trace()` used to expose. It predated this design and
  was judged to add no diagnostic value over the span waterfall. It is gone:
  `event_recorder.py`, its two emitter call sites
  (`filesystem_connector.py`, `query_session.py`'s old `close()`), and the
  session-id registration bookkeeping are all removed.
- **`Session.trace()` now IS the span trace's contact surface** — same method
  name, replaced content: `Session.trace() -> (blob: bytes, node_symbols:
  dict, file_symbols: dict)`. Raises `RuntimeError` if tracing was not armed
  for the query. Deliberately returns the **raw** bundle, not resolved
  records — a caller that only wants to persist a trace (e.g. a worker
  service uploading it alongside a query's results) pays no per-span Python
  object cost.
- **New interpreter, in production code, not `dev/`:** `opteryx.tracing` was
  repurposed (the coarse mechanism's package name, now free) to hold
  `opteryx/tracing/spans.py` — the canonical span-category vocabulary
  (mirroring `trace_bridge_c.h`'s `DrakenTraceCategory`), `parse_spans(blob)`
  (raw field dicts), and `interpret_trace(blob, node_symbols, file_symbols)`
  (flat, JSON-serializable, resolved span records — category name, resolved
  operator/file identity, rows/bytes/timestamps). This is the "binary ->
  meaningful" boundary the architect asked for: any consumer (a script, a
  notebook, a worker service inspecting a stored trace later) can call it
  without depending on `dev/` tooling. `dev/io_waterfall/span_reader.py` now
  builds its chart-shaped views (`operation_timelines`, `exec_timelines`,
  etc.) on top of `interpret_trace`/`parse_spans` instead of duplicating the
  wire-format parsing.
- `OPTERYX_TRACE_SAMPLE_RATE` (the coarse mechanism's per-file sampling knob)
  removed — no replacement; the span tracer's cost-control mechanism is arena
  truncation (`OPTERYX_TRACE_ARENA_SPANS`), not sampling.

Verified: `make compile` clean, `make q` 197/197, a real traced query
produces a `Session.trace()` bundle that `interpret_trace()` resolves to
real operator/IO/decode records, `trace_spans` confirmed absent from both
`QueryTelemetry._reading` and `Session.telemetry` (the `as_dict()` surface),
and `Session.trace()` raises when tracing wasn't armed for the query.

**Follow-up, same day:** `worker.opteryx` (`/Users/justin/Nextcloud/worker.opteryx`,
sibling repo) updated to match — `_write_trace()` now persists the raw
`(blob, node_symbols, file_symbols)` bundle as two GCS objects
(`trace.spans.bin` raw bytes, `trace.symbols.json`), replacing the old
`trace.jsonl` write against the now-deleted coarse-trace contract. Also fixed
there and in `opteryx.tracing.spans`/`dev/io_waterfall`: GCS-backed
`file_symbols` values are full **signed URLs**, including a live,
time-boxed bearer credential in the query string (`X-Goog-Signature=...`).
Neither `interpret_trace()`'s `file` field nor the dev-tool's chart labels
were stripping it — both were echoing a live credential into
whatever consumed them (a rendered chart, in the dev-tool case). Fixed with
`opteryx.tracing.spans.strip_signed_url_query()`, applied in both places.
The raw persisted bytes in `trace.symbols.json` are NOT retroactively
sanitized by this fix — only the Python-side resolved `file` field is.

## 9b. Bug found and fixed: most row-group spans were silently lost on real (non-toy) queries (2026-07-22)

Running the tracer against a real GCS-backed dataset (21M rows, 91 row
groups, 8s wall time) surfaced a real defect: only 16–17 of 91 row groups'
IO/decode spans survived to the drained blob — telemetry (ground truth)
showed all 91 were genuinely read, but most of their spans vanished between
being recorded and being drained. Small local-file test queries never
exposed this because they only ever touch one or two row groups.

**Root cause:** `trace_thread_arena()` (`draken/core/trace.hpp`) stored each
thread's `ThreadArena` as a `thread_local` **value**, with the registry
(`trace_registry()`) holding a raw pointer to it (`&arena`) for the rest of
the process's life. That's safe only if the owning thread outlives the
query. Row-group decode work does not: some of the threads that run
`decode_row_group()` (`rugo/src/parquet/io_pipeline.hpp`) are short-lived —
created for a unit of work and exiting rather than living in a small,
persistent pool. When such a thread exited, its `thread_local ThreadArena`
was destroyed, leaving `trace_registry()` holding a dangling pointer.
`trace_drain()` dereferencing it later is undefined behavior — which in
practice mostly read back as an empty arena (`local_query_seq == 0,
spans.size() == 0`) rather than crashing, so the bug presented as "some
spans are just missing" rather than a hard failure.

**Confirmed via targeted diagnostics** (temporarily added, then removed —
not part of the shipped code): a global push counter proved every row
group's spans genuinely were recorded (271 pushes for a 90-row-group query,
matching exactly); a registry dump showed 75 of 90 registered arenas with
`local_query_seq == 0` and zero spans despite the push counter's proof they
should hold data; every *successful* arena held only ~3 spans (one row
group's worth) — never more, meaning no arena was actually being reused
across multiple row groups by a persistent thread. That combination (proof
of a push with no data at the registered address, one-shot arenas) is the
signature of a lifetime bug, not a logic bug in the drain filter itself.

**Fix:** heap-allocate `ThreadArena` and never free it —
`thread_local ThreadArena* arena_ptr = new ThreadArena();`, registered once,
kept alive for the process's life regardless of which OS thread first
touched it. The small unbounded allocation (one `ThreadArena` per distinct
thread that ever records a span, for the life of the process) is an
accepted tradeoff for a diagnostic subsystem — bounded by the number of
distinct threads a process ever creates, not by query volume.

**Verified**: same real query, before fix: 16/91 row groups traced, 3
files. After fix: 91/91 row groups traced (274 spans), 14 files — exact
match against telemetry's `row_groups_read`. `make q` 197/197 before and
after. The earlier "all row groups start at the same time" observation
turned out to be real, not a symptom of this bug — see §9c.

## 9c. Two follow-up fixes from architect review of the real-query waterfall (2026-07-22)

Showing the fixed tracer's output against the real GDELT query surfaced two
more things worth fixing immediately, not deferring:

**"All downloads start at the same time" — verified real, then made
demonstrable.** Traced (not assumed): `in_flight_limit = decode_workers + 2`
(`rugo` pipeline construction, `opteryx/connectors/parquet_io/pool_reader.pyx`),
and for GCS scans `decode_workers` comes from `config.PARQUET_GCS_IO_WORKERS`,
which defaults to **128** ("each range read pays network RTT, so high
concurrency wins" — `opteryx/config.py`). For a 92-row-group query that means
every fetch fits inside the concurrency window — genuinely by design, not a
bug or a bypassed cap. To make the waterfall visually legible (staggered
bars instead of one wide simultaneous block), set
`PARQUET_GCS_IO_WORKERS=10` for a demo run — `in_flight_limit=12`, and the
rendered waterfall shows real staggered start times and
`max_concurrent_downloads` drops from ~92 to ~26 (some overlap from
in-flight refill as items complete). No code changed for this one — it's an
existing, already-environment-configurable knob.

**"Operators don't run, query answered by magic" — a real, now-fixed gap.**
For a plan shaped `Source (predicate baked in) → Sink (TopN/Sort)` — which
is what a GCS scan with a pushed `WHERE` and an `ORDER BY ... LIMIT`
compiles to — the Operator Execution Waterfall was **completely blank**,
because `TC_OP_EXEC` (the only pipeline-stage probe wired through Phase 1)
only fires inside the loop over `Pipeline::operators`, which is empty for
this shape. The chart wasn't lying, but a blank chart for a real,
21-million-row, 6.7-second query reads exactly as broken. Fixed by wiring
the two remaining pipeline-stage categories that were already reserved but
unused:
- `TC_SOURCE_PULL` around `Source::get_morsel()` in `executor.hpp`'s worker
  loop.
- `TC_SINK` around `Sink::sink()` in the same file's `push()` lambda.

Both follow the exact same bracket-the-existing-timing-code pattern as
`TC_OP_EXEC` (Phase 1) — no new clock reads, just recording the endpoints
the `exec_ns`/`cpu_ns` accumulators already compute. `dev/io_waterfall/span_reader.py`'s
`exec_timelines()`/`operator_profiles()` widened from "TC_OP_EXEC only" to
all three categories, labeling each row with its role (`identity [source]`
/ `identity [operator]` / `identity [sink]`) since a Source and a Sink can
now share screen space with genuine Operators. Verified: the same GDELT
query went from 1 exec span (the whole "Operator Execution Waterfall" was
empty) to 206 real spans across source/sink activity, visibly spread across
the query's full 6.7s wall time. `make q` 197/197 after.

## 9d. Bug found and fixed: `corr_id` collided across `ParquetIOPipeline` instances (2026-07-23)

Rendering a real trace persisted by `worker.opteryx` (two files,
`trace.spans.bin` + `trace.symbols.json` — see §9a) surfaced another real
correctness bug: `total_operations` (distinct `corr_id`s) was half of
`total_download_ops`/`total_decode_ops` — meaning multiple genuinely
distinct row-group gathers shared the same `corr_id`, and
`operation_timelines()`'s per-`corr_id` grouping (which does plain field
assignment, not append) silently kept only the last one, discarding the
rest. Confirmed by clustering spans into time windows: `corr_id` values 1-10
appeared in an early window, then 1-5 repeated in two later windows roughly
300-400ms apart — three separate passes, each restarting its own counter
from 1.

**Root cause:** `next_trace_corr_id_` (`rugo/src/parquet/io_pipeline.hpp`)
was a counter *member of the `ParquetIOPipeline` instance* — correct only if
a query opens exactly one pipeline instance. It doesn't always: this trace
shows a query opening (at least) three, each minting its own colliding
`corr_id` sequence.

**Fix:** moved `corr_id` minting to the same query-wide bridge state
`query_seq` and the file-intern table already use —
`draken_trace_next_corr_id()` (`draken/core/trace_bridge_c.h` /
`draken/core/trace.hpp`'s `g_trace_next_corr_id`), reset alongside
`query_seq` on `trace_start_query()`. `io_pipeline.hpp` now calls this
instead of a local counter; the per-pipeline `next_trace_corr_id_` member is
gone. Same "one shared home in the bridge, not a per-object counter"
pattern §9's cross-.so fix already established — this is the same category
of mistake (state assumed to be query-scoped that was actually
object-scoped) one level up.

**Verified**: same-shape check as §9b — `total_operations ==
total_download_ops == total_decode_ops == telemetry's row_groups_read`
(90 = 90 = 90 = 90) on a real query, confirming exact 1:1 correlation with
no silent overwrites. `make q` 197/197 after. Not verified: a live repro of
the exact multi-pipeline-instance scenario the prod trace exhibited (why a
single query opened 3 pipeline instances ~300-400ms apart is itself
unexplained — worth investigating separately, but orthogonal to the
correlation-id fix, which is correct regardless of why that happens).

## 10. Settled decisions

- Gate: **runtime, env-driven** (`OPTERYX_TRACE=1`). No compile-out.
- Overflow: **truncate + warn** via `add_message`; never fail the query.
- Correlation granularity: **per row group**.
- Overhead budget: **≤ 5%** trace-on; ~0% trace-off.
- Clock: **single shared monotonic epoch**, captured at query start, engine and
  rugo unified onto it.
