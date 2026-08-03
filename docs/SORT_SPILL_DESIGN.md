# External Sort — Spill to Disk / GCS

**Status: DESIGN — NOT APPROVED, NOT IMPLEMENTED.**
Nothing in this document has been built. It exists to be argued with. Every
section marked **[D-n]** is a decision the architect owns; §12 collects them.

Scope: the **SORT** breaker only (`SortSink` in
[`native_sort.hpp`](../src/cpp/engine/native_sort.hpp)). Grouped/hashed
aggregation has the same unbounded posture and is explicitly **out of scope** —
see §11.

Motivating workload: a proposed `OPTIMIZE TABLE <dataset> [DEDUPLICATE BY (cols)]`
that forces a fully-sorted rewrite. Largest known production dataset is ~26 GB
physical — larger than the 10 GB ephemeral disk attachable to Cloud Run, so local
disk alone cannot be the answer.

---

## 1. What exists today (verified, 2026-08-03)

| Fact | Source |
|---|---|
| `SortSink::sink()` appends every morsel to a per-worker `std::vector<MorselPtr>`; `combine()` moves them under one mutex; `finalize()` sorts everything. No bound, no spill. | [`native_sort.hpp:82-111`](../src/cpp/engine/native_sort.hpp) |
| `TopNSink` is the only bounded variant (`ORDER BY … LIMIT n`), via periodic `compact()` to top-N. | [`native_sort.hpp:124-172`](../src/cpp/engine/native_sort.hpp) |
| No spill machinery exists anywhere in the tree. Two prior designs record it as *deliberately deferred*. | [`M4_SEGMENT_SCHEDULER_SHUFFLE_DESIGN.md:178`](M4_SEGMENT_SCHEDULER_SHUFFLE_DESIGN.md), [`DUCKDB_MATURITY_GAP_OVERVIEW.md:688`](DUCKDB_MATURITY_GAP_OVERVIEW.md) |
| There is **no memory budget and no admission control** anywhere in the engine. | same |
| `SortSink`/`TopNSink`/`WindowSink` are pure C++ — `<algorithm>/<atomic>/<mutex>/<vector>` + `operator.hpp`, `pipeline_buffers.hpp`, `morsels/sort.hpp`. Zero Python C-API, zero I/O. | file header |
| `HttpClient` is **read-only**: `get`/`head`/`get_many`/`head_many`. Only `CURLOPT_WRITEFUNCTION`/`HEADERFUNCTION` are wired. No PUT, no POST, no DELETE. | [`http_client.hpp:110-217`](../src/cpp/http_client.hpp), `http_client.cpp` |
| `native_sort.hpp` does not include `http_client.hpp` — separate translation unit. | verified |
| The native scan reaches GCS with **no auth header at all** — Python mints a V4 **signed GET URL** (`method="GET"` hard-coded) and hands the URL down. | [`gcs_filesystem.py:356-403`](../opteryx/connectors/io_systems/gcs_filesystem.py), [`io_pipeline.hpp:1490`](../rugo/src/parquet/io_pipeline.hpp) |
| The KV store (`opteryx/managers/kvstores/`) is **entirely Python** — no `.pyx`/`.pxd`. | verified |
| `KVSTORE_LOCATION`/`KVSTORE_LAYERS` is documented as the per-query shuffle/spill store and has **no first-party spill caller**. Its only live consumers are the manifest and footer caches, which are configured through their **own** separate settings. | [`config.py:160-166, 355-393`](../opteryx/config.py) |
| The KV store has **no TTL**: `touch()` is a documented no-op, `GCSKeyValueStore.set()` takes only key/value. | [`base_kv_store.py:93`](../opteryx/managers/kvstores/base_kv_store.py), [`gcs_kv_store.py:70-73`](../opteryx/managers/kvstores/gcs_kv_store.py) |
| `ColumnStatistics.total_bytes: Optional[int]` landed — **estimated total uncompressed on-disk bytes at this plan node**, rescaled through the plan by `StatisticsRefreshVisitor`, `None` when unavailable, "never fabricated". | [`statistics.py:136-149`](../opteryx/planner/optimizer/statistics.py) |
| `cgroup_memory_limit_bytes()` exists natively and surfaces as the `memory_limit_bytes` SERVER variable — but returns 0 when undetected, and the code comments note gVisor sandboxes (Cloud Run's **default** execution environment) do not reliably expose it. | [`platform.pyx:63`](../opteryx/compiled/platform.pyx), [`variables.py:289-294`](../opteryx/variables.py) |
| The Cloud Run container filesystem is **RAM-backed** unless a real volume is mounted — which is why `MANIFEST_CACHE_PATH` has no default. | [`config.py:345-350`](../opteryx/config.py) |

### 1.1 The vector model, as it actually is

The §11 struct in `.claude/CLAUDE.md` is missing a field. `buffers.h` is
authoritative and carries `uint8_t flags` at offset 36 (layout hints:
`DRAKEN_SEL_IDENTITY`, `DRAKEN_SEL_PERMUTATION`, `DRAKEN_DICT_KEYS_SORTED`,
`DRAKEN_DICT_CODES_DENSE`, `DRAKEN_ROW_SORTED`, `DRAKEN_ROW_SORTED_DESC`).
`sizeof(DrakenVector) == 40`, statically asserted. *(Flagged, not fixed — doc
edits are out of scope for this task.)*

Ownership lives in `VectorOwner` ([`vector_owner.h`](../draken/core/vector_owner.h)):
`data_buf`, `validity_buf`, `codes_buf` (dict only), `arena_buf` (non-inline
strings only), `logical_type` (**borrowed** pointer into a process-global interned
registry), `child_owner` (ARRAY only, recursive), `keyhash_buf` (E37, derived).

Two properties decide the serialization format in §4:

1. **String slots are position-independent.** A long slot stores a `uint32_t
   arena_offset`, never an absolute pointer
   ([`string_slot.h:55, 117`](../draken/core/string_slot.h)). Slots and arena
   are therefore byte-for-byte relocatable.
2. **`logical_type` is a pointer and must not be written as one.** It is
   borrowed from `logical_type_intern()`'s process-global registry
   ([`logical_type.h:108`](../draken/logical_type.h)). The struct behind it is a
   5-field POD (`kind`, `unit`, `offset_minutes`, `precision`, `scale`). It is
   **mandatory** for `DRAKEN_TIMESTAMP64` — a timestamp vector with
   `logical_type == nullptr` is a hard error — and carries DECIMAL precision/scale.
   A spill round-trip that loses it silently corrupts every DECIMAL and TIMESTAMP
   column.

---

## 2. Resolving the native-vs-Python-KV-store tension

This is the first thing to settle, because it determines whether we are adding a
component or duplicating one.

**The claim that we would be duplicating the KV store does not survive contact
with the code.** `KVSTORE_LOCATION` is documented as the per-query shuffle/spill
store. It has **zero first-party callers for that purpose**. Its two real
consumers — `manifest_disk_cache.py:326` and
`parquet_io/footer_remote_cache.py:233` — are content-addressed, long-lived,
cross-query caches, and both are pointed at their own settings
(`OPTERYX_MANIFEST_CACHE_LOCATION`, `OPTERYX_FOOTER_CACHE_LOCATION`) whose
docstrings go out of their way to say they are *deliberately not*
`KVSTORE_LOCATION`, "which is the per-query spill store".

So `KVSTORE_LOCATION` + `KVSTORE_LAYERS` + the spill-shaped `ScopedKeyValueStore`
default is **an abstraction built for a caller that never arrived**. Contract §2
forbids duplicating logic between Python and native "unless explicitly
requested"; it does not require us to route execution through an unused Python
component in order to avoid the appearance of duplication. Contract §1 and §2 are
unambiguous that Python must not be on the execution path — a native `SortSink`
worker calling `GCSKeyValueStore.set()` per run means re-acquiring the GIL inside
the sort, per run, on every worker thread.

**Recommendation:** the native spill store is the **sole** implementation for the
execution path. The Python KV store is **not modified and not used by it** — its
two cache consumers keep working untouched, through their own settings.

That leaves `KVSTORE_LOCATION`/`KVSTORE_LAYERS` provably dead once this lands.
Contract §1: "Dead code rots the system from the inside — cut it out as soon as
it's found." But contract §8 forbids refactoring beyond scope. These pull in
opposite directions, so it is the architect's call, not mine — **[D-1]**.

I am *not* proposing we delete it as part of this work. I am proposing we stop
pretending it is the spill store.

---

## 3. Architecture — run generation and k-way merge

### 3.1 Shape

The existing sink already has the right skeleton: per-worker lock-free
accumulation → `combine` → `finalize`. Spill slots into it without inverting it.

```
sink()      per worker: append morsel to local buffer; add to the byte counter.
            counter over threshold ->  SPILL THIS WORKER'S LOCAL BUFFER:
                                         sort it (existing sort_and_emit path)
                                         write it out as ONE sorted run
                                         clear the local buffer, reset its share
combine()   worker publishes (a) residual in-memory morsels and
                              (b) its list of SpilledRun descriptors
finalize()  runs == 0  ->  EXACTLY TODAY'S PATH. Byte-identical.
            runs  > 0  ->  sort the residual into one final in-memory run,
                           then k-way merge all runs.
```

The zero-spill case is not "fast because we tuned it" — it is *structurally the
same code*. That is the strongest available guarantee against the "don't be
stupid" failure mode.

### 3.2 Run generation

A run is produced by the code that already exists: `sort_and_emit(local.morsels,
spec, SIZE_MAX, chunk_rows, &tmp, err)`. Its output is a vector of sorted morsels
chunked at `chunk_rows` (131072). Those chunks are the run's **blocks** — the
run is written as a sequence of serialized blocks with a block-offset table in a
footer.

Blocks are what make the merge memory-bounded, and they are what makes the GCS
read side free: fetching block *i* of a run is a byte-range GET, which
`HttpClient::get()` already does, and fetching the next block of *every* run at
once is `get_many()`, which already exists and already coalesces.

**The read side of GCS spill requires no new native capability whatsoever. Only
the write side does.**

### 3.3 The merge

A loser tree (or binary heap) over R run cursors. Each cursor holds one decoded
block plus one read-ahead block. Peak merge memory:

```
merge_bytes  ≈  R × block_bytes × (1 + readahead)
```

independent of run *size*. With `chunk_rows = 131072` and a wide-ish row, a block
is order 10–50 MB, so a fanout of R = 32 costs ~1–3 GB with readahead 1. Fanout
is therefore derived from the budget, not hard-coded:
`max_fanout = floor(budget × merge_share / (block_bytes × (1 + readahead)))`.

If `R > max_fanout`, merge in multiple passes (merge groups of `max_fanout` runs
into fewer, larger runs, repeat). Each extra pass is a full re-read + re-write of
the dataset — on GCS at the observed ~64 MB/s per-instance ceiling
(see `cloudrun_network_bandwidth_cap_hypothesis`), a second pass over 26 GB is
~13 minutes. So fanout should be pushed as high as the budget allows, and
`merge_passes` must be a first-class telemetry counter (§9). **[D-6]** covers
whether a large `chunk_rows` reduction for spilling sorts is acceptable to buy
fanout.

**Comparison in the merge reuses draken's key machinery unchanged.** A cursor
block is a `MorselPtr`, so `build_sort_keys(block, spec, …)` produces
`SortKeyColumn`s and `SortKeyCmp` compares them — the *same* comparator that
defines the ordering for the in-memory path. There is exactly one definition of
the order. A merge with its own comparator would be a second definition and is
forbidden.

### 3.4 The output is also unbounded — and this is the biggest hole

`SortSink::finalize()` writes into a `MorselBuffer`, and `BufferSource` hands
those morsels to the dependent pipeline
([`pipeline_buffers.hpp:19-72`](../src/cpp/engine/pipeline_buffers.hpp)).
`MorselBuffer` holds a `std::vector<MorselPtr>` of **the entire result**.

Spilling the *input* to a 26 GB sort and then materializing all 26 GB of *output*
into a `MorselBuffer` does not fit in memory either. **Input spill alone does not
make `OPTIMIZE TABLE` work.**

The fix is to make the merge lazy: replace the buffer hand-off with a
`MergeSource` that advances the loser tree on each `get_morsel()` call and emits
one morsel at a time. This fits the *spirit* of the `Sink` contract — the header
already says "The result is exposed as a Source for the dependent pipeline" — but
it does not fit the current *wiring*, where `set_sort_sink` is handed a
`MorselBuffer` index.

Consequences to accept:

- `MergeSource` produces one totally-ordered stream, so it is inherently **DOP 1**
  downstream. `WindowNode` already runs at DOP 1 for an order-preservation
  reason, so there is precedent, but it means a spilling sort loses downstream
  parallelism. For `OPTIMIZE TABLE` the consumer is a Parquet writer, which wants
  a single ordered stream anyway.
- The merge now runs on the *consuming* pipeline's thread rather than in
  `finalize()`. That is a change to when the work happens, and it is visible in
  the trace waterfall.

This is **[D-2]** and I regard it as the decision that determines whether this
workstream delivers `OPTIMIZE TABLE` or only "sorts don't crash any more".

### 3.5 Where the byte counter lives

Two options:

- **Per-worker share** (`budget / dop`), lock-free, matching the "hot-path
  mutation touches only local state" rule. Cheapest. But sort input is not
  guaranteed evenly distributed across workers, so a skewed input makes one
  worker spill while the others sit far under budget — spilling when we did not
  need to, which is exactly the failure this design is supposed to avoid.
- **One shared `std::atomic<uint64_t>`**, one relaxed `fetch_add` per morsel.
  Per-*morsel*, not per-row — the same granularity `OpStats` already uses for its
  atomics, so it is a precedented cost, and it is amortised over ~131072 rows.
  Honest global accounting; a worker spills when the *query* is over budget.

**Recommendation: the shared atomic.** The precision is worth one uncontended
atomic add per morsel. **[D-5]**.

`cxx_morsel_nbytes()` already exists and is already array-aware and
dict-code-aware ([`cxx_morsel.h:85`](../draken/morsels/cxx_morsel.h)) — this is
the honest footprint number, and `buffers.h` explicitly notes it was fixed
because the old undercount "deferred spill/flush past the safe point". It is
O(columns) per morsel, not O(rows).

---

## 4. Serialization format for spilled runs

### 4.1 Rejected: Parquet (rugo writer)

Parquet is a *durable, portable, self-describing* format. A spilled run is
ephemeral, same-process, same-build, and read back exactly once. We would pay
dictionary construction, page assembly, compression, and a three-representation
round trip (`DrakenVector` → writer model → Parquet → `DecodedColumn` → IPC →
`DrakenVector`) for portability we do not want. It also does not preserve the
draken encoding shape or `flags` — a dict-encoded column would be re-derived, not
restored, and `DRAKEN_ROW_SORTED` (which the sort itself sets on its own output)
would be lost.

### 4.2 Rejected: the existing rugo IPC format

[`rugo/src/parquet/ipc_serialize.hpp`](../rugo/src/parquet/ipc_serialize.hpp) is a
real, well-built binary format — but it serializes `DecodedColumn`, a
**Parquet-decode-shaped** struct, not a `DrakenVector`. Writing a draken vector
into it means converting German-string slots back into arena+offsets and
re-deriving dict codes. Worse, its *deserializer* is split: the fast C++ path
([`ipc_deserialize.hpp`](../src/cpp/ipc_deserialize.hpp)) handles only tags 1–5
and 12; every dict, string, array and exact-width-integer tag returns
`kStatusNotHandled` and falls back to **Cython** (`column_deserializer.pyx`).
Routing spill read-back through that would put Python on the execution path for
every string column.

### 4.3 Recommended: a purpose-built raw draken block format

Because slots carry arena *offsets* and every owned buffer is a flat allocation,
a block is very nearly a `memcpy` of `VectorOwner`'s buffers.

Per block: a small header (`row_count`, `column_count`, names), then per column:

```
  DrakenType type
  uint8_t    flags                 // carried verbatim — hints must survive
  uint32_t   length, data_length
  LogicalType lt_present + POD     // {kind, unit, offset_minutes, precision, scale}
                                   //   re-interned via logical_type_intern() on read
  validity   [ (length+7)/8 bytes ]  or absent
  payload    per family, below
  selection  present iff the vector is dict-shaped (owned codes)
```

Per family:

- **Fixed-width** — `data_length × draken_type_fixed_itemsize(type)` bytes, verbatim.
- **BOOL** — `(data_length+7)/8` bytes, verbatim.
- **String family (`VARCHAR`/`NVARCHAR`/`VARBINARY`/`VARIANT`)** — `data` points at
  a `DrakenStringArena` *struct*, whose `slots` and `arena` members **are absolute
  pointers**. Write the struct's scalar fields (`length`, `arena_used`,
  `arena_cap`, `payloads_elided`, `type`), then the slot block
  (`length × 16`), then `arena_used` arena bytes. On read: allocate, `memcpy`,
  rebuild the two struct pointers. `payloads_elided` must round-trip — a
  length-only column has a NULL arena and slots stamped with
  `STR_ELIDED_PAYLOAD_OFFSET`, and losing the flag turns a trap value into a
  4 GB out-of-bounds read.
- **ARRAY** — the child hangs off `VectorOwner::child_owner` and nests
  recursively. Either a recursive block, or declared unsupported. See **[D-4]**.

Explicitly **dropped, not carried**: `keyhash_buf`. It is a derived E37 cache and
its own header states "Presence == validity: any op that does not explicitly
propagate it yields nullptr, and the consumer falls back to recomputing". A
read-back vector with `keyhash_buf == nullptr` is correct by construction.

### 4.4 The §11 question this format raises

The writer must know **whether `selection` is owned** — for a dense vector it
points at the shared global `draken_identity_sel` and for a constant vector at
`draken_zero_sel`, neither of which is ours and neither of which should be
written out (writing an identity permutation for a 26 GB sort would be pure
waste; writing the pointer would be a bug).

That is an inspection of encoding shape. §11 says shape-specialized dispatch
needs architect approval, and I am not going to smuggle it in on the grounds that
it is "only ownership". **[D-3]**.

My argument that it is admissible: this is a *memory-ownership* question, not an
answer-changing one. `draken_vector_nbytes()` already does exactly this
inspection (`draken_is_dict(v)` → count the codes) for exactly this reason, in
`buffers.h` itself. And the correctness bar §11 sets is met absolutely: the
read-back vector is **bit-identical** to the written one — same `data`, same
`selection` semantics, same `flags`, same `data_length`. There is no path where a
shape discriminant changes the answer, because both branches reconstruct the same
vector.

The round-trip must be property-tested against all three shapes (dense, constant,
dict) for every supported type, and a shape-preservation assertion belongs in the
test, not just a value comparison.

### 4.5 Compression

None for local disk — CPU cost exceeds local-disk bandwidth.

For the GCS tier, at the observed ~64 MB/s per-instance ceiling, bandwidth is the
binding constraint and compression is close to free in wall-clock terms. zstd is
already vendored and already linked (`HAVE_ZSTD` is set in `setup.py` for the
parquet path), so this needs no new dependency. Recommend zstd level 1, per
block, GCS tier only. It is a knob, so it is **[D-7]**.

---

## 5. Spill target tiering

**Recommended: local disk first, GCS on overflow.** Local disk is roughly an
order of magnitude faster, costs nothing, and needs no auth. GCS is the only
option past the ephemeral-disk ceiling.

But there is a trap that must gate the local tier:

> The Cloud Run container filesystem is **RAM-backed** unless a real volume is
> mounted. `config.py:345-350` already says this, which is why
> `MANIFEST_CACHE_PATH` deliberately has no default.

Spilling to a RAM-backed tmpfs consumes the exact memory the spill exists to
free — it would turn an OOM into a *slower* OOM, which is worse than failing.

So the local tier is gated on an explicitly configured path with **no default**,
exactly as the manifest cache already is:

```
OPTERYX_SPILL_PATH            (no default; empty = local tier disabled)
OPTERYX_SPILL_LOCAL_BYTES     (ceiling for the local tier)
OPTERYX_SPILL_REMOTE_LOCATION (gs://bucket/prefix; empty = remote tier disabled)
```

The documented Cloud Run split (10 GB ephemeral ≈ 1 GB cache + 9 GB spill) is
consistent with `MANIFEST_CACHE_BYTES`'s 1 GB default.

**If neither tier is configured and the sort exceeds budget, it fails loud** with
an error naming the two settings. It does not silently degrade, and it does not
quietly succeed by ignoring the budget. Contract §1.

Run sizing: the spill unit is the local buffer at threshold, so runs land
naturally in the tens-to-hundreds of MB — well inside GCS's "large sequential"
sweet spot and far from its small-random-I/O penalty.

---

## 6. The native GCS write path

### 6.1 Where it goes

**Extend `HttpClient`** with `put()` / `put_many()` / `del_many()` rather than
building a sibling class. It already owns the connection pool, the CA-bundle
probe, the retry/backoff policy, the `HttpTuning` per-call override model, and
the CURLM batching idiom. A second client would duplicate all of it.
`CURLOPT_UPLOAD` + `CURLOPT_READFUNCTION` is the mechanical change.

`native_sort.hpp` then gains `#include "http_client.hpp"` — a new edge, since
today the sort engine has no I/O at all. Preferably the sort includes a narrow
`spill_store.hpp` which owns the HTTP and POSIX file details, so `SortSink`
depends on "a place to put bytes", not on libcurl.

PUT retry is safe: an object PUT to a fixed key is idempotent (last write wins),
so the existing transient-retry policy applies unchanged.

### 6.2 Auth — the genuinely open problem

The established pattern for native GCS access is **Python mints a V4 signed URL,
native uses it with no Authorization header**. `rewrite_to_signed_url()` already
does this; `method="GET"` is hard-coded and a PUT variant is a parameter change.

The problem: **the number of runs is not known at plan time.** Options:

1. **Pre-mint a pool of signed PUT URLs** for deterministic keys
   (`spill/{query_id}/{operator_id}/{worker}/{seq}`), sized from the plan-time
   byte estimate with headroom, and **fail loud on exhaustion**. No Python
   re-entry, ever. But it fails a query whose estimate was too low — and §7 is
   built on the premise that estimates are wrong.
2. **Pass a bearer token down by value** at plan time; native sets
   `Authorization: Bearer …`. Unlimited objects. But GCP access tokens live ~1
   hour, and a 26 GB external sort can plausibly exceed that. On expiry the spill
   fails mid-query with a 401.
3. **Native token refresh from the GCE/Cloud Run metadata server.** The workload-
   identity token endpoint is a plain HTTP GET to
   `metadata.google.internal`, and draken already has a JSON parser
   (`draken/ops/json_extract.h`). No Python, no new dependency, no expiry
   ceiling. Does not cover local service-account-key development — but local
   development spills to local disk.

**Recommendation: (2) as the shippable step, (3) as the target.** (2) is small
and unblocks everything; its one-hour ceiling should be stated in the docs rather
than discovered in production. (3) is the only option with no artificial limit
and is fully contract-compliant. **[D-8]** — and note that (3) is me proposing
the engine learn to authenticate itself, which is squarely an architect call.

---

## 7. Trigger policy

**Recommended: runtime-adaptive. Plan-time estimates provision, they never
trigger.**

### Why not plan-time

`ColumnStatistics.total_bytes` is the only byte signal available and it is
unsuitable as a trigger for three independent reasons, all from its own docstring
and the code:

1. It is **estimated uncompressed on-disk** bytes, not in-memory draken
   footprint. Dict encoding shrinks the in-memory form; German-string slots
   (16 B each) inflate it. The two numbers are not interchangeable.
2. It is `None` whenever no signal is available — "never fabricated". A
   plan-time-only trigger has no answer at all for those columns.
3. It is rescaled through the plan by `StatisticsRefreshVisitor` using
   *selectivity guesses* at every Filter/Join/Limit. By the time it reaches a
   Sort above a join it is an estimate of an estimate.

Plan-time-only therefore risks both false positives (spilling a sort that fits,
paying GCS for nothing) and false negatives (OOM anyway).

### Why adaptive is cheap here

The usual objection to adaptive spill — "converting an in-flight in-memory
accumulation into a spilling one is complicated" — does not apply to this design.
The conversion *is* "sort what you have, write it, clear the buffer", which is
the same `sort_and_emit` call the non-spilling path already makes. There is no
second state machine and no mode flag on the hot path; there is a counter and a
threshold.

### What plan-time IS used for

- **Provisioning**: pre-mint signed URLs / open the local file / size the pool
  before execution starts.
- **Early failure**: if the estimate says spill is likely and *no spill tier is
  configured*, fail at plan time with an actionable message, rather than 20
  minutes into a scan.

Both are decisions Python makes at planning time and passes down as parameters —
"Python decides the work, native does it".

### `OPTIMIZE TABLE`

For `OPTIMIZE TABLE` the byte size comes from the manifest —
`FileEntry.uncompressed_size_in_bytes`
([`file_entry.py:29`](../opteryx/models/file_entry.py)) — which is a *measured*
number rather than a selectivity-scaled estimate, so provisioning is accurate.
Two caveats worth stating rather than assuming: the field is `Optional[int]` and
is `None` for files written before it was recorded (`parquet_writer.py:63` still
passes `None`), and it is on-disk uncompressed bytes, not in-memory draken
footprint. The *trigger* therefore stays the runtime counter — there is no reason
for a second mechanism.

---

## 8. Memory budget

There is no budget in the engine today. Proposed:

```
OPTERYX_MEMORY_BUDGET_BYTES   explicit override; wins outright
   else  memory_limit_bytes  × OPTERYX_MEMORY_BUDGET_FRACTION   (cgroup ceiling)
   else  physical_memory_bytes × same fraction                  (bare metal / macOS)
   else  NO BUDGET -> no spill -> exactly today's behaviour
```

Fraction default 0.8 matches DuckDB's `memory_limit` convention, cited in
[`DUCKDB_MATURITY_GAP_OVERVIEW.md`](DUCKDB_MATURITY_GAP_OVERVIEW.md).

Two things to be honest about:

- **Detection can fail exactly where it matters.** `variables.py` records that
  gVisor sandboxes — Cloud Run's *default* execution environment — do not reliably
  expose `/proc/meminfo` or the cgroup limit, and both detectors return 0/None on
  failure. So the explicit override is not a nicety; on the production target it
  may be the only working source. The `OPTIMIZE TABLE` path should pass an
  explicit budget rather than rely on detection.
- **This is a sort-operator budget, not a buffer manager.** One number, handed to
  one operator. It does not account for the scan's in-flight buffers, the join
  build side, or concurrent queries in the same process. A real query-wide memory
  manager is a much larger workstream (DuckDB gap #5 in full) and is not proposed
  here. Whether a single-operator budget is worth having on its own, or whether
  this should wait for a global manager, is **[D-9]**.

---

## 9. Observability

Spilling must never be invisible.

**Runtime counters.** New atomics on the sink: `spill_runs`,
`spill_bytes_written`, `spill_bytes_read`, `spill_write_ns`, `spill_read_ns`,
`merge_passes`, `spill_tier` (none/local/remote/both), `peak_resident_bytes`.

These should **not** be added to `OpStats`. `OpStats` is carried by every source,
operator and sink, and `OpReading` is a fixed struct read across the Cython edge
(`engine.hpp:203`); widening it puts eight sort-only fields on every operator in
every query. Recommend a separate `collect_spill_stats()` harvest alongside
`collect_op_stats()`. **[D-10]**.

**EXPLAIN** (plan time) — the Sort node's `config` string states the provisioned
posture and labels the estimate as an estimate:

```
Sort (est. 26.4GB vs budget 3.2GB; spill armed: local=/mnt/spill, remote=gs://…/spill)
```

**EXPLAIN ANALYZE** (post-run) — actual runs, bytes per tier, merge passes,
time split between sort and spill I/O. A query that quietly started going to GCS
shows up as a large `spill_read_ns` against a small `exec_ns`.

**Trace spans.** Per the existing waterfall design
([`EXECUTION_TRACING_DESIGN.md`](EXECUTION_TRACING_DESIGN.md)), each run write and
each merge-pass block read emits a span, so GCS spill is visible as I/O in the
waterfall rather than as an unexplained slow sort.

**Not proposed: a log line on first spill.** "No hidden behaviour" is satisfied
by EXPLAIN + telemetry + trace; a per-query warning is log noise on a workload
where spilling is the expected, configured behaviour. If the architect reads the
contract as requiring an explicit runtime signal, say so — **[D-11]**.

---

## 10. Failure and cleanup

**Local disk — guaranteed, by the kernel.** Create each run file under
`<SPILL_PATH>/<query_id>/`, then `unlink()` it immediately while holding the fd.
The data stays reachable through the descriptor and is reclaimed automatically
when the process exits, however it exits — normal completion, exception,
cancellation, `SIGSEGV`, or OOM-kill. No cleanup code, no orphan sweeper, no
crash path to get wrong. Costs: the files cannot be inspected for debugging, and
the merge must hold R descriptors open, so `max_fanout` interacts with `RLIMIT_NOFILE`
and must be clamped against it.

**GCS — best-effort, backstopped by infrastructure.** There is no unlink-on-open.
On normal completion and on the engine's error/cancel path, native issues batched
DELETEs. But a hard crash — and there is a known production `SIGSEGV`
(`prod_sigsegv_github_events_hashed_agg`) — leaves orphaned objects paying
storage.

Application-level TTL is **not available**: `touch()` is a documented no-op and
`GCSKeyValueStore.set()` takes no lifecycle argument. So reclamation correctness
must rest on a **bucket lifecycle policy**.

> **Assumed infrastructure, to be provisioned by the architect:** the spill
> bucket/prefix has an age-based lifecycle rule deleting objects older than
> **1 day**. The design assumes this exists; it does not implement a substitute.
> Sizing that window is a cost/safety trade-off (shorter risks deleting a live
> long-running query's runs) — **[D-12]**.

**Cancellation.** The pipeline already carries `std::atomic<bool>* halt`
(`operator.hpp:119`) and rugo carries its own `cancelled_`. The spill store needs
to observe cancellation so an abandoned query stops writing runs and triggers
cleanup, rather than finishing a 26 GB spill nobody will read.

**Partial writes.** A PUT either lands whole or does not land; a truncated local
write is caught by the block-offset footer, which is written last. A run whose
footer is missing is a hard error, never a silently-short run.

---

## 11. Scope boundary

**This design covers the SORT breaker only.** It is not extended to grouped or
hashed aggregation, and it should not be read as covering them.

What is deliberately built to be reusable:

- the **spill store** (tiered write/read/delete of opaque byte runs)
- the **draken block serialization** (§4)
- the **memory budget and accounting** (§8)

What is sort-specific and does **not** generalize:

- run generation via `sort_and_emit`, the k-way merge, the merge source

Grouped aggregation's release valve is a different mechanism entirely — the
already-agreed direction in
[`M4_SEGMENT_SCHEDULER_SHUFFLE_DESIGN.md:178`](M4_SEGMENT_SCHEDULER_SHUFFLE_DESIGN.md)
is to spill by **raising radix bits inside the sink** (more, smaller partitions,
processed one at a time), which is Grace-style partitioning, not run-and-merge.
That design would consume the store and the serialization from here, and none of
the strategy. `ARRAY_AGG`/`MEDIAN`-style unbounded per-group lists
(`native_group_sinks.hpp:316-318`, capped at `aa_max_per_group = 1000` and
failing loud) are a third shape again. **None of that is designed here.**

---

## 12. OPEN DECISIONS FOR THE ARCHITECT

| # | Decision | Recommendation | Trade-off / why it is yours |
|---|---|---|---|
| **D-1** | What happens to `KVSTORE_LOCATION` / `KVSTORE_LAYERS` / spill-scoped `ScopedKeyValueStore`, which this design leaves provably dead. | Land the native store now; schedule the removal as a **separate** audited change. | §1 says cut dead code out; §8 says don't refactor beyond scope. They conflict. Deleting also removes a passing test (`test_kvstores_valkey.py:76`). I will not delete it unasked. |
| **D-2** | **Lazy `MergeSource` vs today's `MorselBuffer` hand-off.** | Build `MergeSource`. | Without it, the *output* of a 26 GB sort is fully materialized and `OPTIMIZE TABLE` still cannot run — input spill alone buys only "doesn't crash". Cost: changes the breaker→pipeline wiring, and forces DOP 1 downstream of a spilling sort. This is the decision that sets what the workstream delivers. |
| **D-3** | §11 approval: the spill writer inspects encoding shape to decide **buffer ownership** (is `selection` owned codes, or a shared global?). | Approve, scoped narrowly to serialization. | It is unavoidable — the alternative is writing a stale pointer or a 26 GB identity permutation. `draken_vector_nbytes()` already does the same inspection for the same reason. But §11 says ask, so I am asking. Round-trip is bit-identical; no branch can change an answer. |
| **D-4** | `DRAKEN_ARRAY` columns as spill **payload** (ARRAY is never a sort key, but `OPTIMIZE TABLE` rewrites whole datasets, so it will meet them). | Support it — recursive child block. | Failing loud is contract-clean but makes `OPTIMIZE TABLE` unusable on any array-bearing dataset. Supporting it is real work (nested `child_owner` subtree). Note VARIANT is fine either way: it is string-shaped storage (`draken_type_is_string_storage`) and round-trips as an arena, even though it can never be a key. |
| **D-5** | Byte accounting: shared atomic vs per-worker share. | Shared `std::atomic`, one relaxed add per **morsel**. | Per-worker is lock-free but spills spuriously on skewed input — the exact "don't be stupid" failure. The atomic is per-morsel, the same granularity `OpStats` already pays. |
| **D-6** | `chunk_rows` (currently 131072) for spilling sorts, since block size sets merge fanout. | Make it configurable for the spilling path; do not change the in-memory default. | Smaller blocks buy fanout (fewer full re-read/re-write passes: one extra pass over 26 GB on GCS is ~13 min) but cost per-block overhead and more GCS requests. |
| **D-7** | zstd compression on the GCS tier. | zstd level 1, GCS tier only, none for local. | zstd is already vendored and linked, so no new dependency. At the ~64 MB/s per-instance ceiling, bandwidth dominates CPU. It is still a new knob and a new format variant. |
| **D-8** | **GCS write auth.** (1) pre-minted signed PUT URL pool, (2) bearer token passed down at plan time, (3) native token refresh from the GCE metadata server. | (2) to ship, (3) as the target. | (1) fails loud when the estimate was low — and §7 exists because estimates are wrong. (2) is small but caps a spilling sort at the ~1h token lifetime. (3) has no ceiling and is fully native, but means the engine learns to authenticate itself — a real architectural addition, not an implementation detail. |
| **D-9** | Is a **single-operator** memory budget acceptable, or should spill wait for a query-wide memory manager? | Ship the single-operator budget. | It does not account for scan buffers, join build sides, or concurrent queries — so the sort can respect its budget while the process still OOMs. A real buffer manager is DuckDB gap #5 in full and is a much larger workstream. |
| **D-10** | Telemetry shape: widen `OpStats`/`OpReading`, or a separate `collect_spill_stats()`. | Separate harvest. | Widening puts eight sort-only fields on every operator in every query and changes a struct read across the Cython edge. A separate harvest is one more collection path. |
| **D-11** | Does "no hidden behaviour" require a **runtime log/warning** on first spill, beyond EXPLAIN + telemetry + trace? | No — telemetry and EXPLAIN are sufficient. | On a workload where spill is configured and expected, a per-query warning is noise. But the contract wording could reasonably be read the other way, and it is your contract. |
| **D-12** | **Infrastructure**: the spill bucket/prefix lifecycle policy this design assumes. | Age-based delete at **1 day**. | The KV store has no TTL and cannot grow one usefully, so app-level expiry is not an option; crash-orphaned objects are reclaimed *only* by this policy. Too short risks deleting a live long-running query's runs; too long pays storage for orphans. Bucket provisioning is yours, not mine — I only state the assumption. |

### Also flagged, not decisions

- `.claude/CLAUDE.md` §11's `DrakenVector` listing omits the `uint8_t flags`
  field that `buffers.h` defines at offset 36. The doc's own rule says
  "buffers.h is right and this section is stale — fix the doc." Not fixed here —
  out of scope, and I am not editing the contract unasked.
- The IPC deserializer's Cython fallback (`kStatusNotHandled` → Cython
  `column_deserializer.pyx` for every string/dict/array tag) is pre-existing
  Python-on-the-execution-path debt. This design routes around it rather than
  extending it; it is noted per §2 ("Call it out when found; do not extend it"),
  not fixed.
