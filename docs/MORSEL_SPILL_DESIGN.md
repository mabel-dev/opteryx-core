# Morsel Buffer — A Residency Contract, Spill Behind It

**Status: FIRST DELIVERY BUILT 2026-08-27 — see §15 for what landed, what was
corrected during the build, and what remains.** Sections marked **[D-n]** are
decisions the architect owns; §14 collects the ones still open.

**Purpose is OOM avoidance, not throughput.** Nothing here is expected to make a
query faster, and a spilling query will be markedly slower. The deliverable is
that a query which today reaches `std::terminate()` instead either completes, or
fails with a message naming the operator and the byte count.

**The unit of work is a contract, not a spill mechanism.** Operators accumulate
into a buffer and are told nothing about what it does. Whether anything reaches
disk, in what format, through what store — all of it is behind the interface.
This is the ruling the rest of the document is organised around (§2, §3).

Companion documents: [`SORT_SPILL_DESIGN.md`](SORT_SPILL_DESIGN.md) (external
sort — §2 of that document settles the native-vs-Python store argument and is not
re-argued here), [`SKENE_FILE_FORMAT_DESIGN.md`](SKENE_FILE_FORMAT_DESIGN.md)
(the serialization format, now an implementation detail of §7).

---

## 1. The structural finding this is built on

Every breaker in the engine already hands its result to the dependent pipeline
through **one struct**: `MorselBuffer`
([`pipeline_buffers.hpp:19`](../src/cpp/engine/pipeline_buffers.hpp)), a bare
`std::vector<MorselPtr>` written single-threaded in `finalize()` and read by
`BufferSource` via atomic claim. The engine owns them in one place —
`Engine::buffers` ([`engine.hpp:229`](../src/cpp/engine/engine.hpp)).

So the hand-off is already centralised. What is **not** centralised is
accumulation, which today happens in two places:

| | Where the bytes sit | Who owns it today |
|---|---|---|
| **Input** | The sink's retained morsels, held until `finalize()` | Private per-sink vectors: `SortLocal::morsels`, `WindowLocal`/`WindowGlobal::morsels` ([`native_sort.hpp:85, 287-288`](../src/cpp/engine/native_sort.hpp)), `FramedWindowLocal::morsels` ([`native_window_frame.hpp:92`](../src/cpp/engine/native_window_frame.hpp)), `BufferAppendSink::Local::morsels` ([`pipeline_buffers.hpp:37`](../src/cpp/engine/pipeline_buffers.hpp)), `Join2BuildLocal`/`Join2BuildGlobal::morsels` ([`native_join2.hpp:95, 253`](../src/cpp/engine/native_join2.hpp)) |
| **Output** | The `MorselBuffer` finalize writes into, held until the consumer drains it | `Engine::buffers` — **shared by every breaker**, including ones otherwise out of scope |

**The encapsulation ruling unifies these.** If residency is the buffer's
business, then a private `std::vector<MorselPtr>` inside a sink is an
*unmanaged* accumulation that the contract cannot see — a hole in the design,
not an optimisation. Every retained morsel pile becomes a buffer instance.

That is the single largest consequence of §2's ruling and it **merges what were
two delivery phases into one**: there is no "output first, input later", because
they become the same object used twice.

⛔ This still does not solve hash-table state. A group-by that OOMs while
*building* its table OOMs exactly as it does today; only its retained morsels
are bounded (§13.2).

---

## 2. Rulings carried in (architect, 2026-08-27)

| | Ruling |
|---|---|
| **Encapsulation** | **The buffer has a contract and is free to execute as it wishes.** Operators call it and do not know or care whether it spills, or what format or store it uses. |
| Trigger | **Budgeted.** Accumulate in memory; spill only once the budget is spent. An eight-row CTE never touches disk. |
| Budget size | **1GB ceiling; flush triggered at 512MB** (revised 2026-08-27 from ~0.8GB, to match the size of a typical skene-written file). The gap is headroom, not slack (§6.1). |
| Budget scope | **Per operator.** These are pipeline breakers, so two of them almost never accumulate concurrently. |
| Spill policy | **Flush all to storage** once the budget is spent — not a hybrid resident/spilled buffer. |
| Order of work | The morsel-pile operators first; **sort follows** (§10). |
| Format | **`.skene`, spill profile — no statistics, no value ordering, compression ON, one flush = one file.** Decided (§7). |
| Store | KV store, disk-backed; opaque key, value carries metadata alongside a reference to the data (§7). |

### 2.1 What the encapsulation ruling settles

Encapsulation is about **where a decision is visible**, not about leaving it
open. Skene is the decision (§7). What the ruling buys is that the decision sits
in one place: the metadata encoding, the unit layout, and the store's shape are
all invisible across the interface, so they are settled by being *correct* rather
than by being ratified, and any of them can change later without touching a
single operator.

That is the acceptance test, and it holds even though the format is pinned:
**changing what the buffer writes must require editing no sink.** If it would,
the contract has leaked.

### 2.2 Why flat 1GB, not the MEDIAN escalation shape

MEDIAN starts entitled to 256MB and doubles **on measured demand** to a 2GB
ceiling before refusing ([`agg_budgets.hpp`](../src/cpp/engine/agg_budgets.hpp)).
That ladder exists to avoid *refusing a working query*, refusal being the only
other outcome available to a buffering aggregate.

Spill removes that pressure. Past the budget the operator keeps working, so a
flat number costs latency and nothing else. The precedent does not transfer, and
the divergence is deliberate.

### 2.3 Why per-operator survives here, having been rejected for the aggregates

The aggregate budgets are separate per aggregate so a failure **names the
aggregate that overspent**, accepting that one query can hold 1.5GB. That is a
blame-attribution argument for a budget whose job is to refuse.

This budget's job is to evict, and the argument is different: breakers
serialize, so concurrent accumulation is rare by construction.

**[D-1] The residual exposure is that "rare" is not "never."** UNION ALL legs
append into one buffer concurrently; a shared CTE's buffer stays resident while a
downstream breaker accumulates. Catch these, or accept the over-commitment the
aggregates already accept?

---

## 3. The contract

This section is the deliverable. Everything after it is either what the contract
permits (§7) or what it forbids (§4).

**Write side.** Accumulation is multi-worker and lock-free today — per-worker
`Local` vectors, combined once under a mutex. The contract must preserve that
shape or it is a throughput regression on every non-spilling query, which is
every query the budget is not reached on:

- `writer()` — a per-worker append handle. Appends are lock-free. The handle's
  bytes accrue to the buffer's single operator-scoped accounting.
- `append(morsel)` on the handle. The buffer decides residency; the caller
  learns nothing.
- `commit(handle)` — the existing `combine()` point. Hands the worker's units to
  the buffer.
- `seal()` — the existing `finalize()` point. No more writes.

**Read side.** `BufferSource` hands morsels out by atomic claim. That survives
unchanged: a claim resolves to a unit, and **the claiming worker materializes
it**, so decode stays parallel and off any single thread. Resident and spilled
differ only in what the claim resolves to.

**[D-2] Per-unit operator metadata.** The write handle accepts an opaque blob
per committed unit, returned with that unit on read. The buffer never interprets
it. This exists for exactly one caller — sort, which needs per-run MIN/MAX to
skip runs during the merge (§10) — and it is what lets sort live behind the same
contract instead of forcing a hole in it. Everyone else passes nothing.

---

## 4. What the contract must guarantee, whatever it does internally

The freedom in §2 is freedom over *mechanism*. Four things are not negotiable,
because a buffer that breaks them changes answers rather than costs.

1. **Round-trip fidelity.** `logical_type` restored, `DrakenVector.flags`
   preserved, dict `selection` **restored and never re-derived**. A round-trip
   that drops `logical_type` silently corrupts every DECIMAL and TIMESTAMP
   column, and a timestamp vector with `logical_type == nullptr` is a hard
   error. This is why format choice is free but format *capability* is not —
   §13.4.
2. **Replayable, not consuming.** A shared CTE's buffer is read by **several**
   consumer pipelines, each of which must see every morsel from the start —
   called out at the definition site
   ([`pipeline_buffers.hpp:20-23`](../src/cpp/engine/pipeline_buffers.hpp)).
   This is the constraint most likely to be missed, because the single-consumer
   case works without it.
3. **[D-3] Deterministic read-back order within a run.** UNION ALL leaves order
   across legs unspecified by design, so the buffer need not promise an order —
   but it must not make a query's output vary between two runs of the same plan
   on the same data purely because one spilled and the other did not. Whether
   that is "stable across the spilled/resident boundary" or the stronger
   "identical to arrival order" is open; the weaker one may be free and the
   stronger one may not.
4. **Loud failure.** Exhaustion raises, naming the operator and the byte count.
   ⛔ Never a silent fallback (§8.3).

---

## 5. Candidates

One phase, because §1 merged the two. Every private retained morsel pile becomes
a buffer instance:

| Operator | Note |
|---|---|
| `MorselBuffer` result hand-off — **all** breakers | Bounds the result side of every breaker in the engine, group-by and distinct included |
| `BufferAppendSink` — CTE materialize, UNION ALL | Append-through; input and output are the same bytes |
| `ScalarGuardSource` | Already an ordinary breaker over a buffer ([`native_scalar_guard.hpp:46`](../src/cpp/engine/native_scalar_guard.hpp)) |
| `WindowSink` | `WindowLocal` + `WindowGlobal` vectors |
| `FramedWindowSink` | Partition-keyed |
| Cross join / nested-loop retained side | Held input, re-scanned |
| `Join2Build{Local,Global}::morsels` | **[D-4]** — see below |

**[D-4] The join build side is a boundary case.** It retains payload column views
in arrival order *alongside* its hash table. The morsel half fits this contract
exactly and is plausibly the larger share of the bytes; the table half does not
fit at all. Converting only the morsel half is coherent and cheap — but it means
a join build that survives its payload can still OOM on its table, which is a
partial protection that must be described honestly rather than sold as coverage.
Include it, or hold it for the Group 2 programme?

---

## 6. Budget mechanics

### 6.1 Ceiling 1GB, flush trigger 512MB

The budget is a **ceiling**; the flush fires at 512MB. The trigger was revised
down from ~0.8GB (2026-08-27) so that **a flush unit is the size of a typical
skene-written file** — spill files stop being an outlier population in whatever
tooling looks at skene files, and the writer runs at the shape it is measured
and tuned at.

The gap between trigger and ceiling is consumed, not spare:

- **Encode working set.** Serializing to skene needs memory *while freeing
  memory*. A flush that begins at the ceiling allocates its encode buffers past
  it — the buffer would OOM inside the operation whose purpose is to prevent
  OOM.
- **Arrival overshoot.** The charge is on capacity growth, so the trigger is
  observed at morsel granularity: the morsel in flight when the trigger fires is
  already charged, and chunking is 131072 rows.
- **Release lag.** Charge returns as owners are freed, which trails the write.
- **Accumulation continues during the flush.** At a 512MB trigger under a 1GB
  ceiling there is room for a full second unit to accumulate while the first is
  being written — the flush need not stall the pipeline. At 0.8 it did not have
  that property; this is the trade the revision buys.

The trigger is now an **independent constant aligned to the file size skene is
tuned for, not a fraction of the budget** — which answers the earlier open
question (was D-5): if the budget ever becomes settable (§6.3), the trigger
stays at 512MB and only the headroom stretches. **[D-5] One residual:** if the
budget were ever set *below* ~768MB, trigger + encode working set no longer fit
under the ceiling; a settable budget needs a floor, or the trigger falls back to
a fraction below that point.

### 6.2 Charge

**[D-6] Charge shape — reuse the ratified one verbatim.** Charge on capacity
growth (one atomic per doubling, never per append); release on free; latch a
`spilled` flag. `GBArrayAggState` is the reference implementation.

**What is charged.** A `MorselPtr` is a view: the charge follows the
`VectorOwner` allocations — `data_buf`, `validity_buf`, `codes_buf`, `arena_buf`
— not the 40-byte `DrakenVector` structs.

**[D-7] Shared ownership is the trap.** Two retained morsels can reference one
`VectorOwner`. Charging both double-counts; charging neither under-counts. Charge
on owner identity, or accept the over-count as conservative?

**[D-8] Accounting across writers.** The budget is per operator but appends are
per worker. The counter is therefore one atomic shared by the buffer's handles —
which is fine at one atomic per capacity doubling, and would not be at one per
append. Confirm this is the intended reading of "per operator".

### 6.3 Visibility

The budget lands as a **session variable** from the start, in the
family of `median_memory_budget_bytes` / `array_agg_memory_budget_bytes`, sourced
from the C++ constant through an accessor. ⛔ Python must never mirror the
number — that is what stopped the aggregate budgets silently disagreeing with the
charge. **[D-9]** SERVER-owned and unsettable, or session-settable? An eviction
threshold is more defensible to expose than a refusal ceiling, but it turns a
constant into configuration.

---

## 7. Behind the interface

Everything in this section is the buffer's business and changeable without
touching an operator.

**Format: `.skene`, spill profile — no statistics, no value ordering.** Decided.
It is the only candidate that satisfies §4.1: it round-trips a `CxxMorsel`
losslessly including everything Parquet drops. Parquet would satisfy the
*interface* and violate the *guarantee* (§13.4).

The spill profile is the format's own existing ruling — spill needs no read
acceleration, so no statistics are computed and no value ordering is applied.
⛔ Note this stays consistent with sort's need for per-run MIN/MAX only because
those bounds live in the **unit metadata** (§3), never in the skene footer. If
they ever migrate into the footer, this ruling is broken and the spill profile
stops being stats-free.

**Compression: ON.** **One flush is one file.** Both ruled 2026-08-27.

### 7.1 Compression — the posture is not `for_fast_reads()`

Compression is not new machinery. Skene already applies a per-section codec
after the encoding, gated at `kCompressMinBytes` (10240) and only on section
kinds measured worth offering
([`format.h:341-354`](../skene/include/skene/format.h),
[`writer.cpp:207-217`](../skene/src/writer.cpp)). It is per section rather than
per file precisely so a column extent stays independently fetchable. The
decision is which codec, and the tree has the measurements.

⛔ **The existing default is documented with the opposite assumption.**
`WriteOptions::codec` is `SectionCodec::kNone`, commented *"Off by default:
spill wants raw bytes"* ([`writer.h:66`](../skene/include/skene/writer.h)). That
rationale is now wrong and the comment must be corrected as part of this work —
it is a documented premise contradicting an architectural ruling, which is worse
than an unset default.

Skene has two named postures, and the benchmark's own conclusion is that **"the
codec a corpus uses states where it is READ, not what it holds"** — local NVMe
reads take `for_fast_reads()` (lz4), deployed data takes `for_storage()`
(zstd-7). Spill is read locally, so that rule points at lz4.

**[D-10] I think spill is the exception, and wants zstd-1.** The posture rule
optimises *read time*. Spill's binding constraint is neither read nor write time
— it is the ~9GB ceiling, past which the query does not run at all. On that axis
the bakeoff is decisive (35MB of TPC-H sections,
[`BENCHMARKS.md`](../skene/BENCHMARKS.md)):

| codec | ratio | encode |
|---|---|---|
| lz4 | 0.43x | 44 ms |
| **zstd-1** | **0.30x** | **58 ms** |
| zstd-9 | 0.26x | 356 ms |

zstd-1 is **30% more headroom for 32% more encode time**, and it dominates
snappy on both axes. Whole-file, the same trade reads 4.0 GiB (lz4) against
2.7 GiB (zstd-7) from 7.8 GiB raw — roughly half again as much surviving query
per gigabyte of disk. On a path whose entire purpose is not dying, ratio is the
axis that converts to outcomes and milliseconds are not.

Higher levels are a bad trade here: zstd-9 is ~12% smaller for ~6x the compress
time, and zstd's decode rate is essentially level-independent (3284/3043/3477
MB/s at 1/3/9), so a higher level buys nothing back on read.

### 7.2 One flush is one file — and what that means for the claim

A flush seals a file; the file is never appended to. Unit, file and KV entry are
1:1:1, which is what makes the lifecycle in §8 tractable.

**[D-11] The claim must resolve to a row group, not a file.** Skene puts 16 row
groups in a file, and made the scan's unit of work `(file, row group)` at the
same time, specifically so file count does not cost parallelism. A 512MB unit
is many row groups; if a claim hands a worker the whole file, one worker
decompresses 512MB while the others idle — a parallelism regression the format
already solved once. Read-back should inherit `(file, row group)`.

**Store: a native disk-backed KV store.** ⛔ The existing store is unreachable
from here — `opteryx/managers/kvstores/` is eleven `.py` files, zero `.pyx`,
zero `.pxd`, and every operator in §5 is native C++. A native worker calling
`FileKeyValueStore.set()` re-acquires the GIL inside the spill path, which §1 and
§2 of the engineering contract forbid outright.

This is not an argument against the store; the config **already specifies exactly
the store the ruling describes**:

> `KVSTORE_LOCATION` … the per-query shuffle/spill store, whose keys are scoped
> by query and operator and whose contents are discarded when the query ends
> — [`config.py:377-381`](../opteryx/config.py)

and it has **no first-party caller for that purpose**. Its two live consumers are
the manifest and footer caches, both content-addressed and long-lived, and both
pointed at their own settings whose docstrings go out of their way to say they
are deliberately *not* `KVSTORE_LOCATION`. The abstraction was built for a caller
that never arrived.

**[D-12] Proposed:** build the native store behind the existing `KVSTORE_LOCATION`
/ `KVSTORE_KEY_PREFIX` contract and key scoping, leaving the Python store to its
Python callers. Not duplication of live logic — supplying the implementation the
config already documents, on the side of the boundary where the caller lives. The
alternative (one implementation, Python store deleted, its callers moved native)
is a materially larger change and should not be bundled here.

**Key:** opaque, random, scoped by query and operator. Content addressing would
buy a hash over the payload and nothing else — spill units are single-writer,
single-query, never deduplicated.

**Value:** a metadata record referencing the skene file, carrying the operator's
opaque blob (§3). Encoding is now internal; a fixed-layout record needs no
encoder to write and no parse to fail, and the schema never leaves the process.

---

## 8. Lifecycle

The disk is ~9GB of a 10GB Cloud Run ephemeral volume, the rest committed to the
manifest cache, shared by every concurrent query on the instance. **Orphaned
spill files are the failure that turns spill into the OOM's replacement.**

Two facts make that real: the KV store has **no TTL** — `touch()` is a documented
no-op ([`base_kv_store.py:93`](../opteryx/managers/kvstores/base_kv_store.py)) —
so "discarded when the query ends" is aspirational; and a killed query unwinds
nothing.

**[D-13] Required in the first delivery, not as follow-ups:**

1. **Scoped deletion** on query end, covering every object a unit comprises.
2. **A startup sweep** for units whose owning query no longer exists. Precedent:
   `ManifestDiskCache` — atomic `tmp` + `os.replace`, stale `.tmp` swept at seed
   time and never read
   ([`manifest_disk_cache.py:123-128, 193-205`](../opteryx/connectors/manifest_disk_cache.py)).
3. **Loud failure on disk exhaustion.** ⛔ Never fall back to the container
   filesystem — it is RAM-backed tmpfs unless a real volume is mounted, so a
   fallback re-creates the OOM this design prevents *while masking its cause*.

⛔ **Corollary to state plainly:** 10GB is a ceiling, not an escape. The
`OPTIMIZE … DEDUPLICATE` workload motivating the sort design is ~26GB physical.
Past the disk, the deliverable is an honest attributed failure rather than
survival — still a large improvement on `std::terminate()`.

---

## 9. Telemetry

Per operator instance: bytes accumulated, budget spent yes/no, units written,
bytes written, bytes read back, wall time attributable to spill. Attribution
lives here rather than in the budget — the budget is per operator, so counter and
attribution already agree.

A spilling query must be **visibly** a spilling query. Spill is by construction a
silent degradation — the query stops failing and starts being slow — and that
sits against "no silent degradation" unless it is observable.

---

## 10. Sort, and the one place the ruling is under tension

For every operator in §5 the budget is an eviction threshold and read-back is a
replay. For sort the budget **is the run size**: fill to budget, sort, write a
sorted run, reset, repeat, and finalize becomes a k-way merge.

So sort needs to influence *what happens at eviction* — which is precisely what
the encapsulation ruling puts out of reach. Two ways out:

- **[D-14a] An eviction hook.** The contract takes an optional
  transform-before-evict supplied at construction; default identity, sort passes
  sort-by-keys. The buffer still owns residency (when and whether to evict); the
  operator owns semantics (what a unit contains). Combined with the per-unit
  metadata channel (§3) this puts external sort entirely behind the contract:
  sort writes sorted runs with bounds attached, and reads them back with the
  bounds it needs to skip runs.
- **[D-14b] Sort keeps its own accumulation** and the contract stays narrower.
  Cleaner interface, one operator outside the residency regime.

I favour (a): the hook is small, it is the difference between the contract
covering the engine and covering most of the engine, and (b) leaves the single
largest accumulator unmanaged. But it does mean the contract is not purely
"append and forget", and that is a change to the ruling's shape rather than an
elaboration of it — so it is yours.

`TopNSink` (`ORDER BY … LIMIT n`) is **already bounded** by periodic `compact()`
and must never spill. It is the counter-example that keeps the rule honest:
bounded state is not a spill candidate however large the input.

---

## 11. What this does not need

- **No cgroup detection.** A configured budget is a local trigger: a buffer knows
  it filled its own 1GB without knowing the container's limit. This matters —
  `cgroup_memory_limit_bytes()` returns 0 under gVisor, Cloud Run's default
  execution environment, so any design triggering on *remaining system memory*
  would be blind there.
- **No admission control.** Out of scope, not a prerequisite.
- **No plan-time estimate.** Spill triggers on measurement, always. The aggregate
  guard's own evidence is decisive: the group-by cardinality estimator falls back
  to `input_rows/2` per unknown key, which on h2o g6 predicts 47.7GB against a
  true 1.2GB.

---

## 12. Acceptance

1. A query that today terminates on any operator in §5 completes, or fails
   naming the operator and the bytes.
2. An eight-row CTE performs zero disk I/O.
3. A buffer holding 600MB has flushed; one holding 400MB has not.
4. Non-spilling queries show no measurable regression — the write path stays
   lock-free per worker, one atomic per capacity doubling.
5. **Changing what the buffer writes requires editing no sink.** This is the
   encapsulation ruling's test.
6. A shared CTE read by three consumers yields identical results spilled and
   resident.
7. A killed query leaves no reachable spill objects after the next startup sweep.
8. A spilled buffer is read back by as many workers in parallel as a resident one
   — the claim granularity is the row group (§7.2).

---

## 13. Out of scope

**13.1 Sort's finalize** — §10, deferred but sharing budget, store and contract.

**13.2 Hash-table state.** Group-by, distinct, join build tables, roaring sets.
Not morsels; spilling them needs a partitioning scheme so a spilled partition can
be read back and re-merged. Separate programme. §1 notes what this design
incidentally bounds; §5 [D-4] notes where it starts.

**13.3 The holistic aggregates.** MEDIAN, ARRAY_AGG, CIDR_AGG — per-group
variable-length heaps, hardest to project to morsels, and the **only** operators
that already fail loudly and informatively, with a named aggregate, a measured
byte count and a plan-time gate. Spilling them trades a good error for a hard
build. Leave them.

**13.4 Format portability.** The contract permits any format satisfying §4.1.
Parquet does not: it drops `LogicalType` and the dict `selection`. "Free to
execute as it wishes" is freedom over mechanism, never over fidelity.

---

## 14. Open decisions

| | Decision |
|---|---|
| **D-1** | Concurrent accumulation under a per-operator budget — catch the UNION-ALL-legs and live-CTE cases, or accept the over-commitment? (§2.3) |
| **D-2** | Per-unit opaque operator metadata in the contract — needed only by sort, but the thing that keeps sort inside it. (§3) |
| **D-3** | Read-back order: stable across the spilled/resident boundary, or identical to arrival order? (§4.3) |
| **D-4** | Convert the join build side's retained morsels now (partial protection, honestly described), or hold for Group 2? (§5) |
| **D-5** | If the budget becomes settable, it needs a floor (~768MB) below which a fixed 512MB trigger no longer leaves encode headroom. (§6.1) |
| **D-6** | Confirm the charge shape is copied verbatim from `GBArrayAggState`. (§6.2) |
| **D-7** | Shared `VectorOwner` — charge on owner identity, or accept the over-count? (§6.2) |
| **D-8** | Confirm one atomic shared across a buffer's write handles is the intended reading of "per operator". (§6.2) |
| **D-9** | Budget variable SERVER-owned and unsettable, or session-settable? (§6.3) |
| **D-10** | Codec: zstd-1 for ceiling headroom, against the local-read posture that would say lz4? (§7.1) |
| **D-11** | Confirm read-back claims at `(file, row group)`, not whole file. (§7.2) |
| **D-12** | Build the native store behind the existing `KVSTORE_*` contract, leaving the Python store in place? (§7) |
| **D-13** | Confirm all three lifecycle obligations are first-delivery, not follow-ups. (§8) |
| **D-14** | *(sort phase, not this one)* Eviction hook inside the contract (a), or sort keeps its own accumulation (b)? Decide when §10 is picked up. |

---

## 15. Delivery record (2026-08-27)

**Built and green** (`make q` 462/462; `tests/sql/test_morsel_spill.py` pins it):

- **The buffer** — `MorselBuffer` in
  [`pipeline_buffers.hpp`](../src/cpp/engine/pipeline_buffers.hpp) is now the
  contract of §3: `append()` for single-threaded finalize writers, per-worker
  `Writer` handles (lock-free local batch, one mutex touch per 32MB splice),
  lazy `seal()` on first read, claim-indexed `get()`. Every breaker's output
  hand-off, `BufferAppendSink` (CTE materialize / UNION ALL — budgeted DURING
  sink, not at combine), and `ScalarGuardSource` run through it. No caller can
  see whether it spilled.
- **The store** — [`spill_store.hpp`](../src/cpp/engine/spill_store.hpp):
  native, per-query `q<pid>-<seq>` directory under `KVSTORE_LOCATION` (the
  first first-party caller of the contract config.py always documented),
  created lazily on first flush. Units deleted at last-consumer release,
  directory removed with the engine, startup sweep removes dead-pid
  directories (ESRCH only), ENOSPC is a loud named error.
- **Thresholds** — [`spill_budgets.hpp`](../src/cpp/engine/spill_budgets.hpp)
  (512MB flush / 1GB ceiling), reported by `SHOW VARIABLES`
  (`spill_flush_bytes`, `spill_ceiling_bytes`) via
  `opteryx/compiled/spill_budgets.pyx` — read from the constants, never
  mirrored. Telemetry via `_operators.get_spill_telemetry()`.
- **Format** — skene spill profile + zstd-1, one flush = one file, one morsel =
  one row group; claims are `(file, row group)` (D-11 as recommended). Columns
  written under synthetic positional names; real names + positions restored
  from the unit record on read (engine names may repeat or differ across UNION
  legs). `writer.h`'s "spill wants raw bytes" premise corrected;
  `for_spill()` itself stays codec-free as the baseline posture — the engine
  layers the codec.
- Measured on the smoke workload (80M-row ORDER BY): 1.28GB decoded reads back
  from 162MB on disk (0.127x), answers identical to the resident run.

**Two corrections the build made to this design:**

1. **§6's ceiling is BACKPRESSURE, not an error.** While a flush is in flight,
   appenders fill a fresh pile; one that finds it at 1GB *waits* for the flush
   and then flushes it itself. A query whose input outruns the disk is
   throttled to disk speed rather than killed — and outstanding memory stays
   bounded at ~ceiling + one in-flight pile + the workers' 32MB batches. Loud
   errors remain what they were: encode failure, disk failure, exhaustion.
2. **§5's window rows are wrong, and §1's merged phase was over-claimed.**
   Spilling a sink's INPUT pile only reduces peak memory if finalize reads it
   back incrementally — and window (like sort) must read everything back to
   sort it, which recreates the same peak later. Flush-and-replay genuinely
   pays only where read-back STREAMS: the breaker output buffers, CTE/UNION
   ALL, scalar guard — which is what was built. Window input joins sort in
   the run-generation phase (§10); it is not a flush-and-replay call site.

**Decisions resolved in the build** (each per the recommendation already in
this document): D-6/D-7 charge is `cxx_morsel_nbytes` per appended morsel, one
shared pile counter per buffer, over-count on shared owners accepted; D-9
SERVER-owned and unsettable; D-11 claims at row-group granularity; D-12 native
store behind `KVSTORE_LOCATION` (a non-file scheme leaves spill unconfigured —
the Python stores keep those callers); D-13 all three lifecycle obligations in
the first delivery. D-2's metadata channel exists as the unit record
(rows + names per row group); the operator-opaque blob waits for sort.

**Still open on THIS phase:** D-1 (concurrent accumulation), D-4 (join build
morsels). D-14 is not open here — it is the first decision of the sort phase
(§10) and nothing delivered depends on it; the contract is complete without
the hook and adding one is additive.

⛔ Design sections above are preserved as argued; where §15 corrects them, §15
is right and the section is stale.
