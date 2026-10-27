# Ungrouped COUNT(DISTINCT) — Append, Then Dedup Per Partition — Design

**Status:** Draft for architect review. Nothing is built; nothing in this document has
been prototyped.
**Date:** 2026-10-27
**Trigger:** ClickBench Q04 (`SELECT COUNT(DISTINCT UserID) FROM hits`) is within a few
percent of DuckDB (0.96x in the 2026-10-27 table). The dict-aware ingest change landed
the same day (Q04 0.718x, Q05 0.704x of the previous build, Mac, 11 interleaved rounds)
and left CarcharSet insert + resize at 77% of Q04's CPU.

**Two rulings are needed before any build (§7): the pre-sizing ruling and the
scatter-loss question.**

---

## 1. Summary

Today every worker owns 64 hash-partitioned `CarcharSet`s per `COUNT(DISTINCT)` spec,
inserts into them while the scan runs, and the finalize step inserts every worker's
distinct hashes a second time into merged sets. Both the per-worker sets and the merged
sets grow by doubling from 256 slots.

Proposal: during the scan a worker only **appends** each morsel's referenced unique
hashes to 64 per-partition buffers (sequential writes, no table). At finalize each
partition is deduplicated **once**, into a single set sized from the exact number of
appended entries, using the batched probe that already exists (`CarcharSet::mark_new`).
The distinct count of a partition is that set's size.

What this removes: the per-worker tables and their doublings, the second insert pass
(`UCDPartition::absorb`), and the `distinct` vector that duplicates every hash a set
already stores. What it costs: the dedup work moves from the scan phase (overlapped with
decode) to the finalize tail, and a worker's buffer grows with appended entries rather
than with distinct values. Both are real risks (§6, §7) — the second is the reason this
document does not recommend building until the open questions are answered.

---

## 2. Where Q04 is today (measured)

All Mac (M5, DOP 16), `hits_rugo_262k`, 99,997,497 rows, 17,630,976 distinct UserID.

Profile before the dict-aware change (samply, 30 loops, symbolicated against a dSYM whose
UUID matched the profiled `.so`; shares are of non-kernel samples):

| Caller | Share |
|---|---|
| `UngroupedAggSink::sink` → `CarcharSet::insert_or_ignore` | 60.6% |
| `UngroupedAggSink::sink` → `CarcharSet::resize` | 6.3% |
| `UCDPartition::absorb` → `insert_or_ignore` | 13.0% |
| `UCDPartition::absorb` → `resize` | 6.6% |

After the dict-aware ingest change (landed in main, uncommitted; `make q` and `make slt`
green): loop minimum 120 → 81 ms; profile: ingest 45.5%, merge (absorb incl. resize)
31.5%, i.e. CarcharSet is still 77% of CPU.

One `EXPLAIN ANALYZE` of the landed build (not a benchmark): `Ungrouped Aggregate`
`merge_ms` = 23.3 ms, self time 748 CPU-ms over 16 workers, `Parquet Read` 21 ms. I have
**not** verified the definition of `merge_ms`; I read it as the finalize wall time. If
that is right, the finalize tail is roughly a quarter of Q04's wall time already, which
matters for §6.

Not measured: x86 (no access as of 2026-10-27), TPC-H/TPC-DS ungrouped `COUNT(DISTINCT)`.

---

## 3. Current flow

`src/cpp/engine/native_group_sinks.hpp`:

- **sink** (`UngroupedAggSink::sink`, the `AggFn::CountDistinct` branch): hash the
  operand with draken (`compute_row_hashes_shaped`); for a compressed vector, mark the
  hashes some valid row references, then `DP[gb_part(h)].insert(h)` once per marked hash;
  otherwise once per valid row. `DP` is the worker's `std::array<UCDPartition, 64>`.
- **`UCDPartition`**: `CarcharSet seen` + `std::vector<uint64_t> distinct` (+ `raw` /
  `raw128` for `distinct_operand` specs). `insert` pushes to `distinct` iff the set took
  the hash. `kGBParts = 64`; the partition is hash bits [32, 38).
- **combine**: moves each non-empty worker `UCDPartition` into
  `g.dpending[spec][part]` under the mutex.
- **finalize**: one item per (spec, partition) with queued tables, `fork_join` over at
  most `query_dop` threads (inline if fewer than 65,536 queued entries). Per item:
  `merged = std::move(list[0])`, then `merged.absorb(list[i])` for the rest — every
  distinct hash of `list[i]` is inserted into `merged.seen`, growing it by doubling.
  `COUNT(DISTINCT)` = sum of the merged sizes.

Structural costs, from the code (not measured): each distinct hash is stored twice per
worker (set slot + `distinct`), the slot array is power-of-two with load ≤ 0.8 (9 bytes
per slot), and every hash a worker saw is inserted again in finalize.

---

## 4. Data facts that shape the design

From the first 6M rows of `hits_rugo_262k` (a prefix, not the whole table):

- 74.6% of rows equal the previous row; within-morsel distinct / rows = 0.25; global
  distinct / rows = 0.23. Cross-morsel repeats are rare in that prefix.
- UserID morsels arrive **dict-encoded**: 65,536 rows, `data_length` 6.8k–31k.

Consequences: after the dict-aware change a worker inserts about one hash per
morsel-distinct value; most of those are genuinely new to that worker's set (so each is a
cache-missing random write into a growing table); the sum over workers of worker-distinct
counts (U_w) is therefore close to the global distinct count plus cross-worker repeats.
**U_m (morsel-unique appends) and U_w are unmeasured**; a counter would give both and is
the first pre-build step (§9).

---

## 5. Proposal

Scope: plain `COUNT(DISTINCT col)` and `COUNT(DISTINCT *)` (count only; no operand
values). `distinct_operand` specs (`SUM/AVG/STDDEV/MEDIAN(DISTINCT)`) carry a value per
distinct hash and stay on the existing path in v1 — **this means the old code stays**;
see §7 R-D.

### 5.1 Sink

Per worker, per spec, per partition: an append-only buffer of `uint64_t` hashes (a
chunked buffer, not a doubling `std::vector`, so growth never copies). The dict-aware
marking logic is unchanged; the insert becomes `buf[gb_part(h)].append(h)`. 64 partial
cache lines are hot per worker; the writes are sequential per stream.

Draken still owns the value hash; NULLs are still skipped; no new shape dispatch beyond
the already-ratified compressed gate (COMPRESSAWARE, 2026-10-18).

### 5.2 Bounding a worker's buffer

This is the main design problem. Today a worker's memory is bounded by its **distinct
values**; an append buffer is bounded by **appended entries**, which for a non-dict
column with many non-adjacent repeats approaches the row count (100M rows × 8 B = 800 MB
for a 1,000-value column).

Two mechanisms, used together (not as a fallback pair):

1. **Front filter** — a tiny direct-mapped recent-hash cache per worker (a few thousand
   entries, L1/L2-resident) that drops an immediate repeat before it is appended. Cheap;
   only effective when repeats are close together.
2. **Window compaction** — when a worker's buffers hold W entries (candidate 1–4M, i.e.
   8–32 MB), deduplicate them in place through a scratch set sized to W and keep only the
   survivors. Adaptive and **sticky**: if a compaction removes less than a fixed fraction,
   stop compacting for the rest of the query (the 2026-10-23 R4 "raw exit" flapped 18
   times on Q18; a flapping policy is not acceptable here).

Memory is charged to the existing query budget as the buffers grow. Whether the current
`UCDPartition` / `CarcharSet` memory is accounted at all, and through which allocator, is
**unverified** and must be read before design sign-off.

### 5.3 Finalize

For each (spec, partition): `n_p` = sum of the workers' buffer lengths for that
partition (known exactly). One `CarcharSet` is built, reserved from `n_p`, and each
worker's buffer is fed through `mark_new` in `kCDChunk` (2048) chunks — the same batched
probe the 2026-10-17 merge-site work measured at −13% on Q08. The partition's distinct
count is `seen.size()`. There is no `distinct` vector, no `absorb`, no per-worker table.

Table size: Q04 has about 17.6M / 64 ≈ 275k distinct per partition; `n_p` is somewhat
larger (repeats across morsels/workers); with load ≤ 0.8 and power-of-two rounding the
table is a few MB (arithmetic, unmeasured) — L2/L3-sized, versus today's per-worker
tables, which are filled from empty by random writes.

Finalize parallelism is unchanged (64 items over `query_dop` threads).

---

## 6. Cost model (structural, unmeasured)

Inserts into a hash table per query, Q04-shaped input:

| | Today (landed) | Proposed |
|---|---|---|
| During scan | U_m probes into growing per-worker tables (random writes, doublings) | U_m sequential appends |
| Finalize | U_w probes into merged tables (doublings) | U_m probes into presized, cache-sized tables, batched |
| Extra | `distinct` vector writes; absorb reads | buffer reads |

Where it could win: the cache-missing inserts and every doubling disappear; the finalize
inserts are batched into a table sized once.

Where it could lose or not show up:

- **Overlap.** Today the scan-phase inserts overlap with decode; the finalize tail does
  not. The proposal moves work *into* the tail and makes it larger (U_m ≥ U_w). If the
  tail is already ~25% of wall (§2, unverified), CPU saved in the scan phase may not
  convert to wall time, and the tail could grow. This is the first thing a prototype must
  measure — wall, not CPU.
- **Buffer growth** for low-NDV / non-dict inputs (§5.2).
- **Cache-resident inputs.** If a column's per-worker sets are cache-resident today
  (the 2026-10-17 note says this about Q05-class cardinalities), a DRAM-bound append
  stream plus a second pass can cost more than a cache-resident probe. See R-B.

---

## 7. Rulings needed from the architect

### R-A. The pre-sizing ruling

`feedback_hashtable_sizing` (investigated 2026-05, GROUP BY roadmap Tier 3 #6): do not
pre-size hashed-aggregate / join tables from NDV estimates; incremental doubling from a
256-slot seed was measured to beat rehash avoidance, because a table pre-sized to ~80 KB
exceeds L2 and every `find_or_insert_id` in the 65k-row morsel loop pays an L3 miss
instead of an L1 hit. It also warns that multi-file KMV estimates overshoot, and that any
future attempt must be measured on ClickBench top-N queries.

Why this proposal is different, and where it is not:

- The size comes from the **exact count of appended entries** (an upper bound on
  distinct), not an NDV estimate, so the multi-file overshoot problem does not arise.
- The reserved table is built in **finalize**, not in a per-morsel hot loop; the ruling's
  mechanism (a small table staying in L1/L2 across a morsel loop) does not apply to a
  one-shot bulk insert of ~275k–400k entries into a ~MB table.
- But it **is** pre-sizing in the sense of the ruling, the upper bound can overshoot the
  true distinct count by the repeat factor (up to a power-of-two step), and nobody has
  measured it on Q04.

Request: permit exact-upper-bound pre-sizing **in the ungrouped COUNT(DISTINCT) finalize
only** — not in any sink hot loop — conditional on an A/B where "presize" and "doubling"
are separate arms inside the new design (§9), measured on Q04/Q05 and the other
`COUNT(DISTINCT)` queries.

### R-B. The scatter-loss question

Recorded at `native_group_sinks.hpp:2250-2258` and in `count_distinct_batched_probe`
(2026-10-17): batching the **ungrouped ingest** probes — which needed a per-morsel
partition scatter of the rows — measured a loss (Q09 +5%, Q05 +23%; the leave-one-out
line also reads Q04 1.08 for the full variant) and was deleted: "those sets are
cache-resident, scatter is pure cost". Merge-site batching, with no ingest scatter, won.

Does that loss transfer to this proposal?

- Argument that it does not: then, the scatter was *added* in front of the same per-worker
  probes (counting sort + `mark_new`, two passes). Here the partition indexing already
  exists (`DP[gb_part(h)]` today) and the append *replaces* the probe; no probe remains in
  the scan phase.
- Argument that it might: Q05 (`SearchPhrase`, a string column) lost badly, and its sets
  were described as cache-resident. If Q05's per-worker sets fit in cache, a streaming
  append plus a finalize pass is extra memory traffic for no saving, and this proposal
  could lose on Q05-class inputs while winning on Q04. The landed dict-aware change also
  moved Q05 by 0.704x, so the baseline is no longer the one the 2026-10-17 numbers were
  taken against.
- I cannot settle this from the notes. It needs the A/B on Q04 **and** Q05 (and any
  other ungrouped `COUNT(DISTINCT)` in TPC-H/TPC-DS) in the same prototype.

Request: confirm that a Q05 regression is a kill condition for the whole design (no
per-column dispatch between two implementations — that would be a fallback in the sense
of the engineering contract).

### R-C. Bounded memory (§5.2)

Is front filter + sticky window compaction the accepted shape, and what are W and the
compaction-exit threshold's acceptance criteria? Alternatives I considered and rejected:
a second code path selected by a runtime ratio (a fallback; two implementations of the
same aggregate), and unbounded appends (an OOM risk on low-NDV non-dict columns).

### R-D. Scope

v1 covers plain `COUNT(DISTINCT)` only. `distinct_operand` specs keep the old
`UCDPartition` path, so `UCDPartition` and `absorb` stay. Alternatives: (a) accept two
paths in v1 and move `distinct_operand` later; (b) carry a parallel raw-value buffer
(8 or 16 B/entry more) and convert everything now. I recommend (a) but this is a
code-duplication judgement call that is yours.

---

## 8. Alternatives considered

- **Hybrid: keep per-worker sets, fix only the finalize.** Adopt the largest worker set
  as the merge base; pre-size the merged set from the sum of worker sizes. Saves roughly
  1/16 of the absorb inserts plus its doublings, leaves the ingest cost untouched. Small,
  low risk, and also touches R-A. Worth measuring as an ablation arm.
- **One shared concurrent set per partition** (workers insert directly; no merge at all).
  Eliminates the second pass and the cross-worker duplicates, overlaps all work with the
  scan. Needs a concurrent CarcharSet (there is a CAS-on-hash-word precedent in
  `GBTicketTable`, R9) and a resize-under-concurrency story; the R9 runtime presize
  fallback was deleted in 2026-10 after Q15 3.49x / Q35 2.17x regressions, so the sizing
  problem is not hypothetical. Much larger change; not recommended first.
- **Grow ×4 instead of ×2.** Fewer rehash passes, lower average load, bigger working set;
  conflicts with the cache-residency finding in `feedback_hashtable_sizing`. Not
  pursued.
- **Skip hashing/inserting from footer statistics.** UserID ranges overlap in every row
  group; no pruning is possible.
- **Approximate structures.** `COUNT(DISTINCT)` is exact; `APPROX_COUNT_DISTINCT` is a
  separate function.

---

## 9. Validation plan

Nothing starts until R-A..R-D are answered. All work in a private tree (MUTPROBE), landed
only after validation; one `_operators.so` differs between arms.

**Pre-build measurements (small, private tree, no behavioural change):**
1. Counters: appended entries U_m and, for the old path, sum of worker-set sizes U_w;
   per-phase wall (scan-overlapped sink vs finalize tail) rather than CPU sums.
2. Read the memory accounting of `CarcharSet` / `UCDPartition` to answer §5.2.

**Go/no-go gate:** if the finalize tail is large and the model in §6 says the moved work
does not shrink, stop here and record why.

**Build-all-then-ablate (ABLATE ruling), arms inside one tree:**
- A: landed baseline.
- B1: append + serial-per-partition finalize with a doubling set.
- B2: B1 + exact presize.
- B3: B2 + batched `mark_new`.
- Hybrid arm from §8.
Leave-one-out from the full variant. Interleaved A/B, alternating arm order, enough
rounds for the control spread to be a small fraction of the effect.

**Targets:** Q04, Q05, any other ungrouped `COUNT(DISTINCT)` (to be identified in
TPC-H/TPC-DS and `--extended`). **Controls:** queries that provably do not touch the
sink or any allocation the change alters — Q20, Q25, Q29 were clean in the 2026-10-27
runs; Q01 is **not** a clean control for allocation changes: its sink allocates the same
group-by lane segments, and it moved 1.28–1.32 under an allocation-only change on
2026-10-27.

**Correctness:** differential A-vs-B answers over filters, empty input, NULL operands
(`CASE`/`NULLIF`), `COUNT(DISTINCT *)`, dict and dense vectors, strings, a low-NDV
high-row column (exercises compaction), and a high-NDV column; unit tests for the
buffer and the compaction invariant (set equality before/after). Then `make q`, `make
slt`; `make dt/st/rt` only if native code is deleted.

**Per-architecture:** ARM measured on the Mac. **x86 is required for sign-off
(ARCHPERF) and there is no x86 access as of 2026-10-27**, so any landing is Mac-only
until that changes, and must say so.

---

## 10. Risks

- Wall time does not improve because the tail grows (§6) — the most likely failure.
- Q05-class regression (R-B).
- Memory blow-up on low-NDV non-dict inputs without compaction; compaction policy
  flapping; compaction cost on adversarial inputs.
- Accounting: buffers must be charged to the query budget like the sets were (if the
  sets were).
- Two code paths remain in v1 (R-D).
- Mac performance/efficiency cores distort DOP scaling above ~6 workers; DOP-16 Mac
  ratios are not predictive of the x86 box (PEcores).

## 11. What I don't know

U_m, U_w, the definition of `merge_ms`, the memory accounting of `CarcharSet`, whether
the finalize tail overlaps anything, how Q05's sets behave under the landed change, and
every number on x86.
