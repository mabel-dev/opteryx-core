# Parquet grouped column-major layout

**Status: IMPLEMENTED 2026-09-24; §8 decisions ruled or closed.** The
writer, the reader (native, latmat and trampoline scan paths) and the sinks
carry the layout described here; §9 records what was built and what was
measured on it. draken and skene are untouched. Architect rulings are marked
**[RULED]**; decisions are **[D-n]** and collected in §8 with their status.

Every number carries its provenance:

| Tag | Meaning |
|---|---|
| **[M]** | Measured in this work, on this repository, 2026-09-24 (method in §7) |
| **[C]** | Read from code (file:line) |
| **[A]** | Architect-provided input, not measured here |
| **[Model]** | Computed by the read-count model (§7.3) from [M] geometry and [A] parameters — not a measurement of a grouped reader, which does not exist |

---

## 1. Purpose

Two row-group sizes were chosen for two different reasons [RULED]:

- **256k rows** was chosen for **IO**: fewer range GETs against GCS.
- **Smaller row groups** are wanted for **engine performance**: the row group
  is the unit of work (morsel) the engine receives.

Today one number has to serve both, because the remote reader makes **one work
item per row group** and coalesces range GETs **only within that row group**
(`build_remote_plan`, [io_pipeline.hpp:2221](../rugo/src/parquet/io_pipeline.hpp)) [C].
Simply writing smaller row groups into today's layout is degenerative: every
projected column costs one GET per row group, so 64k row groups cost about 4×
the requests (§3: 25,305 → 98,554 GETs on the ClickBench battery [Model]).

This design changes the **physical layout** so that files carry small row
groups for the engine while the reader still fetches **256k rows of a column in
one range**, keeping IO at today's level.

Which small row-group size is best for the engine — where CPU-cache benefit,
per-morsel dispatch cost and work balance meet — is measured in §5.5.

## 2. Layout

Row groups of R rows are written in **blocks of 256k rows** (G = 256k / R row
groups per block) [RULED — writes stay in 256k blocks because of other
limitations]. Within a block, each column's chunks for all G row groups are
byte-adjacent, columns in schema order. Bloom filters go in the file tail
[RULED]:

```
Row-major today (G row groups):          Grouped (one 256k block):
[rg1.c1 rg1.c2 rg1.c3]                    [rg1.c1 rg2.c1 .. rgG.c1]
[rg2.c1 rg2.c2 rg2.c3]          →         [rg1.c2 rg2.c2 .. rgG.c2]
...                                       [rg1.c3 rg2.c3 .. rgG.c3]
[rgG.c1 rgG.c2 rgG.c3]                    [next block] ... [blooms] [page index] [footer]
```

Reading c1 over a block is **one range instead of G**.

| Property | Rule |
|---|---|
| Row group | Stays the decode, claim, statistics and morsel unit |
| Block | 256k rows = G row groups; last block of a file may be partial [RULED] |
| Rows per file | A variable, not a constant [RULED] |
| Footer honesty | `RowGroup.file_offset` = min chunk start, `total_compressed_size` = sum of its chunks [RULED] |
| Grouping discovery | The reader infers blocks from chunk offsets, not from metadata [RULED] |
| Bloom filters | File tail, after the last block, before the page index [RULED] |

**Spec legality.** `bloom_filter_offset` / `bloom_filter_length` are a bare
offset and length ([parquet.thrift:925](../rugo/src/parquet_spec/parquet.thrift))
[C]; "after all row groups, before the page index" is the first layout shown in
[BloomFilter.md:330](../rugo/src/parquet_spec/BloomFilter.md). Chunk placement:
the PoC `scratch/interleaved_parquet_poc.py`, re-run 2026-09-24, passes
**12 layout variants × 5 readers (pyarrow, DuckDB, opteryx, rugo, Polars)**,
comparing values, with and without a page index [M]. Not covered: the PoC's
source files are pyarrow-written — a rugo-written grouped file must be verified
against the five readers before approval [D-3].

**Consequence of tail blooms.** Today a pushed =/IN column's adjacent bloom
rides in the same GET as its chunk and is used remotely to **skip decode**, not
to skip fetches (`compute_bloom_prefix`,
[io_pipeline.hpp:1965-1991](../rugo/src/parquet/io_pipeline.hpp)) [C]. With blooms
in the tail that ride no longer exists — see [D-6].

## 3. Values

| Variable | Value | Evidence |
|---|---|---|
| Block | **256k rows** | [RULED] |
| Row-group size R | **64k** | §5.5 [M]: fastest in parallel at SF1 and SF10; 8k/16k lose to per-morsel cost, 32k gives up 1.6–7.6%, 256k 4.7–5.6% at SF1 and ties at SF10 |
| File size | **4 GiB** (unchanged) | [A] memory budget; file size does not change read request counts because footers are read at plan time [A] |
| Coalescer | waste ratio **0.10**, cap **8 MiB** (unchanged) | [C] [config.py:339](../opteryx/config.py); in flight ≤ 32 × 8 MiB = 256 MiB |
| Writer buffer | one block ≈ 256k × 95–102 B/row ≈ 24–26 MiB | [M] bytes/row (§5.1) |

**IO effect** — battery of 42 ClickBench queries, model on measured geometry,
256k blocks, tail blooms [Model]:

| Layout | GETs | GiB read | Additive | Overlapped |
|---|---|---|---|---|
| Today: 256k row groups | 25,305 | 23.38 | 479.0 s | 384.1 s |
| 64k row groups, today's layout (degenerative) | 98,554 | 24.69 | 798.5 s | 577.9 s |
| **64k row groups, 256k blocks** | **23,943** | 23.89 | 481.3 s (+0.5%) | 391.1 s (+1.8%) |
| **32k row groups, 256k blocks** | **24,283** | 24.41 | 491.1 s (+2.5%) | 400.7 s (+4.3%) |
| **16k row groups, 256k blocks** | **25,217** | 25.12 | 506.4 s (+5.7%) | 414.7 s (+8.0%) |

With 256k blocks the request count stays at today's level for every R; what
smaller R costs on the IO side is **bytes**, because smaller row groups make
larger files (§5.1).

## 4. Required changes (for sizing, not approval)

**Writer** (`rugo/src/parquet/_parquet_writer.hpp`):
- Buffer one 256k block (G encoded row groups), then emit it column by column;
  collect bloom filters and write them after the last block. Peak memory = one
  block plus the file's blooms.
- Footer conventions as in §2.

**Reader** (`rugo/src/parquet/io_pipeline.hpp`):
- Work item = the kept row groups of one block. Feed all their extents to the
  existing coalescer. Decode and claims stay per row group.
- Bloom ride from the tail: see [D-6].
- Fetch-ahead arming counts **row groups**
  (`PARQUET_IO_FETCH_AHEAD_MIN_ROW_GROUPS` = 48,
  [config.py:409](../opteryx/config.py)) — see [D-1].

**Sink:** [config.py:98-105](../opteryx/config.py) states the INSERT/CTAS/RMV
sink clamps to one row group per file so `write_parquet_with_bounds` can fill
bounds. This design needs multi-row-group 4 GiB files — see [D-2].

## 5. Evidence

### 5.1 File geometry by row-group size [M]

All 99 `hits` files rewritten with the current rugo writer defaults
(98,997,497 rows).

| R | File B/row | Blooms B/row | vs 256k | Footer B/row | Footer B/chunk |
|---|---|---|---|---|---|
| 16k | 102.08 | 14.42 | +14.1% | 0.5466 | 84.0 |
| 32k | 97.67 | 12.74 | +9.1% | 0.2760 | 84.8 |
| 64k | 95.08 | 11.77 | +6.2% | 0.1445 | 86.0 |
| 128k | 92.26 | 11.05 | +3.1% | 0.0739 | 88.0 |
| 256k | 89.49 | 9.93 | 0 | 0.0385 | 91.6 |

Largest per-column growth 16k vs 256k: Title ×1.43, URL ×1.21 (data pages);
many small string/flag columns switch to dictionary encoding at small R.

### 5.2 Footer cache (128 MiB) [M footer bytes × A cache size]

Footers are read at plan time and cached under a strict size limit [A].
Grouping does not change footer size; R does.

| R | Data whose footers fit | Footer per 4 GiB file | 4 GiB files that fit |
|---|---|---|---|
| 16k | 25 GB | 21.9 MiB | 5.8 |
| 32k | 48 GB | 11.6 MiB | 11 |
| 64k | 88 GB | 6.2 MiB | 21 |
| 256k (today) | 312 GB | 1.8 MiB | 73 |

### 5.3 Read model per query: 64k row groups in 256k blocks [Model]

Additive seconds (GETs):

| Query | Today 256k | 64k, 256k blocks |
|---|---|---|
| Q02 | 1.44 (338) | 1.57 (374) |
| Q03 | 3.57 (756) | 3.56 (756) |
| Q07 | 1.56 (378) | 1.56 (378) |
| Q10 | 11.02 (1,512) | 9.40 (1,134) |
| Q15 | 4.68 (378) | 4.70 (378) |
| Q20 | 4.62 (297) | 3.39 (337) |
| Q24 (SELECT *) | 132.51 (1,890) | 127.37 (1,133) |
| Q31 | 12.24 (1,512) | 12.26 (1,512) |
| Q37 | 0.32 (20) | 0.28 (23) |
| Q41 | 0.30 (20) | 0.24 (26) |
| Q42 | 0.24 (25) | 0.21 (31) |

Pruned queries (Q20, Q37–Q42) read less because 64k prunes more finely (§5.4);
Q24 reads fewer bytes because blooms are no longer interleaved between chunks.
The model does not fetch tail blooms for pushed =/IN columns ([D-6]).

**Larger blocks** would cut requests further — 64k row groups in 4Mi-row
blocks: 4,731 GETs, 405.0 s additive [Model] — but blocks stay at 256k [RULED].

**Bloom placement** at 256k blocks (64k row groups) [Model]: tail 23,943 GETs /
481.3 s; one bloom run per column before its block 28,589 GETs / 505.6 s — the
runs break merges across columns (Q24 1,133 → 3,778 GETs). Tail chosen [RULED].

### 5.4 Pruning granularity [M]

Kept row groups, from real per-16k-block min/max of the predicate columns:

| Query | 16k | 32k | 64k | 128k | 256k |
|---|---|---|---|---|---|
| Q02 (AdvEngineID <> 0) | 3,324 | 1,887 | 1,077 | 610 | 338 |
| Q20 (UserID =, scattered) | 1,862 | 1,258 | 815 | 493 | 297 |
| Q37–Q43 (CounterID = 62, clustered) | 48 | 25 | 14 | 8 | 5 |

Row order: each `hits` file is one EventDate sorted by CounterID, so
CounterID = 62 is clustered within a file and repeated across files.

### 5.5 Engine: morsel size (row-group size) [M]

TPC-H SF1 and SF10 rewritten with the current rugo writer at 8k, 16k, 32k,
64k and 256k row groups, source file boundaries preserved (16 lineitem files).
Result digests identical across all sizes for every query. Timing = sum over
22 queries of each query's minimum; arms interleaved per query with the order
rotated each round; local disk; **whole-file mapping cache on**
(`RUGO_LOCAL_MMAP_CACHE=1`, the x86 default — see [D-4] for why).

**Wall time, parallel:**

| Run | 8k | 16k | 32k | 64k | 256k |
|---|---|---|---|---|---|
| SF1, 14 workers, 5 rounds | +58.1% | +18.5% | +4.6% | **715 ms** | +4.7% |
| SF1, 6 workers, 5 rounds | +46.3% | +12.8% | +1.6% | **818 ms** | +5.6% |
| SF10, 14 workers, 3 rounds | +101.1% | +33.4% | +7.6% | **4,726 ms** | +0.3% |

64k was fastest in every round of every run. Fastest arm per query: SF1/14 —
64k 16, 256k 4, 32k 1, 16k 1; SF1/6 — 64k 11, 32k 6, 16k 3, 256k 2; SF10/14 —
64k 9, 256k 9, 32k 4. Lineitem row groups: SF1 736 / 368 / 192 / 96 / 32;
SF10 7,330 / 3,666 / 1,840 / 928 / 240.

**Per-operator self-time, one worker** (EXPLAIN ANALYZE, best repeat per query,
ms summed over 22 queries):

| SF1 | 8k | 16k | 32k | 64k | 256k |
|---|---|---|---|---|---|
| Execution | 1,988 | 1,840 | 1,782 | **1,779** | 1,841 |
| Table scan | 634 | 585 | 566 | **565** | 609 |
| Inner join | 529 | 523 | **522** | **522** | 526 |
| Aggregate | 340 | 332 | **331** | 342 | 371 |

| SF10 | 8k | 16k | 32k | 64k | 256k |
|---|---|---|---|---|---|
| Execution | 27,081 | 23,246 | 20,871 | 20,974 | **20,363** |
| Table scan | 6,225 | 5,602 | 5,290 | **5,243** | 5,398 |
| Inner join | 7,760 | 7,567 | 7,412 | 7,485 | **6,841** |
| Aggregate | 3,583 | 3,529 | **3,441** | 3,584 | 3,686 |

Reading:
- **Below 32k, per-morsel cost dominates** (8k scan +12% at SF1, +19% at SF10
  against 64k; 8k wall time +46% to +101%).
- **Aggregate** is fastest at 32k at both scales — the cache effect — and 3–8%
  slower at 256k.
- **Scan** is fastest at 64k at both scales (32k within 1%; 256k +3% to +8%).
- **Inner join** is flat at SF1 and 9% faster at 256k at SF10 on one worker —
  the one operator that prefers large morsels.
- In parallel, 64k is best at both scales; 32k gives up 1.6–7.6%, and 256k
  4.7–5.6% at SF1 and ties (+0.3%) at SF10.

**The same sweep with the mapping cache off** (the ARM default) exaggerated the
small-R penalty (SF10: 8k +214%, 16k +69%, 32k +16%) and made 256k look 6.8%
faster than 64k at SF10. Cause: per-row-group `open()`/`mmap()` of the same file,
which grows with row groups per file — SF10 lineitem as one file at 64k (916
row groups): Q06 1,884 ms with the cache off, 229 ms with it on (16 files:
209 ms). See [D-4].

Also measured on `hits` (current layout, 1M-row files, cache off, 3
interleaved rounds): 32k 16,732 ms, 64k 14,333 ms, 128k 13,659 ms, 256k
13,369 ms. Not re-run with the cache on.

### 5.6 Where grouping loses [Model, earlier run with linearly scaled geometry]

Wide projection under pruning: each kept block costs about one GET per
projected column against about 2 per kept row group today. Architect ruling:
this shape occurs mainly on first contact with a table and is **not an
optimisation target** [RULED]. No ClickBench query has this shape; grouping
was never slower than the same R in today's layout across 2,772 query ×
configuration cells.

## 6. Not measured, and the assumptions most likely to be wrong

1. **No grouped writer or reader exists.** Every grouped number is [Model].
2. **Production (GCS) was not measured.** RTT 0.13 s (110–150 ms range), 64 MiB/s
   aggregate bandwidth, Q = 32 concurrent GETs are [A]. Additive and overlapped
   time bracket the answer; they disagree most on latency-bound queries.
3. The model replicates the coalescer rule and assumes which predicates are
   pushed as =/IN and that row-group min/max pruning applies.
4. The current writer's 256k output is **4.2% larger** than the original
   `scratch/hits_rugo_262k` files (rugo 0.9.123 build 3527). Unexplained; all
   comparisons here use the current writer.

## 7. Method and reproduction

Scripts: `scratch/grouped_layout_model/` (paths inside point at the session
scratchpad and must be edited to re-run).

1. **Geometry** (`geom.py`, `rgsweep.py`, `rgmeasure.py`): footers of the 99
   `hits` files, and footers of every file rewritten in memory at 16k–256k with
   `rugo.parquet.write_parquet` defaults.
2. **Pruning** (`prune.py`): per-16k-row-block min/max of the predicate
   columns; any R is an aggregation of blocks.
3. **Read model** (`model.py`, `sweep.py`, `model_measured.py`,
   `bloomplace.py`): replicates `build_remote_plan` (merge while cumulative
   waste ≤ 0.10 × useful and span ≤ cap) over current and grouped layouts;
   time = ⌈GETs/32⌉ × 0.13 s + bytes / 64 MiB/s (additive) or the max of the
   two (overlapped). Files are modelled as 48Mi rows, re-chunked in
   file-number order. Validated against real footer offsets: GETs within ≈5%
   per row group (Q23 ≈12%), bytes within ≈1%.
4. **Local scan benchmark** (`rgwrite.py`, `rgbench.py`): `hits` written at
   32k, 64k, 128k and 256k; one untimed warm pass per arm, then 3 rounds, all
   four arms per query with the arm order rotated each round; on AC power;
   load average 5.3 at start; row counts identical across arms.
5. **Compliance**: `scratch/interleaved_parquet_poc.py` re-run as-is.

## 8. Decisions — status after implementation

- **[D-1] RULED 2026-09-24: count fetch blocks.** The gate counts REMOTE
  fetch blocks after pruning (distinct (file, block) among the submitted row
  groups) — the unit the fetch stage actually issues (`http_fetch_ops`) — so
  the production calibration made on 256k-row fetch units carries over to 64k
  files unchanged. The knob is renamed: config
  `PARQUET_IO_FETCH_AHEAD_MIN_BLOCKS`, variable
  `parquet_io_fetch_ahead_min_blocks` (default 48). On a row-major file every
  row group is its own block, so nothing moves there.
- **[D-2] RESOLVED — already true before this work.** Every writing sink
  (INSERT/CTAS, MERGE, OPTIMIZE) streams through `DataFileStream` →
  `open_parquet_writer` (2026-09-14) and produces multi-row-group files; the
  config.py text about a one-row-group clamp for `write_parquet_with_bounds`
  was stale and is corrected. The sink now batches to rugo's
  `DEFAULT_ROWS_PER_ROW_GROUP` (65,536; `WRITE_COALESCE_ROWS` default 65536)
  — one batch is one row group — and the writer groups row groups into blocks
  of `DEFAULT_ROW_GROUPS_PER_BLOCK` (4). The opteryx_catalog package's writer
  calls `open_parquet_writer` / `write_row_group` unchanged and therefore
  gets the grouped layout through the defaults. A file closes at the size
  target on any row group, so its last block may be partial [RULED legal].
- **[D-3] DONE.** `tests/rugo/test_grouped_layout.py` writes a grouped file
  with rugo (64k row groups, blocks of 4, zstd, page index, 9 column types
  incl. nulls, date, decimal, bool, dictionary and plain strings) and compares
  VALUES across pyarrow, DuckDB, Polars, rugo and opteryx, full, projected and
  filtered.
- **[D-4] Not blocking; measured again in §9.3** with the mapping cache on
  and off on ARM. The ARM default is unchanged (still OFF) — that ruling is the
  architect's.
- **[D-5] OPEN, no code change.** Footer bytes scale with R, not with
  grouping (§5.2): 64k = 6.2 MiB per 4 GiB file, 21 such files per 128 MiB of
  footer cache.
- **[D-6] RULED 2026-09-24: drop the remote bloom decode-skip.** The
  mechanism is removed from the pipeline (`compute_bloom_prefix`, the needle
  probe and the decode branch that consumed the adjacent bloom bytes); nothing
  extends a chunk fetch backwards any more. The dictionary decode-skip
  (`dict_all_filtered`) is untouched, plan-time bloom pruning on local footers
  is untouched, and `bloom_filter_bytes_maybe_contains` stays as rugo's
  in-memory probe API. Fetching tail blooms was rejected because a decode-skip
  saves CPU, never bytes, and either fetch shape costs requests (per block:
  roughly +1 GET per block per pushed =/IN column, ~2× on a scattered point
  lookup; per file: a serial read before the data plan).

## 9. Implementation (2026-09-24)

### 9.1 What was built

**Writer** (`rugo/src/parquet/_parquet_writer.hpp`): `encode_row_group`
produces position-independent chunks; `write_block` places a block
column-major; `write_bloom_tail` writes every bloom filter after the last
block, column-major over the whole file (column c's filters for row groups
0..N are contiguous — the order that keeps every D-6 option a single range);
the footer writes `RowGroup.file_offset` (min chunk start) and
`total_compressed_size` (sum of chunks) [RULED]; the page index is unchanged
(page offsets are pinned at placement). Parameters: `max_rows_per_row_group`
(default **65,536**; was 500,000 in the native module and 262,144 in the
facade) and `row_groups_per_block` (default **4**; 1 = row-major with tail
blooms). `write_parquet_with_bounds` bounds now span every row group. The
streaming writer keeps one-call-one-row-group and holds one block plus the
file's blooms until close. **Memory note, not ruled:** tail blooms are held
for the whole file — at hits' geometry (11.8 B/row of blooms at 64k) a 4 GiB
file holds ≈530 MB of bloom bytes until `close()`.

**Reader** (`rugo/src/parquet/io_pipeline.hpp`): a remote submission is a
**fetch block** — the kept row groups of one block, planned through the
existing coalescer together (`plan_block`), fetched in one batch, shared by
its members, decoded and claimed one row group at a time, one result per row
group. Single row groups are one-member blocks: there is one remote fetch
path. With fetch-ahead the fetch stage fills the block and then publishes its
members; on the coupled path only the block's lead member is claimable and
its decode fetches the block and publishes the followers, so no worker ever
waits on another worker's IO. Blocks are inferred from the footer:
`infer_fetch_blocks` groups consecutive row groups whose projected chunks are
byte-adjacent [RULED: offsets, never metadata]; a row-major file reads as one
block per row group. `http_fetch_ops` counts blocks, `http_request_count`
counts ranges. Wired into `native_parquet_scan_source.hpp` (the window advances
whole blocks, overshooting by at most G−1 row groups; LIMIT caps and runtime
pruning cut a block, which is then fetched partially as one),
`native_latmat_scan_source.hpp` (pass 1, and pass 2 with per-member masks —
pass-1 survivors are re-sorted into file order so block-mates are adjacent)
and the trampoline `IpcRowGroupSource` (pool_reader.pyx).

**Removed under [D-6]:** the remote bloom decode-skip and its adjacent-bloom
fetch extension. **Renamed under [D-1]:** the fetch-ahead gate knob, which
now counts fetch blocks.

**Not changed:** the column patcher (`_parquet_patch.hpp`) still copies
extents per row group in source order, so a patched (ALTER TABLE ADD/DROP)
grouped file comes out row-major with its blooms moved — correct, but it loses
the grouping until rewritten. Flagged, not fixed (out of scope).

**Tests:** `tests/rugo/test_grouped_layout.py` (layout, tail blooms, honest
footer fields, streaming == one-shot, D-3 five readers) and
`tests/unit/connectors/parquet_io/test_fetch_blocks.py` (the native scan over
HTTP: 8 row groups × 1 column = **2 GETs grouped vs 8 row-major**; SELECT *
2 vs 8; two non-adjacent columns 4 vs 16; a pruned single row group is a
one-member block; a range predicate keeping a whole block is one GET; the
Q24-shaped late-materialization query runs pass 2 with masked blocks and
answers identically). `make q` passes.

**Dev tooling:** `dev/rewrite_parquet_layout.py` (rewrite a directory at a
layout), `dev/grouped_layout_get_count.py` (the ClickBench battery over
`dev/throttle_server.py`, on the native path, reporting range GETs and bytes
from the pipeline's own telemetry), `dev/grouped_layout_tpch_bench.py`
(interleaved TPC-H timing across layout arms). The throttle server's accept
backlog was raised from socketserver's default of 5: with 60+ concurrent
range GETs the kernel dropped SYNs and the client's retransmits showed up as
1–5 s GETs with zero retries (a harness artefact; counts were exact, times
were not).

### 9.2 Range GETs on the ClickBench battery [M]

`dev/grouped_layout_get_count.py`: the 43 battery statements, run through SQL
on the native scan path (NativeParquetScanSource / LatmatScanSource) against
each dataset served over `dev/throttle_server.py` on loopback — every byte the
scan reads is a counted range GET — with the production defaults (fetch-ahead
depth 64 armed, coalescer 0.10 / 8 MiB, page index off on these files). All
99 `hits` files, 98,997,497 rows. `today256k` is the existing
`scratch/hits_rugo_262k` (rugo 0.9.123); the other two arms were written by
this writer with `dev/rewrite_parquet_layout.py`. Row counts agree across the
three arms on every query.

| Layout | GETs | fetch ops (blocks) | GiB read | vs today |
|---|---|---|---|---|
| today: 256k row groups, row-major | 26,692 | 15,032 | 15.68 | — |
| **64k row groups, blocks of 4** | **25,150** | 13,315 | 17.22 | **-5.8% GETs**, +9.8% bytes |
| 64k row groups, row-major (degenerative) | 95,048 | 52,238 | 17.22 | 3.56× GETs |

The §3 model predicted 25,305 / 23,943 / 98,554 — within 5% of the measured
counts on every arm. Per query (GETs), the queries §5.3 tabulated:

| Query | today 256k | 64k grouped | 64k row-major |
|---|---|---|---|
| Q02 | 352 | 365 | 1,101 |
| Q03 | 792 | 792 | 3,168 |
| Q07 | 0 | 0 | 0 |
| Q10 | 1,503 | 1,188 | 4,752 |
| Q15 | 406 | 396 | 1,584 |
| Q20 | 592 | 348 | 820 |
| Q24 | 1,230 | 1,510 | 3,022 |
| Q31 | 1,594 | 1,584 | 6,333 |
| Q37 | 16 | 17 | 60 |
| Q41 | 20 | 18 | 60 |
| Q42 | 24 | 22 | 75 |

Reading: a grouped block is one fetch op for as many kept row groups as it
holds (Q03: 792 GETs from 396 fetch ops on both 256k and grouped, 1,584 ops
row-major); pruned point lookups read fewer bytes at 64k because they keep
less (Q20: 199 MB → 133 MB); the byte cost of 64k is the larger string chunks
(§5.1: Q21/Q34/Q35 read 833 → 1,019 MB of URL/Title). Q24 on the row-major
64k arm exhausted the loopback's ephemeral ports during the battery
(`Can't assign requested address` after ~90k short-lived connections — a
harness limit, zero retries elsewhere); re-run in isolation it returns the
same 10 rows with 3,022 GETs, which is the figure in the totals above. Wall
times from this harness are not reported: before the accept-backlog fix in
§9.1 the non-grouped arms' 66-way connection fan-out stalled on SYN
retransmits.

### 9.3 Engine timing on local disk, TPC-H [M]

`dev/grouped_layout_tpch_bench.py`: the 22 queries, one untimed warm pass per
arm, then N rounds with every arm per query and the arm order rotated each
round; figure = sum over queries of each query's minimum; result digests
identical across arms for every query. ARM (Apple Silicon), 14 workers, the
three arms written from `testdata/tpch_1` / `tpch_10` by this writer (SF10's
lineitem stays 16 files of ~3.75M rows). This is the local mmap path, which
issues no range GETs, so grouping is expected to be neutral here — the run
checks the layout costs nothing on the engine side and re-measures R.

| Arm | SF1, 5 rounds, cache ON | SF1, 3 rounds, cache OFF (ARM default) | SF10, 3 rounds, cache ON |
|---|---|---|---|
| 256k row groups, row-major | 746.9 ms | 746.0 ms | 5,009.8 ms |
| 64k row groups, row-major | 716.2 ms (−4.1%) | 724.6 ms (−2.9%) | 5,060.2 ms (+1.0%) |
| **64k row groups, blocks of 4** | **715.2 ms (−4.3%)** | **723.6 ms (−3.0%)** | **5,065.6 ms (+1.1%)** |

Reading: grouped and row-major 64k are within 0.2% of each other everywhere —
the layout is invisible to the local path, as designed. 64k vs 256k repeats
§5.5 (SF1 −4%; SF10 a tie within round-to-round noise). [D-4]: with these
16-files-per-table datasets the mapping cache moves SF1 by ~1% either way;
the one-file-per-table penalty §5.5 measured is not exercised here and the
ARM default is unchanged.

### 9.4 ClickBench on local disk, same code, two layouts [M]

The benchmark dataset `scratch/hits_rugo_262k` was rewritten 2026-09-24 to
64k row groups in blocks of 4 (same 99 files, 98,997,497 rows; 9.41 GB vs
8.86 GB at 256k — the §5.1 geometry). A 256k row-major arm was regenerated
from the same source files under the same code and the two were run
interleaved with `dev/grouped_layout_clickbench_bench.py` (43 statements,
warm pass, 3 rounds rotated, sum of per-query minimums; row counts identical):

| Arm | ARM default (mapping cache OFF) | `RUGO_LOCAL_MMAP_CACHE=1` |
|---|---|---|
| 256k row groups, row-major | 14,306 ms | 13,247 ms |
| **64k row groups, blocks of 4** | 15,582 ms (**+8.9%**) | 13,820 ms (**+4.3%**) |

`make clickbench` on the rewritten dataset alone: 14.36 s (cache off), 13.53
s (cache on). The loss is spread across the many sub-200 ms queries (per
row group: 4× the morsels to dispatch, and with the cache off 4× the
open/mmap/munmap cycles — the [D-4] cost), with a handful of large-scan
queries ±5%. This is the local mmap path, which production does not take;
it does say that on this 105-column, string-heavy table R = 64k costs 4–9%
of local wall time where TPC-H (§9.3, §5.5) gains 4%. The row-group size
that is best for the engine is workload-dependent on the local path; the
choice of R = 64k stands as ruled, and this is the number to weigh it
against. The 256k arm was deleted after the run
(`dev/rewrite_parquet_layout.py scratch/hits <dst> --rows 262144 --block 1`
over the 99 files regenerates it in ~10 min).

### 9.5 Two reader regressions found and fixed; the benchmark re-measured [M]

The +3.8 s between the 23-SEP ClickBench (11.76 s) and the first run on the
rewritten dataset (14.36 s) was investigated query by query. Two causes were
in this work and are fixed:

- **Wake-up storm (introduced by the block reader, fixed).** `publish_items`
  woke every blocked puller (`notify_all`) for each published row group; the
  pre-block pipeline woke one. On a local scan, which publishes one row
  group at a time, that was a thundering herd on every submission. Now one
  item wakes one waiter.
- **Plan build scaled with row groups × columns (exposed by 64k, fixed).**
  `open_native_scan_plan` sized its pool by matching projected column names
  against every chunk of every kept row group, building a Python bytes object
  per chunk: 28.2 ms per query on the 64k dataset (1,584 row groups × 105
  columns). The projected chunk indices are now resolved once per file:
  7.4 ms (256k: 1.9 ms).

`make clickbench` (allocator preload, the method behind the 11.76 s),
2026-09-25, ARM, mapping cache off, both fixes built:

| Dataset | Sum of minimums |
|---|---|
| 23-SEP: old rugo 0.9.123 files, 256k, old tree | 11.76 s |
| 256k row-major, today's writer and tree | 12.53 s |
| **64k in blocks of 4 (the benchmark dataset now)** | **13.01 s** (+3.8% vs 256k; was 14.36 s before the fixes) |

The remaining 0.77 s at 256k is 83% ten string-heavy queries (Q19, Q21–Q24,
Q28, Q29, Q33–Q35), each 5–20% slower; the other 33 are level with 23-SEP.
It is NOT dictionary encoding: an interleaved 25-file A/B of today's writer
with and without dictionaries put the dictionary files 34% faster on those
queries (URL/Title LIKE 54–63%). It is not attributable further without the
23-SEP files (replaced by the rewrite) or a bisect of the tree between
580d2fda and now. Methodology note: `dev/grouped_layout_*_bench.py` run
without the allocator preload and read ~8% slower than `make clickbench`;
compare only within one tool.
