# C7 follow-on: skipping the GET on remote hits, and a decoded-vector cache (T2)

Status: Part A BUILT 2026-10-13 (chunk image + plan_block GET skip; test
tests/unit/connectors/parquet_io/test_chunk_cache_get_skip.py). Also built: the prefetch
ledger and the fetch-stage admission wait count only bytes a fetch buys (plan_block runs
before the wait; the coupled lead is charged at its fetch, not at claim); a local small-chunk
pread is skipped on a hit (D-G3); io_scan_diagnostics `chunk_cache_get_skipped_{chunks,bytes}`
and `chunk_cache_pread_skipped_{chunks,bytes}` (A.5). Contest A/B outstanding. **Part B (T2) RULED OUT 2026-10-13** — see "Part B ruling" below.
Date: 2026-10-13
Parent: `docs/C7_PAGE_CACHE_DESIGN.md` (T1 decompressed-page cache, §24-§28 as built).

Two parts:

- **Part A** fixes the known gap in T1 (C7 §28, 2026-10-11): a remote cache hit still pays for the GET.
- **Part B** is the larger change. It caches decoded Draken vectors, so a hit skips decompression, decoding and the slot build: the engine memcpys the cached buffers into vector-owned buffers.

Both parts need the same new hook: a cache lookup **before IO is planned**. Part B's hits skip the GET through that same hook.

---

## 0. What the code does today (facts, with citations)

**Remote fetch path** (`rugo/src/parquet/io_pipeline.hpp`)
- The unit of remote IO is a `FetchBlock` (:1830): one remote file, one or more row groups.
- `fetch_block` (:2879) calls `plan_block` (:2639), then `fetch_remote_groups` (:2705). The latter issues one `Range` GET per coalesced group, concurrently through `get_many`.
- `plan_block` turns each (member, column) slot into an extent `[cstart, cstart + clen)`, or into its page-prune runs. It sorts the extents and merges them greedily: gap ≤ 64 KiB, or waste ≤ 10% of useful bytes, with each group ≤ 8 MiB (opteryx/config.py). **So one GET routinely spans several column chunks and several row groups.**
- `plan_block` is the latest point at which every chunk's key `(path, chunk_offset = min(dict_page_offset, data_page_offset))` is known before any byte is requested. It runs on the fetch pool with fetch-ahead, or on the decoding worker in the coupled path.
- `decode_row_group` (:3249-3330) slices each column out of the group buffers. If a column's chunk is not covered, the slice is null and decode fails loud: "coalesced range fetch did not cover column" (:3682).

**T1 on a hit is not self-sufficient** (`decode_column.cpp`)
- The cache key is built at decode time (`io_pipeline.hpp:3569`). `ChunkPageSource` looks it up inside `DecodeColumnFromChunk` (:533). By then the bytes have already been fetched.
- On a hit the decoder still reads the raw chunk for several things:
  - every page header: `ParsePageHeader` at the cursor (:1869), `PreScanPages` (:436), the dictionary header (:914) and the dict-skip pre-scan (:1320);
  - DATA_PAGE_V2 repetition and definition levels (:2103-2133);
  - any V2 values region stored uncompressed inside a compressed chunk (:2138; `commit` does not cache it, :644);
  - cursor arithmetic for row-mask page skips and page search (:1937-1966), and the jump-plan cursor check (:1894).
- The entry holds decompressed page **bodies**, indexed by chunk-relative offset. It cannot stand in for the raw chunk.
- Doc nit: chunk_cache.h describes the index as "absolute body offset in the file". The code uses chunk-relative offsets (decode_column.cpp:569, :587).

**Decode output → Draken**
- `DecodeColumnFromChunk` fills a reused `DecodedColumn` scratch (AppendBuffers, `draken_malloc`).
- `emit_col` (io_pipeline.hpp ~3354) picks a `DirectKind`, and a `build_direct_*` builds a `ColumnOut` (:237). Fixed values, validity and the plain-string arena are adopted by `release()`. Slots, dict expansion and RLE rebuilds are copied.
- In the engine, `NativeScanColumnBuilder::build_column` (native_parquet_scan_source.hpp:463-708) wraps each `ColumnOut` in a `VectorOwner`. Along the way it:
  - copies strings into `[DrakenStringArena | slots]` blocks (native_varchar_pool_decode.hpp:163-283);
  - deserialises DK_POOL columns (int-backed DECIMAL, ARRAY, pool VARCHAR);
  - applies the per-query transforms: DATE narrowing, TIMESTAMP retag, decimal descriptor, `widen_types`.
- **One morsel = one row group** (`get_morsel`, :1605-1878).
- Vectors own their buffers (`VectorOwner`, draken/core/vector_owner.h): a cached vector is handed to a query by copying, never by sharing.
- The worker prefilter (`pass1_run_predicate`, io_pipeline.hpp:496) already evaluates pushed predicates over **non-owning DrakenVector views** of decoded columns. It would run unchanged over a cached vector.

---

## Part A — skipping the GET on a remote T1 hit

### A.1 The change in one line

Move the lookup into `plan_block`. A chunk that hits is pinned and contributes **no extent**, and its decode reads from the cache entry instead of from fetched bytes. That requires the entry to be **self-sufficient**: decoding must never need the raw chunk.

### A.2 Making the entry self-sufficient — options

| | Entry holds | Decoder change on a hit | Cost |
|---|---|---|---|
| **A-i (recommended)** | A **decompressed chunk image**: a valid Parquet column chunk with codec UNCOMPRESSED. Each page header is rewritten (`compressed_page_size = uncompressed_page_size`, CRC dropped, V2 `is_compressed = false`), followed by the uncompressed body: V1 the whole page, V2 levels + values, raw-uncompressed V2 values copied as-is. Plus a page-offset remap table (old chunk-relative offset → image offset) for jump plans. | **None in the page loops.** On a hit, decode is called with `data = image` and a copy of `ColumnStats` with `codec = 0`, rebased offsets (`dictionary_page_offset`, `data_page_offset`) and `total_compressed_size = image size`. `ChunkPageSource` is not constructed. Jump-plan `page_offsets` are translated through the remap table. | Header rewrite at fill (rugo already has a thrift-compact writer, `_thrift_writer.hpp`). Headers are tens of bytes per page, which is negligible. |
| A-ii | Today's bodies, plus a per-page side table of parsed header fields, V2 level bytes and uncompressed values | Every header-parse site (5+) and every cursor-arithmetic site learns a second "header source". This touches the hottest loops. | Invasive; two code paths for the same walk. |

**A-i keeps a single decode path.** A hit is just "decode an uncompressed chunk", which is already a supported and tested input.

The fill writes in image layout directly, so there is no extra copy. Before a page is decompressed, its raw header is already parsed, and that header carries both sizes. The fill can therefore write the rewritten header and then decompress straight after it:
- serial path: one page at a time;
- parallel path: `PreScanPages` parses every header before dispatch, and `reserve` is already single-threaded.

`commit` keeps its rule: insert only when every page is present.

**Side benefit (local files, also real):** a hit no longer touches the mapped file's pages. That removes the page faults on hits. It also removes the hits' dependency on the OS file cache, which was the binding constraint in the one-process c6a run (C7 §28). This is recorded as a consequence, not as a goal.

### A.3 Planning with hits

In `plan_block`, for each (member, column) slot:
1. Build the key (path up to `?`, chunk_offset). This is the same key as today, moved earlier.
2. `draken_cc_lookup`, using the `predicated` flag the decode would use. On a hit, hold the pin in a new per-slot array on the `FetchBlock` (`cc_pins[slot]`) and **emit no extent** for the slot.
3. The coalescer runs unchanged on the remaining extents. A hit chunk sitting between two misses becomes a gap. The existing gap/waste rule then decides whether to fetch through it (one request, some waste) or split (two concurrent requests in the same `get_many` batch, so no extra serial round trip).
4. Memory admission (`ledger_charge(est_compressed_bytes)`, :2885) charges only the bytes still fetched.

In `decode_row_group`, a slot with a pin decodes from the image. A slot with neither a pin nor a slice fails exactly as today.

**Pin lifetime:** from `plan_block` until the column's decode returns. With fetch-ahead (default depth 64 blocks), a pin can be held for a while. Pinned bytes cannot be given back to query memory (give-way skips pinned entries). The exposure is bounded by the cached bytes of the in-flight window. Today those bytes would have been **fetched into the same window anyway**, so the peak is no worse than a miss.

**Local files:** the same lookup goes at the local decode sites (mmap :3670, pread :3705). On a hit the chunk is never sliced or preaded.

### A.4 Correctness

- Files are immutable for the life of the process (ruling 2026-10-06), so an entry looked up at plan time is still valid at decode time.
- Every output of a hit equals a miss's output, because both are the same decoder over byte-identical page bodies. Test: repeat a remote scan; answers must match a cache-off run byte for byte.
- A remote repeat query whose chunks are all resident must issue **zero data GETs** (the page-index GET is separate). This is asserted through `record_http_fetch` counts, and the test must fail with the change reverted.
- Page prune, row mask, dict skip and page search are unchanged; they run on the image.

### A.5 Telemetry

Add `chunk_cache_get_skipped_chunks` and `chunk_cache_get_skipped_bytes` (compressed bytes not requested).

The cost model's remote term (30 ms + bytes / 60 MB/s, decode_column.cpp:522) currently prices in a saving that never happens. After this change it prices a real one; the model itself is unchanged.

### A.6 Measurement

Use hadro with GCS-like shaping (`--latency-ms 120 --latency-jitter-ms 30 --bandwidth-mbps 480`) in the official shape, with an A/B on the GET-skip alone. Expected effect: hot remote runs lose their GET round trips for cached columns. For a fully cached query, the floor becomes footer + page-index GETs only.

### A.7 Decisions

| ID | Question | Recommendation |
|---|---|---|
| D-G1 | Entry form: chunk image (A-i) or bodies + side table (A-ii) | **A-i** |
| D-G2 | Lookup point: `plan_block` (after page prune, before the GET) | yes |
| D-G3 | Also skip the slice/pread for local hits (same code path) | yes |
| D-G4 | Should pinned in-flight hits be exempt from give-way (bounded by the fetch-ahead window)? | yes — the alternative is to re-fetch bytes we chose not to fetch, at decode time, and fail-loud if that fails |

---

## Part B — T2: cache decoded vectors, loaded straight into Draken

### Part B ruling (2026-10-13): NOT BUILT — the decompressed-page cache (T1) stays the answer

Architect: the scan pushes filters to avoid work (row masks, page pruning, dictionary
skip, page search), so a decoded cache must either hold FILTERED vectors or decode the
whole column to fill; caching decompressed parquet bytes sits below all of those paths.

Census (Mac, one pass per query, fresh process, cache off; temporary hook, removed;
scripts scratch/beauty/census_run.py / census_analyse.py), share of column decode+build CPU:

| Set | unfiltered | predicate col (dict skip / page search) | masked | length-only |
|---|---|---|---|---|
| ClickBench rugo | 47% | 33% | 12% | 8% |
| ClickBench canon | 63% | 25% | 7.5% | 5% |
| TPC-H SF10 | 88% | 2% | 10% | — |

- An unfiltered-only T2 misses 37-53% of ClickBench decode CPU.
- Filtered vectors repeat across DIFFERENT queries rarely (one-process view: predicate
  columns 4-8%, masked ≤3% of decode CPU); they hit only on a repeat of the same query.
- Working set, all keys once: T1 12.9 / 24.0 / 5.1 GiB vs filtered vectors 27.4 / 20.0 /
  5.6 GiB (rugo / canon / TPC-H; TPC-H pool decimals unsized).
- Remote: decode CPU is small against wall (AIStor ClickBench-10 4.3 s of 44 s; TPC-H
  SF10 31 s of 183 s) — the GET (Part A) is the remote cost, not decode.

The sections below are kept as the record of what was considered.

### B.1 What a hit buys

C7 §2 ceiling (canon Q28, i5):

| Cache | CPU removed | Ceiling |
|---|---|---|
| T1 hit | decompression | ≤1.55× |
| T2 hit (decoded values) | decompression + decode | ≤2.2× |
| T2 hit (cached as vectors) | decompression + decode + slot build | ≤2.8× |

Measured T1 hits realise about ⅓ of decompress time (§15), because faults and the DRAM re-read remain. A T2 hit removes the whole decode walk. What is left per column is a memcpy of the cached buffers into owned ones (a gather when a row mask applies).

### B.2 What an entry is

**One entry = a copy of the buffers of one column chunk's finished `VectorOwner`** (a flat blob, B.6), in the engine's canonical physical form. That is the output of `build_column` **before** the per-query transforms (DATE narrowing, TIMESTAMP retag, decimal descriptor, `widen_types`). Those transforms depend on the plan, and they are cheap, so they are reapplied on every hit:
- retag: tag only;
- decimal descriptor: descriptor only;
- DATE int64→int32: an allocation and a pass;
- widen: a cast kernel.

Two forms were considered and rejected:
- **`DecodedColumn` / `ColumnOut` buffers.** Every hit would still repeat the slot build, the dict-code expansion and the scatter.
- **The post-transform vector.** It would key on plan state (declared types, ALTER history) and duplicate entries per plan.

Contents per shape (all already produced today):
- Fixed width: `data_buf`, `validity_buf`.
- Dict (numeric or string): `data_buf` (unique values or `[DrakenStringArena | slots]` block), `codes_buf` (u32 per row), `validity_buf`, `arena_buf` for long strings. Flags as built (`DRAKEN_DICT_KEYS_SORTED`).
- Dense strings: `[DrakenStringArena | slots]` block plus `arena_buf`.
- Optional E37 `keyhash_buf`, cached when the filling decode produced it (D-T2-9).

### B.3 Key

`(path up to '?', chunk_offset, form_id)`

`form_id` is a canonical encoding of the decode configuration that the **plan** chooses for this column and that changes the output:
- the requested `DirectKind` / declared physical width (`build_direct_fixed` narrows INT8/16/UINT* to the declared width);
- `prefer_dict` (derived from type, but recorded explicitly);
- the nested-assembly plan for STRUCT/MAP JSON (if admitted, D-T2-6);
- DK_POOL vs direct.

Anything the **chunk** decides (dict vs plain pages, a mixed chunk materialised dense, RLE skip-dense) is deterministic for that chunk and form, so it is not part of the key.

A plan whose form differs gets a miss. That is correct; it is never a wrong answer.

### B.4 Which decodes may FILL

Output is a pure function of (chunk, form) **only for an unpredicated full decode**. The modes that change output (from the code map):

| Mode | Output differs? | Fill T2? | Serve from T2? |
|---|---|---|---|
| none (full decode) | — | **yes** | yes |
| prefilter, non-predicate columns (decoded full-length, gathered later by `survivor_mask`, io_pipeline.hpp:3765) | no — full-length | **yes** | yes, memcpy (B.6) |
| `row_mask` (page-index prune, row admission, page-search mask, pass-2/latmat) | yes — survivors only | no (see D-T2-3) | yes, gathered by the mask (B.6) |
| `PageJumpPlan` | as row_mask; pages skipped | no | yes, gathered by the mask; **and the column's GET is skipped (Part A hook)** |
| `skip_pred` (dict prune → `dict_all_filtered`) | yes — the row group may vanish | no | **declined** for that column in the first build (D-T2-4); it takes the T1 path |
| `PageSearchOut` (LIKE on raw pages) | yes — survivors only, mask produced | no | **declined** for that column (D-T2-4); T1 serves it, which is what makes Q22 hot 190 ms |
| `length_only` (string stubs) | yes — payload elided | **no** (never admit stubs) | yes — a full vector is a superset; consumers already honour `payloads_elided = 0` |
| range settlement (predicate columns placeholders) | placeholders, no decode | no | n/a (nothing decoded) |
| absent column (schema evolution) | null constant | no | n/a |

Rule: **fill only from an unpredicated full-length decode. Serve wherever the decode for that column would have carried no raw-byte evaluation** (no skip_pred, no page search). Row masks are applied to hits by gathering out of the cache.

### B.5 Planning with T2 hits (shares Part A's hook)

In `plan_block` and at the local decode sites, look up T2 first, then T1:
1. T2 hit for the slot's form, and the slot carries no skip_pred or page search → pin; **no extent, no decode**. The pipeline puts the pinned handle in the `MorselRef` column slot (a new `DK_CACHED` kind).
2. Otherwise a T1 hit → Part A.
3. Otherwise a miss → fetch and decode as today. If the decode is fill-eligible (B.4), the engine fills T2 after `build_column`.

**Fill = one memcpy into the cache.** Vectors own their buffers, so the cache never shares memory with a query. After `build_column` produces the canonical owner (B.2), a fill-eligible column is copied into one cache-owned blob (`malloc`, accounted as CACHE bytes like T1's fill buffer). The query keeps its own owner untouched. The cold run pays one extra pass over the column's bytes.

### B.6 Serving a hit to the engine — memcpy into owned buffers

An entry is a **flat blob**: every buffer of the canonical owner laid end to end, plus a small POD descriptor:
- the `DrakenVector` template (type, `length`, `data_length`, flags);
- each buffer's offset and length in the blob: data, validity, codes, string arena;
- the logical-type id.

That is the same shape as a T1 entry (bytes + index). The draken C ABI carries no C++ types, and the cache stays in draken_native.so.

`build_column` for a `DK_CACHED` slot:
- **No row mask:** for each buffer in the descriptor, `draken_malloc` + `memcpy` from the blob. Build `VectorOwner{vec, data_buf, validity_buf, codes_buf, arena_buf}` exactly as the decode path does. Unpin. Then the per-query transforms apply as on a miss.
  - Dense and constant selections point at the process globals (`draken_identity_sel` / `draken_zero_sel`), never into the blob.
  - Strings: the `[DrakenStringArena | slots]` block holds **absolute** pointers (`slots`, `arena`, `null_bitmap`, buffers.h:137-147). After the copy they are re-pointed at the new allocations. Slot `arena_offset`s are relative and copy as-is.
- **Row mask (or survivor gather):** a gather from the blob into survivor-sized owned buffers, in one pass:
  - fixed-width values by survivor index;
  - dict shapes: dictionary payload `memcpy`'d, codes gathered (4 B/row);
  - validity re-projected;
  - dense strings: slots gathered, arena `memcpy`'d whole (relative offsets stay valid). Under a selective mask that copies unused bytes; compacting instead is an arm to measure, not a default.
  
  The output is an ordinary owned vector of the shape a masked decode produces today.
- **Morsel row set:** every column of a morsel must cover the same rows. The `MorselRef` must therefore carry the row mask the decoded columns were reduced by. Today it does not, and that is a new field. Hit columns are gathered by that same mask.
- **Worker prefilter on a hit predicate column:** `pass1_run_predicate` builds non-owning `DrakenVector` views from `ColumnOut`. For a `DK_CACHED` column it builds the view over the pinned blob, which is the same predicate kernel over the same values. The pin is held until the column is copied.

**Pin = for the duration of the copy only**, like T1's `ChunkPageSource`. Nothing a query holds points into the cache, so eviction, give-way and process lifetime need no new rules. Cached entries cannot be mutated by query code (no in-place retag risk), and morsels that outlive the query own their bytes.

**What a hit costs:** one read of the cached bytes plus one write of the same bytes (fresh allocations; first-touch page faults on large buffers). That is a memory-bandwidth floor of about 2 × column bytes / bandwidth: the same order as a T1 hit's DRAM read, without the decode walk on top. This makes B.9's size question also a speed question: a column whose T2 form is 4× its T1 form copies 4× the bytes. The ceiling step (B.10.2) measures copy vs decode per column.

### B.7 Immutability

Entries are never handed out, only copied from, so in-place mutators (`cxx_column_retag_timestamp64`, the skene retag) only ever touch query-owned vectors. No tripwire is needed.

### B.8 Where it lives

The policy is the same as T1's, and T1 just passed leave-one-out on it: S3-FIFO probation, cost-weighted CLOCK, give-way, 16 shards.

**Recommended:** extend draken's chunk cache with a second entry kind rather than build a second cache. One budget, one give-way, one set of counters split by kind.
- The C ABI gains `draken_cc_lookup_vec` / `draken_cc_insert_vec`. These take and return the blob plus its descriptor, the same way T1 takes bytes plus a page index.
- The single-instance rule holds: the cache lives in draken_native.so only.

**T1 and T2 coexist:**
- When a T2 entry for a chunk is admitted, drop the T1 entry for the same chunk; it is redundant for unpredicated reads.
- T1 stays the server for predicated columns (dict skip, page search) and for form mismatches.
- Whether T1 still earns its place is settled by leave-one-out once T2 is built (build all, then ablate).

**Cost weighting:** a T2 entry's cost = measured decompress + decode + build_column ns, minus the copy-out cost, plus the remote re-fetch term.

### B.9 Memory — the open risk

A T2 entry can be **larger** than the T1 entry for the same chunk:
- A low-cardinality dict column's pages hold bit-packed codes (≈0.1-1 B/row). T2 holds u32 codes, 4 B/row (§11 fixes selection at u32).
- Plain ints with delta or bit-packing also grow to 8 B/row.
- Long strings are close: the T1 plain page is 4 B length + bytes; T2 is a 16 B slot + arena bytes.

Rough canon arithmetic (100M rows): each dict column costs ~400 MB of codes in T2. The §2 suite-union figure (24.83 GiB decompressed) is the T1 number; the T2 number is unknown.

**Step 1 of the build is a census, not code:**
- per column, T1 bytes vs T2 bytes over the canon and rugo ClickBench files and TPC-H SF1;
- per query, the T2 working set against the 60% − 4 GiB budget on a 32 GiB c6a (14.39 GiB) and on prod's 16 GiB worker (5.6 GiB).

If T2 does not fit where T1 did, the cost-per-byte weighting decides what stays. That is a measurement, not a guess.

### B.10 Measurement plan

1. **Census** (B.9). Decide go/no-go on memory before writing code.
2. **Ceiling without the cache:** a scratch harness times `build_column` + decode vs the memcpy / masked gather from a blob for Q21-Q28 columns on Mac and i5. That bounds what a hit saves per column (the §2 ceilings are Q28-only and CPU-share-based).
3. **A/B, official shape** (process per query, 3 tries), c6a + i5 + Mac:
   - arms: T1 only vs T1+T2, hot and cold;
   - remote run on shaped hadro.
4. **Leave-one-out:**
   - T2 without T1;
   - T1 without T2;
   - masked hits: dense-string arena memcpy'd whole vs compacted.

### B.11 Decisions

| ID | Question | Recommendation |
|---|---|---|
| D-T2-1 | Entry = a copy of the canonical `VectorOwner`'s buffers from `build_column`, before per-query transforms | yes |
| D-T2-2 | Key = (path, chunk_offset, form_id); chunk-decided shape not in the key | yes |
| D-T2-3 | Fill under a row mask: (a) never — fill only from full-length decodes (prefilter non-predicate columns already are); (b) on a fill-eligible miss, decode the full chunk anyway (ignores page prune, fetches more, slower cold) | **(a) first**, then measure (b) as a separate arm. (b) follows the "hot is where the wins are" ruling, but it enlarges remote GETs on page-pruned scans. |
| D-T2-4 | Columns with a raw-byte evaluation (dict skip, page search) decline T2 and use T1 | yes for the first build. Serving them needs a vector-form evaluation with identical semantics, and the planner must then guarantee the conjunct is still evaluated. |
| D-T2-5 | Hits are copied out: unmasked = memcpy per buffer; masked = one gather into survivor-sized owned buffers (dense-string arena memcpy'd whole; compaction measured as an arm) | yes (architect direction 2026-10-13: vectors own their buffers) |
| D-T2-6 | Admit DK_POOL (int-backed DECIMAL, ARRAY) and nested STRUCT/MAP JSON | DK_POOL yes (it is a VectorOwner after build_column). STRUCT/MAP later (multi-leaf key). |
| D-T2-7 | One cache, two entry kinds, one budget; T2 admit drops the chunk's T1 entry | yes |
| D-T2-8 | Entry = flat blob + POD descriptor in draken's chunk cache (no C++ types across the C ABI) | yes |
| D-T2-9 | Cache the E37 keyhash sidecar when present | yes if present at fill; a hit without it recomputes as today |
| D-T2-10 | Order of work: Part A first (small, closes a known gap, gives the plan_block hook) → T2 census → T2 build | yes |

### B.12 What this does NOT change

- No Python on the path. The lookup, copy-out and fill are native; Python only sets the budget, as today.
- No new vector shape or ABI change. DrakenVector is untouched; the only additions are a new `DirectKind` (`DK_CACHED`) and the row-mask field on `MorselRef`.
- No fallbacks. A form mismatch is a miss. A missing pin with no fetched bytes fails loud exactly as an uncovered range does today.
