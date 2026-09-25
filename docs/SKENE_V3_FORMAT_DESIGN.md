# `.skene` v3 — whole-file column-major layout, footer-only pruning, draken-family sketches

**Status: DESIGN — revision 2, 2026-09-24. APPROVED 2026-09-24 with the §11 recommendations. IMPLEMENTED 2026-09-24.** The normative v3 byte layout is `skene/FORMAT.md`; where the two differ, FORMAT.md is right.
Rulings are marked **[RULED]** with the date; the decisions are **[D-n]** and
collected in §11. `skene/FORMAT.md` was rewritten as the v3 normative spec on
2026-09-24 (v2's text frozen in `skene/FORMAT_v2.md`); this document keeps the
rationale.

v2 (`skene/FORMAT.md`, `docs/SKENE_FILE_FORMAT_DESIGN.md`) is the baseline and
is not re-argued. The parquet sibling of this change is
`docs/PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md`; where the two formats share a
ruling it is cited from there.

---

## 1. Why now

The production floor is GCS round-trips, not bytes: a range GET costs 110-150
ms whatever it moves (`performance_floor_local_1ms_prod_is_gcs_round_trips`).
64k-row row groups are the measured engine winner at SF1 and SF10 (parquet
design §5.5), but with the fetch unit tied to the row group they cost ~4x the
requests of 256k (parquet design §3: 25,305 → 98,554 GETs on the ClickBench
battery).

A column-major physical layout separates the two: the row group stays the
decode, claim and statistics unit, while a column's bytes for many row groups
are one contiguous range. Parquet gets this per 256k-row block, because a
parquet reader must infer grouping from offsets and the parquet writer is
held to one buffered block. Skene owns both ends and records what it did, so
it can go further: **the whole file is column-major**, and the model in the
parquet design shows why that matters — 64k row groups in 4Mi-row blocks
issue 4,731 GETs against 23,943 in 256k blocks.

## 2. Requirements and rulings

| # | Ruling | Source |
|---|---|---|
| R1 | **Whole-file column-major** DATA region. Goal: fewer read requests. | architect 2026-09-24 [RULED] |
| R2 | **G written in the footer**, known at write time, immutable, "even if we don't use it later". G = block size in row groups = 256k / R = **4**. | architect 2026-09-24 [RULED] |
| R3 | **One footer.** Row group footers are gone. | architect 2026-09-24 [RULED] |
| R4 | Directory organised **per column**. | architect 2026-09-24 [RULED] |
| R5 | **Pruning decisions for the whole file come from the footer alone** — per-(column, row group) statistics live there; the footer stays small enough for the 128 MiB plan-time footer cache. | architect 2026-09-24 [RULED] |
| R6 | Sketches move to **draken's `Vector.hash()` family** so plan-time can union them with catalog and ANALYZE sketches. **One sketch per column per file.** | architect 2026-09-24 [RULED] |
| R7 | **v2 readable, v1 dropped.** Window [2, 3]; migrate v2→v3 only. | architect 2026-09-24 [RULED] |
| R8 | Benefit measured **on disk**: request count and bytes fetched. | architect 2026-09-24 [RULED] |
| R9 | Writer is **two-pass through a scratch file**; memory bounded to one row group of plans plus the directory. | architect 2026-09-24 [RULED] |
| R10 | Row group **R = 64k**, files **4 GiB**, rows per file variable, last block may be partial. | parquet design §3 [RULED], applied to skene |
| R11 | Accelerators (bloom, zone map) in the **file tail**, before the footer. | parquet design §2 [RULED], applied to skene |
| R12 | Skene's range **coalescer is skene's own** (own variables), same merge rule as parquet's. | architect 2026-09-24 [RULED] |
| R13 | Engine **work item = one block** of G row groups, aligned at multiples of G. | architect 2026-09-24 [RULED] |
| R14 | Nothing else is bundled unless the architect says so. | no ruling — **[D-1]** |

## 3. File structure

```
 byte 0
 ┌────────────────────────────────────────────┐
 │ HEAD                             16 bytes  │  magic FIRST, version 3
 ├════════════════════════════════════════════┤
 │ DATA region, per column in schema order:   │
 │   column 0  DIRECTORY BLOCK                │  chunk records + section entries
 │   column 0  chunk rg0 │ chunk rg1 │ …      │  one column = one contiguous
 │   column 1  DIRECTORY BLOCK                │  range: its directory then all
 │   column 1  chunk rg0 │ chunk rg1 │ …      │  its chunks, row group order
 │   …  (an ARRAY child follows its parent)   │
 ├────────────────────────────────────────────┤
 │ INDEX region (tail accelerators)           │  optional sections, same
 │   column 0 : rg0 │ rg1 │ …   column 1 : …  │  column-major order
 ├════════════════════════════════════════════┤
 │ FILE FOOTER                footer_bytes    │  everything a pruner needs
 ├────────────────────────────────────────────┤
 │ TAIL                             24 bytes  │  magic LAST
 └────────────────────────────────────────────┘
```

A **chunk** is one column's required sections for one row group — exactly
what v2 wrote inside a row group's DATA region for that column. Nothing inside
a chunk changes: section kinds, encodings, codecs, the four string lanes,
64-byte alignment and per-section checksums are v2's.

A **block** is G consecutive row groups, `[k·G, (k+1)·G)`. Blocks are the
engine's work item (R13); the layout does not depend on them, because a
column's chunks are contiguous across the whole file.

Ordering invariants, all validated by the reader (§7.4):

1. Columns in schema (depth-first) order; each column's directory block
   precedes its chunks; chunks in row group order; chunk `i` ends at or before
   chunk `i+1` begins.
2. Column extents do not overlap and are ordered by column.
3. Every section body starts 64-byte aligned (v2 §3, unchanged).

### 3.1 What a reader fetches

After pruning on the footer, for each work item (file, block) and each column
in the read set: the column's chunks for the block's surviving row groups are
one range by construction. Ranges for one work item go through the skene
coalescer (§7.2), which merges adjacent ranges across columns under a waste
ratio and a span cap, exactly as parquet's does. A column's **directory block**
is fetched once per (file, column) and is adjacent to chunk 0, so for a work
item containing block 0 it merges into that column's first range at zero
waste; otherwise it is one extra range on the first work item that needs the
column.

Whole-file contiguity means a **work item may span several blocks** later
without any layout change — when a column is narrow and a byte budget allows
it — which is the request reduction parquet cannot reach. Not built now: the
work item is one block (R13).

## 4. Head and tail

Unchanged in layout. `version` is `3` in both. Bytes 0-5 remain frozen.

## 5. The footer

One flat byte stream, parsed sequentially, checksummed as a whole by the tail.
`footer_version` becomes `3`. Sized so that reading it decides everything
about the file (R5): which row groups survive, which columns are wanted, and
where every block of every column is — with no second metadata fetch before
data can be requested.

Order:

1. **File footer header** — 64 bytes (§5.1)
2. **Writer tag** — `writer_tag_bytes`
3. **Row group table** — `row_group_count` × 16 bytes (§5.2)
4. **Schema directory** — v2 §5.3, unchanged
5. **Cluster spec** — v2 §5.3b, unchanged
6. **Column summaries** — one per column, depth-first (§5.3)
7. **Per-row-group statistics** — column-major (§5.4)

### 5.1 File footer header — 64 bytes

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | `footer_magic` | `SKNI`, unchanged |
| 4 | 2 | `footer_version` | `3` |
| 6 | 2 | `reserved` | `0`, checked |
| 8 | 8 | `row_count` | total rows |
| 16 | 4 | `row_group_count` | ≥ 1 |
| 20 | 4 | `column_count` | top-level columns |
| 24 | 16 | `file_uuid` | |
| 40 | 8 | `created_at_unix_us` | provenance |
| 48 | 4 | `writer_tag_bytes` | |
| 52 | 4 | `block_row_groups` | **G** (R2). `≥ 1` (a file with fewer row groups has one partial block). Written `4`. |
| 56 | 8 | `data_region_bytes` | DATA + INDEX extent from byte 16 to the footer; checked against `footer_offset - 16` |

### 5.2 Row group table entry — 16 bytes

| offset | size | field |
|---|---|---|
| 0 | 8 | `row_count` |
| 8 | 8 | `first_row` |

v2's `data_offset` / `data_bytes` / `footer_*` have no meaning when a row
group is not contiguous. Reader checks: `first_row` is the running sum; row
counts sum to the header's; every column's chunk `i` declares
`length == row_count[i]`.

### 5.3 Column summary

Per column in schema order, ARRAY child nested after its parent. This is what
lets the planner address a block of a column from the footer alone.

**Head — 64 bytes**

| offset | size | field | notes |
|---|---|---|---|
| 0 | 8 | `directory_offset` | absolute; the column's directory block |
| 8 | 4 | `directory_bytes` | |
| 12 | 4 | `reserved0` | `0`, checked |
| 16 | 8 | `directory_checksum` | XXH3-64 over the directory block — recorded here because the footer is the only trusted thing a ranged reader holds when it decides to fetch a directory (the v2 argument for `RowGroupEntry.footer_checksum`) |
| 24 | 8 | `data_offset` | absolute start of the chunk run |
| 32 | 8 | `data_bytes` | the chunk run, all row groups, this column only |
| 40 | 8 | `index_offset` | its optional sections in the INDEX region, `0` if none |
| 48 | 8 | `index_bytes` | |
| 56 | 4 | `block_count` | `ceil(row_group_count / G)` |
| 60 | 4 | `child_count` | `0`, or `1` for ARRAY |

**Then `block_count` block extents — 16 bytes each**

| offset | size | field |
|---|---|---|
| 0 | 8 | `offset` — absolute start of chunk `k·G` |
| 8 | 8 | `bytes` — through the end of chunk `min((k+1)·G, row_group_count) - 1` |

Block extents are derived from the chunks and are **checked against the
directory block** when it is parsed; they exist so a fetch can be planned
before the directory is read.

**Then the sketch** (§6): `hash_family u8 │ reserved u8 │ k u16 │ count u32 │
u64[count]`. One per column per file, built over the whole file. Absent
(`count == 0`, `k == 0`) means not tracked.

**Then the child**, recursively.

Identity (name, field_id, type, logical descriptor) is not repeated: the
schema directory carries it once per column. v2 repeated it per row group so a
row group footer was self-describing; there is no such footer now.

### 5.4 Per-row-group statistics

Column-major: for each column (depth-first), for each row group, a `u32`
length then that many bytes of `ColumnStatistics`. `0` means NOT TRACKED.
Prefix-first growth rule unchanged. The blob is v2's 56 bytes; the sketch is
no longer appended here.

### 5.5 Column directory block

Located by the column summary, fetched with the column's data, checksummed
against the footer. Holds everything decode needs and nothing pruning needs.

**Header — 16 bytes**: `magic u32` (`SKNC`), `node_ordinal u32` (depth-first
index over column nodes, ARRAY children included, checked against the summary
that named it), `chunk_count u32` (MUST
equal `row_group_count`), `section_count u32`.

**Then `chunk_count` chunk records — 64 bytes each**, row group order:

| offset | size | field | notes |
|---|---|---|---|
| 0 | 4 | `length` | logical rows; MUST equal the row group's `row_count` |
| 4 | 4 | `data_length` | physical values |
| 8 | 1 | `vector_flags` | verbatim |
| 9 | 1 | `selection_kind` | |
| 10 | 1 | `value_order` | |
| 11 | 1 | `string_payloads_elided` | |
| 12 | 4 | `section_index` | into this block's section list |
| 16 | 4 | `section_count` | required sections in this chunk |
| 20 | 4 | `index_section_index` | |
| 24 | 4 | `index_section_count` | |
| 28 | 4 | `reserved0` | `0`, checked |
| 32 | 8 | `string_slot_count` | |
| 40 | 8 | `string_arena_used` | |
| 48 | 8 | `string_arena_cap` | |
| 56 | 8 | `reserved1` | `0`, checked |

**Then `section_count` section entries** — v2's 48-byte `SectionEntry`,
unchanged, offsets absolute. A required entry MUST lie inside this column's
`[data_offset, data_offset + data_bytes)`; an optional one inside
`[index_offset, index_offset + index_bytes)`. This replaces v2's "inside THIS
ROW GROUP's extent" rule with "inside THIS COLUMN's extent" — the same
defence against a section addressing another column's bytes (which a
per-section checksum cannot catch) at the granularity the layout now has.

### 5.6 Sizes

ClickBench `hits` shape, 105 columns, 4 GiB file at 64k rows ≈ 555 row groups:

| region | per file | fetched |
|---|---|---|
| file footer: stats 105 × 555 × 60 B, summaries + block extents + sketches ≈ 0.3 MB | **≈ 3.6 MB** | always, at plan time; cache holds ~35 files (parquet at 64k: 6.2 MiB, 21 files) |
| one column directory block: 555 × (64 + ~4 × 48) B | ≈ 140 KB (string columns ≈ 220 KB) | per projected column, with its data |
| all directory blocks | ≈ 17 MB | never all at once unless `SELECT *` |

TPC-H lineitem, 16 columns, 4 GiB ≈ 915 row groups: footer ≈ 0.9 MB; a
directory block ≈ 230 KB.

The first v3 draft put directories and per-row-group sketches in the footer;
that was ~36 MB on the `hits` shape, 3.5 files in the cache. Moving the
directory next to its column and the sketch to per-file is what brings it to
3.6 MB.

## 6. Statistics sketch — draken hash family (R6)

Today's sketch is `KmvHashFamily::kXxh3ValueBytes` (`draken/core/kmv_sketch.h`),
skene's own dedup hash, chosen so the sketch and the value-order dedup could
not disagree. The architect wants skene sketches **unionable with catalog and
ANALYZE sketches**, which are `kDrakenVectorHash` (`draken_hash` /
`simd_hash_i64`).

1. The sketch record carries `hash_family` (a `KmvHashFamily` value), written
   `2`. The retained v2 reader reports its sketches as family `1`. The
   `FileMetadata` / Python surface carries the family so `skene_io`'s
   `merge_min_k` refuses a cross-family union instead of computing one.
2. **One sketch per column per file**, built across every row group at write
   time. A KMV union is exact, so this is the same number `skene_io` computes
   today by merging per-row-group sketches — at one row group's worth of
   footer bytes instead of hundreds. Per-row-group NDV remains as the scalar
   in each stats blob (`ndv`, `NDV` / `NDV_EXACT` flags), which is what
   row-group pruning and the estimator's per-row-group paths read.
3. The writer hashes with draken's per-type hash; the value-order **dedup**
   keeps its bit-pattern key (v2 §7.6 — `-0.0` and `0.0` must remain distinct
   values in `data`). Consequence: the NDV *estimate* and the dedup count can
   disagree where draken canonicalises (fp16 patterns, int64-decimal against
   an equal DECIMAL128, INTERVAL normalisation). `NDV_EXACT` still comes from
   dedup; only the estimate takes draken's notion of distinct — the notion the
   planner uses, which is the point.
4. **[D-2] Nulls.** `Vector.hash()` emits `NULL_HASH` per null row and the
   catalog/ANALYZE sketches include it; skene's sketch sees non-null rows only.
   Recommendation: skene feeds the sentinel once when the column has any null
   row, so the sketch describes the column the way every other sketch in the
   system does.
5. **Build:** `simd_hash_i64` is a compiled TU (`draken/simd/simd_hash.cpp`).
   `skene/Makefile` and `build_common.skene_extensions()` link declarations
   only today; both gain the TU. No runtime dispatch dependency.

## 7. Reader

### 7.1 Version window and migration (R7)

- `kVersion = 3`, `kMinReadVersion = 2`. `reader_v1.{h,cpp}` and
  `tests/fixtures/v1/` are **deleted**; `test_migration.cpp` is rewritten
  against v2 fixtures.
- **Sequencing obligation:** golden **v2 fixtures are generated by this
  tree's v2 writer before the writer changes**, as `dev/skene_gen_v1_fixtures.cpp`
  did at dc5c7aaf. A one-shot `dev/skene_gen_v2_fixtures.cpp` is the first
  code change, and the fixture bytes are committed artifacts. Set: every
  family, all selection kinds, mandatory descriptors, value ordering accepted
  and declined, multiple row groups, all three codec postures, a
  sketch-bearing column, a cluster spec.
- `reader_v2.{h,cpp}` becomes the retained reader. The v2 forms v3 changes
  (`FileFooterHeader`, `RowGroupEntry`, `ColumnEntryHead`,
  `RowGroupFooterHeader`, `ColumnSketchHeader`) are **frozen as copies inside
  `namespace v2`**; format.h describes v3 only.
- `migrate_file` reads v2 row group by row group through the retained reader
  and writes v3 through the two-pass writer. Provenance carried, output
  re-read and compared, as today. Sketches are **recomputed** under family 2
  (the data is being re-read anyway); a v2 file's family-1 sketches are not
  carried. **[D-3]**

### 7.2 Fetch planning — the new API

```cpp
struct FetchRange { uint64_t offset, bytes; };          // one request

struct FetchPolicy {                                     // skene's own (R12)
    double   waste_ratio = 0.10;   // skene_io_coalesce_waste_ratio
    uint64_t max_bytes   = 8 MiB;  // skene_io_coalesce_max_bytes
};

// The ranges to fetch for ONE work item: the read-set columns over the
// surviving row groups of block `block`, plus each column's directory block
// the first time this file needs it. Pure — no IO. Coalesces like parquet's
// build_remote_plan: sort by offset, merge while cumulative waste <= ratio ×
// useful and span <= max_bytes.
Status plan_fetch(const FileReader&, const std::vector<std::string>& columns,
                  uint32_t block, const std::vector<uint32_t>& row_groups,
                  bool need_directories, const FetchPolicy&,
                  std::vector<FetchRange>* out);
```

The variables are skene's (`skene_io_coalesce_waste_ratio`,
`skene_io_coalesce_max_bytes`), defaulted to parquet's measured values.

### 7.3 Decode from ranges

```cpp
struct FetchedRange { uint64_t offset, bytes; const uint8_t* data; };

// Directory blocks are parsed (and checksum-verified against the footer)
// from fetched bytes once per (file, column); the reader caches them.
Status attach_directory(FileReader&, uint32_t column, const FetchedRange&);

Status read_morsel(const FileReader&, uint32_t row_group, const ReadOptions&,
                   const std::vector<FetchedRange>& buffers, CxxMorsel* out);
```

The resolver maps a section's absolute offset into whichever fetched range
covers it and fails loud when none does. The whole-file entry points remain
as the degenerate case (one range covering the file, every directory attached
at open), so tests, Python and `migrate` keep working.

`read_metadata` returns the whole footer: schema, row group table, cluster
spec, per-column summaries with block extents and sketch, per-row-group
statistics. `read_row_group_metadata` is kept for the Python surface and
reads the directory blocks it needs.

### 7.4 Validation order (delta to v2 §11)

7. Row group table: `first_row` running sum; row counts sum to the total.
8. Column summaries: directory and data extents inside the DATA region,
   index extents inside the INDEX region, non-overlapping, ordered by column;
   each directory immediately precedes its data; `block_count` correct;
   block extents inside the data extent, ordered, contiguous.
9. Per directory block, when fetched: checksum against the summary; magic;
   `node_ordinal`; `chunk_count == row_group_count`; every chunk's `length`
   equals its row group's `row_count`; every section inside its column's
   extent; chunk `i` sections precede chunk `i+1`; the block extents recorded
   in the footer equal `[first chunk offset, last chunk end)`.
10. Unchanged structural checks per chunk (selection kind vs counts, code
    bounds, string invariants, array offsets), per-section checksums before
    use.

## 8. Writer — two-pass (R9)

`FileWriter` keeps `begin → add_row_group* → finish`.

**Pass 1, `add_row_group`.** Encodes every column exactly as today (value
ordering, statistics, encodings, codec) and appends each finished section to
a **scratch file** in arrival order, recording per chunk its section entries
with scratch offsets. Only the chunk records, section entries, per-row-group
stats and the running per-column sketches stay in memory. Peak memory is one
row group of plans plus the directory — the same order as parquet's one-block
buffer, without the block being a layout unit.

**Pass 2, `finish`.** For each column in schema order: assign the directory
block's offset, then walk its chunks in row group order copying each section
from scratch to the output at a 64-byte aligned offset, rewriting the section
entry's offset as it goes; write the directory block first (its size is known
from the record counts, so its offset is fixed before the chunks are copied);
compute its checksum. Then the INDEX region, then the footer, then the tail.
Scratch is deleted. The copy is sequential per column with random reads of the
scratch file; the scratch file is written once and read once.

Cost: one extra write and read of the file's bytes on local disk, for a file
written once and read many times. Needs scratch space wherever skene files
are written (dev box; compaction worker). The scratch path is a `WriteOptions`
field with no default — a caller states where it may write.

Cluster verification, schema enforcement, field ids, provenance and the
`write_morsel` single-row-group wrapper are untouched.

**File size and packing (R10).** Skene has no SQL write path; files come from
`dev/parquet_to_skene.py` and the C++ CLI. Their packing constants move to
R = 65,536 rows per row group and a **byte target of 4 GiB** per file, rows
per file variable. The Makefile mirror stamps change with the layout, so the
stamped mirrors regenerate and best-ever times re-baseline.

`patch.cpp` (DROP / RENAME / ADD by rewriting footers) is reworked for the
single footer. Under column-major, DROP omits a column's extent, ADD appends
one, RENAME touches the schema directory only — all simpler than today.

## 9. Engine

`SkeneClaimSet::build` already opens one `FileReader` per file and prunes row
groups on footer statistics. v3:

- **Work items are blocks** (R13): `(file, block)` with the block's surviving
  row groups. Alignment is at multiples of G, so parquet and skene batch the
  same rows.
- Per work item, the worker that claimed it runs `plan_fetch` for the read
  set (projection ∪ predicate columns), reads the ranges with **`pread`** into
  its own buffers (R8 — a request is a real, countable read; the same planner
  output is what a GCS fetcher would issue), attaches any directory blocks the
  file has not seen, then decodes the block's row groups one at a time and
  emits each as now, pushed predicate included.
- **[D-4] Decode balance.** The 64k win at SF1 came from 4x more *decode*
  claims (`skene_row_group_size_64k_wins_and_reader_pays_per_rg_in_file`). A
  block work item that fetches and decodes all G row groups on one worker
  gives that back on lineitem at SF1 (92 row groups → 23 items). Proposed:
  after a worker fetches a block it pushes the block's row-group decode items
  to a shared queue and workers drain that queue before claiming another
  block. Fetch stays per block; decode claims stay per row group. On local
  disk the fetch is microseconds so this is measurable only as balance; on
  GCS it is the fetch/decode decoupling the parquet path already has.
- Latmat pass 1 and pass 2 plan their own read sets the same way.
- Telemetry: `skene_requests_issued`, `skene_bytes_fetched`, and
  `skene_directory_fetches` per scan.

Not in scope: a remote fetcher, fetch-ahead, multi-block work items. Each is a
separate decision.

## 10. Python surface

- `read_metadata` returns the v3 footer shape: block extents and one sketch
  per column, per-row-group statistics without sketches, `hash_family`.
- `skene_io.skene_aggregate_row_group_statistics` no longer unions
  per-row-group sketches; it takes the file sketch and checks the family. The
  scalar merge path is unchanged.
- `SkeneWriter` gains the scratch-path option.

## 11. Open decisions

| # | Decision | Recommendation |
|---|---|---|
| D-1 | Nothing else bundled (chunk-aligned section bodies, uninflatable allocation bound, zero-row CONSTANT round-trip, DECIMAL128 min/max, ARRAY offset encoding all stay out) | confirm |
| D-2 | Null sentinel in the draken-family sketch | feed it once when `null_count > 0` |
| D-3 | Migration recomputes sketches rather than carrying family-1 ones | recompute |
| D-4 | Decode items queued per row group after a block fetch | yes, measured at the A/B |
| D-5 | Directory block adjacent to its column vs all directories in the tail | adjacent — one range for a projected column's metadata and data |

## 12. Measurement plan (R8)

Interleaved A/B, min of 3, same box, jemalloc preload as the bench targets do:

- Mirrors: TPC-H SF1 and SF10 and the `hits` ClickBench mirror, written v2
  (64k × 16 per file) and v3 (64k, 4 GiB) from the same parquet.
- Metrics per query: `skene_requests_issued`, `skene_bytes_fetched`, exec ms,
  scan operator self time.
- Safety property: bytes fetched per query is **identical between runs** of
  the planner and, at `waste_ratio = 0`, equal to the sum of the surviving
  chunks' stored bytes. Any difference is a bug.
- Expected: requests fall by the row-groups-per-block factor on full scans
  against a per-row-group fetch, and the planner output can be fed to the
  parquet design's model (`scratch/grouped_layout_model/`) for the GCS-shaped
  estimate.
- Local exec time is expected to move little; the local floor is ~1 ms.

## 13. Delivery order

1. `dev/skene_gen_v2_fixtures.cpp`; generate and commit v2 fixtures. **Before
   anything else.**
2. `FORMAT.md` rewritten as v3; `format.h` v3 structs; v2 forms frozen into
   `reader_v2`.
3. Writer: two-pass, single footer, directory blocks, G, per-file sketch,
   draken hash + linkage.
4. Reader v3: `read_metadata`, `plan_fetch`, `attach_directory`, `read_morsel`
   from ranges; window [2, 3]; `reader_v1` deleted; migration v2→v3; tests,
   reject-suite and fuzz corpus regenerated; `patch.cpp` reworked.
5. Python binding and `skene_io`.
6. Engine: block work items, `pread` fetch, decode queue, telemetry; latmat.
7. Mirrors regenerated; A/B per §12; results recorded.

## 14. As built (2026-09-25)

Where the implementation departs from, or adds to, the text above:

- **Open is one read.** The engine reads a speculative suffix of each file —
  `max(64 KiB, file_bytes / 1024)`, the whole file below that — which holds the
  tail and, whenever it fits, the footer (parquet's speculative footer read,
  sized for skene footers, which grow with columns × row groups). A range that
  lies wholly inside the suffix is never read again, so a small file costs one
  read in total. A miss costs one extra footer read, never an answer.
- **Directories merge with block 0, as §3.1 intended, but at open.** Directory
  blocks are needed to plan exact chunk ranges, so they are read when the file's
  work items are built. When every non-empty row group of block 0 survives
  pruning, each directory range runs on through its column's block 0
  (`plan_directory_fetch(..., through_first_block=true)`), and block 0's work
  item decodes from those bytes with no read of its own. When block 0 is pruned
  or partly pruned, directories are read alone.
- **Telemetry names** follow the engine's `io_*` convention rather than §9's:
  `io_skene_requests` (chunk reads), `io_skene_metadata_requests` (suffix,
  footer and directory reads — a directory read carrying block 0 is counted
  here once), `io_skene_bytes_fetched`. `io_bytes_claimed` is the planned chunk
  bytes of the claimed row groups, -1 when any file in the scan is v2.
- **G ≥ 1** rather than G ≤ row groups; a file with fewer row groups than G has
  one partial block.
- **Latmat** plans per row group per pass (no block work items) and reads
  through the same suffix.

### Measured (local disk, interleaved, min of rounds)

Against the benchmark mirrors the Makefile built before the bump (v2, 262,144-row
row groups, 16 per file). v2 request counts are what a ranged v2 reader would
issue for the same scans, modelled from the v2 files' own footers with the same
coalescer; v3's are the engine's counted reads. At `waste_ratio = 0` v3 fetches
the same bytes in every query (the safety property held).

| Suite | v3 faster | Exec, sum of mins | Reads, all queries | Bytes |
|---|---|---|---|---|
| TPC-H SF1 | 22 / 22 | 589 → 422 ms | 2,214 → 2,368 (+7%) | within ±5%, except the small `part`/`partsupp` scans (Q02 +25%, Q11 +21%, Q16 +42%) |
| TPC-H SF10 | 21 / 22 | 3,681 → 3,368 ms | 20,448 → 22,382 (+9%); metadata reads fewer on 19 / 22 (Q21 112 → 20) | within ±5% |
| ClickBench | 39 / 43 | 13,854 → 12,527 ms | 39,168 → 29,162 (−26%) | +7..9% on the wide-string scans |

Row counts matched on every query of all three suites.

- **Extra bytes are chunk bytes, not fetch overhead.** At 64k rows a row group
  stores less compactly than at 262k (dictionaries and value ordering see a
  quarter of the rows), which shows on SF1's small tables and on ClickBench's
  URL/Title scans (Q21–Q23, the only non-trivial queries v3 loses there). The
  open and directory reads add ~0.2 MB on SF1 Q16.
- **`SELECT *` over a wide table pays one directory read per column per file**
  (ClickBench Q24: 424 metadata reads, 105 columns × 4 files) — the price of D-5,
  each directory adjacent to its column. With block 0 merged in, those reads
  also carry block 0's data.
- **More chunk reads where a query reads many columns of one table** (SF10 Q01
  474 → 1596, Q09 762 → 1586). A v2 row group holds its columns adjacent, so a
  ranged reader coalesces them into one read; v3 issues one read per column per
  block. Multi-block work items (R13, not built) are the lever: a narrow
  column's run spans many blocks in one range.
- **SF10 Q09 slower (340 → 402 ms) is a plan flip, not IO.** v3's sketches put
  `o_orderkey` at 14.8M distinct (truth 15M); v2's at 6.2M. With the accurate
  number the enumerator joins partsupp before orders, which runs slower. The
  scan pipelines cost the same. The cost model, not the format, is at fault.
- The decode-claim size (64k rows vs 262k) contributes to the exec gains; the
  64k row group also stores ~6% more bytes on ClickBench's wide strings.
