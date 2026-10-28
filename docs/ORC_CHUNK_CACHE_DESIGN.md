# ORC streams in the chunk cache (C7)

**Status:** BUILT 2026-10-28 (uncommitted) with D1–D5 as recommended; measured in §8.
**Date:** 2026-10-28
**Builds on:** docs/C7_PAGE_CACHE_DESIGN.md (the cache, its rulings §12–§28),
docs/ORC_READER_DESIGN.md §6.3–§6.7 (StripeCursor, row-index seek, prefilter, pruning).

## 1. Why

ClickBench over ORC is 2.31x Parquet (9.85 s vs 4.27 s summed, 2026-10-28). Profiling Q20 and
Q22 on ORC (samply, warm runs):

| CPU share | Q20 | Q22 |
|---|---|---|
| zstd decompression | 44% | 44% |
| memset/memcpy in `decompress` | 10% | 10% (fixed since: one allocation, no zero-fill; Q20 −7%, Q22 −10%) |
| copying strings into arenas (`build_slot`) | 16% | 16% |
| the `LIKE` kernel | 22% | 22% |

Parquet's ClickBench numbers are **warm chunk-cache** numbers: the benchmark takes the minimum of
5 runs in one process, and C7 is on by default (60% − 4 GiB). Measured the same day:

| | Parquet, cache on | Parquet, `CHUNK_CACHE_MEMORY_PERCENT=0` | ORC |
|---|---|---|---|
| Q20 | 58 ms | 191 ms | 445 ms |
| Q22 | 130 ms | 334 ms | 1,106 ms |

ORC decompresses every stream on every run. Bringing ORC under the same cache removes the zstd
share on warm runs, the same treatment Parquet already gets.

## 2. The mapping

| C7 concept (parquet) | ORC |
|---|---|
| Entry = one column chunk, decompressed, in one buffer | Entry = **one stream of one stripe** (PRESENT, DATA, LENGTH, SECONDARY, DICTIONARY_DATA), decompressed, in one buffer |
| Key = file path + the chunk's first byte offset | Key = file path + **the stream's first byte offset in the file** (unique in a file; files are immutable, ruling D-C7-4) |
| Page index: page offset in the chunk → bytes in the image | **Chunk index**: each compression chunk's offset in the stream → its start in the decompressed buffer (the `page_index` triples, one per compression chunk) |
| Only complete chunks are cached (ruled, §14) | Only **complete streams** are inserted: the full-stream open (`LeafCursor::seek(nullptr, nullptr)`), which every sequential read and every pass-1 read uses |
| Uncompressed local chunks are not cached (the OS page cache holds them) | Same: an uncompressed ORC file is already a view into its mmap; nothing to cache |
| `chunk_cache_admit` (session variable; compaction off) | Honoured the same way: the ORC Source takes the flag from the plan and does not fill when off |
| Cost = measured decompression ns (+ modelled re-fetch for remote) | Same, measured per stream around the decompress |

The chunk index is what makes a cached stream serve **row-index seeks** (§6.5) for free: a
position's compressed-chunk offset is looked up in the index (`draken_cc_page`), and the
decompressed bytes are already there. So a hit serves both a whole-stream read and any window.

## 3. Read path

`StreamSource::get` gains a cache-aware sibling used by `LeafCursor::open` (and the dictionary
streams at construction):

1. **Lookup** `(path, stream offset)`. A hit pins the entry; the cursor keeps the pin for as long
   as it reads from that window (a `Bytes` view plus the pin, unpinned when the window is replaced
   or the cursor is destroyed).
   - A whole-stream open uses the whole entry.
   - A seek window uses the index to find the chunk's decompressed start, then the window's
     bytes are a view into the entry (no decompression, no copy).
2. **Miss, whole-stream open:** decompress into a `draken_cc_fill_alloc` buffer (sized as the
   decompress already is: chunk headers walked first), building the chunk index as it goes, and
   `draken_cc_insert` it. Admitted: the cursor reads from the inserted entry (pinned). Refused:
   the buffer is freed by the cache, so the cursor decompresses into its own buffer instead — no
   second decompression: the fill buffer is handed back as the cursor's own when the insert is
   refused. *(Needs one API addition, D4.)*
3. **Miss, seek window:** decompress the window as today; nothing is inserted (partial, ruled out).
   The call is a lookup with `predicated = 1`, so a stream only ever read through seeks counts as
   a miss but is not re-offered.

Remote ORC files are still fetched whole (`fetch_whole`) per query: a hit saves decompression
only, as ruling D-C7-1 says for remote parquet. Skipping that GET needs a per-stream remote read
path the ORC Source does not have; out of scope.

## 4. What is NOT cached

- Stripe footers, row-index streams and the file tail: small, parsed per query as today.
- Decoded values (slots, codes): the cache holds decompressed stream bytes only (ruling D-C7-1:
  no decoded tier).
- The stripe dictionary as built slots: the DICTIONARY_DATA and LENGTH streams are cached as
  bytes; slots are rebuilt per stripe as today.

## 5. Fix 2 (zero-copy strings) after this

It does **not** become safe. Borrowing string bytes from a cache entry means every morsel holds a
**pin** on its column's whole stream until anything downstream lets go of it. A pinned entry
cannot be given back when query memory grows (give-way, §20/§23), so the retention problem is
the same as without the cache, and worse, it now blocks the cache's own release. **Recommend:
keep copying strings** (the 16%), and revisit only with a design that copies on retention (e.g. a
sink that keeps rows copies them), which is a separate engine-wide question.

## 6. Expected effect, and how to prove it before building (estimates)

- Warm ORC runs lose the zstd share. On Q20–Q22 that is ~44% of CPU: Q20 ≈ 0.43 → ~0.25 s,
  Q22 ≈ 0.97 → ~0.55 s. Every other ORC query also decompresses every run, so the gain is
  suite-wide; how much depends on each query's decompression share, which has only been
  measured for Q20/Q22.
- If the suite-wide share is near Q20/Q22's, summed ORC drops from 9.85 s to roughly 6.5–7 s,
  under the 2x target (8.5 s). **That is an estimate; prove it first (KNOB rule):**
  - **Step 0, no engine change:** one samply profile of the whole ORC suite (all 43 queries,
    warm, as the runner shapes them), attributing each query's samples to zstd + the copy in
    `decompress`. That share of each query's time is what a warm cache removes; summed, it says
    whether the design clears 8.5 s. About 5 minutes of machine time. If it does not clear 2x
    with margin, stop here.
  - Not used: an **uncompressed** ORC mirror as a ceiling. pyarrow ignores `stripe_size` when
    uncompressed and writes 1,024-row stripes (found 2026-10-28), so that mirror would have about
    100,000 tiny stripes and measure a different file shape, not the cache.
- Memory: entries count as CACHE bytes in the process memory account and give way to query
  memory, exactly as parquet's do. Note the macOS trap from long runs (chunk cache filling tens of
  GB, memory compression skewing later queries — see the C7 notes); the same budget applies.

## 7. Decisions for the architect

- **D1 — granularity.** (a) one entry per **stream** (key = path + stream offset; chunk index for
  seeks); or (b) one entry per **compression chunk** (256 KiB). **Recommend (a):** it matches the
  "complete chunks only" ruling, keeps the entry count low (one per stream, not one per 256 KiB),
  and the index serves seeks.
- **D2 — partial windows.** Seek windows (prefilter pass 2, top-N pass 2, pruned row groups) never
  fill. **Recommend as stated** (the partial-chunks ruling). Consequence: a column only ever read
  through seeks stays uncached — pass 2 of Q23 re-decompresses its few row groups each run (small).
- **D3 — fix 2.** **Recommend not building it** (§5).
- **D4 — refused fills.** Today `draken_cc_insert` frees a refused buffer. ORC wants the bytes
  either way. (a) add `draken_cc_insert_or_keep` (returns the buffer to the caller on refusal);
  or (b) on refusal, decompress again into the cursor's own buffer. **Recommend (a)**: one small
  C function beside `draken_cc_insert`, no double decompression on a full cache. Parquet keeps
  its current call.
- **D5 — Step 0 first.** **Recommend:** profile the whole ORC suite for its decompression share
  (§6) before any engine change, and build only if the projected sum clears 2x with margin.

## 8. What was built and measured (2026-10-28)

- `draken_cc_insert_pinned` (draken/core/chunk_cache.{h,cpp}): admitted → the new entry, pinned;
  refused (including another reader filling the key first) → NULL, buffer stays the caller's.
  `draken_cc_insert` and it share one `insert_entry`, so admission is one rule.
- rugo/src/orc/orc_reader.cpp: `StreamSource::stream` (whole streams: lookup, else decompress
  with the chunk index and offer it) and `StreamSource::cached_part` (a seek window from a cached
  stream via `draken_cc_page`); `Bytes` carries a `CachePin`; `OrcFile::use_chunk_cache(path,
  admit)`. Stripe footers, the Metadata section and ROW_INDEX streams stay uncached.
- Engine/plan: `OrcScanSpec.chunk_cache_admit`, `OrcScanPlan.set_chunk_cache_admit`, set by the
  compiler from `chunk_cache_admit` beside the parquet scans.
- **Not as parquet:** the scan-resistant headroom check (`_chunk_cache_admission`) does not apply
  to ORC scans — their fill size needs every file's stripe footers, which planning does not read.
  An ORC scan larger than the cache's headroom fills (and evicts) as it goes. Not covered by the
  design above; raised here.
- Tests: tests/unit/connectors/test_orc_chunk_cache.py (repeat scan hits with the same answer
  for zstd/zlib/snappy/lz4; a seek reads from a cached stream and never fills; admit=false
  flushes and never fills; uncompressed never cached; cache off never looked up).

ClickBench, Mac, back to back, min of 5 (CSVs FULL_SPLIT_ORC/a8787ff9-20261028T204402.csv,
FULL_SPLIT_RUGO_262K/a8787ff9-20261028T204504.csv):

| | ORC | Parquet | Ratio, summed | Geomean |
|---|---|---|---|---|
| Before the cache | 9.85 s | 4.27 s | 2.31x | 3.29x |
| **With the cache** | **7.60 s** | **4.33 s** | **1.76x** | **2.85x** |

Step 0 projected 6.97 s; measured 7.60 s. Biggest moves: Q20 445 → 226 ms, Q22 1,106 → 594,
Q23 598 → 351, Q27 451 → 224, Q33/Q34 ~680 → ~450. Pruned queries (Q36–Q42) are unchanged, as D2
says (their reads are seeks). The runner flagged 22 queries "unstable": each query's first run
fills the cache and the rest hit it, so the spread is cold vs warm, not machine noise. The sum of
first (cold) runs was 13.8 s against 12.1 s before the cache — the fill cost ruling D-C7-13
accepts, on one run only.
