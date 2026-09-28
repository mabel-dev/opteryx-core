# Parquet selective decode, encoded predicates, dictionary cost, footer cost

**Status:** SHIPPED 2026-09-28 (see §5c): A, B, C, D, E kept; F deleted. Rulings received 2026-09-28.

**Governing rule (architect, 2026-09-28): every option is decided by "whichever is faster", and that is measured, not argued.** Each D-n below that is a speed choice is settled by building both arms, running them ABBA, keeping the faster and deleting the loser. D-3 (bitmask vs run lists), D-4 (packed-code compare), D-6 (deferred D fetch) and D-8 (compact footer) are no longer questions; each is an experiment in its stage. D-7 is not a speed choice: the page-index matcher is left untouched because it is out of scope.
**Scope:** four items from the external-reader comparison
(arrow-rs/DataFusion, Polars, Umbra; memory `external_parquet_readers_arrowrs_polars_umbra`):

1. **Selective decode.** Filter while decoding; do not decode everything and then compact.
2. **Predicates on encoded data.** Evaluate on the dictionary once, then map codes to a row mask; for plain strings, evaluate on page bytes.
3. *(out of scope: adaptive pushdown)*
4. **Dictionary cost.** Defer or cheapen dictionary page decompression and parse.
5. **Fixed per-file footer cost.**
6. *(out of scope: UTF-8 validation)*

Decisions for the architect are marked **D-n**.

---

## 0. Headline

- **Items 1 and 2 are one feature.** A selective decoder is only worth having if something produces a row mask *before* the other columns decode. Today only latmat pass 2 does, and its pass 2 is already small (Q24: 17 ms of ~500).
  - The shape that pays is an **in-worker prefilter**. Inside the rugo decode worker, for one row group: decode the predicate columns, evaluate the pushed predicate, then decode the remaining columns for survivors only.
  - This is the Polars `row_group_data_to_df_prefiltered` shape. It is also the "no barrier" shape that `LATMAT_FILTER_AGGREGATE_ASSESSMENT.md` §6.2 said would be needed.
- **Item 2 must reuse draken kernels, not add matchers to rugo.** Predicates run through the existing injected pass-1 evaluator (`pass1_run_predicate`, `io_pipeline.hpp:380`). They run on *zero-copy DrakenVector views over the decompressed page or dictionary buffer*: the Umbra "transient string" / arrow StringView idea.
  - The existing hand-written rugo dictionary-skip matcher (`decode_column.cpp:1047-1069`) and page-index matcher (`page_index.cpp:181`) already duplicate kernel logic. That violates §2 "never duplicate logic". This design removes the first one (§3.4).
- **Item 4 as arrow-rs does it (lazy dictionary) is worth ~nothing here.** We never submit a chunk whose data pages are all skipped: a fully pruned row group is `empty_filtered` before decode.
  - The measurable dictionary cost is the **copies**: dict page → `string_dict_arena` (per-entry `insert`) → `memcpy` into a new arena → slots.
  - `dict_parse_ns` is 0.99 thread-s of Q23's predicate-column decode, about 20% (LATMAT §4.4).
  - The design replaces that with slots that point into the decompressed dictionary page.
- **Item 5 is mostly done already.** Parsed footers are cached across queries (`_PARSED_FOOTER_CACHE`, 256 MiB, shared_ptr, 2026-09-26).
  - Measured today, standalone bench over all 99 `hits_rugo_262k` footers (14.3 MB of thrift, 166,320 column chunks), min of 7:

    | parse | total | per file | per chunk |
    |---|---|---|---|
    | full | 23.0 ms | 0.23 ms | 138 ns |
    | `schema_only` (row groups skipped via `SkipStruct`) | 16.7 ms | — | — |

  - **A faster or projected parser is not worth building.** Skipping is already 73% of the cost of parsing, so projection saves ≤27% of ≤23 ms, once per process.
  - What remains is:
    - (a) a **whole-file GET** for remote schema inference;
    - (b) parsed footers are 5–6× their encoded size, which limits how many files the cache holds;
    - (c) the cache key has no size or mtime.

**Ceilings, stated before anything is built** (per the profile-first rule):

| item | measured basis | ceiling |
|---|---|---|
| 1+2 on ClickBench local | LATMAT §3: the full cost of every deferrable column set is ≈190 ms of a ~13 s suite | ≈1.5% of suite, realistically ~100 ms; concentrated in Q11, Q12, Q31, Q32 |
| 1+2 string predicate columns | Q23 phase telemetry: decompress 3.54, dict parse 0.99, RLE 0.11, value expand 0.14 thread-s | Compare-on-views saves copies, not decompress. The copy cost of *plain* string pages (`append_string` + slot build) is unmeasured; Stage 0 measures it |
| 4 | Q23 `dict_parse_ns` 0.99 of ~4.8 thread-s | up to ~20% of string predicate-column decode thread time |
| 5 | 23 ms per 99 cold footers; a warm hit is ~0 | parse ≈0. The remote schema full-object GET is unbounded (file size / bandwidth) |

**ClickBench local will not show a large number for 1+2.** Their case is wide projections under selective filters: `SELECT *`-style reads, JOB dimension scans, and remote reads where a skipped column also skips a fetch. **D-1 RULED 2026-09-28:** this is a performance feature. Any stage that measures slower is deleted unless there is a compelling reason. Each stage ships only on an ABBA win against the current path.

---

## 1. What exists (mapped 2026-09-28)

### Decoder (`rugo/src/parquet/decode_column.cpp`)

- Entry point: `DecodeColumnFromChunk(out, data, size, stats, ext_*, row_mask, prefer_dict, skip_pred, jump)` (`decode.hpp:322`, `dc:456`). There is one worker per row-group WorkItem, which decodes every column of that row group (`io_pipeline.hpp:3078/3101/3112`).
- The row mask is `uint8` per row. It is used in only two places:
  - **Per page, to skip whole pages** (`dc:1465-1494`).
  - **Once per chunk, after decode, to compact** (`dc:2672-2980`, `mask_filter_ns`). Before compacting, RLE runs are expanded back to dense and strings are copied again into a new arena.
- No value decoder takes the mask.
- Under a mask, the page-parallel Tier-3 path is disabled (`dc:1176`, which requires `row_mask == nullptr`).
- Bit-unpacking works in **groups of 8 values** (`unpack_group_8_*`, NEON/AVX2, `decode_encodings.cpp:38-253`). One group is exactly one byte of a row bitmask. There is no `skip(n)` anywhere.
- The dictionary page is decompressed into a local buffer and parsed eagerly, before any mask or jump plan is consulted (`dc:665-838`).
  - String dictionaries are copied entry by entry into `string_dict_arena` (`dc:792-808`).
  - `build_direct_string_dict` (`io_pipeline.hpp:816`) then `memcpy`s that arena into a new allocation and builds one `DrakenStringSlot` per entry.
- Plain byte_array pages: `append_string` copies each value (`decode.hpp:192`), then `build_direct_string_plain` (`io_pipeline.hpp:657`) copies again into slots.

### Predicates

- **Single-pass native scan.** The pushed predicate becomes a relocated `ExprFilterOperator` downstream of the source, evaluated on the finished morsel. Filtering is `cxx_mask` → `draken_take` over every column (`draken_native.cpp:6643`).
- **Latmat.** Pass 1 already evaluates the predicate **inside the rugo worker** through an injected c-native bytecode evaluator:
  - `Pass1PredCtx` / `set_pass1_predicate` (`io_pipeline.hpp:3606`);
  - `pass1_run_predicate` (`:380`), over non-owning `pass1_build_dv_view`s;
  - the output is a bit-packed `survivor_mask`.

  rugo stays Python-free and opteryx-free: it holds a function pointer. **This is the mechanism the design reuses.**
- **Dict skip.** `DictSkipPredicate` is all-or-nothing per row group, with its own matchers. It is disarmed under any row mask (`io_pipeline.hpp:3042`).
- **Draken kernels.** String and numeric compare, IN, LIKE, contains, starts/ends-with all evaluate **once per distinct dictionary entry** and scatter (`string_compare.h:463`, `function_kernels.cpp:1149-1594`). So a dict-shaped view gets the Polars "evaluate the dictionary once" behaviour from the kernels already.

---

## 2. Item 1+2 — in-worker prefilter with selective decode

### 2.1 Shape

For a row group in a native parquet scan whose pushed predicate is all c-native (the same admission as today's relocation, `compiler.py:3748`):

```
worker(rg):
  P = predicate columns, D = projection \ P
  for c in P: decode c                        -- item 2: views, not owned copies (§2.3)
  mask = pass1_eval(P views)                  -- existing injected evaluator
  if popcount(mask) == 0: emit nothing (empty_filtered); D is never decoded or fetched
  for c in P ∩ projection: materialise survivors only from the views
  for c in D: DecodeColumnFromChunk(..., selection = mask)   -- item 1: selective
  emit morsel of survivors only
```

The relocated `ExprFilterOperator` is **removed** for a scan that applies its predicate in the worker. The planner decides this once, at plan time; nothing is decided at runtime. That is not a fallback: the predicate runs in exactly one place.

**D-2 RULED 2026-09-28: whichever is faster.** Build it, then ABBA the in-worker prefilter against the downstream `ExprFilter`. The faster one is kept and the other is deleted, with no lingering flag. The reasoned expectation: in-worker is faster whenever any row group has partial survivors, because it skips D's decode for non-survivors and removes one `draken_take` pass. At ~100% survivors they should be equal. A flag exists only for the length of the measurement.

### 2.2 Selective decoder (item 1)

The mask changes from `uint8`-per-row to a **bitmask** (1 bit per row, 64-bit words). Latmat pass 2 converts its `uint8` mask once at submit. **D-3:** the bitmask alone, or run lists as well? arrow-rs picks runs only when the average run is ≥32 rows. Our unit is a 64k row group, and a bitmask is 8 KiB, so I recommend **bitmask only**.

Per page, having computed `sel = popcount(mask[page rows])`:

| sel | action |
|---|---|
| 0 | skip the page (as today; with a jump plan, not even the header is read) |
| = page rows | the **existing unmasked decode**, unchanged. This is the no-regression path, and why no adaptive gate is needed (§2.5) |
| otherwise | selective decode below |

Selective decode by encoding, all writing directly into the owned output (no compaction pass afterwards):

- **RLE_DICTIONARY codes.**
  - RLE run: one dictionary lookup, then `fill(popcount(run's mask range))`.
  - Bit-packed run: for each group of 8, read the mask byte. If it is 0, advance the bit offset without unpacking (Polars `skip_chunks`, at our group size). Otherwise unpack the group and gather where `mask` is set (`ctz` walk).
  - With `prefer_dict` or string dicts, the output stays dict-shaped as compacted codes plus the dictionary, as today.
- **PLAIN fixed width.** Gather the set rows straight from the decompressed page (`ctz` walk, or `memcpy` runs when a mask word is all-ones).
- **PLAIN byte_array.** Walk the length prefixes (unavoidable) and copy only selected values.
- **DELTA_BINARY_PACKED / DELTA_BYTE_ARRAY.** Decode fully (the deltas are sequential), then gather. There is no gain, but no loss either, and it is recorded in telemetry as `sel_decode_fallthrough_pages`.
- **Nullable columns.** Decode def levels fully; they are needed to map rows to value indices. The value index of row `r` is `popcount(validity[0..r))`. Walk the selected rows, emit their validity bit, and gather the value when present.
- **LIST columns.** Out of scope. They keep the current post-decode compaction, which has a suspected defect (§6).

This removes `mask_filter_ns` entirely and replaces `val_expand_ns` with work proportional to survivors. Decompression and RLE unpacking remain page-granular; nothing here changes that.

Tier-3 page parallelism stays unmasked-only. Masked PLAIN numeric chunks keep their current sequential decode.

### 2.3 Predicate columns as views (item 2)

Predicate columns are decoded into **non-owning DrakenVector views whose string slots point into the decompressed buffer**. This is Umbra's transient string: valid only until the worker releases the page buffers.

- **Dict column.** A DK_VARCHAR_DICT or numeric dict view: slots or values over the dictionary page (§3), with codes from the RLE decoder. The pass-1 kernels evaluate once per entry, which is the Polars "evaluate dictionary once" behaviour with no new code.
- **Plain byte_array column.** One slot per row built directly over the decompressed page. There is no `append_string` copy: the extern slot's `arena_offset` is relative to the page buffer.
- **Numeric column.** A view over the decoded values (as `pass1_build_dv_view` does today).

The DrakenVector contract holds: dense or dict shape, `data[selection[i]]`, with nothing shape-dispatched in the kernels. After evaluation:

- P columns that are not projected are dropped without ever being materialised (e.g. Q21's URL).
- P columns that are projected materialise **survivors only**, copying from the views.

One thing is not in this design: evaluating codes→mask *during* unpack (the Polars `decode_single` compare on packed codes). The kernels already evaluate per entry. The remaining gain is the scatter over codes, and Q23's RLE plus value expand totals 0.25 thread-s, so that ceiling is too small. **D-4:** confirm it is excluded.

**Buffer lifetime.** A page's decompressed buffer currently lives in decoder scratch for a single call. The views need the P columns' decompressed pages (and dictionary pages) held for the whole worker step. That costs memory proportional to one row group's P columns, compressed-size × ratio, released at the end of the step.

**D-5 RULED 2026-09-28: whichever is faster.** Holding the pages is faster by construction: copying does strictly more work (every P byte copied, then the copy is evaluated). Its only cost is memory: one row group's decompressed P columns per in-flight worker, bounded by `in_flight_limit`. **Hold them.** Copying would only return if a measured memory-pressure slowdown appeared.

### 2.4 Mask and multiple conjuncts

The pass-1 evaluator takes the whole conjunction over all P views. There is no per-conjunct ordering; that would be item 3, which is out of scope.

### 2.5 Why no selectivity gate

DataFusion keeps pushdown off because of regressions (ClickBench Q6 3.28×). Their costs are per-predicate RowSelection machinery plus decoding filter columns twice. Here:

- the filter is not added; it moves, and the downstream `ExprFilter` disappears;
- the take moves into the decoder;
- at 100% survivors every page takes the unmasked path;
- P columns are decoded once, as views.

**Canary:** Q28 (99.9% selectivity) and Q6-shaped high-selectivity queries must not regress (ABBA). If they do, item 3 becomes a prerequisite, and I will bring that back to you rather than add a gate.

### 2.6 Remote bonus

Fetch is per column chunk, ahead of decode. D columns of a row group with zero survivors are fetched and then discarded. Deferring D's fetch until after the mask is the Umbra/AnyBlob "decide before retrieve" idea. It is a pipeline restructuring, so it is **not in this design**. It is noted because it is where remote wins would come from. **D-6:** do you want it scoped as a follow-on?

---

## 3. Item 4 — dictionary cost

### 3.1 Lazy dictionary: not proposed

arrow-rs's #11168 wins when a whole chunk's data pages are skipped, because the dictionary is then never needed. In our pipeline a fully pruned row group is `empty_filtered` before decode (page index: `io_pipeline.hpp:2880`; latmat pass 2 submits only row groups with survivors). A submitted chunk with a non-empty mask decodes at least one page, so it needs the dictionary.

- **Built with part A:** a counter `dict_pages_parsed_unused` for chunks where the dictionary was parsed and zero data pages decoded. If it is non-zero on real workloads, lazy decoding comes back.

### 3.2 Zero-copy dictionary

- **Now:** decompress into a local buffer → per-entry `insert` into `string_dict_arena` → `memcpy` into a new arena → slots.
- **Proposed:** decompress the dictionary page into an **owned buffer that becomes the draken arena itself**. Slots are built in one pass over the length-prefixed entries: short strings (≤ `STR_INLINE_MAX`) inline, long ones with `arena_offset` = the entry's offset in the page buffer, skipping its 4-byte prefix. Two copies disappear.
- **Arena waste:** the 4-byte prefix per entry stays in the arena. That is 4 B × dict_size, negligible against the strings.
- **Lifetime:** the arena is owned by the column's VectorOwner, exactly as the copied arena is today. Nothing about ownership changes, only where the bytes came from.
- **Uncompressed dictionary pages:** "decompress" is a copy out of the fetch buffer today. It stays one copy, into the owned arena.
- **Numeric dictionaries** are already `resize` + `memcpy` on little-endian (`dc:812`). No change.

Length-only elision (`build_direct_string_dict` `length_only`) keeps working. Slots carry lengths, and the arena can be released if every slot is inline or elided.

### 3.3 Interaction with item 2

The same zero-copy dictionary is what the §2.3 dict views point at. It is built once per chunk and serves both the predicate evaluation and the output.

### 3.4 Retire the rugo dict-skip matcher

Under §2 the dictionary-skip becomes a consequence: a pushed predicate evaluated on a dict view whose per-entry results are all false gives `popcount == 0`, so the row group is `empty_filtered`, now for **any** c-native predicate, not just the five `DictSkipPredicate` kinds. It still needs the dictionary-coverage soundness guard (`dc:977-1015`, every data page dict-encoded). That check moves before the evaluation rather than being lost.

The early exit is kept: if the dictionary verdict is all false, P's data pages are never decompressed. That requires evaluating the kernel on the dictionary-only view *before* decoding codes. Every per-value predicate satisfies "all entries false ⇒ all rows false"; NULL rows are false under WHERE.

- **Delete** `DictSkipPredicate`, `dict_preds_` / `add_int_needles` / `add_str_pred`, the matcher at `dc:1017-1073`, and `_flatten_dict_skip_predicates`.
- **D-7:** `page_index.cpp` `EvaluatePagePredicate` is the other hand matcher (min/max, not values). It stays, because it works on statistics and not data. It currently takes `ColDictPred` as input, so its input moves to the pruning triples it could consume directly. Do you agree?

---

## 4. Item 5 — footer

### 4.1 Remote schema inference fetches the whole file (fix)

`read_blob(just_schema)` (`filesystem_connector.py:350-395`) uses `open_input_stream(...).memoryview`. That is an mmap locally but a **full-object GET** on GCS/S3/HTTP (`gcs_filesystem.py:60-75`). It then parses separately into `_FOOTER_METADATA_CACHE`.

**Proposed:** use `FetchParquetFooter` (one suffix read, then an exact read if needed) and parse through the shared `_PARSED_FOOTER_CACHE`. This is one parse site; the separate Python-side cache goes. The cost falls from one whole file to one or two ranged GETs per cold dataset open.

### 4.2 Compact parsed footer (cache capacity)

Parsed footers are ~520 B per chunk against 84–92 B encoded. On hits that is 86 MB for 99 files, so a 256 MiB budget holds ~290 files of this shape. Past that the cache thrashes, and a miss costs a GET per file remotely.

- `min`/`max` `std::string` → `(offset, len)` into the retained encoded footer bytes, which the cache entry holds.
- `physical_type`/`logical_type` strings and `list_def_thresholds` per ColumnStats → one leaf index into `schema_columns`. This removes the `ApplyLeafInfosByIndex` copy into every row group (`metadata.cpp:1227`).
- Per-chunk `key_value_metadata` `unordered_map` → absent unless present in the file (it is empty on our writer).

This touches every `ColumnStats` consumer: Python pruning, `AggregateColumnStats`, `submit_block`'s per-row-group copy. It is the largest mechanical change in this document for a cache-capacity gain only.

**D-8:** build it now, or only when a dataset is seen exceeding the budget? I recommend **deferring it** and adding a `footer_cache_evictions` counter to find out.

### 4.3 Cache key

`_PARSED_FOOTER_CACHE` is keyed by path only (`footer_cache.pyx:20`).

**D-9 RULED 2026-09-28: files are immutable. That is law.** A path-only key is correct by contract, and nothing changes.

### 4.4 Not proposed

A faster or projected Thrift parser; see §0.

---

## 5. Delivery: build everything, then ablate

**Architect ruling, 2026-09-28:** build every part, then turn parts **off** one at a time. Never add them one at a time from a baseline. At this maturity parts interact, and micro-benchmark wins do not reliably become query wins.

**Parts, each behind a kill switch for the ablation only:**

| part | content |
|---|---|
| A | §3.2 zero-copy dictionary |
| B | §2.2 selective decoder (bitmask; DELTA pages decode fully, then gather) |
| C | §2.1 + §2.3 in-worker prefilter over zero-copy views; relocated `ExprFilter` removed; §3.4 dict-skip matcher retired |
| D | §4.1 remote schema inference via footer fetch |

B only has callers through C (and latmat pass 2). D is independent of A–C.

**Build order** follows dependencies only: A, B, C, D. `make q` must pass with everything on.

**Ablation.** Workloads are ClickBench (full suite), TPC-H SF1 and SF10, and JOB.
- Run FULL (everything on) against FULL-minus-X for each X, ABBA interleaved.
- Decode spans are also taken at `MAX_EXECUTION_WORKERS=1`; wall is taken at the default DOP.
- The in-worker prefilter against the downstream `ExprFilter` is simply C off versus C on.

**Verdicts:**
- If removing X makes things faster, X is deleted.
- If removing X makes things slower, X stays.
- Once verdicts are in, every kill switch is removed; nothing ships disabled.

D is measured by bytes fetched and time on a cold remote open. It has no wall-clock interaction with A–C.

The Stage 0 measurement below stays as the pre-build baseline record.

## 5a. Stage 0 results (2026-09-28)

Setup: `hits_rugo_262k`, `MAX_EXECUTION_WORKERS=1` (the rugo decode pool is still multi-threaded), min of 3 after 1 warm-up. Phases are rugo `get_cpp_telemetry` thread-seconds; wall is seconds. Q numbers are 1-based.

| Q | shape | wall | decompress | dict_parse | rle | val_expand | ba dict / dense rows |
|---|---|---|---|---|---|---|---|
| 11 | `MobilePhoneModel <> ''` GROUP BY | 0.254 | 0.066 | 0.007 | 0.130 | 0.212 | 99.0M / 0 |
| 12 | same + COUNT(DISTINCT UserID) | 0.290 | 0.075 | 0.007 | 0.168 | 0.321 | 99.0M / 0 |
| 13 | `SearchPhrase <> ''` GROUP BY | 0.912 | 0.336 | 0.158 | 0.076 | 0.086 | 97.9M / 1.0M |
| 21 | COUNT(*) `URL LIKE` | 0.898 | 2.247 | 0.515 | 0.014 | 0.020 | 53.1M / 10.5M |
| 22 | + `SearchPhrase <> ''`, MIN(URL) | 0.999 | 2.519 | 0.624 | 0.063 | 0.076 | 115.7M / 11.4M |
| 23 | Title/URL LIKE + SearchPhrase | 1.878 | 3.526 | 0.999 | 0.192 | 0.277 | 231.4M / 11.4M |
| 28 | canary, 99.9% selectivity | 0.943 | 2.069 | 0.464 | 0.034 | 0.117 | 88.5M / 10.5M |
| 31 | `SearchPhrase <> ''`, 4 deferred cols | 0.856 | 0.443 | 0.156 | 0.386 | 0.680 | 97.9M / 1.0M |
| 32 | same | 0.759 | 0.421 | 0.158 | 0.308 | 0.530 | 97.9M / 1.0M |

What the numbers say:

- **Item 1+2 (selective decode).** The target is `val_expand` (and part of `rle`) on deferrable columns. On Q31/Q32, `val_expand` + `rle` is 0.84–1.07 thread-s. With 13% survivors, selective decode can remove most of `val_expand` (~0.45–0.6 thread-s per query). On Q11/Q12 it is 0.34–0.49 thread-s at 5.6% survivors. This is the largest lever of the four items on ClickBench.
- **Item 4 (dictionary copies).** `dict_parse` is 0.46–1.0 thread-s on the string queries (Q21–23, Q28), which is 20–28% of their decompress time. Zero-copy removes the per-entry copy share of that; its share is settled by the part-A ablation.
- **Largest cost, and not in scope.** **Decompression** dominates every string query: 2.1–3.5 thread-s, 3–4× everything else combined. None of items 1, 2, 4 or 5 touch it. Selective decode cannot skip decompression, because survivors are scattered and every page holds one (LATMAT §4.4).
- **Not measurable with current telemetry.** The plain byte_array value decode (`append_string` + slot build) has no timer; `val_expand` covers dict pages only. The dense-row counts show plain-string volume is small here (1–11M rows of 99M), so this is not a priority.

Next: build A–D (§5), then ablate.

## 5b. Build record (2026-09-28)

**Built, all behind ablation switches.** `make q` and the rugo suite (1673 tests) are green with every part on.

| part | switch (ablation arm only) | what landed |
|---|---|---|
| A | `RUGO_ZC_DICT=0` | `DecodedColumn::string_dict_arena` is a `ScratchBuffer` holding the decompressed dict page itself; entries are located by a length-prefix scan (no per-entry copy). The arena is not packed: offsets + lens are authoritative. Fixed the two consumers that assumed packing (`build_direct_string_dict` length from offset delta; `serialize_string_dict` now writes a packed arena). |
| B | `RUGO_SEL_DECODE=0` | Under a row mask (scalar columns) each page emits only its selected rows. `DecodeRLEBitPackedIndicesSelected` skips unpacking 8-value groups with nothing selected; PLAIN fixed-width is compacted per page; byte_array skips unselected values; def levels are compacted per page. The run path is off under a mask. There is no post-loop compaction (`mask_filter_ns` 0.117 → 0.0001 s on the Q12 column set). Test: `tests/rugo/test_selective_decode.py` (9 column shapes × 7 mask shapes × dict/plain). |
| C | `FEATURE_DISABLE_SCAN_WORKER_PREFILTER=1` | Worker: predicate columns first → pass-1 program → empty row group dropped before the other columns decode → other columns decoded under the survivor mask. Source: gathers the predicate columns by the mask (`cxx_mask_c`), or runs the same ExprFilter program itself when the worker declined. Planner: `_arm_scan_prefilter` replaces the relocated ExprFilter when `pass1_worker_predicate_admissible` holds. Live: Q21 63.5M rows in → 15,851 out on the workers; Q12 99.0M → 5.5M. |
| D | none (bytes are the measure) | Schema inference reads only the footer: one ranged read of the tail (64 KiB), and a second exact read when the footer is longer. Before, a remote file was fetched whole. The anonymous GCS/S3 filesystems gained `read_ranges` (delegating to HTTP). |

**Equivalence.** All 43 ClickBench queries, C on vs off: identical results on 35. The other 8 (Q18, Q25, Q32, Q33, Q39–42) are unordered `LIMIT`s or have ties, and differ between two runs of the *same* arm, so they are non-deterministic rather than wrong. B was checked against decode-then-filter over every column shape.

**Agreed scope NOT built, surfaced for direction.**

1. **§2.3 zero-copy views for PLAIN string predicate columns.** Page buffers are decompressed one at a time into a reused scratch buffer. Views need every page of the column kept alive in one arena, since a string slot addresses a single arena base. That means decompressing each page into one contiguous per-column buffer, which reworks the byte_array decode for every column, not just predicate columns.
   - Stage 0 puts plain strings at 1–11M of 99M rows on hits.
   - Dictionary string predicate columns already evaluate over the dictionary: after A they cost a scan, not a copy.
2. **§3.4 retiring the dict-skip matcher.** Its early exit (no data page decompressed when no dictionary entry matches) can only be replaced by evaluating each single-column conjunct on the dictionary. The pass-1 program is the whole conjunction, which may span columns, so this needs per-conjunct programs handed to the decoder. Deleting the matcher without that loses the early exit. It is left in place and still armed; it works alongside C.
3. **The `dict_pages_parsed_unused` counter** (§3.1) was not added.

## 5c. Ablation and verdicts (2026-09-28)

**Status: SHIPPED — A, B, C, D, E kept; F deleted; every ablation switch removed.**

**Method.** Leave-one-out.
- Arms: FULL, FULL−X for each of A, B, C, E, F, and NONE (every part off).
- Each arm runs as its own process. Arms are interleaved, with the order reversed every round, over 4 rounds.
- Each query is timed as the min of 3 runs after a warm-up; the suite total is the sum of per-query medians across rounds.
- Datasets, all parquet: ClickBench `hits_rugo_262k`, TPC-H `testdata/tpch_1` and `tpch_10`, JOB `testdata/job`. Apple Silicon.
- Correctness: every query's result hash was compared across all 7 arms. There were no errors and no mismatches.

Removal cost (FULL−X total ÷ FULL total; above 1 means the part pays):

| suite | −A | −B | −C | −E | −F | NONE |
|---|---|---|---|---|---|---|
| ClickBench (43) | **1.050** (4/4 rounds) | 1.020 | 1.023 | 1.029 | 0.988 | 1.027 |
| JOB (113) | 1.021 (4/4) | **1.114** (4/4) | **1.145** (4/4) | 1.040 (4/4) | 1.004 | **1.169** |
| TPC-H SF1 | 0.999 | 0.995 | 0.999 | 1.004 | 0.998 | 0.995 |
| TPC-H SF10 | 1.002 | 1.003 | 0.999 | 0.989 | 0.975 | 0.961 |

**F** (kernel dictionary probe) lost: ClickBench 1.2% faster without it (Q23 663 → 575 ms, Q21 381 → 353 ms: running the kernel program over large URL/Title dictionaries costs more than the hand matcher), TPC-H SF10 2.5% faster, JOB flat. It is deleted; the hand matcher (`DictSkipPredicate` kinds 0–4) stays.

**TPC-H SF10 follow-up.** NONE 0.961 left an open question, so the kept set (A+B+C+E) was run against NONE alone for 8 rounds: KEEP/NONE = 0.979, faster in 7 of 8 rounds. The earlier deficit was F plus noise.

**Final build:**
- The 170 comparable queries across ClickBench, JOB and TPC-H SF10 return results identical to the pre-change engine.
- Full serial suite: 39 failed / 10,811 passed, the same 39 as before the change (no new failure).
- `make q` green.

**Removed with the verdicts:**
- the switches `RUGO_ZC_DICT`, `RUGO_SEL_DECODE`, `RUGO_ZC_PLAIN` and `FEATURE_DISABLE_SCAN_WORKER_PREFILTER`;
- the per-entry dictionary copy;
- the post-loop scalar mask compaction, and with it `decoded_row_mask`;
- the per-string copy in `build_direct_string_plain`;
- `simd_compact.hpp`, left with no user by the above;
- all of F.

**Headline.** JOB is 17% faster with the full set than with none. ClickBench's per-part wins are 2–5% each, but the full set is only 2.7% ahead of none, so they don't simply add up. TPC-H SF1 is flat and SF10 is about 2% faster.

## 6. Found in passing (not in scope; for direction)

- **Suspected correctness defect.** In the LIST post-decode mask walk (`dc:2922-2978`), the rep/def levels, strings and `dict_indices` are filtered, but **numeric and boolean `*_values` are not**. A masked `list<int>`/`list<bool>` column may return misaligned values. Unverified by a query.
- **Dead code.**
  - `rugo/src/parquet/page_task.hpp` is included nowhere; its struct is duplicated at `dc:324`.
  - `PageDecompressed` (`dc:338`) is unused.
  - `PreScanPages`' mask scan (`dc:403-423`) is only ever called with `nullptr`.
  - `_FOOTER_PREFETCH` (`pool_reader.pyx:131`) is dead.
- **Other parse sites.** `fetch_column_chunk_info` (`pool_reader.pyx:1050`) and `mabel_connector.py:294` parse footers outside the shared cache.
