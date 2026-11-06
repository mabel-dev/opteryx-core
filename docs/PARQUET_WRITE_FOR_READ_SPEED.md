# Writing parquet for read speed

Status: recommendations (§1) are the measured rules already encoded in rugo's
writer defaults and the design docs they cite; proposals (§2) are hypotheses,
each marked with what evidence it rests on and what decision it needs. §3
records what was BUILT and MODELLED on 2026-10-23 (P4, P6, P7 writer side
built, `make q` + `make slt` green; P1 and P3 modelled). Date: 2026-10-23.

Constraints: every file must be spec-legal parquet readable by pyarrow,
DuckDB and Polars (the five-reader check in `tests/rugo/test_grouped_layout.py`
is the gate). Local and remote (GCS/S3) may use different settings; the
"profile" switch is where they differ.

Provenance tags: **[M]** measured in this repository (doc cited);
**[Model]** from the GET-count model in `scratch/grouped_layout_model/`
(validated within 5% of measured GET counts); **[C]** read from code;
**[H]** hypothesis.

---

## 1. What we already know (the current recommendation)

Sources: [PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md](PARQUET_GROUPED_COLUMN_MAJOR_DESIGN.md),
[PARQUET_DICT_AND_PAGE_PRUNING_DESIGN.md](PARQUET_DICT_AND_PAGE_PRUNING_DESIGN.md),
[PARQUET_WRITER_DESIGN.md](PARQUET_WRITER_DESIGN.md),
[PARQUET_SELECTIVE_DECODE_DESIGN.md](PARQUET_SELECTIVE_DECODE_DESIGN.md),
`rugo/src/parquet/_parquet_writer.hpp`, `rugo/src/parquet/parquet_writer.pxi`.

### 1.1 Layout

| Rule | Value | Evidence |
|---|---|---|
| Row group size | 64k rows | [M] TPC-H SF1/SF10: fastest in parallel; 8k/16k lose to per-morsel cost, 256k −4.7 to −5.6% at SF1. Counter-evidence [M]: ClickBench local, 105 cols string-heavy, 64k is +4 to +9% vs 256k. R is workload dependent; 64k is ruled. |
| Column-major blocks | 4 row groups (262k rows) | [M] ClickBench battery GETs: 64k row-major 3.56× the GETs of 256k; 64k in blocks of 4 −5.8% vs 256k. Local: layout invisible (±0.2%). rugo-only to write (reader infers blocks from chunk offsets); any reader can read it. |
| File size | 4 GiB | Ruled. Footers are read at plan time; footer bytes scale with row-group count: ~6.2 MiB per 4 GiB file at 64k, 21 files per 128 MiB footer cache [M]. |
| Sort order | sort each file by the common filter column; pass `sorted_by` | [M] clustering is what makes every prune bite (CounterID clustered: 5 of 383 row groups kept). `sorting_columns` is trusted only from a rugo `created_by`; flows to the engine as `DRAKEN_ROW_SORTED` [C]. |
| Row groups per file, local | keep in the hundreds, or `RUGO_LOCAL_MMAP_CACHE=1` | [M] one-file-per-table lineitem at 64k with the cache off: Q06 1,884 ms vs 229 ms with it on. |

### 1.2 Encoding and compression

| Rule | Value | Evidence |
|---|---|---|
| Codec | ZSTD; level 4, strings 7 under `profile="storage"` | [M] only BYTE_ARRAY responds to level (4→7: −13.7% string bytes, ~2% on ints for 2–3× compress time). Raw fallback when a chunk does not shrink [C]. |
| Dictionaries | whole-chunk or none, byte-gated | [C] gate: dict page ≤ 1.05×PLAIN − code bytes, ceiling max(1 MiB, 0.75×PLAIN). Never a mid-chunk PLAIN spill: a mixed chunk disables the dictionary decode-skip and loses the zero-copy dict shape. [M] dictionary files 34% faster on string-heavy ClickBench queries. Foreign writers: raise the dictionary page limit well past 1 MiB or disable per high-NDV column. |
| Sorted dictionaries | written, `is_sorted=true` for INT32/INT64/strings | [C] [SORTED_DICTIONARY_DESIGN.md](SORTED_DICTIONARY_DESIGN.md). |
| Encodings | PLAIN, RLE_DICTIONARY, RLE (bool) | [C] DELTA_LENGTH_BYTE_ARRAY and BYTE_STREAM_SPLIT are rejected; DELTA_BYTE_ARRAY decodes whole-page to std::string and defeats raw-page LIKE search; DELTA_BINARY_PACKED decodes value by value. Data page v1 and v2 both read. |
| Pages / page index | opt-in: `max_page_bytes > 0`, `page_index=True` | [M] 6.67× local / 5.38× remote on a clustered key with a wide projection; index ≈ 0.2% of file; a cost gate (index bytes ≤ 0.10 × prunable data bytes) keeps narrow projections from paying. Page SIZE never swept. |

### 1.3 Statistics and optional sections, by what the reader consumes

| Section | Writer | Reader | Note |
|---|---|---|---|
| `Statistics.min_value/max_value/null_count` | written, TypeDefinedOrder, unsigned-lexicographic strings, NaN excluded; BYTE_ARRAY cut to 16 bytes (§3) | the ONLY plan-time footer prune | the legacy `min`/`max` (fields 1/2) are read as a fallback when the v2 fields are absent [C] (an earlier draft of this table said they were ignored — wrong). |
| `is_min_value_exact` / `is_max_value_exact` | written; false for a cut bound (§3) | parsed into `min_exact`/`max_exact`, exposed by `read_rowgroup_stats`; no consumer needs them (pruning treats min/max as bounds) | the patch path carries them. |
| `column_orders` | written | not parsed | harmless. |
| `distinct_count` | exact NDV for every hashable chunk, bloom or not (§3) | aggregated to file level; feeds join / group-by estimators | |
| `rugo.ndv.<column>` (FileMetaData KV) | file-level NDV, exact below 1024 distinct, else K=1024 KMV (§3) | AggregateColumnStats: exact always wins; the estimate is applied only where row-group ranges overlapped | carried by the patch path for copied columns. |
| Bloom filters | split-block, 1% FPP, all equality-friendly columns, file tail | plan-time on local footers only; NEVER fetched remotely (D-6) | ~11.8 B/row on hits at 64k ≈ 12% of the file [M]. See P1. |
| `sorting_columns` | per row group from `sorted_by` | trusted only from rugo `created_by` | |
| `key_value_metadata` (`rugo.sum`, draken logical kind) | written | trusted only from rugo | |
| `encoding_stats` | written (§3) | parsed verbatim; consumed only by the patch path so far | see P7 decision. |
| `size_statistics` | `unencoded_byte_array_data_bytes` written (§3) | parsed; consumed only by the patch path so far | see P7 decision. |
| page CRC | not written | ignored | never pay for it. |
| INDEX_PAGE | not written | fails loud | |

### 1.4 The switch today

Common: 64k row groups, ZSTD fast, whole-chunk dictionaries, PLAIN/RLE_DICTIONARY, v2 statistics, sort by the filter key and declare it, page index once a page size is measured.

| | Remote | Local |
|---|---|---|
| Blocks | 4 per block | irrelevant |
| File size | 4 GiB | row groups per file in the hundreds, or mapping cache on |
| Bloom filters | off or key columns only (never fetched) | on |
| Profile | storage if write-once | fast |

The Makefile `mirror_rugo` recipe (65,536 rows, blocks of 4, zstd, 4 GiB) does
not sort, does not split pages, and leaves blooms on every column.

---

## 2. What we could do differently (proposals)

Ordered by expected payoff. Each is spec-legal. None is built.

### P1. Remote profile: key-column blooms the planner can reach **[H]**, re-opens D-6

Why we do little with blooms today (the record, 2026-08 to 2026-09):
- The read-side probe is gated to local paths (`_bloom_excludes` runs only
  under `_is_local_path`); a remote probe needs a ranged fetch of the bloom
  region and was never built.
- D-6 (2026-09-24) dropped the remote bloom *decode-skip*: it saved CPU,
  never bytes, and either fetch shape costs requests.
- Measured 2026-08-25: on clustered keys (CounterID = 62) the dictionary
  probe killed zero row groups beyond footer min/max; the only earnings are
  scattered-key equality (Q20 shape) and LIKE. Blooms have the same reach.
- skene ruling 2026-08-16: blooms string-only; numerics get nothing because
  zone maps already prune them. Bloom build was 47% of skene write time;
  blooms are ~12% of a rugo file and 21% of a skene file.

So the proposal is narrow: write blooms only for caller-named key columns
under the remote profile (bytes −10%), and read them at plan time with one
tail GET per file, only when a pushed `=`/`IN` names such a column, to prune
row groups before the data plan. Unlike D-6 this saves data GETs and bytes,
not CPU. Payoff exists only for scattered-key point lookups, which is one
ClickBench query and an unknown share of real log-table workloads.
Break-even: one GET per file versus the row-group fetches avoided; the
bloom for a 64k-row high-NDV key is tens of KB per row group, so it is its
own range, never part of the footer envelope. Decision needed: whether the
scattered-key shape matters enough to re-open D-6.

### P2. Remote profile: bigger blocks **[Model]**, re-opens the 256k ruling

Not tested for parquet remote. The 4Mi-row figure (4,731 GETs vs 23,943)
is the GET-count model only. The 256k ruling was about writer buffer
memory (G = 64 means a ~380 MiB block). The one *measured* bigger-block
result is negative and local: skene v3 with blocks of 4 over 262k-row row
groups (≈1M-row blocks) made ClickBench Q21–23 +7 to +9% on M5, G = 1
recovered it, attributed to locality of the queued follower row groups.
A remote parquet measurement at 16 row groups per block (1M rows, ~100 MB
buffer) is the missing arm. The 8 MiB coalescer cap floors wide string
columns at bytes/8 MiB GETs regardless, so the gain is on narrow columns.

### P3. Column placement order inside a block **[H]**, cheap to model

Mechanism: `build_remote_plan` merges two extents into one GET when the gap
between them is within the waste budget (cumulative gap ≤ 0.10 × useful
bytes, span ≤ 8 MiB). Inside a block the column runs sit in schema order:
`c1[rg1..rg4] c2[rg1..rg4] c3[...]`. A query projecting c1 and c3 has c2's
whole run as the gap, so whether it merges depends on c2's width. On hits
55 of 105 columns are under 0.1 B/row and a handful (URL, Title, Referer)
are 20–50 B/row, interleaved in schema order. Placing narrow columns
first and wide last makes the narrow set contiguous, so a projection of k
narrow columns is one or two GETs per block instead of up to k; wide
columns each cost their own GETs anyway under the 8 MiB cap. Bound on the
payoff: in the model, requests are ⌈GETs/32⌉ × RTT ≈ 97 s of the battery's
~480 s additive, bytes the rest, so P3 can touch at most that fifth. The
GET model can score it from footers without writing a file.

### P4. NDV: where the circle is **[C]**

Three things exist and one is missing:
- Per-chunk `distinct_count` in the parquet footer: written by rugo, exact,
  but only inside the bloom build (`_parquet_writer.hpp` bloom branch); a
  column with blooms off has none. Parsed and exposed via
  `read_rowgroup_stats`.
- The manifest's `distinct_counts` column (estimate-only, 2026-09-15) and
  the manifest sketches (KMV, Stage 1 landed): the planner's NDV source.
- A footer KMV sketch (FOOTERKMV): built, measured a wash on TPC-H, TPC-DS
  and ClickBench, REMOVED 2026-10-20.
- Missing: the **file-level NDV scalar in the footer** identified
  2026-10-20 as the fix for rugo file-level NDV running 5–50× low on
  mid-cardinality multi-row-group columns (TPC-DS Q72 3.5× slower than on
  skene). Ruled as the fix, NOT built.

The small coupling (NDV lost when blooms are off) is real and a one-line
fix: the row hashes already exist for the dictionary decision. The item
that matters is the file-level scalar, and it is the one that has not moved.

### P5. Page index by default when `sorted_by` is set, then sweep the page size **[M]** one shape

What it is: today a column chunk (64k rows) is one data page, and the
footer's min/max is per chunk. With `max_page_bytes > 0` the chunk is cut
into several pages on a shared row grid, and the tail carries per-page
min/max and null counts (ColumnIndex) plus page offsets (OffsetIndex). A
pushed predicate then keeps only the pages whose range matches: finer than
row-group pruning without changing the morsel (the row group stays 64k).
Remotely the reader fetches only the kept pages' ranges; locally it skips
their decompress and decode. It is the parquet form of skene's 8192-row
zone maps, whose skippable ceiling measured 71% of chunks on Q20.

Why tie it to `sorted_by`: on an unsorted column every page spans the
whole range, nothing prunes, and the file paid index bytes, page headers
and lost zstd context at every page boundary. The reader's cost gate
(index bytes ≤ 0.10 × prunable data bytes) already stops narrow
projections from paying for the index read.

Pages and the index are opt-in and no sink or mirror uses them; the
measured 6.67× local / 5.38× remote is one shape at one page size. Sweep
64 KiB to 1 MiB on a sorted mirror before fixing the number.

### P6. Footer diet: truncated string stats and per-column opt-out **[C]** not done today

rugo does NOT truncate: the BYTE_ARRAY branch of `compute_stats` assigns
the full min and max value (`st.min_bytes.assign(lo.ptr, lo.ptr + lo.len)`)
and writes the exact flags true. The 16-character memory is the manifest
and skene zone-map side, where strings are ordinalized to an int64 from
their leading bytes; parquet footers carry whole values, so a long Title
or URL writes two full strings per chunk.

Proposal: truncate BYTE_ARRAY min/max to 16 bytes (max rounded up by
incrementing the last byte, exact flags false, the Arrow convention), and
a `stats_columns` opt-out so free-text columns carry no stats or blooms.
At 64k the footer is ~86 B/chunk [M] and the string bounds are the bulk of
it on text columns; D-5 (footer cache capacity) is open. Pruning stays
sound without reading the exact flags (a truncated bound is still a
bound). Check at implementation: `write_parquet_with_bounds` builds the
catalog FileEntry bounds from these stats, so truncation propagates to the
manifest unless bounds are taken from the full values.

### P7. Write `encoding_stats` and `size_statistics`; read them **[C]** writer side trivial, reader work real

`encoding_stats` states from the footer whether every data page is
dictionary-encoded. Today the reader proves that by walking every page
header (`PreScanPages`) after the chunk is fetched. With the footer answer,
the dictionary skip can be decided from the dictionary page alone, fetched
first (its extent is `data_page_offset − dictionary_page_offset`), so on a
remote path a rejected row group's data pages are never fetched. That is
a two-phase fetch in the reader; the writer field is one line.
`unencoded_byte_array_data_bytes` lets the string arena be sized once per
chunk instead of grown. Modest CPU.

### P8. Benchmark mirrors exercise the recipe **agreed 2026-10-23**

Architect: the benchmarks show off what the system can do; every corpus
except canon ClickBench may be rewritten. So `mirror_rugo` moves to the
full recipe once three inputs exist: a sort key per table, the page size
from P5's sweep, and the key-bloom column list per table. Canon ClickBench
stays as it is.

### Not proposed, and why

- BYTE_STREAM_SPLIT for floats: bytes, not speed; reader rejects it; a
  reader+writer pair for a small remote-only gain.
- DATA_PAGE_V2: levels stay uncompressed, marginal; both page versions read.
- Column-chunk `file_path` (hot columns in one file, wide in another, one
  footer): spec-legal, but pyarrow and most readers do not follow it.
- CRCs: ignored by the reader.
- A file-level "sorted" claim: derivable from `sorting_columns` plus
  monotone row-group bounds; the reader can derive it, nothing to write.

### 2.1 The switch, with the proposals

| | Remote | Local |
|---|---|---|
| Blocks | P2: 16 per block (1M rows) | 1 or 4, irrelevant |
| Blooms | P1: key columns only, planner-reachable | all equality columns |
| NDV | P4: always | P4: always |
| Pages | P5: on when sorted | P5: on when sorted |
| Stats | P6: truncated, filterable columns only | same |
| Column order | P3: narrow-first | schema order |

### 2.2 Open evidence gaps

- No remote number is from real GCS; all are loopback throttle-server
  measurements or the GET model.
- Page size has never been swept.
- R is measured both ways (TPC-H favours 64k, ClickBench local favours
  256k); a per-profile R is the one knob with contradictory evidence.

---

## 3. Built and modelled, 2026-10-23

### 3.1 Built (main, uncommitted; `make q` and `make slt` green)

| Item | What | Where |
|---|---|---|
| P6 | BYTE_ARRAY chunk min/max longer than 16 bytes written as bounds of at most 16 bytes: lower = prefix cut on a UTF-8 character boundary, upper = cut after the last incrementable byte (UTF-8: ASCII < 0x7F) with it incremented; exact flags false. No incrementable byte in the first 16 → max written whole, exact. A cut min can never equal a max, so `min == max` still means a constant chunk. The page index (ColumnIndex) is NOT truncated: the cut is not monotone across pages and the index declares a boundary order. Manifest bounds (`write_parquet_with_bounds`) stay exact: truncation happens only at footer serialisation. | `_parquet_writer.hpp` `truncate_stat_lower/upper` |
| P4 | Every hashable chunk (ints, floats, strings, FLBA) writes an exact `distinct_count`, bloom filter or not. The writer's dictionary decision now always receives that exact count, so its KMV estimate branch (`dict_estimated_ndv`) is deleted. File-level NDV per column merged from per-chunk K=1024 sketches over the hashes the writer already computes, stated as `rugo.ndv.<column>` = `exact:<n>` / `kmv1024:<n>`. The reader applies an exact count always and an estimate only where the row-group merge saw overlapping ranges (sorted or touching row groups stay on the merge, which is tighter). | `file_ndv.hpp`, `FileNdv`, `AggregateColumnStats` |
| P7 (writer) | `encoding_stats` (dictionary page + data page counts) and `size_statistics.unencoded_byte_array_data_bytes` written for every chunk; the reader parses both and the patch path carries them verbatim, so a rename still changes only the name. | `chunk_encoding_stats`, `ParseColumnMeta` cases 13 / 16 |
| patch | Carries exactness flags, encoding_stats, unencoded bytes, and a rugo source's `rugo.ndv` for byte-copied columns under their new names (never a foreign file's key, never for re-encoded columns). | `_parquet_patch.hpp` |

Evidence [M]:
- On main, before: a half-hot-value skewed key (the ss_customer_sk shape) read file NDV 644 against a true 16,239; a 700-value shuffled key read 561. After: both within the sketch's error / exact (`tests/unit/optimizer/statistics/test_parquet_footer_ndv_estimate.py`).
- Footers decoded by thriftpy2 from the vendored parquet.thrift, pyarrow 25.0.1 and DuckDB 1.5.4 (`min_is_exact`/`max_is_exact`, `parquet_kv_metadata`) all read the new sections (`tests/rugo/test_parquet_footer_sections.py`, 16 tests).

Test changes that move an assertion (not loosen it): `test_all_metadata_fields` gains the two flags; the longer-rename test now counts the name three times (the NDV key embeds it).

Write cost, measured 2026-10-23: interleaved A/B, process per arm, 6 rounds, median of 15 writes after 3 warm-ups. Input is a 200k × 50 CSV. The old arm is this writer with only the NDV changes reversed. Harness: `scratch/grouped_layout_model/writer_ab/`.

| shape | NDV work as first built | + `KmvSketch::add_many` (one-pass K smallest per chunk) | + linear sorted-union `KmvSketch::merge` (chunk → file sketch) |
|---|---|---|---|
| wide, blooms | 1.09 | 1.056 | **1.028** (69.6 → 71.6 ms) |
| wide, no blooms | 1.07 | 1.041 | **1.009** |
| 5,000 rows | 1.75 | 1.364 | **1.053** (1.34 → 1.41 ms) |
| floats only | 1.13 | 1.088 | **1.040** |

Each fix leaves the sketches identical, checked against the per-hash `add()` reference (198 add cases, 1,538 merge cases including self-merge and a 200-chunk fold). Output files are byte-identical across 10 write shapes once `(build N)` in `created_by` is normalised. In a profile, the 5,000-row residual before the merge fix was `FileNdv::add` → `merge`: one binary search and insert per hash, 0.37 of 0.48 ms, serial on the calling thread. The linear merge beat its predecessor in 24 of 24 per-round pairs. The mirrors were rebuilt under `.rugo-v2`; the fixes do not change bytes, so they need no rebuild.

### 3.2 P3 modelled: column order inside a block [M geometry, Model GETs]

`scratch/grouped_layout_model/layout_model_real_footers.py` on the REAL footers of `scratch/hits_rg64k_b4` (100 files, 1,600 row groups), footer min/max pruning, rugo's coalescer rule. The model reproduces the measured battery: 25,528 modelled GETs vs 25,150 measured (§9.2 of the grouped-layout doc).

| Order | GETs | GiB | additive s |
|---|---|---|---|
| schema (today) | 25,528 | 24.08 | 491.4 |
| narrow-first by bytes | 26,410 (+3.5%) | 24.06 | 494 |
| co-access greedy, fitted to this battery | 21,162 (−17%) | 24.09 | 474.1 (−3.5%) |
| floor: every query's columns contiguous at once (unreachable) | 14,798 (−42%) | — | — |

Reading: a width heuristic LOSES — ClickBench's schema already sits co-accessed columns together (RegionID, UserID). Even an order fitted to the exact workload buys 3.5% of remote time, because bytes (≈385 s at 64 MiB/s) dominate requests (≈104 s). **Recommendation: do not build.** Revisit only if a sink knows its workload and remote latency rather than bandwidth dominates.

### 3.3 P1 modelled: planner-reachable key blooms, Q20 [M blooms, Model time]

Same files; UserID = 435090932899640449; the files' REAL UserID blooms probed with rugo's own `bloom_filter_maybe_contains`.

| | row groups kept | data GETs | data MiB | bloom fetch | Q20 additive s |
|---|---|---|---|---|---|
| today (min/max only) | 834 | 353 | 134.4 | — | 3.66 |
| + plan-time bloom read | 6 | 6 | 1.1 | 95 GETs, 32.9 MiB | 1.05 (3.5x) |

The blooms already exist in every file (the writer default); what is missing is a REMOTE plan-time read of one column's bloom run per file (column-major tail: one range). It costs one serial round of GETs before the data plan, which is pure loss on a clustered key (CounterID = 62 prunes to 5 row groups on min/max alone). **Decision needed:** build the remote plan-time bloom read with a footer-arithmetic gate (bloom-run bytes against the min/max-kept data bytes, as the page-index gate does), or leave the scattered-key lookup slow. Dropping blooms from non-key columns is a separate, storage-only saving (~12% of bytes); it does not change any read.

### 3.3a P1 gate: why a footer-arithmetic gate cannot work [M blooms + footers, Model time]

Architect asked (2026-10-23) for the remote plan-time bloom read with a
page-index-style footer gate. Measured BEFORE building
(`scratch/grouped_layout_model/bloom_gate_model.py`; every battery query with a
pushed =/IN; real per-row-group blooms probed with rugo):

| Query | files | rg kept by min/max | rg kept after bloom | bloom bytes / prunable bytes (p50) |
|---|---|---|---|---|
| Q20 UserID = | 95 | 834 | 6 | 0.150 |
| Q37-Q40, Q43 (CounterID, flags) | 3 | 15 | 15 | 0.000-0.001 |
| Q41 (+ RefererHash =, TraficSourceID IN) | 3 | 15 | 15 | 0.055 |
| Q42 (+ URLHash =) | 3 | 15 | 15 | 0.145 |

- **A byte-ratio gate (the page-index form) cannot separate them.** The
  losers have SMALLER ratios than the winner. At any R that admits Q20
  (>= 0.19), all seven others probe and reject nothing: +0.13 s each (one
  serial round) on 0.17-0.58 s queries (+22% to +76%). Battery sum 5.80 ->
  4.45 s, carried entirely by Q20. At R = 0.10 nothing fires except the losers.
- **A presence estimate from the footer fails too.** distinct_count / value
  span predicts P(reject) = 1.0 for Q20 (right: 828 of 834) AND for Q41/Q42
  (wrong: 0 of 15) — the queried hash values are real, frequent values. A
  literal in a WHERE clause is usually one that exists; the footer cannot know
  how often.
- The cost being gated is latency (one serial round before the data plan),
  not bytes, and no footer field predicts whether it is repaid.

Options put to the architect (not built):
1. Build as asked (byte gate, R chosen so Q20 fires): net battery win, seven
   per-query regressions.
2. Move the bloom read into the native scan: issue the bloom runs in the SAME
   concurrent round as the first fetch blocks and drop not-yet-fetched work
   items as bloom results land (the scan already cuts blocks for LIMIT and
   runtime pruning). No serial round; the downside is the bloom bytes only.
3. Option 1 or 2 plus a budgeted bloom-run cache (files are immutable), so
   repeat queries pay nothing; no help under the official process-per-query
   ClickBench shape.

### 3.3b Built: the native bloom round (option 2, ruled 2026-10-23)

Main, uncommitted; `make q` and `make slt` green.

| Phase | Where | What |
|---|---|---|
| Planning | `pool_reader.pyx` `_remote_bloom_probes` | For a REMOTE file's min/max-kept row groups, the Eq/InList bloom tests (shared `_bloom_probe_list`, the same list the local plan-time read uses), attached to the native plan's work items. A file is probed only when its bloom bytes are <= 0.25 x the projected + predicate chunk bytes they guard (Q20 measured 0.150 p50 / 0.189 max). |
| Execution | `parquet_bloom_probe.hpp`, `NativeParquetScanSource::make_global` | Each file's bloom ranges coalesced by the pipeline's own coalescer (`coalesce_into`, factored out of `plan_block`), issued as one pool ticket PER FILE (`fetch_ranges_async`; one get_many per file, because one get_many for all would funnel through one client's 3-per-host connection cap), on the fetch-ahead pool when armed. Returns at once. |
| Exclusion | `get_morsel` walk | Once the round lands (`poll_bloom_round`, under the scan lock), units whose bloom proves every needle absent are flagged; a flag becomes a skip only when the walk reaches that unit, through the same `unit_skip` / `units_skipped` the Top-N boundary uses (renamed from `topn_skip`), so already-submitted units are read as before and the finish count stays exact. A fetch failure fails the scan (read error); a bloom that fails to parse keeps its row group (the plan-time contract). |
| Telemetry | `row_groups_pruned_bloom` | Present only when a probe was planned; folded into `row_groups_pruned` / `row_groups_read`. |

Measured [M] on the test fixture (one file, 400 row groups of 512 rows, scattered int64 key, `dev/throttle_server.py`, `tests/unit/connectors/parquet_io/test_remote_bloom_round.py`): in-flight window 66 units; the point lookup reads 130-170 of 400 row groups at RTT 0-130 ms (the rest skipped) — the units walked before the 416 KB bloom run lands are read as before. Answers equal the local read in every case; a literal in every row group skips 0.

Not covered: the latmat (two-pass) and trampoline scans still read remote blooms not at all; local files keep the plan-time pread.

**Remote ClickBench A/B, 2026-10-23 [M].** `scratch/hits_rg64k_b4` (100 files, 64k row groups, blocks of 4) over `dev/throttle_server.py` at 130 ms RTT, 16 Mbit/s per connection (~64 MiB/s at 32 streams); one process per run, chunk cache off, arms interleaved with the order alternating per query and round, 5 rounds, twice (second run recording execution time alone). Arm "off" = a temporary planner switch in the private bench tree that planned no probe (deleted after; never in main). Harness and logs: `scratch/grouped_layout_model/bloom_ab/` (re-add the two-line switch to reproduce the off arm).

| Query | execution off -> on, s (min of 5) | on/off | row groups read | GETs | MiB |
|---|---|---|---|---|---|
| Q20 UserID = | 4.473 -> 1.303 | **0.29** | 834 -> 145 | 335 -> 198 | 134.4 -> 46.3 |
| Q37 | 2.657 -> 2.684 | 1.01 | 15 -> 15 | 16 -> 25 | 14.9 -> 14.9 |
| Q38 | 0.525 -> 0.527 | 1.00 | 15 -> 15 | 16 -> 25 | 2.4 -> 2.4 |
| Q39 | 2.686 -> 2.638 | 0.98 | 15 -> 15 | 16 -> 25 | 14.9 -> 14.9 |
| Q40 | 4.685 -> 4.638 | 0.99 | 15 -> 15 | 13 -> 19 | 27.7 -> 27.7 |
| Q41 | 1.863 -> 1.884 | 1.01 | 15 -> 15 | 17 -> 29 | 9.7 -> 10.2 |
| Q42 | 1.178 -> 1.211 | 1.03 | 15 -> 15 | 20 -> 32 | 6.0 -> 6.9 |
| Q43 | 0.543 -> 0.528 | 0.97 | 15 -> 15 | 12 -> 21 | 2.5 -> 2.5 |

- Q20 execution 3.4x faster; total wall 18.55 -> 15.37 s. The model (§3.3) predicted 3.66 -> 1.05 s.
- Q37-Q43: blooms reject nothing; the round costs 6-12 extra GETs issued in parallel with the first wave and up to +0.9 MiB (Q42's URLHash blooms). Execution within -3% .. +3% (mixed signs; medians -2% .. +4%) — no serial round, as designed.
- Answers: identical row digests across arms for Q20, Q37, Q38, Q43; for Q39-Q42 the off arm alone produced 5 different row digests in 5 runs (ties under `ORDER BY PageViews ... LIMIT/OFFSET`), and the sorted PageViews keys are identical across arms.
- **Separate finding, not this feature:** every query's wall time carries ~14 s of planning, almost all `time_binding_metadata` — on this harness's served HTTP filesystem the binder reads the 100 footers one after another (~130 ms each). Execution is 0.5-4.7 s of 14.6-19 s. Not investigated; whether production GCS binding takes the same serial path is unverified.

### 3.4 Still to do

- **P7 reader decision.** With `encoding_stats`, the dictionary decode-skip could be decided from the dictionary page alone, fetched before the data pages. On a remote path that is a second serial round trip for every row group the dictionary does NOT reject. Needs a ruling: two-phase fetch (gated how), or footer knowledge used only to skip the local `PreScanPages` header walk.
- **P5 (heavy).** Page-size sweep on a sorted TPC-H SF1 mirror. Wait for "machine's free".
- **P2: measured 2026-11-06, see §4.** 16- and 64-row-group blocks over hadro shaped to the fleet and on the fleet VM against GCS: blocks of 4 stay the default; a per-table larger block for narrow tables needs a ruling (§4.5).
- **P8.** Mirror recipe needs a sort key per table, P5's page size and the P1 decision.

## 4. Fleet-regime IO sweep, 2026-11-06: coalescer knobs and block size [M]

Production moved from Cloud Run to n4a-standard-4 VMs (4 vCPU, us-east1,
GCS over gRPC). Job telemetry from the fleet node, 2026-10-30..11-05: 71% of
range GETs under 50 ms (Cloud Run: 41%), scan throughput p50 15 MB/s, p90 420
MB/s, max 957 MB/s, median 12.7 requests in flight against a limit of 66. The
§7.3 read model with those inputs makes REQUEST count the larger term on the
ClickBench battery (≈79 s of requests against 19-44 s of bytes); under the
model's original 130 ms / 64 MiB/s, and under Cloud Run, bytes dominated. This
sweep asks whether the knobs chosen in the bytes-dominated regime still hold.
Harness, raw JSONL and rewrite logs: `scratch/hadro_io_sweep/`.

### 4.1 Method

- **Server:** hadro (the S3 test server, `../hadro`) over `scratch/`, each
  sub-directory a bucket, shaped `--latency-ms 30 --latency-jitter-ms 15
  --bandwidth-mbps 1800 --total-bandwidth-mbps 7000 --fault-seed 7`. Measured
  with curl while a sweep ran [M]: 64 KiB range time-to-first-byte 32-42 ms;
  one 20 MB stream 92 MB/s; 12 parallel 20 MB streams 450 MB/s aggregate.
  Unshaped, hadro does 2.9 GB/s on one stream and 3.3 GB/s on eight, so the
  caps are what the reader sees. Per-stream delivery (92 MB/s) is below the
  fleet's measured 220 MiB/s per stream (2026-10-30 curl from an
  n4a-standard-4), which is why the VM check below matters for anything that
  merges requests into larger ones.
- **Client:** one fresh interpreter per (round, arm) running the whole battery
  (`child.py`), `CHUNK_CACHE_MEMORY_PERCENT=0`, `MAX_EXECUTION_WORKERS=4` (the
  node's DOP), knobs as environment variables, S3 via `OpteryxS3FileSystem`
  (native scan path; `scan_sources` = NativeParquetScanSource on every query).
  A warm query first so footers are cached; arms rotate order every round; the
  figure is the sum over queries of each query's minimum wall (the §9.3 method
  of the grouped-layout doc). Range GETs, fetch ops and bytes are the
  pipeline's own `io_scan_diagnostics`.
- **VM confirmation:** the same battery from `gcs_child.py` inside the
  production image (`opteryx-worker:fleet`, digest `d92eea6c…`, opteryx-core
  0.9.191) on `worker-n4a-test` (n4a-standard-4, us-east1-b) against copies of
  the same files under `gs://opteryx_data/_bench/hadro_sweep/`, `GCS_TRANSPORT=grpc`,
  otherwise the same settings. GET counts matched hadro's exactly on every
  query checked (pypi: 189 / 4 / 361 / 217), so both paths issue the same
  requests; only the wall clock differs.
- **Datasets:**
  - `pypi_b4|b16|b64`: four production `opteryx.test.pypi_large` data files as
    written by the fleet (36-60 MB, 12.3M rows each, 19 columns, 65,536-row row
    groups in blocks of 4; the 4 GiB sink target is UNCOMPRESSED, and this
    table compresses ~110×), rewritten at 16 and 64 row groups per block with
    `dev/rewrite_parquet_layout.py`. A 256k-row block of one column is ≈2.5 KB.
    Ten queries (`battery.json` → `pypi`): the production shapes (`SELECT project
    … GROUP BY ALL`, point lookups, `SELECT *` with a filter, `LIMIT 50`), plus
    two with non-adjacent columns.
  - `hits_4g_b4|b16|b64`: the 99M-row ClickBench `hits` repacked into 4 GiB
    files (`--file-bytes 4294967296`; NB that is COMPRESSED bytes, so these are
    2 × 4.6 GiB files, larger than the sink would write) at 4, 16 and 64 row
    groups per block, same rows, same 64k row groups. Battery Q00-Q42; Q28
    errors in every arm (`REGEXP_REPLACE` replacement syntax) and is excluded.
    The 100 × 1M-row `hits_rg64k_*` sets are NOT usable as block arms: at 1M
    rows a file is one block of 16, so b16 and b64 are the same layout.
- **Answer check:** row-set digest per query, order-insensitive. Identical
  across arms on every pypi query and on 34 of 42 hits queries; the eight that
  differ (Q24, Q30-32, Q38-41) differ BETWEEN ROUNDS OF THE SAME ARM too (tie
  order under `ORDER BY … LIMIT`, float summation order in `AVG`), all return
  10 rows. Not an arm effect.
- **Rewrite memory:** `/usr/bin/time -l` on the rewrite tool is read-dominated
  (20 GB RSS on pypi at every block size; it reads each file whole) and says
  nothing about the writer's block buffer. The hits repack (one streaming
  writer) measured 5.9 / 6.2 / 7.3 GB RSS at b4 / b16 / b64.

### 4.2 Coalescer waste ratio — inert or harmful; keep 0.10

pypi, hadro, 10 queries × 3 rounds: ratio 0.10 / 0.5 / 1.0 / 2.0 give
byte-identical plans — 1,347 GETs, 0.13 GiB, 3.74-3.84 s. On a narrow table
every chunk a block holds sits inside `PARQUET_IO_COALESCE_GAP_BYTES`
(512 KiB), so the gap rule already merges the whole projection and the ratio
never decides anything.

hits, 42 queries × 3 rounds:

| Arm | hadro Σ min wall | vs 0.10 | GETs | vs 0.10 | VM Σ min wall | vs 0.10 |
|---|---|---|---|---|---|---|
| ratio 0.10 (today) | 37.21 s | — | 16,978 | — | 39.88 s | — |
| ratio 0.50 | 37.53 s | +0.9% | 16,809 | −1.0% | | |
| ratio 1.00 | 38.70 s | +4.0% | 16,088 | −5.2% | | |
| ratio 2.00 | 41.12 s | +10.5% | 15,080 | −11.2% | 40.52 s | +1.6% |

Bytes read are identical (13.84 GiB; the pipeline counts useful bytes). The
merged requests cost time on hadro (Q22 2.87 → 3.78 s, Q31 2.02 → 2.99 s) and
most of that is hadro's 92 MB/s per-stream pacing: on the VM against GCS the
same arm is +1.6%, a wash within round-to-round noise, for 11% fewer GETs.
There is no regime in which it wins. **Keep 0.10.**

### 4.3 Coalescer max bytes — does not bind at blocks of 4; keep 8 MiB

hits, hadro: 8 MiB (today) 37.31 s / 16,970 GETs; 32 MiB 37.58 s / 16,827;
unbounded 37.36 s / 16,851. A block of 4 × 64k rows rarely puts more than
8 MiB of one projection in a run, so the cap is reached only on the widest
string columns (Q20/Q33/Q34: 399 → 382 GETs). Nothing to gain; not re-run on
the VM.

### 4.4 Block size — the trade is GETs against pipeline parallelism, and it goes opposite ways on narrow and wide tables

Narrow table (pypi), 10 queries × 3 rounds. `g0` = `PARQUET_IO_FETCH_AHEAD_MIN_BLOCKS=0`:

| Arm | hadro Σ min wall | vs b4 | VM Σ min wall | vs b4 | GETs | fetch ops | in-flight (median, VM) |
|---|---|---|---|---|---|---|---|
| b4 (today) | 3.55-3.80 s | — | 2.95 s | — | 1,347 | 1,147 | 11.9 |
| b16 | 3.58-3.80 s | ±0% | 2.94 s | −0.4% | 449 | 298 | 4.7 |
| b64 | 5.21-5.68 s | +47-50% | 3.77 s | +28% | 178 | 82 | 1.7 |
| b16-g0 | 3.58 s | +0.9% | 3.04 s | +3.1% | 449 | 298 | 4.6 |
| **b64-g0** | **3.34 s** | **−5.9%** | **2.73 s** | **−7.4%** | **178** | **82** | 2.5 |

Reading. Blocks of 16 cost nothing and remove two thirds of the requests.
Blocks of 64 are SLOWER as shipped and FASTEST with the gate lowered: four
files × 188 row groups at 64 per block is 12 fetch blocks, under the
`PARQUET_IO_FETCH_AHEAD_MIN_BLOCKS` default of 48, so the scan runs the coupled
path (fetch and decode on the same four threads, ≤4 requests in flight). The
gate counts fetch blocks [D-1], so a bigger block makes the same scan look
smaller to it. The pruned point lookups (P4 `SELECT * WHERE project = …`, P8
`LIMIT 50`) cost more GETs at big blocks (4 → 37) because a kept row group's
chunks for different columns are no longer within 512 KiB of each other — the
§5.6 shape — but they read ~0.1 MiB and their wall does not move.

Wide table (hits, 105 columns, 42 queries × 3 rounds), hadro:

| Arm | Σ min wall | vs b4 | GETs | vs b4 | fetch ops | mean GET | in-flight (median) |
|---|---|---|---|---|---|---|---|
| b4 (today) | 37.30 s | — | 16,979 | — | 9,854 | 0.85 MB | 21.2 |
| b16 | 46.85 s | +25.6% | 5,810 | −65.8% | 2,563 | 2.5 MB | 11.3 |
| b64 | 112.15 s | +200.6% | 3,091 | −81.8% | 677 | 4.7 MB | 6.5 |
| b16-g0 | 46.70 s | +25.2% | 5,825 | −65.7% | 2,573 | 2.5 MB | 11.5 |
| b64-g0 | 61.48 s | +64.8% | 3,113 | −81.7% | 681 | 4.7 MB | 9.3 |

Here the request saving is real (−66% / −82%) and the wall still gets WORSE.
Bytes are identical, so it is not the GETs or the bytes: it is the pipeline
granularity. A fetch block is fetched whole before any of its row groups
decodes, so 16 or 64 row groups wait on one batch instead of four; with four
decode workers the fetch/decode overlap that b4 gets from 9,854 small units
collapses to 2,563 or 677 big ones (in-flight 21 → 11 → 6.5), and on a
per-stream-capped link each 2.5-4.7 MB GET also takes longer to land. Q22
(SELECT with URL and Title, 1.98 GiB): 2.87 → 3.74 → 12.11 s. At b64 the two
files are 13 blocks each, so the gate bites on top (112 s coupled vs 61 s with
g0). The gate alone is not the story on wide tables: b16 is identical with and
without it (51 blocks per file, above the gate), and is still +25%.

Wide table on the VM (real GCS, gRPC, 220 MiB/s per stream), same arms, 42
queries × 3 rounds:

| Arm | Σ min wall | vs b4 | GETs | vs b4 | fetch ops | in-flight (median) |
|---|---|---|---|---|---|---|
| b4 (today) | 40.05 s | — | 17,028 | — | 9,909 | 16.9 |
| b16 | 41.07 s | +2.5% | 5,883 | −65.5% | 2,618 | 9.4 |
| b64 | 51.72 s | +29.1% | 3,109 | −81.7% | 682 | 4.1 |
| b16-g0 | 41.99 s | +4.8% | 5,814 | −65.9% | 2,566 | 9.3 |
| b64-g0 | 41.47 s | +3.5% | 3,123 | −81.7% | 685 | 6.2 |

Per query (min wall / GETs / MiB), VM:

| Query | b4 | b16 | b64 | b64-g0 |
|---|---|---|---|---|
| Q01 `COUNT(*) WHERE AdvEngineID <> 0` | 0.48 s / 339 / 1 | 0.44 s / 96 / 1 | 0.53 s / 24 / 1 | **0.31 s** / 24 / 1 |
| Q04 `AVG(UserID)` | 0.78 s / 382 / 234 | 1.00 s / 96 / 234 | 1.21 s / 45 / 234 | 0.77 s / 45 / 234 |
| Q18 `UserID, EventDate GROUP BY` | 3.04 s / 946 / 667 | 3.14 s / 289 / 667 | 3.37 s / 127 / 667 | 3.03 s / 127 / 667 |
| Q20 `UserID = …` | 1.76 s / 399 / 1022 | 2.02 s / 181 / 1022 | 2.42 s / 151 / 1022 | 1.92 s / 151 / 1022 |
| Q22 `SELECT … URL, Title` (1.98 GiB) | **3.05 s** / 1,185 / 1977 | 3.24 s / 459 / 1977 | 3.95 s / 333 / 1977 | 3.28 s / 333 / 1977 |
| Q31 wide `GROUP BY` | **1.39 s** / 1,380 / 1138 | 1.57 s / 555 / 1138 | 2.08 s / 229 / 1138 | 1.57 s / 229 / 1138 |

Same GETs as hadro to the request, same direction, smaller magnitudes:
against GCS a 2.5-4.7 MB GET lands in a fifth of the time it takes on hadro's
92 MB/s streams, so the granularity penalty that hadro put at +25% / +65% is
+2.5% / +3.5% on the fleet (b16 / b64-g0), and the un-gated b64 at +29%
rather than +200%. The per-query split is the finding: queries over NARROW
columns (Q01, Q07: 1 MiB over 339 GETs) run 35% faster at b64-g0, because
their cost is round trips; queries over WIDE columns (Q22, Q31: 1-2 GiB) run
7-13% slower, because their cost is bytes and a bigger block delays the first
decode. One block size per file cannot have both.

### 4.5 Recommendation and what needs a ruling

1. **Coalescer: no change.** Ratio 0.10 and cap 8 MiB are right in the fleet
   regime too (§4.2, §4.3), measured on hadro and confirmed on the VM.
2. **One block size per file: do not raise the default.** On the fleet VM
   blocks of 16 are free on the narrow table and +2.5% on the wide one; blocks
   of 64 lose as shipped (+28% narrow, +29% wide, the gate) and with the gate
   lowered are −7% on the narrow table and +3.5% on the wide one, where the
   wide-column queries pay 7-13% so the narrow-column ones can gain 35%. A
   per-FILE block size trades one set of queries against the other. The
   4-per-block default stands for the general writer, and the 2026-09-24
   ruling that "writes stay in 256k blocks" holds — for a reason the GET model
   does not see: a block is fetched whole before any of its row groups
   decodes, so block BYTES set the pipeline's granularity against the
   4-worker decode pool, while block COUNT sets the GETs.
3. **Proposed — a per-COLUMN block size, chosen by chunk bytes.** The two
   effects in (2) pull on different columns of the same file: a narrow
   column's cost is round trips (hits Q01: 1 MiB over 339 GETs), a wide
   column's is bytes (Q22: 1.98 GiB over 1,185 GETs, 1.7 MB each). So keep
   wide columns in blocks of 4 and give narrow columns a long run — up to the
   whole file when the column is small (threshold in bytes, e.g. a column
   whose chunks are under some KB, or whose whole-file size is under ~1 MB).
   Why this needs no reader change: `infer_fetch_blocks` splits a block
   wherever ANY projected column's chunk is not byte-adjacent to its
   predecessor, so a projection of narrow columns alone reads the long runs
   as a few big (byte-small) fetch blocks, a projection that includes a wide
   column falls back to blocks of 4, and a narrow column in a wide projection
   costs what it costs today. Parquet places chunks by offset, so any reader
   reads the file. Writer side: `StreamingParquetWriter` stages a block of 4
   and places it (`write_block`); narrow columns would instead be held back
   and placed as runs — either every N blocks or in one run before the bloom
   tail — and the memory is bounded by the threshold × narrow column count
   (hits: 55 columns × ~1 MB). Expected gain, from the b64-g0 column: the
   narrow-column queries' 35% (Q01/Q07) and pypi's 7% (every query there is
   narrow), with the wide-column queries unchanged rather than −7-13%. Build
   the writer option, write `hits_4g_pc` and `pypi_pc` arms, re-run this
   harness; the decision rule (chunk bytes vs whole-column bytes, and the
   threshold) is what the measurement should settle.
4. **Required with (3) — the fetch-ahead gate on long-run files.** [D-1]
   counts fetch blocks, so a scan of a few long runs looks tiny to it and runs
   coupled: that is the b64 column above (Q01 0.53 s coupled vs 0.31 s armed).
   Count row groups or bytes instead of blocks, or arm fetch-ahead whenever any
   block exceeds some size. Not needed only if (3) is declined.
5. **Not a layout lever at all — file count.** The slow production queries that
   motivated this (pypi_large, 500-700 s) were 17,998 files of ~1.4 MB, since
   fixed; at that file count every layout is request-bound on footers and
   first blocks. Keep compaction ahead of the ingest.

What hadro got right and wrong against the VM: GET counts exact; direction of
every arm the same; magnitudes exaggerated where arms differ in request size
(ratio 2.0: +10.5% on hadro, +1.6% on GCS), because hadro's per-stream rate
(92 MB/s) is ~2.4× below GCS's. Use hadro for request-count and direction, the
VM for the number.

### 4.6 Per-column block size, built and measured 2026-11-06 [M]

§4.5 item 3 built on branch `per-column-block-size` (no default changed; the
option is off unless asked for):

- **Writer.** `write_parquet` / `open_parquet_writer` / `write_parquet_stream`
  / `write_parquet_with_bounds` take `narrow_chunk_bytes` (0 = off) and
  `narrow_run_blocks` (0 = one run per file). `NarrowColumnRuns` in
  `rugo/src/parquet/_parquet_writer.hpp`: when the first block of a file is
  encoded, a column whose MEAN encoded chunk over that block is at or below the
  threshold is NARROW for the whole file; wide columns are placed exactly as
  `write_block` places them (column-major, blocks of 4, schema order), narrow
  columns' chunks are held and placed as one byte-adjacent run per column, in
  row-group order, narrow columns in schema order, either after every N blocks
  or once before the bloom tail. Writer memory is the held chunks — at the
  threshold, narrow columns × row groups per run × threshold; the streaming
  writer reports the exact figure as `held_narrow_bytes`. Pinned in
  `tests/rugo/test_per_column_layout.py` (byte order, first-block rule, the
  reader's adjacency rule reproduced, values identical to the default layout in
  pyarrow and rugo, streaming == one-shot, threshold 0 byte-identical to today)
  and `tests/unit/connectors/parquet_io/test_fetch_blocks_per_column.py` (GET
  counts through the compiler's native scan over HTTP: a narrow-only projection
  reads a run as one fetch op, one GET; a projection with a wide column falls
  back to blocks of four; pruned shapes cost what the grouped file costs).
- **Gate.** `PARQUET_IO_FETCH_AHEAD_MIN_BLOCKS` keeps its name and default
  (48) and now counts fetch blocks in 256k-ROW UNITS — a block of up to 262,144
  kept rows is one, a longer one ceil(rows / 262,144) (`_fetch_block_units`,
  `pool_reader.pyx`; documented in `opteryx/config.py`; [D-1] amended in the
  grouped-layout doc). A block of four 64k row groups is still one unit and a
  row-major 64k file still one per row group, so nothing moves on today's
  files; a whole-file run of 816 row groups counts 204, a 64-row-group block
  16, so §4.4's b64 and this layout's runs arm fetch-ahead as the work they
  are. Pinned in `test_fetch_blocks_per_column.py`.
- **Arms** (`dev/rewrite_parquet_layout.py --narrow-bytes T
  [--narrow-run-blocks N]`; the repack now counts held narrow bytes toward the
  4 GiB close so every arm shares b4's file boundaries, 816 + 710 row groups):
  `pc32` = 32 KiB, one run per file; `pc64` = 64 KiB, one run per file;
  `pc32r16` = 32 KiB, a run every 16 blocks (64 row groups); pypi likewise
  `pc64`, `pc32`, `pc64r16`. Repack memory, `held_narrow_bytes` peak per file:
  hits pc32 774 / 674 MiB, pc64 1,325 / 767 MiB, pc32r16 148 / 96 MiB; pypi
  pc64 ≈ 54 MiB (every column narrow).
- **Where.** hadro as §4.1 with the branch build (new gate), 3 rounds, arms
  rotated; hadro's hits rounds 1-2 overlapped the GCS uploads of the hits
  arms (network and one core), round 0 did not — minimums are reported. VM:
  `worker-n4a-test`, production image `opteryx-worker:fleet` digest
  `d92eea6c…` (opteryx-core 0.9.191, so the OLD gate), `GCS_TRANSPORT=grpc`,
  copies under `gs://opteryx_data/_bench/hadro_sweep/` (deleted after). Since
  the image has the old gate, each per-column arm on the VM runs twice: as is
  (old gate: a few long runs look small and run coupled) and with
  `PARQUET_IO_FETCH_AHEAD_MIN_BLOCKS=0` (`-g0`), which is what the new gate
  does on every battery query but the pruned handful. Raw JSONL:
  `scratch/hadro_io_sweep/results/{pypi_pc,hits_pc}.jsonl`,
  `scratch/hadro_io_sweep/vm_vm_{pypi,hits}_pc.jsonl`.

#### 4.6.1 Narrow table (pypi), 10 queries × 3 rounds

hadro (branch build, new gate; `-coupled` = gate forced off, i.e. the old gate's
coupled path on these 1-4-block scans):

| Arm | Σ min wall | vs b4 | GETs | vs b4 | fetch ops | in-flight (median) |
|---|---|---|---|---|---|---|
| b4 (today) | 3.56 s | — | 1,347 | — | 1,147 | 16.9 |
| pc64 (all 19 columns narrow) | 3.35 s | −5.7% | 92 | −93.2% | 31 | 1.9 |
| pc32 (15 narrow) | 3.27 s | −7.9% | 417 | −69.0% | 125 | 2.9 |
| pc64r16 | 3.25 s | −8.7% | 175 | −87.0% | 82 | 3.1 |
| pc64-coupled | 3.31 s | −6.8% | 92 | −93.2% | 31 | 1.8 |

VM (real GCS, production image):

| Arm | Σ min wall | vs b4 | GETs | vs b4 | fetch ops | in-flight (median) |
|---|---|---|---|---|---|---|
| b4 (today) | 2.94 s | — | 1,347 | — | 1,147 | 11.2 |
| pc64 (old gate) | 2.75 s | −6.4% | 92 | −93.2% | 31 | 1.3 |
| **pc64-g0** | **2.57 s** | **−12.5%** | 92 | −93.2% | 31 | 1.2 |
| pc32 (old gate) | 2.81 s | −4.4% | 417 | −69.0% | 125 | 2.7 |
| pc32-g0 | 2.66 s | −9.5% | 417 | −69.0% | 125 | 2.6 |
| pc64r16-g0 | 2.78 s | −5.3% | 175 | −87.0% | 82 | 2.6 |

Per query, VM (min wall / GETs / MiB):

| Query | b4 | pc64 | pc64-g0 | pc32-g0 | pc64r16-g0 |
|---|---|---|---|---|---|
| P1 `SELECT project GROUP BY ALL` | 0.34 s / 189 / 8 | 0.24 s / 4 / 9 | **0.22 s** / 4 / 9 | 0.24 s / 4 / 9 | 0.30 s / 12 / 9 |
| P2 `COUNT(*), project` | 0.32 s / 189 / 8 | 0.18 s / 4 / 9 | 0.20 s / 4 / 9 | 0.19 s / 4 / 9 | 0.22 s / 12 / 9 |
| P3 pruned, 2 columns | 0.07 s / 4 / 0 | 0.06 s / 4 / 0 | 0.07 s / 4 / 0 | 0.07 s / 4 / 0 | 0.08 s / 6 / 0 |
| P4 `SELECT * WHERE project = …` | **0.07 s** / 4 / 0 | 0.10 s / 29 / 0 | 0.10 s / 29 / 0 | 0.08 s / 27 / 0 | 0.11 s / 35 / 0 |
| P5 `country_code, COUNT(*)` | 0.31 s / 189 / 18 | 0.25 s / 4 / 18 | 0.21 s / 4 / 18 | 0.25 s / 4 / 18 | 0.24 s / 12 / 18 |
| P6 two 35 MiB columns | 0.38 s / 189 / 34 | 0.47 s / 8 / 35 | 0.39 s / 8 / 35 | 0.42 s / 102 / 35 | 0.42 s / 12 / 35 |
| P7 `COUNT(*) WHERE project = …` | 0.06 s / 4 / 0 | 0.05 s / 2 / 0 | 0.06 s / 2 / 0 | 0.06 s / 2 / 0 | 0.07 s / 3 / 0 |
| P8 `SELECT * LIMIT 50` | **0.06 s** / 1 / 0 | 0.07 s / 11 / 0 | 0.07 s / 11 / 0 | 0.07 s / 9 / 0 | 0.07 s / 11 / 0 |
| P9 `project, http_status_code` | 0.35 s / 361 / 8 | 0.31 s / 8 / 9 | 0.30 s / 8 / 9 | 0.32 s / 8 / 9 | 0.36 s / 24 / 9 |
| P10 six columns, 54 MiB | 0.97 s / 217 / 53 | 1.03 s / 18 / 54 | 0.94 s / 18 / 54 | 0.98 s / 253 / 54 | 0.92 s / 48 / 54 |

Reading. The round-trip-bound queries (P1, P2, P5, P9: one or two columns,
8-18 MiB over 189-361 GETs) gain 30-40% on both rigs and both gates: 4 files
→ 4 GETs, nothing left to overlap, so the gate hardly matters for them (the
−12.5% / −6.4% split between pc64-g0 and pc64 is P6 and P10, the 35-54 MiB
projections, which the coupled path serves with ≤ 4 requests in flight). The
byte-bound P6 and P10 are flat. The two shapes that LOSE are the ones that
read one or a few row groups of every column — P4 (`SELECT *` pruned to a
handful of row groups) and P8 (`LIMIT 50`): a kept row group's 19 chunks are
now in 19 runs megabytes apart, 4 → 29 GETs and 1 → 11, which costs 30 ms on
GCS (concurrent ranges) and 180 ms on hadro (the §4.5 caveat about hadro
exaggerating request costs, again). pc32 vs pc64: at 32 KiB four columns
(details_python 27.5 KiB in the first block but 36 KiB over the file,
file_version, http_bytes_served, file_filename) are classified per FILE, so
P6/P10 read some files as runs and some as blocks of four (102 and 253 GETs);
64 KiB makes every pypi column narrow and the file whole-file column-major.
pc64r16 (a run every 64 row groups) gives back a third of the gain on P1/P2/P9
(12-24 GETs) for a memory bound of 64 × 19 × 64 KiB — see §4.6.3 for why.

#### 4.6.2 Wide table (hits, 105 columns), 42 queries × 3 rounds

hadro (branch build, new gate):

| Arm | Σ min wall | vs b4 | GETs | vs b4 | fetch ops | in-flight (median) |
|---|---|---|---|---|---|---|
| b4 (today) | 36.90 s | — | 16,965 | — | 9,847 | 21.2 |
| pc32 | 38.14 s | +3.4% | 19,809 | +16.8% | 9,179 | 29.5 |
| pc64 | 42.22 s | +14.4% | 15,018 | −11.5% | 6,243 | 24.5 |
| pc32r16 | 38.50 s | +4.3% | 19,199 | +13.2% | 9,224 | 27.2 |
| pc32-coupled (gate forced off) | 95.61 s | +159% | 19,709 | +16.2% | 9,101 | 11.6 |

Per query, hadro (min wall / GETs / MiB):

| Query | b4 | pc32 | pc64 | pc32r16 |
|---|---|---|---|---|
| Q01 `COUNT(*) WHERE AdvEngineID <> 0` | 0.67 s / 339 / 1 | **0.14 s** / 2 / 1 | 0.14 s / 2 / 1 | 0.54 s / 25 / 1 |
| Q07 `AdvEngineID … GROUP BY` | 0.67 s / 339 / 1 | **0.14 s** / 2 / 1 | 0.13 s / 2 / 1 | 0.52 s / 25 / 1 |
| Q04 `COUNT(DISTINCT UserID)` (234 MiB) | 1.05 s / 382 / 234 | 1.06 s / 382 / 234 | **1.26 s** / 194 / 234 | 1.06 s / 382 / 234 |
| Q18 `UserID, EventTime, SearchPhrase` | 1.61 s / 946 / 667 | 1.60 s / 914 / 667 | 1.60 s / 1,024 / 667 | 1.64 s / 914 / 667 |
| Q20 `URL LIKE` (1,022 MiB) | 1.72 s / 399 / 1022 | 1.73 s / 399 / 1022 | 1.73 s / 399 / 1022 | 1.73 s / 399 / 1022 |
| Q22 `URL, Title …` (1,977 MiB) | 2.85 s / 1,185 / 1977 | 2.83 s / 1,156 / 1977 | 2.88 s / 1,365 / 1977 | 2.84 s / 1,156 / 1977 |
| Q23 `SELECT * WHERE URL LIKE … ORDER BY EventTime LIMIT 10` | **0.62 s** / 240 / 207 | 1.42 s / 665 / 206 | 1.52 s / 670 / 202 | 0.96 s / 399 / 201 |
| Q31 wide `GROUP BY` (IsRefresh, ResolutionWidth narrow) | 2.02 s / 1,380 / 1138 | 2.13 s / 1,901 / 1138 | 2.12 s / 1,908 / 1138 | 2.11 s / 1,899 / 1138 |
| Q36 `CounterID = 62 …` pruned | 0.14 s / 26 / 14 | 0.14 s / 26 / 14 | 0.14 s / 26 / 14 | 0.14 s / 26 / 14 |

VM (real GCS, production image = old gate; `-g0` = fetch-ahead armed, what the
new gate does here):

| Arm | Σ min wall | vs b4 | GETs | vs b4 | fetch ops | in-flight (median) |
|---|---|---|---|---|---|---|
| b4 (today) | 40.39 s | — | 17,070 | — | 9,955 | 17.1 |
| pc32 (old gate) | 39.74 s | −1.6% | 19,944 | +16.8% | 9,318 | 19.4 |
| pc32-g0 | 40.46 s | +0.2% | 19,812 | +16.1% | 9,183 | 19.8 |
| pc32r16-g0 | 39.22 s | −2.9% | 19,422 | +13.8% | 9,441 | 18.4 |
| pc64-g0 | 39.46 s | −2.3% | 15,118 | −11.4% | 6,336 | 17.6 |

Per query, VM (min wall / GETs / MiB):

| Query | b4 | pc32 | pc32-g0 | pc32r16-g0 | pc64-g0 |
|---|---|---|---|---|---|
| Q01 | 0.48 s / 339 / 1 | **0.14 s** / 2 / 1 | 0.15 s / 2 / 1 | 0.34 s / 25 / 1 | 0.13 s / 2 / 1 |
| Q07 | 0.44 s / 339 / 1 | **0.09 s** / 2 / 1 | 0.11 s / 2 / 1 | 0.34 s / 25 / 1 | 0.12 s / 2 / 1 |
| Q04 | 0.84 s / 382 / 234 | 0.85 s / 382 / 234 | 0.87 s / 382 / 234 | 0.82 s / 382 / 234 | 0.84 s / 194 / 234 |
| Q18 | 3.14 s / 946 / 667 | 3.01 s / 914 / 667 | 3.07 s / 914 / 667 | 3.03 s / 914 / 667 | 3.06 s / 1,024 / 667 |
| Q20 | 1.84 s / 399 / 1022 | 1.78 s / 399 / 1022 | 1.79 s / 399 / 1022 | 1.80 s / 399 / 1022 | 1.86 s / 399 / 1022 |
| Q22 | 3.18 s / 1,185 / 1977 | 3.08 s / 1,156 / 1977 | 3.07 s / 1,156 / 1977 | 3.05 s / 1,156 / 1977 | 3.08 s / 1,365 / 1977 |
| Q23 | **0.59 s** / 218 / 196 | 0.83 s / 663 / 204 | 0.87 s / 661 / 203 | 0.67 s / 405 / 204 | 0.90 s / 669 / 201 |
| Q31 | 1.45 s / 1,380 / 1138 | 1.40 s / 1,901 / 1138 | 1.51 s / 1,901 / 1138 | 1.37 s / 1,899 / 1138 | 1.51 s / 1,908 / 1138 |
| Q36 | 0.13 s / 26 / 14 | 0.12 s / 26 / 14 | 0.13 s / 26 / 14 | 0.13 s / 26 / 14 | 0.12 s / 26 / 14 |

The battery total is within ±3% of b4 on every arm (round-to-round noise on
this battery is ±2%): the per-column layout neither wins nor loses the hits
battery as a whole, it moves the per-query costs around as items 1-4 below
describe. Two hadro findings the VM corrects: Q04 at 64 KiB is NOT slower on
GCS (0.84 s on both; the 122 MiB UserID block is fetched as concurrent 8 MiB
ranges and lands in time), and Q23's penalty is +40% (0.59 → 0.83 s), not the
+130% hadro showed — the same request-count exaggeration §4.5 noted. What the
VM confirms exactly: the GET counts, Q01/Q07 at 2 GETs and −70-80%, the wide
queries flat, and the +17% battery-wide GET count at 32 KiB.

Reading (hadro, confirmed or corrected by the VM table above).

1. **The narrow-column gain arrives, and the wide-column loss does not.** Q01
   and Q07 go from 339 GETs to 2 — one per file — and from 0.67 s to 0.14 s on
   hadro (§4.4's b64-g0 got them to 0.31 s on the VM with 24 GETs). The
   byte-bound queries over wide columns (Q04, Q18, Q20, Q22) are unchanged at
   32 KiB: their columns are wide, so their blocks are the blocks of four they
   had, and the bytes, GETs and wall are b4's within noise. This is the result
   §4.5 item 3 predicted.
2. **The threshold must keep the 100-150 KiB-a-chunk INT columns wide.** At
   64 KiB UserID (53 KiB in the first block, 154 KiB over the file) and
   ClientIP/RemoteIP/FUniqID are classified narrow; Q04 (`COUNT(DISTINCT
   UserID)`) then reads each file's UserID as ONE 122 MiB fetch block — fetched
   whole before any row group decodes — and goes 1.05 → 1.26 s (+20%) on
   hadro, where a stream is capped at 92 MB/s; on GCS the block's concurrent
   8 MiB ranges land in time and Q04 is flat (0.84 s). Q18/Q22 pick up 10-15%
   more GETs either way. 32 KiB keeps them wide. The 32 KiB
   boundary on hits falls in a natural gap (§4.6.4: nothing between 42 KiB and
   100 KiB except SearchPhrase at 55 KiB, which the type rule keeps wide
   anyway).
3. **Mixed projections pay more requests, not more bytes.** A narrow column in
   a projection with a wide one is fetched from its run, a separate range per
   block, where in b4 it sat inside the block next to the other projected
   columns and the 512 KiB gap rule merged it. Q31 (WatchID, ClientIP,
   SearchPhrase wide; IsRefresh, ResolutionWidth narrow): 1,380 → 1,901 GETs
   (+38%), +5% wall on hadro. Over the battery this is why hits' GET total RISES
   17% at pc32 while the two narrow-only queries fall by 674. The §4.5
   assumption that "a narrow column in a wide projection costs what it costs
   today" was wrong: it costs one more range per block, and the ranges of
   different narrow columns do not merge with each other because each run is
   megabytes long (816 row groups × its chunk).
4. **The shape that loses is `SELECT *` over few row groups.** Q23 (two-pass
   late materialization: pass 2 re-reads the surviving row groups with masks,
   every column) goes 0.62 → 1.42 s and 240 → 665 GETs: a kept row group's 88
   narrow chunks are in 88 runs, and the shorter the run the closer together
   they sit (pc32r16: 399 GETs, 0.96 s). On the VM: 0.59 → 0.83 s (+40%),
   0.67 s at pc32r16. pypi's P4 and P8 are the same shape (+30 ms on the VM).
   This is the per-column layout's cost, and it falls on point lookups,
   `LIMIT` and latmat pass 2.
5. **A run every 16 blocks bounds memory but serialises the fetch.** pc32r16
   holds 148 MiB per file instead of 774, yet Q01 is 0.54 s with 25 GETs — the
   13 runs per file are 64 row groups each, and the submission window
   (`max(workers, fetch_ahead) + 2` = 66 ROW GROUPS) admits one such block at a
   time, so fetch-ahead has one request in flight per file whatever its depth
   (VM: 0.34 s against 0.14 s for the whole-file run and 0.48 s for b4). The
   window would have to count blocks, or bytes, for runs of that length to
   pipeline; §4.4's b64 arm had the same window against it. Whole-file runs
   dodge this only because there is one block per file and the files are read
   in parallel.
6. **The first-block rule misclassifies columns that grow.** hits is sorted
   by CounterID; its first block has near-empty OriginalURL (27 KiB a chunk,
   379 KiB over the file) and WindowName (0.2 KiB, 41 KiB), so both are narrow
   at 32 KiB and their runs are 364 MiB and 41 MiB. Nothing in the battery
   projects OriginalURL alone, so it costs nothing measured, but it is why the
   held memory is 774 MiB rather than the 194 MiB the whole-file chunk means
   predict, and it would make a query over OriginalURL fetch a 364 MiB block
   before decoding a row.
7. **The old gate, on these files, is the coupled path for every narrow-only
   query** (whole-file runs are 2 fetch blocks): `pc32-coupled` is +159% over
   the battery on hadro because the gate was forced off for EVERY query, the
   wide ones included, so it overstates the old gate's cost; on the VM the
   production image's old gate (`pc32`) and the armed arm (`pc32-g0`) are
   within noise of each other on hits, because the narrow-only queries have
   nothing to overlap at 2 blocks and the wide ones are armed under both.
   §4.6.1's pypi arms isolate the gate at 6 points of 12.5 (P6/P10). The new
   gate arms these scans; nothing else about the reader moved.

#### 4.6.3 What the data supports

- **Decision rule: chunk bytes, decided per column, with the threshold at 32
  KiB (first-block mean), not 64 KiB.** 64 KiB flips the incompressible INT
  columns (UserID, ClientIP, RemoteIP, FUniqID) to narrow and costs Q04 20%;
  32 KiB leaves every byte-bound hits query at b4 and keeps all but the four
  widest pypi columns narrow. The rule should use the first block's MAXIMUM
  chunk rather than its mean, or demote a column to wide when a later chunk
  exceeds the threshold several-fold (placing its held run so far and
  switching — legal under the adjacency rule, and it bounds memory to the
  threshold × the narrow count × the run length as stated): item 6 is the
  cost of not doing so on sorted data.
- **One run per file is the layout that wins; its memory is the cost to
  rule on.** Whole-file runs give the narrow-only queries one GET per file and
  pipeline across files; runs of 64 row groups give back a third of the gain
  (pypi) to most of it (hits Q01) against today's row-group-counted window,
  for memory of ~150 MiB instead of ~750 MiB per hits file at 32 KiB (pypi:
  54 MiB either way). If the sink's memory cannot carry ~0.8 GB per open
  file, the fix on the reader side is to size the submission window in blocks
  or bytes (item 5), after which short runs would pipeline and the
  run-per-N-blocks variant would be the bounded choice.
- **Net on the fleet, at 32 KiB whole-file: pypi −9.5% (−12.5% at 64 KiB),
  hits −1.6% (within the ±2% noise; Q01/Q07 −70-80%, Q23 +40%).** The gain is concentrated on the shapes production runs
  most (pypi's `GROUP BY` over one or two columns, −30-40%); the cost is
  concentrated on `SELECT *` over few row groups (+30 ms on pypi, Q23 on hits)
  and on mixed projections' request counts. Whether that trade is wanted is
  the ruling; the data says it is a real, repeatable trade, not a free gain.
- **Not supported: a type-based rule** (variable-width stays at 4, fixed-width
  goes to a run) — see §4.6.4: it puts WatchID, URLHash, RefererHash, HID, the
  three EventTime columns and UserID (137-512 KiB a chunk, the columns Q04/Q18/
  Q22 are bound by) into runs, and keeps 20 dictionary-encoded hits strings
  under 1 KiB a chunk, and every pypi string column including `project` and
  `country_code` (P1/P2/P5's whole gain), in blocks of four.

#### 4.6.4 Column classes from the footers: bytes rule vs type rule

Per column of the b4 files: physical type, dictionary or plain, the first
block's mean compressed chunk (the writer's classifier input), the whole-file
mean, the class under the bytes rule at 32 and 64 KiB, the class under a
type rule (BYTE_ARRAY stays in blocks of 4, fixed-width goes to a run), and
where they disagree. Fixed-width columns that are WIDE in bytes and
variable-width columns that are NARROW are the two disagreements; the type
rule disagrees with the 32 KiB bytes rule on 37 of hits' 105 columns and 14
of pypi's 19.

**hits_4g_b4/part-000.parquet** (816 row groups; first block = row groups 0-3; bytes are zstd-compressed, per row group)

| Column | Physical type | Encoding | First-block chunk | File mean chunk | Bytes rule 32 KiB | Bytes rule 64 KiB | Type rule | Disagree |
|---|---|---|---|---|---|---|---|---|
| GoodEvent | INT32 | dict | 0.0 KiB | 0.0 KiB | narrow | narrow | narrow (run) |  |
| EventDate | INT32 | dict | 0.0 KiB | 0.1 KiB | narrow | narrow | narrow (run) |  |
| CounterClass | INT32 | dict | 0.0 KiB | 0.1 KiB | narrow | narrow | narrow (run) |  |
| Params | BYTE_ARRAY | dict | 0.0 KiB | 0.7 KiB | narrow | narrow | wide (4) | **32/64** |
| IsLink | INT32 | dict | 0.0 KiB | 1.1 KiB | narrow | narrow | narrow (run) |  |
| IsDownload | INT32 | dict | 0.0 KiB | 0.4 KiB | narrow | narrow | narrow (run) |  |
| IsNotBounce | INT32 | dict | 0.0 KiB | 2.6 KiB | narrow | narrow | narrow (run) |  |
| IsOldCounter | INT32 | dict | 0.0 KiB | 0.0 KiB | narrow | narrow | narrow (run) |  |
| IsEvent | INT32 | dict | 0.0 KiB | 0.0 KiB | narrow | narrow | narrow (run) |  |
| IsParameter | INT32 | dict | 0.0 KiB | 0.3 KiB | narrow | narrow | narrow (run) |  |
| WithHash | INT32 | dict | 0.0 KiB | 0.2 KiB | narrow | narrow | narrow (run) |  |
| OpenerName | INT32 | dict | 0.0 KiB | 0.0 KiB | narrow | narrow | narrow (run) |  |
| SocialNetwork | BYTE_ARRAY | dict | 0.0 KiB | 0.0 KiB | narrow | narrow | wide (4) | **32/64** |
| SocialAction | BYTE_ARRAY | dict | 0.0 KiB | 0.0 KiB | narrow | narrow | wide (4) | **32/64** |
| HTTPError | INT32 | dict | 0.0 KiB | 0.0 KiB | narrow | narrow | narrow (run) |  |
| SendTiming | INT32 | dict | 0.0 KiB | 12.2 KiB | narrow | narrow | narrow (run) |  |
| ParamOrderID | BYTE_ARRAY | dict | 0.0 KiB | 0.1 KiB | narrow | narrow | wide (4) | **32/64** |
| ParamCurrencyID | INT32 | dict | 0.0 KiB | 0.0 KiB | narrow | narrow | narrow (run) |  |
| FromTag | BYTE_ARRAY | dict | 0.0 KiB | 0.5 KiB | narrow | narrow | wide (4) | **32/64** |
| CLID | INT32 | dict | 0.0 KiB | 0.2 KiB | narrow | narrow | narrow (run) |  |
| CounterID | INT32 | dict | 0.1 KiB | 0.1 KiB | narrow | narrow | narrow (run) |  |
| ParamCurrency | BYTE_ARRAY | dict | 0.1 KiB | 0.1 KiB | narrow | narrow | wide (4) | **32/64** |
| ParamPrice | INT64 | dict | 0.1 KiB | 0.1 KiB | narrow | narrow | narrow (run) |  |
| SilverlightVersion4 | INT32 | dict | 0.1 KiB | 0.1 KiB | narrow | narrow | narrow (run) |  |
| SocialSourcePage | BYTE_ARRAY | dict | 0.1 KiB | 1.2 KiB | narrow | narrow | wide (4) | **32/64** |
| SocialSourceNetworkID | INT32 | dict | 0.1 KiB | 0.5 KiB | narrow | narrow | narrow (run) |  |
| CookieEnable | INT32 | dict | 0.1 KiB | 0.2 KiB | narrow | narrow | narrow (run) |  |
| HistoryLength | INT32 | dict | 0.2 KiB | 10.7 KiB | narrow | narrow | narrow (run) |  |
| JavascriptEnable | INT32 | dict | 0.2 KiB | 0.2 KiB | narrow | narrow | narrow (run) |  |
| CodeVersion | INT32 | dict | 0.2 KiB | 0.7 KiB | narrow | narrow | narrow (run) |  |
| PageCharset | BYTE_ARRAY | dict | 0.2 KiB | 1.2 KiB | narrow | narrow | wide (4) | **32/64** |
| HitColor | BYTE_ARRAY | dict | 0.2 KiB | 3.6 KiB | narrow | narrow | wide (4) | **32/64** |
| WindowName | INT32 | dict | 0.2 KiB | 41.5 KiB | narrow | narrow | narrow (run) |  |
| UTMContent | BYTE_ARRAY | dict | 0.4 KiB | 0.3 KiB | narrow | narrow | wide (4) | **32/64** |
| UTMTerm | BYTE_ARRAY | dict | 0.4 KiB | 0.3 KiB | narrow | narrow | wide (4) | **32/64** |
| OpenstatServiceName | BYTE_ARRAY | dict | 0.4 KiB | 0.5 KiB | narrow | narrow | wide (4) | **32/64** |
| MobilePhoneModel | BYTE_ARRAY | dict | 0.4 KiB | 2.0 KiB | narrow | narrow | wide (4) | **32/64** |
| OpenstatCampaignID | BYTE_ARRAY | dict | 0.5 KiB | 0.8 KiB | narrow | narrow | wide (4) | **32/64** |
| OpenstatSourceID | BYTE_ARRAY | dict | 0.5 KiB | 0.7 KiB | narrow | narrow | wide (4) | **32/64** |
| DontCountHits | INT32 | dict | 0.5 KiB | 3.2 KiB | narrow | narrow | narrow (run) |  |
| MobilePhone | INT32 | dict | 0.6 KiB | 2.5 KiB | narrow | narrow | narrow (run) |  |
| BrowserLanguage | BYTE_ARRAY | dict | 0.7 KiB | 4.8 KiB | narrow | narrow | wide (4) | **32/64** |
| UTMMedium | BYTE_ARRAY | dict | 0.7 KiB | 1.0 KiB | narrow | narrow | wide (4) | **32/64** |
| IsMobile | INT32 | dict | 0.7 KiB | 2.5 KiB | narrow | narrow | narrow (run) |  |
| OpenstatAdID | BYTE_ARRAY | dict | 0.8 KiB | 1.6 KiB | narrow | narrow | wide (4) | **32/64** |
| UTMSource | BYTE_ARRAY | dict | 0.8 KiB | 1.0 KiB | narrow | narrow | wide (4) | **32/64** |
| URLCategoryID | INT32 | dict | 0.8 KiB | 3.1 KiB | narrow | narrow | narrow (run) |  |
| URLRegionID | INT32 | dict | 0.9 KiB | 3.8 KiB | narrow | narrow | narrow (run) |  |
| UTMCampaign | BYTE_ARRAY | dict | 1.0 KiB | 1.3 KiB | narrow | narrow | wide (4) | **32/64** |
| NetMinor | INT32 | dict | 1.1 KiB | 1.4 KiB | narrow | narrow | narrow (run) |  |
| NetMajor | INT32 | dict | 1.2 KiB | 1.5 KiB | narrow | narrow | narrow (run) |  |
| HasGCLID | INT32 | dict | 1.3 KiB | 0.3 KiB | narrow | narrow | narrow (run) |  |
| AdvEngineID | INT32 | dict | 1.9 KiB | 0.5 KiB | narrow | narrow | narrow (run) |  |
| FlashMajor | INT32 | dict | 2.1 KiB | 4.9 KiB | narrow | narrow | narrow (run) |  |
| JavaEnable | INT32 | dict | 2.2 KiB | 4.9 KiB | narrow | narrow | narrow (run) |  |
| ResolutionDepth | INT32 | dict | 2.3 KiB | 5.9 KiB | narrow | narrow | narrow (run) |  |
| SilverlightVersion2 | INT32 | dict | 2.4 KiB | 4.8 KiB | narrow | narrow | narrow (run) |  |
| UserAgentMinor | BYTE_ARRAY | dict | 2.5 KiB | 14.7 KiB | narrow | narrow | wide (4) | **32/64** |
| BrowserCountry | BYTE_ARRAY | dict | 2.6 KiB | 10.8 KiB | narrow | narrow | wide (4) | **32/64** |
| ClientTimeZone | INT32 | dict | 2.6 KiB | 10.2 KiB | narrow | narrow | narrow (run) |  |
| SilverlightVersion1 | INT32 | dict | 3.1 KiB | 6.9 KiB | narrow | narrow | narrow (run) |  |
| Sex | INT32 | dict | 3.6 KiB | 7.3 KiB | narrow | narrow | narrow (run) |  |
| Income | INT32 | dict | 3.8 KiB | 7.5 KiB | narrow | narrow | narrow (run) |  |
| OS | INT32 | dict | 3.9 KiB | 10.6 KiB | narrow | narrow | narrow (run) |  |
| SilverlightVersion3 | INT32 | dict | 4.0 KiB | 9.4 KiB | narrow | narrow | narrow (run) |  |
| FlashMinor | INT32 | dict | 4.4 KiB | 8.9 KiB | narrow | narrow | narrow (run) |  |
| UserAgent | INT32 | dict | 4.8 KiB | 13.0 KiB | narrow | narrow | narrow (run) |  |
| DNSTiming | INT32 | dict | 4.9 KiB | 4.6 KiB | narrow | narrow | narrow (run) |  |
| Age | INT32 | dict | 4.9 KiB | 10.6 KiB | narrow | narrow | narrow (run) |  |
| FlashMinor2 | BYTE_ARRAY | dict | 5.4 KiB | 11.8 KiB | narrow | narrow | wide (4) | **32/64** |
| IsArtifical | INT32 | dict | 5.9 KiB | 3.1 KiB | narrow | narrow | narrow (run) |  |
| UserAgentMajor | INT32 | dict | 6.2 KiB | 15.5 KiB | narrow | narrow | narrow (run) |  |
| ResolutionWidth | INT32 | dict | 6.3 KiB | 18.4 KiB | narrow | narrow | narrow (run) |  |
| ResolutionHeight | INT32 | dict | 6.4 KiB | 19.6 KiB | narrow | narrow | narrow (run) |  |
| Robotness | INT32 | dict | 6.6 KiB | 12.5 KiB | narrow | narrow | narrow (run) |  |
| SearchEngineID | INT32 | dict | 6.7 KiB | 8.1 KiB | narrow | narrow | narrow (run) |  |
| IsRefresh | INT32 | dict | 6.7 KiB | 3.1 KiB | narrow | narrow | narrow (run) |  |
| RegionID | INT32 | dict | 8.5 KiB | 27.7 KiB | narrow | narrow | narrow (run) |  |
| TraficSourceID | INT32 | dict | 8.8 KiB | 13.4 KiB | narrow | narrow | narrow (run) |  |
| RefererRegionID | INT32 | dict | 10.1 KiB | 15.1 KiB | narrow | narrow | narrow (run) |  |
| Interests | INT32 | dict | 10.5 KiB | 24.0 KiB | narrow | narrow | narrow (run) |  |
| RefererCategoryID | INT32 | dict | 11.5 KiB | 17.5 KiB | narrow | narrow | narrow (run) |  |
| ConnectTiming | INT32 | dict | 13.4 KiB | 10.6 KiB | narrow | narrow | narrow (run) |  |
| WindowClientHeight | INT32 | dict | 16.7 KiB | 41.2 KiB | narrow | narrow | narrow (run) |  |
| WindowClientWidth | INT32 | dict | 20.3 KiB | 36.8 KiB | narrow | narrow | narrow (run) |  |
| IPNetworkID | INT32 | dict | 20.8 KiB | 49.5 KiB | narrow | narrow | narrow (run) |  |
| OriginalURL | BYTE_ARRAY | dict | 27.0 KiB | 378.6 KiB | narrow | narrow | wide (4) | **32/64** |
| FetchTiming | INT32 | dict | 28.8 KiB | 17.3 KiB | narrow | narrow | narrow (run) |  |
| ClientIP | INT32 | dict | 36.0 KiB | 101.5 KiB | wide | narrow | narrow (run) | **32** |
| RemoteIP | INT32 | dict | 36.1 KiB | 100.7 KiB | wide | narrow | narrow (run) | **32** |
| ResponseEndTiming | INT32 | dict | 41.5 KiB | 29.7 KiB | wide | narrow | narrow (run) | **32** |
| FUniqID | INT64 | dict | 47.6 KiB | 144.5 KiB | wide | narrow | narrow (run) | **32** |
| ResponseStartTiming | INT32 | dict | 50.1 KiB | 35.8 KiB | wide | narrow | narrow (run) | **32** |
| UserID | INT64 | dict | 53.3 KiB | 153.8 KiB | wide | narrow | narrow (run) | **32** |
| SearchPhrase | BYTE_ARRAY | dict | 55.2 KiB | 125.8 KiB | wide | narrow | wide (4) | **64** |
| ClientEventTime | INT64 | dict | 136.8 KiB | 163.2 KiB | wide | wide | narrow (run) | **32/64** |
| LocalEventTime | INT64 | dict | 140.0 KiB | 163.3 KiB | wide | wide | narrow (run) | **32/64** |
| EventTime | INT64 | dict | 140.3 KiB | 163.2 KiB | wide | wide | narrow (run) | **32/64** |
| RefererHash | INT64 | dict | 182.9 KiB | 229.4 KiB | wide | wide | narrow (run) | **32/64** |
| URLHash | INT64 | dict | 207.8 KiB | 260.4 KiB | wide | wide | narrow (run) | **32/64** |
| HID | INT32 | dict | 249.9 KiB | 236.4 KiB | wide | wide | narrow (run) | **32/64** |
| URL | BYTE_ARRAY | dict | 336.8 KiB | 615.6 KiB | wide | wide | wide (4) |  |
| Referer | BYTE_ARRAY | dict | 448.4 KiB | 682.3 KiB | wide | wide | wide (4) |  |
| WatchID | INT64 | plain | 512.0 KiB | 512.0 KiB | wide | wide | narrow (run) | **32/64** |
| Title | BYTE_ARRAY | dict | 585.1 KiB | 470.9 KiB | wide | wide | wide (4) |  |

Columns where the type rule and the bytes rule disagree: 37 at 32 KiB, 32 at 64 KiB (of 105).

**pypi_b4/pypi_18e541c56d3d58b2.parquet** (188 row groups; first block = row groups 0-3; bytes are zstd-compressed, per row group)

| Column | Physical type | Encoding | First-block chunk | File mean chunk | Bytes rule 32 KiB | Bytes rule 64 KiB | Type rule | Disagree |
|---|---|---|---|---|---|---|---|---|
| http_method | BYTE_ARRAY | dict | 0.1 KiB | 0.1 KiB | narrow | narrow | wide (4) | **32/64** |
| http_status_code | INT64 | dict | 0.2 KiB | 0.2 KiB | narrow | narrow | narrow (run) |  |
| http_range_header | BYTE_ARRAY | dict | 0.8 KiB | 0.5 KiB | narrow | narrow | wide (4) | **32/64** |
| tls_protocol | BYTE_ARRAY | dict | 1.2 KiB | 1.2 KiB | narrow | narrow | wide (4) | **32/64** |
| details_implementation_name | BYTE_ARRAY | dict | 1.2 KiB | 1.4 KiB | narrow | narrow | wide (4) | **32/64** |
| tls_cipher | BYTE_ARRAY | dict | 1.3 KiB | 1.2 KiB | narrow | narrow | wide (4) | **32/64** |
| file_type | BYTE_ARRAY | dict | 1.7 KiB | 1.3 KiB | narrow | narrow | wide (4) | **32/64** |
| timestamp | INT64 | dict | 4.6 KiB | 1.6 KiB | narrow | narrow | narrow (run) |  |
| details_cpu | BYTE_ARRAY | dict | 10.9 KiB | 9.6 KiB | narrow | narrow | wide (4) | **32/64** |
| details_ci | BOOLEAN | plain | 11.4 KiB | 10.7 KiB | narrow | narrow | narrow (run) |  |
| project | BYTE_ARRAY | dict | 12.4 KiB | 16.6 KiB | narrow | narrow | wide (4) | **32/64** |
| file_project | BYTE_ARRAY | dict | 12.4 KiB | 16.6 KiB | narrow | narrow | wide (4) | **32/64** |
| details_installer_name | BYTE_ARRAY | dict | 13.3 KiB | 12.6 KiB | narrow | narrow | wide (4) | **32/64** |
| details_distro_name | BYTE_ARRAY | dict | 23.6 KiB | 18.4 KiB | narrow | narrow | wide (4) | **32/64** |
| details_python | BYTE_ARRAY | dict | 27.5 KiB | 36.4 KiB | narrow | narrow | wide (4) | **32/64** |
| file_version | BYTE_ARRAY | dict | 27.7 KiB | 39.2 KiB | narrow | narrow | wide (4) | **32/64** |
| country_code | BYTE_ARRAY | dict | 28.6 KiB | 25.6 KiB | narrow | narrow | wide (4) | **32/64** |
| http_bytes_served | INT64 | dict | 31.0 KiB | 48.9 KiB | narrow | narrow | narrow (run) |  |
| file_filename | BYTE_ARRAY | dict | 32.1 KiB | 53.0 KiB | wide | narrow | wide (4) | **64** |

Columns where the type rule and the bytes rule disagree: 14 at 32 KiB, 15 at 64 KiB (of 19).

### 4.7 Hardening, window and sink wiring, 2026-11-06 [M]

Rulings applied (architect, via the review session): threshold 32 KiB;
whole-file runs under a memory budget rather than a fixed run length; schema
order kept within blocks and among runs (narrow-first ruled out); no default
layout change.

#### 4.7.1 Classification hardened (§4.6 items 5-6)

`NarrowColumnRuns` (`rugo/src/parquet/_parquet_writer.hpp`) now:

1. **Classifies on the first block's LARGEST chunk**, not its mean, so one
   near-empty row group cannot average a wide column under the threshold.
2. **Demotes on growth**: a narrow column whose chunk in a later block exceeds
   4 × the threshold is wide for the rest of the file; its run so far is placed
   at that block boundary, before the block. Legal under the adjacency rule —
   the reader sees the column as one run up to the demotion and as blocks of
   four after.
3. **Holds under a budget** (`narrow_held_budget_bytes`, 0 = none): before a
   block whose narrow chunks would take the held bytes past the budget, every
   run is placed and new ones start with that block — whole-file runs when they
   fit, shorter runs when they do not, boundaries always on block boundaries.
   The streaming writer reports `peak_held_narrow_bytes`,
   `narrow_demoted_columns` and `narrow_budget_closes`; with a budget the peak
   is at most the budget plus one block's narrow chunks.

Pinned in `tests/rugo/test_per_column_layout.py` (demotion byte order and the
blocks the reader infers on the result; the max rule against a column whose
mean is under and max over; a budget of 2.5 blocks writing the same bytes as a
fixed run of two; a budget under one block writing a run per block; threshold
0 byte-identical with or without a budget).

On hits at 32 KiB (`dev/rewrite_parquet_layout.py --narrow-bytes 32768
[--narrow-budget B]`, the same 816 + 710 row groups):

| Arm | Narrow at block 0 | Demoted (block) | Peak held, part-000 / part-001 | Budget closes |
|---|---|---|---|---|
| pc32 (§4.6, mean rule) | 88 | — | 774 / 674 MiB | — |
| pc32m (max rule + demotion) | 88 | OriginalURL (1; 2), WindowName (58; 10) | 439 / 301 MiB | 0 |
| pc32b256 (+ 256 MiB budget) | 88 | the same | 256 / 255 MiB | 1 / 1 |

The max rule alone moves no hits column at 32 KiB (OriginalURL's first block
is 26-28 KiB a chunk throughout; it grows to 285 KiB at row group 7); demotion
is what catches it, and WindowName (0.3 KiB a chunk in the first block, 42 KiB
mean over the file). The 364 MiB OriginalURL run of §4.6 item 6 is gone, and so
is 40% of the held memory; the 256 MiB budget costs one extra run boundary
per file.
