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

Owed: a write-time A/B (every hashable chunk is now hashed and counted even without a bloom; floats newly get a `distinct_count`). Not run — heavy runs wait for "machine's free". Existing mirrors carry none of this until regenerated (bump `.rugo-v1`).

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
- **P5 / P2 (heavy).** Page-size sweep on a sorted TPC-H SF1 mirror; 16-row-group blocks over the remote throttle server. Wait for "machine's free".
- **P8.** Mirror recipe needs a sort key per table, P5's page size and the P1 decision.
