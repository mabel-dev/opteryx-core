# Parquet page search: evaluate `LIKE '%x%'` on raw page bytes

**Status:** RULED 2026-10-06 — D1 (a) pruning only, D2 build §4 then ablate, D3 NOT LIKE in v1, D4 anchored = follow-on, D5 rename to ValuePredicate. Stages 0–2 done 2026-10-06; built; §4 KEPT; canary regression ACCEPTED 2026-10-06 (architect: +6% on high-match scans is a fair trade for ~3× on selective ones); every measurement switch deleted.
**Builds on:** [PARQUET_SELECTIVE_DECODE_DESIGN.md](PARQUET_SELECTIVE_DECODE_DESIGN.md), which shipped A (zero-copy dictionary), B (selective decode), C (in-worker prefilter) and E (zero-copy plain strings) on 2026-09-28.

## 0. Headline

A substring predicate does not need strings. It needs bytes.

A PLAIN `byte_array` page, once decompressed, is one contiguous buffer: `[len32][bytes][len32][bytes]…`.

- Run **one SIMD search over the whole buffer**, ignoring row boundaries.
- **No hit:** the page is finished. Its rows are dropped without:
  - walking the length prefixes;
  - decoding definition levels;
  - copying into an arena;
  - building slots;
  - running the kernel.
- **Hits:** walk the length prefixes only as far as the last hit, and map each hit to its value. Then copy only the values that matched, using the existing selective byte_array decode (B).

The column is never turned into strings to answer a yes/no question.

The same buffer layout is the PLAIN **dictionary page**. So one primitive also gives a per-entry hit vector. That vector drops rows from dictionary-encoded data pages by code, which is what DuckDB's `DictionaryDecoder::Filter` does, and it replaces the scalar `std::search` loop in today's dictionary-skip matcher.

## 1. What happens today

For `URL LIKE '%google%'` on a PLAIN page (`decode_column.cpp:2636`, `io_pipeline.hpp:673`):

1. Decompress the page.
2. Append the whole page to `string_arena`. This is copy 1.
3. Walk every length prefix, then `push_back` an offset and a length per row.
4. `build_direct_string_plain` copies the arena into the vector (copy 2) and builds one `DrakenStringSlot` per row.
5. The pass-1 program runs `draken_contains` row by row over the slots.
6. For `COUNT(*)`, the column is then thrown away.

For a dictionary-encoded chunk:

- `DictSkipPredicate` kind 4 (`decode_column.cpp:1074`) runs `std::search` (scalar) over each dictionary entry.
- It is **all-or-nothing per row group**: if any entry matches, nothing is dropped.
- The codes are then decoded for every row.
- The pass-1 kernel evaluates contains once per distinct entry, a second time, and scatters the result to every row.

The §2.3 views for PLAIN predicate columns were agreed but not built (§5b item 1). They were blocked on keeping every page alive in one arena. **This design does not need views**: it searches each page while its decompressed buffer is live in scratch, and copies only the matching values.

## 2. The primitive

```
// rugo/src/parquet/page_search.hpp (new)
// Searches a PLAIN byte_array value region for any of `needles`.
// Sets hit[v] = 1 for each value v (0..n_values) that contains a needle.
// Returns the number of values hit. When it returns 0, `hit` was not written.
uint32_t page_search_plain(const uint8_t* region, size_t region_len, uint32_t n_values,
                           const PageNeedles& needles, uint8_t* hit /* n_values */);
```

**Algorithm (one needle; several needles are ORed, see §6):**

1. `pos = find_first_last(region + from, ...)` uses the Muła first-and-last-byte SIMD loop from `simd_contains_cs`. It returns an **offset** rather than a bool, so `volnitsky.h` gains a find-position variant that shares the existing inner loop, rather than a second copy of it.
2. Nothing found: stop. The page has no hits.
3. Found: advance a lazy length-prefix cursor `(v, start, end)` until `end > pos`.
   - **Valid hit:** `pos >= start && pos + nlen <= end`. Set `hit[v] = 1`, then resume the search at the start of value `v+1`, which skips the rest of the row.
   - **Otherwise** the match crosses a row boundary or touches a length prefix. Resume at `pos + 1`; candidate matches can overlap.
4. The cursor never moves backwards. The walk costs as much as the distance to the last hit and no more. A page with zero hits does no walk at all.

**The correctness argument fits on one line:** a value contains the needle if and only if some occurrence lies entirely inside that value's bytes. Step 3 accepts exactly those occurrences, and the search does not skip past a candidate occurrence without checking it.

**Encodings:**

| page encoding | treatment |
|---|---|
| PLAIN `byte_array` | the primitive, as above |
| PLAIN dictionary page | the primitive, giving `hit[entry]` (§4) |
| DELTA_LENGTH_BYTE_ARRAY | search the concatenated byte block; map hits through the prefix sum of the lengths |
| DELTA_BYTE_ARRAY (front-coded) | **not searchable raw.** Decode as today. Counted as `page_search_fallthrough_pages`. |

## 3. Data pages: hits become a row mask

The search runs inside `DecodeColumnFromChunk`'s page loop, after decompression and before the byte_array decode.

- **Locating the values without decoding definition levels:**
  - v1 pages: the levels block carries its own 4-byte length, so the value region starts right after it.
  - v2 pages: the level lengths are in the page header.
- **Zero hits:** the page's rows are all zero in the mask. No definition-level decode, no walk, no copy. The cursor advances by `compressed_page_size`.
- **Hits:** decode the definition levels (needed to map value index to row; NULL never matches contains). Build the page's selection from `hit` ANDed with any existing `page_sel_mask`. Then hand it to the **existing** selective byte_array path as `page_vsel` (B), which copies only the selected values.
  - Partial hits go through the selective per-value loop.
  - A page where every value hits keeps the zero-copy whole-page path.
- **New output:** `DecodedColumn::search_row_mask`, one `uint8` per row-group row, in the same convention as `row_mask`. Also `search_rows_kept`.

Scope: `max_repetition_level == 0`, the same restriction as the page-jump plan. LIST columns are declined.

## 4. Dictionary pages: hits per entry become a code filter

The dictionary page is decompressed first, as today (`dc:665`). Then:

1. Run `page_search_plain` over it to get `hit[entry]`. This **replaces the `std::search` loop** of kind 4 in `DictSkipPredicate`, so there is still one matcher, now SIMD.
2. **Zero entries hit:** same as today's `dict_all_filtered`, with no data page decompressed.
3. **Some entries hit:** on each data page, decode the codes and set `row = hit[code]`.
   - An RLE run of a code with no hit is a run of zeros, with no unpacking.
   - A bit-packed group of 8 needs unpacking.
   - Rows with no hit are dropped by `search_row_mask`, exactly as in §3. The codes are then compacted to the survivors by B's selective path.
4. **Every entry hits:** no mask, and the decode is unchanged.

This is the per-row half of DuckDB's dictionary filter. We have the all-or-nothing half today.

**Expected value is smaller than §3.** After A, dictionary predicate columns already avoid per-row string copies, and the kernel already evaluates once per entry. What §4 removes:

- the second per-entry evaluation;
- the gather of codes for rows the downstream filter would drop;
- the scalar `std::search`.

Settled by ablation (§8), not argued.

## 5. Worker integration (`io_pipeline.hpp:3380`)

Today the prefilter does this: decode the predicate columns, run pass 1, then decode the other columns under the survivor mask.

With the search armed for column `S`:

```
decode S first, search armed          -> S compacted to search hits, search_row_mask M_s
if popcount(M_s) == 0: empty_filtered  (nothing else decoded or fetched further)
decode other predicate columns under M_s (selective, B)
pass1 over the compacted predicate columns -> survivor mask (over M_s rows)
compose: rows the other columns need = M_s ∘ survivor   (the existing rank loop that already
         composes the page-prune mask_ptr with the survivor mask; M_s joins mask_ptr)
decode the remaining columns under the composed mask
```

- The **non-prefilter path** (pass 1 not admissible) uses the same sequence: decode `S` first, then use `M_s` as `mask_ptr` for every other column. The downstream ExprFilter still runs over the survivors.
- **Pass-2 items** (`item.row_mask` non-empty): not armed. This is the same rule as the dictionary skip, and for the same reason: those rows already matched.
- Several searchable columns: use the first one in the registration order (§7 D5). The others are evaluated by pass 1 over survivors only.

## 6. Predicate scope

Armed from the same planner extraction as the dictionary skip (`predicates.py`). Only top-level AND conjuncts of the form `col <op> literal`:

| predicate | v1 | note |
|---|---|---|
| `col LIKE '%x%'` (InStr, case-sensitive) | **yes** | kind 4 today |
| several kind-4 needles on one column (LIKE ANY) | yes | one SIMD pass per needle, ORed; fused multi-needle is follow-on work |
| `col NOT LIKE '%x%'` | D3 | a verified hit drops the row; zero hits keeps every non-null row (still copies nothing for an unprojected column) |
| `'x%'` / `'%x'` (kinds 2/3) | D4 | not a buffer-wide search. It is a per-value `memcmp` during the prefix walk, with no copy and no slot. Same seam. |
| ILIKE / case-insensitive | no | case folding, and the open utf8h gaps |
| `'%a%b%'`, `_` | no | decided by pass 1 as today |

## 7. Decisions for the architect

- **D1 — Authority.**
  - (a) **Pruning only:** the search drops rows that cannot match, and pass 1 still evaluates the full predicate over the survivors. The contract is the same as `DictSkipPredicate` today: one filter, and the decoder only drops rows that are provably dead.
  - (b) **Authoritative:** the conjunct is removed from the pass-1 program.

  **Recommend (a).** The re-check costs only as much as the survivor count, the planner is untouched, and nothing can disagree. (b) needs planner surgery for a saving that is close to zero when the predicate is selective.
- **D2 — Build §4 (the dictionary per-entry filter)?** Recommend: build it alongside §3, then leave-one-out ablate. The shared primitive replacing `std::search` stays either way.
- **D3 — NOT LIKE in v1?** This needs a new kind and planner extraction. Recommend yes. The mechanism is the same mask with the opposite polarity, and it is what Q23's `URL NOT LIKE '%.google.%'` conjunct needs.
- **D4 — Anchored prefix and suffix on raw pages in v1?** Recommend a follow-on. It is the same seam, but a different inner loop.
- **D5 — Rename.** `DictSkipPredicate`, `ColDictPred` and `dict_preds_` stop being dictionary-only. Proposed names: `ValuePredicate`, `ColValuePred` and `value_preds_`. This also needs an ordering rule for several searchable columns. Recommend: the planner registers them in conjunct order, and the first searchable conjunct wins.

## 8. Delivery

**Stage 0 — before any edit. Needs your go-ahead to run benchmarks.**

- For URL and Title, on `hits_rugo_262k` **and** the foreign split files: what fraction of rows arrive in PLAIN pages and what fraction in dictionary pages, measured at the decoder rather than through a projection.
  - The selective-decode Stage 0 put plain strings at 1–11M of 99M rows on `hits_rugo_262k`. If that holds, §3 alone barely moves our own ClickBench files, and the value there sits in §4 and in foreign files. That would change the order in which the parts are built.
- Current baseline for Q21, Q22, Q23 and Q24, plus a profile split showing decompress / walk + copy / slot build / kernel for the URL column.

**Stage 0 result 1 — page census (2026-10-06).** Method: walked the page headers only, with a scratch tool built on rugo's `ReadParquetMetadata` and `ParsePageHeader`, over the full 100M rows.

| dataset | column | rows in PLAIN pages | rows in dict pages | dictionary entries | dict pages, uncompressed | PLAIN pages, uncompressed |
|---|---|---|---|---|---|---|
| `hits_rugo_262k` (zstd) | URL | 10.4% | 89.6% | 21.9M | 3.03 GB | 1.54 GB |
| | Title | 0% | 100% | 19.2M | 2.56 GB | — |
| `hits` canon split (snappy, the DuckDB reference set) | URL | **76.1%** | 23.9% | 3.4M | 0.32 GB | **7.95 GB** |
| | Title | **71.0%** | 29.0% | 2.6M | 0.31 GB | **7.48 GB** |

What this means:
- **Canon split (DuckDB's dataset): §3 is the main lever.** Three quarters of URL and Title rows sit in PLAIN pages, about 8 GB per column. Today every one of those bytes is copied twice and gets a slot built.
- **`hits_rugo_262k`: §4 is the main lever.** Rows are mostly dictionary-encoded, but the dictionaries are huge: 22M URL entries, 3 GB, which is 22% of the row count. Today the kernel evaluates contains once per entry through per-slot calls. §4's single SIMD pass over the dictionary page replaces that, and also replaces the dictionary-skip matcher's scalar `std::search` over the same entries.
- The "1–11M plain rows" figure from the selective-decode Stage 0 is correct for `hits_rugo_262k` URL (10.4M), and irrelevant for canon.

DuckDB reference times (canon split, this Mac, `results.local.json` from 2026-05-06): Q21 0.334 s, Q22 0.276 s, Q23 0.528 s, Q24 0.147 s.

**Stage 0 result 2 — baseline (2026-10-06, this Mac, 6P+12E).** Method: warm, then the minimum of 3 runs. Q0 is a decode-floor control, `COUNT(*) WHERE URL <> ''`. CPU time is process user+sys time; `decompress` is rugo's `decompress_s` summed across threads.

| dataset | query | wall (s) | CPU (s) | decompress (thread-s) |
|---|---|---|---|---|
| canon `hits` | Q21 | 0.597 | 9.60 | 2.87 |
| | Q22 / Q23 / Q24 | 0.683 / 1.093 / 0.182 | | |
| | Q0 floor | 0.537 | 8.65 | 2.78 |
| `hits_rugo_262k` | Q21 | 0.284 | 4.75 | — |
| | Q22 / Q23 / Q24 | 0.313 / 0.502 / 0.077 | | |
| | Q0 floor | 0.222 | 3.73 | — |

`decompress_s` reads above total CPU on `hits_rugo_262k`, so it is timing waits (mmap faults) as well as decompression. It is not usable as a share there.

What this means:
- **Canon:** Q21 is 1.8× DuckDB. The decode floor (Q0) is already 90% of Q21, so the search itself is not the cost. Decompression is about 30% of CPU; the remaining **~6.7 thread-s is spent after decompression**. That covers:
  - interning 74.2M plain values into a dictionary (`ba_intern_values`, the foreign-file re-derive policy);
  - arena copies;
  - slot builds;
  - the kernel.

  §3 removes all of that for values with no hit, because the search runs before interning.
- **`hits_rugo_262k`:** the predicate costs about 1.0 thread-s over the floor (4.75 vs 3.73). That is the per-entry kernel plus the `std::search` matcher over 22M dictionary entries, and it is what §4 targets.

**Stage 1 — built (2026-10-06).**
- `simd_find_cs` in `src/cpp/volnitsky.h`; `simd_contains_cs` is now a wrapper over it, so there is one inner loop.
- `rugo/src/parquet/page_search.hpp` holds the primitive.
- `DictSkipPredicate`, `ColDictPred` and `dict_preds_` became `ValuePredicate`, `ColValuePred` and `value_preds_`. Kind 5 is `NotInStr`.
- The search output is a caller-owned `PageSearchOut` out-parameter, not a `DecodedColumn` member. That keeps `DecodedColumn`'s reset contract untouched.
- The kind 4/5 dictionary-skip matcher uses the primitive instead of `std::search`.
- Worker: the first registered kind 4/5 column decodes first, and the rest decode under its rows.
- **Constraint found while building:** the search is not armed under the latmat pass-1 predicate without the prefilter. Pass 2 maps that survivor mask back to row-group rows, so those columns must stay full-length.
- Tests: `tests/unit/connectors/parquet_io/test_page_search.py`, 162 cases. A per-value oracle runs over 7 file shapes. Length prefixes collide with needle bytes by construction (lengths 32, 97 and 98 are prefix bytes `' '`, `'a'`, `'b'`). Mutation-checked: accepting every occurrence fails 56 of the tests.
- Results with the search on and off are identical on 12 queries × 2 ClickBench datasets.
- `make q` is green, as are the parquet_io and rugo suites (2,440). `tests/sql` has one failure, `test_window_catalog_matches_engine[COUNT_DISTINCT]`; it fails identically with the search off and is unrelated.

**Stage 2 — ablation (2026-10-06, this Mac).** Method: 3 arms, each in its own process, order reversed every round, 4 rounds. Per query, the minimum of 3 runs after a warm-up; the figure is the median across rounds.

| query | FULL | FULL−§4 | NONE | FULL/NONE | FULL faster in |
|---|---|---|---|---|---|
| canon Q21 | 0.187 | 0.196 | 0.647 | **0.289** | 4/4 |
| canon Q22 | 0.221 | 0.227 | 0.752 | **0.294** | 4/4 |
| canon Q23 | 0.310 | 0.427 | 1.215 | **0.255** | 4/4 |
| canon Q24 | 0.189 | 0.195 | 0.191 | 0.987 | 2/4 |
| canon `LIKE '%http%'` (no-prune canary) | 0.725 | 0.733 | 0.682 | **1.064** | 1/4 |
| canon `NOT LIKE '%.google.%'` (no-prune canary) | 0.831 | 0.833 | 0.747 | **1.112** | 0/4 |
| canon control `URL <> ''` | 0.590 | 0.590 | 0.578 | 1.020 | 2/4 |
| rugo_262k Q21 | 0.270 | 0.305 | 0.328 | 0.824 | 4/4 |
| rugo_262k Q22 | 0.315 | 0.349 | 0.378 | 0.833 | 4/4 |
| rugo_262k Q23 | 0.458 | 0.599 | 0.611 | 0.750 | 4/4 |
| rugo_262k Q24 | 0.084 | 0.084 | 0.087 | 0.966 | 4/4 |
| rugo_262k `LIKE '%http%'` | 0.362 | 0.351 | 0.341 | **1.062** | 1/4 |
| rugo_262k `NOT LIKE '%.google.%'` | 0.448 | 0.449 | 0.425 | **1.055** | 1/4 |
| rugo_262k control | 0.263 | 0.264 | 0.267 | 0.984 | 3/4 |
| **total** | **5.252** | 5.602 | 7.248 | 0.725 | |

Verdicts:
- On canon, Q21, Q22 and Q23 are now ahead of DuckDB's reference times (0.334 / 0.276 / 0.528).
- **§4 is KEPT**: removing it costs 1.03–1.38×, most on Q23. Its ablation switch is deleted.
- **Canaries regress by 5–11%** where nearly every row passes. Per §2.5 of the selective-decode design, this went back to the architect rather than getting a gate. **Ruled 2026-10-06: accepted.** `RUGO_PAGE_SEARCH` is deleted.

**Stage 1 — original plan.**
- The primitive, with the find-position variant in `volnitsky.h`.
- §3, §4 (subject to D2) and §5.
- Correctness tests in `tests/rugo/` (shape grid below), checked against the same scan with the search unarmed.

**Stage 2 — interleaved ABBA ablation.** Arms are FULL, FULL−§4, and NONE. The ablation switch exists only for the measurement and is deleted with the verdict. Queries:
- Q21–Q24;
- a non-selective canary, `URL LIKE '%http%'`, where nearly every row hits. This is the regression risk: one extra SIMD pass that prunes nothing;
- a string-free control.

**Test grid (correctness):**
- plain, dictionary, mixed dictionary→plain chunks, and DELTA_LENGTH pages;
- required and nullable columns;
- needles that:
  - sit at the start or end of a value;
  - span two values;
  - contain bytes that look like a length prefix (`"\x05\x00\x00\x00ab"`);
  - overlap themselves (`"aa"` in `"aaaa"`);
  - are 1 byte long;
  - are longer than every value;
- empty values;
- a page where every value hits, and a page where none does;
- multiple pages per chunk;
- page-pruned plus jump-plan row groups;
- pass-2 items (search must be unarmed);
- two searchable conjuncts.

## 9. Telemetry

Counted in locals and flushed once per chunk, never with a per-row atomic:
- `page_search_pages`;
- `page_search_pages_discarded`;
- `page_search_rows_in`;
- `page_search_rows_out`;
- `page_search_fallthrough_pages`;
- `dict_search_entries_hit`.

## 10. Not in this design

- Skipping **decompression** of a zero-hit page. That needs page-level statistics or a bloom filter that can answer "contains", and parquet has neither.
- Fetching other columns only after the mask is known (D-6 of the selective-decode design).
- Fused multi-needle search, and case-insensitive search.
