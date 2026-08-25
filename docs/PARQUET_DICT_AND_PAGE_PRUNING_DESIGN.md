# Parquet pruning: dictionary-page filtering (A) and page-index pruning (B)

Design note, 2026-08-25. Investigation only — no engine changes made.
Prompted by a review of Apache Impala's scan pruning. Architect decisions
required are marked **DECISION**.

Out of scope (separate threads): bloom filters, join-build-side runtime
min/max filters, reservation-based buffer pool.

---

## 0. Headline

* **Item A is largely already built**, and reaches further than the review
  assumed: strings, IN-lists, and LIKE prefix/suffix/contains are all covered
  today, not just integer equality. The sign-extension trap named in the brief
  was fixed on 2026-08-21 and is pinned by a regression test.
* **The generalisations still missing from Item A do not pay on our benchmark
  data.** Measured: on ClickBench the one equality predicate family that
  survives to the probe (`CounterID = 62`, Q37–Q43) is pruned to *exactly* the
  same row groups by footer min/max alone — the dictionary probe kills **zero**
  additional row groups. Recommendation is to harden tests, not widen scope.
* **Item B is not actionable as things stand.** Our writer emits no
  ColumnIndex/OffsetIndex, our reader parses the *pointers* but no page index
  structures, our writer emits **one data page per column chunk** by default (so
  a page index would have nothing to index), and the ClickBench files carry no
  page index either. Simulated ceiling on the only query family where it could
  apply: ~35 % of the decode work inside the 4.8 % of row groups those queries
  actually read ≈ **1.7 % of total scan work**.

---

## ITEM A — dictionary-page filtering

### A.1 Where the decision is made and enforced

Three stages, Python → Cython → C++:

| Stage | Location | Role |
|---|---|---|
| Extract | [predicates.py:227](opteryx/connectors/parquet_io/predicates.py:227) `_try_extract_str_func` | pulls `_STARTS_WITH` / `_ENDS_WITH` / `InStr` (the optimizer's LIKE lowering) out of the pushed expression tree |
| Extract | [predicates.py:458](opteryx/connectors/parquet_io/predicates.py:458) `_can_prune_rowgroup`, [:515](opteryx/connectors/parquet_io/predicates.py:515) `row_group_may_satisfy` | the **separate** footer min/max row-group prune |
| Encode | [pool_reader.pyx:1368](opteryx/connectors/parquet_io/pool_reader.pyx:1368) `_needle_slot` | puts every int needle into the probe's int64 slot (two's-complement for values above `INT64_MAX`) |
| Flatten | [pool_reader.pyx:1405](opteryx/connectors/parquet_io/pool_reader.pyx:1405) `_flatten_dict_skip_predicates` | `(col, op, val)` triples → `int_needles` / `str_preds`; called at [:1940](opteryx/connectors/parquet_io/pool_reader.pyx:1940) and [:2421](opteryx/connectors/parquet_io/pool_reader.pyx:2421) |
| Carry | [io_pipeline.hpp:1626](rugo/src/parquet/io_pipeline.hpp:1626) `dict_preds_`, setters at [:2720](rugo/src/parquet/io_pipeline.hpp:2720) `add_int_needles` / [:2724](rugo/src/parquet/io_pipeline.hpp:2724) `add_str_pred` | per-column predicate carried to the decode worker |
| **Enforce** | [decode_column.cpp:910](rugo/src/parquet/decode_column.cpp:910) soundness guard, [:933–:995](rugo/src/parquet/decode_column.cpp:933) the probe | evaluates the conjunct against the just-decoded dictionary; on disjoint sets `result.dict_all_filtered` ([decode.hpp:81](rugo/src/parquet/decode.hpp:81)) and returns **without decoding any data page** |
| Propagate | [io_pipeline.hpp:2218–2324](rugo/src/parquet/io_pipeline.hpp:2218) | a filtered column sets `result.empty_filtered` ([:208](rugo/src/parquet/io_pipeline.hpp:208)); the whole row group's remaining columns are abandoned and the consumer skips the morsel entirely |

The predicate struct is `DictSkipPredicate` at [decode.hpp:12](rugo/src/parquet/decode.hpp:12).

### A.2 Exactly how far it reaches today

**Types.** Physical `int32` (incl. every narrow/unsigned logical width, which
Parquet stores as int32), physical `int64` (incl. `uint64`), and
`byte_array` (VARCHAR/BLOB). Reached only when the column decodes in
dict mode (`int64_dict_mode` / `int32_dict_mode` / `byte_array_dict_mode`).

**Predicate forms.**

| Form | `kind` | Covered |
|---|---|---|
| `col = <int>` | 0 | ✅ |
| `col IN (<ints>)` | 0 | ✅ |
| `col = <string>` | 1 | ✅ |
| `col IN (<strings>)` | 1 | ✅ |
| `col LIKE 'p%'` → `_STARTS_WITH` | 2 | ✅ |
| `col LIKE '%s'` → `_ENDS_WITH` | 3 | ✅ |
| `col LIKE '%x%'` → `InStr` | 4 | ✅ |
| one-sided ranges `<`, `>`, `<=`, `>=` | — | deliberately **not** pushed — see A.4 |
| `BETWEEN` interior gaps | — | not implemented |
| `<>`, `NOT IN`, `NOT LIKE` | — | not implemented — see A.5 |
| case-insensitive `_CI_*` / `IInStr` | — | not implemented (needs case folding) |
| float / double membership | — | not implemented |

**Not "IN-lists are missing"** — multi-member IN is fully covered for both ints
and strings, and an IN-list is treated as a disjunction: one member that cannot
be represented forfeits the *whole* list's skip rather than pruning on the
survivors ([pool_reader.pyx:1429](opteryx/connectors/parquet_io/pool_reader.pyx:1429)).

**Gates.** The probe is disabled when: a `row_mask` is active (late-materialization
pass 2 already has survivors); `num_values <= 0`; or — the important one — when
**any data page in the chunk is not dictionary-encoded**. That last is the
dictionary-fallback guard at [decode_column.cpp:910](rugo/src/parquet/decode_column.cpp:910):
Arrow/Spark spill to PLAIN pages once a dictionary outgrows its page limit, and
those values are *not* in the dictionary page. Skipping on a dict miss there
would silently drop real matches. rugo pre-scans the data-page headers
(header-only, no decompression) and permits the skip only when every data page
is `RLE_DICTIONARY` or `PLAIN_DICTIONARY`. **This guard is load-bearing and must
survive any widening** — 4 of ClickBench's 105 columns have a PLAIN fallback page.

Per-column, **one** string predicate is used ("first wins"); further string
conjuncts on the same column are ignored. Sound, but see A.6.

### A.3 The signed/unsigned trap — current state

Fixed. [decode_column.cpp:957](rugo/src/parquet/decode_column.cpp:957) widens each
int32 dictionary entry as `result.is_unsigned ? (int64_t)(uint32_t)dv : (int64_t)dv`
and compares in int64. `is_unsigned` is derived from the logical-type string at
[decode_column.cpp:559](rugo/src/parquet/decode_column.cpp:559), well before the probe.
The int64 path compares raw bit patterns, which is correct precisely because
`_needle_slot` delivers a `uint64` needle as its two's-complement pattern —
exactly what a UINT64 dictionary entry is.

The invariant that makes the whole thing safe is stated in `_needle_slot`'s
docstring and is worth restating, because any widening must preserve it:

> **The safe direction is "decline the needle", never "matches nothing".**
> `any_match = true` only *disables* the skip, so a false match costs a slower
> scan. A failure to match *drops rows*.

Cross-type is handled by construction rather than by a promotion rule: an int32
column probed with a needle outside int32 range simply matches nothing, and
that is the right answer — the column cannot hold the value. Nothing widens the
*needle* to the column's type, which is what prevents a wrap-around match.

**What the tests actually cover** (`tests/sql/test_unsigned_dict_scan_equality.py`,
`tests/unit/connectors/parquet_io/test_dict_int_filter.py`): uint32 values on
both sides of the signed midpoint in one file; a bare-uint32 control alongside
the IPV4 column; uint64 above `INT64_MAX`; uint64 IN-lists; absent values; int32/
int64 equality and IN, present and absent; date32/timestamp/float64/float32 dict
columns; string `=`, `<>`, LIKE prefix/contains/absent.

**Gaps in that coverage**, all cheap to close and all in the class the brief
flags as the primary risk:

1. No **exact boundary values**: `0`, `0x7FFFFFFF`, `0x80000000` (the midpoint
   itself), `0xFFFFFFFF`, and at 64 bits `INT64_MAX`, `INT64_MAX+1`, `UINT64_MAX`.
   The existing uint32 test straddles the midpoint but never lands on it.
2. No **UINT8/UINT16** column. These can never set bit 31 so they are safe by
   construction, but that is an argument, not a test.
3. No test that a **signed int32 column with negative dictionary entries** is
   unaffected by the unsigned widening.
4. No test that the **dictionary-fallback guard** actually fires — i.e. a file
   with a needle that lands in a PLAIN spill page must still return its rows.
   This is the guard whose failure mode is silent wrong answers, and it is
   currently unpinned.

**DECISION A-1 — ✅ DONE 2026-08-25.** Closed as a test-only change, no engine
edits:

* `tests/unit/connectors/parquet_io/test_dict_skip_fallback_guard.py` (9 tests) —
  gap 4. Each fixture forces a real dictionary spill and places the needle
  *after* it, so the needle is in a PLAIN page and absent from the dictionary.
  Covers string `=`/`IN`, all three LIKE lowerings, int `=`/`IN`, plus two
  controls that the guard is not satisfied by matching everything. Every fixture
  asserts the file really carries both `RLE_DICTIONARY` and `PLAIN`, so it cannot
  silently stop reproducing the condition.
* `tests/unit/connectors/parquet_io/test_dict_skip_boundary_values.py` (50 tests) —
  gaps 1–3. Lands on `0x80000000`, `0xFFFFFFFF`, `INT64_MAX±1`, `UINT64_MAX` at
  uint8/16/32/64, with int32/int64 negatives as the control that an unsigned fix
  does not zero-extend a signed column.

**Verified by defeating the guard, not by inspection**: forcing
`dict_covers_all_rows = true` and rebuilding turns all 7 guard assertions red,
each returning exactly `0` — the silent wrong answer — while the two controls
stay green. Source restored byte-identical; `make q` 462/462.

One boundary case is NOT reachable from SQL: a `u64` IN-list mixing members
above and below `INT64_MAX` is refused by the logical planner
(`ArrayWithMixedTypesError`) before it reaches the scan. That is a pre-existing
limitation pinned elsewhere; the test groups members by bind type rather than
re-testing the refusal.

### A.4 Range predicates — do they duplicate the column-chunk statistics?

**One-sided ranges: yes, entirely.** `max(dict) > X` is arithmetically identical
to `chunk_max > X`, which the footer already gives us for free at
[predicates.py:458](opteryx/connectors/parquet_io/predicates.py:458). Pushing them
would cost a dictionary-page read to re-derive a statistic we already hold.
Correctly declined today; should stay declined.

**Interior gaps: no, not duplicated** — a dictionary can prove
`BETWEEN 40 AND 50` matches nothing even when the chunk spans `[1, 100]`,
which min/max cannot. This is the only genuinely new pruning power a dictionary
offers over the footer.

But it only pays when the distinct values are *clustered with gaps* and the
predicate lands in one. Measured on ClickBench: `CounterID` has 3 distinct
values per row group and `EventDate` has 1, so min/max is already exact for
both; every other range-filtered column is a hash or a timestamp with dense
coverage. **No measured case in our benchmark data where an interior-gap probe
prunes anything min/max does not.**

**DECISION A-2.** Do not implement interior-gap range probing. Revisit only if a
real workload shows a clustered-with-gaps numeric column under a range filter.

### A.5 What is NOT safe to answer from the dictionary, and why

Pruning asymmetry: a false *keep* costs time, a false *skip* is a wrong answer.
Everything below is on the wrong-answer side of that line.

1. **Negated predicates (`<>`, `NOT IN`, `NOT LIKE`).** The skip test for a
   positive predicate is "no dictionary value satisfies it". For a negated one
   it inverts to "*every* dictionary value satisfies the positive form", which
   for `col <> 'x'` means the chunk is the single constant `'x'` — already
   detectable from `min == max` in the footer, for free. Implementing these buys
   nothing and doubles the surface where a mis-inverted test drops rows.
   **This matters for us specifically**: ClickBench Q02, Q08, Q11–Q15, Q25–Q29,
   Q31, Q32 are all `<> 0` / `<> ''`. That is 13 of the 25 filtered queries, and
   none of them is skippable by any dictionary mechanism.
2. **Any predicate over a chunk with dictionary fallback.** Covered by the
   existing guard; do not weaken it.
3. **NULLs.** The dictionary never contains NULL. This is currently benign only
   because every supported form is positive and NULL satisfies none of them
   (`NULL = x`, `NULL LIKE p` are both NULL, i.e. not selected), so "no dict
   value matches ⇒ no row matches" holds. **Any negated or `IS NULL`-adjacent
   form breaks this**, and the definition-level information needed to reason
   about it is not consulted by the probe at all.
4. **Case-insensitive matching.** Not unsafe in principle, but requires a case
   fold that agrees *exactly* with the kernel's, on the same Unicode data. A
   fold that is even slightly more aggressive than the kernel's drops rows.
   Correctly deferred.
5. **Collation- or locale-sensitive comparison.** Same reason; we do byte-exact
   `memcmp` and must keep doing so.
6. **Float equality.** The probe would have to reproduce the kernel's exact
   float comparison semantics, including −0.0 == +0.0 and NaN. Not worth it for
   a predicate form that is a code smell anyway.

### A.6 Measured value of the remaining generalisations

Data: `scratch/hits_partitioned/` — 100 files, 1 M rows each, 105 columns, every
column dictionary-encoded (101 pure dict, 4 with a PLAIN fallback). Sample of 20
files / 63 row groups / 20 M rows.

**Dictionary-probe cost.** The probe is not free: it reads and decompresses the
dictionary page. Dictionary page as a fraction of the column chunk, row group 0:

| Column | chunk | dict page | probe cost |
|---|---:|---:|---:|
| Title | 19.56 MB | 0.52 MB | 2.6 % |
| URL | 6.46 MB | 0.41 MB | 6.4 % |
| Referer | 7.61 MB | 0.53 MB | 7.0 % |
| WatchID | 3.88 MB | 1.05 MB | 27.0 % |
| UserID | 0.46 MB | 0.25 MB | 54.3 % |
| SearchPhrase | 0.63 MB | 0.47 MB | 74.8 % |
| OriginalURL | 0.27 MB | 0.22 MB | 82.2 % |
| *all 105 columns* | 52.1 MB | 7.0 MB | 13.5 % |

This is why the mechanism pays on wide string columns and is close to break-even
on narrow ones — and why the payoff is asymmetric: the probe is a bet that costs
2–7 % on the columns that matter and wins ~93–97 % when it hits.

**Row-group skip rates actually achievable** (20 files, 63 row groups):

| Predicate | source | row groups skippable |
|---|---|---:|
| `UserID = 435090932899640449` | Q20 | **100 %** |
| `Title LIKE '%Google%'` | Q23 | 14.3 % |
| `URL LIKE '%google%'` | Q21–24 | 15.9 % |
| `CounterID = 62` | Q37–43 | 95.2 % |

**And the decisive control** — how much of that `CounterID` figure the footer
statistics already deliver:

```
row groups                         63
survive footer min/max prune        3  (4.8 %)
survive dictionary probe            3  (4.8 %)
ADDITIONAL row groups killed by the dictionary:  0
```

Zero. For Q37–Q43 the dictionary probe is pure overhead: it reads a dictionary
page to reach a conclusion min/max reached for free. The probe's real earnings on
ClickBench are Q20 (min/max useless on a random 64-bit key — already delivered,
105 ms → ~37 ms per the prior work) and Q21–Q24 (`LIKE`, which min/max cannot
prune *at all*, at a modest 14–16 % — also already delivered).

**Conclusion for Item A.** The high-value generalisations named in the brief —
string equality, string IN, multi-member IN — are all already implemented. The
ones that are not implemented are, on our data, either worthless (negated forms:
13 of 25 filtered queries, none skippable), redundant with min/max (ranges), or
blocked on a correctness obligation not worth taking on (case folding, floats).

**DECISION A-3.** Recommend **no functional widening of the dictionary probe**.
Spend the effort on A-1 (boundary and fallback-guard tests) instead. The one
functional change I would put to you as arguable is allowing **multiple string
predicates per column** rather than "first wins" — `Title LIKE '%a%' AND Title
LIKE '%b%'` currently probes only one — but I have no measured case where it
changes a skip decision, so my recommendation is no.

---

## ITEM B — page-index (ColumnIndex / OffsetIndex) pruning

### B.1 Do we parse it? Do we write it?

**Reader: pointers only, no structures.** The `ColumnChunk` fields are parsed
into the metadata struct — [metadata.cpp:716–729](rugo/src/parquet/metadata.cpp:716),
stored at [metadata.hpp:49–52](rugo/src/parquet/metadata.hpp:49), and even exposed
to Cython at [parquet_reader.pxd:43–46](rugo/src/parquet/parquet_reader.pxd:43).
**Nothing reads them.** There is no `ColumnIndex` or `OffsetIndex` thrift parser
anywhere in `rugo/src/`; the `parquet.thrift` spec copy carries the definitions
and nothing else does. So: we know where the page index *is* in every file that
has one, and we cannot decode it.

**Writer: emits nothing.** No page-index emission in
[_parquet_writer.hpp](rugo/src/parquet/_parquet_writer.hpp); the only `Statistics`
struct written is the per-chunk one at
[_parquet_writer.hpp:1859](rugo/src/parquet/_parquet_writer.hpp:1859).
Confirmed empirically — a file written by `rugo.parquet.write_parquet` reports
`has_column_index=False, has_offset_index=False`.

**Worse, and this is the load-bearing fact: our writer emits one data page per
column chunk.** `write_parquet`'s `max_page_bytes` defaults to `0`, and
`max_page_bytes == 0` disables page splitting entirely
([_parquet_writer.hpp:958, :973, :1026](rugo/src/parquet/_parquet_writer.hpp:958)).
Opteryx never passes the parameter — it appears nowhere in `opteryx/`. So every
file we write today has exactly one page per chunk, and **a page index over a
single page carries precisely the information the chunk statistics already
carry.** Page-index pruning on our own files is not merely unimplemented; it is
structurally impossible until the writer also splits pages, which is a change to
compression ratio and layout, not a metadata addition.

**And the benchmark files do not have one either.** ClickBench `hits_*.parquet`
was written by `parquet-cpp 1.5.1-SNAPSHOT` (2018), predating the page index:
`has_column_index=0, has_offset_index=0` across all 210 column chunks of
`hits_0.parquet`.

So the prerequisite chain, stated plainly, is:

> writer page splitting → writer page-index emission → reader index parsing →
> reader skip integration

and **none of it produces a single skipped page on any file we own today.**

### B.2 How page skipping would interact with the decode loop

The honest version: **most of this substrate already exists**, because the
late-materialization pass-2 path already skips data pages. That is the good news.
The bad news is where it does *not* apply.

**What exists.** `PreScanPages` ([decode_column.cpp:352](rugo/src/parquet/decode_column.cpp:352))
walks page headers accumulating `page_row_offset`, and tests each page's slice
of the `row_mask` with a word-at-a-time scan
([:403–:425](rugo/src/parquet/decode_column.cpp:403)), marking `PageTask::skip_page`.
The sequential decode loop honours it at
[:1337–:1368](rugo/src/parquet/decode_column.cpp:1337), counting `pages_skipped` /
`pages_decoded` ([decode.hpp:58](rugo/src/parquet/decode.hpp:58)). So "skip a data
page mid-column" is a solved problem in this codebase.

**Row indexing.** The existing skip keeps output dense: a skipped page appends
nothing to the output vectors *and* nothing to `decoded_row_mask`, so the two
stay in lockstep and the post-loop compaction at
[:2489](rugo/src/parquet/decode_column.cpp:2489) is unaffected. A page-index skip
would have to preserve exactly this discipline. Note the mechanism is a
*decode* skip, not an *I/O* skip: the page-header chain is still walked, because
`page_row_offset` is derived by summing `num_values`. An `OffsetIndex` would
supply `first_row_index` and byte ranges directly, which is what turns this into
a real I/O skip — and is the actual argument for `OffsetIndex` over hand-walking.

**Where it gets genuinely hard, stated without gloss:**

1. **The parallel decode path is incompatible as written.** Tier-3 requires
   `row_mask == nullptr` ([:1107](rugo/src/parquet/decode_column.cpp:1107)) and calls
   `PreScanPages` with a `nullptr` mask, so `skip_page` is always false there;
   the guard at [:1171](rugo/src/parquet/decode_column.cpp:1171) is defensive only.
   Crucially, each parallel page task writes at an **absolute** `out_offset`
   computed over *all* pages including skipped ones. Skipping a page on that
   path leaves an uninitialised hole in the output buffer. Reconciling
   page-index skipping with parallel page decode means either recomputing
   `out_offset` over surviving pages only (and carrying a per-page row map for
   the residual filter), or accepting that page-skipped columns fall back to the
   sequential loop and lose intra-column parallelism. That is a real
   performance trade, not a detail.
2. **Skipping mid-column while siblings read densely is the crux.** Page
   boundaries do not align across columns — each column chunk pages
   independently. A page-index skip on the *predicate* column yields a set of
   surviving row ranges, which then has to be projected onto every *other*
   column's page layout. Columns whose pages straddle a boundary must still
   decode the straddling page and discard rows. So the saving on non-predicate
   columns is always ≤ the saving on the predicate column, and degrades as page
   sizes diverge. The existing `row_mask` machinery already expresses exactly
   this (a per-row survivor mask projected onto each column's own page layout),
   which is the natural integration point — a page-index prune is, in effect, a
   cheaply-derived pass-1 `row_mask` that costs no pass-1 decode.
3. **Definition levels.** The existing skip is gated on
   `max_repetition_level == 0` ([:1337](rugo/src/parquet/decode_column.cpp:1337));
   LIST columns take a different route that walks repetition levels
   ([:2742](rugo/src/parquet/decode_column.cpp:2742)). Definition levels for
   nullable non-repeated columns are stored per page, so a page skip drops them
   consistently — fine. Repeated columns are not fine, because a page boundary
   is a *value* boundary and not a *row* boundary; `OffsetIndex.first_row_index`
   exists precisely to resolve this, and would be mandatory rather than
   optional for LIST columns.
4. **RLE / dictionary decoding.** Both are page-local in Parquet — an RLE run
   never crosses a page boundary, and the dictionary is chunk-level and read
   before any data page. So neither obstructs a page skip. This is the one part
   that is genuinely easy.

### B.3 Composition with pushed LIMIT and with row-group pruning

**With row-group pruning:** strictly subordinate, and that ordering is already
enforced structurally — footer min/max prunes at manifest time, the dictionary
probe fires inside the worker on chunks that survived, and page pruning would sit
one rung below that, on row groups that survived both. Composition is clean.

**With a pushed LIMIT:** this is the one that needs care. Today the pushed limit
short-circuits row groups at [pool_reader.pyx:1839](opteryx/connectors/parquet_io/pool_reader.pyx:1839):

```
cdef bint limit_gate = (limit is not None) and not predicates
```

— the limit only stops early when there are **no predicates at all**, because
with a predicate the reader cannot know how many rows a row group will yield.
Page pruning does not change that: knowing a page's min/max tells you a page
*may* match, never how many rows it yields, so a pruned page count can never
loosen `limit_gate`. Page pruning and pushed LIMIT compose safely by simply not
interacting — which is the correct answer, since the pushed limit is a
correctness obligation on this path and any attempt to have page statistics
inform the row budget would be the kind of "provably safe or not at all" gate
that has burned us before.

### B.4 Measured ceiling

ClickBench has no page index, so the ceiling has to be *simulated*: take the row
groups that survive existing pruning, cut them into hypothetical pages, and ask
what fraction a per-page min/max would kill. Predicate `CounterID = 62`
(Q37–Q43 — the only ClickBench family with a range/equality predicate that page
statistics could serve; the `LIKE` queries cannot be page-pruned by min/max at
all, and Q20's random 64-bit key defeats min/max at every granularity):

Surviving row groups: 3 of 63 (956 238 rows).

| simulated page size | pages | pages skippable | rows still read |
|---:|---:|---:|---:|
| 2 048 | 468 | 35.7 % | 64.2 % |
| 8 192 | 118 | 34.7 % | 64.9 % |
| 20 000 | 50 | 34.0 % | 66.1 % |
| 65 536 | 16 | 31.2 % | 70.9 % |
| 131 072 | 9 | 22.2 % | 77.7 % |
| 450 560 (= 1 page/chunk, our writer's default) | 4 | **0 %** | 100 % |

Two things fall out. First, the benefit **saturates at ~35 %** and never
approaches the near-total pruning row-group statistics achieve on this
predicate — `CounterID = 62` rows are spread thinly across most of each
surviving row group, so this data is weakly clustered and page statistics cannot
rescue that. Second, the benefit **is entirely a function of page size**, and at
our writer's current one-page-per-chunk default it is exactly zero.

Scaled to the whole dataset: 34.7 % of the decode work inside 4.8 % of the row
groups ≈ **1.7 % of total scan work** for Q37–Q43, and **0 %** for every other
ClickBench query.

The fairer framing for a decision is the other one: for the queries that
*do* benefit, page pruning would cut up to ~35 % of the row-group decode work
they actually perform. That is a real number, but it is an upper bound on a
simulated layout, it requires the full four-step prerequisite chain, and it
requires re-writing every file we own with smaller pages.

**No speedup is claimed here. Nothing was benchmarked** — these are file
statistics and simulations, not timings. Any implementation would need an
interleaved A/B baseline established before the first edit.

### B.5 Recommendation

**DECISION B-1.** Do not implement page-index pruning now. It cannot pay on any
file we currently own, and the prerequisite (writer page splitting) is itself a
layout change with its own compression and read-pattern consequences that should
be evaluated on its own merits, not smuggled in as a dependency of a pruning
feature.

**DECISION B-2.** If you want the door left open, the cheap, self-contained,
non-speculative first step is **reader-side `ColumnIndex`/`OffsetIndex`
parsing**, since it makes us able to exploit page indexes in *foreign* files
(Spark and modern Arrow both write them by default) without touching our writer
or our file layout at all. That is a genuinely separable piece of work, and it is
the one I would put first if we do anything here. It would also give us the
`first_row_index` needed to turn the existing page skip from a decode skip into
an I/O skip.

**DECISION B-3.** Writer-side emission is a separate question that should be
decided together with page splitting, and only if a workload appears whose
predicate columns are well clustered. ClickBench's are not.

---

## Summary of decisions requested

| # | Decision | My recommendation |
|---|---|---|
| A-1 | Add boundary-value tests (0, midpoints, `INT64_MAX`, `UINT64_MAX`), a UINT8/16 case, a signed-negative case, and a test pinning the dictionary-fallback guard | ✅ **DONE** — 59 tests, verified red on a real guard-defeat rebuild |
| A-2 | Implement interior-gap range probing | **No** — no measured case beats min/max |
| A-3 | Any further functional widening of the probe (negated forms, CI LIKE, floats, multi-predicate-per-column) | **No** — measured as worthless, redundant, or a correctness liability |
| B-1 | Implement page-index pruning | **No** — zero benefit on files we own |
| B-2 | Implement reader-side ColumnIndex/OffsetIndex parsing alone, for foreign files | **Defer, but this is the one worth doing first if we do anything** |
| B-3 | Writer-side page splitting + page-index emission | **No** — decide with page splitting on its own merits |
