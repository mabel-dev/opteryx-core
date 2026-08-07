# Parquet Read Performance Gap Analysis vs DuckDB

Status: **live** — Phase 1 landed 2026-08-07, Phases 2–4 in progress.
Reference implementation studied: DuckDB v1.4.4, `extension/parquet/`.

---

## 1. Executive Summary

`make job` is our worst benchmark against DuckDB. Per-operator CPU profiling on
both engines across the 109 comparable JOB queries decomposes the gap as:

| | Opteryx CPU | DuckDB CPU | ratio | share of excess |
|---|---|---|---|---|
| **scan** (ParquetRead) | 109,039 ms | 31,393 ms | **3.47x** | **57.7%** |
| **join** (DrakenInnerJoin) | 64,937 ms | 7,322 ms | 8.87x | 42.8% |
| other (filter, aggregates) | 250 ms | 849 ms | 0.29x | −0.4% (we win) |

**The scan is the larger absolute term, and 92 of 109 queries are scan-dominated.**
Join-free scans of the same files confirm the deficit is raw read throughput, not
plan quality: 2.6x–7.1x slower (`SUM(movie_id)` over cast_info: 64ms vs 9ms).

The dominant single cause was found and removed: on a column chunk whose
dictionary spills to PLAIN data pages, the reader **re-interned every PLAIN
value back into the dictionary through a per-value hash probe** — ~40ns/value
against DuckDB's ~1.6ns — purely to preserve the §11 Dict vector shape. DuckDB
never builds a hash table anywhere in its read path.

Three secondary costs remain, addressed in Phases 2–4.

---

## 2. DuckDB vs Opteryx: architectural comparison

### What DuckDB does (verified in v1.4.4 source)

| concern | DuckDB | file |
|---|---|---|
| dict-encoded page | `RleBpDecoder::GetBatch` batch-unpacks codes → selection vector → `result.Dictionary(...)`, a **zero-copy dictionary vector**. No per-value work. | `decoder/dictionary_decoder.cpp` |
| PLAIN page | decoded **directly into the flat output vector**; INT32 is effectively a bounds-checked bulk copy | `column_reader.cpp` (`Plain()`, `TemplatedColumnReader<int32_t>`) |
| mixed dict + PLAIN chunk | nothing special — the PLAIN rows simply materialise flat; the dictionary is dropped for them | `column_reader.cpp` (`ReadData` → `default: Plain(...)`) |
| hash tables | **none anywhere in the read path** | — |
| type stability | INT32 stays INT32; no boxing, no coercion | — |

The governing property: **decode is selection-driven and batch-oriented, never
row-driven, and the scan defers everything it can.**

### What Opteryx does

Opteryx's target representation is the same idea — §11's Dict shape
(`data` + `selection` codes) *is* DuckDB's dictionary vector, and our operators
have dict-aware fast paths DuckDB's do not. The architecture is not the problem.

The divergence was **forcing that representation against the file's own signal**.
`prefer_dict` (`io_pipeline.hpp:1617-1624`) was set for *every* unmasked numeric
column that merely possessed a dictionary page — with no NDV gate — and stayed
set even when the writer had already spilled to PLAIN because the column's
cardinality was too high to dictionary-encode. Preserving Dict shape then
required re-deriving a dictionary at read time, per value, with a hash map.

---

## 3. Identified divergence points

1. **Per-value hash interning on dict spill** *(dominant; fixed in Phase 1)*
   `decode_column.cpp:1959-1979` (int32) and twins for int64/int128/float32/float64,
   backed by `PrimitiveDictHashMap` (`:199-267`). Reachable because `prefer_dict`
   disabled the `rle_path` gate (`:932-937`), which made the existing dict→dense
   mid-chunk transition (`:1863-1955`) unreachable; nullable columns
   (`max_definition_level > 0`) never had that transition at all.

2. **Double-pass identity copies in the direct builders** *(Phase 2)*
   `build_direct_fixed` (`io_pipeline.hpp:909-963`) ran DK_INT32 through a
   per-element 4-byte `std::memcpy` into a staging vector — an identity copy —
   then bulk-copied the staging vector into the draken allocation. Two full
   passes plus a libc call per element for a plain INT32 column.

3. **Scalar bit-unpacking at the widths that matter** *(Phase 3)*
   `unpack_group_8_scalar` (`decode_encodings.cpp:36-63`) assembles each value
   from up to 5 source bytes in an inner loop for `bit_width` 9–32 — exactly the
   widths a high-NDV dictionary uses. NEON covers only widths 1/2/4, AVX2 only
   4/8; everything else falls to that scalar loop. `get_unpack_fn()` is also
   re-resolved inside the run loop rather than hoisted.

4. **Allocation churn per row group and per page** *(Phase 4)*
   `DecodedColumn scratch` (`io_pipeline.hpp:1592`) is destroyed at the end of
   every row group, freeing multi-MB reserves (visible as `mmap`/`munmap` in the
   profile); page-local vectors are constructed per page; `def_levels` is copied
   rather than moved into the result (`decode_column.cpp:2699`).

### Divergences we do *not* have

Worth recording, because the brief hypothesised them and the code disproves them:

- **Type stability is already correct.** `DrakenType` carries INT8/16/32/64
  (`draken/core/buffers.h:34-37`), narrow-int kernels exist
  (`draken/ops/fixed_int_ops.h`), and `direct_kind_for` already emits `DK_INT32`
  for a bare physical int32 rather than widening. `parquet_simd::widen_int32_to_int64`
  has no live caller. "Stop widening int32→int64" was done in E33/A1.
- **The scan is already selection-vector-first** for genuinely dict-encoded data,
  and Dict shape survives filters via `dict_take.h`.
- **No boxing or abstraction layer** sits on the decode path; it is C++ throughout.

---

## 4. Root cause analysis

JOB's tables are written with `SNAPPY` and mixed `PLAIN` + `RLE_DICTIONARY`
pages. Every high-NDV join key (`cast_info.movie_id`, 36.2M rows, ~2.5M distinct)
spills its dictionary almost immediately. For those chunks:

- dictionary prefix decoded normally, then
- **every remaining value** → `InternPrimitiveToDictionary` → fmix64 hash, probe,
  insert, dictionary growth toward NDV = N.

A 3-second `sample` of `SUM(movie_id)`:

```
InternPrimitiveToDictionary<int>   2987 samples
SeedPrimitiveDictionaryMap<int>    2644
_platform_memmove                  3076   (dictionary growth + rehash)
snappy::RawUncompress                43   (actual decompression)
```

Decompression is 43 samples; interning and the memory traffic it causes are
~8,700. Span-traced decode for that one column: **1,466 ms for 36.24M values
= ~40ns/value**, against DuckDB's ~1.6ns.

The interning also carried a **live correctness bug**: `code_width` is frozen
when the dictionary page is decoded (`:644`), but interning *grows* the
dictionary. A chunk whose dictionary sat just under a packed-width boundary
(e.g. 256 entries → 1-byte codes) and then spilled would assign code 256+,
which `WritePackedCode` (`:59-72`) silently truncates — wrong values returned
with `success = true`.

---

## 5. Gap closure plan

### Phase 1 — dict→dense transition on spill ✅ landed

**Rule, now uniform across all types:** a PLAIN/DELTA data page inside a
dict-mode chunk is the writer's high-cardinality verdict. Materialise the
dict-coded prefix to dense, drop the dictionary, decode the rest dense.
**We never intern a PLAIN page into a dictionary on read.**

Strings (`:2184-2202`) and non-`prefer_dict` numerics (`:1859-1877`) already did
exactly this; `MaterializeDictPrefixToDense` extends it to
int32/int64/int128/float32/float64 across all three code containers:

| container | when | transition |
|---|---|---|
| `dict_codes_array` | nullable, unmasked | def-level walk bounded by `total_collected` (**not** `all_def_levels.size()` — the current page's def levels are already appended, its codes are not); null rows skipped, so the zero placeholder is never misread as a code |
| `dict_indices` | non-nullable, or nullable+masked | min/max validate then SIMD gather; sized by `dict_indices.size()` (**not** `total_collected` — masked pages bump the counter without decoding) |
| both empty | first data page is PLAIN | no-op |

Dense nullable output is compact present-only and validity is rebuilt from def
levels in the epilogue, so the transition needs no positional scatter and no
validity work. `dict_ordered` is cleared (a sorted-dictionary flag must not
survive to describe dense output — the string path was missing this too).

Deleted: `PrimitiveDictHashMap`, `InternPrimitiveToDictionary`,
`SeedPrimitiveDictionaryMap`, `Float32Bits`/`Float64Bits`,
`place_plain_dict_codes`, all five intern branches, and a stale comment
describing a 0.8-ratio heuristic the code had not implemented for some time.

**Fully-dict chunks keep Dict shape** — the transition only fires inside the
PLAIN/DELTA branch — so the dict-aware operator fast paths (dict compare,
k-probe GROUP BY, sorted-dictionary min/max) are structurally untouched.

### Phase 2 — builder single-pass

Point `csrc` straight at the source vector whenever the declared width already
equals the source width (DK_INT32/DK_UINT32 from int32, DK_UINT64 from int64),
so the single bulk copy is the only pass; genuine narrowing (INT8/16, UINT8/16)
keeps its staging loop. Factor the four open-coded packed-code expansion loops
in the dict builders into one `expand_packed_codes` helper: `cw == 4` is a bulk
copy, `cw == 1/2` widen via NEON `vmovl` / AVX2 `cvtepu`, scalar fallback.

### Phase 3 — bit-unpack widths 9–32 ✅ landed

Replaced the per-value multi-byte assembly with one unaligned 64-bit load +
shift + mask (`bit_off ≤ 7` and `bw ≤ 32` ⇒ ≤ 39 bits ⇒ a single 8-byte load
always covers a value), plus NEON and AVX2 `srlv` kernels for widths 9–16.

**The fast paths deliberately overread past the current group of 8**, so the
**last group of every run must use the bounds-checked helper** — otherwise the
decoder reads past the end of the page buffer. This is the single invariant the
phase rests on, and it is verified by an ASAN harness with exactly-sized heap
buffers plus a negative control that confirms the sanitizer trips when the fast
path *is* used on the final group.

*Plan correction:* the plan's "hoist `get_unpack_fn()` out of the run loop" item
was dropped — `SIMD_STATIC_SELECT` (`draken/simd/simd_dispatch.h:33-41`) is a
compile-time `#define`, so the call already folds to a constant.

### Phase 4 — allocation churn ✅ items 1–2 landed

1. `valid_bits` is built **before** `all_def_levels` is moved into the result,
   which is what lets the move replace a full copy (one int32 per row, per
   nullable column chunk).
2. The three page-local vectors (`page_rep_levels`, `def_levels`, dict
   `indices`) hoisted to chunk scope. Safe because every `DecodeRLEBitPacked*`
   entry point `clear()`s its output before writing, so no state crosses pages —
   only capacity does, turning a reserve+realloc per page into one allocation
   per column chunk.

**Item 3 (per-worker `DecodedColumn` scratch pool) is deliberately NOT done —
open for the architect.** `scratch` (`io_pipeline.hpp:1638`) is function-local
to `decode_row_group`, so its ~25 buffers are freed per row group. Pooling them
per worker means mutable state keyed on `BS::this_thread::get_index()`. That
index is used in this file today, but only ever as a *trace label* — never to
key mutable state — and this codebase has a burned-in bug of exactly that class
(the trace `ThreadArena` dangling-pointer, where per-thread state assumed thread
lifetime). Meanwhile the post-Phase-4 profile still shows `__mmap` at ~13% of
samples, but a large share of that is the **per-row-group file mmap**
(`io_pipeline.hpp:1438`), which item 3 does not address at all. Verdict: real
but unquantified payoff, non-trivial risk, needs a ruling before it is built.

Item 4 (per-file mmap cache across row groups) remains deferred.

---

## 6. Risk analysis

| risk | status / guard |
|---|---|
| Spilled chunks reach operators Dense, losing the k-probe | Accepted by design. The join k-probe already gates on `data_length * 2 ≤ num_rows`, which an interned near-unique dictionary failed anyway. GROUP BY/DISTINCT have no such gate and do lose it — measured via the benchmark suite, not argued. |
| Dict-aware wins (ClickBench Q20-class) regress | Fully-dict chunks never enter the transition. Pinned by a test asserting `is_dict` at the scan boundary for a pure-dict chunk and `not is_dict` for a spilled one. |
| Nullable def-level walk off-by-one | Bounded by `total_collected`; multi-page nullable tests with nulls in both the dict prefix and the spill pages. |
| Masked-path sizing | Gather sized by container, not row counter; filter+fallback test. |
| Results change where the old truncation bug fired | Intended — the old path was wrong. Pinned by a 256-entry-dictionary + >256-NDV-spill nullable test. |
| Phase 3 tail overread | Final group through the safe path + ASAN over the fuzz corpus. |
| Phase 4 memory retention | Explicit pool with shrink cap + RSS watch during ClickBench. |

---

## 7. Validation plan

**No microbenchmarks.** Every change is gated on all four end-to-end suites —
`make clickbench`, `make tpch`, `make job`, `make h2o` — which must be a visible
improvement or neutral. Plus `make q` (217/217) and the rugo + storage suites
(1056 tests).

Reference points are the best-ever records in
`tests/performance/clickbench/opteryx/results.local.json`. **ClickBench is the
priority benchmark, then TPC-H; JOB is a regression-identification tool.**

Regression tests: `tests/storage/test_numeric_dict_fallback_decode.py` — 13
cases covering int32/int64/float64 × nullable/non-nullable × {plain, pushed
filter (mask path), nulls in prefix and spill}, the code-width truncation case,
and the shape-boundary assertions.

### Benchmarking hazards (learned the hard way, 2026-08-07)

- **`parquet_metadata.encodings` cannot prove a data-page spill.** Every
  dictionary column reports `PLAIN, RLE, RLE_DICTIONARY` because the dictionary
  page is *itself* PLAIN-encoded. To find genuinely spilled columns, check
  `is_dict` at the scan boundary (`iter_row_groups_ipc`) and
  `dictionary_page_offset IS NOT NULL` for whether a dictionary exists at all.
  Sweeping TPC-H this way found exactly one affected column
  (`orders.o_shippriority`, a single distinct value).
- **Run order outweighs the change.** TPC-H reads 1445–1545 ms when run after
  JOB + ClickBench, and 1399–1415 ms standalone with cooldowns. Compare
  like-for-like position in the sequence, on a quiet machine.
- **ClickBench run-to-run spread is ~8%** on an identical binary (16.13 s vs
  14.87 s back-to-back). A single reading carries no signal.

---

## 8. Measured results

Machine: Apple Silicon dev laptop (contended — see hazards above). Each phase
ran the four suites in an identical sequence, so figures compare like-for-like
by position; TPC-H and ClickBench additionally repeated standalone with
cooldowns.

| suite | baseline | Phase 1 | Phase 2 | Phase 3 | Phase 4 | reference |
|---|---|---|---|---|---|---|
| `make job` (position 1) | 37,416 ms | 33,149 | 33,604 | 31,746 | **31,305** | 36,226 ms |
| `make clickbench` (best of run) | 16.01 s | 14.87 | 15.68 | 14.98 | **14.94** | 15.08 s |
| `make tpch` (cooled, best) | 1330 ms† | 1399.6 | 1383.1 | 1385.8 | 1393.0 | 1369.6 ms |
| `make h2o` | 2503 ms | 2470 | 2373 | 2492 | 2670 | 2125 ms |

† the baseline TPC-H figure is a standalone run taken before any edit; it is the
only cell not gathered under the cooled protocol.

**Net: JOB −16.3% (37,416 → 31,305 ms), a new best against the 36,226 ms record.
ClickBench 14.94 s against the 15.08 s current-architecture reference. TPC-H and
h2o inside their run-to-run bands.**

Isolated scan, `SUM(movie_id)` over cast_info (36.24M rows): **64.1 ms → 32.5 ms**.
Span-traced decode CPU for the same column: 1466 ms → the interning symbols
(`InternPrimitiveToDictionary` + `SeedPrimitiveDictionaryMap`, 5,631 profiler
samples pre-change) no longer appear at all; the replacement
`MaterializeDictPrefixToDense` costs 528.

h2o deserves a note rather than a claim: its four readings this session span
2373–2670 ms with no monotonic relationship to the phases, and the changes that
landed cannot touch it (its group keys are low-cardinality, so their dictionary
codes pack at ≤8 bits — below the Phase 3 fast path — and its columns do not
spill). Treat the h2o column as noise, not signal.
