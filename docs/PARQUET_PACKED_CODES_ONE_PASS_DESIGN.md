# One-pass packed dictionary codes for nullable int columns

Status: **RULED 2026-10-17 (option D): build 3a-3c with ClickBench Q1/Q7 added as
wall targets; 3d and RLE-run counting decided after. 3a-3c BUILT and measured on ARM
and x86 (§9); keep/ship ruling pending.**
Source profile: 2026-10-16, samply + dSYM, `scratch.hits_part0` (one ClickBench
shard, 14,155,776 rows, 216 row groups, every column declared nullable with
`null_count = 0`). Query: 20 int columns, `GROUP BY OS`.

## 1. The measured problem

For a nullable, dictionary-encoded numeric column the decoder builds the packed
per-row code array (`DecodedColumn::dict_codes_array`, `code_width` bytes per row,
zero doubling as the null placeholder) in two passes per page:

| step | code | share of ALL work samples | share of reader self time |
|---|---|---|---|
| hybrid RLE/bit-packed stream → `int32` indices | `DecodeRLEBitPackedIndicesNoPrefix`, decode_encodings.cpp:444-497 (hot line 491: RLE-run `resize_fill`) | 5.0% | 18% |
| per-row scatter of those indices into the packed array | decode_column.cpp:2686-2690 (`page_all_present` branch) | 13.5% | 48% |

The scatter loop does, per row: load the int32 index, `code < 0 || code >= dict_size`,
`switch (code_width)` in `WritePackedCode`, one 1/2/4-byte store. The generated
code (0xb0bd4-0xb0c3c in pool_reader.so) reloads `code_width`, the codes pointer
and `dict_size` from the stack every iteration — the enclosing function is
~9,900 instructions and the compiler does not keep them in registers.

Upstream of both, the whole array is zero-filled once up front
(decode_column.cpp:1755, `resize_fill(total_needed * code_width, 0)`), then every
slot of an all-present page is written again by the scatter.

## 2. The packed-code contract (unchanged by this proposal)

- `code_width ∈ {1, 2, 4}` bytes per row, chosen once from the dictionary page
  (`CodeWidthForDictSize`: ≤256 → 1, ≤65,536 → 2, else 4) and frozen for the chunk.
- Positional: row `i` lives at byte `i * code_width`; a null row holds 0.
- Consumers read it as is: `expand_packed_codes` (io_pipeline.hpp:881) widens to the
  `uint32` selection of the emitted dict-shaped DrakenVector; `ipc_serialize.hpp`
  writes `num_rows * code_width` bytes; the standalone reader's `_read_code`
  (parquet_reader.pxi:833); `MaterializeDictPrefixToDense` reads it via
  `ReadPackedCode` on a dict→PLAIN spill.

Nothing downstream changes. The emitted vector has the same shape (dictionary +
`uint32` codes), so this is decoder internals, not §11 shape dispatch — **the
architect should confirm that reading.**

## 3. Proposal

### 3a. A width-targeted hybrid decoder

```
// Decodes `num_values` codes of the RLE/bit-packed hybrid stream straight into
// `dst` at `code_width` bytes per value. Returns values decoded (-1 on a short
// stream). *max_code is the largest code written: ONE bounds check per page.
int32_t DecodeRLEBitPackedCodesPacked(const uint8_t* data, size_t size,
                                      int32_t num_values, int bit_width,
                                      uint8_t* dst, uint8_t code_width,
                                      uint32_t* max_code);
```

- **Bit-packed groups** (8 values, `bit_width` bytes per group for widths ≤ 8):
  unpack with the existing NEON / AVX2 group unpackers (`unpack_group_8_neon`,
  `unpack_bitpacked_groups_avx2`), then narrow-store instead of storing `int32x4`:
  `code_width 1` → `vqmovn`/`_mm256_cvtepi32_epi8`-style 8-byte store,
  `code_width 2` → 16-byte store, `code_width 4` → today's store. Bit widths 9-16
  already have a NEON wide path; widths > 16 only ever land in `code_width 4`.
  The running max is a vector max over the unpacked lanes (codes are non-negative).
- **RLE runs** (one value, `count` repeats): `memset` for width 1, a 2/4-byte
  vector fill otherwise, exactly what `resize_fill` does today minus the int32
  intermediate and the later narrowing. The run value feeds `max_code` once.
- `bit_width == 0` (single-entry dictionary): `memset(dst, 0, n * code_width)`.
- Scalar fallbacks mirror `unpack_bitpacked_groups_scalar`; RISC-V stays scalar.

### 3b. Call site: the all-present page writes in place

In the `page_uses_dictionary` dense path (decode_column.cpp ~2660), when
`!result.dict_codes_array.empty() && page_all_present`:

```
uint8_t* dst = result.dict_codes_array.data() + (size_t)total_collected * code_width;
uint32_t max_code = 0;
int32_t decoded = DecodeRLEBitPackedCodesPacked(data_ptr, data_size, present_count,
                                                bit_width, dst, code_width, &max_code);
if (decoded != present_count || max_code >= (uint32_t)dict_size) {
  result.error_message = ...;   // carries a reason (see §5)
  return;
}
```

No `_page_dict_indices`, no scatter, no per-row switch. A code can never be
negative (the stream is unsigned) and can never exceed `2^bit_width - 1`, so the
one `max_code >= dict_size` compare per page is the whole bounds check.

### 3c. Pages WITH nulls keep today's path

When `def_levels` were expanded (the page has at least one null), the value stream
is compact (present values only) and must be scattered through the def levels.
That path (decode_column.cpp:2700-2710) is left exactly as it is. It is not on the
measured profile (the fixtures carry no nulls) and it is the path where a mistake
produces a wrong answer, so it earns no change in this step.

### 3d. The up-front zero fill (optional second step)

With 3b every slot of an all-present page is written positionally, so the
`resize_fill(…, 0)` at 1755 is a redundant memset for those pages (1 byte/row for
`code_width 1`: small, but a full pass). Second step, after 3a-3c measure: replace
it with `resize_uninit` and zero only the rows of pages that take the null path
(before their scatter). Not part of the first A/B.

## 4. Interactions — what this does NOT touch

| path | why it is outside this change |
|---|---|
| `sel_mode` / row mask (selective decode) | the packed array is never allocated under a mask (condition at 1740-1752: `row_mask == nullptr`); masked columns use `dict_indices` + `DecodeRLEBitPackedIndicesSelected` |
| page search (LIKE on raw pages) | `byte_array` only |
| nullable `byte_array` dict columns | use `dict_indices` (present-only) + a null walk in `build_direct_string_dict`; same two-pass smell, different code — a separate proposal |
| non-nullable ints with `prefer_dict` | `rle_path` off → `dict_indices` (int32) → builder widening loop; adjacent, not in scope |
| `ext_*` zero-copy buffers | the packed array is only allocated when no ext buffer is supplied |
| dict → PLAIN spill mid-chunk | `MaterializeDictPrefixToDense` reads the same format |
| standalone rugo (`parquet_reader.pxi`) | reads the same format via `_read_code` |
| `rle_path` (non-nullable) | unaffected; it never builds the packed array |

## 5. Fail-loud

Today the scatter's bounds failure is a bare `return` (on the silent-failure
worklist, docs/… `parquet_decode_silent_failures_list`: "dict index decode/code
range ~2322-2455"). The new site sets `error_message` with the page, the offending
`max_code` and `dict_size`, so a corrupt stream is refused with a reason rather than
reported as "column absent".

## 6. Expected effect — stated before building

From the profile of the 20-int-column query (shares of all work samples):

| | today | after 3a-3c (estimate) |
|---|---|---|
| RLE fill + scatter | 18.5% | ~4-5% (one streaming pass at the narrow width, no per-row branch) |
| reader self time | 28% of query CPU | ~14-15% |
| query CPU | 100% | ~86% |

**Wall time on that query: expect ≈ 0.** The reader's self time was 108 ms summed
over 16 workers against 72 ms of wall, i.e. the reader is not on the critical path;
the grouped aggregate is. The dictionary-copy A/B of the same day showed exactly
this: a copy worth 10.7% of work samples moved wall by ≤ 1%, failed the keep rule
below, and was reverted. The gain here is CPU
(what a loaded Cloud Run cell pays), and wall only on shapes where int decode IS
the critical path (narrow int scans with trivial consumers). Anyone building this
must therefore measure **process CPU and reader self time**, interleaved ABBA on
`scratch.hits_part0`, and separately on x86 (the narrow stores are AVX2 code).
A build that shows no CPU reduction is deleted.

## 7. Verification plan (if ruled)

1. Baseline first (wall, process CPU, Parquet Read self time) on: the 20-int
   `GROUP BY OS` query, `SELECT OS, MIN(WatchID), MAX(UserID) … GROUP BY OS`,
   `time_cols`, and one nullable-with-nulls fixture (testdata) for the untouched path.
2. Result equality against the baseline build on every query (hash of all values).
3. Interleaved ABBA, ≥ 4 rounds, both arms as separate `.so` builds.
4. Keep rule, fixed before the first number: keep only if at least one target
   shape reaches a median wall OR process-CPU ratio (new/baseline) of ≤ 0.98 with
   the new build winning ≥ 10 of 12 paired runs, and no measured shape is worse
   than 1.00. Otherwise the whole change is reverted — a neutral result is not kept.
5. `make q`, then `make slt`.
6. x86 run of the same before merging (ARCHPERF: perf is measured per arch); the
   keep rule applies on both architectures.

## 8. Decision requested

- Confirm §2: a decoder-internal change to how the packed array is produced, same
  emitted shape — not a §11 shape specialisation needing its own ruling.
- Go / no-go on 3a-3c given §6: a CPU win with no single-query wall win on the
  profiled shape.

## 9. As built (2026-10-17, ARM)

- `DecodeRLEBitPackedCodesPacked` (decode_encodings.cpp/.hpp). Bit-packed groups
  unpack through the existing per-arch group unpackers into a 512-value stack tile
  (L1), then narrow-store at the code width with a running max; RLE runs are a
  `fill_n` at the code width; `bit_width == 0` is a memset. The code width is a
  template parameter, so neither the store nor the max carries a per-value branch.
  This is §3a with one stated difference: the int32 lanes survive only in the tile,
  never in a page-sized buffer.
- Call site (decode_column.cpp, dense dict path): taken when the packed array is in
  use, the page is a dictionary page, `page_all_present`, and `page_vsel == nullptr`.
  Every failure sets `error_message` (count mismatch, page past the chunk, short
  stream, code outside the dictionary). Pages with nulls and selective decode keep
  the two-pass path unchanged (§3c); its bare returns are still on the silent-failure
  worklist. 3d not built.
- Test: `make packed-codes-test` (in `make q`), ASan/UBSan, bit widths 0-32 x code
  widths 1/2/4, mixed RLE/bit-packed streams, cut and halved streams, a bit-packed
  run one byte short, invalid widths; checked against the stream's own values and
  against the two-pass path.
- Found, not fixed (outside scope): `DecodeRLEBitPackedIndicesNoPrefix` masks an RLE
  value with `(1U << bit_width) - 1`, an undefined shift at `bit_width == 32`
  (decode_encodings.cpp ~487; only reachable with a dictionary over 2^31 entries).

### 9.1 Measured (Mac, interleaved, A/B order alternating, both .so swapped)

12 rounds x 8 reps (B/A median of per-round minima, wins out of 12). Result hashes
identical on every query except q41, whose answer is nondeterministic within ONE arm
(12 distinct hashes in A alone: ties at `OFFSET 10000`).

| shape | wall | CPU |
|---|---|---|
| 20 int columns `GROUP BY OS` (hits_part0) | 0.771 (12/12) | 0.736 (12/12) |
| `OS, MIN(WatchID), MAX(UserID) GROUP BY OS` | 0.897 (12/12) | 0.872 (12/12) |
| ClickBench Q1 | 0.840 (12/12) | 0.778 (12/12) |
| ClickBench Q7 | 0.836 (12/12) | 0.785 (12/12) |
| Q15 | 0.955 (12/12) | 0.953 (12/12) |
| Q36 / Q41 | 0.975 / 0.960 | 0.964 / 0.856 |

Guards, 16-round rerun (the 12-round run showed q12 1.007/1.009): q12 1.000 / 0.997,
q19 0.996 / 1.005, multi-page nullable fixture with pages with and without nulls in
one chunk 1.011 wall (7/16; best-of B 2.01 ms vs A 2.13 ms) / 0.951 CPU (15/16),
second fixture query 0.988 / 0.913. The §7 keep rule's "no shape worse than 1.00"
is met by every shape except the 2 ms fixture query's median wall (1.011) — reported
for the ruling, not waived. `make q` + `make slt` green. x86: §9.2.

### 9.2 Measured on x86 (2026-10-17, i5-8500 6c, Debian 12, GCC 12 build, -march=haswell)

Isolated tree (`dev/arch_tree.sh`), arms = the same tree with only decode_column.cpp
reverted, both .so swapped, 12 rounds x 8 reps, order alternating. `packed_codes_test`
passes on this box under clang 14 AND GCC 12 (ASan/UBSan, AVX2 unpackers). Result hashes
identical except q41 (nondeterministic within each arm, as on ARM).

| shape | wall | CPU |
|---|---|---|
| 20 int columns `GROUP BY OS` | 0.808 (12/12) | 0.673 (12/12) |
| `OS, MIN(WatchID), MAX(UserID) GROUP BY OS` | 0.833 (12/12) | 0.793 (12/12) |
| ClickBench Q1 | 0.743 (12/12) | 0.723 (12/12) |
| ClickBench Q7 | 0.743 (12/12) | 0.723 (12/12) |
| Q15 | 0.980 (11/12) | 0.933 (12/12) |
| Q36 / Q41 | 0.979 / 0.937 | 0.969 / 0.787 |
| null-page fixture (2 queries) | 0.963 / 0.962 (12/12) | 0.920 / 0.912 (12/12) |
| q12 (strings, path untouched) | 1.001 (5/12) | 0.999 (8/12) |
| q19 | 0.995 | 0.997 |

The fixture shape that read 1.011 on ARM is a clear win here. The one median above 1.00
is q12 wall at 1.001 with CPU 0.999 — a string-only query this change does not reach,
the same reading as ARM's 16-round 1.000. Reported for the ruling.

### 9.3 Step 3d — built, measured, REVERTED (2026-10-17, ARM)

Built as specified: `dict_codes_array` allocated with `resize_uninit`, each page that
takes the null path zeroing its own slice (with a bounds check) before the scatter.
Safety was proven before measuring: a poison build (fill `0xA5`) kept every result hash,
and a temporary end-of-chunk check that every null row holds code 0 fired with the
per-page `memset` removed (negative control) and stayed silent with it in place across
the harness queries, `make q` and `make slt`. Result hashes identical in the A/B.

12 rounds vs the kept one-pass build: int20 0.994 (8/12), `OS, MIN(WatchID)…` 0.974
(9/12), Q1 1.002 (6/12), Q7 1.006 (5/12), q12 1.011, others 0.96-1.00. No target
reached 0.98 with >= 10/12 wins, so by the §7 keep rule it was reverted (the zero fill
it removes is ~1 byte/row in one streaming memset — too small to see). The tree is back
to the §9.1 state; `make q` + `make slt` green after the revert. Not run on x86.
