# One-pass packed dictionary codes for nullable int columns

Status: **PROPOSAL — not built. Awaiting the architect's ruling.**
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
