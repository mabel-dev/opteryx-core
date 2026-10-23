# Rugo reader: DELTA_LENGTH_BYTE_ARRAY (6), BYTE_STREAM_SPLIT (9), ALP (10)

Status: BUILT 2026-10-23 (reader 6/9/10 + opt-in ALP writer), `make q` and
`make slt` green; see §8. §1–§7 are the original scoping note. Date: 2026-10-23.

Provenance tags: **[C]** read from code; **[S]** read from the parquet-format
spec; **[H]** hypothesis, not measured.

---

## 1. Why

Rugo refuses all three encodings by name today: "unsupported page encoding N"
from `PageValueEncodingError` (`rugo/src/parquet/decode_column.cpp:409`) **[C]**.
The refusal is honest, but it means any file that uses one of these encodings
can't be read at all.

| id | Encoding | Spec status | Types | Who writes it |
|---|---|---|---|---|
| 6 | DELTA_LENGTH_BYTE_ARRAY | Standard | BYTE_ARRAY | Opt-in in pyarrow (`column_encoding`); parquet-java V2 writers |
| 9 | BYTE_STREAM_SPLIT | Standard | INT32, INT64, FLOAT, DOUBLE, FLBA | Opt-in in pyarrow (`use_byte_stream_split`); common for float columns in ML/telemetry pipelines |
| 10 | ALP | **Preview** since 2026-08-01 **[S]** | FLOAT, DOUBLE | Opt-in in arrow-rs 60 and Arrow C++ (merged 2026-10-13); **not** in pyarrow 25.0.1 (`Unsupported column encoding: 'ALP'`) |

The case for 6 and 9 is that they are standard: a reader that claims to read
Parquet should read them. The case for 10 is that we'd be ready ahead of its
adoption. We gain no speed on our own data, because rugo doesn't write any of
these three and this note doesn't change the writer.

---

## 2. Where they plug in

Every decode site that has to change **[C]**:

| Site | Location | Change |
|---|---|---|
| File admission | `CanDecode` (`decode.cpp:60`) | admit 6/9/10 |
| Chunk admission | `has_supported_encoding` (`decode_column.cpp:1014`) plus its error-message list | admit 6/9/10 |
| Page admission | `PageValueEncodingError` (`decode_column.cpp:409`), shared by both walks | admit 6/9/10, **type-aware** (see §3) |
| Encoding name | `metadata.cpp:125` | add `10 → "ALP"` |
| Tier-3 parallel worker | `decode_column.cpp:~2085` (int32/int64/float32/float64, non-null, no dict) | branch for 9 and 10 alongside the existing `delta` branch |
| Sequential fixed-width walk | `decode_column.cpp:~3260–3440` | branch for 9 (all fixed widths including FLBA) and 10 (float32/float64) |
| Sequential BYTE_ARRAY walk | `decode_column.cpp:~3527` | branch for 6, next to the existing `page_encoding == 7` |
| Page search (LIKE on raw pages) | `decode_column.cpp:~2550` | no change at first: 6 falls through and is counted, as 7 is |
| Dictionary-skip soundness guard | `decode_column.cpp:1670` | no change: any page that isn't a dictionary page already disables the skip |

The same list of admitted encodings is hand-copied in three places (file,
chunk, page). Adding three more ids to three copies is how they drift: the
`decode.cpp:50` comment records exactly that drift happening once already.
That's decision D1.

What stays the same for all three:
- Values are encoded only for the non-null rows of a page, as with PLAIN.
  The existing scatter of nulls by definition level applies unchanged.
- `ResolvePageValuesRegion` already finds where the values start in V1 and V2
  pages. ALP's offsets are measured from that point, after decompression and
  after the levels **[S]**.
- Selective decode keeps its current shape: fixed-width pages decode the whole
  page and then compact to the selection (`compact_page_tail`). For strings,
  unselected values are skipped as they are read.
- Output is an ordinary dense draken vector. No shape-specific path is needed
  (§11).

---

## 3. Prerequisite: page admission has to check the column type

There's an open report from 2026-10-10: `PageValueEncodingError` looks only at
the encoding id, not the column type. So a float page marked
DELTA_BINARY_PACKED, or an int page marked DELTA_BYTE_ARRAY, gets read as
PLAIN and returns garbage with no error.

All three new encodings are only valid for some types. Admitting them through
a gate that ignores type would widen that hole: BYTE_STREAM_SPLIT on a
BYTE_ARRAY column, or ALP on an INT64 column, would be decoded as something
else. The gate needs the physical type, and should refuse any (encoding, type)
pair the spec doesn't allow, naming both. This fixes the open report too,
which is why it needs your approval (D2).

---

## 4. Per-encoding design

### 4.1 DELTA_LENGTH_BYTE_ARRAY (6)

Layout **[S]**: a DELTA_BINARY_PACKED stream of `n` lengths, followed by all
`n` values' bytes back to back.

- This is the second half of DELTA_BYTE_ARRAY, which rugo already decodes
  (`DecodeDeltaByteArray`, `decode_encodings.cpp:1074`). The length decoding
  and `SkipDeltaBinaryPacked` are reused as-is.
- The difference: there is no prefix to rebuild, so the bytes are already
  laid out the way the string arena stores them. Decode the lengths, check
  that they sum to no more than the remaining bytes, then append. With no
  selection, one bulk copy plus an offsets prefix sum. With a selection, one
  copy per selected value.
- Not built: a `std::vector<std::string>` per page, the way 7 does it. Path 7
  is slow because front-coding forces it; path 6 has no such reason.
- Dictionary re-derivation (`rederive`, `decode_column.cpp:3515`): treat 6 like
  PLAIN, since its bytes can be interned. Decision D3.
- Errors: a lengths stream that comes up short, a negative length, or lengths
  adding up to more bytes than the page holds all refuse the page with a
  message, never a bare `return`.

### 4.2 BYTE_STREAM_SPLIT (9)

Layout **[S]**: for values `k` bytes wide, `k` streams of `n` bytes each.
Stream `j` holds byte `j` of every value.

- Decoding is a byte transpose: `out[i*k + j] = in[j*n + i]`. Widths are
  4 (INT32, FLOAT), 8 (INT64, DOUBLE), and any FLBA width.
- For FLBA, transpose into the usual `k`-byte rows and then feed the existing
  big-endian FLBA reads (DECIMAL to int64/int128, plus float16 and UUID). The
  big-endian conversion code stays as it is.
- Kernel: start with one scalar transpose, then specialise widths 4 and 8 with
  NEON (`vld4`/`vst4`-style interleave) and AVX2 shuffles. Per §3, take a baseline
  of the scalar version first; the SIMD versions stay only if
  they measure faster.
- Tier 3: a worker branch for 9 that transposes the decompressed page straight
  into `dst + off`, with no intermediate buffer.
- The page-size check (`FixedWidthPageTooShort`) still applies: the page must
  hold `n*k` bytes.
- The tests in `tests/unit/connectors/parquet_io/test_parallel_page_validation.py`
  currently assert that BSS is **refused**, for V1 and V2. They become
  round-trip tests for the same files. Those files are the exact shape that
  once returned silent zeros, so they're the right guard to keep.

### 4.3 ALP (10)

Layout **[S]**:
- A 7-byte page header: `compression_mode`, `integer_encoding`,
  `log_vector_size`, `num_elements`.
- A uint32 offset per vector.
- Each vector: `AlpInfo` (exponent, factor, num_exceptions), `ForInfo`
  (frame of reference, bit width), the bit-packed deltas, the exception
  positions, and the exception values.

Decoding:
1. Unpack the `bit_width`-bit deltas.
2. Add the frame of reference, with wrap-around.
3. Multiply by `10^factor`, then by `10^-exponent`, as two separate multiplies
   using correctly rounded constants.
4. Overwrite the exception positions with their stored values.

- **Validate the header up front.** Refuse by name if `compression_mode != 0`
  (ALP-RD is reserved, not specified), `integer_encoding != 0`,
  `log_vector_size` is outside [3, 15], `exponent`/`factor`/`bit_width` are
  outside their per-type ranges, `num_elements` doesn't match the page's count
  of non-null values, or any offset or exception position is out of bounds.
  A Preview spec will grow new modes, and we must refuse them, not misread
  them.
- **Bit-unpacking:** the order is least-significant-bit first, the same as the
  RLE hybrid and DELTA_BINARY_PACKED. The DELTA_BINARY_PACKED unpacker is
  written inline inside the `DecodeDeltaBinaryPacked` template. Moving it out
  into a shared width-specialised function (widths 0–64) lets ALP reuse it, at
  the cost of touching a hot decoder. Decision D4.
- **Bit-exact output is mandatory.** Use two lookup tables of correctly
  rounded constants (`10^0..10^18` and `10^-0..10^-18`, one pair for float and
  one for double), never `pow()`. Patch exceptions in by raw bit pattern, so
  NaN payloads and -0.0 come through untouched.
- **Kernel:** steps 1–3 are a straight vectorisable loop over each vector
  (1024 values by default). Step 4 is a scatter of
  `num_exceptions` values, usually small. Write the scalar version first;
  SIMD only if it measures faster.
- **Tier 3:** a worker branch for 10 that decodes straight into
  `dst + off`.
- **Not in scope:** filtering in the integer domain before the multiply
  (a pushed-down comparison on ALP integers). That's a shape-specific fast
  path and needs its own §11 approval.

---

## 5. Test fixtures

| Encoding | Fixture source |
|---|---|
| 6 | pyarrow `column_encoding={'s': 'DELTA_LENGTH_BYTE_ARRAY'}` in `tests/`: nullable and non-nullable, empty strings, V1 and V2, multi-page |
| 9 | pyarrow `use_byte_stream_split` across int32/int64/float/double/FLBA decimal: nullable, non-nullable (Tier 3), multi-page, V1 and V2 |
| 10 | **Open (D5).** pyarrow 25.0.1 can't write ALP. Options: (a) check apache/parquet-testing for ALP files and vendor them under `tests/`; (b) a `dev/` generator built on arrow-rs 60 that commits its output files; (c) wait for a pyarrow release with the Arrow C++ writer |

ALP's checks are bit-for-bit comparisons, not `==`. The fixtures must include
NaN with a payload, ±Inf, -0.0, vectors made entirely of exceptions, a short
final vector, `bit_width = 0` (constant vectors), and malformed headers for
each refusal in §4.3.

New tests build their values in plain Python, without NumPy. Running
`make q` then `make slt` signs the work off.

---

## 6. Decisions for the architect

- **D1** Merge the three hand-copied lists of admitted encodings into one
  shared predicate. Recommended: yes. Otherwise this change touches three
  lists that already drifted once.
- **D2** Make page admission check the column type as part of this work. That
  fixes the 2026-10-10 open report. Recommended: yes, as a precondition (§3).
- **D3** Encoding 6 joins dictionary re-derivation like PLAIN. Recommended:
  yes, since nothing about 6 rules it out the way front-coding rules out 7.
- **D4** Move the DELTA_BINARY_PACKED bit-unpacker into a shared function so
  ALP can reuse it, versus writing a separate ALP unpacker. Recommended: move
  it out, with a before/after benchmark of DELTA_BINARY_PACKED decode, and
  revert if it gets slower.
- **D5** Where the ALP fixtures come from (§5).
- **D6** Order of work. Recommended: D1+D2 first, then 9 (smallest, and
  already has a guard test), then 6, then 10. Alternative: drop 10 until a
  pyarrow release can write it, so the tests don't depend on a non-Python
  generator.

---

## 7. Size estimate [H]

- 9: one transpose kernel plus about four call-site branches.
- 6: a short decoder built from existing pieces, plus one branch.
- 10: a new decoder (header, unpack, multiply, patch) with the strictest
  validation of the three, plus the fixture question.

Of the three, only ALP adds a genuinely new algorithm to rugo.

---

## 8. What was built (2026-10-23)

Rulings: D1 do all, D2 do it, D4 share the unpacker, D5 rugo writes the ALP
fixtures (and ALP writing is a feature: platform workloads, not just tests),
D6 order as recommended. **D3 is still open** (see below).

- **One admission rule.** `ValueEncodingRefusal(encoding, physical_type)` in
  `decode_page.hpp` is asked by `CanDecode`, the chunk guard and both page
  walks. It checks the column type, which also closes the 2026-10-10 report
  (float + DELTA_BINARY_PACKED, int + DELTA_BYTE_ARRAY, FLBA + DELTA_BYTE_ARRAY
  were read as PLAIN garbage). FLBA + DELTA_BYTE_ARRAY is now refused by name;
  it is spec-legal but not decoded.
- **Reader.** `DecodeByteStreamSplit`, `DecodeAlp` and
  `DecodeDeltaLengthByteArray` (`decode_encodings.cpp`) decode into PLAIN
  layout. The parallel walk decodes 9/10 straight into the output slot. The
  sequential walk decodes 6/9/10 into page scratch, then the page's encoding
  becomes PLAIN, so page search, selective decode and dictionary re-derivation
  run unchanged. Cost: one extra copy of the page on the sequential walk, for
  these three encodings only.
- **Shared unpacker.** `ReadPackedLSB` (`decode_primitives.hpp`) serves
  DELTA_BINARY_PACKED and ALP. ⛔ It fixes a silent wrong answer: deltas of
  57–64 bits straddle nine bytes, and the old inline unpacker dropped the ninth
  byte (and shifted by 64 at width 64). Measured on the pre-change build: an
  int64 DELTA_BINARY_PACKED column of 60-bit spread decoded 4965 of 5000 rows
  wrong, with no error.
- **Writer.** An opt-in `alp=` option (False / True / column names) on
  `write_parquet`, `write_parquet_with_bounds`, `open_parquet_writer` and
  `write_parquet_stream`. It covers FLOAT/DOUBLE data pages, ARRAY leaves
  included. Dictionary pages stay PLAIN, and dictionary chunks are unchanged.
  The parameter search is the paper's two-level preset; vectors are 1024
  values.
- **Tests.** `tests/rugo/test_parquet_value_encodings.py` covers round trips
  on both walks, V1 and V2 pages, nulls, FLBA decimals and the engine path.
  ALP is checked bit-exact (NaN, ±Inf, -0.0, subnormals), plus the refusals.
  The BYTE_STREAM_SPLIT tests in `test_parallel_page_validation.py` now assert
  correct decoding instead of refusal.

### Measured ALP size (rugo, zstd on top, 20k values) [M, single run]

| data | ALP | PLAIN |
|---|---|---|
| float64 prices (2 dp) | 43,085 B | 63,096 B |
| float64 integral | 78,085 B | 99,514 B |
| float64 uniform random | 158,744 B | 150,371 B |
| float64 ±10^±20 magnitudes | 16,335 B | 9,311 B |

ALP wins on decimal-shaped data and loses on random or wide-magnitude doubles.
Those are ALP-RD's case, and ALP-RD is not in the spec. Per-column opt-in is
the right default.

### Interop

- Every installed third-party reader fails on an ALP file. pyarrow 25.0.1
  says "Unknown encoding type". DuckDB 1.5.4 and Polars 1.44.2 fail on the
  footer itself, so **the whole file** becomes unreadable to them, not just
  the ALP column.
- Our reader has only been checked against our own ALP writer. An independent
  check is available: apache/parquet-testing publishes
  `data/alp_extended.zstd.parquet` plus BYTE_STREAM_SPLIT and
  DELTA_LENGTH_BYTE_ARRAY reference files. Not downloaded yet; that needs the
  architect's OK.

### Open

- **D3.** A DELTA_LENGTH_BYTE_ARRAY page becomes PLAIN, so in a foreign
  dict-then-fallback chunk it is re-derived into the dictionary exactly like a
  PLAIN fallback page. That needs no code; refusing it would need a new
  condition. Re-derivation is about the chunk's cardinality, not the fallback
  page's encoding. DELTA_BYTE_ARRAY (7) is excluded only because it decodes
  through `std::string`.
- Not measured: the shared unpacker's effect on DELTA_BINARY_PACKED decode
  speed (it reads 8 bytes per value where the old loop read up to 9 one at a
  time), and SIMD for the BYTE_STREAM_SPLIT transpose. Both wait on a free
  machine.
