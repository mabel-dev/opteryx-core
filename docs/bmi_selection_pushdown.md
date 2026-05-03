# BMI Selection Pushdown Design

**Based on:** "Selection Pushdown in Column Stores using Bit Manipulation Instructions"  
Li, Lu, Chandramouli — *Proc. ACM Manag. Data*, Vol. 1, No. 2, Article 178 (June 2023)  
https://doi.org/10.1145/3589323

---

## Problem Statement

When Opteryx evaluates a multi-predicate scan query, each column follows the same pipeline:
**decode all values → evaluate predicate → build selection indices → take**

This is wasteful. Once a first predicate has filtered rows to, say, 5% selectivity, the decode
cost of subsequent columns is still paid for 100% of values. Decoding dominates scan time
even with SIMD-accelerated bitpacking (Figure 1 of the paper demonstrates this clearly for
TPC-H Q6: decoding accounts for ~60% of total query time even after SIMD optimisation).

The paper proposes eliminating most of that decoding cost using two x86 BMI instructions —
`PEXT` (parallel bit extract) and `PDEP` (parallel bit deposit) — to push selection down to
the encoded representation. The result is that only the rows that survive all prior predicates
are ever decoded from subsequent columns.

---

## Background: PEXT and PDEP

The BMI2 instruction set (Intel Haswell 2013, AMD Zen 2017 microcode / Zen 3 hardware)
provides two instructions with no efficient software equivalent:

```
PEXT(src, mask) → dest
  For each set bit in mask, extract the corresponding bit from src
  and pack them into the low-order bits of dest.

PDEP(src, mask) → dest
  For each set bit in mask, deposit the next low-order bit from src
  into that position of dest.
```

A naïve software loop achieves ~8–18 M ops/s; hardware PEXT/PDEP run at ~1100–1700 M ops/s
— a 90–140× gap (Li et al., Table 1). This gap is what makes the techniques in the paper
viable, and what makes them impossible to replicate efficiently on ARM (Apple Silicon / AWS
Graviton have no equivalent instructions).

---

## Current Opteryx Architecture

### Rugo Parquet decode path

`rugo/src/parquet/decode_column.cpp` decodes columns in three phases:

1. **PreScanPages** (line 158): scans `row_mask` word-at-a-time to mark entire pages for
   skipping. This is the only filter-before-decode optimisation currently present.
2. **Decode loop** (line 627+): for each non-skipped page, decompresses and decodes the
   full dictionary-index stream via `DecodeRLEBitPackedIndices()`, then gathers actual
   values via `gather_int32_avx2()`.
3. **Post-loop row-mask filter** (line 1693): after all pages are decoded, applies
   `decoded_row_mask` row-by-row (`for (uint8_t m : decoded_row_mask)`) to compact the
   output. At this point decoding cost has already been paid in full.

The page-level skip in phase 1 is effective when entire pages fall outside the selection.
Within a page, decoding is unconditional — all values are decoded regardless of the
selection bitmap.

### Draken filter path

`draken/morsels/morsel.pyx` applies filters after decoding:

1. Expression evaluation on decoded values produces a `BoolVector` mask.
2. `filter_mask()` scans the bit-packed mask, building an `int32_t` indices array.
3. `_take_inplace()` uses the indices array to rewrite every column in the morsel.

There is no mechanism to carry a partially-accumulated selection bitmap back into the
decode layer for a subsequent column.

### Predicate ordering

`opteryx/planner/optimizer/strategies/predicate_ordering.py` orders filters by a cost
model combining data-type cost estimates with default selectivity values. It does not
account for the encoded bit-width of dictionary columns, which matters because — as shown
in Li et al. Section 4.4 — the cost of the select operator scales with bit-width even
when selectivity is held constant.

---

## Proposed Changes

Three independent improvements, in ascending implementation complexity.

---

### Technique 1 — Filter ordering: add encoded bit-width term

**Paper reference:** Section 4.4, Equation 1.

The paper's cost model for a sequence of filters is:

```
cost = Σ (bit_width_i / word_size) + Σ Π selectivity_j   (j < i)
```

The first term reflects that the BMI-based select operator cost scales linearly with
bit-width; the second term is the cumulative selectivity reduction. The insight is that
a highly selective but wide-coded column may not be the best first filter.

**Current gap:** `_base_cost()` in `predicate_ordering.py` uses only data type and
operator, with no knowledge of cardinality or dictionary bit-width. The `selectivity`
term uses static defaults (Eq=0.1, comparison=0.5) regardless of column statistics.

**Change:** Extend `_base_cost()` to accept an optional `encoded_bit_width` parameter
sourced from Parquet row-group metadata (available in `rugo/src/parquet/metadata.cpp`).
The scan operator already reads column chunk metadata; the dictionary bit-width for each
column is the `ceil(log2(num_distinct_values))` field present in the column chunk
descriptor. Pass this into the optimiser when the scan is over a Parquet source.

This is a pure Python change in the planner; it does not touch any compiled code.

**Expected benefit:** Correct ordering when a highly selective predicate targets a wide
dictionary (e.g., 16-bit timestamp codes) competing with a less selective but narrow
predicate (e.g., 3-bit status code). Currently both get the same cost estimate; with
this change the narrow column is correctly favoured as a later filter even at lower
selectivity.

---

### Technique 2 — PDEP bitmap transform in Draken filter chain

**Paper reference:** Section 4.3 (Bitmap Transform Operator).

When a second predicate is evaluated on the values selected by a first predicate, the
result bitmap has one bit per *selected* value, not one bit per *total* row. To combine
it back into a position bitmap for the full morsel, the bits must be scattered into the
positions of the 1s in the prior bitmap. The paper shows this is a single `PDEP`
instruction:

```
bitmap_refined = PDEP(filter_result_bits, bitmap_prior)
```

**Current gap:** Draken rebuilds the indices array from scratch for each filter pass.
There is no mechanism to scatter per-selected-value results back into a position bitmap.
The post-filter index array and the pre-filter selection bitmap are never connected.

**Change:** Add a `scatter_bits_into_mask()` intrinsic to
`src/cpp/simd_bitops.h` with two implementations:

```cpp
// x86 — BMI2
#ifdef __BMI2__
inline uint64_t scatter_bits_into_mask(uint64_t src, uint64_t mask) {
    return _pdep_u64(src, mask);
}
#else
// ARM / scalar fallback — iterate over set bits of mask
inline uint64_t scatter_bits_into_mask(uint64_t src, uint64_t mask) {
    uint64_t dest = 0, bit = 0;
    while (mask) {
        uint64_t lsb = mask & (-mask);
        if ((src >> bit) & 1) dest |= lsb;
        mask &= mask - 1;
        ++bit;
    }
    return dest;
}
#endif
```

Wire this into `filter_mask()` in `morsel.pyx` so that when a second predicate runs on
an already-masked morsel, the result is merged back via `scatter_bits_into_mask` rather
than rebuilding the index array from the full row range.

The ARM fallback is O(set bits) not O(64), so correctness is preserved everywhere; the
performance gain is x86-only.

**Expected benefit:** For a two-predicate scan at 10% + 50% selectivity, the current
path builds an index array of 10% of rows then scans it again to apply the second
predicate. With PDEP-based merge the second bitmap refinement is 1 instruction per 64
rows over the already-sparse bitmap. Draken profiling (`rugo_draken_python_audit.md`)
flags the Python-layer bitmap manipulation as a bottleneck; this addresses part of it
in the compiled path.

---

### Technique 3 — PEXT-based intra-page selection pushdown in Rugo

**Paper reference:** Sections 3 and 4 (Bit-Parallel Select Operator; Selection Pushdown
Framework).

This is the highest-value change and the most invasive.

**Core algorithm** (Li et al., Algorithm 1 and 2):

Given a vector of `w`-bit values packed into 64-bit words and a selection bitmap, extract
only the selected values using four instructions per word regardless of selectivity or
bit-width:

```
mask  = 0^(w-1)1 ... 0^(w-1)1        // one 1 per w-bit field, rightmost position
low   = PDEP(bitmap, mask)
high  = PDEP(bitmap, mask - 1)
ext   = high - low                    // extended bitmap: w bits per selected entry
out   = PEXT(values, ext)             // extract selected bits, pack to output
```

For non-power-of-two bit widths (Parquet's common case — 3-bit, 5-bit, 6-bit codes),
the mask is shifted per-word to align with the bit-boundary layout (Algorithm 3).
Masks are precomputed once per `(word_size, bit_width)` pair and reused.

**Change in Rugo:**

The post-loop row-mask filter at `decode_column.cpp:1693` currently decodes all values
then applies a byte-level mask. The change moves selection into the decode loop itself:

1. **Before the decode loop:** if `row_mask != nullptr`, compute per-page bit-width and
   precompute the word masks using Algorithm 3.
2. **Inside `DecodeRLEBitPackedIndices()`:** for each 64-bit word of bit-packed codes,
   read the next 64 bits of the row-selection bitmap and apply the PEXT-based select
   operator. Output only the surviving codes into the output buffer.
3. **Dictionary gather:** `gather_int32_avx2()` then runs over the compacted index buffer
   — a fraction of the original size.
4. **Remove** the post-loop row-mask filter (lines 1693–1789) for this path; it becomes
   redundant.

For RLE runs the approach differs: a selected RLE run `(value, count)` emits only
`popcount(bitmap_slice)` copies of `value`. This uses the existing POPCNT path and
requires no BMI.

**Platform guard:** wrap all PEXT/PDEP paths under `#ifdef __BMI2__`. The scalar
fallback decodes then filters as today. The ARM dev build hits the fallback; the x86
production build (GCP Cloud Run) gets the fast path. This matches the existing pattern
in `simd_gather.hpp` and `simd_bitops.h` where AVX2 paths are guarded with
`#ifdef __AVX2__`.

**Expected benefit:** Consistent with Li et al. Figure 12 and Figure 13, the speedup is
largest when selectivity is low and values are narrow. For a typical Opteryx workload —
scanning Parquet files with dictionary-encoded string or integer columns at 5–20%
selectivity after a leading filter — the paper's micro-benchmark results (Figure 13b)
project a 3–6× improvement on the decode+select step for the second and subsequent
columns in a multi-predicate scan. For repeated or nullable columns (common in nested
event schemas) the improvement reaches 13–21× (Figure 14b, 14c) because the level
decoding and bitmap transformation also benefit from BMI.

---

## Impact Summary

| Change | Files touched | Platform benefit | Complexity |
|--------|--------------|-----------------|------------|
| Filter ordering bit-width term | `predicate_ordering.py`, `metadata.cpp` binding | x86 + ARM (logical, no ISA dep.) | Low |
| PDEP bitmap scatter in Draken | `simd_bitops.h`, `morsel.pyx` | x86 fast path, ARM scalar fallback | Medium |
| PEXT intra-page select in Rugo | `decode_encodings.cpp`, `decode_column.cpp` | x86 fast path, ARM scalar fallback | High |

The three changes are independent. Technique 1 can ship without 2 or 3. Technique 2
has value on its own even without the Rugo layer change. Technique 3 delivers the largest
absolute gain but requires the most careful integration with Rugo's page/run decoding
state machine.

---

## What This Does Not Cover

- **Repeated field bitmap transformation** (paper Section 5.4.1): extending the selection
  bitmap from record granularity to level granularity using the `extend` operator. Rugo
  handles repeated fields via a different path; this would be a fourth, separate change.

- **Parquet-format changes:** all techniques operate on the existing Parquet encoding
  without modifying the on-disk format, consistent with Li et al. Section 6.

- **ARM equivalents:** the paper explicitly notes ARM processors lack PEXT/PDEP (Section
  2.1.1). No ARM equivalent exists today. The scalar fallbacks preserve correctness at
  the cost of not improving over the current implementation on the dev platform.

---

## References

Li, Y., Lu, J., & Chandramouli, B. (2023). Selection Pushdown in Column Stores using Bit
Manipulation Instructions. *Proc. ACM Manag. Data*, 1(2), Article 178.
https://doi.org/10.1145/3589323

Willhalm, T., Popovici, N., Boshmaf, Y., Plattner, H., Zeier, A., & Schaffner, J. (2009).
SIMD-Scan: Ultra Fast in-Memory Table Scan using on-Chip Vector Processing Units.
*Proc. VLDB Endow.*, 2(1), 385–394. (baseline decoding technique used in Parquet and in
Rugo's current `DecodeRLEBitPackedIndices()` implementation)
