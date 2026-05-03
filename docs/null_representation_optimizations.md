# Null Representation Optimizations

**Status:** Proposal  
**Area:** Rugo (Parquet decode), Draken (vectorized execution)  
**Target platforms:** AVX2 (x86, production), NEON (ARM, dev)

---

## 1. Background

This document is grounded in the findings of:

> Zeng, X., Meng, R., Pavlo, A., McKinney, W., & Zhang, H. (2024). **NULLS! Revisiting Null Representation in Modern Columnar Formats.** *DaMoN '24*, June 2024, Santiago, Chile. ACM. https://doi.org/10.1145/3662010.3663452

The paper performs the first rigorous analysis of how null representation in columnar formats affects both compression ratio and decode speed. Its findings are directly applicable to Opteryx because Opteryx spans both sides of the boundary the paper identifies:

- **On disk**: Parquet uses the **Compact layout** — only non-null values are stored densely, with a separate definition-level bitmap indicating which rows are null.
- **In memory**: Draken uses the **Placeholder layout** — all rows are stored (nulls as zeros), with a separate validity bitmap. This matches Arrow's in-memory convention.

Every column decoded from Parquet therefore requires a **Compact-to-Placeholder (C⤏P) conversion**: the packed non-null values must be scattered to their correct logical positions in the Draken buffer, guided by the definition-level bitmap. The paper demonstrates that this scatter step is the dominant cost in Parquet decode pipelines under realistic null ratios (Figure 2 of the paper shows that decode time nearly doubles at 20% null ratio compared to the null-free case).

---

## 2. Current Implementation

### 2.1 Parquet → Draken null path (Rugo)

The Parquet decode pipeline in `rugo/src/parquet/decode_column.cpp` accumulates definition levels for an entire row group into `all_def_levels` (a `std::vector<int32_t>`), then calls `parquet_simd::build_validity_bitmap()` from `rugo/src/parquet/simd_validity_bitmap.hpp` to produce a packed validity bitmap.

The AVX2 path in `simd_validity_bitmap.hpp` processes **8 definition levels per iteration** using `_mm256_cmpeq_epi32`, which is efficient for bitmap construction. However, the scatter step — writing non-null Parquet values to their correct positions in the Draken buffer — is not addressed by this SIMD path. The bitmap is built; the scatter of values is handled separately, without the batching or SIMD acceleration the paper identifies as critical.

### 2.2 Draken vector null representation

Draken vectors (`DrakenFixedBuffer`, `DrakenVarBuffer`, `DictAccessor`, `DrakenRLEBuffer` — defined in `draken/core/buffers.pxd`) all carry an optional `null_bitmap` field (one bit per row, Arrow convention: 1 = valid, 0 = null). Null values in the data array are left as zero (unspecified, from allocation). There is no sentinel/special-value scheme.

SIMD bitmap operations (`simd_and_mask`, `simd_or_mask`, etc. in `src/cpp/simd_bitops.h`) handle AVX2/NEON dispatch for combining validity bitmaps across two vectors. However, the comparison primitives themselves (e.g., `Int64Vector._compare_vector` in `draken/vectors/int64_vector.pyx`) use scalar loops with bit-level null checks inside the hot loop:

```python
# draken/vectors/int64_vector.pyx ~line 570
valid1 = True if null1 == NULL else ((null1[i >> 3] >> (i & 7)) & 1) != 0
valid2 = True if null2 == NULL else ((null2[i >> 3] >> (i & 7)) & 1) != 0
valid = valid1 and valid2
if valid:
    if out_null != NULL:
        out_null[i >> 3] |= (1 << (i & 7))
    if self._compare_int64_values(data1[i], data2[i], op):
        dst[i >> 3] |= (1 << (i & 7))
```

This is a **branching** null evaluation pattern. The paper (§5.4, Listing 2) demonstrates that a branchless equivalent materially reduces CPU cycles at any null ratio, due to eliminated branch mispredictions.

### 2.3 Null fill value

Draken currently uses zero as the implicit placeholder for null positions. This is Arrow's default. The paper (§4) shows this is suboptimal for most data distributions when any downstream encoding is applied.

---

## 3. Proposed Changes

### Change 1: Batch-fused C⤏P conversion in the Parquet decoder

**Target file:** `rugo/src/parquet/decode_column.cpp` and a new `rugo/src/parquet/simd_null_expand.hpp`

#### Problem

The C⤏P conversion is a scatter operation: take `k` non-null values packed contiguously (Compact layout) and write them to positions `p₀, p₁, ..., pₖ₋₁` in the output Placeholder buffer, where positions come from the definition-level bitmap.

The paper (§3) surveys four algorithms for this operation and benchmarks them at 2048 values:

| Algorithm | Best at null ratio |
|-----------|-------------------|
| Arrow BitRunReader | Low (< ~10%) — excessive memcpy branches at medium ratios |
| SIMD BM→SV + Scatter | Medium (10–80%) |
| Optimized Scalar (TZCNT+BLSR) | High (> 80%) |
| AVX512 EXPAND | Low-medium (< 80%) — not applicable to our prod target |

For the Opteryx production target (AVX2/x86), the paper recommends **SIMD BM→SV + Scatter** with a batch size of **k = 16 words = 1024 values** (Appendix C, Figure 13). This batch size keeps the intermediate selection vector in L1 cache, avoiding the memory bandwidth penalty of a two-pass approach.

At high null ratios (> 80%), **Optimized Scalar** (using `__builtin_ctzll` / BLSR to iterate set bits) outperforms the SIMD path because SIMD lanes are underutilised when few bits are set.

#### Proposed design

Implement a new `simd_null_expand.hpp` with two entry points:

```cpp
// Scatter 'count' non-null values from 'src' to 'dst' at positions
// indicated by 'bitmap'. Chooses algorithm based on null_ratio.
void null_expand_int32(const int32_t* src, int32_t* dst,
                       const uint8_t* bitmap, size_t length,
                       float null_ratio);

void null_expand_int64(const int64_t* src, int64_t* dst,
                       const uint8_t* bitmap, size_t length,
                       float null_ratio);
```

Internally:
- If `null_ratio > 0.80`: use Optimized Scalar (TZCNT+BLSR loop, no SIMD lanes wasted)
- Otherwise: use SIMD BM→SV + Scatter with 16-word batches (1024-value chunks)

The null ratio is available without extra cost: `popcount(bitmap) / length` can be computed using the existing `simd_popcount()` in `src/cpp/simd_bitops.h`, which already dispatches to AVX2 POPCNT.

For the ARM/NEON dev path, a scalar fallback using `__builtin_ctzll` applies to both branches.

#### Expected benefit

The paper (Figure 6, uniform distribution) shows that at a 20% null ratio the Placeholder decode path is **2–3× faster** than Compact decode with a naïve C⤏P conversion. The current Rugo implementation is closer to the naïve case (no batched BM→SV fusion). At 50% null ratio the gap widens further. For a query reading a column with 20% nulls across 10M rows, this represents millions of wasted scatter operations per column per query.

The improvement is most visible on analytics workloads that scan large Parquet files with partially-populated optional columns — a common pattern in real-world datasets (the paper cites a survey showing ~80% of SQL developers encounter nulls in production).

---

### Change 2: Branchless null evaluation in Draken vectorized primitives

**Target files:** `draken/vectors/int64_vector.pyx`, and analogous vector types

#### Problem

The current two-column comparison in Draken (`_compare_vector`) evaluates nulls with a **branching pattern**: check `valid1`, check `valid2`, branch on the combined result, conditionally write the output bitmap, conditionally write the output data bit. This generates unpredictable branches at every row when null ratio is between 5% and 95%.

The paper (§5.4, Listing 2) demonstrates the branchless equivalent:

```c
// Branching (current Draken pattern):
if (!IsNull(col1, i) && !IsNull(col2, i) && col1[i] < col2[i])
    out_sv[cnt++] = i;

// Branchless (proposed):
out_sv[cnt] = i;
cnt += (!IsNull(col1,i)) & (!IsNull(col2,i)) & (col1[i] < col2[i]);
```

The key difference is the use of bitwise `&` rather than short-circuit `&&`, which eliminates the conditional branch and allows the CPU to speculate the increment. The paper (Figure 9) shows this reduces cycles by **2–4× under SVPartial** for any null ratio, with larger gains when null ratio is moderate (10–70%).

In Draken's Cython layer, the equivalent is:

```cython
# Proposed pattern for _compare_vector:
cdef int valid = 1
if null1 != NULL:
    valid &= (null1[i >> 3] >> (i & 7)) & 1
if null2 != NULL:
    valid &= (null2[i >> 3] >> (i & 7)) & 1
# Branchless write:
cdef int result = valid & self._compare_int64_values(data1[i], data2[i], op)
dst[i >> 3] |= result << (i & 7)
if out_null != NULL:
    out_null[i >> 3] |= valid << (i & 7)
```

Note: `_compare_int64_values` is always evaluated (no short-circuit). This is acceptable for scalar comparisons because the cost of the comparison is far lower than the cost of a branch misprediction. The paper validates this trade-off experimentally (§5.4).

#### Expected benefit

This change is architecture-agnostic and benefits both AVX2 (x86 prod) and NEON (ARM dev). It applies to every filter predicate involving a nullable column — which is the common case for any real-world schema with optional fields. The paper observes that Compact is **always slower** than Placeholder under SVPartial regardless of null ratio (Figure 9b), and the branchless Placeholder pattern is the fastest across the board.

The gain is realised directly in query execution latency for any query with `WHERE` predicates on nullable columns.

---

### Change 3: LastNonNull placeholder fill in Draken

**Target files:** `rugo/src/parquet/decode_column.cpp` (or Draken buffer allocation paths)

#### Problem

Draken currently leaves null positions as zero (the allocation default). The paper (§4.2, Figure 5) systematically evaluates five null-fill strategies across four real-world data distributions:

| Strategy | Best case | Notes |
|----------|-----------|-------|
| Zero | Uniform | Arrow default; mediocre elsewhere |
| Random | — | Actively harmful |
| MostFreq | Hotspot/zipf | Requires a pre-scan |
| **LastNonNull** | Uniform, hotspot, zipf | Best across 3 of 4 distributions |
| LinearInterpolation | Serial-correlated (timestamps) | Best for delta-encoded sequences |

LastNonNull fills each null position with the nearest preceding non-null value. This maximally exploits local run-length structure, which RLE can then compress efficiently. The paper shows LastNonNull matches or beats all other strategies on uniform, hotspot, and gentle_zipf distributions — the three most common in real-world OLAP data.

For serial-correlated data (timestamps, monotonic counters) LinearInterpolation is better, as it preserves the delta structure that makes Delta encoding effective.

This change is relevant to any path where Draken buffers are later compressed or where the placeholder values influence SIMD computation (e.g., accidentally including null positions in an aggregation without a null guard). Even for correctness-only paths, predictable placeholder values simplify debugging.

#### Proposed design

In `decode_column.cpp`, during the value decode loop, track the last decoded non-null value and write it to null positions rather than leaving the zero from allocation. This can be fused with the existing decode loop at zero extra pass cost.

For timestamp/date columns, apply LinearInterpolation: when a run of nulls is encountered, interpolate between the preceding and following non-null values. This requires a small lookahead on the value stream, which is feasible because the full column is already buffered before the bitmap is applied.

The decision between strategies can be made per-column based on the Draken type tag:
- `DATE32`, `TIMESTAMP`, or any type with serial-correlation heuristics → LinearInterpolation
- All others → LastNonNull

No sampling phase is required; this is simpler than the full SmartNull strategy in the paper (§4.1) and captures most of the benefit.

#### Expected benefit

The paper (Figure 5, hotspot/zipf) shows LastNonNull achieves **30–50% better compression ratio** than Zero at null ratios between 20% and 60%, which is the typical range for optional columns in analytics schemas. For timestamp columns with LinearInterpolation, the benefit extends to compression ratio parity with the Compact layout at null ratios up to 80% (Figure 5d). This directly reduces Parquet file sizes written by Opteryx and improves cache utilisation for large column reads.

---

## 4. What Is Not Proposed

**AVX512 EXPAND**: The paper's highest-performing C⤏P algorithm uses the `_mm512_maskz_expand_epi32` instruction, which has no equivalent on the production target (AVX2/x86) or dev target (NEON/ARM). The SIMD BM→SV + Scatter approach in Change 1 is the appropriate AVX2 analogue and captures the same structural benefit (batched, cache-friendly, no intermediate SV spill to memory).

**SpecialVal (sentinel) representation**: The paper (§5.3) shows SpecialVal has no meaningful advantage over Placeholder+bitmap for compression, and creates correctness problems for types with small value domains (Boolean, Int8). Draken's existing bitmap approach is correct and should not be replaced with sentinels.

**Roaring bitmap for null bitmaps**: The paper briefly mentions Roaring as a bitmap compression option. Opteryx operates on in-memory morsels where the overhead of Roaring's adaptive encoding is not justified — it is designed for persistent storage bitmaps, not row-level validity vectors in a hot execution path.

**Full SmartNull encoder sampling**: The complete SmartNull strategy requires a sampling phase during encoding to select the optimal fill strategy per encoding scheme. The simplified version in Change 3 (type-based dispatch) captures the primary benefit without implementing a general encoder sampler, which is out of scope for the current Draken architecture.

---

## 5. Implementation Priority

| Change | Complexity | Impact | Target |
|--------|-----------|--------|--------|
| 1: Batched C⤏P scatter in Rugo | Medium — new C++ file, integrates with decode_column.cpp | High — every nullable column in every Parquet read | `rugo/src/parquet/simd_null_expand.hpp` |
| 2: Branchless null evaluation | Low — Cython loop pattern change | High — every filter predicate on nullable columns | `draken/vectors/*.pyx` |
| 3: LastNonNull fill | Low — decode loop modification | Medium — compression ratio, cache footprint | `rugo/src/parquet/decode_column.cpp` |

Changes 2 and 3 are independent. Change 1 partially overlaps with Change 3 (both touch the decode loop).

---

## 6. References

- Zeng et al. (2024) — primary reference for all algorithmic decisions in this document. https://doi.org/10.1145/3662010.3663452
- Afroozeh & Boncz (2023), *The FastLanes Compression Layout* — underpins the FLS comparison in §5.2 of the paper; relevant if Opteryx adopts transposed vector layouts.
- Lemire & Boytsov (2013), *Decoding billions of integers per second through vectorization* — underlying SIMD integer compression, referenced in paper §4.2.
- Lemire et al. (2019), *Really fast bitset decoding for "average" densities* — basis for the Optimized Scalar BM→SV algorithm in §3.
- Lemire et al. (2016), *Consistently faster and smaller compressed bitmaps with Roaring* — cited in paper §5.3; not proposed for adoption.
- Ngom et al. (2021), *Filter Representation in Vectorized Query Execution* — basis for SVPartial/BMFull terminology used in §5.4 of the paper and Change 2 of this document.
