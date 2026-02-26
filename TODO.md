
- Review Draken date/timestamp handling, ensure able to quickly compare between values in these two encodings

---

# rugo Parquet Decoder — Performance Optimisations

Current single-threaded baseline (on-power): **rugo 1.02x PyArrow** — practical parity.
fastparquet is 1.64x PyArrow for reference.

C++ phase breakdown (11728 `DecodeColumnFromChunk` calls, TPCH lineitem 16 files):

| Phase | Time | % |
|---|---|---|
| ZSTD decompress | ~688ms | 59% |
| Value expansion  | ~148ms | 13% |
| Dict value parse | ~189ms | 16% |
| RLE index decode | ~121ms | 10% | ← was 289ms, SIMD dispatched ✓ |
| Other            |  ~24ms |  2% |

---

## ✅ 1. Bit-width specialised RLE unpacker — DONE

**Target**: `DecodeRLEBitPackedIndicesNoPrefix` in `decode_encodings.cpp`.

The inner bit-packed loop currently processes one value at a time:

```cpp
for (int32_t i = 0; i < values_in_run; i++) {
    int bit_pos  = i * bit_width;
    int byte_pos = bit_pos / 8;
    int bit_off  = bit_pos % 8;
    for (int b = 0; b < 5; b++) value |= ptr[byte_pos+b] << (b*8);
    value = (value >> bit_off) & mask;
    indices.push_back(value);   // bounds-checked each time
}
```

**Fix**: `switch (bit_width)` dispatching to cases 1–8 (covers >99% of real data).
Each case unpacks all 8 values in a group with fixed shifts and masks — no inner loop,
no division, no variable-width masking. NEON: `vld1q_u8` + shift/mask to unpack 16
values per cycle.

Also fix the RLE constant-run fill in the same function:

```cpp
// before
for (int32_t i = 0; i < count; i++) indices.push_back(value);
// after
indices.resize(old_sz + count);
std::fill_n(indices.data() + old_sz, count, (int32_t)value);   // auto-vectorised
```

**Result**: RLE 289ms → 121ms (−58%). Overall rugo/PyArrow 1.41x → 1.02x.

Implemented in `third_party/mabel/rugo/parquet/decode_encodings.cpp`:
- `unpack_bitpacked_groups_scalar` — uint64 window, eliminates inner byte loop and division
- `unpack_bitpacked_groups_neon` — NEON intrinsics for bw=1 (`vshl_u8`), bw=2 (`vshlq_u16`), bw=4 (`vshlq_u32`); scalar fallback for bw=3,5,6,7
- `unpack_bitpacked_groups_avx2` — AVX2 `_mm256_cvtepu8_epi32` for bw=8, `_mm256_srlv_epi32` for bw=4
- Dispatched via `simd::select_dispatch<unpack_groups_fn_t>` with `static std::atomic` cache
- RLE constant-run fill changed from `push_back` loop to `resize + std::fill_n`
- All three `DecodeRLEBitPacked*` functions updated
- `src/cpp/cpu_features.cpp` added to parquet extension sources in `setup.py`

---

## 2. Validity bitmap construction from def_levels (est. −5ms, "other")

**Target**: end-of-column loop in `decode_column.cpp`:

```cpp
for (int32_t i = 0; i < total_rows; i++)
    if (all_def_levels[i] == max_def)
        valid_bits[i/8] |= (1 << (i%8));
```

**Fix**: process 8 `int32_t` def_levels at a time:
1. Load 8 `int32_t` from `all_def_levels`
2. Compare each == `max_def` → 8 bools
3. Pack 8 bools into 1 byte (NEON: `vceqq_s32` × 2 + `vmovn`/`vshrn`; AVX2: `_mm256_movemask_epi8`)
4. Write 1 byte directly to `valid_bits`

**Follow the existing dispatch pattern** (same as item 1).

---

## 3. Gather for numeric dict expansion (est. −20ms, value expansion)

**Target**: post-RLE scatter loops in `decode_column.cpp`:

```cpp
int64_t* dst = result.int64_values.data() + old_sz;
for (int32_t i = 0; i < n; ++i)
    dst[i] = dict_int64[indices[i]];    // random gather
```

**Fix**:
- AVX2: `_mm256_i32gather_epi64` — 4 int64 gathers per instruction
- NEON: manual 4× unroll (no native gather), eliminates loop overhead
- Apply to `float64`, `int32`, `float32` gather loops too

**Follow the existing dispatch pattern** (same as item 1).

---

## 4. Prefetch ahead in string dict scatter (est. −30ms, Cython string path)

**Target**: `_make_string_vector` in `parquet_reader.pyx` — inner dict expand loop:

```cython
for i in range(num_rows):
    dict_idx = decoded_col.dict_indices[i]
    slen = dict_lens[dict_idx]
    memcpy(dst + offset, dict_ptrs[dict_idx], slen)
```

When dict entry lengths vary the `memcpy` source is unpredictable — classic cache miss.
Prefetch 8 entries ahead:

```cython
if i + 8 < num_rows:
    __builtin_prefetch(dict_ptrs[decoded_col.dict_indices[i + 8]], 0, 1)
```

Compiles to `PRFM` on ARM and `PREFETCHT1` on x86 — no SIMD dispatch needed.

---

## Benchmark / build

```bash
# build after any C++ or Cython edit
touch third_party/mabel/rugo/parquet/parquet_reader.pyx
python3 setup.py build_ext --inplace -q

# compare vs PyArrow
python3 tests/performance/benchmarks/bench_parquet_decoders_compare.py

# C++ phase telemetry
python3 scratch/rugo_telemetry.py
```