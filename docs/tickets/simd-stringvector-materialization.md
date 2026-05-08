# Ticket: SIMD/branchless StringVector materialization from dict codes

## Problem

When a dict-encoded column **must** be materialized to a flat `StringVector`
(projections, joins on the string value, expressions that don't yet have a
dict-aware fast path), the materialization loop in `_make_string_vector`
(in `rugo/src/parquet/parquet_reader.pyx`, around the 800–950 line range —
verify) is scalar:

- Pre-counts total bytes by walking dict_lens[codes[i]] in a `for` loop.
- Allocates the StringVector with exact capacity (good — already done).
- Walks codes again, copying body bytes per row with one memcpy per row,
  accumulating offsets with scalar `+=`.
- The nullable variant adds a per-row branch on the validity bitmap.

For URL on ClickBench (millions of rows, hundreds-of-bytes average length)
this is the second-biggest CPU cost on the materialization path after
decompression. The work is embarrassingly vectorizable.

This ticket: replace the two scalar passes with SIMD/branchless equivalents
on NEON (ARM, dev) and AVX2 (x86, prod).

## Why this matters

Dict-aware aggregation (separate ticket) eliminates materialization for many
URL queries, but **not all**: projections, joins, and any string-returning
expression still hit this path. Cutting the materialization cost is a
durable win that compounds with the other tickets, not a duplicate of them.

## Scope

In scope:
- The materialization path in `rugo/src/parquet/parquet_reader.pyx`
  (function: `_make_string_vector`, or whichever the dict→flat path is in
  the current code — verify name and location).
- A small C/C++ helper if SIMD is easier to write outside Cython; place
  alongside other native helpers in `rugo/src/parquet/`.

Out of scope:
- Other vector types (numeric dict expansion).
- Changing the StringVector layout itself.
- The `_dict_codes`/`_dict_values` accessor surface in Draken (separate
  ticket adds the encoded-form accessors).

## Approach

### Pass 1 — byte-count via SIMD gather of `dict_lens[codes]`

Current: scalar `total += dict_lens[codes[i]]`.

Replace with width-specialized SIMD:
- If `dict_code_width == 1`: process 16 codes at a time on NEON, 32 on AVX2.
  Do a SIMD table lookup if the dict size fits in a register-width LUT
  (rare — usually no), otherwise gather.
- If `dict_code_width == 2`: 8 codes/iteration NEON, 16 AVX2.
- If `dict_code_width == 4`: 4 codes/iteration NEON, 8 AVX2.

Use AVX2 `vpgatherdd` for x86 with 32-bit codes; manual gather (4× scalar
loads, vector combine) where gather is slow or unavailable. Sum the gathered
lengths with a horizontal add.

### Pass 2 — branchless prefix-sum on offsets

Current: `offsets[i+1] = offsets[i] + dict_lens[codes[i]]` in a scalar loop.

Replace with vectorized prefix-sum (Hillis-Steele or a SIMD scan):
- Build a vector of lengths via the same gather as Pass 1.
- Apply a SIMD inclusive scan (log-N shift+add).
- Add the running carry from the previous block.
- Store offsets.

This pass can be **fused with Pass 1** if you keep the gathered lengths in
register — single read of codes for both byte-count and offset construction.

### Pass 3 — gather-copy bodies

Current: one `memcpy(out + offset[i], dict_ptrs[code], dict_lens[code])` per
row.

Two strategies, pick based on dict value length distribution:
- **Short values dominant (URLs are not this case)**: SIMD gather of values
  packed into fixed-width lanes, scatter-store.
- **Variable-length values (URLs)**: keep memcpy, but **unroll 4–8 wide**
  with software prefetch on `dict_ptrs[codes[i+8]]`. Modern memcpy is
  already SIMD-internal; the win is hiding pointer-chase latency via
  prefetch and ILP across the unrolled iterations.

For URL (avg ~50–100 bytes), strategy 2 is the right call. Don't try to
out-clever libc memcpy for the body bytes themselves.

### Nullable variant — branchless validity

Current: per-row `if (bitmap[i >> 3] & (1 << (i & 7)))`.

Replace with: process 64 rows at a time by loading a `uint64_t` from the
validity bitmap, using `__builtin_ctzll` (or `_BitScanForward64`) to walk
set bits. Or: precompute an index array of valid row positions, then run
Pass 1–3 over those positions only. The latter trades a small allocation
for a fully branchless inner loop and is usually faster when the null
fraction is moderate.

### Architecture handling

- Single C/C++ helper file with `#if defined(__ARM_NEON)` / `#if defined(__AVX2__)`
  guards.
- Scalar fallback for any other arch.
- Compile-time dispatch — no runtime CPU detection needed (build targets
  are pinned: NEON for dev, AVX2 for prod, per CLAUDE.md §6).

## Constraints (from CLAUDE.md)

- **Performance-first.** Specialization is acceptable; duplication for its
  own sake is not. Width-specialize on `dict_code_width` (1/2/4) — that's
  three variants, not three copies of unrelated logic.
- **No Python.** Cython only at the boundary; SIMD in C/C++ if cleaner.
  `cdef nogil` for any Cython hot loop.
- **No `object` in Cython** — typed all the way down.
- **No fallback duplication** beyond what arch-specific SIMD requires.
- **Fail fast.** Bounds-check dict codes once at entry (assert
  `max_code < dict_size`); the inner loop trusts.
- **Naming**: if you add a C/C++ helper, prefix with `_` per CLAUDE.md §5.
- **Do not commit.**

## Files (verify before editing)

- `rugo/src/parquet/parquet_reader.pyx` — `_make_string_vector` (around
  lines 800–950, verify with `grep -n _make_string_vector`).
- New: `rugo/src/parquet/_string_materialize.{hpp,cpp}` — SIMD helpers, if
  written in C++.

## Tests

- Correctness: byte-identical output vs the current scalar path. Test:
  - Empty input, single-row, exactly one SIMD block, one block + tail.
  - Each `dict_code_width` (1, 2, 4).
  - With and without nulls; null fractions 0%, 1%, 50%, 99%, 100%.
  - Dict values of varied length (1 byte, 64 bytes, 1 KB).
- `make q` must pass.
- Microbench: time `_make_string_vector` on a synthetic URL-shaped column
  (10 M rows, dict size 100 K, avg 60 bytes). Report before/after.
- `make clickbench` for queries that materialize URL (projections,
  ORDER BY URL): report wall-time deltas in PR.

## Definition of done

- Pass 1 + Pass 2 fused into a single SIMD pass over codes.
- Pass 3 unrolled with prefetch.
- Nullable variant branchless.
- NEON + AVX2 implementations, scalar fallback.
- Bit-identical output verified by tests.
- `make q` passes; ClickBench numbers in PR.
- No `try/except` for control flow; no silent degradation.
