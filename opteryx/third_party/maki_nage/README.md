# Opteryx Distogram

This directory vendors and heavily modifies `maki-nage/distogram` for Opteryx.
The upstream `README.rst` is retained for provenance; this file describes the
Opteryx implementation.

## Purpose

Distograms are used by the cost-based optimizer to estimate predicate
selectivity from compact column distributions. Optimizer latency must be low
enough that there is no practical question of skipping optimization.

The primary CBO path is:

1. Manifest/file statistics provide per-column equi-width histogram counts.
2. `Manifest.get_distogram()` builds a combined `Distogram` for a column.
3. Predicate selectivity calls `count_up_to()` to estimate equality, range,
   `IN`, and `BETWEEN` selectivity.

## Hot Paths

The relevant hot functions are:

- `load_counts()` for Python sequence histogram counts.
- `load_counts_i64()` for contiguous native `int64_t` histogram counts.
- `merge()` when combining per-file distograms.
- `count_up_to()` when estimating predicate selectivity.

`quantile()` and `bulkload()` are optimized but are not currently the main CBO
path.

## Implementation

`distogram.pyx` is the Cython wrapper and owns the Python-facing API. The
distogram itself stores bins in C memory:

- `Bin* bins_data`
- `int64_t* prefix_counts`
- native `bins_length`, `bins_capacity`, and `total_count`

`count_up_to()` uses C-level binary search, interpolation, and cached prefix
counts. It does not sum bins on every call.

`load_counts_i64()` accepts a contiguous typed buffer:

```cython
def load_counts_i64(const int64_t[::1] counts, double minimum, double maximum)
```

This is the preferred ingestion shape for compiled/statistics paths because it
can run the count summation through native kernels under `nogil`.

## Native Kernels

The native kernel layer is C++:

- `_distogram_core.h`
- `_distogram_core.cpp`
- `_distogram_avx2.cpp`
- `_distogram_neon.cpp`
- `_distogram_rvv.cpp`

The current native kernel is `distogram_sum_i64()`, used by `load_counts_i64()`
to sum contiguous histogram counts. Dispatch selects the best compiled kernel:

- scalar C++ fallback
- x86 AVX2
- ARM NEON
- RISC-V RVV

Runtime dispatch uses the existing Opteryx SIMD dispatch helpers and honors
`OPTERYX_DISABLE_SIMD`.

## Constraints

- No PyArrow or NumPy in engine code.
- No Python fallback implementation for compiled hot paths.
- No dynamic dispatch inside tight loops.
- Release the GIL where the loop only touches native memory.
- Benchmark performance changes; correctness failures make perf informational.

The current manifest storage may still provide Python lists. That path uses
`load_counts()`. Native histogram storage should use a contiguous `int64_t`
buffer and call `load_counts_i64()`.

## Build

Use the repository build path:

```bash
make compile
```

The distogram extension is built as C++ and links the native kernel files via
`setup.py`.

## Focused Tests

Run distogram correctness tests:

```bash
python -m pytest \
  opteryx/third_party/maki_nage/tests/test_count.py \
  opteryx/third_party/maki_nage/tests/test_quantile.py \
  opteryx/third_party/maki_nage/tests/test_update.py \
  -q
```

Run the distogram performance benchmark:

```bash
python -m pytest tests/performance/test_distogram_perf.py -s -q
```

Save or refresh the benchmark baseline:

```bash
SAVE_BASELINE=1 python tests/performance/test_distogram_perf.py
```

## Current Known Boundary

The native/SIMD path exists for contiguous `int64_t` histogram counts. Full CBO
benefit requires upstream statistics storage to provide native contiguous
histogram buffers instead of Python `list[int]` values.
