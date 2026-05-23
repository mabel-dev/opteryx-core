# Milestone E.0 — Binding POC

Proves the canonical Cython→C++ binding pattern for compiled draken consumers.

## What it proves

1. `cimport draken.core.buffers` works: Cython binds `DrakenVector` directly from
   the hand-written `buffers.pxd` (`cdef extern from "core/buffers.h"`).
2. `cdef extern from "ops/..."  namespace "draken::ops"` works: op functions in C++
   namespaces are callable from Cython without a wrapper.
3. A manually-constructed `DrakenVector` (stack data + stack selection) passes through
   the `data[selection[i]]` access path correctly.
4. `i64_sum`, `i64_min`, `i64_max` return correct results, run nogil.

## What it does NOT yet prove

- `draken_vector_unwrap` (Phase 0 plumbing): extracting `DrakenVector` from a Python
  `Vector` nanobind handle. That requires `draken/core/draken_bridge.h` + an
  `extern "C"` function in `draken_native.cpp`. See §2.1 of `E0_consumer_rewrite_scoping.md`.
- `draken_hash` (pulls in carchar_set + simd_hash via hash.h): works in draken_native
  build (correct include path set); the POC avoids it only because the standalone
  build doesn't include the full `src/cpp/` chain with carchar.

## Build

From repo root:
```bash
python draken/poc/setup_poc.py build_ext --inplace --build-lib draken/poc
```

Requires: Cython. Does NOT require mimalloc.o (the POC uses plain malloc).

## Run

```bash
python draken/poc/run_poc.py
```

Expected output:
```
length         : 8
non_null_count : 8
sum            : 150
min            : -7
max            : 100

All assertions passed — POC proves:
  [+] cimport draken.core.buffers binds DrakenVector struct via buffers.pxd
  [+] cdef extern from ops/int64_reductions.h namespace draken::ops works
  [+] i64_sum / i64_min / i64_max run correctly (nogil, data[selection[i]] pattern)
  [+] Manually-constructed DrakenVector (no mimalloc, no vector_alloc) is valid
```
