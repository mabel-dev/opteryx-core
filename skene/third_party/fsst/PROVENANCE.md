# FSST — vendored

Fast Static Symbol Table compression for short strings. Boncz, Neumann, Leis
(CWI), VLDB 2020.

| | |
|---|---|
| upstream | https://github.com/cwida/fsst |
| commit | `e638d4cf8c26129d73c242a4127b42b975de5b63` |
| vendored | 2026-08-06 |
| licence | MIT — see `LICENSE`, retained verbatim |

## What is here, and what is not

Vendored: `fsst.h`, `fsst.cpp`, `libfsst.hpp`, `libfsst.cpp`,
`fsst_avx512.cpp`, `fsst_avx512*.inc`, `LICENSE`, `README.md`.

**Omitted deliberately:**

- `fsst12.*` — the 12-bit-symbol variant. A second, parallel implementation
  with its own API. Nothing here uses it.
- `CMakeLists.txt`, `Makefile.linux` — skene has its own build.
- the presentation and paper media (~18 MB, including a video). Reference
  material, not source.

Sources are **unmodified**. If a local change ever becomes necessary it must be
recorded here, because an undocumented divergence from upstream is a bug that
only appears at the next update.

## Architecture note

`fsst_avx512.cpp` guards its SIMD body on `__AVX512F__` at compile time *and*
`fsst_hasAVX512()` at runtime, falling back to the scalar path otherwise. It
therefore builds and runs correctly on ARM (dev) as well as x86 (prod) with no
guard of our own.

## Status

**Evaluated, not adopted.** Vendored to measure against zstd-1 on the string
arena — see BENCHMARKS.md, "FSST on the string arena". It is built only into the
`fsst_arena` bench binary; `libskene.a` does not link it and has no dependency
on it. Adopting it would be a format and draken-ABI decision, not a build one.
