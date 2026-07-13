# Third-Party Libraries

This directory contains third-party Python, Cython, and wrapper integrations used by Opteryx Core.

## Structure

The third-party code is organized into two locations:

1. **`/third_party/`** (repository root)
   - The single canonical home for all vendored C/C++ source, including the
     compression libraries (`zstd`, `lz4`, `snappy`) shared by both the
     `opteryx_core` and standalone `rugo` wheels.
   - These are the upstream library implementations.
   - Example: `third_party/cyan4973/xxhash.h` (header + `xxhash.c` impl stub)

2. **`/opteryx/third_party/`** (this directory)
   - Contains only Python wrappers and Cython interfaces over the C/C++ that
     lives in `/third_party/` — bindings, not vendored source.
   - Includes `.pyx` (Cython implementation) and `.pxd` (Cython interface) files
   - Also contains pure Python third-party modules
   - Example: `opteryx/third_party/cyan4973/xxhash.pyx` wraps `third_party/cyan4973`

## Current Third-Party Libraries

### Compiled Extensions and Native Wrappers

- **cyan4973** - xxHash fast hashing (Cython wrapper; C source in `third_party/cyan4973`)
- **fastfloat** - Fast float parsing
- **mabel** - Base encoding helpers (base16/base64/base85)
- **mbleven** - Owned Cython implementation of modified Levenshtein distance
- **pcg** - `pcg.pxd` binding only; C++ headers in `third_party/pcg`
- **yyjson** - JSON parsing wrapper (Cython; C source in `third_party/yyjson`)

> Compression libraries (`zstd`, `lz4`, `snappy`) and Ryu (`ulfjack`) have **no**
> wrapper here — they are compiled straight from `third_party/` by the shared
> build (`build_common.py`). Do not re-add empty placeholder directories for them.

### Pure Python Libraries

- **maki_nage** - Distogram (approximate histogram)
- **sqloxide** - SQL parser
- **travers** - Owned graph algorithms package by @joocer

## Building

The extensions are built by the top-level build path:

```bash
make compile
```

Each extension is defined in `setup.py` with:

- Source files from both `/third_party/` and `/opteryx/third_party/`
- Appropriate include directories
- Compiler flags for C or C++

## Previous Structure (Deprecated)

Before consolidation, there were three locations:
- `/third_party/` - C/C++ source
- `/opteryx/third_party/` - Python and `.pxd` files
- `/opteryx/compiled/third_party/` - `.pyx` files (**removed**)

The `.pyx` files have been moved from `/opteryx/compiled/third_party/` to `/opteryx/third_party/` 
to consolidate all Python/Cython wrappers in one location.

## Owned Forks

Some code in this directory is intentionally kept under `opteryx/third_party/`
because it started from external work or carries third-party license notices,
but it is now an Opteryx-owned implementation. Do not blindly revendor or
replace these files from upstream sources.

- **`travers/`** is maintained by @joocer for Opteryx graph planning use cases.
- **`mbleven.pyx`** is a significantly rewritten Cython implementation and must
  not be replaced with the original Python implementation.
