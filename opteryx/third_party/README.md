# Third-Party Libraries

This directory contains third-party Python, Cython, and wrapper integrations used by Opteryx Core.

## Structure

The third-party code is organized into two locations:

1. **`/third_party/`** (repository root)
   - Contains C/C++ source code for third-party libraries
   - These are the original library implementations
   - Example: `third_party/cyan4973/xxhash.c`

2. **`/opteryx/third_party/`** (this directory)
   - Contains Python wrappers and Cython interfaces
   - Includes `.pyx` (Cython implementation) and `.pxd` (Cython interface) files
   - Also contains pure Python third-party modules
   - Example: `opteryx/third_party/cyan4973/xxhash.pyx`

## Current Third-Party Libraries

### Compiled Extensions and Native Wrappers

- **cyan4973** - xxHash fast hashing
- **fastfloat** - Fast float parsing
- **fuzzy** - Soundex phonetic algorithm
- **lz4** - LZ4 compression wrappers
- **mabel** - Base encoding helpers
- **tdigest-c** - t-digest quantile sketch
- **ulfjack** - Ryu floating point to string conversion
- **yyjson** - JSON parsing wrappers

### Pure Python Libraries

- **maki_nage** - Distogram (approximate histogram)
- **mbleven** - Modified Levenshtein distance
- **sqloxide** - SQL parser
- **travers** - Graph algorithms

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
