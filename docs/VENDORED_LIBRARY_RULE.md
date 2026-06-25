# Vendored Library Linking Rule

## The Problem

When C/C++ source code `#include`s a header from a vendored library (e.g., `#include "ryu.h"`), the implementation files for that library must be explicitly listed in `setup.py`'s extension `sources=` list. Failure to do this results in **undefined symbol errors at runtime** when the wheel is installed on a different platform (e.g., x86_64 Linux) where linking is stricter.

This happened in June 2026 with `d2s_buffered_n` from the Ryu library:
- `draken_native.cpp` and `cast_numeric.cpp` `#include "ryu.h"`
- `ryu.h` declares `d2s_buffered_n` and `d2fixed_buffered_n`
- But only `d2fixed.c` was in the sources; `d2s.c` was missing
- Result: `undefined symbol: d2s_buffered_n` at import time on the Pi

## The Rule

**For every `#include "vendored_header.h"` in a C/C++ source file:**

1. Identify which `.c`/`.cpp` files implement that header
2. Add those implementation files to the `Extension.sources=` list of the extension that includes the source file
3. Run `make check-symbols` after compilation to verify all symbols resolve

## Verification

After `make c` (or `make compile`), verify there are no undefined symbols:

```bash
make check-symbols
```

This runs `dev/check_undefined_symbols.py`, which uses `nm -u` to inspect all compiled `.so` files for unresolved symbols (excluding expected Python C API symbols).

## Common Vendored Libraries and Their Implementation Files

| Header | Implementation Files | Used By |
|--------|---------------------|---------|
| `ryu.h` | `d2s.c`, `d2fixed.c` | draken_native, cast_numeric |
| `yyjson.h` | `yyjson.c` | (currently unused in main codebase) |
| `xxhash.h` | `xxhash.c` | *(header-only when XXH_INLINE_ALL is defined)* |
| `lz4.h` | `lz4.c` | parquet compression |
| `zstd/*.h` | zstd vendor sources | parquet compression |
| `snappy.h` | snappy sources | parquet compression |

## Implementation Checklist

- [ ] Add implementation `.c`/`.cpp` files to extension `sources=`
- [ ] Compile locally: `make c`
- [ ] Check symbols: `make check-symbols` (must pass)
- [ ] Only then commit and deploy to CI

## Related

- `setup.py`: Extension definitions (lines 715–2000+)
- `dev/check_undefined_symbols.py`: Symbol verification script
- `Makefile`: `check-symbols` target
