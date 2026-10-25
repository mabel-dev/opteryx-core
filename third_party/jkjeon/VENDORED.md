# Vendored: jk-jeon/dragonbox

- Upstream: https://github.com/jk-jeon/dragonbox
- Version: tag 1.1.3 (2022-06-18)
- Taken: 2026-10-25, `include/dragonbox/dragonbox.h`, `LICENSE-Apache2-LLVM`,
  `LICENSE-Boost` and `README.md` unmodified from that tag.
- SHA-256 of `dragonbox.h`: d3795bba52fb25970d9a1819429f6f225bb0db09fb1db26e55bdec290291e98c
- Licence: Apache-2.0 with LLVM exceptions OR Boost-1.0 (same pair as ulfjack/ryu).
- Used by: `draken/interop/value_format.hpp` (shortest double/float digits for the
  CSV / JSONL / Avro text writers, and the scientific text CAST FLOAT -> VARCHAR prints
  beyond 9.9e24) and `src/cpp/planner/manifest_prune.hpp` (a float literal as
  `Decimal(str(value))`). Gated by `draken/interop/float_text_test.cpp`
  (`make float-text-test`), whose oracle is `std::to_chars` plus a plain repr layout.
- Replaced ulfjack/ryu's `d2s.c` / `f2s.c` (deleted 2026-10-25). Only ryu's `d2fixed.c`
  remains in `third_party/ulfjack/ryu`: dragonbox is shortest-only and cannot do the
  fixed-precision `%.Nf` / `%.Ne` formatting CAST and HUMANIZE use.
- Header-only; include as `#include "dragonbox/dragonbox.h"` (include dir
  `third_party/jkjeon/include`).
