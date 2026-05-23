#pragma once
// draken/core/interval_slot.h — Physical slot for DRAKEN_INTERVAL (Milestone D.12).
//
// Storage: 16 bytes/row = [months: int64][ms: int64].
//
// Both components are preserved separately:
//   months — calendar months (variable-length: 28–31 days each)
//   ms     — millisecond component
//
// Normalization for compare/hash/order (not stored — computed at op entry only):
//   total_ms = months × INTERVAL_MONTH_MS + ms
//   where INTERVAL_MONTH_MS = 2_592_000_000  (30 days × 86_400_000 ms/day)
//
// Arithmetic is component-wise (months and ms independently — not normalized):
//   (1mo, 0) + (2mo, 0) = (3mo, 0); neg: negate both components.
//
// Flag: future ts ± interval arithmetic will be ms-granular (lossy sub-ms).

#include <cstdint>

struct DrakenIntervalSlot {
    int64_t months;
    int64_t ms;
};
static_assert(sizeof(DrakenIntervalSlot) == 16, "DrakenIntervalSlot must be 16 bytes");
static_assert(alignof(DrakenIntervalSlot) == 8, "DrakenIntervalSlot must be 8-byte aligned");

// Normalization constant: 1 month = 30 days × 86_400_000 ms/day.
static constexpr int64_t INTERVAL_MONTH_MS = 2'592'000'000LL;
// Informational: 1 day = 86_400_000 ms (not used for storage, documented here).
static constexpr int64_t INTERVAL_DAY_MS   =    86'400'000LL;
