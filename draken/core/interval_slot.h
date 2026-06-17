#pragma once
// draken/core/interval_slot.h — Physical slot for DRAKEN_INTERVAL (Milestone D.12).
//
// Storage: 16 bytes/row = [months: int64][us: int64].
//
// Both components are preserved separately:
//   months — calendar months (variable-length: 28–31 days each)
//   us     — sub-month component, in MICROSECONDS
//
// The sub-month field carries MICROSECONDS — this is the canonical engine unit:
// INTERVAL literals are built as (months, µs) and stored verbatim, and
// temporal − temporal / temporal ± interval both produce and consume µs.
//
// Normalization for compare/hash/order (not stored — computed at op entry only):
//   total_us = months × INTERVAL_MONTH_US + us
//   where INTERVAL_MONTH_US = 2_592_000_000_000  (30 days × 86_400_000_000 µs/day)
//
// Arithmetic is component-wise (months and us independently — not normalized):
//   (1mo, 0) + (2mo, 0) = (3mo, 0); neg: negate both components.

#include <cstdint>

struct DrakenIntervalSlot {
    int64_t months;
    int64_t us;
};
static_assert(sizeof(DrakenIntervalSlot) == 16, "DrakenIntervalSlot must be 16 bytes");
static_assert(alignof(DrakenIntervalSlot) == 8, "DrakenIntervalSlot must be 8-byte aligned");

// Normalization constant: 1 month = 30 days × 86_400_000_000 µs/day.
static constexpr int64_t INTERVAL_MONTH_US = 2'592'000'000'000LL;
// Informational: 1 day = 86_400_000_000 µs (not used for storage, documented here).
static constexpr int64_t INTERVAL_DAY_US   =    86'400'000'000LL;
