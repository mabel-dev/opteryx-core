#pragma once
// draken/logical_type.h — Logical-type descriptor infrastructure (Milestone D.8).
//
// Design (doc 06, § "Resolved: physical type vs logical type"):
//
//   Physical type  = DrakenType on DrakenVector.  The storage layout and the dispatch
//                    key.  FROZEN 40-byte ABI.  NEVER carries logical params.
//
//   Logical type   = SQL type WITH its parameters (unit, tz, precision/scale, …).
//                    Lives OUT-OF-BAND: NOT a field on DrakenVector.  Carried on the
//                    nanobind Vector handle (VectorOwner) as a BORROWED POINTER into
//                    a process-global interned registry.
//
// Interning guarantees that two TIMESTAMP(us, +0) vectors reference the SAME
// descriptor instance (pointer equality ⟹ descriptor equality).  The registry never
// moves or frees entries, so borrowed pointers are stable for process lifetime.
//
// Mandatory for parameterized physical types (TIMESTAMP64, DECIMAL, VECTOR):
//   logical_type == nullptr on a TIMESTAMP64 vector is a HARD ERROR.
//   Unlike statistics ("absent = don't know"), a missing descriptor means the data
//   is uninterpretable — fail loud, never silently degrade.
//
// Hot dispatch is on the PHYSICAL type only; logical type is read at ingestion,
// readback, and op-entry edges — never per-row inside a kernel.

#include <cstdint>
#include <deque>
#include <mutex>

// ---------------------------------------------------------------------------
// Timestamp unit codes (ascending precision, matches Arrow/pandas convention).
// ---------------------------------------------------------------------------
enum class TimestampUnit : uint8_t {
    SECONDS      = 0,
    MILLISECONDS = 1,
    MICROSECONDS = 2,
    NANOSECONDS  = 3,
};

// ---------------------------------------------------------------------------
// Logical-type kind tag.
// NONE      = no logical type (all non-parameterized physical types).
// TIMESTAMP = DRAKEN_TIMESTAMP64 with unit + fixed UTC offset.
// TIME      = DRAKEN_TIME32 / DRAKEN_TIME64 with unit only (no offset).
// DECIMAL   = DRAKEN_DECIMAL with precision (1..18) and scale (0..precision).
//             Physical storage is int64 unscaled value.
// ---------------------------------------------------------------------------
enum class LogicalKind : uint8_t {
    NONE      = 0,
    TIMESTAMP = 1,
    TIME      = 2,
    DECIMAL   = 3,
    VECTOR    = 4,  // DRAKEN_VECTOR_FP16: carries dimension parameter.
};

// ---------------------------------------------------------------------------
// Immutable logical-type descriptor.
//
// For TIMESTAMP:
//   unit           — storage resolution (s / ms / us / ns).
//   offset_minutes — fixed UTC offset in minutes (e.g., +60 for +01:00, -330
//                    for -05:30).  UTC when 0.  NOT a named zone; no DST.
//
// For DECIMAL:
//   precision — total significant digits (1..18); fits unscaled value in int64.
//   scale     — digits to the right of the decimal point (0..precision).
//   unit and offset_minutes are unused (zero).
//
// All other kinds: all parameter fields are zero.
// ---------------------------------------------------------------------------
struct LogicalType {
    LogicalKind   kind            = LogicalKind::NONE;
    TimestampUnit unit            = TimestampUnit::MICROSECONDS;
    int16_t       offset_minutes  = 0;
    uint8_t       precision       = 0;   // DECIMAL: 1..18; others: 0
    uint8_t       scale           = 0;   // DECIMAL: 0..precision; others: 0
    uint32_t      dimension       = 0;   // VECTOR: fp16 embedding dimension (≥1); others: 0

    bool operator==(const LogicalType& o) const noexcept {
        return kind           == o.kind
            && unit           == o.unit
            && offset_minutes == o.offset_minutes
            && precision      == o.precision
            && scale          == o.scale
            && dimension      == o.dimension;
    }
    bool operator!=(const LogicalType& o) const noexcept { return !(*this == o); }
};

// ---------------------------------------------------------------------------
// Process-global interned registry.
//
// Returns a BORROWED pointer to the canonical LogicalType instance for lt.
// Two calls with equal lt values return the same pointer.
//
// Storage: std::deque<LogicalType> — push_back never invalidates existing
// element addresses (unlike std::vector), so borrowed pointers stay stable.
//
// Thread-safety: guarded by a process-global mutex. Reachable off-GIL via
// vecresult_to_owner (e.g. take/mask/slice on a TIMESTAMP64 column inside a
// gil_scoped_release window), so the GIL can no longer be relied on to
// serialise the iterate+push_back. The deque keeps borrowed pointers stable;
// the mutex only protects the lookup/insert, and the returned pointer remains
// valid (and lock-free to dereference) for the process lifetime.
// ---------------------------------------------------------------------------
static inline const LogicalType* logical_type_intern(const LogicalType& lt) {
    static std::deque<LogicalType> registry;
    static std::mutex registry_mutex;
    std::lock_guard<std::mutex> lk(registry_mutex);
    for (const LogicalType& entry : registry) {
        if (entry == lt) return &entry;
    }
    registry.push_back(lt);
    return &registry.back();
}

// ---------------------------------------------------------------------------
// Calendar arithmetic (Howard Hinnant's civil-calendar algorithms).
//
// Both functions operate in UTC; the caller is responsible for applying any
// UTC offset before/after calling them.
// ---------------------------------------------------------------------------

// Convert calendar parts → microseconds since Unix epoch (1970-01-01 00:00:00 UTC).
// Valid for the full Python datetime range (year 1–9999).
static inline int64_t parts_to_us_epoch(int y, int mo, int d,
                                         int h, int mi, int s, int us) noexcept {
    int adj = y - (mo <= 2 ? 1 : 0);
    int64_t era = (adj >= 0 ? adj : adj - 399) / 400;
    unsigned yoe = static_cast<unsigned>(adj - era * 400);
    unsigned doy = (153u * static_cast<unsigned>(mo > 2 ? mo - 3 : mo + 9) + 2u) / 5u
                   + static_cast<unsigned>(d) - 1u;
    unsigned doe = yoe * 365u + yoe / 4u - yoe / 100u + doy;
    int64_t days = era * 146097LL + static_cast<int64_t>(doe) - 719468LL;
    int64_t total_s = days * 86400LL
                    + static_cast<int64_t>(h)  * 3600LL
                    + static_cast<int64_t>(mi) * 60LL
                    + static_cast<int64_t>(s);
    return total_s * 1000000LL + static_cast<int64_t>(us);
}

// Convert microseconds since Unix epoch → calendar parts (UTC).
// Handles pre-epoch (negative) values correctly via floor division.
static inline void us_epoch_to_parts(int64_t us_epoch,
                                      int& y, int& mo, int& d,
                                      int& h, int& mi, int& s, int& us) noexcept {
    // Floor-divide into whole seconds + sub-second microseconds.
    int64_t sec = us_epoch / 1000000LL;
    int64_t sub = us_epoch % 1000000LL;
    if (sub < 0) { sub += 1000000LL; sec -= 1; }
    us = static_cast<int>(sub);

    // Floor-divide into whole days + time-of-day seconds.
    int64_t days = sec / 86400LL;
    int64_t sod  = sec % 86400LL;
    if (sod < 0) { sod += 86400LL; days -= 1; }

    h  = static_cast<int>(sod / 3600);
    mi = static_cast<int>((sod % 3600) / 60);
    s  = static_cast<int>(sod % 60);

    // Howard Hinnant civil_from_days → (year, month, day).
    int64_t z   = days + 719468LL;
    int64_t era = (z >= 0 ? z : z - 146096LL) / 146097LL;
    int64_t doe = z - era * 146097LL;
    int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int64_t y64 = yoe + era * 400LL;
    int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int64_t mp  = (5 * doy + 2) / 153;
    d  = static_cast<int>(doy - (153 * mp + 2) / 5 + 1);
    mo = static_cast<int>(mp < 10 ? mp + 3 : mp - 9);
    y  = static_cast<int>(y64 + (mo <= 2 ? 1 : 0));
}

// ---------------------------------------------------------------------------
// Unit-conversion helpers.
// ---------------------------------------------------------------------------

// Raw instant value (in the column's unit) → microseconds since epoch.
// For NANOSECONDS: truncates sub-microsecond precision (Python datetime is
// at most microsecond-precise).
static inline int64_t ts_to_us(int64_t raw, TimestampUnit unit) noexcept {
    switch (unit) {
        case TimestampUnit::SECONDS:      return raw * 1000000LL;
        case TimestampUnit::MILLISECONDS: return raw * 1000LL;
        case TimestampUnit::MICROSECONDS: return raw;
        case TimestampUnit::NANOSECONDS:  return raw / 1000LL;
    }
    return raw;
}

// Microseconds since epoch → raw instant value in the given unit.
// For NANOSECONDS: the Python datetime source has only microsecond precision,
// so the output is microseconds * 1000 (nanosecond part is always 0 on input
// from Python datetime, but round-trip is exact at microsecond granularity).
static inline int64_t us_to_ts(int64_t us, TimestampUnit unit) noexcept {
    switch (unit) {
        case TimestampUnit::SECONDS:      return us / 1000000LL;
        case TimestampUnit::MILLISECONDS: return us / 1000LL;
        case TimestampUnit::MICROSECONDS: return us;
        case TimestampUnit::NANOSECONDS:  return us * 1000LL;
    }
    return us;
}
