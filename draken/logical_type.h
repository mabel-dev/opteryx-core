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

#include "core/buffers.h"   // DrakenType, draken_type_fixed_itemsize

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
// IPV4      = DRAKEN_UINT32 reinterpreted as an IPv4 address.
//
// IPV4 is the first kind that REFINES an otherwise-unparameterized physical
// type rather than completing a parameterized one.  TIMESTAMP/TIME/DECIMAL/
// VECTOR are mandatory — their physical tag is uninterpretable without the
// descriptor.  IPV4 is optional: a DRAKEN_UINT32 vector with no descriptor is
// a perfectly well-formed unsigned integer column, and one carrying
// LogicalKind::IPV4 is the SAME 32 bits with a narrower meaning.
//
// Consequences, which every consumer must respect:
//   - Dispatch stays on the PHYSICAL tag.  IPv4 sorts, groups, joins, hashes
//     and compares as UINT32, which is exactly correct for IPv4 ordering.
//   - Only the value-rendering and cast edges read this kind.  Nothing in a
//     hot loop may branch on it.
//   - Dropping the descriptor degrades an IPv4 column to UINT32 — a display
//     and cast regression, never a wrong answer.
//
// No parameter fields are used: the prefix length is NOT carried on the value
// (unlike a Postgres `inet`).  Prefix length is always an operand of the
// operation that needs it (see IP_TRUNC / `<<=`).
// ---------------------------------------------------------------------------
enum class LogicalKind : uint8_t {
    NONE      = 0,
    TIMESTAMP = 1,
    TIME      = 2,
    DECIMAL   = 3,
    VECTOR    = 4,  // DRAKEN_VECTOR_FP16: carries dimension parameter.
    IPV4      = 5,  // DRAKEN_UINT32: dotted-decimal rendering; no parameters.
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
// Interned registry.
//
// Returns a BORROWED pointer to the canonical LogicalType instance for lt.
// Two calls with equal lt values return the same pointer.
//
// Storage: std::deque<LogicalType> — push_back never invalidates existing
// element addresses (unlike std::vector), so borrowed pointers stay stable.
// The registry never moves or frees entries, so a borrowed pointer remains
// valid (and lock-free to dereference) for the process lifetime.
//
// LINKAGE — load-bearing, do not add `static`.
//
// This function is `inline`, NOT `static inline`. `static` at namespace scope
// gives INTERNAL linkage, which would give every translation unit including
// this header its own copy of the function AND its own function-local
// `registry` — so two vectors interned from different .cpp files would carry
// different pointers for the same descriptor, and the identity guarantee below
// would silently be false. It was `static inline` until 2026-08-04; nothing
// compared these pointers at the time, so the defect was latent rather than
// live. Plain `inline` has external linkage and the standard then guarantees
// the function-local statics are ONE entity across every TU in the module.
//
// SCOPE OF THE IDENTITY GUARANTEE: pointer equality ⟹ descriptor equality
// holds within a linked module. It does NOT extend across extension `.so`
// boundaries in general — that depends on symbol visibility and load flags
// (RTLD_GLOBAL + -fvisibility=default merges them on Linux; macOS's two-level
// namespace need not). This mirrors the rule vector_alloc.h already states for
// draken_identity_sel/draken_zero_sel: each extension may hold its own copy,
// so cross-module code MUST compare descriptors BY VALUE (operator== is
// provided) and MUST NOT compare pointers. Within one module, pointer
// comparison is exact and cheap.
//
// Thread-safety: guarded by a mutex. Reachable off-GIL via vecresult_to_owner
// (e.g. take/mask/slice on a TIMESTAMP64 column inside a gil_scoped_release
// window), so the GIL cannot be relied on to serialise the iterate+push_back.
// The mutex protects only the lookup/insert.
// ---------------------------------------------------------------------------
inline const LogicalType* logical_type_intern(const LogicalType& lt) {
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
// Physical width (bytes) of ONE element, for the row-store style copies that
// materialize a column value-by-value (join build payload, sort/group-by row
// gather, vector concat).
//
// This is the descriptor-aware layer over draken_type_fixed_itemsize (buffers.h).
// Almost every fixed-width type's stride is decided by the physical tag alone and
// comes straight from that canonical table. VECTOR_FP16 is the exception: its
// `data` is a flat uint16 array strided by the embedding dimension, and the
// dimension lives ONLY in the logical descriptor — the physical tag cannot answer
// it. Callers hold the descriptor at every one of these sites, so this takes it
// rather than each consumer growing its own FP16 special case.
//
// Returns 0 for "no flat per-element width", which every caller must treat as
// unsupported-here: the bit-packed (BOOL), arena-backed (string family) and
// child-vector (ARRAY) families all carry their own materialization and must be
// intercepted BEFORE this is reached.
//
// A VECTOR_FP16 column with a missing or zero-dimension descriptor is broken, not
// zero-width — dimension is mandatory for the parameterized physical types (see
// the header comment above). It returns 0 so the caller fails loud rather than
// materializing an uninterpretable column.
// ---------------------------------------------------------------------------
static inline size_t draken_type_itemsize(DrakenType t, const LogicalType* lt) {
    if (t == DRAKEN_VECTOR_FP16) {
        if (lt == nullptr || lt->dimension == 0u) return 0u;
        return static_cast<size_t>(lt->dimension) * sizeof(uint16_t);
    }
    return draken_type_fixed_itemsize(t);
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
