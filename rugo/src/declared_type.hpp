#pragma once
// rugo/src/declared_type.hpp — the type vocabulary a caller may declare in a
// reader's `explicit_schema`, and the strict per-value parse for each entry.
//
// WHY THIS EXISTS
// A caller that already knows the destination schema (the upload service reads it
// from the catalog) should be able to have values parsed straight into the right
// Draken type. Before this, the vocabulary was four names — "int64", "double",
// "boolean", "string" — so a column the catalog declares IPV4 could only be
// inferred and then cast, and inference silently produced INT64 (numeric JSON) or
// VARCHAR (dotted-quad JSON) instead.
//
// SPELLING: the PLATFORM'S CANONICAL NAMES (architect's ruling, 2026-08-18).
// The accepted names are exactly what `str(ColumnType)` writes into a stored
// schema — IPV4, UINT32, DECIMAL(18, 2), TIMESTAMP[us], DATE — so a caller passes
// the catalog's own type string through unchanged, with no translation table in
// between (a translation table is where a UINT32 and an IPV4 stop agreeing).
// Matching is case-insensitive and leading/trailing space is trimmed.
//
// This mirrors `try_parse_column_type` in opteryx/types/logical_type.py, which is
// the inverse of `str(ColumnType)`. rugo is deliberately opteryx-free, so the
// grammar is restated here rather than imported; keep the two in step. The
// physical tags and the LogicalKind ordinals come from draken, which is shared,
// so only the NAMES are duplicated — not the meanings.
//
// The four original names keep working unchanged and always will: "int64",
// "double", "boolean" and "string" all resolve through the canonical table and
// the alias table below (INT64; DOUBLE -> FLOAT64; BOOLEAN -> BOOL; STRING ->
// VARCHAR), case-insensitively, so no existing caller changes.
//
// STRICTNESS IS THE POINT, and it is not softened by widening the vocabulary. A
// declared column is parsed STRICTLY as that type: no speculative inference, no
// widening, no fallback to VARCHAR. A value that does not fit is an error that
// names the column and the row. A malformed address must FAIL, never quietly
// become NULL or 0 — that is the whole reason a caller declares a schema.
//
// Every text form routes through draken's own parser for that type
// (draken/core/ipv4.h, core/iso_datetime.h, core/decimal_text.h), so a value read
// here and the same value CAST in a query cannot disagree about what it means.
// For IPv4 that is a security property, not a tidiness one: shorthand ("10.1")
// and leading zeros ("010.1.1.1") are refused because a reader and an access rule
// disagreeing about which address a string denotes is a security bug.

#include <cstdint>
#include <string>

#include "buffers.h"   // DrakenType

namespace rugo {

// A resolved declared type: the physical tag plus the logical descriptor that
// refines it. The descriptor fields are the ENUM ORDINALS from
// draken/logical_type.h, matching draken_vector_own_raw_logical's contract, so
// this struct can be handed to the bridge without a second translation.
struct DeclaredType {
    DrakenType type           = DRAKEN_VARCHAR;
    uint8_t    logical_kind   = 0;   // LogicalKind: 0 NONE, 1 TIMESTAMP, 3 DECIMAL, 5 IPV4
    uint8_t    unit           = 2;   // TimestampUnit: 0 s, 1 ms, 2 us, 3 ns
    int16_t    offset_minutes = 0;
    uint8_t    precision      = 0;   // DECIMAL only
    uint8_t    scale          = 0;   // DECIMAL only
};

// LogicalKind ordinals, restated so this header does not have to pull in
// logical_type.h (which brings <deque>, <mutex> and the interning registry).
// draken/logical_type.h is the definition; these must not drift from it.
constexpr uint8_t LK_NONE      = 0;
constexpr uint8_t LK_TIMESTAMP = 1;
constexpr uint8_t LK_DECIMAL   = 3;
constexpr uint8_t LK_IPV4      = 5;

// Resolve a declared type name. Returns false if `name` is not a type a reader
// can parse values into — which includes names that ARE valid opteryx types but
// have no strict scalar text form here (TIME, INTERVAL, VECTOR, ARRAY, VARIANT,
// NULL). Those are refused rather than approximated.
bool parse_declared_type(const std::string& name, DeclaredType* out);

// Human-readable list of what `parse_declared_type` accepts, for error messages.
// One place, so the error can never list a vocabulary the parser does not have.
const char* declared_type_vocabulary();

}  // namespace rugo
