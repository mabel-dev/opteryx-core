#pragma once
// rugo/src/predicate_literal.hpp — which pushed predicate literals a column can
// be compared against. Shared by the CSV and JSONL readers so the two cannot
// disagree about it.
//
// THE CONTRACT: FAIL LOUD. A predicate whose literal is of a different kind from
// the column raises (std::invalid_argument -> ValueError at the Cython edge),
// naming the column, its type and the literal. It is never answered with "no
// rows", and never coerced: `s = 1` on a VARCHAR column and `a = '1'` on an
// INT64 column are both errors. This is the same stance rugo.parquet's
// _check_predicate_values takes for None and for BOOLEAN columns.
//
//   column family                     accepts literal kind
//   ---------------------------------------------------------------
//   string  (VARCHAR/NVARCHAR/VARBINARY)   STRING (Python str / bytes)
//   numeric (INT*/UINT*/FLOAT*)            INT or FLOAT (never bool)
//   BOOL                                   BOOL
//   UUID (declared)                        STRING that is a UUID (strict 8-4-4-4-12)
//   anything else (DATE, TIMESTAMP, DECIMAL, IPV4, ARRAY, VARIANT, ...) — NOT COVERED: the
//     reader's existing comparison applies, unchanged.
//
// UUID is compared on its 16 BYTES, not left to "the existing comparison": that
// is on the field's raw text, and UUID text is case-insensitive, so `= 'ffff...'`
// against a stored 'FFFF...' would answer "no rows". The literal is parsed once
// (uuid_literal_bytes; a non-UUID string raises) and each field with the same
// strict parser; byte order is the type's order, so all six comparisons hold.
//
// The literal's kind is its PYTHON type, decided once at the Cython edge — not
// sniffed from its text, which is what let the string '1' compare as a number.
//
// WHERE EACH READER CHECKS IT
//   CSV   — against every predicate column's sniffed (or declared) type, before
//           a single row is filtered. A predicate on a column the file does not
//           have is refused too.
//   JSONL — against a DECLARED column's type up front; against every non-null
//           value in the head sample (the records column types are inferred
//           from) before filtering; and against every value a predicate is
//           evaluated on. JSONL has per-VALUE kinds, so the rule there is
//           literal vs JSON kind: string with string, number with int/float,
//           boolean with bool — and an array or object with nothing. A column
//           of mixed JSON kinds therefore raises for any literal. An absent key
//           is a NULL, not an unknown column: it matches no comparison.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include "buffers.h"            // DrakenType
#include "declared_parse.hpp"   // declared_is_string

namespace rugo {

enum LiteralKind : uint8_t {
    LITERAL_STRING = 0,
    LITERAL_INT    = 1,
    LITERAL_FLOAT  = 2,
    LITERAL_BOOL   = 3,
};

inline const char* literal_kind_name(uint8_t kind) noexcept {
    switch (kind) {
        case LITERAL_STRING: return "string";
        case LITERAL_INT:    return "int";
        case LITERAL_FLOAT:  return "float";
        case LITERAL_BOOL:   return "bool";
    }
    return "unknown";
}

inline bool type_is_numeric(DrakenType t) noexcept {
    switch (t) {
        case DRAKEN_INT8: case DRAKEN_INT16: case DRAKEN_INT32: case DRAKEN_INT64:
        case DRAKEN_UINT8: case DRAKEN_UINT16: case DRAKEN_UINT32: case DRAKEN_UINT64:
        case DRAKEN_FLOAT32: case DRAKEN_FLOAT64:
            return true;
        default:
            return false;
    }
}

// True when the contract above covers a column of physical type `t` with
// logical kind `logical_kind` (declared_type.hpp's LK_*). Any logical refinement
// takes the column out of the contract: IPV4 shares UINT32's physical tag, and
// must not be read as a number.
inline bool literal_contract_covers(DrakenType t, uint8_t logical_kind) noexcept {
    if (logical_kind != LK_NONE) return false;
    return declared_is_string(t) || type_is_numeric(t) || t == DRAKEN_BOOL;
}

// True when a literal of `kind` may be compared with the column. Types outside
// the contract accept every kind (their comparison is unchanged).
inline bool literal_fits_type(DrakenType t, uint8_t logical_kind, uint8_t kind) noexcept {
    if (t == DRAKEN_UUID) return kind == LITERAL_STRING;
    if (!literal_contract_covers(t, logical_kind)) return true;
    if (declared_is_string(t)) return kind == LITERAL_STRING;
    if (type_is_numeric(t))    return kind == LITERAL_INT || kind == LITERAL_FLOAT;
    if (t == DRAKEN_BOOL)      return kind == LITERAL_BOOL;
    return true;
}

// The error for a literal that does not fit its column. `type_name` is the
// column's type as the caller would recognise it (declared name, or the sniffed
// / inferred type).
inline std::string literal_mismatch_message(const std::string& column,
                                            const std::string& type_name,
                                            uint8_t kind,
                                            const std::string& literal) {
    return "predicate on column '" + column + "' (" + type_name + ") cannot take the " +
           literal_kind_name(kind) + " value " +
           (kind == LITERAL_STRING ? "'" + literal + "'" : literal) +
           "; a predicate literal must be of the column's type — it is not coerced";
}

// UUID predicates: the literal's 16 bytes, parsed once with the reader's strict
// parser (draken/core/uuid_text.h). Throws naming the column for a non-UUID literal.
inline void uuid_literal_bytes(const std::string& column, const std::string& literal,
                               uint8_t* out) {
    if (!draken::ops::uuid_parse(literal.data(), literal.size(), out))
        throw std::invalid_argument("predicate on column '" + column + "' (UUID): '" + literal +
                                    "' is not a UUID (expected 8-4-4-4-12 hex digits)");
}

// op: 0=EQ 1=NE 2=LT 3=LE 4=GT 5=GE, on 16 bytes in network order (== UUID order).
inline bool apply_op_uuid(uint8_t op, const uint8_t* field, const uint8_t* literal) noexcept {
    const int c = std::memcmp(field, literal, 16);
    switch (op) {
        case 0: return c == 0;
        case 1: return c != 0;
        case 2: return c < 0;
        case 3: return c <= 0;
        case 4: return c > 0;
        case 5: return c >= 0;
    }
    return false;
}

}  // namespace rugo
