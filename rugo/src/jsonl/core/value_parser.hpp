#ifndef _JSONL_VALUE_PARSER_HPP_
#define _JSONL_VALUE_PARSER_HPP_

#include <cstdint>
#include <string>
#include <optional>
#include "markers.hpp"
#include "parse_context.hpp"

namespace rugo::_jsonl {

// Parse value from buffer given FieldSpan bounds
// Returns true if parsing succeeded, false if malformed
bool parse_int64(const uint8_t* buffer, uint32_t start, uint32_t end, int64_t& out);
bool parse_float64(const uint8_t* buffer, uint32_t start, uint32_t end, double& out);
bool parse_bool(const uint8_t* buffer, uint32_t start, uint32_t end, bool& out);

// Extract string value (unescaping not performed; raw bytes between quotes)
std::string extract_string(const uint8_t* buffer, uint32_t start, uint32_t end);

// Check if value is null
bool is_null(const uint8_t* buffer, uint32_t start, uint32_t end);

// Parse bool with out parameter (Cython-friendly)
bool parse_bool_wrapper(const uint8_t* buffer, uint32_t start, uint32_t end, bool& out);
inline bool parse_bool_wrapper(const uint8_t* buffer, uint32_t start, uint32_t end, bool& out) {
    return parse_bool(buffer, start, end, out);
}

// Parse pred.value ONCE, as its literal kind only (pred.kind: int/float/bool; a
// string literal needs no parse), and cache the result on the Predicate. Call once
// per predicate before the per-record evaluation loop — evaluate_predicate() reads
// the cached fields instead of re-parsing pred.value on every call.
void prepare_predicate(Predicate& pred);

// "JSON string", "JSON number", ... for a FieldSpan::type, for error messages.
const char* json_value_kind_name(uint8_t value_type);

// Whether a literal of `kind` (rugo::LiteralKind) can be compared with a JSON value
// of `value_type`: string with string, number with int/float, boolean with bool.
// A JSON array or object compares with nothing. JSON null is the caller's to handle.
bool literal_fits_json_value(uint8_t value_type, uint8_t kind);

// Evaluate pred against a present field value. pred must have been passed through
// prepare_predicate() first (its numeric cache is read, not computed here). SQL
// three-valued logic: a JSON null satisfies only IS NULL — and an empty NOT IN, which
// asks no comparison. Throws std::invalid_argument when the field's JSON kind cannot
// be compared with the literal's kind (literal_fits_json_value) — fail loud, never
// "no match".
bool evaluate_predicate(
    const uint8_t* buffer,
    const FieldSpan& value_span,
    const Predicate& pred
);

// Whether a record that does NOT carry pred's column at all passes pred. An absent key
// is a NULL cell, so only IS NULL and an empty NOT IN accept it.
inline bool predicate_accepts_absent(const Predicate& pred) {
    return pred.op == 8 || (pred.op == 7 && pred.members.empty());
}

}  // namespace rugo::_jsonl

#endif  // _JSONL_VALUE_PARSER_HPP_
