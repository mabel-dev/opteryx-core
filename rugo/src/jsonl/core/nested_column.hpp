#ifndef _JSONL_NESTED_COLUMN_HPP_
#define _JSONL_NESTED_COLUMN_HPP_

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "markers.hpp"
#include "parse_context.hpp"

// Nested column requests: one level into a top-level object, named with SQL's own
// extraction notation — `key->>'sub'` (the value as text) and `key->'sub'` (the value as
// JSON). The record walk (interpreter.cpp) emits a span for the sub-value directly, so the
// container is never materialised and never re-parsed downstream.
//
// A nested span's key bytes are its CONTAINER's key (`key`), which every nested column on
// that container shares, so spans are told apart by FieldSpan::slot instead: each distinct
// nested request gets a slot 1..255, assigned deterministically from the ParseContext so
// the map builder (which tags spans) and the column builder (which reads them) agree
// without sharing any state. Top-level fields carry slot 0.

namespace rugo::_jsonl {

struct ColumnSpec {
    std::string key;        // the top-level key (the whole name when not nested)
    std::string sub;        // the key inside it (nested only)
    bool nested  = false;
    bool as_json = false;   // `->`: JSON text; `->>`: text, a JSON string unquoted
};

// Parse a single-quoted SQL literal occupying s[at..] to the END of `s` ('' escapes a
// quote). False if s[at] is not a quote or anything trails the closing quote.
inline bool parse_quoted_literal_to_end(const std::string& s, size_t at, std::string& out) {
    if (at >= s.size() || s[at] != '\'') return false;
    out.clear();
    size_t i = at + 1;
    while (i < s.size()) {
        if (s[i] == '\'') {
            if (i + 1 < s.size() && s[i + 1] == '\'') { out.push_back('\''); i += 2; continue; }
            return i + 1 == s.size();
        }
        out.push_back(s[i]);
        ++i;
    }
    return false;  // unterminated
}

// `key->>'sub'` / `key->'sub'` -> nested; anything else is a plain top-level name. The
// first `->`/`->>` whose remainder is exactly one quoted literal wins, so a key that
// itself contains `->` still parses. An empty key or sub-key is refused rather than read
// as a plain name: it is a malformed request, not a column called that.
inline ColumnSpec parse_column_spec(const std::string& s) {
    ColumnSpec c;
    for (size_t p = s.find("->"); p != std::string::npos; p = s.find("->", p + 1)) {
        const bool text = (p + 2 < s.size() && s[p + 2] == '>');
        std::string sub;
        if (!parse_quoted_literal_to_end(s, p + (text ? 3 : 2), sub)) continue;
        if (p == 0 || sub.empty())
            throw std::invalid_argument(
                "read_jsonl: nested column '" + s + "' needs a non-empty key and sub-key");
        c.key = s.substr(0, p);
        c.sub = std::move(sub);
        c.nested = true;
        c.as_json = !text;
        return c;
    }
    c.key = s;
    return c;
}

// The slot of `spec` if it is a nested request, else 0. Slots number the DISTINCT nested
// requests in order of first appearance across the projected columns, then the predicate
// columns — the same order interpret_jsonl builds its wanted set in.
inline uint8_t nested_slot(const ParseContext& ctx, const std::string& spec) {
    std::vector<const std::string*> seen;
    auto visit = [&](const std::string& name) -> int {
        for (size_t k = 0; k < seen.size(); ++k)
            if (*seen[k] == name) return static_cast<int>(k);
        if (!parse_column_spec(name).nested) return -1;
        if (seen.size() == 255)
            throw std::invalid_argument("read_jsonl: more than 255 nested columns requested");
        seen.push_back(&name);
        return static_cast<int>(seen.size() - 1);
    };
    int found = -1;
    for (const auto& c : ctx.projected_columns) {
        const int k = visit(c);
        if (found < 0 && k >= 0 && c == spec) found = k;
    }
    for (const auto& p : ctx.predicates) {
        const int k = visit(p.column);
        if (found < 0 && k >= 0 && p.column == spec) found = k;
    }
    return found < 0 ? uint8_t(0) : static_cast<uint8_t>(found + 1);
}

}  // namespace rugo::_jsonl

#endif  // _JSONL_NESTED_COLUMN_HPP_
