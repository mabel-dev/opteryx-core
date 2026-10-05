#include "interpreter.hpp"
#include "nested_column.hpp"
#include "json_array_walker.hpp"   // escaped nested-key comparison
#include "field_span.hpp"
#include "value_parser.hpp"   // evaluate_predicate (inline filter pushdown)
#include "predicate_literal.hpp" // check_predicate_literals: literal vs column type contract
#include "declared_type.hpp"     // check_predicate_literals: declared column types
#include "structural_scan.hpp" // scan_structural_index (build_map's windows)
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace rugo::_jsonl {

namespace {

// Fast whitespace test (no locale overhead)
inline bool is_ws(uint8_t c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

// One window of the masked structural index (structural_scan.hpp): `ix[0, n)` are
// absolute positions into `buf`, and a position's byte is its kind.
struct StructuralWindow {
    const uint8_t*  buf;
    const uint32_t* ix;
    size_t          n;
    inline uint8_t at(size_t i) const { return buf[ix[i]]; }
};

// Depth walk from index `from` with depth `depth` already open: the index of the bracket
// or brace that brings it to 0, or of the first newline, or w.n — whichever comes first.
// The index is masked, so every bracket in it is structure: quotes and the strings they
// delimit cannot hold one, and no string or escape state is tracked (simdjson's
// skip_child).
inline size_t depth_walk(const StructuralWindow& w, size_t from, int depth) {
    for (size_t j = from; j < w.n; ++j) {
        const uint8_t c = w.at(j);
        if (c == '{' || c == '[') ++depth;
        else if (c == '}' || c == ']') { if (--depth == 0) return j; }
        else if (c == '\n') return j;
    }
    return w.n;
}

// Bound a container value whose opening '[' or '{' is entry `open_idx`.
//
// On success sets `closed`, `close_pos` to the matching close bracket/brace, and returns
// its index. On a truncated/unterminated container returns w.n with `close_pos = limit -
// 1`; on ANY newline before the close — inside a nested string (invalid JSON — RFC 8259
// requires the two bytes '\'+'n', not this control byte; confirmed against a real defect
// in the JSONBench Bluesky dump, see tests/performance/jsonbench/README.md's "Known
// data-quality defect"), escaped, or between members — returns the newline's index with
// `close_pos` at the newline: a JSONL record never spans lines, so the container is
// unterminated on its line.
inline size_t bound_container(const StructuralWindow& w, size_t open_idx, uint32_t limit,
                              bool& closed, uint32_t& close_pos) {
    const size_t j = depth_walk(w, open_idx, 0);
    if (j == w.n)          { closed = false; close_pos = limit - 1; return j; }
    if (w.at(j) == '\n')   { closed = false; close_pos = w.ix[j];   return j; }
    closed = true;
    close_pos = w.ix[j];
    return j;
}

// Coarse value-type tag from the first non-whitespace byte of the slice.
// The structural pass only assigns this hint; the value reader does the real
// parse (and validates / falls back). " is handled on its own marker path.
inline ValueType classify_first(uint8_t c) {
    switch (c) {
        case '"': return ValueType::String;
        case '{': return ValueType::Object;
        case '[': return ValueType::Array;
        case 't':
        case 'f': return ValueType::Boolean;
        case 'n': return ValueType::Null;
        default:  return ValueType::Integer;  // digit, '-', or unexpected
    }
}

// Is [p, p+n) exactly one JSON scalar token (RFC 8259): `true`, `false`, `null`, or a
// number -?(0|[1-9][0-9]*)(.[0-9]+)?([eE][+-]?[0-9]+)? ? Strings and containers are
// recognised by their own delimiters and never reach here.
inline bool valid_scalar(const uint8_t* p, uint32_t n) {
    switch (p[0]) {
        case 't': return n == 4 && std::memcmp(p, "true", 4) == 0;
        case 'f': return n == 5 && std::memcmp(p, "false", 5) == 0;
        case 'n': return n == 4 && std::memcmp(p, "null", 4) == 0;
        default: break;
    }
    auto digit = [](uint8_t c) { return c >= '0' && c <= '9'; };
    uint32_t i = 0;
    if (p[i] == '-') ++i;
    if (i >= n) return false;
    if (p[i] == '0') ++i;
    else if (p[i] >= '1' && p[i] <= '9') { while (i < n && digit(p[i])) ++i; }
    else return false;
    if (i < n && p[i] == '.') {
        if (++i >= n || !digit(p[i])) return false;
        while (i < n && digit(p[i])) ++i;
    }
    if (i < n && (p[i] == 'e' || p[i] == 'E')) {
        if (++i < n && (p[i] == '+' || p[i] == '-')) ++i;
        if (i >= n || !digit(p[i])) return false;
        while (i < n && digit(p[i])) ++i;
    }
    return i == n;
}

// Does the escaped key body [k, k+klen) — the bytes between its quotes — decode to exactly
// `sub`? A body that is not a valid JSON string never matches.
inline bool escaped_key_equals(const uint8_t* k, uint32_t klen, const char* sub, uint32_t sub_len) {
    const uint8_t* cur = k;
    JsonArrayElement e;
    if (!jsonarr::scan_string(cur, k + klen + 1, e) || cur != k + klen + 1) return false;
    if (e.str_decoded_len != sub_len) return false;
    std::vector<uint8_t> decoded(sub_len);
    jsonarr::decode_string(e.str_raw, e.str_raw_len, decoded.data());
    return sub_len == 0 || std::memcmp(decoded.data(), sub, sub_len) == 0;
}

// Find a ONE-LEVEL nested key inside an already-bounded object container and report its
// value's span. `open_idx`/`close_idx` are the container's own index entries (its '{' and
// matching '}') as returned by bound_container. Walks only the index — the same bytes
// the SIMD scan already classified — so reading `commit.collection` costs a fraction of the
// container's extent instead of materialising it and re-parsing it per row downstream.
//
// DEPTH SAFETY IS STRUCTURAL, not tracked: `commit.collection` must never match
// `commit.record.collection`, and here it cannot, because any nested container met in value
// position is skipped WHOLESALE via bound_container (j jumps to its close). The walk
// therefore only ever sees depth-1 keys — there is no depth counter to get wrong.
//
// Value semantics mirror the top-level path exactly (END_STRING_VAL / emit_unquoted /
// emit_container in MapBuilder), because a nested projection must be byte-identical to what
// the downstream column extraction would have produced for the same path:
//   string    -> the content BETWEEN the quotes (unquoted), ValueType::String
//   container -> the whole `{...}` / `[...]` slice as JSON text
//   scalar    -> the ws-trimmed slice, coarse-classified by first byte
// Returns false when the container is not an object, the key is absent, or the value is
// JSON null — all of which mean a NULL output cell.
inline bool find_nested_field(
    const StructuralWindow& w,
    size_t open_idx,
    size_t close_idx,
    const char* sub,
    uint32_t sub_len,
    uint8_t sub_first,
    uint32_t& out_start,
    uint32_t& out_width,
    ValueType& out_type) {

    const uint8_t* buf = w.buf;
    if (w.at(open_idx) != '{') return false;  // an array has no keys

    // Close an unquoted scalar running from the ':' to `end` (exclusive terminator).
    auto emit_scalar = [&](uint32_t colon_pos, uint32_t end) -> bool {
        uint32_t vs = colon_pos + 1;
        while (vs < end && is_ws(buf[vs])) ++vs;
        if (vs >= end) return false;                      // empty => NULL cell
        uint32_t ve = end - 1;
        while (ve > vs && is_ws(buf[ve])) --ve;
        // Exactly `null` is a NULL cell. Anything else starting with 'n' is NOT JSON and is
        // emitted as-is, so the column builder's strict check refuses it loudly rather than
        // it passing as a NULL.
        if (ve - vs + 1 == 4 && std::memcmp(buf + vs, "null", 4) == 0) return false;
        out_start = vs;
        out_width = ve - vs + 1;
        out_type  = classify_first(buf[vs]);
        return true;
    };

    enum St : uint8_t { KEY_EXPECT, KEY_IN, COLON_EXPECT, VALUE_EXPECT, VALUE_STR_IN, AFTER_VALUE };
    St st = KEY_EXPECT;
    bool wanted = false;
    uint32_t key_start = 0, val_start = 0, colon_pos = 0;
    const uint32_t close_byte = w.ix[close_idx];

    // The index is masked: the entry after a string's opening quote is its closing quote,
    // so KEY_IN and VALUE_STR_IN end on the next quote with no escape tracking.
    for (size_t j = open_idx + 1; j < close_idx; ++j) {
        const uint32_t p = w.ix[j];
        const uint8_t t = buf[p];

        switch (st) {
        case KEY_EXPECT:
            if (t == '"') { key_start = p + 1; st = KEY_IN; }
            break;

        case KEY_IN:
            if (t == '"') {
                const uint32_t klen = p - key_start;
                // Keys compare DECODED, as yyjson's object lookup (draken's `->>`) does:
                // `"col\u006cection"` IS the key `collection`. Only a key that actually
                // carries an escape pays for the decode.
                // Decoding only ever shrinks a key, so one shorter than `sub` can never be
                // it, escaped or not — and needs no scan for a backslash.
                bool key_escaped = false;
                if (klen >= sub_len)
                    for (uint32_t k = 0; k < klen && !key_escaped; ++k) key_escaped = buf[key_start + k] == '\\';
                wanted = klen < sub_len ? false : key_escaped
                    ? escaped_key_equals(buf + key_start, klen, sub, sub_len)
                    : (klen == sub_len && buf[key_start] == sub_first &&
                       std::memcmp(buf + key_start, sub, sub_len) == 0);
                st = COLON_EXPECT;
            }
            break;

        case COLON_EXPECT:
            if (t == ':') { colon_pos = p; st = VALUE_EXPECT; }
            break;

        case VALUE_EXPECT:
            if (t == '"') {
                val_start = p + 1; st = VALUE_STR_IN;
            } else if (t == '{' || t == '[') {
                bool closed = false;
                uint32_t cpos = 0;
                const size_t cidx = bound_container(w, j, close_byte + 1, closed, cpos);
                if (!closed || cidx >= close_idx) return false;   // malformed/overrunning
                if (wanted) {
                    out_start = p;
                    out_width = cpos - p + 1;
                    out_type  = (t == '[') ? ValueType::Array : ValueType::Object;
                    return true;
                }
                j  = cidx;          // skip the whole container — this is the depth guard
                st = AFTER_VALUE;
            } else if (t == ',') {
                if (wanted) return emit_scalar(colon_pos, p);
                st = KEY_EXPECT;
            }
            break;

        case VALUE_STR_IN:
            if (t == '"') {
                if (wanted) {
                    out_start = val_start;
                    out_width = p - val_start;
                    out_type  = ValueType::String;
                    return true;
                }
                st = AFTER_VALUE;
            }
            break;

        case AFTER_VALUE:
            if (t == ',') st = KEY_EXPECT;
            break;
        }
    }

    // The wanted key was the container's LAST member and its unquoted scalar value is
    // terminated by the closing '}' rather than by a comma.
    if (st == VALUE_EXPECT && wanted) return emit_scalar(colon_pos, close_byte);
    return false;
}

} // anonymous namespace

// Document-map builder, in one of two sinks chosen at compile time:
//
//   kColumns = true  — the bulk read (build_columns). Each wanted column's value is
//                      captured as the record is parsed, and when the record closes one
//                      span per output column (absent_span when not carried) is appended
//                      to the ColumnMap. Minimal extent: once every wanted column is
//                      resolved, nothing more is materialised for the record.
//   kColumns = false — the head sample (build_map): every field of every record, row-major.
//
// Value shape is coarse and read only from the structural delimiter; key identity is never
// hashed. Records are parsed strictly from the masked structural index by parse_record;
// walk_window drives the line discipline around them.
namespace {
template <bool kColumns>
struct MapBuilder {
    RecordSet rs;   // kColumns = false: the row-major map
    ColumnMap cm;   // kColumns = true:  the column-major map
    const uint8_t* buffer;
    uint32_t buffer_length;
    uint32_t key_start = 0, key_width = 0;
    uint32_t value_start = 0, value_width = 0;
    ValueType value_type = ValueType::Unknown;
    uint32_t ordinal = 0;

    // Column capture (kColumns). Per capture slot (WantedColumn::out): `cur` holds the
    // record's value when cur_gen == rec_gen; res_gen == rec_gen once the column is
    // RESOLVED in this record (a value, or nothing to read) — its first occurrence decides,
    // later occurrences are ignored. rec_gen advances per record, so nothing is cleared.
    const MapProjection* proj = nullptr;
    std::vector<FieldSpan> cur;
    std::vector<uint32_t>  cur_gen, res_gen;
    uint32_t rec_gen = 0;
    // Per ordinal: 1 + the wanted column whose key matched there last. NDJSON holds key
    // order stable across records, so the steady state is one compare per key, however
    // wide the projection.
    std::vector<uint16_t> key_hint;
    // One-lookup rejection of a key no wanted column can match: its first byte, and its
    // width (bit w of `width_mask` for widths < 64; wider keys skip the width test).
    bool first_wanted[256] = {};
    uint64_t width_mask = 0;
    bool any_wide = false;
    size_t num_wanted = 0;
    size_t found = 0;
    // The matched wanted column (head of its same-key chain) for the key in hand, or
    // nullptr when the key is not wanted.
    const WantedColumn* cur_col = nullptr;
    bool record_dead = false;

    // Early exit (build_columns, kColumns). `prefix_mode`: the index in hand covers only the
    // head of the record's line, and parse_record returns PREFIX_DONE the moment the record
    // needs nothing more. `prefix_ran_out`: the last parse_prefix stopped because the index
    // ended, not because the line is malformed. `tail_from` / `tail_depth`: the first byte
    // after the value that finished the record, and the brackets open there — where
    // check_line_tail takes over.
    bool prefix_mode = false;
    bool prefix_ran_out = false;
    uint32_t tail_from = 0;
    int tail_depth = 0;

    // Line discipline. A JSONL line is a record: whitespace, ONE object whose closing '}'
    // ends it, whitespace. Anything else — content before the '{', a second object or any
    // marker after the '}', a newline before the '}' (truncated record, raw newline in a
    // string), content trailing the '}', or a record that is not well-formed JSON at its
    // top level (parse_record) — makes the WHOLE line malformed: its rows (if any were
    // banked) are rolled back and it is counted once. Judging every line on its own is what
    // makes the outcome independent of where the line sits in the buffer, of chunking and
    // of the projection, and bounds rows by lines.
    //
    // `line_start`: first byte of the current line. `tail_start`: first byte from which
    // only whitespace may follow up to the newline (line_start before a record opens; one
    // past its '}' after it closes). `line_rows_base`: rows banked when the line began —
    // rollback point for reject_line.
    uint32_t line_start = 0;
    uint32_t tail_start = 0;
    size_t   line_rows_base = 0;
    uint32_t cur_record_start_pos = 0;  // position of the current record's '{'
    // Whether this line already opened a record — a second '{' on the line is refused.
    bool saw_open_brace_since_newline = false;
    bool malformed_found = false;
    uint32_t malformed_at = 0;
    uint32_t malformed_count = 0;
    inline void flag_malformed(uint32_t pos) {
        if (!malformed_found) { malformed_found = true; malformed_at = pos; }
        ++malformed_count;
    }

    inline size_t rows_banked() const {
        if constexpr (kColumns) return cm.rows;
        else return rs.offsets.size() - 1;
    }

    // Start a new line at `next` (one past a newline).
    inline void begin_line(uint32_t next) {
        line_start = next;
        tail_start = next;
        line_rows_base = rows_banked();
        saw_open_brace_since_newline = false;
    }

    // The current line is not one valid record: count it once and drop every row it
    // produced — banked (a second object, trailing content) or in progress.
    inline void reject_line() {
        flag_malformed(saw_open_brace_since_newline ? cur_record_start_pos : line_start);
        if constexpr (kColumns) {
            if (cm.rows > line_rows_base) {
                for (size_t c = 0; c < cm.cols.size(); ++c) {
                    auto& col = cm.cols[c];
                    // A copied column's arena rolls back to where the first dropped row's
                    // bytes begin.
                    if (cm.copied[c])
                        for (size_t r = line_rows_base; r < col.size(); ++r)
                            if (!span_absent(col[r])) { cm.arena[c].resize(col[r].value_start); break; }
                    col.resize(line_rows_base);
                }
                cm.rows = line_rows_base;
            }
        } else {
            rs.offsets.resize(line_rows_base + 1);
            rs.spans.resize(rs.offsets.back());
        }
        record_dead = false;
    }

    // Is [from, to) whitespace only?
    inline bool blank(uint32_t from, uint32_t to) const {
        for (uint32_t p = from; p < to; ++p)
            if (!is_ws(buffer[p])) return false;
        return true;
    }

    MapBuilder(const uint8_t* buf, uint32_t buf_len, const MapProjection* p)
        : buffer(buf), buffer_length(buf_len), proj(p) {
        if constexpr (kColumns) {
            size_t max_out = p->ncols;
            for (const WantedColumn& w : *p->columns) max_out = std::max<size_t>(max_out, w.out + 1);
            cur.resize(max_out);
            cur_gen.assign(max_out, 0);
            res_gen.assign(max_out, 0);
            num_wanted = p->columns->size();
            cm.cols.resize(p->ncols);
            cm.arena.resize(p->ncols);
            cm.copied = *p->copy_bytes;
            for (const WantedColumn& w : *p->columns) {
                first_wanted[w.first] = true;
                if (w.len < 64) width_mask |= uint64_t(1) << w.len;
                else any_wide = true;
            }
        } else {
            rs.offsets.push_back(0);
        }
    }

    // The next wanted column sharing the current key (WantedColumn::next), or nullptr.
    inline const WantedColumn* next_in_group(const WantedColumn* w) const {
        return w->next < 0 ? nullptr : &(*proj->columns)[static_cast<size_t>(w->next)];
    }

    // Exact match of the key in hand against the wanted set — a one-lookup reject on the
    // key's first byte and width, the ordinal hint, then length + first-byte reject and
    // memcmp over the set. No hashing. Forced inline: once per top-level key, a call was
    // ~6% of a narrow projection's walk.
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((always_inline))
#endif
    inline void match_key() {
        if constexpr (kColumns) {
            cur_col = nullptr;
            const uint8_t* key = buffer + key_start;
            if (key_width == 0 || !first_wanted[key[0]] ||
                (key_width < 64 ? !((width_mask >> key_width) & 1u) : !any_wide))
                return;
            const std::vector<WantedColumn>& cols = *proj->columns;
            auto eq = [&](const WantedColumn& w) {
                return key_width == w.len && key[0] == w.first && std::memcmp(key, w.name, w.len) == 0;
            };
            if (ordinal < key_hint.size() && key_hint[ordinal] != 0) {
                const WantedColumn& w = cols[key_hint[ordinal] - 1u];
                if (eq(w)) { cur_col = &w; return; }
            }
            for (size_t i = 0; i < cols.size(); ++i) {
                if (!eq(cols[i])) continue;
                cur_col = &cols[i];
                if (ordinal < 0xFFFFu && i < 0xFFFFu) {
                    if (key_hint.size() <= ordinal) key_hint.resize(ordinal + 1, 0);
                    key_hint[ordinal] = static_cast<uint16_t>(i + 1);
                }
                return;
            }
        }
    }

    // Resolve wanted column `w` to a value in this record and evaluate its inline predicate.
    // A column already resolved in this record (a repeated key) is left as it is. Returns
    // true when the record needs no more materialisation (predicate failed, or the last
    // wanted column is now resolved).
    inline bool stage(const WantedColumn* w, uint32_t vs, uint32_t vw, ValueType vt, uint8_t slot) {
        const uint32_t k = w->out;
        if (res_gen[k] == rec_gen) return false;
        res_gen[k] = rec_gen;
        cur[k] = FieldSpan(key_start, key_width, vs, vw, vt, static_cast<uint16_t>(ordinal), slot);
        cur_gen[k] = rec_gen;
        if (w->pred_idx >= 0 &&
            !evaluate_predicate(buffer, cur[k], (*proj->predicates)[w->pred_idx])) {
            record_dead = true;
            return true;
        }
        return ++found >= num_wanted;
    }

    // A wanted column that resolved to NOTHING (nested sub-key absent, its value JSON null,
    // or the key's value is not an object): an absent cell, exactly how a missing top-level
    // key reads. It still counts toward `found`, so minimal extent stops on schedule.
    //
    // Deliberately does NOT kill the record when the column carries a predicate: an absent
    // predicate column is judged when the record closes (predicate_accepts_absent), nested
    // and top-level alike.
    inline bool resolved_missing(const WantedColumn* w) {
        const uint32_t k = w->out;
        if (res_gen[k] == rec_gen) return false;
        res_gen[k] = rec_gen;
        return ++found >= num_wanted;
    }

    // Commit the scalar/string value in hand (value_start/width/type) for every wanted
    // column on this key: a top-level column takes the value; a nested column has nothing
    // to read inside a non-object. Unprojected (the head sample), the field itself. Always
    // advances the ordinal ONCE, so captured spans keep their true object position.
    inline bool commit_field() {
        bool stop = false;
        if constexpr (kColumns) {
            for (const WantedColumn* w = cur_col; w != nullptr && !record_dead; w = next_in_group(w))
                stop |= w->sub_len ? resolved_missing(w) : stage(w, value_start, value_width, value_type, 0);
        } else {
            rs.spans.emplace_back(key_start, key_width, value_start, value_width, value_type,
                                  static_cast<uint16_t>(ordinal), 0);
        }
        ++ordinal;
        return stop;
    }

    // Commit a container value [start .. close] (open/close are its index entries) for
    // every wanted column on this key: a top-level column takes the whole container; a
    // nested column takes its sub-key's value from inside it (find_nested_field), or
    // resolves to nothing when the sub-key is absent / JSON null / the container is an array.
    inline bool commit_container(const StructuralWindow& win,
                                 size_t open_idx, size_t close_idx,
                                 uint32_t start, uint32_t close, ValueType t) {
        if constexpr (!kColumns) {
            value_start = start;
            value_width = close - start + 1;
            value_type = t;
            return commit_field();
        } else {
            bool stop = false;
            for (const WantedColumn* w = cur_col; w != nullptr && !record_dead; w = next_in_group(w)) {
                if (w->sub_len == 0) {
                    stop |= stage(w, start, close - start + 1, t, 0);
                    continue;
                }
                if (res_gen[w->out] == rec_gen) continue;  // a repeated key: already resolved
                uint32_t nstart = 0, nwidth = 0;
                ValueType ntype = ValueType::Unknown;
                stop |= find_nested_field(win, open_idx, close_idx,
                                          w->sub, w->sub_len, w->sub_first, nstart, nwidth, ntype)
                    ? stage(w, nstart, nwidth, ntype, w->slot)
                    : resolved_missing(w);
            }
            ++ordinal;
            return stop;
        }
    }

    // Close the in-progress record as a row. Row-major: its end offset — always, even with
    // zero spans (an empty object `{}` is still one NDJSON row). Column-major: every
    // predicate is judged on the record's captured values (an absent column passes only a
    // predicate that accepts NULL — predicate_accepts_absent); a passing record appends
    // one span per output column. A record with none of the output columns is still a row
    // (all-absent), so every column keeps the same row count.
    inline void bank_record() {
        if constexpr (kColumns) {
            const std::vector<Predicate>& preds = *proj->predicates;
            for (size_t i = 0; i < preds.size(); ++i) {
                const uint32_t k = (*proj->pred_slot)[i];
                const bool present = cur_gen[k] == rec_gen;
                if (present ? !evaluate_predicate(buffer, cur[k], preds[i])
                            : !predicate_accepts_absent(preds[i]))
                    return;
            }
            const FieldSpan none = absent_span();
            for (size_t c = 0; c < cm.cols.size(); ++c) {
                if (cur_gen[c] != rec_gen) { cm.cols[c].push_back(none); continue; }
                if (!cm.copied[c]) { cm.cols[c].push_back(cur[c]); continue; }
                // Copied column: the value's bytes go to the arena now, while the record is
                // still in cache. A string keeps its closing quote after the body, exactly
                // as in the source, for readers that check it (jsoncanon::raw_text_view).
                FieldSpan f = cur[c];
                ByteArena& ar = cm.arena[c];
                const uint32_t at = static_cast<uint32_t>(ar.size());
                const uint32_t quote = f.type == static_cast<uint8_t>(ValueType::String) ? 1u : 0u;
                ar.resize(at + f.value_width + quote);
                std::memcpy(ar.data() + at, buffer + f.value_start, f.value_width + quote);
                f.value_start = at;
                cm.cols[c].push_back(f);
            }
            ++cm.rows;
        } else {
            rs.offsets.push_back(static_cast<uint32_t>(rs.spans.size()));
        }
    }
    // Drop the in-progress record (an inline predicate failed).
    inline void discard_record() {
        if constexpr (!kColumns) rs.spans.resize(rs.offsets.back());
    }

    static constexpr size_t NPOS = static_cast<size_t>(-1);
    // parse_record in prefix_mode: every wanted column is resolved (or an inline predicate
    // failed); the rest of the line is check_line_tail's, from tail_from / tail_depth.
    static constexpr size_t PREFIX_DONE = static_cast<size_t>(-2);

    // Does the nested key body [ks, ks+klen) decode to column `c`'s sub-key? Exactly
    // find_nested_field's comparison.
    inline bool nested_key_eq(uint32_t ks, uint32_t klen, const WantedColumn* c) const {
        if (klen < c->sub_len) return false;
        bool escaped = false;
        for (uint32_t q = 0; q < klen && !escaped; ++q) escaped = buffer[ks + q] == '\\';
        return escaped ? escaped_key_equals(buffer + ks, klen, c->sub, c->sub_len)
                       : (klen == c->sub_len && buffer[ks] == c->sub_first &&
                          std::memcmp(buffer + ks, c->sub, c->sub_len) == 0);
    }

    // Resolve nested column `c` to the unquoted scalar member value running from ':' to
    // `end`: exactly find_nested_field's emit_scalar (empty or `null` = resolved missing).
    inline bool nested_scalar(const WantedColumn* c, uint32_t colon_pos, uint32_t end) {
        uint32_t vs = colon_pos + 1;
        while (vs < end && is_ws(buffer[vs])) ++vs;
        if (vs >= end) return resolved_missing(c);
        uint32_t ve = end - 1;
        while (ve > vs && is_ws(buffer[ve])) --ve;
        if (ve - vs + 1 == 4 && std::memcmp(buffer + vs, "null", 4) == 0) return resolved_missing(c);
        return stage(c, vs, ve - vs + 1, classify_first(buffer[vs]), c->slot);
    }

    // Is every wanted column on the key in hand a nested (`key->>'sub'`) column?
    inline bool chain_all_nested() const {
        if (cur_col == nullptr) return false;
        for (const WantedColumn* c = cur_col; c != nullptr; c = next_in_group(c))
            if (c->sub_len == 0) return false;
        return true;
    }

    // Early exit, nested: read the nested columns on the key in hand in ONE walk of the
    // object whose '{' is entry `open_idx`, WITHOUT bounding it first, so the walk can stop
    // at the value that finishes the record. Value semantics, first-occurrence and depth
    // safety are find_nested_field's (a nested container is skipped whole by
    // bound_container). Returns 1 when the record is finished (tail_from / tail_depth set),
    // 0 when the object closed first (`at` = its closing entry; every chain column it did
    // not carry resolved missing), -1 when the line is malformed or the index ran out at
    // `at`.
    inline int nested_walk(const StructuralWindow& w, size_t open_idx, size_t& at) {
        enum St : uint8_t { KEY_EXPECT, KEY_IN, COLON_EXPECT, VALUE_EXPECT, VALUE_STR_IN, AFTER_VALUE };
        St st = KEY_EXPECT;
        // The chain columns this member's key matched (a chain holds every wanted column on
        // one top-level key; each is matched at most once per record).
        constexpr int kMaxHits = 16;
        const WantedColumn* hit[kMaxHits];
        int nhit = 0;
        uint32_t ks = 0, val_start = 0, colon_pos = 0;
        bool fin = false;
        auto closed_at = [&](size_t j) {
            for (const WantedColumn* c = cur_col; c != nullptr && !record_dead; c = next_in_group(c))
                resolved_missing(c);
            at = j;
            return 0;
        };
        for (size_t j = open_idx + 1; j < w.n; ++j) {
            const uint32_t p = w.ix[j];
            const uint8_t t = buffer[p];
            if (t == '\n') { at = j; return -1; }
            switch (st) {
            case KEY_EXPECT:
                if (t == '"') { ks = p + 1; st = KEY_IN; }
                else if (t == '}' || t == ']') return closed_at(j);
                break;
            case KEY_IN:
                if (t == '"') {
                    nhit = 0;
                    for (const WantedColumn* c = cur_col; c != nullptr; c = next_in_group(c))
                        if (res_gen[c->out] != rec_gen && nested_key_eq(ks, p - ks, c)) {
                            if (nhit == kMaxHits)
                                throw std::length_error("JSONL: more than 16 nested columns on one key");
                            hit[nhit++] = c;
                        }
                    st = COLON_EXPECT;
                }
                break;
            case COLON_EXPECT:
                if (t == ':') { colon_pos = p; st = VALUE_EXPECT; }
                break;
            case VALUE_EXPECT:
                if (t == '"') {
                    val_start = p + 1;
                    st = VALUE_STR_IN;
                } else if (t == '{' || t == '[') {
                    bool closed = false;
                    uint32_t cpos = 0;
                    const size_t cidx = bound_container(w, j, buffer_length, closed, cpos);
                    if (!closed) { at = cidx; return -1; }
                    for (int h = 0; h < nhit && !record_dead; ++h)
                        fin |= stage(hit[h], p, cpos - p + 1,
                                     t == '[' ? ValueType::Array : ValueType::Object, hit[h]->slot);
                    if (fin) { tail_from = cpos + 1; tail_depth = 2; return 1; }
                    j = cidx;
                    st = AFTER_VALUE;
                } else if (t == ',' || t == '}' || t == ']') {
                    for (int h = 0; h < nhit && !record_dead; ++h) fin |= nested_scalar(hit[h], colon_pos, p);
                    if (fin) { tail_from = p; tail_depth = 2; return 1; }
                    if (t != ',') return closed_at(j);
                    st = KEY_EXPECT;
                }
                break;
            case VALUE_STR_IN:
                if (t == '"') {
                    for (int h = 0; h < nhit && !record_dead; ++h)
                        fin |= stage(hit[h], val_start, p - val_start, ValueType::String, hit[h]->slot);
                    if (fin) { tail_from = p + 1; tail_depth = 2; return 1; }
                    st = AFTER_VALUE;
                }
                break;
            case AFTER_VALUE:
                if (t == ',') st = KEY_EXPECT;
                else if (t == '}' || t == ']') return closed_at(j);
                break;
            }
        }
        at = w.n;
        return -1;
    }

    // Early exit: parse the record that opens the current line from a PREFIX index `w` of
    // the line. True when every wanted column was resolved (or an inline predicate failed)
    // inside the prefix — the record is staged, and check_line_tail decides the rest; then
    // accept_prefix or reset_prefix. False otherwise, with no effect on the line's state:
    // `prefix_ran_out` says whether a longer prefix could still finish it.
    inline bool parse_prefix(const StructuralWindow& w) {
        prefix_ran_out = false;
        if (w.n == 0) { prefix_ran_out = true; return false; }
        if (w.at(0) != '{' || !blank(line_start, w.ix[0])) return false;
        size_t fail_at = 0;
        prefix_mode = true;
        const size_t r = parse_record(w, 0, fail_at);
        prefix_mode = false;
        if (r == PREFIX_DONE) return true;
        prefix_ran_out = r == NPOS && fail_at >= w.n;
        reset_prefix();
        return false;
    }
    // The staged record is not taken: the line is judged again by the full rules.
    inline void reset_prefix() {
        saw_open_brace_since_newline = false;
        record_dead = false;
    }
    // The staged record passed check_line_tail: bank it (or drop it — a predicate failed);
    // `line_end` is its line's newline (or the buffer end), so nothing remains to judge.
    inline void accept_prefix(uint32_t line_end) {
        if (record_dead) { discard_record(); record_dead = false; }
        else bank_record();
        tail_start = line_end;
    }

    // Parse the record whose '{' is entry `i`, strictly, at its top level:
    //
    //   record := '{' ws ( '}' | member ( ws ',' ws member )* ws '}' )
    //   member := '"' key '"' ws ':' ws value
    //   value  := '"' string '"' | container | scalar
    //
    // Every gap between tokens must be whitespace, and a scalar must be one JSON token
    // (valid_scalar). A container is bounded by bracket depth (bound_container); its
    // interior is not validated here. The masked index makes the string cases exact: the
    // entry after an opening quote is its closing quote, or the newline that truncates it.
    //
    // Minimal extent: once every wanted column is resolved, or an inline predicate failed,
    // keys are no longer matched and nothing is materialised; the rest of the record is
    // parsed by the same rules — except in prefix_mode (early exit), which returns
    // PREFIX_DONE right there and leaves the rest of the line to check_line_tail.
    //
    // Returns the index of the record's closing '}', PREFIX_DONE (prefix_mode only), or
    // NPOS with `fail_at` = the entry where it stopped being valid (or w.n when the index
    // ran out).
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((always_inline))
#endif
    inline size_t parse_record(const StructuralWindow& w, size_t i, size_t& fail_at) {
        ordinal = 0; found = 0; record_dead = false;
        if constexpr (kColumns) ++rec_gen;
        cur_record_start_pos = w.ix[i];
        saw_open_brace_since_newline = true;
        const size_t n = w.n;
        auto gap_ok = [this](uint32_t from, uint32_t to) { return from == to || blank(from, to); };
#define RUGO_FAIL(at) do { fail_at = (at); return NPOS; } while (0)
        bool done = false;            // minimal extent reached: parse, don't materialise
        uint32_t prev = w.ix[i] + 1;  // first byte after the last token
        size_t j = i + 1;
        if (j >= n) RUGO_FAIL(n);
        if (w.at(j) == '}') {
            if (!gap_ok(prev, w.ix[j])) RUGO_FAIL(j);
            return j;
        }
        for (;;) {
            // key
            if (j + 3 >= n) RUGO_FAIL(n);
            if (w.at(j) != '"' || !gap_ok(prev, w.ix[j])) RUGO_FAIL(j);
            if (w.at(j + 1) != '"') RUGO_FAIL(j + 1);
            if (w.at(j + 2) != ':' || !gap_ok(w.ix[j + 1] + 1, w.ix[j + 2])) RUGO_FAIL(j + 2);
            key_start = w.ix[j] + 1;
            key_width = w.ix[j + 1] - key_start;
            const uint32_t colon = w.ix[j + 2];
            if (!done) match_key();

            // value
            const size_t v = j + 3;
            const uint8_t vc = w.at(v);
            size_t sep;
            uint32_t after;
            bool stop = false;
            if (vc == '"') {
                if (!gap_ok(colon + 1, w.ix[v])) RUGO_FAIL(v);
                if (v + 1 >= n || w.at(v + 1) != '"') RUGO_FAIL(v + 1 < n ? v + 1 : n);
                if (!done) {
                    value_start = w.ix[v] + 1;
                    value_width = w.ix[v + 1] - value_start;
                    value_type = ValueType::String;
                    stop = commit_field();
                }
                after = w.ix[v + 1] + 1;
                sep = v + 2;
            } else if (kColumns && prefix_mode && !done && vc == '{' && chain_all_nested()) {
                // Early exit through a nested key: read its sub-keys without bounding the
                // object first, so the walk can stop inside it.
                if (!gap_ok(colon + 1, w.ix[v])) RUGO_FAIL(v);
                size_t at = 0;
                const int r = nested_walk(w, v, at);
                if (r < 0) RUGO_FAIL(at);
                if (r > 0) return PREFIX_DONE;
                ++ordinal;
                stop = record_dead || found >= num_wanted;
                after = w.ix[at] + 1;
                sep = at + 1;
            } else if (vc == '{' || vc == '[') {
                if (!gap_ok(colon + 1, w.ix[v])) RUGO_FAIL(v);
                bool closed = false;
                uint32_t close = 0;
                const size_t ci = bound_container(w, v, buffer_length, closed, close);
                // Truncated: ran out of index, or a newline before the close.
                if (!closed) RUGO_FAIL(ci);
                if (!done)
                    stop = commit_container(w, v, ci, w.ix[v], close,
                                            vc == '[' ? ValueType::Array : ValueType::Object);
                after = close + 1;
                sep = ci + 1;
            } else if (vc == ',' || vc == '}') {
                // A scalar has no entry of its own: it is the bytes between ':' and here.
                uint32_t s = colon + 1, e = w.ix[v];
                while (s < e && is_ws(buffer[s])) ++s;
                while (e > s && is_ws(buffer[e - 1])) --e;
                if (s == e || !valid_scalar(buffer + s, e - s)) RUGO_FAIL(v);
                if (!done) {
                    value_start = s;
                    value_width = e - s;
                    value_type = classify_first(buffer[s]);
                    stop = commit_field();
                }
                after = w.ix[v];
                sep = v;
            } else {
                RUGO_FAIL(v);  // ':' / ']' / a newline where a value must start
            }
            done |= stop;
            if (kColumns && prefix_mode && done) {
                tail_from = after;
                tail_depth = 1;
                return PREFIX_DONE;
            }

            // separator
            if (sep >= n) RUGO_FAIL(n);
            const uint8_t sc = w.at(sep);
            if (!gap_ok(after, w.ix[sep])) RUGO_FAIL(sep);
            if (sc == '}') return sep;
            if (sc != ',') RUGO_FAIL(sep);
            prev = w.ix[sep] + 1;
            j = sep + 1;
        }
#undef RUGO_FAIL
    }

    inline void finish() {
        // The buffer's last line has no newline: the same line-end rule as a newline —
        // only whitespace may follow the record (or fill the line).
        if (!blank(tail_start, buffer_length)) reject_line();
    }
};
}  // namespace

namespace {

// Window size for build_columns / build_map: input bytes indexed per scan. The index of
// one window (4 bytes per entry, ~1 entry per 5-8 bytes of JSON) stays cache-resident
// while it is walked; a window is extended to the end of its last line, so a line is
// never split.
constexpr size_t kWindowBytes = static_cast<size_t>(256) << 10;

// Walk one window's index: line discipline around parse_record. The builder's state
// carries across windows; every window but the range's last ends with a newline entry,
// so no record or resync ever needs an entry from the next window. `li` is the prefilter
// line cursor (kLines only), also carried across windows.
template <bool kColumns, bool kLines>
void walk_window(MapBuilder<kColumns>& b, const StructuralWindow& w, uint32_t buffer_length,
                 const std::vector<LineSpan>* lines, size_t& li) {
    const size_t L = kLines ? lines->size() : 0;
    // A line was rejected at entry `k`: recover at its end, the first newline entry at or
    // after `k` (every newline is an entry). Deliberately line-based rather than resuming
    // on the corrupt tail: the rest of a bad line still holds well-formed-looking `{...}`
    // fragments (Bluesky's nested JSON is full of them), and none may start a phantom
    // record. The FOLLOWING line is never skipped: it is judged on its own (the orphaned
    // second half of a record split by a raw newline is rejected by its own first byte).
    auto resync = [&](size_t k) -> size_t {
        while (k < w.n && w.at(k) != '\n') ++k;
        if (k < w.n) { b.begin_line(w.ix[k] + 1); return k; }
        b.begin_line(buffer_length);  // the range's last line ends with the buffer
        return w.n - 1;
    };
    for (size_t i = 0; i < w.n; ++i) {
        const uint32_t pos = w.ix[i];
        // Entering a later surviving line: begin it at its own start. The line before it
        // was already closed by its own newline entry (a line's entries always include
        // its newline; only the range's last line can lack one, and nothing follows it),
        // so this only moves the line start past the bytes the prefilter skipped.
        if constexpr (kLines) {
            while (li + 1 < L && pos >= (*lines)[li + 1].start) b.begin_line((*lines)[++li].start);
        }
        const uint8_t ch = w.buf[pos];
        if (ch == '\n') {
            // End of line: only whitespace may follow the record (or fill the line).
            if (!b.blank(b.tail_start, pos)) b.reject_line();
            b.begin_line(pos + 1);
            continue;
        }
        // Only a line's first object may open here, after whitespace only. Any other entry
        // — content before the '{' (the orphaned second half of a split record starts
        // mid-string), a second object, anything after the '}' — rejects the line.
        if (ch != '{' || b.saw_open_brace_since_newline || !b.blank(b.tail_start, pos)) {
            b.reject_line();
            i = resync(i);
            continue;
        }
        size_t fail_at = 0;
        const size_t close = b.parse_record(w, i, fail_at);
        if (close == MapBuilder<kColumns>::NPOS) {
            b.reject_line();
            i = resync(fail_at);
            continue;
        }
        if (b.record_dead) { b.discard_record(); b.record_dead = false; }
        else b.bank_record();
        b.tail_start = w.ix[close] + 1;
        i = close;
    }
}

// The driver, specialised at compile time on the sink and on whether the prefilter's line
// spans are present: the per-entry line check exists only in the kLines instantiation, so
// the unfiltered walk's hot loop carries no line cursor.
template <bool kColumns, bool kLines>
MapBuilder<kColumns> run_map(
    const uint8_t* buffer,
    size_t buffer_length,
    const MapProjection* proj,
    size_t range_start,
    const std::vector<LineSpan>* lines) {
    MapBuilder<kColumns> b(buffer, static_cast<uint32_t>(buffer_length), proj);
    const uint32_t blen = static_cast<uint32_t>(buffer_length);
    // With `lines` the input is those lines only: the first is begun at its own start (no
    // lines: at the range end, so the empty input judges nothing).
    const size_t L = kLines ? lines->size() : 0;
    size_t li = 0;
    if constexpr (kLines) b.begin_line(L ? (*lines)[0].start : blen);
    else                  b.begin_line(static_cast<uint32_t>(range_start));
    const size_t range_bytes = buffer_length > range_start ? buffer_length - range_start : 0;
    if constexpr (!kColumns) {
        b.rs.offsets.reserve(range_bytes / 100 + 2);
        b.rs.spans.reserve(range_bytes / 16 + 1);
    }

    // The one index buffer, reused by every window (scan_structural_index's contract:
    // room for the window's bytes + 64).
    std::vector<uint32_t> index(std::min(range_bytes, kWindowBytes) + 64);
    auto room = [&index](size_t needed) {
        if (index.size() < needed) index.resize(needed);
    };
    // Column-major: size every column from the first window's row density, so the
    // per-row appends never reallocate mid-range in the common (homogeneous) case.
    size_t scanned = 0;
    bool reserved = !kColumns;
    auto reserve_from_density = [&]() {
        if constexpr (kColumns) {
            if (reserved || b.cm.rows == 0 || scanned == 0) return;
            reserved = true;
            const double scale = static_cast<double>(range_bytes) / static_cast<double>(scanned) * 1.05;
            const size_t est = static_cast<size_t>(static_cast<double>(b.cm.rows) * scale) + 16;
            for (auto& col : b.cm.cols) col.reserve(est);
            for (auto& ar : b.cm.arena)
                if (!ar.empty()) ar.reserve(static_cast<size_t>(static_cast<double>(ar.size()) * scale) + 64);
        }
    };

    if constexpr (kLines) {
        // A window is a run of whole surviving lines, each scanned on its own (the bytes
        // between them are not input); each line is scanned with its newline.
        size_t next = 0;
        while (next < L) {
            size_t n = 0, bytes = 0;
            while (next < L && bytes < kWindowBytes) {
                const LineSpan& l = (*lines)[next++];
                const size_t to = l.end < buffer_length ? static_cast<size_t>(l.end) + 1 : buffer_length;
                room(n + (to - l.start) + 64);
                n += scan_structural_index(buffer + l.start, to - l.start, l.start, index.data() + n);
                bytes += to - l.start;
            }
            walk_window<kColumns, true>(b, StructuralWindow{buffer, index.data(), n}, blen, lines, li);
        }
    } else if (kColumns && proj->early_exit) {
        // Early exit (build_columns): one line at a time. The line is indexed 64 bytes at a
        // time (state carried), starting from what the previous line needed, and parsed as
        // the index grows; each step looks for the newline only inside its own chunk.
        // Once the record is finished, check_line_tail reads the rest of the line once and
        // finds its end. A line whose record is not finished before the line ends, that is
        // malformed inside the indexed part, or that fails the tail check is indexed whole
        // and judged by walk_window — the full rules.
        size_t hint = 64;
        size_t a = range_start;
        while (a < buffer_length) {
            b.begin_line(static_cast<uint32_t>(a));
            uint64_t state[2] = {0, 0};
            size_t n = 0, indexed = 0, take = hint, line_end = 0;
            for (;;) {
                const size_t pos = a + indexed;
                const size_t avail = buffer_length - pos;
                size_t t = std::min(take, avail);
                const void* nl = std::memchr(buffer + pos, '\n', t);
                const bool whole = nl != nullptr || t == avail;
                if (nl) t = static_cast<size_t>(static_cast<const uint8_t*>(nl) - (buffer + pos)) + 1;
                room(indexed + t + 64);
                n += scan_structural_index_cont(buffer + pos, t, static_cast<uint32_t>(pos), index.data() + n, state);
                indexed += t;
                const StructuralWindow win{buffer, index.data(), n};
                if (whole) {
                    line_end = a + indexed;
                    walk_window<kColumns, false>(b, win, blen, lines, li);
                    break;
                }
                if (b.parse_prefix(win)) {
                    const uint32_t from = b.tail_from;
                    size_t close_off = 0, nl_off = 0;
                    bool ok = check_line_tail(buffer + from, buffer_length - from, b.tail_depth, &close_off, &nl_off);
                    const size_t le = from + nl_off;
                    line_end = le < buffer_length ? le + 1 : buffer_length;
                    if (ok) {
                        // The record's close is the line's last non-whitespace byte.
                        size_t e = le;
                        while (e > from && is_ws(buffer[e - 1])) --e;
                        ok = from + close_off == e - 1 && buffer[e - 1] == '}';
                    }
                    if (ok) {
                        b.accept_prefix(static_cast<uint32_t>(le));
                        hint = indexed;
                        break;
                    }
                    b.reset_prefix();
                    room((line_end - a) + 64);
                    const size_t nn = scan_structural_index(buffer + a, line_end - a, static_cast<uint32_t>(a), index.data());
                    walk_window<kColumns, false>(b, StructuralWindow{buffer, index.data(), nn}, blen, lines, li);
                    break;
                }
                if (!b.prefix_ran_out) {
                    // Malformed (or the record closed) inside the indexed part: index the
                    // rest of the line and judge it by the full rules.
                    const void* q = std::memchr(buffer + a + indexed, '\n', buffer_length - (a + indexed));
                    line_end = q ? static_cast<size_t>(static_cast<const uint8_t*>(q) - buffer) + 1 : buffer_length;
                    room((line_end - a) + 64);
                    n += scan_structural_index_cont(buffer + a + indexed, line_end - (a + indexed),
                                                    static_cast<uint32_t>(a + indexed), index.data() + n, state);
                    walk_window<kColumns, false>(b, StructuralWindow{buffer, index.data(), n}, blen, lines, li);
                    break;
                }
                take = 64;
            }
            scanned += line_end - a;
            if (scanned >= kWindowBytes) reserve_from_density();
            a = line_end;
        }
    } else {
        // No wanted column reads inside a top-level container (no `key->>'sub'`), so the
        // walk only ever bounds one: the depth <= 1 index is enough
        // (scan_structural_index_top), and bound_container steps from a container's
        // opening bracket straight to its closing one.
        bool top_only = true;
        if constexpr (kColumns)
            for (const WantedColumn& w : *proj->columns) top_only &= w.sub_len == 0;
        size_t a = range_start;
        while (a < buffer_length) {
            size_t z = std::min(buffer_length, a + kWindowBytes);
            if (z < buffer_length) {
                const void* nl = std::memchr(buffer + z, '\n', buffer_length - z);
                z = nl ? static_cast<size_t>(static_cast<const uint8_t*>(nl) - buffer) + 1 : buffer_length;
            }
            room((z - a) + 64);
            const size_t n = top_only
                ? scan_structural_index_top(buffer + a, z - a, static_cast<uint32_t>(a), index.data())
                : scan_structural_index(buffer + a, z - a, static_cast<uint32_t>(a), index.data());
            walk_window<kColumns, false>(b, StructuralWindow{buffer, index.data(), n}, blen, lines, li);
            scanned += z - a;
            reserve_from_density();
            a = z;
        }
    }
    // The last surviving line was closed by its newline; the bytes after it, to the range
    // end, were skipped by the prefilter and are not input.
    if constexpr (kLines) {
        if (L == 0 || (*lines)[L - 1].end < buffer_length)
            b.begin_line(blen);
    }
    b.finish();
    return b;
}

}  // namespace

void require_chunk_length(size_t length, const char* what) {
    if (length > kMaxChunkBytes)
        throw std::length_error(std::string(what) + ": " + std::to_string(length) +
                                " bytes in one buffer; at most " + std::to_string(kMaxChunkBytes) +
                                " (4 GiB) can be parsed at once");
}

size_t chunk_end(const uint8_t* buffer, size_t from, size_t length) {
    if (length - from <= kMaxChunkBytes) return length;
    for (size_t p = from + kMaxChunkBytes; p > from; --p)
        if (buffer[p - 1] == '\n') return p;
    throw std::length_error("read_jsonl: the line at byte " + std::to_string(from) +
                            " is longer than " + std::to_string(kMaxChunkBytes) +
                            " bytes (4 GiB), the most that can be parsed at once");
}

ColumnMap build_columns(
    const uint8_t* buffer,
    size_t buffer_length,
    const MapProjection& proj,
    size_t range_start,
    const std::vector<LineSpan>* lines) {
    require_chunk_length(buffer_length, "build_columns");
    MapBuilder<true> b = lines
        ? run_map<true, true>(buffer, buffer_length, &proj, range_start, lines)
        : run_map<true, false>(buffer, buffer_length, &proj, range_start, nullptr);
    b.cm.malformed = b.malformed_found;
    b.cm.malformed_pos = b.malformed_found ? b.malformed_at : 0;
    b.cm.malformed_count = b.malformed_count;
    return std::move(b.cm);
}

RecordSet build_map(const uint8_t* buffer, size_t buffer_length) {
    require_chunk_length(buffer_length, "build_map");
    MapBuilder<false> b = run_map<false, false>(buffer, buffer_length, nullptr, 0, nullptr);
    b.rs.malformed = b.malformed_found;
    b.rs.malformed_pos = b.malformed_found ? b.malformed_at : 0;
    b.rs.malformed_count = b.malformed_count;
    return std::move(b.rs);
}

std::vector<std::string> sample_record_keys(
    const RecordSet& rs, const uint8_t* buffer, size_t sample_records) {
    std::vector<std::string> keys;
    const size_t limit = std::min(sample_records, rs.num_records());
    if (limit == 0) return keys;

    // Views into `buffer`, which outlives this call — the returned strings own their bytes,
    // the set only dedupes while we build.
    std::unordered_set<std::string_view> seen;
    keys.reserve(rs[0].size());
    seen.reserve(rs[0].size());

    for (size_t r = 0; r < limit; ++r) {
        for (const FieldSpan& f : rs[r]) {
            const std::string_view key(
                reinterpret_cast<const char*>(buffer + f.key_start), f.key_width);
            if (seen.insert(key).second) keys.emplace_back(key);
        }
    }
    return keys;
}

// The unprojected, unfiltered map of the input's head: grown by physical lines until
// it banks `want` records (blank and malformed lines bank none) or covers the whole
// buffer — or its first chunk (kMaxChunkBytes, cut after a newline), the most the parser
// takes at once: past that the sample holds fewer than `want` records. The head always ends
// just after a newline (or at the buffer end), so no record in it is cut short. May hold
// more than `want` records; callers take the first `want`.
static RecordSet build_head(const uint8_t* buffer, size_t buffer_length, size_t want) {
    const size_t limit = chunk_end(buffer, 0, buffer_length);
    for (size_t lines = want; ; lines *= 2) {
        size_t end = 0;
        for (size_t seen = 0; seen < lines && end < limit; ++seen) {
            const void* nl = std::memchr(buffer + end, '\n', limit - end);
            end = nl ? static_cast<size_t>(static_cast<const uint8_t*>(nl) - buffer) + 1
                     : limit;
        }
        RecordSet head = build_map(buffer, end);
        if (head.num_records() >= want || end == limit) return head;
    }
}

std::vector<uint8_t> head_copy_columns(const uint8_t* buffer, size_t buffer_length,
                                       const std::vector<std::string>& columns,
                                       const ParseContext& context) {
    std::vector<uint8_t> copy(columns.size(), 1);
    const RecordSet head = build_head(buffer, buffer_length, context.infer_sample_size);
    const size_t limit = std::min(context.infer_sample_size, head.num_records());
    for (size_t c = 0; c < columns.size(); ++c) {
        const ColumnSpec sp = parse_column_spec(columns[c]);
        const auto it = context.explicit_schema.find(columns[c]);
        rugo::DeclaredType dt;
        const bool declared_structured = it != context.explicit_schema.end() &&
            rugo::parse_declared_type(it->second, &dt) && rugo::declared_is_structured(dt.type);
        if (sp.as_json || declared_structured) { copy[c] = 0; continue; }
        if (sp.nested) continue;
        for (size_t r = 0; r < limit; ++r) {
            const FieldSpan* f = nullptr;
            for (const FieldSpan& s : head[r])
                if (s.key_width == sp.key.size() &&
                    std::memcmp(buffer + s.key_start, sp.key.data(), s.key_width) == 0) { f = &s; break; }
            if (f == nullptr || is_null(buffer, f->value_start, f->value_start + f->value_width - 1)) continue;
            copy[c] = (f->type == static_cast<uint8_t>(ValueType::Object) ||
                       f->type == static_cast<uint8_t>(ValueType::Array)) ? 0 : 1;
            break;
        }
    }
    return copy;
}

std::vector<std::string> discover_column_names(
    const uint8_t* buffer, size_t buffer_length, const ParseContext& context) {
    const size_t want = context.infer_sample_size;
    const RecordSet head = build_head(buffer, buffer_length, want);
    std::vector<std::string> keys = sample_record_keys(head, buffer, want);
    if (context.projected_columns.empty()) return keys;

    std::vector<std::string> projected;
    projected.reserve(context.projected_columns.size());
    // A nested request (`key->>'sub'`) is never a top-level key, so the sampled key set
    // cannot vouch for it: it is always built, all-null where the sub-key never appears
    // (ParsedColumn::key_absent then says so), exactly like a declared column.
    for (const auto& c : context.projected_columns)
        if ((parse_column_spec(c).nested || std::find(keys.begin(), keys.end(), c) != keys.end()) &&
            std::find(projected.begin(), projected.end(), c) == projected.end())
            projected.push_back(c);
    return projected;
}

void check_predicate_literals(
    const uint8_t* buffer, size_t buffer_length, const ParseContext& context) {
    // The scalar comparisons to check: ops 0-5 themselves, IN / NOT IN via their
    // members. IS [NOT] NULL carries no literal.
    std::vector<const Predicate*> scalars;
    for (const auto& p : context.predicates) {
        if (p.op <= 5) scalars.push_back(&p);
        else if (p.op == 6 || p.op == 7)
            for (const auto& m : p.members) scalars.push_back(&m);
    }
    if (scalars.empty()) return;

    // A DECLARED column's type is known before any byte is read.
    for (const Predicate* p : scalars) {
        const auto it = context.explicit_schema.find(p->column);
        if (it == context.explicit_schema.end()) continue;
        rugo::DeclaredType dt;
        if (!rugo::parse_declared_type(it->second, &dt)) continue;  // the Cython edge refuses it first
        if (!rugo::literal_fits_type(dt.type, dt.logical_kind, p->kind))
            throw std::invalid_argument(
                rugo::literal_mismatch_message(p->column, it->second, p->kind, p->value));
    }

    // Every other column: the non-null values in the head sample — the records the
    // reader infers the column's type from — before a single row is filtered, so the
    // check does not depend on which rows the predicates keep, or in what order they
    // are evaluated.
    if (buffer_length == 0) return;
    const size_t want = context.infer_sample_size;
    const RecordSet head = build_head(buffer, buffer_length, want);
    const size_t limit = std::min(want, head.num_records());
    for (size_t r = 0; r < limit; ++r) {
        for (const FieldSpan& f : head[r]) {
            if (is_null(buffer, f.value_start, f.value_start + f.value_width - 1)) continue;
            for (const Predicate* p : scalars) {
                if (p->column.size() != f.key_width ||
                    std::memcmp(p->column.data(), buffer + f.key_start, f.key_width) != 0)
                    continue;
                if (!literal_fits_json_value(f.type, p->kind))
                    throw std::invalid_argument(rugo::literal_mismatch_message(
                        p->column, json_value_kind_name(f.type), p->kind, p->value));
            }
        }
    }
}

} // namespace rugo::_jsonl
