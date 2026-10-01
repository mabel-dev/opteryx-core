#include "interpreter.hpp"
#include "nested_column.hpp"
#include "json_array_walker.hpp"   // escaped nested-key comparison
#include "field_span.hpp"
#include "value_parser.hpp"   // evaluate_predicate (inline filter pushdown)
#include "predicate_literal.hpp" // check_predicate_literals: literal vs column type contract
#include "declared_type.hpp"     // check_predicate_literals: declared column types
#include "structural_scan.hpp" // scan_structural_markers (discover_column_names)
#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <unordered_set>
#include <utility>

namespace rugo::_jsonl {

// -----------------------------------------------------------------------------
// Optimised table‑driven JSONL parser
// -----------------------------------------------------------------------------

namespace {

// Character classes for fast lookup
enum class CharClass : uint8_t {
    LBRACE,    // {
    RBRACE,    // }
    QUOTE,     // "
    COLON,     // :
    COMMA,     // ,
    NEWLINE,   // \n
    DIGIT,     // 0-9
    MINUS,     // -
    T,         // t
    F,         // f
    N,         // n
    WS,        // space, \t, \r
    OTHER
};

// Pre‑computed character → class table
constexpr auto make_char_class_table() {
    std::array<CharClass, 256> table{};
    for (int i = 0; i < 256; ++i) {
        unsigned char c = static_cast<unsigned char>(i);
        if (c == '{') table[i] = CharClass::LBRACE;
        else if (c == '}') table[i] = CharClass::RBRACE;
        else if (c == '"') table[i] = CharClass::QUOTE;
        else if (c == ':') table[i] = CharClass::COLON;
        else if (c == ',') table[i] = CharClass::COMMA;
        else if (c == '\n') table[i] = CharClass::NEWLINE;
        else if (c >= '0' && c <= '9') table[i] = CharClass::DIGIT;
        else if (c == '-') table[i] = CharClass::MINUS;
        else if (c == 't') table[i] = CharClass::T;
        else if (c == 'f') table[i] = CharClass::F;
        else if (c == 'n') table[i] = CharClass::N;
        else if (c == ' ' || c == '\t' || c == '\r') table[i] = CharClass::WS;
        else table[i] = CharClass::OTHER;
    }
    return table;
}

constexpr auto char_class_table = make_char_class_table();

// Parser states
enum class State : uint8_t {
    EXPECT_RECORD_START,
    EXPECT_KEY_QUOTE,
    IN_KEY,
    EXPECT_COLON,
    EXPECT_VALUE,
    IN_STRING_VALUE,
    IN_UNQUOTED_VALUE,
    EXPECT_SEPARATOR,
    NUM_STATES
};

// Actions that are dispatched after a transition
enum class Action : uint8_t {
    NONE                     = 0,
    START_RECORD             = 1,   // reset ordinal, clear record
    START_KEY                = 2,   // begin of key string
    END_KEY                  = 3,   // end of key string
    START_VALUE              = 4,   // begin of value (type determined by char)
    END_STRING_VAL           = 5,   // closing quote of a string value
    END_UNQUOTED_VAL         = 6,   // comma / } ending an unquoted value
    PUSH_RECORD              = 8,   // }
    END_UNQUOTED_VAL_RECORD  = 11,  // '}' ending an unquoted value + finish record
    SET_COLON                = 9    // remember ':' position — anchors the unquoted slice
};

struct Transition {
    State  next_state;
    Action action;
};

// Main transition table: [state][charclass]
constexpr std::array<std::array<Transition, 13>, 8> build_transition_table() {
    // We use the int values of CharClass enum (0..12).
    // Helper macros to keep it readable
    constexpr size_t C = 13; // total number of classes
    std::array<std::array<Transition, C>, 8> t{};

    using S = State;
    using A = Action;
    using K = CharClass;

    // Default: stay in same state, no action
    for (int st = 0; st < 8; ++st)
        for (int cl = 0; cl < C; ++cl)
            t[st][cl] = { static_cast<S>(st), A::NONE };

    // NEWLINE has no entry in any state: MapBuilder::step ends the line before the table
    // is consulted (a newline in any state but EXPECT_RECORD_START is a truncated record),
    // and validates EXPECT_RECORD_START's '{' and every other marker there itself.

    // State 0: EXPECT_RECORD_START
    t[0][int(K::LBRACE)] = { S::EXPECT_KEY_QUOTE,   A::START_RECORD };

    // State 1: EXPECT_KEY_QUOTE
    t[1][int(K::QUOTE)]  = { S::IN_KEY,            A::START_KEY };
    t[1][int(K::RBRACE)] = { S::EXPECT_RECORD_START, A::PUSH_RECORD };

    // State 2: IN_KEY
    t[2][int(K::QUOTE)]  = { S::EXPECT_COLON,      A::END_KEY };

    // State 3: EXPECT_COLON
    t[3][int(K::COLON)]  = { S::EXPECT_VALUE,       A::SET_COLON };

    // State 4: EXPECT_VALUE
    // A scalar value (number / true / false / null) produces NO structural marker
    // of its own — the scanner is content-blind. So the value's presence is only
    // visible as the slice between the ':' (remembered via SET_COLON) and the next
    // ',' / '}' / '\n'. Those terminators therefore close an unquoted value here.
    // Strings, objects and arrays DO start with a marker ('"' / '{' / '[') and take
    // the marker-driven paths below.
    t[4][int(K::QUOTE)]   = { S::IN_STRING_VALUE,   A::START_VALUE };
    t[4][int(K::LBRACE)]  = { S::IN_UNQUOTED_VALUE, A::START_VALUE };
    t[4][int(K::OTHER)]   = { S::IN_UNQUOTED_VALUE, A::START_VALUE }; // '[' (array) and anything else
    t[4][int(K::COMMA)]   = { S::EXPECT_KEY_QUOTE,    A::END_UNQUOTED_VAL };
    // See the note on t[6][RBRACE] below: this '}' closes the record as well as the
    // scalar. (This is the entry that actually fires for a bare scalar -- true/false/
    // null/number emit no marker of their own, so the FSA is still HERE, not in
    // IN_UNQUOTED_VALUE, when the terminator arrives.)
    t[4][int(K::RBRACE)]  = { S::EXPECT_RECORD_START, A::END_UNQUOTED_VAL_RECORD };

    // State 5: IN_STRING_VALUE
    t[5][int(K::QUOTE)]   = { S::EXPECT_SEPARATOR,  A::END_STRING_VAL };

    // State 6: IN_UNQUOTED_VALUE
    t[6][int(K::COMMA)]   = { S::EXPECT_KEY_QUOTE,  A::END_UNQUOTED_VAL };
    // A '}' here is BOTH the terminator of the unquoted scalar AND the close of the
    // record -- there is no further delimiter to wait for. Parking in EXPECT_SEPARATOR
    // and leaning on the following '\n' to PUSH_RECORD loses the row outright at EOF:
    // `{"ok":true}` with no trailing newline reached finish() with committed spans and
    // was reported as malformed. (A string value doesn't hit this: its closing quote is
    // its own terminator, so the record's '}' is still free to push -- hence the bug
    // only ever showed on a record whose LAST value was a bare true/false/null/number.)
    t[6][int(K::RBRACE)]  = { S::EXPECT_RECORD_START, A::END_UNQUOTED_VAL_RECORD };

    // State 7: EXPECT_SEPARATOR
    t[7][int(K::COMMA)]   = { S::EXPECT_KEY_QUOTE,   A::NONE };
    t[7][int(K::RBRACE)]  = { S::EXPECT_RECORD_START, A::PUSH_RECORD };

    return t;
}

constexpr auto transition_table = build_transition_table();

// Fast whitespace test (no locale overhead)
inline bool is_ws(uint8_t c) {
    return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

// Bound a container value whose opening '[' or '{' is markers[open_idx]. Walks the
// marker list (not raw bytes — every byte that can change string/escape/depth state IS
// a structural marker, so the SIMD scan already found them all) tracking string state
// and backslash escapes so interior commas, brackets and braces — including those
// inside quoted strings — do not close the container early. With markers from the
// masked scan the same state machine degenerates correctly: backslashes and in-string
// structurals are simply absent, and every emitted quote is a real delimiter.
//
// On success sets `closed`, `close_pos` to the matching close bracket/brace, and
// returns its marker index. On a truncated/unterminated container returns markers.size()
// with `close_pos = limit - 1`; on ANY newline before the close — inside a nested string
// (invalid JSON — RFC 8259 requires the two bytes '\'+'n', not this control byte;
// confirmed against a real defect in the JSONBench Bluesky dump, see
// tests/performance/jsonbench/README.md's "Known data-quality defect"), escaped, or
// between members — returns the newline's marker index with `close_pos` at the newline:
// a JSONL record never spans lines, so the container is unterminated on its line.
inline size_t scan_container_markers(
    const std::vector<MarkerPosition>& markers,
    size_t open_idx,
    uint32_t limit,
    bool& closed,
    uint32_t& close_pos) {
    int depth = 0;
    bool in_string = false;
    uint32_t escaped_until = 0xFFFFFFFFu;  // byte position escaped by a preceding '\'
    const size_t M = markers.size();
    for (size_t j = open_idx; j < M; ++j) {
        const uint32_t p = markers[j].position;
        const uint8_t t = markers[j].marker_type;
        // A line is a record: a newline anywhere inside the container — in a string, after
        // a backslash, or between members — means it never closed on its line.
        if (t == static_cast<uint8_t>(MarkerType::NEWLINE)) { closed = false; close_pos = p; return j; }
        if (in_string) {
            if (p == escaped_until) { escaped_until = 0xFFFFFFFFu; continue; }  // escaped content
            switch (static_cast<MarkerType>(t)) {
            case MarkerType::BACKSLASH: escaped_until = p + 1; break;  // escapes next byte
            case MarkerType::QUOTE:     in_string = false; break;
            default:                    break;  // in-string structural — content
            }
            continue;
        }
        switch (static_cast<MarkerType>(t)) {
        case MarkerType::QUOTE:
            in_string = true;
            break;
        case MarkerType::BRACE_OPEN:
        case MarkerType::BRACKET_OPEN:
            ++depth;
            break;
        case MarkerType::BRACE_CLOSE:
        case MarkerType::BRACKET_CLOSE:
            if (--depth == 0) { closed = true; close_pos = p; return j; }
            break;
        default:
            break;  // ':', ',', '\\' outside a string — not structure for bounding
        }
    }
    closed = false;
    close_pos = limit - 1;
    return M;
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
// value's span. `open_idx`/`close_idx` are the container's own marker indices (its '{' and
// matching '}') as returned by scan_container_markers. Walks only markers — the same bytes
// the SIMD scan already classified — so reading `commit.collection` costs a fraction of the
// container's extent instead of materialising it and re-parsing it per row downstream.
//
// DEPTH SAFETY IS STRUCTURAL, not tracked: `commit.collection` must never match
// `commit.record.collection`, and here it cannot, because any nested container met in value
// position is skipped WHOLESALE via scan_container_markers (j jumps to its close). The walk
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
    const uint8_t* buf,
    const std::vector<MarkerPosition>& markers,
    size_t open_idx,
    size_t close_idx,
    const char* sub,
    uint32_t sub_len,
    uint8_t sub_first,
    uint32_t& out_start,
    uint32_t& out_width,
    ValueType& out_type) {

    if (buf[markers[open_idx].position] != '{') return false;  // an array has no keys

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
    uint32_t escaped_until = 0xFFFFFFFFu;
    bool key_escaped = false;
    const uint32_t close_byte = markers[close_idx].position;

    for (size_t j = open_idx + 1; j < close_idx; ++j) {
        const uint32_t p = markers[j].position;
        const MarkerType t = static_cast<MarkerType>(markers[j].marker_type);

        switch (st) {
        case KEY_EXPECT:
            if (t == MarkerType::QUOTE) { key_start = p + 1; key_escaped = false; st = KEY_IN; }
            break;

        case KEY_IN:
            if (p == escaped_until) { escaped_until = 0xFFFFFFFFu; break; }
            if (t == MarkerType::BACKSLASH) { escaped_until = p + 1; key_escaped = true; break; }
            if (t == MarkerType::QUOTE) {
                const uint32_t klen = p - key_start;
                // Keys compare DECODED, as yyjson's object lookup (draken's `->>`) does:
                // `"col\u006cection"` IS the key `collection`. Only a key that actually
                // carries an escape pays for the decode.
                wanted = key_escaped
                    ? escaped_key_equals(buf + key_start, klen, sub, sub_len)
                    : (klen == sub_len && buf[key_start] == sub_first &&
                       std::memcmp(buf + key_start, sub, sub_len) == 0);
                st = COLON_EXPECT;
            }
            break;

        case COLON_EXPECT:
            if (t == MarkerType::COLON) { colon_pos = p; st = VALUE_EXPECT; }
            break;

        case VALUE_EXPECT:
            if (t == MarkerType::QUOTE) {
                val_start = p + 1; st = VALUE_STR_IN;
            } else if (t == MarkerType::BRACE_OPEN || t == MarkerType::BRACKET_OPEN) {
                bool closed = false;
                uint32_t cpos = 0;
                const size_t cidx = scan_container_markers(markers, j, close_byte + 1, closed, cpos);
                if (!closed || cidx >= close_idx) return false;   // malformed/overrunning
                if (wanted) {
                    out_start = p;
                    out_width = cpos - p + 1;
                    out_type  = (buf[p] == '[') ? ValueType::Array : ValueType::Object;
                    return true;
                }
                j  = cidx;          // skip the whole container — this is the depth guard
                st = AFTER_VALUE;
            } else if (t == MarkerType::COMMA) {
                if (wanted) return emit_scalar(colon_pos, p);
                st = KEY_EXPECT;
            }
            break;

        case VALUE_STR_IN:
            if (p == escaped_until) { escaped_until = 0xFFFFFFFFu; break; }
            if (t == MarkerType::BACKSLASH) { escaped_until = p + 1; break; }
            if (t == MarkerType::QUOTE) {
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
            if (t == MarkerType::COMMA) st = KEY_EXPECT;
            break;
        }
    }

    // The wanted key was the container's LAST member and its unquoted scalar value is
    // terminated by the closing '}' rather than by a comma.
    if (st == VALUE_EXPECT && wanted) return emit_scalar(colon_pos, close_byte);
    return false;
}

} // anonymous namespace

// Document-map builder. Value shape is coarse and read only from the structural
// delimiter; key identity is never hashed. With a projection it materialises only the
// wanted fields and stops scanning each record once all are found (minimal extent);
// without one it emits every field (data-blind full map). Feed one structural byte at a
// time via step(); container values are bounded by the driver loop before they reach
// step() (see build_map).
namespace {
struct MapBuilder {
    RecordSet rs;
    const uint8_t* buffer;
    uint32_t buffer_length;
    State state = State::EXPECT_RECORD_START;
    uint32_t key_start = 0, key_end = 0, key_width = 0;
    uint32_t value_start = 0, value_end = 0, value_width = 0;
    uint32_t colon_pos = 0;  // position of the ':' for the value currently expected
    ValueType value_type = ValueType::Unknown;
    uint32_t ordinal = 0;

    // Projection + predicate pushdown (nullptr => emit everything). `cur_wanted`/
    // `cur_pred_idx` are set per key by END_KEY; `found` counts matched wanted columns in
    // the record; `skip_rest` is raised once all are in hand; `record_dead` is raised when
    // an inline predicate fails so the driver can discard the record and skip its tail.
    const MapProjection* proj = nullptr;
    size_t num_wanted = 0;
    size_t found = 0;
    int cur_pred_idx = -1;
    bool cur_wanted = true;
    // The matched wanted column for the key currently in hand, or nullptr. Only needed to
    // carry its optional nested sub-key to the container branch in build_map; the flat
    // path reads cur_wanted/cur_pred_idx as before.
    const WantedColumn* cur_col = nullptr;
    bool skip_rest = false;
    bool record_dead = false;
    uint32_t escaped_until = 0xFFFFFFFFu;  // byte escaped by a preceding '\' in a key/string

    // Line discipline. A JSONL line is a record: whitespace, ONE object whose closing '}'
    // ends it, whitespace. Anything else — content before the '{', a second object or any
    // marker after the '}', a newline before the '}' (truncated record, raw newline in a
    // string), content trailing the '}' — makes the WHOLE line malformed: its rows (if any
    // were banked) are rolled back and it is counted once. Judging every line on its own
    // is what makes the outcome independent of where the line sits in the buffer, of
    // chunking and of the masked/unmasked scan, and bounds rows by lines.
    //
    // `line_start`: first byte of the current line. `tail_start`: first byte from which
    // only whitespace may follow up to the newline while EXPECT_RECORD_START (line_start
    // before a record opens; one past its '}' after it closes). `line_records_base`:
    // rs.offsets.size() when the line began — rollback point for reject_line.
    uint32_t line_start = 0;
    uint32_t tail_start = 0;
    size_t   line_records_base = 1;
    uint32_t cur_record_start_pos = 0;  // position of the current record's '{'
    // EXPECT_RECORD_START is BOTH "nothing has happened yet" and "a record just closed" —
    // this tells them apart, so a second '{' on the line is refused.
    bool saw_open_brace_since_newline = false;
    bool malformed_found = false;
    uint32_t malformed_at = 0;
    uint32_t malformed_count = 0;
    inline void flag_malformed(uint32_t pos) {
        if (!malformed_found) { malformed_found = true; malformed_at = pos; }
        ++malformed_count;
    }

    // Set to the byte offset where a line was rejected mid-line; the driver then skips to
    // the first newline AT OR AFTER it (see build_map) and starts the next line there.
    // NO_RESYNC = nothing pending. Resyncing is line-based, not structure-based: the rest
    // of a bad line is arbitrary garbage that still contains well-formed-looking `{...}`
    // fragments (Bluesky's nested JSON is full of them), and the FSA must not resume on
    // them. The FOLLOWING line is never skipped: it is judged on its own (the orphaned
    // second half of a record split by a raw newline is rejected by its own first byte).
    static constexpr uint32_t NO_RESYNC = 0xFFFFFFFFu;
    uint32_t resync_from = NO_RESYNC;

    // Start a new line at `next` (one past a newline).
    inline void begin_line(uint32_t next) {
        line_start = next;
        tail_start = next;
        line_records_base = rs.offsets.size();
        saw_open_brace_since_newline = false;
        state = State::EXPECT_RECORD_START;
    }

    // The current line is not one valid record: count it once and drop every row it
    // produced — banked (a second object, trailing content) or in progress.
    inline void reject_line() {
        flag_malformed(saw_open_brace_since_newline ? cur_record_start_pos : line_start);
        rs.offsets.resize(line_records_base);
        rs.spans.resize(rs.offsets.back());
        record_dead = false;
        skip_rest = false;
        state = State::EXPECT_RECORD_START;
    }

    // Is [from, to) whitespace only?
    inline bool blank(uint32_t from, uint32_t to) const {
        for (uint32_t p = from; p < to; ++p)
            if (!is_ws(buffer[p])) return false;
        return true;
    }

    MapBuilder(const uint8_t* buf, uint32_t buf_len, const MapProjection* p)
        : buffer(buf), buffer_length(buf_len), proj(p),
          // keep_unwanted: the whole row is wanted, so `found` must never reach the stop.
          num_wanted(p ? (p->keep_unwanted ? SIZE_MAX : p->num_wanted) : 0) {
        rs.offsets.push_back(0);
    }

    // First span index of the in-progress record. Invariant: at each record start,
    // rs.spans.size() == record_start() (every record either banks or discards, restoring it).
    inline uint32_t record_start() const { return rs.offsets.back(); }

    // The next wanted column sharing the current key (WantedColumn::next), or nullptr.
    inline const WantedColumn* next_in_group(const WantedColumn* w) const {
        return w->next < 0 ? nullptr : &(*proj->columns)[static_cast<size_t>(w->next)];
    }

    // Append one span for a wanted column (or, unprojected, for the field itself) and
    // evaluate its inline predicate. Returns true when the driver should stop the record
    // (predicate failed, or the last wanted column is now in hand).
    inline bool stage(uint32_t vs, uint32_t vw, ValueType vt, uint8_t slot, int pred_idx) {
        rs.spans.emplace_back(key_start, key_width, vs, vw, vt, static_cast<uint16_t>(ordinal), slot);
        if (pred_idx >= 0 &&
            !evaluate_predicate(buffer, rs.spans.back(), (*proj->predicates)[pred_idx])) {
            record_dead = true;
            return true;
        }
        return proj && ++found >= num_wanted;
    }

    // A wanted column that resolved to NOTHING (nested sub-key absent, its value JSON null,
    // or the key's value is not an object). Emits NO span — exactly how an absent top-level
    // column already represents a NULL cell — but still counts toward `found`, so
    // minimal-extent stops the record on schedule rather than scanning the tail for a
    // column that will never arrive.
    //
    // Deliberately does NOT kill the record when the column carries a predicate: an absent
    // top-level predicate column doesn't drop the row inline either; finalize_records then
    // keeps it only if the predicate accepts NULL. Nested and flat predicates mean the same.
    inline bool resolved_missing() { return proj && ++found >= num_wanted; }

    // Commit the staged scalar/string value (value_start/width/type) for every wanted
    // column on this key: a top-level column takes the value; a nested column has nothing
    // to read inside a non-object and is a NULL cell. Always advances the ordinal ONCE, so
    // emitted spans keep their true object position.
    inline bool commit_field() {
        bool stop = false;
        if (cur_col == nullptr) {
            // Unprojected (no projection, or keep_unwanted for a non-wanted key).
            if (cur_wanted) stop = stage(value_start, value_width, value_type, 0, cur_pred_idx);
        } else {
            for (const WantedColumn* w = cur_col; w != nullptr && !record_dead; w = next_in_group(w))
                stop |= w->sub_len ? resolved_missing()
                                   : stage(value_start, value_width, value_type, 0, w->pred_idx);
        }
        ++ordinal;
        return stop;
    }

    // Commit a container value [start .. close] (open/close are its marker indices) for
    // every wanted column on this key: a top-level column takes the whole container; a
    // nested column takes its sub-key's value from inside it (find_nested_field), or is a
    // NULL cell when the sub-key is absent / JSON null / the container is an array.
    inline bool commit_container(const std::vector<MarkerPosition>& markers,
                                 size_t open_idx, size_t close_idx,
                                 uint32_t start, uint32_t close, ValueType t) {
        if (cur_col == nullptr) return emit_container(start, close, t);
        bool stop = false;
        for (const WantedColumn* w = cur_col; w != nullptr && !record_dead; w = next_in_group(w)) {
            if (w->sub_len == 0) {
                stop |= stage(start, close - start + 1, t, 0, w->pred_idx);
                continue;
            }
            uint32_t nstart = 0, nwidth = 0;
            ValueType ntype = ValueType::Unknown;
            stop |= find_nested_field(buffer, markers, open_idx, close_idx,
                                      w->sub, w->sub_len, w->sub_first, nstart, nwidth, ntype)
                ? stage(nstart, nwidth, ntype, w->slot, w->pred_idx)
                : resolved_missing();
        }
        ++ordinal;
        return stop;
    }

    // Unquoted scalar slice (number / true / false / null), ws-trimmed; coarse type
    // from the first byte.
    inline bool emit_unquoted(uint32_t pos) {
        value_start = colon_pos + 1;
        while (value_start < pos && is_ws(buffer[value_start])) ++value_start;
        value_end = pos - 1;
        while (value_end > value_start && is_ws(buffer[value_end])) --value_end;
        value_width = value_end - value_start + 1;
        value_type = classify_first(buffer[value_start]);
        return commit_field();
    }

    // Container value ['['/'{' .. matching close]; bounds computed by the driver loop.
    inline bool emit_container(uint32_t start, uint32_t close, ValueType t) {
        value_start = start;
        value_end = close;
        value_width = close - start + 1;
        value_type = t;
        return commit_field();
    }

    // Close the in-progress record. Bank: record its end offset — always, even with zero
    // spans (an empty object `{}`, or a record with none of the wanted/projected columns,
    // is still one NDJSON row and must not desync from the other columns' row counts).
    // Discard: drop its partial spans (predicate failed).
    inline void bank_record() {
        rs.offsets.push_back(static_cast<uint32_t>(rs.spans.size()));
    }
    inline void discard_record() { rs.spans.resize(record_start()); }

    inline void step(uint32_t pos, uint8_t ch) {
        CharClass cls = char_class_table[ch];
        if (cls == CharClass::NEWLINE) {
            // End of line — checked before escapes, so a '\' cannot carry a record across it.
            // In any state but EXPECT_RECORD_START the record never reached its '}' on this
            // line: truncated, or a raw newline inside a key/string (RFC 8259 requires
            // control characters in a string to be escaped; the JSONBench Bluesky dump has
            // records split this way, tests/performance/jsonbench/README.md "Known
            // data-quality defect"). Otherwise only whitespace may follow the record.
            if (state != State::EXPECT_RECORD_START || !blank(tail_start, pos)) reject_line();
            begin_line(pos + 1);
            return;
        }
        // Escape handling inside keys/string values: a '\' makes the next byte literal, so an
        // escaped quote (\") or backslash (\\) is content, not a delimiter. (~free; the
        // alternative — masking escapes out of the scan — costs ~1.4× scan for no net win
        // below ~40% in-string density. See scan_structural_masked.)
        if (state == State::IN_KEY || state == State::IN_STRING_VALUE) {
            if (pos == escaped_until) { escaped_until = 0xFFFFFFFFu; return; }  // escaped content
            if (ch == '\\') { escaped_until = pos + 1; return; }               // escapes next byte
        }
        if (state == State::EXPECT_RECORD_START) {
            // Only a line's first object may open here, after whitespace only. Any other
            // marker — content before the '{' (the orphaned second half of a split record
            // starts mid-string), a second object, anything after the '}' — rejects the line.
            if (cls != CharClass::LBRACE || saw_open_brace_since_newline || !blank(tail_start, pos)) {
                reject_line();
                resync_from = pos;
                return;
            }
        }
        const Transition& tr = transition_table[static_cast<int>(state)][static_cast<int>(cls)];
        switch (tr.action) {
        case Action::START_RECORD:
            ordinal = 0; found = 0; record_dead = false; escaped_until = 0xFFFFFFFFu;
            cur_record_start_pos = pos;
            saw_open_brace_since_newline = true;
            break;
        case Action::SET_COLON:
            colon_pos = pos; break;
        case Action::START_KEY:
            key_start = pos + 1; break;
        case Action::END_KEY:
            key_end = pos - 1; key_width = key_end - key_start + 1;
            if (proj) {
                // Exact match against the wanted set — length + first-byte reject, then
                // memcmp. No hashing.
                cur_wanted = proj->keep_unwanted; cur_pred_idx = -1; cur_col = nullptr;
                const uint8_t first = buffer[key_start];
                for (const WantedColumn& w : *proj->columns) {
                    if (key_width == w.len && first == w.first &&
                        std::memcmp(buffer + key_start, w.name, w.len) == 0) {
                        cur_wanted = true; cur_pred_idx = w.pred_idx; cur_col = &w; break;
                    }
                }
            }
            break;
        case Action::START_VALUE:
            // Strings skip the opening quote (END_STRING_VAL stops before the closing one).
            value_start = pos + (ch == '"' ? 1u : 0u);
            value_type = (ch == '"') ? ValueType::String : ValueType::Integer;
            break;
        case Action::END_STRING_VAL:
            value_end = pos - 1;
            value_width = value_end - value_start + 1;
            if (commit_field()) skip_rest = true;
            break;
        case Action::END_UNQUOTED_VAL:
            if (emit_unquoted(pos)) skip_rest = true;
            break;
        case Action::END_UNQUOTED_VAL_RECORD:
            emit_unquoted(pos);  // record ends at this '}'; bank/discard here (no driver skip)
            if (record_dead) { discard_record(); record_dead = false; }
            else bank_record();
            tail_start = pos + 1;
            break;
        case Action::PUSH_RECORD:
            bank_record();
            tail_start = pos + 1;
            break;
        case Action::NONE:
        default:
            break;
        }
        state = tr.next_state;
    }

    inline void finish() {
        // The buffer's last line has no newline: the same line-end rule as step()'s
        // newline. A record still open here (end of file mid-record) or content after its
        // '}' rejects the line — never banked as a row.
        if (state != State::EXPECT_RECORD_START || !blank(tail_start, buffer_length)) reject_line();
    }
};
}  // namespace

RecordSet build_map(
    const uint8_t* buffer,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers,
    const MapProjection* proj,
    size_t range_start) {
    MapBuilder b(buffer, static_cast<uint32_t>(buffer_length), proj);
    b.begin_line(static_cast<uint32_t>(range_start));
    b.rs.offsets.reserve(markers.size() / 20 + 2);
    b.rs.spans.reserve(markers.size() / 3 + 1);
    const size_t M = markers.size();
    const uint8_t NL = static_cast<uint8_t>(MarkerType::NEWLINE);
    for (size_t i = 0; i < M; ++i) {
        const uint32_t pos = markers[i].position;
        const uint8_t ch = buffer[pos];
        // A value-position '[' or '{' opens a container. Bound it with a string- and
        // escape-aware byte scan (interior commas/brackets must not truncate it), emit
        // the whole slice, then skip every marker the container swallowed.
        if ((ch == '[' || ch == '{') && b.state == State::EXPECT_VALUE) {
            bool closed = false;
            uint32_t close = 0;
            const size_t close_idx = scan_container_markers(
                markers, i, static_cast<uint32_t>(buffer_length), closed, close);
            if (!closed) {
                // Truncated/malformed container value (ran out of buffer, or a newline
                // before its close -- see scan_container_markers) means this line's JSON
                // was never valid. The whole line must be dropped, not banked with this
                // one field's value silently replaced by a truncated slice -- a
                // wrong-but-plausible-looking row is worse than no row. Do NOT
                // emit_container() the truncated slice: that would stage a bogus field
                // value AND leave the FSA mid-record on garbage.
                b.reject_line();
                b.resync_from = close;  // the newline at/after `close` ends the line below
            } else {
                // Every wanted column on this key — the container whole, and/or nested
                // sub-keys inside it — is served from this one bounded container.
                if (b.commit_container(markers, i, close_idx, pos, close,
                                       ch == '[' ? ValueType::Array : ValueType::Object)) {
                    b.skip_rest = true;
                }
                b.state = State::EXPECT_SEPARATOR;
                i = close_idx;  // every marker the container swallowed is now behind us
            }
        } else {
            b.step(pos, ch);
        }
        // A line was rejected mid-line (here or inside step()): recover at its end, the
        // first newline at or after resync_from. Deliberately a dumb byte scan for '\n'
        // rather than resuming the FSA on the corrupt tail -- see MapBuilder::resync_from.
        // Markers inside the skipped span are dropped wholesale, so no `{` in the garbage
        // can start a phantom record. The newline itself is consumed here.
        if (b.resync_from != MapBuilder::NO_RESYNC) {
            uint32_t r = b.resync_from;
            while (r < static_cast<uint32_t>(buffer_length) && buffer[r] != '\n') ++r;
            b.resync_from = MapBuilder::NO_RESYNC;
            b.begin_line(r + 1);
            while (i + 1 < M && markers[i + 1].position <= r) ++i;
            continue;
        }
        // Minimal extent: an inline predicate failed (discard the record) OR all wanted
        // columns are found (bank it) — either way the tail never materialises a field.
        // It is still WALKED, structurally: the record must close with its own '}' on this
        // line (depth back to 0, no newline inside a string), or the line is truncated and
        // rejected — otherwise a record cut short after its wanted columns would be banked
        // as a row. We are at depth 1 (inside the record's object), outside any string.
        if (b.skip_rest) {
            b.skip_rest = false;
            int depth = 1;
            bool in_str = false;
            uint32_t esc = 0xFFFFFFFFu;
            size_t j = i + 1;
            for (; j < M; ++j) {
                const uint8_t t = markers[j].marker_type;
                if (t == NL) break;  // the line ended before the record closed
                const uint32_t p = markers[j].position;
                if (in_str) {
                    if (p == esc) { esc = 0xFFFFFFFFu; continue; }
                    if (t == static_cast<uint8_t>(MarkerType::BACKSLASH)) esc = p + 1;
                    else if (t == static_cast<uint8_t>(MarkerType::QUOTE)) in_str = false;
                    continue;
                }
                if (t == static_cast<uint8_t>(MarkerType::QUOTE)) in_str = true;
                else if (t == static_cast<uint8_t>(MarkerType::BRACE_OPEN) ||
                         t == static_cast<uint8_t>(MarkerType::BRACKET_OPEN)) ++depth;
                else if ((t == static_cast<uint8_t>(MarkerType::BRACE_CLOSE) ||
                          t == static_cast<uint8_t>(MarkerType::BRACKET_CLOSE)) && --depth == 0) break;
            }
            if (j < M && markers[j].marker_type != NL) {
                // Closed at markers[j] (its '}'): bank or discard, then hand the rest of
                // the line back to step(), which allows only whitespace up to the newline.
                if (b.record_dead) { b.discard_record(); b.record_dead = false; }
                else b.bank_record();
                b.state = State::EXPECT_RECORD_START;
                b.tail_start = markers[j].position + 1;
                i = j;
            } else {
                // The newline (or the end of the buffer) ends the rejected line.
                b.reject_line();
                b.begin_line((j < M ? markers[j].position : static_cast<uint32_t>(buffer_length)) + 1);
                i = j < M ? j : M - 1;
            }
        }
    }
    b.finish();
    if (b.malformed_found) { b.rs.malformed = true; b.rs.malformed_pos = b.malformed_at; }
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
// buffer. The head always ends just after a newline (or at the buffer end), so no
// record in it is cut short. May hold more than `want` records; callers take the first
// `want`.
static RecordSet build_head(const uint8_t* buffer, size_t buffer_length, size_t want) {
    for (size_t lines = want; ; lines *= 2) {
        size_t end = 0;
        for (size_t seen = 0; seen < lines && end < buffer_length; ++seen) {
            const void* nl = std::memchr(buffer + end, '\n', buffer_length - end);
            end = nl ? static_cast<size_t>(static_cast<const uint8_t*>(nl) - buffer) + 1
                     : buffer_length;
        }
        const auto markers = scan_structural_markers(buffer, end);
        RecordSet head = build_map(buffer, end, markers, nullptr);
        if (head.num_records() >= want || end == buffer_length) return head;
    }
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
