#include "interpreter.hpp"
#include "field_span.hpp"
#include "value_parser.hpp"   // evaluate_predicate (inline filter pushdown)
#include <algorithm>
#include <array>
#include <cstring>
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
    END_UNQUOTED_VAL_NEWLINE = 7,   // newline ending an unquoted value + finish record
    PUSH_RECORD              = 8,   // }
    SET_COLON                = 9,   // remember ':' position — anchors the unquoted slice
    ABANDON_RECORD           = 10   // newline before the first key closed the record early
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

    // State 0: EXPECT_RECORD_START
    t[0][int(K::LBRACE)] = { S::EXPECT_KEY_QUOTE,   A::START_RECORD };
    // other characters: stay (already set)

    // State 1: EXPECT_KEY_QUOTE
    t[1][int(K::QUOTE)]  = { S::IN_KEY,            A::START_KEY };
    t[1][int(K::RBRACE)] = { S::EXPECT_RECORD_START, A::PUSH_RECORD };
    t[1][int(K::NEWLINE)]= { S::EXPECT_RECORD_START, A::ABANDON_RECORD };  // unexpected newline -> reset

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
    t[4][int(K::RBRACE)]  = { S::EXPECT_SEPARATOR,    A::END_UNQUOTED_VAL };
    t[4][int(K::NEWLINE)] = { S::EXPECT_RECORD_START, A::END_UNQUOTED_VAL_NEWLINE };

    // State 5: IN_STRING_VALUE
    t[5][int(K::QUOTE)]   = { S::EXPECT_SEPARATOR,  A::END_STRING_VAL };

    // State 6: IN_UNQUOTED_VALUE
    t[6][int(K::COMMA)]   = { S::EXPECT_KEY_QUOTE,  A::END_UNQUOTED_VAL };
    t[6][int(K::RBRACE)]  = { S::EXPECT_SEPARATOR,  A::END_UNQUOTED_VAL };
    t[6][int(K::NEWLINE)] = { S::EXPECT_RECORD_START,A::END_UNQUOTED_VAL_NEWLINE };

    // State 7: EXPECT_SEPARATOR
    t[7][int(K::COMMA)]   = { S::EXPECT_KEY_QUOTE,   A::NONE };
    t[7][int(K::RBRACE)]  = { S::EXPECT_RECORD_START, A::PUSH_RECORD };
    t[7][int(K::NEWLINE)] = { S::EXPECT_RECORD_START, A::PUSH_RECORD };

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
// with `close_pos = limit - 1`; on a raw, unescaped newline inside a nested string
// (invalid JSON — RFC 8259 requires the two bytes '\'+'n', not this control byte;
// confirmed against a real defect in the JSONBench Bluesky dump, see
// tests/performance/jsonbench/README.md's "Known data-quality defect") returns the
// newline's marker index with `close_pos` at the newline, so the caller resyncs at the
// line boundary rather than silently absorbing garbage as string content.
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
        if (in_string) {
            if (p == escaped_until) { escaped_until = 0xFFFFFFFFu; continue; }  // escaped content
            switch (static_cast<MarkerType>(t)) {
            case MarkerType::BACKSLASH: escaped_until = p + 1; break;  // escapes next byte
            case MarkerType::QUOTE:     in_string = false; break;
            case MarkerType::NEWLINE:   closed = false; close_pos = p; return j;
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
            break;  // ':', ',', '\n', '\\' outside a string — not structure for bounding
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
        if (vs >= end || buf[vs] == 'n') return false;   // empty, or JSON null => NULL cell
        uint32_t ve = end - 1;
        while (ve > vs && is_ws(buf[ve])) --ve;
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
    const uint32_t close_byte = markers[close_idx].position;

    for (size_t j = open_idx + 1; j < close_idx; ++j) {
        const uint32_t p = markers[j].position;
        const MarkerType t = static_cast<MarkerType>(markers[j].marker_type);

        switch (st) {
        case KEY_EXPECT:
            if (t == MarkerType::QUOTE) { key_start = p + 1; st = KEY_IN; }
            break;

        case KEY_IN:
            if (p == escaped_until) { escaped_until = 0xFFFFFFFFu; break; }
            if (t == MarkerType::BACKSLASH) { escaped_until = p + 1; break; }
            if (t == MarkerType::QUOTE) {
                const uint32_t klen = p - key_start;
                wanted = (klen == sub_len && buf[key_start] == sub_first &&
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

    // Malformed-input tracking (fail_on_error support). `line_start` is the byte position
    // right after the previous top-level newline (or 0); it lets a NEWLINE marker hit while
    // still EXPECT_RECORD_START cheaply check whether the "line" it just closed held any
    // non-whitespace content that never opened a record (a garbage line, e.g. "NOT JSON").
    // Only the FIRST occurrence is kept — this is a detector, not a full diagnostic pass.
    uint32_t line_start = 0;
    uint32_t cur_record_start_pos = 0;  // position of the current record's '{'
    // EXPECT_RECORD_START is BOTH "nothing has happened yet" and "a record just closed
    // and we're ready for the next one" — state alone can't tell a garbage line ("NOT
    // JSON") apart from a line that legitimately opened and closed a record. This tracks
    // whether a '{' was seen since the last top-level newline, so the garbage check below
    // only fires when NO record was ever attempted on this line.
    bool saw_open_brace_since_newline = false;
    bool malformed_found = false;
    uint32_t malformed_at = 0;
    uint32_t malformed_count = 0;
    inline void flag_malformed(uint32_t pos) {
        if (!malformed_found) { malformed_found = true; malformed_at = pos; }
        ++malformed_count;
    }

    // Set to the byte offset where a malformed record was detected; the driver then
    // resyncs at the next PHYSICAL line boundary (see build_map). NO_RESYNC = nothing
    // pending. Resyncing has to be line-based, not structure-based: after a raw newline
    // splits one JSON record across two physical lines, the tail left on the second line
    // is arbitrary garbage that still contains perfectly well-formed-looking `{...}`
    // fragments (Bluesky's nested JSON is full of them). Letting the FSA resume on that
    // tail makes it bank those fragments as extra spurious records — the observed
    // ~27-29 phantom rows per affected shard. A JSONL record is defined by its line, so
    // the only sound recovery point is the next '\n'.
    static constexpr uint32_t NO_RESYNC = 0xFFFFFFFFu;
    uint32_t resync_from = NO_RESYNC;

    MapBuilder(const uint8_t* buf, uint32_t buf_len, const MapProjection* p)
        : buffer(buf), buffer_length(buf_len), proj(p), num_wanted(p ? p->num_wanted : 0) {
        rs.offsets.push_back(0);
    }

    // First span index of the in-progress record. Invariant: at each record start,
    // rs.spans.size() == record_start() (every record either banks or discards, restoring it).
    inline uint32_t record_start() const { return rs.offsets.back(); }

    // Append the staged value as a field iff it is wanted, evaluating an inline predicate
    // on it; always advance the ordinal so emitted spans keep their true object position.
    // Returns true when the driver should stop the record (predicate failed, or last wanted
    // column found).
    inline bool commit_field() {
        bool stop = false;
        if (cur_wanted) {
            rs.spans.emplace_back(key_start, key_width, value_start, value_width, value_type, ordinal);
            if (cur_pred_idx >= 0 &&
                !evaluate_predicate(buffer, rs.spans.back(), (*proj->predicates)[cur_pred_idx])) {
                record_dead = true;
                stop = true;
            } else if (proj && ++found >= num_wanted) {
                stop = true;
            }
        }
        ++ordinal;
        return stop;
    }

    // A wanted column that resolved to NOTHING (nested sub-key absent, or its value was
    // JSON null). Emits NO span — which is exactly how an absent top-level column already
    // represents a NULL cell, since column lookup is by key and a missing key yields null —
    // but still advances the ordinal and the found count so minimal-extent stops the record
    // on schedule rather than scanning the tail for a column that will never arrive.
    //
    // Deliberately does NOT kill the record when the column carries a predicate: an absent
    // top-level predicate column doesn't drop the row today either (no span => the
    // predicate is never evaluated), and silently diverging from that here would make
    // nested and flat predicates mean different things. Nested predicate pushdown has to
    // settle that question explicitly when it is built.
    inline bool miss_field() {
        ++ordinal;
        return proj && ++found >= num_wanted;
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
        // Escape handling inside keys/string values: a '\' makes the next byte literal, so an
        // escaped quote (\") or backslash (\\) is content, not a delimiter. (~free; the
        // alternative — masking escapes out of the scan — costs ~1.4× scan for no net win
        // below ~40% in-string density. See scan_structural_masked.)
        if (state == State::IN_KEY || state == State::IN_STRING_VALUE) {
            if (pos == escaped_until) { escaped_until = 0xFFFFFFFFu; return; }  // escaped content
            if (ch == '\\') { escaped_until = pos + 1; return; }               // escapes next byte
        }
        CharClass cls = char_class_table[ch];
        if (cls == CharClass::NEWLINE) {
            // A raw, unescaped newline while a key/string is still open is not ordinary
            // content -- RFC 8259 requires control characters (U+0000-U+001F) inside a
            // JSON string to be escaped. Confirmed against a real defect in the JSONBench
            // Bluesky dump (tests/performance/jsonbench/README.md's "Known data-quality
            // defect"): left unchecked, the FSA just kept consuming bytes as "string
            // content" until it happened to find some LATER, unrelated quote to treat as
            // the close -- silently fabricating garbage records from fragments of 2+ real
            // records instead of ever failing loud. Abandon this record immediately
            // rather than let one bad byte corrupt everything the scan reads afterward.
            if (state == State::IN_KEY || state == State::IN_STRING_VALUE) {
                flag_malformed(cur_record_start_pos);
                discard_record();
                state = State::EXPECT_RECORD_START;
                resync_from = pos;  // driver skips to the next physical line boundary
                return;
            }
            // A top-level newline (still EXPECT_RECORD_START) that closes a line which
            // never even opened a record, yet held non-whitespace content, is a line that
            // was never JSON at all — e.g. "NOT JSON AT ALL". Today that's silently
            // dropped either way; this only records where it happened so fail_on_error
            // can raise on it. Must gate on saw_open_brace_since_newline, not just state:
            // EXPECT_RECORD_START is equally the state right after a record legitimately
            // closed on this same line.
            if (state == State::EXPECT_RECORD_START && !saw_open_brace_since_newline) {
                for (uint32_t p = line_start; p < pos; ++p) {
                    if (!is_ws(buffer[p])) { flag_malformed(line_start); break; }
                }
            }
            line_start = pos + 1;
            saw_open_brace_since_newline = false;
        }
        const Transition& tr = transition_table[static_cast<int>(state)][static_cast<int>(cls)];
        switch (tr.action) {
        case Action::START_RECORD:
            ordinal = 0; found = 0; record_dead = false; escaped_until = 0xFFFFFFFFu;
            cur_record_start_pos = pos;
            saw_open_brace_since_newline = true;
            break;
        case Action::ABANDON_RECORD:
            flag_malformed(cur_record_start_pos);
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
                cur_wanted = false; cur_pred_idx = -1; cur_col = nullptr;
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
        case Action::END_UNQUOTED_VAL_NEWLINE:
            emit_unquoted(pos);  // record ends at the newline; bank/discard here (no driver skip)
            if (record_dead) { discard_record(); record_dead = false; }
            else bank_record();
            break;
        case Action::PUSH_RECORD:
            bank_record();
            break;
        case Action::NONE:
        default:
            break;
        }
        state = tr.next_state;
    }

    inline void finish() {
        // A record with committed field-spans but no closing brace by the time the scan
        // ends is truncated, not complete -- a record that closed normally already
        // banked via Action::PUSH_RECORD, so record_start() == spans.size() here and this
        // is a no-op for it (see commit_field()/bank_record()). What reaches this branch
        // is genuine end-of-file mid-record, OR (before the raw-newline check above
        // existed) a threaded chunk boundary (interpret_jsonl_threaded's own newline
        // scan is exactly as JSON-unaware as this one used to be) landing on a malformed
        // embedded newline. Previously this unconditionally banked the fragment as if it
        // were a real row -- the second, silent source (alongside the raw-newline case
        // above) of the extra/garbage rows described in
        // tests/performance/jsonbench/README.md's "Known data-quality defect" section.
        if (rs.spans.size() > record_start()) {
            flag_malformed(cur_record_start_pos);
            discard_record();
        }
    }
};
}  // namespace

RecordSet build_map(
    const uint8_t* buffer,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers,
    const MapProjection* proj) {
    MapBuilder b(buffer, static_cast<uint32_t>(buffer_length), proj);
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
                // Truncated/malformed container value (ran out of buffer, or a raw
                // newline inside a nested string -- see scan_container) means this
                // record's JSON was never valid. The whole record must be dropped, not
                // banked with this one field's value silently replaced by a truncated
                // slice -- a wrong-but-plausible-looking row is worse than no row. Do
                // NOT emit_container() the truncated slice: that would stage a bogus
                // field value AND leave the FSA mid-record on garbage.
                b.flag_malformed(pos);
                b.discard_record();
                b.state = State::EXPECT_RECORD_START;
                b.resync_from = close;  // resync at the next physical line boundary below
            } else {
                // Nested projection: the wanted column named a sub-key inside this
                // container (`commit.collection`), so emit a span for the SUB-VALUE and
                // never materialise the container itself. A miss — key absent, value JSON
                // null, or the container is an array — is a NULL cell, which is what
                // commit_field() records when cur_wanted is left false, matching what the
                // downstream extraction would have produced for the same path.
                if (b.cur_wanted && b.cur_col && b.cur_col->sub_len) {
                    uint32_t nstart = 0, nwidth = 0;
                    ValueType ntype = ValueType::Unknown;
                    if (find_nested_field(buffer, markers, i, close_idx,
                                          b.cur_col->sub, b.cur_col->sub_len,
                                          b.cur_col->sub_first, nstart, nwidth, ntype)) {
                        if (b.emit_container(nstart, nstart + nwidth - 1, ntype))
                            b.skip_rest = true;
                    } else {
                        // Absent/null: still counts as this wanted column being resolved,
                        // so minimal-extent can stop the record on schedule. commit_field
                        // is bypassed (no span) but the ordinal must still advance.
                        if (b.miss_field()) b.skip_rest = true;
                    }
                } else if (b.emit_container(pos, close, ch == '[' ? ValueType::Array : ValueType::Object)) {
                    b.skip_rest = true;
                }
                b.state = State::EXPECT_SEPARATOR;
                i = close_idx;  // every marker the container swallowed is now behind us
            }
        } else {
            b.step(pos, ch);
        }
        // A malformed record was detected (here or inside step()): recover at the next
        // PHYSICAL line boundary. Deliberately a dumb byte scan for '\n' rather than
        // resuming the FSA on the corrupt tail -- see MapBuilder::resync_from. Markers
        // inside the skipped span are dropped wholesale, so no `{` in the garbage can
        // start a phantom record.
        if (b.resync_from != MapBuilder::NO_RESYNC) {
            uint32_t r = b.resync_from + 1;
            while (r < static_cast<uint32_t>(buffer_length) && buffer[r] != '\n') ++r;
            b.resync_from = MapBuilder::NO_RESYNC;
            b.state = State::EXPECT_RECORD_START;
            b.line_start = r + 1;
            b.saw_open_brace_since_newline = false;
            while (i + 1 < M && markers[i + 1].position <= r) ++i;
            continue;
        }
        // Minimal extent: an inline predicate failed (discard the record) OR all wanted
        // columns are found (bank it) — either way jump to the record's newline, skipping
        // the tail entirely so failing/satisfied rows never materialise their later fields.
        if (b.skip_rest) {
            b.skip_rest = false;
            if (b.record_dead) { b.discard_record(); b.record_dead = false; }
            else b.bank_record();
            b.state = State::EXPECT_RECORD_START;
            while (i + 1 < M && markers[i + 1].marker_type != NL) ++i;
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

// -----------------------------------------------------------------------------
// Legacy compatibility (unchanged, not used)
// -----------------------------------------------------------------------------

std::vector<FieldSpan> RecordInterpreter::parse_record(
    const uint8_t*, uint32_t, uint32_t,
    const std::vector<MarkerPosition>&,
    const std::map<std::string, uint32_t>&) {
    return {};
}

uint32_t RecordInterpreter::find_closing_quote(
    uint32_t, const std::vector<MarkerPosition>&,
    const std::map<std::string, uint32_t>&, uint32_t) {
    return 0;
}

ValueType RecordInterpreter::classify_value_type(const uint8_t*, uint32_t) {
    return ValueType::Unknown;
}

uint32_t RecordInterpreter::skip_whitespace(const uint8_t*, uint32_t, uint32_t) {
    return 0;
}

std::string RecordInterpreter::extract_key(const uint8_t*, uint32_t, uint32_t) {
    return "";
}

} // namespace rugo::_jsonl
