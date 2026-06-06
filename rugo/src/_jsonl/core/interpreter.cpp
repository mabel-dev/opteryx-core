#include "interpreter.hpp"
#include "field_span.hpp"
#include "value_parser.hpp"   // evaluate_predicate (inline filter pushdown)
#include <array>
#include <cstring>
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

    // State 0: EXPECT_RECORD_START
    t[0][int(K::LBRACE)] = { S::EXPECT_KEY_QUOTE,   A::START_RECORD };
    // other characters: stay (already set)

    // State 1: EXPECT_KEY_QUOTE
    t[1][int(K::QUOTE)]  = { S::IN_KEY,            A::START_KEY };
    t[1][int(K::RBRACE)] = { S::EXPECT_RECORD_START, A::PUSH_RECORD };
    t[1][int(K::NEWLINE)]= { S::EXPECT_RECORD_START, A::NONE };  // unexpected newline -> reset

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

// Bound a container value whose opening '[' or '{' is at `start`. Walks raw bytes
// tracking string state and backslash escapes so interior commas, brackets and braces
// — including those inside quoted strings — do not close it early. Returns the index of
// the matching closing bracket/brace, or `limit - 1` if the container never closes.
inline uint32_t scan_container(const uint8_t* buf, uint32_t start, uint32_t limit) {
    int depth = 0;
    bool in_string = false;
    bool escaped = false;
    for (uint32_t p = start; p < limit; ++p) {
        const uint8_t c = buf[p];
        if (in_string) {
            if (escaped)          escaped = false;
            else if (c == '\\')   escaped = true;
            else if (c == '"')    in_string = false;
        } else if (c == '"') {
            in_string = true;
        } else if (c == '[' || c == '{') {
            ++depth;
        } else if (c == ']' || c == '}') {
            if (--depth == 0) return p;
        }
    }
    return limit - 1;
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
    bool skip_rest = false;
    bool record_dead = false;

    MapBuilder(const uint8_t* buf, const MapProjection* p)
        : buffer(buf), proj(p), num_wanted(p ? p->num_wanted : 0) {
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

    // Close the in-progress record. Bank: record its end offset (dropping empty records,
    // matching prior finalize semantics). Discard: drop its partial spans (predicate failed).
    inline void bank_record() {
        if (rs.spans.size() > record_start())
            rs.offsets.push_back(static_cast<uint32_t>(rs.spans.size()));
    }
    inline void discard_record() { rs.spans.resize(record_start()); }

    inline void step(uint32_t pos, uint8_t ch) {
        CharClass cls = char_class_table[ch];
        const Transition& tr = transition_table[static_cast<int>(state)][static_cast<int>(cls)];
        switch (tr.action) {
        case Action::START_RECORD:
            ordinal = 0; found = 0; record_dead = false; break;
        case Action::SET_COLON:
            colon_pos = pos; break;
        case Action::START_KEY:
            key_start = pos + 1; break;
        case Action::END_KEY:
            key_end = pos - 1; key_width = key_end - key_start + 1;
            if (proj) {
                // Exact match against the wanted set — length + first-byte reject, then
                // memcmp. No hashing.
                cur_wanted = false; cur_pred_idx = -1;
                const uint8_t first = buffer[key_start];
                for (const WantedColumn& w : *proj->columns) {
                    if (key_width == w.len && first == w.first &&
                        std::memcmp(buffer + key_start, w.name, w.len) == 0) {
                        cur_wanted = true; cur_pred_idx = w.pred_idx; break;
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

    inline void finish(bool emit_trailing) {
        if (emit_trailing && rs.spans.size() > record_start()) bank_record();
    }
};
}  // namespace

RecordSet build_map(
    const uint8_t* buffer,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers,
    const MapProjection* proj) {
    MapBuilder b(buffer, proj);
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
            const uint32_t close = scan_container(buffer, pos, static_cast<uint32_t>(buffer_length));
            if (b.emit_container(pos, close, ch == '[' ? ValueType::Array : ValueType::Object))
                b.skip_rest = true;
            b.state = State::EXPECT_SEPARATOR;
            while (i + 1 < M && markers[i + 1].position <= close) ++i;
        } else {
            b.step(pos, ch);
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
    b.finish(true);
    return std::move(b.rs);
}

std::vector<std::string> first_record_keys(const RecordSet& rs, const uint8_t* buffer) {
    std::vector<std::string> keys;
    if (rs.num_records() == 0) return keys;
    const RecordView rec = rs[0];
    keys.reserve(rec.size());
    for (const FieldSpan& f : rec)
        keys.emplace_back(reinterpret_cast<const char*>(buffer + f.key_start), f.key_width);
    return keys;
}

// SPIKE: same data-blind FSM as the no-projection build_map, but driven by iterating set
// bits of a structural bitmap rather than walking a marker vector.
RecordSet build_map_bitmap(
    const uint8_t* buffer,
    size_t buffer_length,
    const std::vector<uint64_t>& bitmap) {
    RecordSet rs;
    rs.spans.reserve(buffer_length / 8 + 1);
    rs.offsets.reserve(buffer_length / 64 + 2);
    rs.offsets.push_back(0);

    State state = State::EXPECT_RECORD_START;
    uint32_t key_start = 0, key_width = 0;
    uint32_t value_start = 0, value_width = 0, colon_pos = 0, ordinal = 0;
    ValueType vt = ValueType::Unknown;

    auto commit = [&]() {
        rs.spans.emplace_back(key_start, key_width, value_start, value_width, vt, ordinal);
        ++ordinal;
    };
    auto end_record = [&]() {
        rs.offsets.push_back(static_cast<uint32_t>(rs.spans.size()));
        ordinal = 0;
    };
    auto emit_unquoted = [&](uint32_t pos) {
        value_start = colon_pos + 1;
        while (value_start < pos && is_ws(buffer[value_start])) ++value_start;
        uint32_t ve = pos - 1;
        while (ve > value_start && is_ws(buffer[ve])) --ve;
        value_width = ve - value_start + 1;
        vt = classify_first(buffer[value_start]);
        commit();
    };

    const size_t nwords = bitmap.size();
    uint32_t skip_until = 0;  // structural bits with pos <= skip_until were swallowed by a container
    for (size_t w = 0; w < nwords; ++w) {
        uint64_t word = bitmap[w];
        const uint32_t base = static_cast<uint32_t>(w << 6);
        while (word) {
            const uint32_t pos = base + static_cast<uint32_t>(__builtin_ctzll(word));
            word &= word - 1;  // clear lowest set bit
            if (skip_until && pos <= skip_until) continue;

            const uint8_t ch = buffer[pos];
            if ((ch == '[' || ch == '{') && state == State::EXPECT_VALUE) {
                const uint32_t close = scan_container(buffer, pos, static_cast<uint32_t>(buffer_length));
                value_start = pos; value_width = close - pos + 1;
                vt = ch == '[' ? ValueType::Array : ValueType::Object;
                commit();
                state = State::EXPECT_SEPARATOR;
                skip_until = close;
                continue;
            }
            const CharClass cls = char_class_table[ch];
            const Transition& tr = transition_table[static_cast<int>(state)][static_cast<int>(cls)];
            switch (tr.action) {
            case Action::START_RECORD: ordinal = 0; break;
            case Action::SET_COLON:    colon_pos = pos; break;
            case Action::START_KEY:    key_start = pos + 1; break;
            case Action::END_KEY:      key_width = (pos - 1) - key_start + 1; break;
            case Action::START_VALUE:
                value_start = pos + (ch == '"' ? 1u : 0u);
                vt = (ch == '"') ? ValueType::String : ValueType::Integer;
                break;
            case Action::END_STRING_VAL:
                value_width = (pos - 1) - value_start + 1; commit(); break;
            case Action::END_UNQUOTED_VAL:
                emit_unquoted(pos); break;
            case Action::END_UNQUOTED_VAL_NEWLINE:
                emit_unquoted(pos); end_record(); break;
            case Action::PUSH_RECORD:
                end_record(); break;
            case Action::NONE:
            default: break;
            }
            state = tr.next_state;
        }
    }
    if (rs.spans.size() > rs.offsets.back()) end_record();
    return rs;
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
