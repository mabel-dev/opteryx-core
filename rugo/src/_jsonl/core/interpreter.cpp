#include "interpreter.hpp"
#include "field_span.hpp"
#include <cstring>
#include <cctype>
#include <algorithm>
#include <array>

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
    PUSH_RECORD              = 8    // }
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
    t[3][int(K::COLON)]  = { S::EXPECT_VALUE,       A::NONE };

    // State 4: EXPECT_VALUE
    t[4][int(K::QUOTE)]  = { S::IN_STRING_VALUE,    A::START_VALUE };
    t[4][int(K::LBRACE)] = { S::IN_UNQUOTED_VALUE,  A::START_VALUE };
    t[4][int(K::MINUS)]   = { S::IN_UNQUOTED_VALUE, A::START_VALUE };
    t[4][int(K::DIGIT)]   = { S::IN_UNQUOTED_VALUE, A::START_VALUE };
    t[4][int(K::T)]       = { S::IN_UNQUOTED_VALUE, A::START_VALUE };
    t[4][int(K::F)]       = { S::IN_UNQUOTED_VALUE, A::START_VALUE };
    t[4][int(K::N)]       = { S::IN_UNQUOTED_VALUE, A::START_VALUE };
    t[4][int(K::OTHER)]   = { S::IN_UNQUOTED_VALUE, A::START_VALUE }; // catches '[' and anything else

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

} // anonymous namespace

std::vector<std::vector<FieldSpan>> build_map(
    const uint8_t* buffer,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers) {

    std::vector<std::vector<FieldSpan>> records;
    std::vector<FieldSpan> current_record;
    // Heuristic pre‑allocation
    records.reserve(markers.size() / 20 + 1);
    current_record.reserve(16);

    State state = State::EXPECT_RECORD_START;
    uint32_t key_start = 0, key_end = 0, key_width = 0;
    uint32_t value_start = 0, value_end = 0, value_width = 0;
    ValueType value_type = ValueType::Unknown;
    uint32_t ordinal = 0;

    for (const auto& m : markers) {
        uint8_t ch = buffer[m.position];
        CharClass cls = char_class_table[ch];

        // Transition
        const Transition& tr = transition_table[static_cast<int>(state)][static_cast<int>(cls)];
        Action action = tr.action;

        // --- execute action ---
        switch (action) {
        case Action::START_RECORD:
            ordinal = 0;
            current_record.clear();
            break;

        case Action::START_KEY:
            key_start = m.position + 1; // first char after opening quote
            break;

        case Action::END_KEY:
            key_end = m.position - 1;   // last char before closing quote
            key_width = key_end - key_start + 1;
            break;

        case Action::START_VALUE: {
            // Determine value type and start
            value_start = m.position;
            switch (ch) {
            case '"': value_type = ValueType::String;   break;
            case '{': value_type = ValueType::Object;   break;
            case '[': value_type = ValueType::Array;    break;
            case 't': case 'f': value_type = ValueType::Boolean; break;
            case 'n': value_type = ValueType::Null;     break;
            default:  value_type = ValueType::Integer;  break; // minus / digit / unexpected
            }
            break;
        }
        case Action::END_STRING_VAL:
            value_end = m.position - 1; // before closing quote
            value_width = value_end - value_start + 1;
            current_record.emplace_back(key_start, key_width,
                                        value_start, value_width,
                                        value_type, ordinal);
            ++ordinal;
            break;

        case Action::END_UNQUOTED_VAL: {
            value_end = m.position - 1; // char before comma / }
            // Fast path: no trailing whitespace
            if (value_end > value_start && is_ws(buffer[value_end])) {
                while (value_end > value_start && is_ws(buffer[value_end])) {
                    --value_end;
                }
            }
            value_width = value_end - value_start + 1;
            current_record.emplace_back(key_start, key_width,
                                        value_start, value_width,
                                        value_type, ordinal);
            ++ordinal;
            break;
        }
        case Action::END_UNQUOTED_VAL_NEWLINE: {
            // Newline ends the value and the record
            value_end = m.position - 1; // char before newline
            if (value_end > value_start && is_ws(buffer[value_end])) {
                while (value_end > value_start && is_ws(buffer[value_end])) {
                    --value_end;
                }
            }
            value_width = value_end - value_start + 1;
            current_record.emplace_back(key_start, key_width,
                                        value_start, value_width,
                                        value_type, ordinal);
            ++ordinal;
            // now push the completed record
            records.push_back(std::move(current_record));
            current_record.clear();
            break;
        }
        case Action::PUSH_RECORD:
            records.push_back(std::move(current_record));
            current_record.clear();
            break;

        case Action::NONE:
        default:
            break;
        }

        state = tr.next_state;
    }

    // Handle incomplete final record
    if (!current_record.empty()) {
        records.push_back(std::move(current_record));
    }

    return records;
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

// Parallel document mapping (delegates to sequential for now)
InterpreterResult interpret_jsonl_parallel(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers,
    const ParseContext& context,
    OrdinalPredictor& predictor,
    size_t min_rows_per_thread) {
    // For now, just call sequential version
    // TODO: Implement actual parallelization with BS::thread_pool
    return interpret_jsonl(buffer_data, buffer_length, markers, context, predictor);
}

} // namespace rugo::_jsonl
