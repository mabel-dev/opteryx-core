#include <Python.h>  // must precede any draken header that uses PyObject

#include "column_builder.hpp"
#include "fast_parsers.hpp"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <future>

// Producer surface (definitions resolved at load via RTLD_GLOBAL from draken_native.so):
#include "draken_bridge.h"  // draken_vector_own_string, draken_vector_own_array(_numeric)
#include "string_slot.h"    // DrakenStringSlot, draken_build_string_slot, str_init_null
#include "alloc.h"          // draken_malloc
#include "buffers.h"        // DrakenType, DRAKEN_VARCHAR
#include "BS_thread_pool.hpp"
#include "yyjson.h"         // per-row array element parsing (parse_array_column)

// extract_column() pulls one column out as raw-byte slices (StringColumnResult);
// build_typed_vector / build_varchar_vector materialise owned Draken vectors, and
// merge_string_column stitches a column across chunks. See ARCHITECTURE.md.

namespace rugo::_jsonl {

namespace {

static inline bool key_matches(
    const uint8_t* buf,
    uint32_t       key_start,
    uint32_t       key_width,
    const char*    name,
    size_t         name_len) noexcept
{
    return static_cast<size_t>(key_width) == name_len &&
           std::memcmp(buf + key_start, name, name_len) == 0;
}

// Parse exactly 4 hex digits at p into *out. Returns false on any non-hex byte.
static inline bool parse_hex4(const uint8_t* p, uint32_t* out) noexcept {
    uint32_t v = 0;
    for (int k = 0; k < 4; ++k) {
        const uint8_t c = p[k];
        uint32_t d;
        if (c >= '0' && c <= '9')      d = c - '0';
        else if (c >= 'a' && c <= 'f') d = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') d = c - 'A' + 10;
        else return false;
        v = (v << 4) | d;
    }
    *out = v;
    return true;
}

// Append codepoint cp as UTF-8 to out (1–4 bytes). Lenient on lone surrogates (WTF-8).
static inline void append_utf8(uint32_t cp, std::vector<uint8_t>& out) {
    if (cp <= 0x7F) {
        out.push_back(static_cast<uint8_t>(cp));
    } else if (cp <= 0x7FF) {
        out.push_back(static_cast<uint8_t>(0xC0 | (cp >> 6)));
        out.push_back(static_cast<uint8_t>(0x80 | (cp & 0x3F)));
    } else if (cp <= 0xFFFF) {
        out.push_back(static_cast<uint8_t>(0xE0 | (cp >> 12)));
        out.push_back(static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<uint8_t>(0x80 | (cp & 0x3F)));
    } else {
        out.push_back(static_cast<uint8_t>(0xF0 | (cp >> 18)));
        out.push_back(static_cast<uint8_t>(0x80 | ((cp >> 12) & 0x3F)));
        out.push_back(static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3F)));
        out.push_back(static_cast<uint8_t>(0x80 | (cp & 0x3F)));
    }
}

// Append the JSON-unescaped form of src[0..len) to `out`: handles \" \\ \/ \b \f \n \r \t
// and \uXXXX (with surrogate pairs -> UTF-8). Bulk-copies runs without backslashes, and is
// lenient on malformed escapes (copies the byte literally). The unescaped form is never
// longer than the input, so callers can size on `len`.
static void json_unescape(const uint8_t* src, uint32_t len, std::vector<uint8_t>& out) {
    uint32_t i = 0;
    while (i < len) {
        // Bulk-copy the run up to the next backslash.
        uint32_t run = i;
        while (run < len && src[run] != '\\') ++run;
        if (run > i) { out.insert(out.end(), src + i, src + run); i = run; }
        if (i >= len) break;
        // src[i] == '\\'
        if (i + 1 >= len) { out.push_back('\\'); ++i; break; }  // trailing backslash
        const uint8_t e = src[i + 1];
        switch (e) {
            case '"':  out.push_back('"');  i += 2; break;
            case '\\': out.push_back('\\'); i += 2; break;
            case '/':  out.push_back('/');  i += 2; break;
            case 'b':  out.push_back(0x08); i += 2; break;
            case 'f':  out.push_back(0x0C); i += 2; break;
            case 'n':  out.push_back(0x0A); i += 2; break;
            case 'r':  out.push_back(0x0D); i += 2; break;
            case 't':  out.push_back(0x09); i += 2; break;
            case 'u': {
                uint32_t cp;
                if (i + 6 > len || !parse_hex4(src + i + 2, &cp)) { out.push_back('\\'); ++i; break; }
                i += 6;
                if (cp >= 0xD800 && cp <= 0xDBFF && i + 6 <= len &&
                    src[i] == '\\' && src[i + 1] == 'u') {
                    uint32_t lo;
                    if (parse_hex4(src + i + 2, &lo) && lo >= 0xDC00 && lo <= 0xDFFF) {
                        cp = 0x10000u + ((cp - 0xD800u) << 10) + (lo - 0xDC00u);
                        i += 6;
                    }
                }
                append_utf8(cp, out);
                break;
            }
            default: out.push_back(e); i += 2; break;  // unknown escape -> literal char
        }
    }
}

static ColumnType infer_numeric_type(
    const uint8_t*                            buffer,
    const RecordSet&                          records,
    const char*                               col_ptr,
    size_t                                    col_len)
{
    bool saw_float = false;
    size_t limit   = std::min(records.size(), static_cast<size_t>(64));

    for (size_t row = 0; row < limit; ++row) {
        for (const auto& f : records[row]) {
            if (!key_matches(buffer, f.key_start, f.key_width, col_ptr, col_len))
                continue;
            if (is_null(buffer, f.value_start, f.value_start + f.value_width - 1))
                break;
            if (f.type == static_cast<uint8_t>(ValueType::Integer) ||
                f.type == static_cast<uint8_t>(ValueType::Double))
            {
                int64_t tmp_i = 0;
                if (!fast_parse_int64(buffer, f.value_start, f.value_start + f.value_width - 1, tmp_i)) {
                    double tmp_d = 0.0;
                    if (fast_parse_float64(buffer, f.value_start, f.value_start + f.value_width - 1, tmp_d))
                        saw_float = true;
                }
            }
            break;
        }
    }
    return saw_float ? ColumnType::Float64 : ColumnType::Int64;
}

}  // namespace

StringColumnResult extract_column(
    const uint8_t*                            buffer,
    const RecordSet&                          records,
    const std::string&                         column_name,
    OrdinalPredictor&                         predictor,
    bool                                       copy_bytes,
    bool                                       may_have_escapes,
    size_t                                     sample_size)
{
    const size_t num_rows = records.size();
    const size_t col_len  = column_name.size();

    StringColumnResult result;
    result.num_rows = num_rows;

    if (num_rows == 0) {
        return result;
    }

    // Allocate null bitmap (all valid by default)
    const size_t bitmap_bytes = (num_rows + 7) >> 3;
    result.null_bitmap.assign(bitmap_bytes, 0xFF);

    // Resolve each row to its FieldSpan (or nullptr if the key is absent); infer the
    // column's type from the first non-null value and detect column-scoped escapes in the
    // SAME pass (one combined key-matching walk, same total cost as the old resolve pass —
    // the old code's separate infer-loop duplicated this key-matching work in its own pass).
    // Value emission is a second, cheap pointer-chase pass below, once do_unescape is known.
    //
    // Column-scoped escape detection: a column only pays the copy+unescape cost if one of
    // ITS OWN values contains a backslash. `may_have_escapes` is a cheap whole-buffer
    // pre-check (computed once by the caller) that short-circuits this to zero cost when the
    // file has no backslashes anywhere; when it does, this narrows the penalty from "every
    // string column in the file" down to "columns that actually need unescaping" (e.g. a
    // nested text field with a backslash no longer taxes sibling scalar columns).
    std::vector<const FieldSpan*> resolved(num_rows, nullptr);

    // First-byte fast reject (mirrors WantedColumn's `first` field in interpreter.hpp):
    // key_width already gates out most non-matching fields, but same-length different-
    // content keys (common in wide/hetero-order records) still paid a full memcmp call.
    // Checking the first byte first turns most of those into a single byte compare.
    const uint8_t col_first = col_len ? static_cast<uint8_t>(column_name[0]) : 0;

    auto candidates = predictor.get_candidates(column_name);
    uint16_t last_seen = candidates.empty() ? 0xFFFF : candidates[0];
    bool inferred = false;
    bool col_has_escape = false;

    for (size_t row = 0; row < num_rows; ++row) {
        const auto& record = records[row];
        const FieldSpan* found = nullptr;

        // Fast path: try predicted ordinal first
        if (last_seen != 0xFFFF && last_seen < record.size()) {
            const auto& f = record[last_seen];
            if (f.key_width == col_len && buffer[f.key_start] == col_first &&
                std::memcmp(buffer + f.key_start, column_name.data(), col_len) == 0) {
                found = &f;
            }
        }

        // Slow path: linear scan (fallback if prediction missed)
        if (found == nullptr) {
            for (size_t i = 0; i < record.size(); ++i) {
                const auto& f = record[i];
                if (f.key_width == col_len && buffer[f.key_start] == col_first &&
                    std::memcmp(buffer + f.key_start, column_name.data(), col_len) == 0) {
                    last_seen = static_cast<uint16_t>(i);
                    predictor.update_history(column_name, last_seen);
                    found = &f;
                    break;
                }
            }
        }

        if (found != nullptr) {
            resolved[row] = found;
            const bool val_null =
                is_null(buffer, found->value_start, found->value_start + found->value_width - 1);
            if (!val_null) {
                result.any_value_seen = true;
                if (!inferred && row < sample_size) {
                    uint8_t vt = found->type;
                    if (vt == static_cast<uint8_t>(ValueType::String))
                        result.inferred_type = ColumnType::String;
                    else if (vt == static_cast<uint8_t>(ValueType::Boolean))
                        result.inferred_type = ColumnType::Bool;
                    else if (vt == static_cast<uint8_t>(ValueType::Integer))
                        result.inferred_type = ColumnType::Int64;
                    else if (vt == static_cast<uint8_t>(ValueType::Double))
                        result.inferred_type = ColumnType::Float64;
                    else if (vt == static_cast<uint8_t>(ValueType::Array))
                        result.inferred_type = ColumnType::Array;
                    else if (vt == static_cast<uint8_t>(ValueType::Object))
                        result.inferred_type = ColumnType::Variant;
                    inferred = true;
                }
                if (may_have_escapes && !col_has_escape &&
                    found->type == static_cast<uint8_t>(ValueType::String) &&
                    std::memchr(buffer + found->value_start, '\\', found->value_width) != nullptr) {
                    col_has_escape = true;
                }
            }
        } else {
            result.null_bitmap[row >> 3] &= ~(uint8_t(1u << (row & 7u)));
        }
    }

    const bool do_unescape = col_has_escape && result.inferred_type == ColumnType::String;
    result.data_owned = copy_bytes || do_unescape;

    // Preallocate estimated string data (rough estimate)
    if (result.data_owned) result.data.reserve(num_rows * 16);
    result.offsets.resize(num_rows);
    result.lengths.resize(num_rows);

    // Emit one value: NULL marks the bitmap; otherwise copy+unescape (do_unescape), copy
    // (copy_bytes), or reference the original buffer (zero-copy).
    auto emit_value = [&](const FieldSpan& f, size_t row) {
        const uint32_t vend = f.value_start + f.value_width - 1;
        if (is_null(buffer, f.value_start, vend)) {
            result.null_bitmap[row >> 3] &= ~(uint8_t(1u << (row & 7u)));
        } else if (do_unescape) {
            const uint32_t start = static_cast<uint32_t>(result.data.size());
            json_unescape(buffer + f.value_start, f.value_width, result.data);
            result.offsets[row] = start;
            result.lengths[row] = static_cast<uint32_t>(result.data.size() - start);
        } else if (copy_bytes) {
            result.offsets[row] = static_cast<uint32_t>(result.data.size());
            result.lengths[row] = f.value_width;
            result.data.insert(result.data.end(),
                               buffer + f.value_start, buffer + f.value_start + f.value_width);
        } else {
            result.offsets[row] = f.value_start;  // index into the original buffer
            result.lengths[row] = f.value_width;
        }
    };

    for (size_t row = 0; row < num_rows; ++row) {
        const FieldSpan* f = resolved[row];
        if (f != nullptr) emit_value(*f, row);
    }

    return result;
}

namespace {
// Copy scr's validity bitmap into a draken_malloc'd, SIMD-padded buffer (Arrow:
// bit set = valid), tail bits past n masked. Returns nullptr when every row is
// valid (normalization invariant — lets consumer fast-paths skip null handling).
static uint8_t* own_validity_from_scr(StringColumnResult& scr, uint32_t n) {
    if (n == 0 || scr.null_bitmap.empty()) return nullptr;
    const size_t nb = (static_cast<size_t>(n) + 7) >> 3;

    bool has_nulls = false;
    for (size_t b = 0; b < nb; ++b) {
        uint8_t valid_mask = 0xFF;
        if (b == nb - 1 && (n & 7)) valid_mask = static_cast<uint8_t>((1u << (n & 7)) - 1);
        if ((scr.null_bitmap[b] & valid_mask) != valid_mask) { has_nulls = true; break; }
    }
    if (!has_nulls) return nullptr;

    const uint32_t padded = ((static_cast<uint32_t>(nb) + 7u) & ~7u);
    const size_t alloc = padded ? padded : 8u;
    uint8_t* v = static_cast<uint8_t*>(draken_malloc(alloc));
    std::memset(v, 0xFF, alloc);
    std::memcpy(v, scr.null_bitmap.data(), nb);
    if (n & 7) v[nb - 1] &= static_cast<uint8_t>((1u << (n & 7)) - 1);  // mask tail
    return v;
}
}  // namespace

// ---------------------------------------------------------------------------
// build_varchar_vector — StringColumnResult → owned Draken VARCHAR Vector.
//
// Single C++ pass; no Python objects touch the data. Slots and arena are sized
// from the extracted bytes, populated as German-string slots, and handed to
// draken_vector_own_string which assumes ownership of all three buffers.
// ---------------------------------------------------------------------------
// Parse a column into VARCHAR string buffers (slots + arena + validity). No Python —
// safe off the GIL. Wrapped into a Vector later by wrap_column().
static ParsedColumn parse_varchar_column(const uint8_t* base, StringColumnResult& scr) {
    const uint32_t n = static_cast<uint32_t>(scr.num_rows);
    const bool has_nulls = !scr.null_bitmap.empty();
    const uint8_t* src = base;  // slices live at base + offsets[i]

    // Pass 1: size the arena — long-form slots (> STR_INLINE_MAX) only, valid rows only.
    size_t arena_size = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (has_nulls && !((scr.null_bitmap[i >> 3] >> (i & 7)) & 1)) continue;
        if (scr.lengths[i] > STR_INLINE_MAX) arena_size += scr.lengths[i];
    }

    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(
        draken_malloc(static_cast<size_t>(n) * sizeof(DrakenStringSlot)));
    uint8_t* arena = arena_size
        ? static_cast<uint8_t*>(draken_malloc(arena_size))
        : nullptr;

    // Pass 2: populate slots (and arena for long strings).
    uint32_t arena_offset = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (has_nulls && !((scr.null_bitmap[i >> 3] >> (i & 7)) & 1)) {
            str_init_null(&slots[i]);
            continue;
        }
        const uint32_t off = scr.offsets[i];
        const uint32_t len = scr.lengths[i];
        const uint8_t* bytes = len ? src + off : reinterpret_cast<const uint8_t*>("");
        if (len > STR_INLINE_MAX) {
            std::memcpy(arena + arena_offset, bytes, len);
            draken_build_string_slot(&slots[i], bytes, len, arena_offset);
            arena_offset += len;
        } else {
            draken_build_string_slot(&slots[i], bytes, len, 0);
        }
    }

    ParsedColumn pc;
    pc.is_string = true;
    pc.type      = DRAKEN_VARCHAR;
    pc.length    = n;
    pc.slots     = slots;
    pc.arena     = arena;
    pc.arena_len = arena_size;
    pc.validity  = own_validity_from_scr(scr, n);  // SIMD-padded, NULL when all-valid
    pc.all_null  = !scr.any_value_seen;
    return pc;
}

PyObject* build_varchar_vector(const uint8_t* base, StringColumnResult& scr) {
    ParsedColumn pc = parse_varchar_column(base, scr);
    return wrap_column(pc);
}

namespace {

static inline bool row_valid(const StringColumnResult& scr, uint32_t i) {
    return (scr.null_bitmap[i >> 3] >> (i & 7)) & 1u;
}

// Parse every valid slice (at base + offsets[i]) as int64 into data[i] (0 for nulls).
// False on first miss.
static bool try_fill_int64(const uint8_t* base, StringColumnResult& scr, uint32_t n, int64_t* data) {
    for (uint32_t i = 0; i < n; ++i) {
        if (!row_valid(scr, i)) { data[i] = 0; continue; }
        const uint32_t off = scr.offsets[i], len = scr.lengths[i];
        if (len == 0 || !fast_parse_int64(base, off, off + len - 1, data[i])) return false;
    }
    return true;
}

static bool try_fill_float64(const uint8_t* base, StringColumnResult& scr, uint32_t n, double* data) {
    for (uint32_t i = 0; i < n; ++i) {
        if (!row_valid(scr, i)) { data[i] = 0.0; continue; }
        const uint32_t off = scr.offsets[i], len = scr.lengths[i];
        if (len == 0 || !fast_parse_float64(base, off, off + len - 1, data[i])) return false;
    }
    return true;
}

}  // namespace

namespace {

// Coarse element-kind seen while classifying a column's array elements. Mirrors
// parse_typed_column's own speculate-then-widen shape (int64 -> float64 -> fallback),
// applied to the flattened set of every element across every row instead of one scalar
// per row. Nested containers or a genuine mix of incompatible scalar kinds (e.g. a
// number next to a string) put the WHOLE column out of v1 scope.
struct ArrayElementSurvey {
    bool saw_bool = false;
    bool saw_int = false;
    bool saw_real = false;
    bool saw_string = false;
    bool saw_nested = false;  // element itself is an array/object -> out of scope
};

// Classify one row's array text (yyjson_read + walk). Returns false on a JSON parse
// failure (shouldn't happen — the structural scanner already bounded/typed this span —
// but a parse fails safely into the varchar fallback rather than crashing).
static bool survey_array_row(const uint8_t* text, uint32_t len, ArrayElementSurvey& survey, size_t& elem_count) {
    yyjson_doc* doc = yyjson_read(reinterpret_cast<const char*>(text), len, 0);
    if (doc == nullptr) return false;
    yyjson_val* root = yyjson_doc_get_root(doc);
    if (root == nullptr || !yyjson_is_arr(root)) { yyjson_doc_free(doc); return false; }

    yyjson_val* val;
    yyjson_arr_iter iter = yyjson_arr_iter_with(root);
    while ((val = yyjson_arr_iter_next(&iter)) != nullptr) {
        ++elem_count;
        if (yyjson_is_null(val)) continue;
        if (yyjson_is_arr(val) || yyjson_is_obj(val)) { survey.saw_nested = true; break; }
        if (yyjson_is_bool(val)) survey.saw_bool = true;
        else if (yyjson_is_real(val)) survey.saw_real = true;
        else if (yyjson_is_int(val)) survey.saw_int = true;
        else if (yyjson_is_str(val)) survey.saw_string = true;
        else { survey.saw_nested = true; break; }  // defensive: unknown yyjson value kind
    }
    yyjson_doc_free(doc);
    return true;
}

// Fill parent_offsets/child buffers for a resolved child_type. Returns false if a
// row's array fails to re-parse (defensive only — survey_array_row already validated
// every row once) or its content no longer matches child_type (can't happen barring a
// concurrent-mutation bug, but this must never silently mis-type a value).
static bool fill_numeric_array_column(
    const uint8_t* base, StringColumnResult& scr, uint32_t n, DrakenType child_type,
    size_t total_elements, ParsedColumn& pc)
{
    int32_t* offsets = static_cast<int32_t*>(draken_malloc(static_cast<size_t>(n + 1) * sizeof(int32_t)));
    const bool is_bool = (child_type == DRAKEN_BOOL);
    const size_t elem_width = is_bool ? 1 : (child_type == DRAKEN_FLOAT64 ? sizeof(double) : sizeof(int64_t));
    uint8_t* child_data = nullptr;
    if (total_elements > 0) {
        if (is_bool) {
            const size_t alloc = ((total_elements + 7) >> 3);
            child_data = static_cast<uint8_t*>(draken_malloc(alloc ? alloc : 1));
            std::memset(child_data, 0, alloc ? alloc : 1);
        } else {
            child_data = static_cast<uint8_t*>(draken_malloc(total_elements * elem_width));
        }
    }
    const size_t child_bm_bytes = (total_elements + 7) >> 3;
    uint8_t* child_validity = static_cast<uint8_t*>(draken_malloc(child_bm_bytes ? child_bm_bytes : 1));
    std::memset(child_validity, 0xFF, child_bm_bytes ? child_bm_bytes : 1);
    bool any_child_null = false;

    offsets[0] = 0;
    size_t cursor = 0;
    for (uint32_t i = 0; i < n; ++i) {
        offsets[i + 1] = offsets[i];
        if (!row_valid(scr, i)) continue;  // absent/null row: zero-length slice
        const uint32_t off = scr.offsets[i], len = scr.lengths[i];
        yyjson_doc* doc = yyjson_read(reinterpret_cast<const char*>(base + off), len, 0);
        if (doc == nullptr) { draken_free(offsets); draken_free(child_data); draken_free(child_validity); return false; }
        yyjson_val* root = yyjson_doc_get_root(doc);
        yyjson_val* val;
        yyjson_arr_iter iter = yyjson_arr_iter_with(root);
        while ((val = yyjson_arr_iter_next(&iter)) != nullptr) {
            const size_t e = cursor++;
            if (yyjson_is_null(val)) {
                any_child_null = true;
                child_validity[e >> 3] &= static_cast<uint8_t>(~(1u << (e & 7)));
            } else if (is_bool) {
                if (yyjson_get_bool(val)) child_data[e >> 3] |= static_cast<uint8_t>(1u << (e & 7));
            } else if (child_type == DRAKEN_FLOAT64) {
                double d = yyjson_is_real(val) ? yyjson_get_real(val) : static_cast<double>(yyjson_get_sint(val));
                reinterpret_cast<double*>(child_data)[e] = d;
            } else {  // DRAKEN_INT64
                reinterpret_cast<int64_t*>(child_data)[e] = yyjson_get_sint(val);
            }
            offsets[i + 1] += 1;
        }
        yyjson_doc_free(doc);
    }

    pc.array_parent_offsets = offsets;
    pc.array_child_type = child_type;
    pc.array_child_length = static_cast<uint32_t>(total_elements);
    pc.array_child_data = child_data;
    if (any_child_null) {
        pc.array_child_validity = child_validity;
    } else {
        draken_free(child_validity);
        pc.array_child_validity = nullptr;
    }
    return true;
}

// String-family child (array of strings). Two passes: size the arena for long-form
// slots (mirrors parse_varchar_column), then populate slots + arena.
static bool fill_string_array_column(
    const uint8_t* base, StringColumnResult& scr, uint32_t n, size_t total_elements, ParsedColumn& pc)
{
    int32_t* offsets = static_cast<int32_t*>(draken_malloc(static_cast<size_t>(n + 1) * sizeof(int32_t)));
    const size_t child_bm_bytes = (total_elements + 7) >> 3;
    uint8_t* child_validity = static_cast<uint8_t*>(draken_malloc(child_bm_bytes ? child_bm_bytes : 1));
    std::memset(child_validity, 0xFF, child_bm_bytes ? child_bm_bytes : 1);
    bool any_child_null = false;

    // Pass 1: walk every element, sizing the arena for long-form strings (re-walked
    // via yyjson in pass 2 rather than staged in memory — keeps this a plain two-pass
    // count-then-fill, mirroring parse_varchar_column).
    size_t arena_size = 0;
    offsets[0] = 0;
    for (uint32_t i = 0; i < n; ++i) {
        offsets[i + 1] = offsets[i];
        if (!row_valid(scr, i)) continue;
        const uint32_t off = scr.offsets[i], len = scr.lengths[i];
        yyjson_doc* doc = yyjson_read(reinterpret_cast<const char*>(base + off), len, 0);
        if (doc == nullptr) { draken_free(offsets); draken_free(child_validity); return false; }
        yyjson_val* root = yyjson_doc_get_root(doc);
        yyjson_val* val;
        yyjson_arr_iter iter = yyjson_arr_iter_with(root);
        while ((val = yyjson_arr_iter_next(&iter)) != nullptr) {
            offsets[i + 1] += 1;
            if (!yyjson_is_null(val) && yyjson_get_len(val) > STR_INLINE_MAX)
                arena_size += yyjson_get_len(val);
        }
        yyjson_doc_free(doc);
    }

    DrakenStringSlot* slots = static_cast<DrakenStringSlot*>(
        draken_malloc(std::max<size_t>(total_elements, 1) * sizeof(DrakenStringSlot)));
    uint8_t* arena = arena_size ? static_cast<uint8_t*>(draken_malloc(arena_size)) : nullptr;

    // Pass 2: populate slots (+ arena for long strings).
    size_t cursor = 0;
    uint32_t arena_offset = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!row_valid(scr, i)) continue;
        const uint32_t off = scr.offsets[i], len = scr.lengths[i];
        yyjson_doc* doc = yyjson_read(reinterpret_cast<const char*>(base + off), len, 0);
        if (doc == nullptr) { draken_free(offsets); draken_free(child_validity); draken_free(slots); draken_free(arena); return false; }
        yyjson_val* root = yyjson_doc_get_root(doc);
        yyjson_val* val;
        yyjson_arr_iter iter = yyjson_arr_iter_with(root);
        while ((val = yyjson_arr_iter_next(&iter)) != nullptr) {
            const size_t e = cursor++;
            if (yyjson_is_null(val)) {
                any_child_null = true;
                child_validity[e >> 3] &= static_cast<uint8_t>(~(1u << (e & 7)));
                str_init_null(&slots[e]);
                continue;
            }
            const char* sbytes = yyjson_get_str(val);
            const size_t slen = yyjson_get_len(val);
            const uint8_t* bytes = slen ? reinterpret_cast<const uint8_t*>(sbytes)
                                        : reinterpret_cast<const uint8_t*>("");
            if (slen > STR_INLINE_MAX) {
                std::memcpy(arena + arena_offset, bytes, slen);
                draken_build_string_slot(&slots[e], bytes, static_cast<uint32_t>(slen), arena_offset);
                arena_offset += static_cast<uint32_t>(slen);
            } else {
                draken_build_string_slot(&slots[e], bytes, static_cast<uint32_t>(slen), 0);
            }
        }
        yyjson_doc_free(doc);
    }

    pc.array_parent_offsets = offsets;
    pc.array_child_type = DRAKEN_VARCHAR;
    pc.array_child_length = static_cast<uint32_t>(total_elements);
    pc.array_child_slots = slots;
    pc.array_child_arena = arena;
    pc.array_child_arena_len = arena_size;
    if (any_child_null) {
        pc.array_child_validity = child_validity;
    } else {
        draken_free(child_validity);
        pc.array_child_validity = nullptr;
    }
    return true;
}

// Parse a column whose sampled type is a JSON array into a DRAKEN_ARRAY ParsedColumn.
// v1 scope: every element across every row must be a uniform scalar kind (all-bool,
// all-numeric [int widens to float], all-string, or all-null/empty) — nested containers
// or a genuine mix of kinds fall back to raw JSON text (parse_varchar_column), same as
// parse_arrays=False, with ParsedColumn.array_fallback set so the Cython edge can warn
// (this function runs off the GIL and must not touch Python itself).
static ParsedColumn parse_array_column(const uint8_t* base, StringColumnResult& scr) {
    const uint32_t n = static_cast<uint32_t>(scr.num_rows);

    ArrayElementSurvey survey;
    size_t total_elements = 0;
    bool parse_ok = true;
    for (uint32_t i = 0; i < n && parse_ok && !survey.saw_nested; ++i) {
        if (!row_valid(scr, i)) continue;
        parse_ok = survey_array_row(base + scr.offsets[i], scr.lengths[i], survey, total_elements);
    }

    const int kinds = (survey.saw_bool ? 1 : 0) + ((survey.saw_int || survey.saw_real) ? 1 : 0) +
                       (survey.saw_string ? 1 : 0);
    const bool out_of_scope = !parse_ok || survey.saw_nested || kinds > 1;

    if (out_of_scope) {
        ParsedColumn pc = parse_varchar_column(base, scr);
        pc.array_fallback = true;
        return pc;
    }

    DrakenType child_type = DRAKEN_VARCHAR;  // default when no scalar kind was ever seen
    bool as_string = true;
    if (survey.saw_bool) { child_type = DRAKEN_BOOL; as_string = false; }
    else if (survey.saw_int || survey.saw_real) { child_type = survey.saw_real ? DRAKEN_FLOAT64 : DRAKEN_INT64; as_string = false; }
    else if (survey.saw_string) { child_type = DRAKEN_VARCHAR; as_string = true; }

    ParsedColumn pc;
    pc.type = DRAKEN_ARRAY;
    pc.length = n;
    pc.validity = own_validity_from_scr(scr, n);
    pc.all_null = !scr.any_value_seen;

    const bool ok = as_string
        ? fill_string_array_column(base, scr, n, total_elements, pc)
        : fill_numeric_array_column(base, scr, n, child_type, total_elements, pc);
    if (!ok) {
        // Defensive-only path (see fill_*'s docs) — the survey already validated every
        // row once, so a re-parse failure here would indicate a real bug, not bad data.
        // Fail loud rather than silently emitting a wrong/empty array column.
        throw std::runtime_error("parse_array_column: array element re-parse failed after survey succeeded");
    }
    return pc;
}

}  // namespace

// ---------------------------------------------------------------------------
// parse_typed_column — extracted column → owned typed buffers, with fallback.
// No Python — safe off the GIL. Wrapped into a Vector by wrap_column().
// ---------------------------------------------------------------------------
static ParsedColumn parse_typed_column(const uint8_t* base, StringColumnResult& scr, const ParseContext& context) {
    const uint32_t n = static_cast<uint32_t>(scr.num_rows);

    // String / all-null columns: nothing to parse.
    if (scr.inferred_type == ColumnType::String ||
        scr.inferred_type == ColumnType::Null || n == 0) {
        return parse_varchar_column(base, scr);
    }

    // Object columns (parse_objects): VARIANT is physically identical to VARCHAR
    // (German-string storage holding raw JSON text) — only the type tag differs, so
    // this reuses parse_varchar_column verbatim rather than duplicating it.
    if (scr.inferred_type == ColumnType::Variant) {
        if (!context.parse_objects) return parse_varchar_column(base, scr);
        ParsedColumn pc = parse_varchar_column(base, scr);
        pc.type = DRAKEN_VARIANT;
        return pc;
    }

    // Array columns (parse_arrays): real structural materialization, scoped to
    // uniform-scalar-element arrays (see parse_array_column).
    if (scr.inferred_type == ColumnType::Array) {
        if (!context.parse_arrays) return parse_varchar_column(base, scr);
        return parse_array_column(base, scr);
    }

    if (scr.inferred_type == ColumnType::Bool) {
        const uint32_t bm = (n + 7u) >> 3;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t alloc = padded ? padded : 8u;
        uint8_t* data = static_cast<uint8_t*>(draken_malloc(alloc));  // bit-packed
        std::memset(data, 0, alloc);
        bool ok = true;
        for (uint32_t i = 0; i < n; ++i) {
            if (!row_valid(scr, i)) continue;  // bit stays 0
            const uint32_t off = scr.offsets[i], len = scr.lengths[i];
            bool b;
            if (len == 0 || !parse_bool(base, off, off + len - 1, b)) { ok = false; break; }
            if (b) data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
        if (ok) {
            ParsedColumn pc; pc.type = DRAKEN_BOOL; pc.length = n;
            pc.data = data; pc.validity = own_validity_from_scr(scr, n);
            return pc;
        }
        draken_free(data);
        return parse_varchar_column(base, scr);
    }

    // Numeric: speculate int64, widen to float64, else fall back to VARCHAR.
    {
        int64_t* data = static_cast<int64_t*>(draken_malloc(static_cast<size_t>(n) * sizeof(int64_t)));
        if (try_fill_int64(base, scr, n, data)) {
            ParsedColumn pc; pc.type = DRAKEN_INT64; pc.length = n;
            pc.data = data; pc.validity = own_validity_from_scr(scr, n);
            return pc;
        }
        draken_free(data);
    }
    {
        double* data = static_cast<double*>(draken_malloc(static_cast<size_t>(n) * sizeof(double)));
        if (try_fill_float64(base, scr, n, data)) {
            ParsedColumn pc; pc.type = DRAKEN_FLOAT64; pc.length = n;
            pc.data = data; pc.validity = own_validity_from_scr(scr, n);
            return pc;
        }
        draken_free(data);
    }
    return parse_varchar_column(base, scr);
}

PyObject* build_typed_vector(const uint8_t* base, StringColumnResult& scr) {
    ParsedColumn pc = parse_typed_column(base, scr, ParseContext());
    return wrap_column(pc);
}

// Wrap parsed buffers into an owned Draken Vector (creates a Python object — GIL).
PyObject* wrap_column(ParsedColumn& pc) {
    if (pc.type == DRAKEN_ARRAY) {
        // String-family child iff fill_string_array_column populated slots;
        // fill_numeric_array_column never touches array_child_slots.
        if (pc.array_child_slots != nullptr) {
            return draken_vector_own_array(
                pc.array_parent_offsets, pc.array_child_slots, pc.array_child_arena,
                pc.array_child_arena_len, pc.array_child_length, pc.array_child_type,
                pc.array_child_validity, pc.validity, pc.length);
        }
        return draken_vector_own_array_numeric(
            pc.array_parent_offsets, pc.array_child_data, pc.array_child_validity,
            pc.array_child_length, pc.array_child_type, pc.validity, pc.length);
    }
    if (pc.is_string)
        return draken_vector_own_string(pc.slots, pc.arena, pc.arena_len,
                                        pc.validity, pc.length, pc.type,
                                        /*keyhash=*/nullptr);   // E37: jsonl producer = task #5
    return draken_vector_own_raw(pc.data, pc.validity, pc.length, pc.type);
}

// Parse a column STRICTLY as its explicit_schema-declared type: every non-null value must
// parse as that type or this throws std::invalid_argument (a declared-schema mismatch is a
// real data/schema error — unlike the speculative path, it must never silently fall back to
// VARCHAR). "string" always succeeds (any JSON scalar's raw bytes are valid as a string).
static ParsedColumn parse_column_explicit(
    const uint8_t* buffer, const RecordSet& records, const std::string& name,
    const std::string& declared, bool may_have_escapes) {

    OrdinalPredictor pred;
    StringColumnResult scr = extract_column(buffer, records, name, pred,
                                            /*copy_bytes=*/false, may_have_escapes);
    const uint8_t* base = scr.data_owned ? scr.data_ptr() : buffer;
    const uint32_t n = static_cast<uint32_t>(scr.num_rows);

    if (declared == "string") {
        return parse_varchar_column(base, scr);
    }
    if (declared == "int64") {
        int64_t* data = static_cast<int64_t*>(draken_malloc(std::max<size_t>(n, 1) * sizeof(int64_t)));
        for (uint32_t i = 0; i < n; ++i) {
            if (!row_valid(scr, i)) { data[i] = 0; continue; }
            const uint32_t off = scr.offsets[i], len = scr.lengths[i];
            if (len == 0 || !fast_parse_int64(base, off, off + len - 1, data[i])) {
                draken_free(data);
                throw std::invalid_argument(
                    "explicit_schema: column '" + name + "' row " + std::to_string(i) +
                    " is not a valid int64 (declared type mismatch)");
            }
        }
        ParsedColumn pc; pc.type = DRAKEN_INT64; pc.length = n;
        pc.data = data; pc.validity = own_validity_from_scr(scr, n);
        return pc;
    }
    if (declared == "double") {
        double* data = static_cast<double*>(draken_malloc(std::max<size_t>(n, 1) * sizeof(double)));
        for (uint32_t i = 0; i < n; ++i) {
            if (!row_valid(scr, i)) { data[i] = 0.0; continue; }
            const uint32_t off = scr.offsets[i], len = scr.lengths[i];
            if (len == 0 || !fast_parse_float64(base, off, off + len - 1, data[i])) {
                draken_free(data);
                throw std::invalid_argument(
                    "explicit_schema: column '" + name + "' row " + std::to_string(i) +
                    " is not a valid double (declared type mismatch)");
            }
        }
        ParsedColumn pc; pc.type = DRAKEN_FLOAT64; pc.length = n;
        pc.data = data; pc.validity = own_validity_from_scr(scr, n);
        return pc;
    }
    if (declared == "boolean") {
        const uint32_t bm = (n + 7u) >> 3;
        const uint32_t padded = ((bm + 7u) & ~7u);
        const size_t alloc = padded ? padded : 8u;
        uint8_t* data = static_cast<uint8_t*>(draken_malloc(alloc));
        std::memset(data, 0, alloc);
        for (uint32_t i = 0; i < n; ++i) {
            if (!row_valid(scr, i)) continue;
            const uint32_t off = scr.offsets[i], len = scr.lengths[i];
            bool b;
            if (len == 0 || !parse_bool(base, off, off + len - 1, b)) {
                draken_free(data);
                throw std::invalid_argument(
                    "explicit_schema: column '" + name + "' row " + std::to_string(i) +
                    " is not a valid boolean (declared type mismatch)");
            }
            if (b) data[i >> 3] |= static_cast<uint8_t>(1u << (i & 7));
        }
        ParsedColumn pc; pc.type = DRAKEN_BOOL; pc.length = n;
        pc.data = data; pc.validity = own_validity_from_scr(scr, n);
        return pc;
    }
    // Cython validates the declared type string eagerly before this ever runs; this is a
    // defensive backstop, not a user-facing path.
    throw std::invalid_argument(
        "explicit_schema: unsupported type '" + declared + "' for column '" + name + "'");
}

// Parse all named columns in parallel — one task per column. Pure C++, no Python;
// the caller wraps each ParsedColumn under the GIL.
std::vector<ParsedColumn> parse_all_columns(
    const uint8_t*                             buffer,
    const RecordSet&                          records,
    const std::vector<std::string>&            column_names,
    size_t                                     max_threads,
    bool                                       may_have_escapes,
    const ParseContext&                        context) {

    const size_t ncols = column_names.size();
    std::vector<ParsedColumn> out(ncols);
    if (ncols == 0) return out;

    size_t hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 1;
    size_t nt = std::min(std::min(hw, max_threads ? max_threads : hw), ncols);

    auto do_one = [&](size_t c) {
        const auto it = context.explicit_schema.find(column_names[c]);
        if (it != context.explicit_schema.end()) {
            out[c] = parse_column_explicit(buffer, records, column_names[c], it->second, may_have_escapes);
            return;
        }
        OrdinalPredictor pred;  // thread-local; per-column, no sharing
        StringColumnResult scr = extract_column(buffer, records, column_names[c], pred,
                                                /*copy_bytes=*/false, may_have_escapes,
                                                context.infer_sample_size);
        // Unescaped (or copied) columns own their bytes in scr.data; zero-copy columns
        // reference the original buffer.
        const uint8_t* base = scr.data_owned ? scr.data_ptr() : buffer;
        out[c] = parse_typed_column(base, scr, context);
    };

    if (nt <= 1) {
        for (size_t c = 0; c < ncols; ++c) do_one(c);
        return out;
    }

    BS::thread_pool<> pool(nt);
    std::vector<std::future<void>> futs;
    futs.reserve(ncols);
    for (size_t c = 0; c < ncols; ++c)
        futs.push_back(pool.submit_task([&, c]() { do_one(c); }));
    for (auto& f : futs) f.get();
    return out;
}

}  // namespace rugo::_jsonl
