#include <Python.h>  // must precede any draken header that uses PyObject

#include "column_builder.hpp"
#include "fast_parsers.hpp"

#include <algorithm>
#include <cstring>
#include <thread>
#include <future>

// Producer surface (definitions resolved at load via RTLD_GLOBAL from draken_native.so):
#include "draken_bridge.h"  // draken_vector_own_string
#include "string_slot.h"    // DrakenStringSlot, draken_build_string_slot, str_init_null
#include "alloc.h"          // draken_malloc
#include "buffers.h"        // DrakenType, DRAKEN_VARCHAR
#include "BS_thread_pool.hpp"

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
    bool                                       may_have_escapes)
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
                if (!inferred) {
                    uint8_t vt = found->type;
                    if (vt == static_cast<uint8_t>(ValueType::String))
                        result.inferred_type = ColumnType::String;
                    else if (vt == static_cast<uint8_t>(ValueType::Boolean))
                        result.inferred_type = ColumnType::Bool;
                    else if (vt == static_cast<uint8_t>(ValueType::Integer))
                        result.inferred_type = ColumnType::Int64;
                    else if (vt == static_cast<uint8_t>(ValueType::Double))
                        result.inferred_type = ColumnType::Float64;
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

// ---------------------------------------------------------------------------
// parse_typed_column — extracted column → owned typed buffers, with fallback.
// No Python — safe off the GIL. Wrapped into a Vector by wrap_column().
// ---------------------------------------------------------------------------
static ParsedColumn parse_typed_column(const uint8_t* base, StringColumnResult& scr) {
    const uint32_t n = static_cast<uint32_t>(scr.num_rows);

    // String / all-null columns: nothing to parse.
    if (scr.inferred_type == ColumnType::String ||
        scr.inferred_type == ColumnType::Null || n == 0) {
        return parse_varchar_column(base, scr);
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
    ParsedColumn pc = parse_typed_column(base, scr);
    return wrap_column(pc);
}

// Wrap parsed buffers into an owned Draken Vector (creates a Python object — GIL).
PyObject* wrap_column(ParsedColumn& pc) {
    if (pc.is_string)
        return draken_vector_own_string(pc.slots, pc.arena, pc.arena_len,
                                        pc.validity, pc.length, pc.type);
    return draken_vector_own_raw(pc.data, pc.validity, pc.length, pc.type);
}

// Parse all named columns in parallel — one task per column. Pure C++, no Python;
// the caller wraps each ParsedColumn under the GIL.
std::vector<ParsedColumn> parse_all_columns(
    const uint8_t*                             buffer,
    const RecordSet&                          records,
    const std::vector<std::string>&            column_names,
    size_t                                     max_threads,
    bool                                       may_have_escapes) {

    const size_t ncols = column_names.size();
    std::vector<ParsedColumn> out(ncols);
    if (ncols == 0) return out;

    size_t hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 1;
    size_t nt = std::min(std::min(hw, max_threads ? max_threads : hw), ncols);

    auto do_one = [&](size_t c) {
        OrdinalPredictor pred;  // thread-local; per-column, no sharing
        StringColumnResult scr = extract_column(buffer, records, column_names[c], pred,
                                                /*copy_bytes=*/false, may_have_escapes);
        // Unescaped (or copied) columns own their bytes in scr.data; zero-copy columns
        // reference the original buffer.
        const uint8_t* base = scr.data_owned ? scr.data_ptr() : buffer;
        out[c] = parse_typed_column(base, scr);
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
