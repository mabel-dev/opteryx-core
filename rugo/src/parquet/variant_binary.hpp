// Parquet Variant binary encoding (parquet-format VariantEncoding.md, version 1),
// decoded to JSON text.
//
// A Variant value is two byte strings: `metadata` (a dictionary of object field
// names) and `value` (one self-describing value, nested values inline). This
// renders a value as JSON text with the SAME rules nested_json.hpp uses for
// STRUCT / MAP columns, so a Variant and a STRUCT holding the same data render
// alike:
//   * date / time / timestamp = quoted ISO-8601   * binary = quoted base64
//   * decimal = a bare number                     * uuid   = quoted 8-4-4-4-12
//   * NaN / +-Inf = `null`                        * object members in the value's
//                                                   own field order (by name)
//
// Every read is bounds-checked against the value's container: a malformed value
// is an error naming what was wrong, never a guess and never a NULL.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

#include "base64/_base64.h"           // mabel bintob64 / b64_encoded_size
#include "interop/value_format.hpp"   // fmt_*, json_string
#include "ops/float_ops.h"            // fp_canon: the same -0.0 / NaN canon as the scan

namespace rugo {
namespace variant {

[[noreturn]] inline void bad(const std::string& what) {
    throw std::runtime_error("malformed Variant: " + what);
}

// Unsigned little-endian integer of 1..4 bytes.
inline uint32_t rd(const uint8_t* p, uint32_t width) {
    uint32_t v = 0;
    std::memcpy(&v, p, width);
    return v;
}

struct Metadata {
    const uint8_t* offsets = nullptr;   // dictionary_size + 1 offsets, `width` bytes each
    const uint8_t* bytes = nullptr;     // the dictionary strings, back to back
    uint32_t n = 0;                     // dictionary_size
    uint32_t bytes_len = 0;
    uint32_t width = 1;

    void key(uint32_t id, const char*& s, uint32_t& len) const {
        if (id >= n) bad("field id " + std::to_string(id) + " is outside the " + std::to_string(n) + "-entry dictionary");
        const uint32_t a = rd(offsets + static_cast<size_t>(id) * width, width);
        const uint32_t b = rd(offsets + static_cast<size_t>(id + 1) * width, width);
        if (a > b || b > bytes_len) bad("dictionary offsets of field id " + std::to_string(id) + " are out of range");
        s = reinterpret_cast<const char*>(bytes) + a;
        len = b - a;
    }
};

inline Metadata parse_metadata(const uint8_t* p, size_t len) {
    if (len < 1) bad("empty metadata");
    const uint32_t version = p[0] & 0x0F;
    if (version != 1) bad("metadata version " + std::to_string(version) + " (only version 1 is read)");
    Metadata m;
    m.width = ((p[0] >> 6) & 3u) + 1;
    if (len < 1 + static_cast<size_t>(m.width)) bad("metadata is shorter than its header");
    m.n = rd(p + 1, m.width);
    const size_t table = (static_cast<size_t>(m.n) + 1) * m.width;
    const size_t head = 1 + static_cast<size_t>(m.width);
    if (len - head < table) bad("metadata is shorter than its offset table");
    m.offsets = p + head;
    m.bytes = m.offsets + table;
    m.bytes_len = static_cast<uint32_t>(len - head - table);
    return m;
}

// A value that is the Variant null (basic type 0, primitive type 0).
inline bool is_null(const uint8_t* v, size_t len) { return len >= 1 && v[0] == 0; }

constexpr int kMaxDepth = 1024;

inline void render(std::string& o, const Metadata& md, const uint8_t* v, const uint8_t* end, int depth);

namespace detail {

template <typename T>
inline T fixed(const uint8_t* v, const uint8_t* end, const char* what) {
    if (static_cast<size_t>(end - v) < 1 + sizeof(T)) bad(std::string("truncated ") + what);
    T x;
    std::memcpy(&x, v + 1, sizeof(T));
    return x;
}

template <typename T>
inline void decimal(std::string& o, const uint8_t* v, const uint8_t* end) {
    if (static_cast<size_t>(end - v) < 2 + sizeof(T)) bad("truncated decimal");
    const uint32_t scale = v[1];
    if (scale > 38) bad("decimal scale " + std::to_string(scale) + " exceeds 38");
    T x;
    std::memcpy(&x, v + 2, sizeof(T));
    rugo_text::fmt_decimal(o, static_cast<__int128>(x), static_cast<int>(scale));
}

// A 4-byte-length-prefixed run of bytes (the long string and binary primitives).
inline void sized(const uint8_t* v, const uint8_t* end, const uint8_t*& p, uint32_t& len, const char* what) {
    if (end - v < 5) bad(std::string("truncated ") + what);
    len = rd(v + 1, 4);
    if (static_cast<size_t>(end - v) - 5 < len) bad(std::string(what) + " runs past its container");
    p = v + 5;
}

inline void primitive(std::string& o, const uint8_t* v, const uint8_t* end) {
    const uint32_t id = v[0] >> 2;
    switch (id) {
    case 0: o.append("null"); return;
    case 1: o.append("true"); return;
    case 2: o.append("false"); return;
    case 3: rugo_text::fmt_int64(o, fixed<int8_t>(v, end, "int8")); return;
    case 4: rugo_text::fmt_int64(o, fixed<int16_t>(v, end, "int16")); return;
    case 5: rugo_text::fmt_int64(o, fixed<int32_t>(v, end, "int32")); return;
    case 6: rugo_text::fmt_int64(o, fixed<int64_t>(v, end, "int64")); return;
    case 7: {
        const double d = draken::ops::fp_canon(fixed<double>(v, end, "double"));
        if (rugo_text::double_is_nan_or_inf(d)) o.append("null");
        else rugo_text::fmt_double(o, d);
        return;
    }
    case 8: decimal<int32_t>(o, v, end); return;
    case 9: decimal<int64_t>(o, v, end); return;
    case 10: decimal<__int128>(o, v, end); return;
    case 11: rugo_text::fmt_date_quoted(o, fixed<int32_t>(v, end, "date")); return;
    case 12:
    case 13: rugo_text::fmt_timestamp_quoted(o, fixed<int64_t>(v, end, "timestamp"), rugo_text::U_US); return;
    case 14: {
        const float f = draken::ops::fp_canon(fixed<float>(v, end, "float"));
        if (rugo_text::double_is_nan_or_inf(static_cast<double>(f))) o.append("null");
        else rugo_text::fmt_float(o, f);
        return;
    }
    case 15: {
        const uint8_t* p; uint32_t len;
        sized(v, end, p, len, "binary");
        o.push_back('"');
        const size_t at = o.size();
        o.resize(at + b64_encoded_size(len));            // includes bintob64's trailing NUL
        bintob64(&o[at], p, len);
        o.resize(at + b64_encoded_size(len) - 1);
        o.push_back('"');
        return;
    }
    case 16: {
        const uint8_t* p; uint32_t len;
        sized(v, end, p, len, "string");
        rugo_text::json_string(o, reinterpret_cast<const char*>(p), len);
        return;
    }
    case 17: rugo_text::fmt_time_quoted(o, fixed<int64_t>(v, end, "time"), rugo_text::U_US); return;
    case 18:
    case 19: rugo_text::fmt_timestamp_quoted(o, fixed<int64_t>(v, end, "timestamp"), rugo_text::U_NS); return;
    case 20:
        if (end - v < 17) bad("truncated uuid");
        rugo_text::fmt_uuid_quoted(o, v + 1);
        return;
    default:
        bad("unknown primitive type id " + std::to_string(id));
    }
}

// The header of an object (`ids` non-null) or array: where its tables and values are.
struct Container {
    const uint8_t* ids = nullptr;
    const uint8_t* offs = nullptr;
    const uint8_t* vals = nullptr;
    const uint8_t* vals_end = nullptr;
    uint32_t n = 0;
    uint32_t id_w = 0;
    uint32_t off_w = 0;

    const uint8_t* value(uint32_t i) const {
        const uint32_t off = rd(offs + static_cast<size_t>(i) * off_w, off_w);
        if (off >= static_cast<size_t>(vals_end - vals)) bad("a field offset points past its container");
        return vals + off;
    }
};

inline Container container(const uint8_t* v, const uint8_t* end, bool object) {
    const uint32_t hdr = v[0] >> 2;
    Container c;
    c.off_w = (hdr & 3u) + 1;
    bool large;
    if (object) {
        c.id_w = ((hdr >> 2) & 3u) + 1;
        large = (hdr >> 4) & 1u;
    } else {
        large = (hdr >> 2) & 1u;
    }
    const uint8_t* p = v + 1;
    const uint32_t count_w = large ? 4 : 1;
    if (static_cast<size_t>(end - p) < count_w) bad("truncated element count");
    c.n = rd(p, count_w);
    p += count_w;
    const size_t tables = static_cast<size_t>(c.n) * c.id_w + (static_cast<size_t>(c.n) + 1) * c.off_w;
    if (static_cast<size_t>(end - p) < tables) bad("an object or array is shorter than its offset tables");
    if (object) c.ids = p;
    c.offs = p + static_cast<size_t>(c.n) * c.id_w;
    c.vals = p + tables;
    const uint32_t total = rd(c.offs + static_cast<size_t>(c.n) * c.off_w, c.off_w);
    if (static_cast<size_t>(end - c.vals) < total) bad("an object or array runs past its container");
    c.vals_end = c.vals + total;
    return c;
}

inline void members(std::string& o, const Metadata& md, const Container& c, bool& first, int depth) {
    for (uint32_t i = 0; i < c.n; ++i) {
        const char* k; uint32_t klen;
        md.key(rd(c.ids + static_cast<size_t>(i) * c.id_w, c.id_w), k, klen);
        if (!first) o.push_back(',');
        first = false;
        rugo_text::json_string(o, k, klen);
        o.push_back(':');
        render(o, md, c.value(i), c.vals_end, depth + 1);
    }
}

}  // namespace detail

// Render the value starting at `v`; `end` bounds the bytes it may occupy.
inline void render(std::string& o, const Metadata& md, const uint8_t* v, const uint8_t* end, int depth) {
    if (v >= end) bad("empty value");
    if (depth > kMaxDepth) bad("nested deeper than " + std::to_string(kMaxDepth));
    switch (v[0] & 3u) {
    case 0:
        detail::primitive(o, v, end);
        return;
    case 1: {
        const uint32_t len = v[0] >> 2;
        if (static_cast<size_t>(end - v) - 1 < len) bad("a short string runs past its container");
        rugo_text::json_string(o, reinterpret_cast<const char*>(v + 1), len);
        return;
    }
    case 2: {
        const detail::Container c = detail::container(v, end, true);
        bool first = true;
        o.push_back('{');
        detail::members(o, md, c, first, depth);
        o.push_back('}');
        return;
    }
    default: {
        const detail::Container c = detail::container(v, end, false);
        o.push_back('[');
        for (uint32_t i = 0; i < c.n; ++i) {
            if (i) o.push_back(',');
            render(o, md, c.value(i), c.vals_end, depth + 1);
        }
        o.push_back(']');
        return;
    }
    }
}

inline void render(std::string& o, const Metadata& md, const uint8_t* v, size_t len) {
    render(o, md, v, v + len, 0);
}

// The members of the OBJECT at `v`, appended as `"k":v` pairs to an object the
// caller has already opened (the residual of a partially shredded object).
// `first` says whether a member has been written yet, and is updated.
inline void object_members(std::string& o, const Metadata& md, const uint8_t* v, size_t len, bool& first) {
    if (len < 1 || (v[0] & 3u) != 2) bad("the residual value of a partially shredded object is not an object");
    detail::members(o, md, detail::container(v, v + len, true), first, 0);
}

// The value of field `key` of the OBJECT at [v, end), or nullptr when the value is not an
// object or has no such field. On a hit `end` becomes the bound of the field's value.
inline const uint8_t* find_field(const Metadata& md, const uint8_t* v, const uint8_t*& end,
                                 const char* key, size_t klen) {
    if (v >= end) bad("empty value");
    if ((v[0] & 3u) != 2) return nullptr;
    const detail::Container c = detail::container(v, end, true);
    // Linear: the spec lists field ids in name order, but DuckDB's objects are not, so a
    // binary search over a file we did not write cannot be trusted.
    for (uint32_t i = 0; i < c.n; ++i) {
        const char* k; uint32_t n;
        md.key(rd(c.ids + static_cast<size_t>(i) * c.id_w, c.id_w), k, n);
        if (n == klen && std::memcmp(k, key, klen) == 0) {
            end = c.vals_end;
            return c.value(i);
        }
    }
    return nullptr;
}

// The bytes of the STRING at [v, end) (short or long form); false when it is not a string.
inline bool string_bytes(const uint8_t* v, const uint8_t* end, const uint8_t*& p, uint32_t& len) {
    if (v >= end) bad("empty value");
    if ((v[0] & 3u) == 1) {
        len = v[0] >> 2;
        if (static_cast<size_t>(end - v) - 1 < len) bad("a short string runs past its container");
        p = v + 1;
        return true;
    }
    if (v[0] != (16u << 2)) return false;
    detail::sized(v, end, p, len, "string");
    return true;
}

}  // namespace variant
}  // namespace rugo
