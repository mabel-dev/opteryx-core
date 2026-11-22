// JSON text -> Parquet Variant, shredded (parquet-format VariantEncoding.md version 1,
// VariantShredding.md). The write side of variant_binary.hpp; docs/parquet_variant_design.md §5.
//
// A VARIANT column reaches the writer as one JSON document per row. This turns a row
// group of them into the LEAF columns of a Parquet VARIANT group:
//
//   metadata                 the row's dictionary of the field names its binary values use
//   value                    the value in Variant binary - or the part of it not shredded
//   typed_value...           the shredded part: a typed leaf, or an object whose fields are
//                            each again a {value, typed_value} pair
//
// Which paths are shredded is learnt ONCE per file, from the first row group (infer), and
// fixed from then on: a later row that does not fit goes to `value`, which is always right.
//
// Rules (architect rulings D3, D6, W6):
//   * a row is parsed exactly as `->` / `->>` parse it (draken's flags and parse arena), so
//     a row those operators read is a row this writes;
//   * an object's members are written in name order; of members sharing a name the FIRST is
//     kept (the one `->` returns) and the rest are dropped;
//   * an integer literal in int64 range is the narrowest int that holds it; any other
//     literal of at most 38 significant digits, no exponent, and not a negative zero is a
//     decimal at the literal's own scale - both render back as the text they were; every
//     other number is a double (`1e2` reads back `100.0`). A literal no double can hold
//     (`1e999`) is REFUSED: stored as a double it would read back as null;
//   * arrays are never shredded.
#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <deque>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "fast_float/fast_float.h"
#include "ops/json_extract.h"   // kJsonReadFlags, ReadPool, JDocGuard: draken's parse of a row

namespace rugo {
namespace variant {

// ── numbers ─────────────────────────────────────────────────────────────────

struct Number {
    enum Kind : uint8_t { Int, Decimal, Double } kind = Double;
    int64_t i = 0;            // Int
    __int128 unscaled = 0;    // Decimal
    int scale = 0;            // Decimal: digits after the point
    int digits = 0;           // Decimal: significant digits of `unscaled`
    double d = 0;             // Double
};

// What a JSON number literal (yyjson has already checked its spelling) is stored as.
inline Number classify_number(const char* s, size_t n) {
    Number r;
    const bool neg = n > 0 && s[0] == '-';
    unsigned __int128 mag = 0;
    int digits = 0, scale = 0;
    bool fraction = false, plain = true, nonzero = false;
    for (size_t i = neg ? 1 : 0; i < n; ++i) {
        const char c = s[i];
        if (c == '.') { fraction = true; continue; }
        if (c < '0' || c > '9') { plain = false; break; }       // an exponent
        if (digits > 0 || c != '0') ++digits;                   // leading zeros are not significant
        if (fraction) ++scale;
        if (digits > 38 || scale > 38) { plain = false; break; }
        mag = mag * 10 + static_cast<unsigned>(c - '0');
        nonzero |= c != '0';
    }
    if (plain && (nonzero || !neg)) {                            // -0 has no int or decimal form
        const unsigned __int128 int_limit = static_cast<unsigned __int128>(INT64_MAX) + (neg ? 1 : 0);
        if (!fraction && mag <= int_limit) {
            r.kind = Number::Int;
            r.i = neg ? static_cast<int64_t>(-static_cast<__int128>(mag)) : static_cast<int64_t>(mag);
            return r;
        }
        r.kind = Number::Decimal;
        r.unscaled = neg ? -static_cast<__int128>(mag) : static_cast<__int128>(mag);
        r.scale = scale;
        r.digits = digits;
        return r;
    }
    const auto parsed = fast_float::from_chars(s, s + n, r.d);
    if (parsed.ec != std::errc() || parsed.ptr != s + n || r.d - r.d != 0)
        throw std::runtime_error("the number " + std::string(s, n) +
                                 " cannot be held in a VARIANT: it is outside the range of a double");
    return r;
}

// ── binary writer ───────────────────────────────────────────────────────────

namespace enc {

inline void raw(std::string& o, const void* p, size_t n) { o.append(static_cast<const char*>(p), n); }

// An unsigned little-endian integer of `width` bytes.
inline void uint_le(std::string& o, uint32_t v, uint32_t width) { raw(o, &v, width); }

inline uint32_t width_of(uint32_t v) { return v <= 0xFF ? 1 : v <= 0xFFFF ? 2 : v <= 0xFFFFFF ? 3 : 4; }

inline void primitive(std::string& o, uint32_t type_id) { o.push_back(static_cast<char>(type_id << 2)); }

inline void integer(std::string& o, int64_t v) {
    if (v >= INT8_MIN && v <= INT8_MAX) { primitive(o, 3); const int8_t x = static_cast<int8_t>(v); raw(o, &x, 1); }
    else if (v >= INT16_MIN && v <= INT16_MAX) { primitive(o, 4); const int16_t x = static_cast<int16_t>(v); raw(o, &x, 2); }
    else if (v >= INT32_MIN && v <= INT32_MAX) { primitive(o, 5); const int32_t x = static_cast<int32_t>(v); raw(o, &x, 4); }
    else { primitive(o, 6); raw(o, &v, 8); }
}

inline void decimal(std::string& o, const Number& n) {
    const char scale = static_cast<char>(n.scale);
    if (n.digits <= 9) { primitive(o, 8); o.push_back(scale); const int32_t x = static_cast<int32_t>(n.unscaled); raw(o, &x, 4); }
    else if (n.digits <= 18) { primitive(o, 9); o.push_back(scale); const int64_t x = static_cast<int64_t>(n.unscaled); raw(o, &x, 8); }
    else { primitive(o, 10); o.push_back(scale); raw(o, &n.unscaled, 16); }
}

inline void number(std::string& o, const Number& n) {
    if (n.kind == Number::Int) integer(o, n.i);
    else if (n.kind == Number::Decimal) decimal(o, n);
    else { primitive(o, 7); raw(o, &n.d, 8); }
}

inline void string(std::string& o, const char* s, size_t len) {
    if (len > UINT32_MAX) throw std::runtime_error("a string of more than 4 GiB cannot be held in a VARIANT");
    if (len < 64) o.push_back(static_cast<char>((len << 2) | 1));          // short string
    else { primitive(o, 16); uint_le(o, static_cast<uint32_t>(len), 4); }
    raw(o, s, len);
}

}  // namespace enc

constexpr int kMaxEncodeDepth = 1024;   // variant_binary.hpp refuses to render deeper

// One row's values in Variant binary: its dictionary of field names, and the values that use it.
struct RowEncoder {
    struct Key { const char* s; size_t len; };
    struct Member { Key key; yyjson_val* val; };

    std::vector<Key> keys;                       // sorted, unique: the row's `metadata`
    std::deque<std::string> scratch;             // one buffer per nesting depth (stable references)
    std::deque<std::vector<Member>> members;     // likewise

    static bool key_less(const Key& a, const Key& b) {
        const int c = std::memcmp(a.s, b.s, std::min(a.len, b.len));
        return c != 0 ? c < 0 : a.len < b.len;
    }
    static bool key_equal(const Key& a, const Key& b) {
        return a.len == b.len && std::memcmp(a.s, b.s, a.len) == 0;
    }

    void begin_row() { keys.clear(); }

    // Every field name under `v` joins the row's dictionary.
    void collect(yyjson_val* v, int depth) {
        if (depth > kMaxEncodeDepth)
            throw std::runtime_error("a value nested deeper than " + std::to_string(kMaxEncodeDepth) +
                                     " cannot be held in a VARIANT");
        if (yyjson_is_obj(v)) {
            yyjson_obj_iter it = yyjson_obj_iter_with(v);
            while (yyjson_val* k = yyjson_obj_iter_next(&it)) {
                keys.push_back({yyjson_get_str(k), yyjson_get_len(k)});
                collect(yyjson_obj_iter_get_val(k), depth + 1);
            }
        } else if (yyjson_is_arr(v)) {
            yyjson_arr_iter it = yyjson_arr_iter_with(v);
            while (yyjson_val* e = yyjson_arr_iter_next(&it)) collect(e, depth + 1);
        }
    }

    void seal_keys() {
        std::sort(keys.begin(), keys.end(), key_less);
        keys.erase(std::unique(keys.begin(), keys.end(), key_equal), keys.end());
    }

    uint32_t id_of(const Key& k) const {
        return static_cast<uint32_t>(std::lower_bound(keys.begin(), keys.end(), k, key_less) - keys.begin());
    }

    // The row's `metadata`: version 1, sorted and unique strings.
    void metadata(std::string& o) const {
        size_t bytes = 0;
        for (const Key& k : keys) bytes += k.len;
        if (bytes > UINT32_MAX) throw std::runtime_error("a row's field names exceed 4 GiB");
        const uint32_t w = enc::width_of(std::max<uint32_t>(static_cast<uint32_t>(bytes),
                                                             static_cast<uint32_t>(keys.size())));
        o.push_back(static_cast<char>(1 | (1 << 4) | ((w - 1) << 6)));
        enc::uint_le(o, static_cast<uint32_t>(keys.size()), w);
        uint32_t at = 0;
        for (const Key& k : keys) { enc::uint_le(o, at, w); at += static_cast<uint32_t>(k.len); }
        enc::uint_le(o, at, w);
        for (const Key& k : keys) enc::raw(o, k.s, k.len);
    }

    // The members of object `v` in name order, the first of each name only.
    std::vector<Member>& sorted_members(yyjson_val* v, int depth) {
        while (members.size() <= static_cast<size_t>(depth)) members.emplace_back();
        std::vector<Member>& m = members[static_cast<size_t>(depth)];
        m.clear();
        yyjson_obj_iter it = yyjson_obj_iter_with(v);
        while (yyjson_val* k = yyjson_obj_iter_next(&it))
            m.push_back({{yyjson_get_str(k), yyjson_get_len(k)}, yyjson_obj_iter_get_val(k)});
        std::stable_sort(m.begin(), m.end(),
                         [](const Member& a, const Member& b) { return key_less(a.key, b.key); });
        m.erase(std::unique(m.begin(), m.end(),
                            [](const Member& a, const Member& b) { return key_equal(a.key, b.key); }),
                m.end());
        return m;
    }

    std::string& buffer(int depth) {
        while (scratch.size() <= static_cast<size_t>(depth)) scratch.emplace_back();
        scratch[static_cast<size_t>(depth)].clear();
        return scratch[static_cast<size_t>(depth)];
    }

    // An object of `m[i]` for the `n` listed indexes (already in name order).
    void object(std::string& o, const std::vector<Member>& m, const std::vector<uint32_t>& take, int depth) {
        std::string& body = buffer(depth);
        std::vector<uint32_t> offsets;
        uint32_t max_id = 0;
        for (uint32_t i : take) {
            offsets.push_back(static_cast<uint32_t>(body.size()));
            max_id = std::max(max_id, id_of(m[i].key));
            value(body, m[i].val, depth + 1);
            if (body.size() > UINT32_MAX) throw std::runtime_error("an object of more than 4 GiB cannot be held in a VARIANT");
        }
        offsets.push_back(static_cast<uint32_t>(body.size()));
        const uint32_t n = static_cast<uint32_t>(take.size());
        const uint32_t id_w = enc::width_of(max_id), off_w = enc::width_of(offsets.back());
        const bool large = n > 0xFF;
        o.push_back(static_cast<char>((((large ? 1u : 0u) << 4 | (id_w - 1) << 2 | (off_w - 1)) << 2) | 2));
        enc::uint_le(o, n, large ? 4 : 1);
        for (uint32_t i : take) enc::uint_le(o, id_of(m[i].key), id_w);
        for (uint32_t off : offsets) enc::uint_le(o, off, off_w);
        o.append(body);
    }

    // Append the encoding of `v`.
    void value(std::string& o, yyjson_val* v, int depth) {
        if (depth > kMaxEncodeDepth)
            throw std::runtime_error("a value nested deeper than " + std::to_string(kMaxEncodeDepth) +
                                     " cannot be held in a VARIANT");
        switch (yyjson_get_type(v)) {
        case YYJSON_TYPE_NULL: enc::primitive(o, 0); return;
        case YYJSON_TYPE_BOOL: enc::primitive(o, yyjson_is_true(v) ? 1 : 2); return;
        case YYJSON_TYPE_RAW: enc::number(o, classify_number(yyjson_get_raw(v), yyjson_get_len(v))); return;
        case YYJSON_TYPE_STR: enc::string(o, yyjson_get_str(v), yyjson_get_len(v)); return;
        case YYJSON_TYPE_OBJ: {
            const std::vector<Member>& m = sorted_members(v, depth);
            std::vector<uint32_t> all(m.size());
            for (uint32_t i = 0; i < all.size(); ++i) all[i] = i;
            object(o, m, all, depth);
            return;
        }
        case YYJSON_TYPE_ARR: {
            std::string& body = buffer(depth);
            std::vector<uint32_t> offsets;
            yyjson_arr_iter it = yyjson_arr_iter_with(v);
            while (yyjson_val* e = yyjson_arr_iter_next(&it)) {
                offsets.push_back(static_cast<uint32_t>(body.size()));
                value(body, e, depth + 1);
                if (body.size() > UINT32_MAX) throw std::runtime_error("an array of more than 4 GiB cannot be held in a VARIANT");
            }
            offsets.push_back(static_cast<uint32_t>(body.size()));
            const uint32_t n = static_cast<uint32_t>(offsets.size() - 1);
            const uint32_t off_w = enc::width_of(offsets.back());
            const bool large = n > 0xFF;
            o.push_back(static_cast<char>((((large ? 1u : 0u) << 2 | (off_w - 1)) << 2) | 3));
            enc::uint_le(o, n, large ? 4 : 1);
            for (uint32_t off : offsets) enc::uint_le(o, off, off_w);
            o.append(body);
            return;
        }
        default:
            throw std::runtime_error("a JSON value of a kind a VARIANT cannot hold");
        }
    }
};

// ── shredding ───────────────────────────────────────────────────────────────

struct ShredField;

// What the `typed_value` of a {value, typed_value} pair is. None = the pair has no typed_value.
struct ShredType {
    enum Kind : uint8_t { None, String, Bool, Int64, Decimal, Object } kind = None;
    int scale = 0;                    // Decimal
    int precision = 0;                // Decimal: 18 or 38
    std::vector<ShredField> fields;   // Object: its shredded fields, in name order
};
struct ShredField {
    std::string name;
    ShredType type;
};

// The fixed limits of automatic shredding (D2: no setting).
constexpr uint32_t kShredPresence = 64;     // a field is shredded when present in >= 1 in 64 of its parent objects
constexpr int kShredDepth = 8;              // objects nested deeper than this are not looked into
constexpr int kShredFields = 128;           // shredded fields per column
constexpr size_t kShredTracked = 1024;      // distinct field names tracked per object while learning

// What the rows of the first row group hold at one path.
struct Seen {
    uint32_t n = 0;                             // times the path was present
    uint32_t strings = 0, bools = 0, ints = 0, objects = 0;
    uint32_t decimals[39] = {};                 // by scale
    uint8_t decimal_digits[39] = {};            // most significant digits seen, by scale
    std::unordered_map<std::string, Seen> fields;

    void observe(yyjson_val* v, int depth) {
        ++n;
        switch (yyjson_get_type(v)) {
        case YYJSON_TYPE_STR: ++strings; return;
        case YYJSON_TYPE_BOOL: ++bools; return;
        case YYJSON_TYPE_RAW: {
            const Number num = classify_number(yyjson_get_raw(v), yyjson_get_len(v));
            if (num.kind == Number::Int) ++ints;
            else if (num.kind == Number::Decimal) {
                ++decimals[num.scale];
                decimal_digits[num.scale] = std::max<uint8_t>(decimal_digits[num.scale], static_cast<uint8_t>(num.digits));
            }
            return;
        }
        case YYJSON_TYPE_OBJ: {
            ++objects;
            if (depth >= kShredDepth) return;
            yyjson_obj_iter it = yyjson_obj_iter_with(v);
            while (yyjson_val* k = yyjson_obj_iter_next(&it)) {
                const std::string name(yyjson_get_str(k), yyjson_get_len(k));
                auto at = fields.find(name);
                if (at == fields.end()) {
                    if (fields.size() >= kShredTracked) continue;
                    at = fields.emplace(name, Seen{}).first;
                }
                at->second.observe(yyjson_obj_iter_get_val(k), depth + 1);
            }
            return;
        }
        default: return;   // null, array: never typed
        }
    }

    // The shredding for this path; `budget` is the shredded fields the column may still take.
    ShredType decide(int& budget) const {
        ShredType t;
        uint32_t best = 0;
        auto offer = [&](uint32_t count, ShredType::Kind kind) { if (count > best) { best = count; t.kind = kind; } };
        offer(strings, ShredType::String);
        offer(ints, ShredType::Int64);
        offer(bools, ShredType::Bool);
        offer(objects, ShredType::Object);
        for (int s = 0; s <= 38; ++s) {
            if (decimals[s] > best) {
                best = decimals[s];
                t.kind = ShredType::Decimal;
                t.scale = s;
                t.precision = decimal_digits[s] <= 18 && s <= 18 ? 18 : 38;
            }
        }
        if (best == 0 || static_cast<uint64_t>(best) * 2 < n) return ShredType{};   // no dominant type
        if (t.kind != ShredType::Object) return t;
        // The most present fields first, while the budget lasts; the schema lists them by name.
        std::vector<const std::pair<const std::string, Seen>*> order;
        for (const auto& f : fields)
            if (static_cast<uint64_t>(f.second.n) * kShredPresence >= objects) order.push_back(&f);
        std::sort(order.begin(), order.end(), [](const auto* a, const auto* b) {
            return a->second.n != b->second.n ? a->second.n > b->second.n : a->first < b->first;
        });
        for (const auto* f : order) {
            if (budget <= 0) break;
            --budget;
            ShredType child = f->second.decide(budget);
            if (child.kind == ShredType::None) { ++budget; continue; }
            t.fields.push_back({f->first, std::move(child)});
        }
        if (t.fields.empty()) return ShredType{};   // an object with nothing shredded is just `value`
        std::sort(t.fields.begin(), t.fields.end(),
                  [](const ShredField& a, const ShredField& b) { return a.name < b.name; });
        return t;
    }
};

// One leaf column of the VARIANT group, for one row group. Every vector that is used holds
// ONE entry per row (a row where the leaf is null holds a zero); `def` says which rows hold a
// value: def[r] == max_def.
struct ShredLeaf {
    enum Kind : uint8_t { Metadata, Value, String, Bool, Int64, Decimal } kind = Value;
    std::vector<std::string> path;    // path_in_schema, from the column's name down
    int max_def = 1;
    int scale = 0, precision = 0;     // Decimal
    std::vector<uint8_t> def;
    std::string bytes;                // Metadata / Value / String: the rows' bytes, back to back
    std::vector<uint32_t> offset;     // ... and each row's start; its length is the next start less this one
    std::vector<int64_t> i64;         // Int64; Decimal of precision 18
    std::vector<__int128> i128;       // Decimal of precision 38
    std::vector<uint8_t> boolean;     // Bool
};

class Shredder {
  public:
    // Learn a column's shredding from the documents of its first row group. `docs[r]` has
    // `.ptr` / `.len`; `validity` is a bit per row, or null when every row holds a document.
    template <typename Doc>
    static ShredType infer(const Doc* docs, const uint8_t* validity, size_t rows) {
        Seen root;
        each_row(docs, validity, 0, rows, [&](size_t, yyjson_val* v) { if (v != nullptr) root.observe(v, 0); });
        int budget = kShredFields;
        return root.decide(budget);
    }

    Shredder(const std::string& column, ShredType type) : type_(std::move(type)) {
        ShredLeaf metadata;
        metadata.kind = ShredLeaf::Metadata;
        metadata.path = {column, "metadata"};
        metadata.max_def = 1;
        leaves_.push_back(std::move(metadata));
        root_ = plan(type_, {column}, 1);
    }

    const ShredType& type() const { return type_; }
    // The leaves in schema order: metadata, then each pair's `value` followed by its typed_value.
    const std::vector<ShredLeaf>& leaves() const { return leaves_; }

    // Fill the leaves with rows [begin, end) of `docs` (replacing what they held). `validity`
    // is the bitmap of all of `docs`, from row 0. Rows are encoded independently, so encoding
    // a range gives exactly that range's entries of encoding them all, with `offset` relative
    // to this Shredder's own `bytes` (docs/parquet_variant_parallel_encode_design.md §3).
    template <typename Doc>
    void encode(const Doc* docs, const uint8_t* validity, size_t rows) { encode(docs, validity, 0, rows); }

    template <typename Doc>
    void encode(const Doc* docs, const uint8_t* validity, size_t begin, size_t end) {
        for (ShredLeaf& l : leaves_) {
            l.def.clear(); l.bytes.clear(); l.offset.clear(); l.i64.clear(); l.i128.clear(); l.boolean.clear();
            l.def.reserve(end - begin);
        }
        each_row(docs, validity, begin, end, [&](size_t, yyjson_val* v) {
            if (v == nullptr) {                                   // SQL NULL: the group itself is null
                for (size_t i = 0; i < leaves_.size(); ++i) null_at(static_cast<int>(i), 0);
                return;
            }
            row_.begin_row();
            pair(root_, v, 1, true);
            row_.seal_keys();
            ShredLeaf& md = leaves_[0];
            md.def.push_back(1);
            md.offset.push_back(static_cast<uint32_t>(md.bytes.size()));
            row_.metadata(md.bytes);
            pair(root_, v, 1, false);
        });
        for (ShredLeaf& l : leaves_) {
            if (l.kind == ShredLeaf::Metadata || l.kind == ShredLeaf::Value || l.kind == ShredLeaf::String) {
                if (l.bytes.size() > UINT32_MAX)
                    throw std::runtime_error("VARIANT column '" + l.path[0] + "': a row group holds more than 4 GiB in one of its columns");
                l.offset.push_back(static_cast<uint32_t>(l.bytes.size()));
            }
        }
    }

  private:
    // A {value, typed_value} pair: its leaves, and for an object typed_value its fields' pairs.
    struct Pair {
        ShredType::Kind kind = ShredType::None;
        int scale = 0, precision = 0;
        int value_leaf = -1;
        int typed_leaf = -1;                        // a scalar typed_value
        std::vector<std::string> names;             // Object: field names, sorted ...
        std::vector<int> fields;                    // ... and each one's pair
        int first_leaf = 0, end_leaf = 0;           // every leaf under the pair
    };

    // Parse rows [begin, end) as `->` does and hand each root (null for a SQL NULL row) to `fn`.
    template <typename Doc, typename Fn>
    static void each_row(const Doc* docs, const uint8_t* validity, size_t begin, size_t end, Fn fn) {
        size_t longest = 0;
        for (size_t r = begin; r < end; ++r)
            if (validity == nullptr || ((validity[r >> 3] >> (r & 7)) & 1)) longest = std::max<size_t>(longest, docs[r].len);
        draken::ops::ReadPool pool(longest);
        for (size_t r = begin; r < end; ++r) {
            if (validity != nullptr && !((validity[r >> 3] >> (r & 7)) & 1)) { fn(r, nullptr); continue; }
            yyjson_read_err err;
            draken::ops::JDocGuard doc(pool.read(reinterpret_cast<const char*>(docs[r].ptr), docs[r].len, &err));
            if (doc.doc == nullptr)
                throw std::runtime_error("a VARIANT holds invalid JSON at row " + std::to_string(r) + ": " +
                                         (err.msg ? err.msg : "unknown error"));
            fn(r, yyjson_doc_get_root(doc.doc));
        }
    }

    // Lay out the pair for `type` under `path`; `level` is the definition level at which the
    // pair's enclosing group is present.
    int plan(const ShredType& type, const std::vector<std::string>& path, int level) {
        const int at = static_cast<int>(pairs_.size());
        pairs_.emplace_back();
        Pair p;
        p.kind = type.kind;
        p.scale = type.scale;
        p.precision = type.precision;
        p.first_leaf = static_cast<int>(leaves_.size());
        auto leaf = [&](const char* name, ShredLeaf::Kind kind) {
            ShredLeaf l;
            l.kind = kind;
            l.path = path;
            l.path.emplace_back(name);
            l.max_def = level + 1;
            l.scale = type.scale;
            l.precision = type.precision;
            leaves_.push_back(std::move(l));
            return static_cast<int>(leaves_.size() - 1);
        };
        p.value_leaf = leaf("value", ShredLeaf::Value);
        switch (type.kind) {
        case ShredType::None: break;
        case ShredType::String: p.typed_leaf = leaf("typed_value", ShredLeaf::String); break;
        case ShredType::Bool: p.typed_leaf = leaf("typed_value", ShredLeaf::Bool); break;
        case ShredType::Int64: p.typed_leaf = leaf("typed_value", ShredLeaf::Int64); break;
        case ShredType::Decimal: p.typed_leaf = leaf("typed_value", ShredLeaf::Decimal); break;
        case ShredType::Object:
            for (const ShredField& f : type.fields) {
                std::vector<std::string> below = path;
                below.emplace_back("typed_value");
                below.push_back(f.name);
                p.names.push_back(f.name);
                p.fields.push_back(plan(f.type, below, level + 1));   // a field's group is required
            }
            break;
        }
        p.end_leaf = static_cast<int>(leaves_.size());
        pairs_[static_cast<size_t>(at)] = std::move(p);
        return at;
    }

    void null_at(int leaf, int level) {
        ShredLeaf& l = leaves_[static_cast<size_t>(leaf)];
        l.def.push_back(static_cast<uint8_t>(level));
        switch (l.kind) {
        case ShredLeaf::Metadata:
        case ShredLeaf::Value:
        case ShredLeaf::String: l.offset.push_back(static_cast<uint32_t>(l.bytes.size())); break;
        case ShredLeaf::Bool: l.boolean.push_back(0); break;
        case ShredLeaf::Int64: l.i64.push_back(0); break;
        case ShredLeaf::Decimal:
            if (l.precision <= 18) l.i64.push_back(0); else l.i128.push_back(0);
            break;
        }
    }

    // The typed leaf of a scalar pair takes `v`? Writes it when it does (and `write`).
    bool typed(const Pair& p, yyjson_val* v, int level, bool write) {
        ShredLeaf& l = leaves_[static_cast<size_t>(p.typed_leaf)];
        const yyjson_type kind = yyjson_get_type(v);
        switch (p.kind) {
        case ShredType::String:
            if (kind != YYJSON_TYPE_STR) return false;
            if (write) {
                l.offset.push_back(static_cast<uint32_t>(l.bytes.size()));
                l.bytes.append(yyjson_get_str(v), yyjson_get_len(v));
            }
            break;
        case ShredType::Bool:
            if (kind != YYJSON_TYPE_BOOL) return false;
            if (write) l.boolean.push_back(yyjson_is_true(v) ? 1 : 0);
            break;
        case ShredType::Int64: {
            if (kind != YYJSON_TYPE_RAW) return false;
            const Number n = classify_number(yyjson_get_raw(v), yyjson_get_len(v));
            if (n.kind != Number::Int) return false;
            if (write) l.i64.push_back(n.i);
            break;
        }
        case ShredType::Decimal: {
            if (kind != YYJSON_TYPE_RAW) return false;
            const Number n = classify_number(yyjson_get_raw(v), yyjson_get_len(v));
            // Only a literal of the column's own scale: at any other it would read back as other text.
            if (n.kind != Number::Decimal || n.scale != p.scale || n.digits > p.precision) return false;
            if (write) {
                if (p.precision <= 18) l.i64.push_back(static_cast<int64_t>(n.unscaled));
                else l.i128.push_back(n.unscaled);
            }
            break;
        }
        default: return false;
        }
        if (write) l.def.push_back(static_cast<uint8_t>(level + 1));
        return true;
    }

    // One row of pair `pi`. `v` is its value, or null when the field is missing; `level` the
    // definition level its enclosing group is present at. `collect` only gathers the field
    // names the row's binary values will use; otherwise every leaf under the pair gets its row.
    void pair(int pi, yyjson_val* v, int level, bool collect) {
        const Pair& p = pairs_[static_cast<size_t>(pi)];
        if (v == nullptr) {                                        // missing: value and typed_value both null
            if (!collect) for (int i = p.first_leaf; i < p.end_leaf; ++i) null_at(i, level);
            return;
        }
        if (p.kind == ShredType::Object && yyjson_is_obj(v)) {
            // The shredded fields go to their pairs; what is left is the residual object.
            const std::vector<RowEncoder::Member>& m = row_.sorted_members(v, 0);
            std::vector<uint32_t> residual;
            std::vector<yyjson_val*> found(p.names.size(), nullptr);
            for (uint32_t i = 0; i < m.size(); ++i) {
                const auto at = std::lower_bound(p.names.begin(), p.names.end(), m[i].key,
                    [](const std::string& name, const RowEncoder::Key& k) {
                        return RowEncoder::key_less({name.data(), name.size()}, k);
                    });
                if (at != p.names.end() && at->size() == m[i].key.len &&
                    std::memcmp(at->data(), m[i].key.s, m[i].key.len) == 0)
                    found[static_cast<size_t>(at - p.names.begin())] = m[i].val;
                else
                    residual.push_back(i);
            }
            if (collect) {
                for (uint32_t i : residual) {
                    row_.keys.push_back(m[i].key);
                    row_.collect(m[i].val, 1);
                }
            } else if (residual.empty()) {
                null_at(p.value_leaf, level);
            } else {
                ShredLeaf& l = leaves_[static_cast<size_t>(p.value_leaf)];
                l.def.push_back(static_cast<uint8_t>(level + 1));
                l.offset.push_back(static_cast<uint32_t>(l.bytes.size()));
                row_.object(l.bytes, m, residual, 0);
            }
            // `m` is the encoder's depth-0 scratch, which a field's own object reuses: the
            // members were copied out into `found` above.
            for (size_t f = 0; f < p.fields.size(); ++f) pair(p.fields[f], found[f], level + 1, collect);
            return;
        }
        if (p.typed_leaf >= 0 && typed(p, v, level, !collect)) {
            if (!collect) null_at(p.value_leaf, level);
            return;
        }
        // Not what the pair shreds: the whole value is binary, and nothing under typed_value is set.
        if (collect) { row_.collect(v, 0); return; }
        ShredLeaf& l = leaves_[static_cast<size_t>(p.value_leaf)];
        l.def.push_back(static_cast<uint8_t>(level + 1));
        l.offset.push_back(static_cast<uint32_t>(l.bytes.size()));
        row_.value(l.bytes, v, 0);
        for (int i = p.first_leaf; i < p.end_leaf; ++i)
            if (i != p.value_leaf) null_at(i, level);
    }

    ShredType type_;
    std::vector<ShredLeaf> leaves_;
    std::vector<Pair> pairs_;
    int root_ = 0;
    RowEncoder row_;
};

}  // namespace variant
}  // namespace rugo
