#include "avro_writer.hpp"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <exception>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include <climits>

#include "avro_container.hpp"                // take_bz_internal_error
#include "bzlib.h"
#include "compression/stream_decompress.hpp"  // crc32_update: the CRC the reader verifies with
#include <ankerl/unordered_dense.h>

#include "core/string_slot.h"
#include "core/uuid_text.h"
#include "libdeflate.h"
#include "snappy.h"
#include "zstd.h"

// The writer-identity string (-DRUGO_PARQUET_CREATED_BY, see build_common.py); the
// same string the parquet footer carries as created_by. Named for parquet because
// that is where it was first used; the value is the build's writer id.
#ifndef RUGO_PARQUET_CREATED_BY
#define RUGO_PARQUET_CREATED_BY "opteryx-rugo"
#endif

namespace rugo::avro {

namespace {

[[noreturn]] void refuse(const std::string& msg) { throw std::invalid_argument("Avro writer: " + msg); }

const char* type_name(DrakenType t) {
    switch (t) {
        case DRAKEN_INT8: return "INT8";
        case DRAKEN_INT16: return "INT16";
        case DRAKEN_INT32: return "INT32";
        case DRAKEN_INT64: return "INT64";
        case DRAKEN_DECIMAL: return "DECIMAL";
        case DRAKEN_FLOAT32: return "FLOAT32";
        case DRAKEN_FLOAT64: return "FLOAT64";
        case DRAKEN_DATE32: return "DATE";
        case DRAKEN_TIMESTAMP64: return "TIMESTAMP";
        case DRAKEN_TIME32: return "TIME32";
        case DRAKEN_TIME64: return "TIME64";
        case DRAKEN_INTERVAL: return "INTERVAL";
        case DRAKEN_BOOL: return "BOOL";
        case DRAKEN_VARCHAR: return "VARCHAR";
        case DRAKEN_NVARCHAR: return "NVARCHAR";
        case DRAKEN_VARBINARY: return "VARBINARY";
        case DRAKEN_VARIANT: return "VARIANT";
        case DRAKEN_ARRAY: return "ARRAY";
        case DRAKEN_NULL: return "NULL";
        case DRAKEN_VECTOR_FP16: return "VECTOR_FP16";
        case DRAKEN_DECIMAL128: return "DECIMAL128";
        case DRAKEN_UINT8: return "UINT8";
        case DRAKEN_UINT16: return "UINT16";
        case DRAKEN_UINT32: return "UINT32";
        case DRAKEN_UINT64: return "UINT64";
        case DRAKEN_UUID: return "UUID";
        default: return "unknown";
    }
}

// ---- binary primitives (the inverse of avro_varint.hpp) ----

inline uint64_t zigzag(int64_t v) { return (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63); }

// Bytes of the varint encoding of z: ceil(bits / 7), at least 1, at most 10.
inline uint32_t varint_len(uint64_t z) {
    return static_cast<uint32_t>((70 - __builtin_clzll(z | 1)) / 7);
}

inline uint8_t* put_varint(uint8_t* p, uint64_t z) {
    while (z >= 0x80) {
        *p++ = static_cast<uint8_t>(z | 0x80);
        z >>= 7;
    }
    *p++ = static_cast<uint8_t>(z);
    return p;
}

void append_varint(std::vector<uint8_t>& out, uint64_t z) {
    uint8_t tmp[10];
    uint8_t* e = put_varint(tmp, z);
    out.insert(out.end(), tmp, e);
}

void append_long(std::vector<uint8_t>& out, int64_t v) { append_varint(out, zigzag(v)); }

void append_bytes(std::vector<uint8_t>& out, const void* p, size_t n) {
    const auto* b = static_cast<const uint8_t*>(p);
    out.insert(out.end(), b, b + n);
}

void append_string(std::vector<uint8_t>& out, const std::string& s) {
    append_long(out, static_cast<int64_t>(s.size()));
    append_bytes(out, s.data(), s.size());
}

// Union branch indexes, zigzag-varint encoded: branch 0 (null) and branch 1 (value).
constexpr uint8_t kNullBranch = 0x00;
constexpr uint8_t kValueBranch = 0x02;

// ---- the Avro type of a field, and the encoder that writes it ----

// One encoder per (Draken physical type, logical descriptor) the writer admits. The
// choice is made once per field from its FieldSpec; both passes dispatch on it once per
// column (never per value).
enum class Enc : uint8_t {
    Bool, I8, I16, I32, I64, U8, U16, U32, F32, F64, Str,
    Null,            // DRAKEN_NULL: every row is the null branch
    Date,            // DATE32 days -> int + date
    TimeMs32, TimeMs32FromS, TimeUs64, TimeMs64, TimeMs64FromS,  // TIME on 4- or 8-byte storage
    TsUs, TsMs, TsMsFromS,                                        // TIMESTAMP64 by unit
    DecFixed64, DecFixed128,  // DECIMAL / DECIMAL128 -> fixed(width) + decimal(p, s), big-endian
    UuidFixed,       // UUID 16 bytes -> fixed(16) + uuid, the bytes as stored
    Ipv4,            // UINT32 + IPV4 descriptor -> long + "opteryx.type": "IPV4"
    Enum,            // VARCHAR / NVARCHAR with symbols -> enum, codes from the edge's analysis
    Array,           // DRAKEN_ARRAY: offsets over a child chain; the leaf has its own Enc
};

// The smallest fixed width that holds every unscaled value of DECIMAL(p, s) as two's
// complement: the smallest n with 10^p - 1 <= 2^(8n - 1) - 1 (Java's and Iceberg's rule).
uint32_t decimal_fixed_width(int precision) {
    unsigned __int128 ten = 1;
    for (int i = 0; i < precision; ++i) ten *= 10;  // precision <= 38: 10^38 < 2^127
    for (uint32_t n = 1; n <= 16; ++n)
        if (ten <= (static_cast<unsigned __int128>(1) << (8 * n - 1))) return n;
    throw std::logic_error("Avro writer: decimal precision beyond 38");
}

// Named types (fixed, enum) are defined at their first use and referenced by full name
// after it, as the spec requires; every name lives in the `rugo` namespace.
struct NamedTypes {
    std::unordered_set<std::string> defined;
    // The type JSON for `name`: `definition` the first time, a reference after.
    std::string use(const std::string& name, const std::string& definition) {
        if (defined.insert(name).second) return definition;
        return "\"rugo." + name + "\"";
    }
};

// The Avro type (JSON text, without the null branch) of one scalar type, its encoder,
// and — for a fixed decimal — its width. docs/AVRO_WRITER_DESIGN.md §5: every type the
// plan maps is written; every type the plan refuses says why; a parameterised type
// without its descriptor fails loud. `col` names the column (or the array element).
Enc choose_scalar(const std::string& col, const ElemSpec& f, NamedTypes& named, std::string& type_json,
                  uint8_t& width) {
    auto plain = [&](const char* t) { type_json = std::string("\"") + t + "\""; };
    auto logical = [&](const char* t, const char* lt) {
        type_json = std::string("{\"type\":\"") + t + "\",\"logicalType\":\"" + lt + "\"}";
    };
    auto unit_refused = [&]() {
        refuse(col + " is in nanoseconds, which Avro cannot carry without loss (the reader refuses "
               "timestamp-nanos, and there is no time-nanos); cast it to microseconds first");
    };
    width = 0;
    switch (f.logical_kind) {
        case 0: break;
        case 1: {  // TIMESTAMP
            if (f.type != DRAKEN_TIMESTAMP64 && f.type != DRAKEN_INT64)
                refuse(col + " carries a TIMESTAMP descriptor on " + type_name(f.type));
            if (f.unit == 3) unit_refused();
            if (f.unit == 2) { logical("long", "timestamp-micros"); return Enc::TsUs; }
            logical("long", "timestamp-millis");
            return f.unit == 0 ? Enc::TsMsFromS : Enc::TsMs;
        }
        case 2: {  // TIME
            const bool wide = f.type == DRAKEN_TIME64 || f.type == DRAKEN_INT64;
            if (!wide && f.type != DRAKEN_TIME32 && f.type != DRAKEN_INT32)
                refuse(col + " carries a TIME descriptor on " + type_name(f.type));
            if (f.unit == 3) unit_refused();
            if (f.unit == 2) {
                if (!wide) refuse(col + " is a 32-bit TIME in microseconds, which the writer does not expect");
                logical("long", "time-micros");
                return Enc::TimeUs64;
            }
            logical("int", "time-millis");
            if (wide) return f.unit == 0 ? Enc::TimeMs64FromS : Enc::TimeMs64;
            return f.unit == 0 ? Enc::TimeMs32FromS : Enc::TimeMs32;
        }
        case 3: {  // DECIMAL -> fixed (doc §13, measured: decode 19% / skip 27% faster than bytes)
            if (f.type != DRAKEN_DECIMAL && f.type != DRAKEN_DECIMAL128)
                refuse(col + " carries a DECIMAL descriptor on " + type_name(f.type));
            if (f.precision < 1 || f.precision > 38 || f.scale < 0 || f.scale > f.precision)
                refuse(col + " has an invalid DECIMAL descriptor (precision " + std::to_string(f.precision) +
                       ", scale " + std::to_string(f.scale) + ")");
            if (f.type == DRAKEN_DECIMAL && f.precision > 18)
                refuse(col + " is a 64-bit DECIMAL with precision " + std::to_string(f.precision) + " (above 18)");
            width = static_cast<uint8_t>(decimal_fixed_width(f.precision));
            const std::string p = std::to_string(f.precision), sc = std::to_string(f.scale);
            const std::string name = "decimal_" + p + "_" + sc;
            type_json = named.use(name, "{\"type\":\"fixed\",\"name\":\"" + name + "\",\"namespace\":\"rugo\",\"size\":" +
                                            std::to_string(width) + ",\"logicalType\":\"decimal\",\"precision\":" + p +
                                            ",\"scale\":" + sc + "}");
            return f.type == DRAKEN_DECIMAL ? Enc::DecFixed64 : Enc::DecFixed128;
        }
        case 4: refuse(col + " is a VECTOR_FP16, which Avro has no type for");
        case 5: {  // IPV4
            if (f.type != DRAKEN_UINT32) refuse(col + " carries an IPV4 descriptor on " + type_name(f.type));
            type_json = "{\"type\":\"long\",\"opteryx.type\":\"IPV4\"}";
            return Enc::Ipv4;
        }
        default:
            refuse(col + " carries an unknown logical descriptor (kind " + std::to_string(f.logical_kind) + ")");
    }
    switch (f.type) {
        case DRAKEN_BOOL: plain("boolean"); return Enc::Bool;
        case DRAKEN_INT8: plain("int"); return Enc::I8;
        case DRAKEN_INT16: plain("int"); return Enc::I16;
        case DRAKEN_INT32: plain("int"); return Enc::I32;
        case DRAKEN_UINT8: plain("int"); return Enc::U8;
        case DRAKEN_UINT16: plain("int"); return Enc::U16;
        case DRAKEN_INT64: plain("long"); return Enc::I64;
        case DRAKEN_UINT32: plain("long"); return Enc::U32;
        case DRAKEN_FLOAT32: plain("float"); return Enc::F32;
        case DRAKEN_FLOAT64: plain("double"); return Enc::F64;
        case DRAKEN_VARCHAR:
        case DRAKEN_NVARCHAR:
        case DRAKEN_VARIANT: plain("string"); return Enc::Str;
        case DRAKEN_VARBINARY: plain("bytes"); return Enc::Str;
        case DRAKEN_DATE32: logical("int", "date"); return Enc::Date;
        case DRAKEN_UUID:  // fixed(16) + uuid (ruled 2026-10-16; Avro 1.12, Iceberg's form)
            type_json = named.use("uuid", "{\"type\":\"fixed\",\"name\":\"uuid\",\"namespace\":\"rugo\",\"size\":16,"
                                          "\"logicalType\":\"uuid\"}");
            return Enc::UuidFixed;
        case DRAKEN_NULL: plain("int"); return Enc::Null;  // as the parquet writer: an all-null int column
        case DRAKEN_TIME32:
        case DRAKEN_TIME64:
        case DRAKEN_TIMESTAMP64:
        case DRAKEN_DECIMAL:
        case DRAKEN_DECIMAL128:
            refuse(col + " is " + type_name(f.type) + " but carries no logical descriptor (unit / precision)");
        case DRAKEN_UINT64: refuse(col + " is UINT64, which Avro has no type for (a value above INT64_MAX would read back wrong)");
        case DRAKEN_INTERVAL: refuse(col + " is INTERVAL, which Avro cannot carry (its `duration` is refused by the reader)");
        case DRAKEN_VECTOR_FP16: refuse(col + " is VECTOR_FP16, which Avro has no type for");
        case DRAKEN_ARRAY: refuse(col + " is an ARRAY where a scalar was expected");
        default:
            refuse(col + " has type " + type_name(f.type) + ", which the Avro writer does not write");
    }
}

// The field's Avro type (without the null branch; build_schema adds it when the plan
// has a branch) and its plan. An ARRAY becomes `{"type":"array","items":["null", T]}`,
// nested as deep as the element chain goes; its leaf is chosen with choose_scalar. A
// column with no element anywhere (empty chain) writes `array<["null","int"]>` — only
// null and empty arrays, so the item type carries no value (the parquet writer's
// convention for the same case). A column with symbols is an enum. A required field
// (no nulls; one-shot writes only) has no union; a typeless NULL column always has one.
FieldPlan choose(const FieldSpec& f, NamedTypes& named, std::string& type_json) {
    FieldPlan plan;
    plan.branch = !f.required || f.type == DRAKEN_NULL;
    const std::string col = "column '" + f.name + "'";
    if (!f.symbols.empty()) {
        if (f.type != DRAKEN_VARCHAR && f.type != DRAKEN_NVARCHAR)
            refuse(col + " has enum symbols but is " + type_name(f.type) + " (only VARCHAR / NVARCHAR)");
        if (f.logical_kind != 0) refuse(col + " has enum symbols and a logical descriptor");
        std::unordered_set<std::string> seen;
        std::string def = "{\"type\":\"enum\",\"name\":\"" + f.name + "_enum\",\"namespace\":\"rugo\",\"symbols\":[";
        for (size_t i = 0; i < f.symbols.size(); ++i) {
            if (!is_avro_name(f.symbols[i]))
                refuse(col + ": enum symbol '" + f.symbols[i] + "' is not a valid Avro name");
            if (!seen.insert(f.symbols[i]).second) refuse(col + ": enum symbol '" + f.symbols[i] + "' is repeated");
            if (i) def.push_back(',');
            def += "\"" + f.symbols[i] + "\"";
        }
        def += "]}";
        if (named.defined.count(f.name + "_enum")) refuse(col + ": the enum name '" + f.name + "_enum' is taken");
        type_json = named.use(f.name + "_enum", def);
        plan.enc = static_cast<uint8_t>(Enc::Enum);
        return plan;
    }
    if (f.type != DRAKEN_ARRAY) {
        if (!f.elements.empty()) refuse(col + " is not an ARRAY but has element types");
        ElemSpec e{f.type, f.logical_kind, f.unit, f.scale, f.precision};
        plan.enc = static_cast<uint8_t>(choose_scalar(col, e, named, type_json, plan.width));
        return plan;
    }
    if (f.logical_kind != 0) refuse(col + " is an ARRAY carrying a logical descriptor");
    plan.enc = static_cast<uint8_t>(Enc::Array);
    plan.depth = 1;
    std::string leaf_json = "\"int\"";
    plan.leaf = static_cast<uint8_t>(Enc::I32);  // unused: no element is ever written
    for (size_t i = 0; i < f.elements.size(); ++i) {
        const ElemSpec& e = f.elements[i];
        if (e.type == DRAKEN_ARRAY) {
            if (e.logical_kind != 0) refuse(col + ": a nested ARRAY carries a logical descriptor");
            if (i + 1 == f.elements.size()) refuse(col + ": the element chain ends on an ARRAY");
            ++plan.depth;
            continue;
        }
        if (i + 1 != f.elements.size()) refuse(col + ": a scalar element is followed by more levels");
        plan.leaf = static_cast<uint8_t>(choose_scalar(col + " (its elements)", e, named, leaf_json, plan.leaf_width));
    }
    type_json = leaf_json;
    for (uint32_t d = 0; d < plan.depth; ++d) type_json = "{\"type\":\"array\",\"items\":[\"null\"," + type_json + "]}";
    return plan;
}

const char* codec_name(WriteCodec c) {
    switch (c) {
        case WriteCodec::Null: return "null";
        case WriteCodec::Deflate: return "deflate";
        case WriteCodec::Snappy: return "snappy";
        case WriteCodec::Zstandard: return "zstandard";
        case WriteCodec::Bzip2: return "bzip2";
    }
    return "";
}

// ---- column encoders ----
//
// Each encoder is a pair of static functions over one physical value index `j`
// (= selection[i]): `len` says how many bytes the value takes, `put` writes exactly
// that many. Both passes are instantiated per type and per validity presence, so the
// row loops carry no dispatch.

template <typename T>
struct IntEnc {
    static uint32_t len(const DrakenVector* v, uint32_t j) {
        return varint_len(zigzag(static_cast<int64_t>(static_cast<const T*>(v->data)[j])));
    }
    static uint8_t* put(uint8_t* p, const DrakenVector* v, uint32_t j) {
        return put_varint(p, zigzag(static_cast<int64_t>(static_cast<const T*>(v->data)[j])));
    }
};

template <typename T>
struct FixedEnc {
    static uint32_t len(const DrakenVector*, uint32_t) { return sizeof(T); }
    static uint8_t* put(uint8_t* p, const DrakenVector* v, uint32_t j) {
        std::memcpy(p, static_cast<const T*>(v->data) + j, sizeof(T));  // little-endian on every supported target
        return p + sizeof(T);
    }
};

struct BoolEnc {
    static uint32_t len(const DrakenVector*, uint32_t) { return 1; }
    static uint8_t* put(uint8_t* p, const DrakenVector* v, uint32_t j) {
        *p = (static_cast<const uint8_t*>(v->data)[j >> 3] >> (j & 7)) & 1;  // bit-packed per physical value
        return p + 1;
    }
};

struct StrEnc {
    static uint32_t len(const DrakenVector* v, uint32_t j) {
        const auto* sa = static_cast<const DrakenStringArena*>(v->data);
        const uint32_t n = str_length(&sa->slots[j]);
        return varint_len(static_cast<uint64_t>(n) << 1) + n;
    }
    static uint8_t* put(uint8_t* p, const DrakenVector* v, uint32_t j) {
        const auto* sa = static_cast<const DrakenStringArena*>(v->data);
        const DrakenStringSlot* s = &sa->slots[j];
        const uint32_t n = str_length(s);
        p = put_varint(p, static_cast<uint64_t>(n) << 1);
        std::memcpy(p, str_data(s, sa->arena), n);
        return p + n;
    }
};

// A temporal value: T storage, scaled by Mult into the Avro unit (checked), optionally
// range-checked for an Avro `int` (time-millis).
template <typename T, int64_t Mult, bool Int32Range>
struct ScaledEnc {
    static int64_t value(const DrakenVector* v, uint32_t j) {
        const int64_t x = static_cast<int64_t>(static_cast<const T*>(v->data)[j]);
        int64_t y = x;
        if (Mult != 1 && __builtin_mul_overflow(x, Mult, &y))
            throw std::runtime_error("Avro writer: a temporal value overflows when converted to the Avro unit");
        if (Int32Range && (y < INT32_MIN || y > INT32_MAX))
            throw std::runtime_error("Avro writer: a time-millis value (" + std::to_string(y) +
                                     ") is outside the 32-bit range of an Avro int");
        return y;
    }
    static uint32_t len(const DrakenVector* v, uint32_t j) { return varint_len(zigzag(value(v, j))); }
    static uint8_t* put(uint8_t* p, const DrakenVector* v, uint32_t j) { return put_varint(p, zigzag(value(v, j))); }
};

// A fixed decimal: the unscaled value (StoreW-byte little-endian storage: 8 = int64,
// 16 = __int128) as OutW-byte big-endian two's complement. A value whose significant
// bits do not fit OutW bytes (outside the column's declared precision) is refused —
// a truncated fixed would read back as another number.
template <uint32_t StoreW, uint32_t OutW>
struct DecFixedEnc {
    using T = std::conditional_t<StoreW == 8, int64_t, __int128>;
    static uint32_t len(const DrakenVector*, uint32_t) { return OutW; }
    static uint8_t* put(uint8_t* p, const DrakenVector* v, uint32_t j) {
        T x;
        std::memcpy(&x, static_cast<const uint8_t*>(v->data) + static_cast<size_t>(j) * StoreW, StoreW);
        if constexpr (OutW < StoreW) {
            const T hi = x >> (8 * OutW - 1);
            if (hi != 0 && hi != -1)
                throw std::runtime_error("Avro writer: a DECIMAL value does not fit its declared precision");
        }
        for (uint32_t i = 0; i < OutW; ++i) p[OutW - 1 - i] = static_cast<uint8_t>(x >> (8 * i));
        return p + OutW;
    }
};

// UUID: the 16 stored bytes (network order, docs/UUID_TYPE_DESIGN.md) as fixed(16).
struct UuidFixedEnc {
    static uint32_t len(const DrakenVector*, uint32_t) { return 16; }
    static uint8_t* put(uint8_t* p, const DrakenVector* v, uint32_t j) {
        std::memcpy(p, static_cast<const uint8_t*>(v->data) + static_cast<size_t>(j) * 16, 16);
        return p + 16;
    }
};

inline bool row_valid(const uint8_t* validity, uint32_t i) { return (validity[i >> 3] >> (i & 7)) & 1; }

// Length passes cover rows [first, first + count) and add into row_len[first + i]
// (row_len is the batch's base), so disjoint row ranges can run on separate threads.
// `Branch`: the field is `["null", T]` and every value carries its union byte; a
// required field (no nulls, so no validity is consulted) has none.
template <typename Enc, bool HasValidity, bool Branch>
void length_pass(const DrakenVector* v, uint32_t first, uint32_t count, uint64_t* row_len) {
    const uint32_t* sel = v->selection;
    const uint8_t* validity = v->validity;
    const uint32_t end = first + count;
    for (uint32_t i = first; i < end; ++i) {
        if (HasValidity && !row_valid(validity, i)) { row_len[i] += 1; continue; }
        row_len[i] += (Branch ? 1 : 0) + Enc::len(v, sel[i]);
    }
}

template <typename Enc, bool HasValidity, bool Branch>
void write_pass(const DrakenVector* v, uint32_t first, uint32_t count, uint8_t** cursor) {
    const uint32_t* sel = v->selection + first;
    const uint8_t* validity = v->validity;
    for (uint32_t r = 0; r < count; ++r) {
        uint8_t* p = cursor[r];
        if (HasValidity && !row_valid(validity, first + r)) {
            *p++ = kNullBranch;
        } else {
            if (Branch) *p++ = kValueBranch;
            p = Enc::put(p, v, sel[r]);
        }
        cursor[r] = p;
    }
}

template <typename Enc>
void length_column(const DrakenVector* v, bool branch, uint32_t first, uint32_t count, uint64_t* row_len) {
    if (!branch) length_pass<Enc, false, false>(v, first, count, row_len);
    else if (v->validity) length_pass<Enc, true, true>(v, first, count, row_len);
    else length_pass<Enc, false, true>(v, first, count, row_len);
}

template <typename Enc>
void write_column(const DrakenVector* v, bool branch, uint32_t first, uint32_t count, uint8_t** cursor) {
    if (!branch) write_pass<Enc, false, false>(v, first, count, cursor);
    else if (v->validity) write_pass<Enc, true, true>(v, first, count, cursor);
    else write_pass<Enc, false, true>(v, first, count, cursor);
}

// ---- ENUM ----
//
// An enum value is its symbol index as an Avro int. `codes` maps each PHYSICAL value of
// the vector to its symbol (built by build_enum), so a row reads codes[selection[i]] —
// the uniform access, whatever the vector's shape.
template <bool HasValidity, bool Branch>
void length_enum_pass(const DrakenVector* v, const uint32_t* codes, uint32_t first, uint32_t count,
                      uint64_t* row_len) {
    const uint32_t* sel = v->selection;
    const uint8_t* validity = v->validity;
    const uint32_t end = first + count;
    for (uint32_t i = first; i < end; ++i) {
        if (HasValidity && !row_valid(validity, i)) { row_len[i] += 1; continue; }
        row_len[i] += (Branch ? 1 : 0) + varint_len(static_cast<uint64_t>(codes[sel[i]]) << 1);
    }
}

template <bool HasValidity, bool Branch>
void write_enum_pass(const DrakenVector* v, const uint32_t* codes, uint32_t first, uint32_t count,
                     uint8_t** cursor) {
    const uint32_t* sel = v->selection + first;
    const uint8_t* validity = v->validity;
    for (uint32_t r = 0; r < count; ++r) {
        uint8_t* p = cursor[r];
        if (HasValidity && !row_valid(validity, first + r)) {
            *p++ = kNullBranch;
        } else {
            if (Branch) *p++ = kValueBranch;
            p = put_varint(p, static_cast<uint64_t>(codes[sel[r]]) << 1);
        }
        cursor[r] = p;
    }
}

// ---- ARRAY ----
//
// An Avro array is `long count` (positive), that many items, then `0`; an empty array
// is just `0`. Every item is `["null", T]`. A Draken ARRAY vector's data is
// int32 offsets[length + 1] into its child; the child of an ARRAY<ARRAY<T>> is itself
// an ARRAY vector, down to a scalar leaf. `levels[0]` is the top array vector,
// `levels[d]` for d in 1..depth-1 the nested array vectors, `levels[depth]` the leaf.
// The leaf encoder is a template parameter, so the only per-element branch is the
// level test; the leaf's own type is decided once per column.
struct ArrayChain {
    const DrakenVector* const* levels;
    uint32_t depth;
};

template <typename Leaf>
struct ArrEnc {
    // The encoded size of the array at physical index j of level lvl (without the
    // enclosing branch byte).
    static uint64_t len(const ArrayChain& ch, uint32_t lvl, uint32_t j) {
        const DrakenVector* av = ch.levels[lvl];
        const int32_t* off = static_cast<const int32_t*>(av->data);
        const uint32_t b = static_cast<uint32_t>(off[j]), e = static_cast<uint32_t>(off[j + 1]);
        const uint32_t n = e - b;
        uint64_t bytes = n ? varint_len(zigzag(n)) + 1 : 1;
        if (n == 0) return bytes;
        const DrakenVector* child = ch.levels[lvl + 1];
        const bool leaf = lvl + 1 == ch.depth;
        const bool all_null = leaf && child->type == DRAKEN_NULL;
        const uint8_t* validity = child->validity;
        const uint32_t* sel = child->selection;
        for (uint32_t k = b; k < e; ++k) {
            bytes += 1;
            if (all_null || (validity != nullptr && !row_valid(validity, k))) continue;
            bytes += leaf ? Leaf::len(child, sel[k]) : len(ch, lvl + 1, sel[k]);
        }
        return bytes;
    }
    static uint8_t* put(uint8_t* p, const ArrayChain& ch, uint32_t lvl, uint32_t j) {
        const DrakenVector* av = ch.levels[lvl];
        const int32_t* off = static_cast<const int32_t*>(av->data);
        const uint32_t b = static_cast<uint32_t>(off[j]), e = static_cast<uint32_t>(off[j + 1]);
        const uint32_t n = e - b;
        if (n == 0) { *p++ = 0; return p; }
        p = put_varint(p, zigzag(n));
        const DrakenVector* child = ch.levels[lvl + 1];
        const bool leaf = lvl + 1 == ch.depth;
        const bool all_null = leaf && child->type == DRAKEN_NULL;
        const uint8_t* validity = child->validity;
        const uint32_t* sel = child->selection;
        for (uint32_t k = b; k < e; ++k) {
            if (all_null || (validity != nullptr && !row_valid(validity, k))) { *p++ = kNullBranch; continue; }
            *p++ = kValueBranch;
            p = leaf ? Leaf::put(p, child, sel[k]) : put(p, ch, lvl + 1, sel[k]);
        }
        *p++ = 0;  // end of the array's blocks
        return p;
    }
};

template <typename Leaf, bool HasValidity, bool Branch>
void length_array_pass(const ArrayChain& ch, uint32_t first, uint32_t count, uint64_t* row_len) {
    const DrakenVector* v = ch.levels[0];
    const uint32_t* sel = v->selection;
    const uint8_t* validity = v->validity;
    const uint32_t end = first + count;
    for (uint32_t i = first; i < end; ++i) {
        if (HasValidity && !row_valid(validity, i)) { row_len[i] += 1; continue; }
        row_len[i] += (Branch ? 1 : 0) + ArrEnc<Leaf>::len(ch, 0, sel[i]);
    }
}

template <typename Leaf, bool HasValidity, bool Branch>
void write_array_pass(const ArrayChain& ch, uint32_t first, uint32_t count, uint8_t** cursor) {
    const DrakenVector* v = ch.levels[0];
    const uint32_t* sel = v->selection + first;
    const uint8_t* validity = v->validity;
    for (uint32_t r = 0; r < count; ++r) {
        uint8_t* p = cursor[r];
        if (HasValidity && !row_valid(validity, first + r)) {
            *p++ = kNullBranch;
        } else {
            if (Branch) *p++ = kValueBranch;
            p = ArrEnc<Leaf>::put(p, ch, 0, sel[r]);
        }
        cursor[r] = p;
    }
}

template <typename T> struct Tag { using type = T; };

// fn(Tag<DecFixedEnc<StoreW, W>>{}) for the runtime width `w` (1..StoreW).
template <uint32_t StoreW, uint32_t W, typename Fn>
void with_dec_width(uint32_t w, Fn& fn) {
    if constexpr (W > StoreW) {
        throw std::logic_error("Avro writer: a fixed decimal width beyond its storage");
    } else {
        if (w == W) return fn(Tag<DecFixedEnc<StoreW, W>>{});
        return with_dec_width<StoreW, W + 1>(w, fn);
    }
}

// Calls fn(Tag<Encoder>{}) for the scalar encoder `e` names (and, for a fixed decimal,
// its width) — the one switch the passes dispatch through, once per column.
template <typename Fn>
void with_scalar(Enc e, uint32_t width, Fn&& fn) {
    switch (e) {
        case Enc::Bool: return fn(Tag<BoolEnc>{});
        case Enc::I8: return fn(Tag<IntEnc<int8_t>>{});
        case Enc::I16: return fn(Tag<IntEnc<int16_t>>{});
        case Enc::I32:
        case Enc::Date: return fn(Tag<IntEnc<int32_t>>{});
        case Enc::I64:
        case Enc::TsUs:
        case Enc::TimeUs64: return fn(Tag<IntEnc<int64_t>>{});
        case Enc::U8: return fn(Tag<IntEnc<uint8_t>>{});
        case Enc::U16: return fn(Tag<IntEnc<uint16_t>>{});
        case Enc::U32:
        case Enc::Ipv4: return fn(Tag<IntEnc<uint32_t>>{});
        case Enc::F32: return fn(Tag<FixedEnc<float>>{});
        case Enc::F64: return fn(Tag<FixedEnc<double>>{});
        case Enc::Str: return fn(Tag<StrEnc>{});
        case Enc::TimeMs32: return fn(Tag<ScaledEnc<int32_t, 1, true>>{});
        case Enc::TimeMs32FromS: return fn(Tag<ScaledEnc<int32_t, 1000, true>>{});
        case Enc::TimeMs64: return fn(Tag<ScaledEnc<int64_t, 1, true>>{});
        case Enc::TimeMs64FromS: return fn(Tag<ScaledEnc<int64_t, 1000, true>>{});
        case Enc::TsMs: return fn(Tag<ScaledEnc<int64_t, 1, false>>{});
        case Enc::TsMsFromS: return fn(Tag<ScaledEnc<int64_t, 1000, false>>{});
        case Enc::DecFixed64: return with_dec_width<8, 1>(width, fn);
        case Enc::DecFixed128: return with_dec_width<16, 1>(width, fn);
        case Enc::UuidFixed: return fn(Tag<UuidFixedEnc>{});
        case Enc::Null:   // handled by the caller (every value null)
        case Enc::Enum:   // handled by the caller (codes from the edge)
        case Enc::Array:  // handled by the caller (child chain)
            break;
    }
    throw std::logic_error("Avro writer: no scalar encoder for a field the schema admitted");
}

void length_array(const FieldPlan& plan, const WriteColumn& col, uint32_t first, uint32_t count, uint64_t* row_len) {
    // levels: the column's own vector, then its child chain.
    std::vector<const DrakenVector*> levels;
    levels.reserve(col.levels.size() + 1);
    levels.push_back(col.vec);
    levels.insert(levels.end(), col.levels.begin(), col.levels.end());
    const ArrayChain ch{levels.data(), plan.depth};
    const Enc leaf = col.levels.empty() ? Enc::I32 : static_cast<Enc>(plan.leaf);
    with_scalar(leaf, plan.leaf_width, [&](auto tag) {
        using L = typename decltype(tag)::type;
        if (!plan.branch) length_array_pass<L, false, false>(ch, first, count, row_len);
        else if (col.vec->validity) length_array_pass<L, true, true>(ch, first, count, row_len);
        else length_array_pass<L, false, true>(ch, first, count, row_len);
    });
}

void write_array(const FieldPlan& plan, const WriteColumn& col, uint32_t first, uint32_t count, uint8_t** cursor) {
    std::vector<const DrakenVector*> levels;
    levels.reserve(col.levels.size() + 1);
    levels.push_back(col.vec);
    levels.insert(levels.end(), col.levels.begin(), col.levels.end());
    const ArrayChain ch{levels.data(), plan.depth};
    const Enc leaf = col.levels.empty() ? Enc::I32 : static_cast<Enc>(plan.leaf);
    with_scalar(leaf, plan.leaf_width, [&](auto tag) {
        using L = typename decltype(tag)::type;
        if (!plan.branch) write_array_pass<L, false, false>(ch, first, count, cursor);
        else if (col.vec->validity) write_array_pass<L, true, true>(ch, first, count, cursor);
        else write_array_pass<L, false, true>(ch, first, count, cursor);
    });
}

// DRAKEN_NULL: every row is the null branch, whatever the vector's buffers say.
void length_null(uint32_t first, uint32_t count, uint64_t* row_len) {
    for (uint32_t i = first; i < first + count; ++i) row_len[i] += 1;
}
void write_null(uint32_t count, uint8_t** cursor) {
    for (uint32_t r = 0; r < count; ++r) *cursor[r]++ = kNullBranch;
}

// Static dispatch, once per column per pass.
void length_dispatch(const FieldPlan& plan, const WriteColumn& col, uint32_t first, uint32_t count,
                     uint64_t* row_len) {
    const Enc e = static_cast<Enc>(plan.enc);
    const DrakenVector* v = col.vec;
    switch (e) {
        case Enc::Array: return length_array(plan, col, first, count, row_len);
        case Enc::Null: return length_null(first, count, row_len);
        case Enc::Enum:
            if (!plan.branch) return length_enum_pass<false, false>(v, col.enum_codes, first, count, row_len);
            if (v->validity) return length_enum_pass<true, true>(v, col.enum_codes, first, count, row_len);
            return length_enum_pass<false, true>(v, col.enum_codes, first, count, row_len);
        default:
            return with_scalar(e, plan.width, [&](auto tag) {
                length_column<typename decltype(tag)::type>(v, plan.branch, first, count, row_len);
            });
    }
}

void write_dispatch(const FieldPlan& plan, const WriteColumn& col, uint32_t first, uint32_t count,
                    uint8_t** cursor) {
    const Enc e = static_cast<Enc>(plan.enc);
    const DrakenVector* v = col.vec;
    switch (e) {
        case Enc::Array: return write_array(plan, col, first, count, cursor);
        case Enc::Null: return write_null(count, cursor);
        case Enc::Enum:
            if (!plan.branch) return write_enum_pass<false, false>(v, col.enum_codes, first, count, cursor);
            if (v->validity) return write_enum_pass<true, true>(v, col.enum_codes, first, count, cursor);
            return write_enum_pass<false, true>(v, col.enum_codes, first, count, cursor);
        default:
            return with_scalar(e, plan.width, [&](auto tag) {
                write_column<typename decltype(tag)::type>(v, plan.branch, first, count, cursor);
            });
    }
}

// A string column whose payload bytes were never materialised (payloads_elided) or
// that has no storage at all while holding a valid row cannot be written.
void check_string_storage(const FieldSpec& f, const DrakenVector* v, uint32_t nrows) {
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    if (sa != nullptr) {
        if (sa->payloads_elided)
            refuse("column '" + f.name + "': the string payloads were not materialised (length-only column)");
        return;
    }
    for (uint32_t i = 0; i < nrows; ++i)
        if (v->validity == nullptr || row_valid(v->validity, i))
            refuse("column '" + f.name + "' has a non-null row but no string storage");
}

// An ARRAY column's child chain must be the one its FieldSpec was built from (same
// length, same types), every level's last offset must stay inside its child, and a
// string leaf must hold its payloads.
void check_array_chain(const FieldSpec& f, const WriteColumn& col) {
    const std::string name = "column '" + f.name + "'";
    if (col.levels.size() != f.elements.size())
        refuse(name + " has " + std::to_string(col.levels.size()) + " nested levels in this batch but " +
               std::to_string(f.elements.size()) + " in the schema");
    const DrakenVector* parent = col.vec;
    for (size_t i = 0; i < col.levels.size(); ++i) {
        const DrakenVector* child = col.levels[i];
        if (child == nullptr) refuse(name + ": a nested level has no vector");
        if (child->type != f.elements[i].type)
            refuse(name + ": nested level " + std::to_string(i) + " is " + type_name(child->type) +
                   " in this batch but " + type_name(f.elements[i].type) + " in the schema");
        if (parent->data_length > 0) {
            const int32_t last = static_cast<const int32_t*>(parent->data)[parent->data_length];
            if (last < 0 || static_cast<uint32_t>(last) > child->length)
                refuse(name + ": array offsets run past the child vector");
        }
        if (draken_type_is_string_storage(child->type)) check_string_storage(f, child, child->length);
        parent = child;
    }
    if (col.levels.empty() && col.vec->data_length > 0) {
        // No child: every row must be null or empty.
        const int32_t last = static_cast<const int32_t*>(col.vec->data)[col.vec->data_length];
        if (last != 0) refuse(name + " has elements but no child vector");
    }
}

}  // namespace

// ---- public ----

WriteCodec parse_write_codec(const std::string& name) {
    if (name == "null") return WriteCodec::Null;
    if (name == "deflate") return WriteCodec::Deflate;
    if (name == "snappy") return WriteCodec::Snappy;
    if (name == "zstandard") return WriteCodec::Zstandard;
    if (name == "bzip2") return WriteCodec::Bzip2;
    refuse("codec '" + name + "' is not an Avro codec (null, deflate, snappy, zstandard and bzip2 are)");
}

namespace {

bool name_ok(std::string_view s) {
    if (s.empty()) return false;
    auto start_ok = [](char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'; };
    if (!start_ok(s[0])) return false;
    for (char c : s)
        if (!start_ok(c) && !(c >= '0' && c <= '9')) return false;
    return true;
}

}  // namespace

bool is_avro_name(const std::string& s) { return name_ok(s); }

namespace {

// The record schema and, per field, its plan and its type JSON (the `T` of
// `["null", T]`), which a streamed batch is compared against.
std::string build_schema(const std::vector<FieldSpec>& fields, const std::string& record_name,
                         std::vector<FieldPlan>& plans, std::vector<std::string>& types) {
    if (fields.empty()) refuse("a record needs at least one column");
    if (!is_avro_name(record_name))
        refuse("record name '" + record_name + "' is not a valid Avro name ([A-Za-z_][A-Za-z0-9_]*)");
    plans.assign(fields.size(), FieldPlan{});
    types.assign(fields.size(), std::string());
    NamedTypes named;
    std::unordered_set<std::string> seen;
    std::string s = "{\"type\":\"record\",\"name\":\"" + record_name + "\",\"namespace\":\"rugo\",\"fields\":[";
    for (size_t i = 0; i < fields.size(); ++i) {
        const FieldSpec& f = fields[i];
        if (!is_avro_name(f.name))
            refuse("column name '" + f.name + "' is not a valid Avro name ([A-Za-z_][A-Za-z0-9_]*); rename it before writing");
        if (!seen.insert(f.name).second) refuse("column name '" + f.name + "' appears more than once");
        plans[i] = choose(f, named, types[i]);
        if (i) s.push_back(',');
        s += "{\"name\":\"" + f.name + "\",\"type\":" +
             (plans[i].branch ? "[\"null\"," + types[i] + "]" : types[i]) + "}";
    }
    s += "]}";
    return s;
}

// Rows per task of the parallel length pass.
constexpr uint32_t kLengthChunkRows = 65536;

// Runs fn(task, worker) for every task in [0, tasks) on up to `threads` threads (the
// calling thread is worker 0). Tasks are claimed in increasing order; after a failure
// no new task is claimed, and the failure of the LOWEST task index is rethrown once
// every thread has stopped — every lower task was already claimed and runs to its end,
// so this is the exception a serial run would have thrown.
template <typename Fn>
void run_tasks(size_t tasks, size_t threads, Fn&& fn) {
    if (threads <= 1 || tasks <= 1) {
        for (size_t t = 0; t < tasks; ++t) fn(t, 0);
        return;
    }
    std::atomic<size_t> next{0};
    std::atomic<bool> failed{false};
    std::mutex mu;
    std::exception_ptr err;
    size_t err_task = SIZE_MAX;
    auto work = [&](size_t worker) {
        for (;;) {
            if (failed.load(std::memory_order_acquire)) return;
            const size_t t = next.fetch_add(1, std::memory_order_relaxed);
            if (t >= tasks) return;
            try {
                fn(t, worker);
            } catch (...) {
                std::lock_guard<std::mutex> g(mu);
                if (t < err_task) { err_task = t; err = std::current_exception(); }
                failed.store(true, std::memory_order_release);
                return;
            }
        }
    };
    std::vector<std::thread> pool;
    pool.reserve(threads - 1);
    for (size_t w = 1; w < threads; ++w) pool.emplace_back(work, w);
    work(0);
    for (std::thread& t : pool) t.join();
    if (err) std::rethrow_exception(err);
}

}  // namespace

bool column_has_nulls(const DrakenVector* v) {
    if (v->type == DRAKEN_NULL) return v->length > 0;
    if (v->validity == nullptr) return false;
    const uint8_t* b = v->validity;
    const uint32_t n = v->length;
    const uint32_t full = n >> 3;
    for (uint32_t k = 0; k < full; ++k)
        if (b[k] != 0xFF) return true;
    for (uint32_t i = full << 3; i < n; ++i)
        if (!row_valid(b, i)) return true;
    return false;
}

namespace {

// A string of at most STR_INLINE_MAX (12) bytes IS its 16-byte slot: the length plus
// the inline bytes, zero beyond the length (core/string_slot.h), so two such slots are
// equal exactly when the strings are — the key needs no hashing of the bytes.
struct SlotKey {
    uint64_t lo, hi;
    bool operator==(const SlotKey& o) const { return lo == o.lo && hi == o.hi; }
};

// ankerl's mix is a 128-bit multiply of its two words: a zero word zeroes the product.
// A string of up to 4 bytes has an all-zero high word (zero padding), so each word is
// xored with its own non-zero constant first — without it every 2-letter value hashed
// to 0 (measured: country codes probed 2x slower than the std::unordered_map it replaced).
struct SlotKeyHash {
    using is_avalanching = void;
    uint64_t operator()(const SlotKey& k) const noexcept {
        return ankerl::unordered_dense::detail::hash_impl::mix(k.lo ^ 0x9E3779B97F4A7C15ull,
                                                               k.hi ^ 0xE7037ED1A0B428DBull);
    }
};

// One physical range's distinct values: short strings by slot, longer ones by content;
// local ids in first-seen order, `texts[id]` their bytes (views into the vector).
struct EnumRange {
    ankerl::unordered_dense::map<SlotKey, uint32_t, SlotKeyHash> short_ids;
    ankerl::unordered_dense::map<std::string_view, uint32_t> long_ids;
    std::vector<std::string_view> texts;
};

// Rows per task of the parallel analysis (as the length pass).
constexpr uint32_t kEnumChunk = 65536;
// Values checked serially first: a column with a non-name early (the common way a
// column is not an enum) is rejected with no threads started.
constexpr uint32_t kEnumPrecheck = 256;

}  // namespace

bool build_enum(const DrakenVector* v, size_t max_symbols, size_t max_threads, std::vector<std::string>& symbols,
                std::vector<uint32_t>& codes) {
    symbols.clear();
    codes.clear();
    if (v->type != DRAKEN_VARCHAR && v->type != DRAKEN_NVARCHAR) return false;
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    if (sa == nullptr || sa->payloads_elided) return false;
    const uint32_t n = v->length;
    const uint32_t dl = v->data_length;
    const uint32_t* sel = v->selection;
    const uint8_t* validity = v->validity;
    auto text_of = [&](uint32_t j) {
        const DrakenStringSlot* slot = &sa->slots[j];
        return std::string_view(reinterpret_cast<const char*>(str_data(slot, sa->arena)), str_length(slot));
    };

    // 0. Serial precheck of the first values.
    for (uint32_t i = 0, seen = 0; i < n && seen < kEnumPrecheck; ++i) {
        if (validity != nullptr && !row_valid(validity, i)) continue;
        if (!name_ok(text_of(sel[i]))) return false;
        ++seen;
    }
    const size_t threads = max_threads ? max_threads : std::max<size_t>(1, std::thread::hardware_concurrency());

    // 1. Rows, by range: which physical values a non-null row uses (several rows may
    //    share one — a relaxed store of the same byte), and the column's size as strings.
    std::vector<uint8_t> used(dl, 0);
    const size_t row_tasks = (static_cast<size_t>(n) + kEnumChunk - 1) / kEnumChunk;
    std::vector<uint64_t> plain(row_tasks, 0), valid(row_tasks, 0);
    run_tasks(row_tasks, std::min(threads, row_tasks), [&](size_t t, size_t) {
        const uint32_t first = static_cast<uint32_t>(t * kEnumChunk);
        const uint32_t end = std::min<uint32_t>(n, first + kEnumChunk);
        uint64_t bytes = 0, rows = 0;
        for (uint32_t i = first; i < end; ++i) {
            if (validity != nullptr && !row_valid(validity, i)) continue;
            const uint32_t j = sel[i];
            std::atomic_ref<uint8_t>(used[j]).store(1, std::memory_order_relaxed);
            const uint32_t len = str_length(&sa->slots[j]);
            bytes += varint_len(static_cast<uint64_t>(len) << 1) + len;
            ++rows;
        }
        plain[t] = bytes;
        valid[t] = rows;
    });
    uint64_t plain_bytes = 0, valid_rows = 0;
    for (size_t t = 0; t < row_tasks; ++t) { plain_bytes += plain[t]; valid_rows += valid[t]; }
    if (valid_rows == 0) return false;

    // 2. Physical values, by range: each range's distinct used values, each checked
    //    against the symbol rule once; a range past max_symbols rejects the column.
    const size_t phys_tasks = (static_cast<size_t>(dl) + kEnumChunk - 1) / kEnumChunk;
    std::vector<EnumRange> ranges(phys_tasks);
    std::vector<uint32_t> local(dl);
    std::atomic<bool> reject{false};
    run_tasks(phys_tasks, std::min(threads, phys_tasks), [&](size_t t, size_t) {
        EnumRange& rg = ranges[t];
        const uint32_t first = static_cast<uint32_t>(t * kEnumChunk);
        const uint32_t end = std::min<uint32_t>(dl, first + kEnumChunk);
        for (uint32_t j = first; j < end; ++j) {
            if (!used[j]) continue;
            const DrakenStringSlot* slot = &sa->slots[j];
            const uint32_t next = static_cast<uint32_t>(rg.texts.size());
            uint32_t id;
            bool fresh;
            if (str_is_inline(slot)) {
                auto r = rg.short_ids.try_emplace(SlotKey{slot->raw.lo, slot->raw.hi}, next);
                id = r.first->second;
                fresh = r.second;
            } else {
                auto r = rg.long_ids.try_emplace(text_of(j), next);
                id = r.first->second;
                fresh = r.second;
            }
            if (fresh) {
                const std::string_view tx = text_of(j);
                if (!name_ok(tx) || rg.texts.size() == max_symbols || reject.load(std::memory_order_relaxed)) {
                    reject.store(true, std::memory_order_relaxed);
                    return;
                }
                rg.texts.push_back(tx);
            }
            local[j] = id;
        }
    });
    if (reject.load()) return false;

    // 3. Merge (serial; at most max_symbols per range): one id per distinct text.
    ankerl::unordered_dense::map<std::string_view, uint32_t> global;
    std::vector<std::string_view> texts;
    std::vector<std::vector<uint32_t>> to_global(phys_tasks);
    for (size_t t = 0; t < phys_tasks; ++t) {
        to_global[t].resize(ranges[t].texts.size());
        for (size_t k = 0; k < ranges[t].texts.size(); ++k) {
            auto r = global.try_emplace(ranges[t].texts[k], static_cast<uint32_t>(texts.size()));
            if (r.second) {
                if (texts.size() == max_symbols) return false;  // not low-cardinality
                texts.push_back(ranges[t].texts[k]);
            }
            to_global[t][k] = r.first->second;
        }
    }
    if (texts.empty()) return false;
    // Smaller or not at all: every row's code at the widest code's size, plus the
    // symbols in the header, must undercut the strings.
    uint64_t symbol_bytes = 0;
    for (std::string_view t : texts) symbol_bytes += t.size() + 3;
    const uint64_t enum_bytes = valid_rows * varint_len(static_cast<uint64_t>(texts.size() - 1) << 1) + symbol_bytes;
    if (enum_bytes >= plain_bytes) return false;
    // Symbols in byte order, so the same values always make the same schema.
    std::vector<uint32_t> order(texts.size());
    for (uint32_t k = 0; k < order.size(); ++k) order[k] = k;
    std::sort(order.begin(), order.end(), [&](uint32_t x, uint32_t y) { return texts[x] < texts[y]; });
    std::vector<uint32_t> rank(texts.size());
    symbols.reserve(texts.size());
    for (uint32_t r = 0; r < order.size(); ++r) {
        rank[order[r]] = r;
        symbols.emplace_back(texts[order[r]]);
    }

    // 4. Codes, by physical range (a value no non-null row uses gets 0 and is never read).
    codes.resize(dl);
    run_tasks(phys_tasks, std::min(threads, phys_tasks), [&](size_t t, size_t) {
        const uint32_t first = static_cast<uint32_t>(t * kEnumChunk);
        const uint32_t end = std::min<uint32_t>(dl, first + kEnumChunk);
        const std::vector<uint32_t>& g = to_global[t];
        for (uint32_t j = first; j < end; ++j) codes[j] = used[j] ? rank[g[local[j]]] : 0;
    });
    return true;
}

std::string schema_json(const std::vector<FieldSpec>& fields, const std::string& record_name) {
    std::vector<FieldPlan> plans;
    std::vector<std::string> types;
    return build_schema(fields, record_name, plans, types);
}

std::string schema_json(const std::vector<FieldSpec>& fields, const std::string& record_name,
                        std::vector<FieldPlan>& plans) {
    std::vector<std::string> types;
    return build_schema(fields, record_name, plans, types);
}

// Per-worker encoding state, reused across blocks and batches: one block's
// uncompressed payload and per-row cursors, the compressed scratch, and the codec
// contexts (created on first use).
struct BlockEncoder {
    std::vector<uint8_t>  block;
    std::vector<uint8_t>  zbuf;
    std::vector<uint8_t*> cursor;
    ::libdeflate_compressor* deflater = nullptr;
    ZSTD_CCtx*               zctx = nullptr;

    BlockEncoder() = default;
    BlockEncoder(const BlockEncoder&) = delete;
    BlockEncoder& operator=(const BlockEncoder&) = delete;
    ~BlockEncoder() {
        if (deflater != nullptr) libdeflate_free_compressor(deflater);
        if (zctx != nullptr) ZSTD_freeCCtx(zctx);
    }

    // Appends `long size` then the payload compressed with `codec` to `out`. Each branch
    // reserves room for the size varint, the payload and the 16-byte sync the caller
    // appends next, so `out` is allocated once per block (growing it by appends copied
    // the whole block a second time — measured 10% on the uncompressed path).
    static void reserve_for(std::vector<uint8_t>& out, size_t payload) { out.reserve(out.size() + 10 + payload + 16); }

    void compress(WriteCodec codec, const uint8_t* payload, size_t n, std::vector<uint8_t>& out) {
        switch (codec) {
            case WriteCodec::Null:
                reserve_for(out, n);
                append_long(out, static_cast<int64_t>(n));
                append_bytes(out, payload, n);
                return;
            case WriteCodec::Deflate: {
                if (deflater == nullptr) {
                    deflater = libdeflate_alloc_compressor(6);  // zlib's default level
                    if (deflater == nullptr) throw std::bad_alloc();
                }
                const size_t bound = libdeflate_deflate_compress_bound(deflater, n);
                zbuf.resize(bound);
                const size_t z = libdeflate_deflate_compress(deflater, payload, n, zbuf.data(), bound);
                if (z == 0) throw std::runtime_error("Avro writer: deflate compression failed");
                reserve_for(out, z);
                append_long(out, static_cast<int64_t>(z));
                append_bytes(out, zbuf.data(), z);
                return;
            }
            case WriteCodec::Snappy: {
                // Avro's snappy framing: the raw snappy block, then the big-endian CRC32 of
                // the UNCOMPRESSED bytes (the reader verifies it, avro_container.cpp).
                zbuf.resize(snappy::MaxCompressedLength(n) + 4);
                size_t z = 0;
                snappy::RawCompress(reinterpret_cast<const char*>(payload), n,
                                    reinterpret_cast<char*>(zbuf.data()), &z);
                const uint32_t crc = rugo::compression::crc32_update(0, payload, n);
                uint8_t* t = zbuf.data() + z;
                t[0] = static_cast<uint8_t>(crc >> 24);
                t[1] = static_cast<uint8_t>(crc >> 16);
                t[2] = static_cast<uint8_t>(crc >> 8);
                t[3] = static_cast<uint8_t>(crc);
                reserve_for(out, z + 4);
                append_long(out, static_cast<int64_t>(z + 4));
                append_bytes(out, zbuf.data(), z + 4);
                return;
            }
            case WriteCodec::Zstandard: {
                if (zctx == nullptr) {
                    zctx = ZSTD_createCCtx();
                    if (zctx == nullptr) throw std::bad_alloc();
                }
                const size_t bound = ZSTD_compressBound(n);
                zbuf.resize(bound);
                const size_t z = ZSTD_compressCCtx(zctx, zbuf.data(), bound, payload, n, ZSTD_CLEVEL_DEFAULT);
                if (ZSTD_isError(z))
                    throw std::runtime_error(std::string("Avro writer: zstandard compression failed: ") + ZSTD_getErrorName(z));
                reserve_for(out, z);
                append_long(out, static_cast<int64_t>(z));
                append_bytes(out, zbuf.data(), z);
                return;
            }
            case WriteCodec::Bzip2: {
                // One Avro block = one complete bzip2 stream. libbzip2's buffer API takes
                // 32-bit lengths; a block is cut at block_bytes, but one row can be larger.
                if (n > UINT_MAX - 600 || n / 100 > UINT_MAX / 102)
                    throw std::runtime_error("Avro writer: a bzip2 block would exceed 4 GiB");
                // Worst case per the libbzip2 manual: 1% larger plus 600 bytes.
                unsigned int cap = static_cast<unsigned int>(n + n / 100 + 600);
                zbuf.resize(cap);
                take_bz_internal_error();  // per thread: this worker's own record
                const int rc = BZ2_bzBuffToBuffCompress(reinterpret_cast<char*>(zbuf.data()), &cap,
                                                        const_cast<char*>(reinterpret_cast<const char*>(payload)),
                                                        static_cast<unsigned int>(n), 9, 0, 0);
                const int internal = take_bz_internal_error();
                if (internal != 0)
                    throw std::runtime_error("Avro writer: bzip2 internal error " + std::to_string(internal) + " in libbzip2");
                if (rc == BZ_MEM_ERROR) throw std::bad_alloc();
                if (rc != BZ_OK) throw std::runtime_error("Avro writer: bzip2 compression failed (code " + std::to_string(rc) + ")");
                reserve_for(out, cap);
                append_long(out, static_cast<int64_t>(cap));
                append_bytes(out, zbuf.data(), cap);
                return;
            }
        }
        throw std::logic_error("Avro writer: a codec parse_write_codec does not admit");
    }
};

AvroWriter::AvroWriter(std::vector<FieldSpec> fields, const WriteOptions& opts)
    : fields_(std::move(fields)), opts_(opts) {
    if (opts_.block_bytes == 0) refuse("block_bytes must be positive");
    schema_json_ = build_schema(fields_, opts_.record_name, plans_, types_);
    threads_ = opts_.max_threads ? opts_.max_threads
                                 : std::max<size_t>(1, std::thread::hardware_concurrency());
    if (!opts_.have_sync) {
        std::random_device rd;
        for (int i = 0; i < 16; i += 4) {
            const uint32_t r = rd();
            std::memcpy(opts_.sync + i, &r, 4);
        }
    }
    emit_header();
}

AvroWriter::~AvroWriter() = default;

void AvroWriter::emit_header() {
    static constexpr uint8_t kMagic[4] = {'O', 'b', 'j', 1};
    std::vector<uint8_t> h;
    append_bytes(h, kMagic, 4);
    append_long(h, 3);  // one map block of three entries
    append_string(h, "avro.schema");
    append_string(h, schema_json_);
    append_string(h, "avro.codec");
    append_string(h, codec_name(opts_.codec));
    append_string(h, "rugo.created_by");
    append_string(h, RUGO_PARQUET_CREATED_BY);
    append_long(h, 0);  // end of the map
    append_bytes(h, opts_.sync, 16);
    chunks_.push_back(std::move(h));
}

void AvroWriter::check_fields(const std::vector<FieldSpec>& fields) const {
    if (fields.size() != fields_.size())
        refuse("this batch has " + std::to_string(fields.size()) + " columns, the first batch had " +
               std::to_string(fields_.size()) + " (every batch must share the first batch's schema)");
    for (size_t i = 0; i < fields.size(); ++i)
        if (fields[i].name != fields_[i].name)
            refuse("column " + std::to_string(i) + " is '" + fields[i].name + "' in this batch but '" + fields_[i].name +
                   "' in the first batch (every batch must share the first batch's schema)");
    std::vector<FieldPlan> plans;
    std::vector<std::string> types;
    build_schema(fields, opts_.record_name, plans, types);
    for (size_t i = 0; i < fields.size(); ++i) {
        const FieldSpec& f = fields[i];
        if (f.type != fields_[i].type || types[i] != types_[i] || !(plans[i] == plans_[i]))
            refuse("column '" + f.name + "' is " + type_name(f.type) + " written as " + types[i] + " in this batch but " +
                   type_name(fields_[i].type) + " written as " + types_[i] +
                   " in the first batch (every batch must share the first batch's schema)");
    }
}

BlockEncoder& AvroWriter::encoder(size_t worker) {
    return *encoders_[worker];
}

void AvroWriter::write_batch(const std::vector<FieldSpec>& fields, const WriteColumn* cols, uint32_t nrows) {
    check_fields(fields);
    for (size_t c = 0; c < fields_.size(); ++c) {
        const DrakenVector* v = cols[c].vec;
        if (v == nullptr) refuse("column '" + fields_[c].name + "' has no vector");
        if (v->type != fields_[c].type)
            refuse("column '" + fields_[c].name + "' is " + type_name(v->type) + " in this batch but " +
                   type_name(fields_[c].type) + " in the schema (every batch must share the first batch's schema)");
        if (v->length != nrows)
            refuse("column '" + fields_[c].name + "' has " + std::to_string(v->length) + " rows, the batch " +
                   std::to_string(nrows));
        if (draken_type_is_string_storage(v->type)) check_string_storage(fields_[c], v, nrows);
        if (v->type == DRAKEN_ARRAY) check_array_chain(fields_[c], cols[c]);
        if (!plans_[c].branch && column_has_nulls(v))
            refuse("column '" + fields_[c].name + "' is written as required (no nulls) but this batch has a null");
        if (static_cast<Enc>(plans_[c].enc) == Enc::Enum) {
            if (cols[c].enum_codes == nullptr) refuse("column '" + fields_[c].name + "' is an enum but has no codes");
        } else if (cols[c].enum_codes != nullptr) {
            refuse("column '" + fields_[c].name + "' has enum codes but is not an enum");
        }
    }
    if (nrows == 0) return;

    // Length pass, by row range: every column adds its per-row encoded size.
    row_len_.assign(nrows, 0);
    uint64_t* row_len = row_len_.data();
    const size_t len_tasks = (static_cast<size_t>(nrows) + kLengthChunkRows - 1) / kLengthChunkRows;
    run_tasks(len_tasks, std::min(threads_, len_tasks), [&](size_t t, size_t) {
        const uint32_t first = static_cast<uint32_t>(t * kLengthChunkRows);
        const uint32_t count = std::min<uint32_t>(kLengthChunkRows, nrows - first);
        for (size_t c = 0; c < fields_.size(); ++c) length_dispatch(plans_[c], cols[c], first, count, row_len);
    });

    // Cut rows into blocks of at least block_bytes (a single larger row is its own block),
    // at most kMaxBlockRows rows. The cuts depend only on the lengths, so the file is the
    // same whatever the thread count.
    struct Cut { uint32_t first, count; uint64_t bytes; };
    std::vector<Cut> cuts;
    uint32_t first = 0;
    uint64_t acc = 0;
    for (uint32_t i = 0; i < nrows; ++i) {
        acc += row_len[i];
        const uint32_t count = i + 1 - first;
        if (acc >= opts_.block_bytes || count == kMaxBlockRows || i + 1 == nrows) {
            cuts.push_back(Cut{first, count, acc});
            first = i + 1;
            acc = 0;
        }
    }

    // Encode + compress every block, on up to threads_ workers, each into its own chunk;
    // the chunks are appended in file order.
    const size_t workers = std::min(threads_, cuts.size());
    while (encoders_.size() < workers) encoders_.push_back(std::make_unique<BlockEncoder>());
    std::vector<std::vector<uint8_t>> out(cuts.size());
    run_tasks(cuts.size(), workers, [&](size_t b, size_t w) {
        encode_block(encoder(w), cols, cuts[b].first, cuts[b].count, cuts[b].bytes, out[b]);
    });
    for (std::vector<uint8_t>& chunk : out) chunks_.push_back(std::move(chunk));
    rows_written_ += nrows;
    blocks_written_ += cuts.size();
}

void AvroWriter::encode_block(BlockEncoder& enc, const WriteColumn* cols, uint32_t first, uint32_t count,
                              uint64_t payload_bytes, std::vector<uint8_t>& out) const {
    const uint64_t* row_len = row_len_.data();
    enc.block.resize(payload_bytes);
    enc.cursor.resize(count);
    uint8_t* p = enc.block.data();
    for (uint32_t r = 0; r < count; ++r) {
        enc.cursor[r] = p;
        p += row_len[first + r];
    }
    for (size_t c = 0; c < fields_.size(); ++c) write_dispatch(plans_[c], cols[c], first, count, enc.cursor.data());

    // Every row's cursor must now sit exactly at the next row's start: a disagreement
    // between an encoder's len and put would otherwise corrupt the block silently.
    p = enc.block.data();
    for (uint32_t r = 0; r < count; ++r) {
        p += row_len[first + r];
        if (enc.cursor[r] != p) throw std::logic_error("Avro writer: a row's encoded size disagrees with its length pass");
    }

    out.clear();
    append_long(out, static_cast<int64_t>(count));
    enc.compress(opts_.codec, enc.block.data(), enc.block.size(), out);
    append_bytes(out, opts_.sync, 16);
}

std::vector<std::vector<uint8_t>> AvroWriter::take_pending() {
    std::vector<std::vector<uint8_t>> out;
    out.swap(chunks_);
    return out;
}

}  // namespace rugo::avro
