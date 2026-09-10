#pragma once
// src/cpp/pg/pg_decode.hpp — PostgreSQL binary-format cells -> Draken columns.
//
// One PgColumnDecoder per emitted column. The scan Source feeds it the cells of
// each DataRow (binary result format, see pg_client.hpp) and calls finish() at
// the morsel cut to take the column as a CxxColumn (VectorOwner-backed, built
// with the same nogil emitters the parquet and skene Sources use).
//
// The Draken type each column is emitted as comes from the PLAN (PgScanSpec):
// numeric lands as DECIMAL (int64) or DECIMAL128 depending on the precision the
// binder saw, json/jsonb as VARIANT, everything else as pg_oid_to_draken says.
// The decode itself is keyed on the OID the server reported, which the Source
// has already checked against the OID the binder saw.
//
// Unrepresentable values (NaN / infinite numerics, infinite dates or
// timestamps, a numeric with more fractional digits than the declared scale)
// fail the scan with the column named. Never coerced, never nulled.

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/vector_owner.h"
#include "logical_type.h"
#include "morsels/cxx_morsel.h"
#include "pg/pg_client.hpp"
#include "engine/native_varchar_pool_decode.hpp"  // consolidate_string_block

namespace opteryx::pg {

namespace detail {

inline uint16_t be16(const uint8_t* p) { return (uint16_t)((p[0] << 8) | p[1]); }
inline uint32_t be32(const uint8_t* p) {
    return ((uint32_t)p[0] << 24) | ((uint32_t)p[1] << 16) | ((uint32_t)p[2] << 8) | (uint32_t)p[3];
}
inline uint64_t be64(const uint8_t* p) { return ((uint64_t)be32(p) << 32) | be32(p + 4); }

// Postgres epoch (2000-01-01) relative to the Unix epoch.
constexpr int32_t kPgEpochDays = 10957;
constexpr int64_t kPgEpochMicros = 946684800000000LL;

inline __int128 pow10_i128(int e) {
    __int128 v = 1;
    for (int i = 0; i < e; i++) v *= 10;
    return v;
}

}  // namespace detail

class PgColumnDecoder {
public:
    void init(const std::string& name, uint32_t oid, DrakenType dtype, int precision, int scale,
              uint32_t capacity) {
        name_ = name;
        oid_ = oid;
        dtype_ = dtype;
        precision_ = precision;
        scale_ = scale;
        capacity_ = capacity;
        width_ = (uint32_t)draken_type_fixed_itemsize(dtype);
        is_string_ = (dtype == DRAKEN_VARCHAR || dtype == DRAKEN_VARBINARY ||
                      dtype == DRAKEN_NVARCHAR || dtype == DRAKEN_VARIANT);
        is_bool_ = (dtype == DRAKEN_BOOL);
        if (!is_string_ && !is_bool_ && width_ == 0)
            throw PgError("column '" + name_ + "': cannot emit Draken type " +
                          std::to_string((int)dtype) + " from a Postgres stream");
        reset();
    }

    uint32_t rows() const { return rows_; }

    void append_null() {
        ensure_buffers();
        if (is_string_) {
            DrakenStringSlot s;
            str_init_null(&s);
            slots_.push_back(s);
        } else if (!is_bool_) {
            std::memset(fixed_ + (size_t)rows_ * width_, 0, width_);
        }
        any_null_ = true;
        rows_++;
    }

    void append(const uint8_t* p, int32_t len) {
        ensure_buffers();
        switch (oid_) {
            case 16:
                if (len != 1) bad_length("bool");
                if (p[0]) bits_[rows_ >> 3] |= (uint8_t)(1u << (rows_ & 7));
                break;
            case 21: { if (len != 2) bad_length("int2"); int16_t v = (int16_t)detail::be16(p); put(&v); break; }
            case 23: { if (len != 4) bad_length("int4"); int32_t v = (int32_t)detail::be32(p); put(&v); break; }
            case 26: { if (len != 4) bad_length("oid");  uint32_t v = detail::be32(p); put(&v); break; }
            case 20: { if (len != 8) bad_length("int8"); int64_t v = (int64_t)detail::be64(p); put(&v); break; }
            case 700: { if (len != 4) bad_length("float4"); uint32_t u = detail::be32(p); put(&u); break; }
            case 701: { if (len != 8) bad_length("float8"); uint64_t u = detail::be64(p); put(&u); break; }
            case 1082: {
                if (len != 4) bad_length("date");
                const int32_t d = (int32_t)detail::be32(p);
                if (d == INT32_MAX || d == INT32_MIN)
                    throw PgError("column '" + name_ + "': an infinite date is not representable");
                const int32_t v = d + detail::kPgEpochDays;
                put(&v);
                break;
            }
            case 1114: case 1184: {
                if (len != 8) bad_length("timestamp");
                const int64_t us = (int64_t)detail::be64(p);
                if (us == INT64_MAX || us == INT64_MIN)
                    throw PgError("column '" + name_ + "': an infinite timestamp is not representable");
                const int64_t v = us + detail::kPgEpochMicros;
                put(&v);
                break;
            }
            case 1700: append_numeric(p, len); break;
            case 3802:
                if (len < 1 || p[0] != 1)
                    throw PgError("column '" + name_ + "': unsupported jsonb binary version");
                append_string(p + 1, (uint32_t)(len - 1));
                break;
            case 2950: {
                if (len != 16) bad_length("uuid");
                static const char* hexd = "0123456789abcdef";
                char buf[36]; int k = 0;
                for (int i = 0; i < 16; i++) {
                    if (i == 4 || i == 6 || i == 8 || i == 10) buf[k++] = '-';
                    buf[k++] = hexd[p[i] >> 4]; buf[k++] = hexd[p[i] & 15];
                }
                append_string((const uint8_t*)buf, 36);
                break;
            }
            default:
                // Remaining string-family OIDs (text, varchar, bpchar, name, "char",
                // bytea, json): raw bytes.
                if (!is_string_)
                    throw PgError("column '" + name_ + "': no decoder for Postgres type " + pg_oid_name(oid_));
                append_string(p, (uint32_t)len);
                break;
        }
        set_valid();
        rows_++;
    }

    // Take the staged rows as a column and reset for the next batch.
    void finish(CxxColumn& out) {
        ensure_buffers();
        uint8_t* validity = nullptr;
        if (any_null_) {
            const size_t nbytes = ((size_t)rows_ + 7u) >> 3;
            validity = (uint8_t*)draken_malloc(nbytes ? nbytes : 1);
            if (!validity) throw PgError("draken_malloc failed (validity)");
            std::memset(validity, 0, nbytes ? nbytes : 1);
            std::memcpy(validity, validity_.data(), std::min(nbytes, validity_.size()));
        }

        if (is_string_) {
            DrakenStringArena* sa = nullptr;
            uint8_t* block = opteryx::engine::consolidate_string_block(
                slots_.data(), rows_, arena_.data(), arena_.size(), dtype_, &sa);
            DrakenVector v = draken_vector_from_dense(sa, rows_, dtype_, validity);
            out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(block), OwnedBuffer<uint8_t>(validity));
            out.view = out.own->vec;
        } else if (is_bool_) {
            DrakenVector v = draken_vector_from_dense(bits_, rows_, DRAKEN_BOOL, validity);
            out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(bits_), OwnedBuffer<uint8_t>(validity));
            out.view = out.own->vec;
            bits_ = nullptr;
        } else {
            if (dtype_ == DRAKEN_DECIMAL || dtype_ == DRAKEN_DECIMAL128) materialize_decimals();
            DrakenVector v = draken_vector_from_dense(fixed_, rows_, dtype_, validity);
            out.own = std::make_shared<VectorOwner>(v, OwnedBuffer<void>(fixed_), OwnedBuffer<uint8_t>(validity));
            fixed_ = nullptr;
            if (dtype_ == DRAKEN_TIMESTAMP64) {
                LogicalType lt;
                lt.kind = LogicalKind::TIMESTAMP;
                lt.unit = TimestampUnit::MICROSECONDS;
                lt.offset_minutes = 0;
                out.own->logical_type = logical_type_intern(lt);
            } else if (dtype_ == DRAKEN_DECIMAL || dtype_ == DRAKEN_DECIMAL128) {
                LogicalType lt;
                lt.kind = LogicalKind::DECIMAL;
                lt.precision = (uint8_t)precision_;
                lt.scale = (uint8_t)scale_;
                out.own->logical_type = logical_type_intern(lt);
            }
            out.view = out.own->vec;
        }
        reset();
    }

    ~PgColumnDecoder() { release_buffers(); }

private:
    struct Dec { __int128 coef; int exp10; };

    [[noreturn]] void bad_length(const char* what) {
        throw PgError("column '" + name_ + "': malformed binary " + what + " value");
    }

    template <typename T>
    void put(const T* v) { std::memcpy(fixed_ + (size_t)rows_ * width_, v, sizeof(T)); }

    void set_valid() {
        const size_t byte = rows_ >> 3;
        if (validity_.size() <= byte) validity_.resize(byte + 1, 0);
        validity_[byte] |= (uint8_t)(1u << (rows_ & 7));
    }

    void append_string(const uint8_t* p, uint32_t len) {
        DrakenStringSlot s;
        if (len <= STR_INLINE_MAX) {
            draken_build_string_slot(&s, p, len, 0);
        } else {
            if (arena_.size() + len > 0xFFFFFFFFu)
                throw PgError("column '" + name_ + "': string arena exceeds 4 GB in one morsel");
            const uint32_t off = (uint32_t)arena_.size();
            arena_.insert(arena_.end(), p, p + len);
            draken_build_string_slot(&s, p, len, off);
        }
        slots_.push_back(s);
    }

    void append_numeric(const uint8_t* p, int32_t len) {
        if (len < 8) bad_length("numeric");
        const int16_t ndigits = (int16_t)detail::be16(p);
        const int16_t weight = (int16_t)detail::be16(p + 2);
        const uint16_t sign = detail::be16(p + 4);
        if (len != 8 + 2 * ndigits) bad_length("numeric");
        if (sign == 0xC000) throw PgError("column '" + name_ + "': numeric NaN is not representable");
        if (sign == 0xD000 || sign == 0xF000)
            throw PgError("column '" + name_ + "': an infinite numeric is not representable");
        __int128 coef = 0;
        for (int i = 0; i < ndigits; i++) coef = coef * 10000 + detail::be16(p + 8 + 2 * i);
        if (sign == 0x4000) coef = -coef;
        const int exp10 = ndigits > 0 ? 4 * (weight - ndigits + 1) : 0;
        decs_.push_back({coef, exp10});
        // The fixed slot is filled at finish(); keep the row index aligned.
    }

    // Rescale every staged numeric to the plan's scale and write the unscaled
    // integers into the fixed buffer (int64 for DECIMAL, int128 for DECIMAL128).
    void materialize_decimals() {
        size_t d = 0;
        for (uint32_t r = 0; r < rows_; r++) {
            const bool valid = (validity_.size() > (r >> 3)) && (validity_[r >> 3] & (1u << (r & 7)));
            __int128 v = 0;
            if (valid) {
                if (d >= decs_.size()) throw PgError("column '" + name_ + "': numeric staging out of step");
                const Dec& dec = decs_[d++];
                const int e = dec.exp10 + scale_;
                v = dec.coef;
                if (e >= 0) {
                    if (e > 38) throw PgError("column '" + name_ + "': numeric value exceeds DECIMAL(38) range");
                    v *= detail::pow10_i128(e);
                } else {
                    if (-e > 38 || v % detail::pow10_i128(-e) != 0)
                        throw PgError("column '" + name_ + "': numeric value has more fractional digits than the declared scale " +
                                      std::to_string(scale_));
                    v /= detail::pow10_i128(-e);
                }
                if (dtype_ == DRAKEN_DECIMAL && (v > (__int128)INT64_MAX || v < (__int128)INT64_MIN))
                    throw PgError("column '" + name_ + "': numeric value exceeds the int64 DECIMAL tier");
            }
            if (dtype_ == DRAKEN_DECIMAL) {
                const int64_t v64 = (int64_t)v;
                std::memcpy(fixed_ + (size_t)r * 8, &v64, 8);
            } else {
                std::memcpy(fixed_ + (size_t)r * 16, &v, 16);
            }
        }
        decs_.clear();
    }

    void ensure_buffers() {
        if (is_string_) return;
        if (is_bool_) {
            if (bits_ == nullptr) {
                const size_t nbytes = ((size_t)capacity_ + 7u) >> 3;
                bits_ = (uint8_t*)draken_malloc(nbytes ? nbytes : 1);
                if (!bits_) throw PgError("draken_malloc failed (bool)");
                std::memset(bits_, 0, nbytes ? nbytes : 1);
            }
            return;
        }
        if (fixed_ == nullptr) {
            const size_t nbytes = (size_t)capacity_ * width_;
            fixed_ = (uint8_t*)draken_malloc(nbytes ? nbytes : 1);
            if (!fixed_) throw PgError("draken_malloc failed (fixed)");
        }
    }

    void release_buffers() {
        if (fixed_) { draken_free(fixed_); fixed_ = nullptr; }
        if (bits_) { draken_free(bits_); bits_ = nullptr; }
    }

    void reset() {
        release_buffers();
        slots_.clear();
        arena_.clear();
        decs_.clear();
        validity_.clear();
        any_null_ = false;
        rows_ = 0;
    }

    std::string name_;
    uint32_t oid_ = 0;
    DrakenType dtype_ = DRAKEN_NULL;
    int precision_ = 0, scale_ = 0;
    uint32_t capacity_ = 0;
    uint32_t width_ = 0;
    bool is_string_ = false, is_bool_ = false;

    uint8_t* fixed_ = nullptr;   // draken_malloc'd, capacity_ * width_
    uint8_t* bits_ = nullptr;    // draken_malloc'd BOOL bitmap
    std::vector<DrakenStringSlot> slots_;
    std::vector<uint8_t> arena_;
    std::vector<Dec> decs_;
    std::vector<uint8_t> validity_;
    bool any_null_ = false;
    uint32_t rows_ = 0;
};

}  // namespace opteryx::pg
