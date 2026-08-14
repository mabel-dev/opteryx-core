#pragma once
// decode_primitives.hpp
// Low-level, header-only byte readers and varint helpers used throughout the
// Parquet decoder.  All functions are static inline so that every translation
// unit that includes this header gets its own copy without linker conflicts.

#include <cstdint>
#include <cstring>

// ---------------------------------------------------------------------------
// Little-endian integer / float readers
// ---------------------------------------------------------------------------

static inline int32_t ReadLE32(const uint8_t *p) {
  return (int32_t)p[0] | ((int32_t)p[1] << 8) | ((int32_t)p[2] << 16) |
         ((int32_t)p[3] << 24);
}

static inline int64_t ReadLE64(const uint8_t *p) {
  return (int64_t)p[0] | ((int64_t)p[1] << 8) | ((int64_t)p[2] << 16) |
         ((int64_t)p[3] << 24) | ((int64_t)p[4] << 32) |
         ((int64_t)p[5] << 40) | ((int64_t)p[6] << 48) |
         ((int64_t)p[7] << 56);
}

// Unsigned reads (E33): identical bit construction to ReadLE32/ReadLE64, but the
// unsigned return type means a later widen to a larger width zero-extends instead
// of sign-extends. Use these whenever the column's Parquet IntType annotation says
// isSigned=false — using the signed readers there corrupts the value (a raw byte
// pattern with the high bit set decodes as negative instead of as the true, larger
// unsigned magnitude).
static inline uint32_t ReadLE32U(const uint8_t *p) {
  return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
         ((uint32_t)p[3] << 24);
}

static inline uint64_t ReadLE64U(const uint8_t *p) {
  return (uint64_t)p[0] | ((uint64_t)p[1] << 8) | ((uint64_t)p[2] << 16) |
         ((uint64_t)p[3] << 24) | ((uint64_t)p[4] << 32) |
         ((uint64_t)p[5] << 40) | ((uint64_t)p[6] << 48) |
         ((uint64_t)p[7] << 56);
}

static inline float ReadFloat32(const uint8_t *p) {
  uint32_t bits = ReadLE32(p);
  float value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

static inline double ReadFloat64(const uint8_t *p) {
  uint64_t bits = (uint64_t)ReadLE64(p);
  double value;
  std::memcpy(&value, &bits, sizeof(value));
  return value;
}

// ---------------------------------------------------------------------------
// INT96 (deprecated physical type) → int64 nanoseconds since the Unix epoch
// ---------------------------------------------------------------------------
// Wire layout, 12 bytes, as written by Impala/Hive and by pyarrow under
// use_deprecated_int96_timestamps: 8 bytes little-endian UNSIGNED
// nanoseconds-since-midnight, then 4 bytes little-endian INT32 Julian day
// number. The Unix epoch is Julian day 2440588. The Parquet spec assigns INT96
// exactly one meaning — a nanosecond timestamp — so the conversion is total,
// not a heuristic.
//
// Returns false (and leaves *out untouched) when the encoded instant cannot be
// represented as int64 nanoseconds. days * 86400e9 overflows int64 outside
// ±106751 days (~±292 years around 1970), so a corrupt or out-of-range Julian
// day must be rejected rather than silently wrapped to a plausible-looking
// wrong instant. Callers turn false into a decode error.
static inline bool Int96ToUnixNanos(const uint8_t *p, int64_t *out) {
  const uint64_t nanos_of_day = ReadLE64U(p);
  const int32_t julian_day = ReadLE32(p + 8);
  const int64_t days = (int64_t)julian_day - 2440588LL;
  if (days > 106751LL || days < -106751LL) return false;
  if (nanos_of_day > (uint64_t)INT64_MAX) return false;
  const int64_t day_nanos = days * 86400000000000LL;
  const int64_t nod = (int64_t)nanos_of_day;
  if (day_nanos > INT64_MAX - nod) return false;
  *out = day_nanos + nod;
  return true;
}

// ---------------------------------------------------------------------------
// Varint readers
// ---------------------------------------------------------------------------

// Unsigned LEB128 varint.
static inline uint64_t ReadUnsignedVarint(const uint8_t *&ptr,
                                          const uint8_t *end) {
  if (ptr >= end) return 0;
  uint64_t result = 0;
  int shift = 0;
  while (ptr < end && shift < 64) {
    uint8_t byte = *ptr++;
    result |= ((uint64_t)(byte & 0x7F)) << shift;
    if ((byte & 0x80) == 0) return result;
    shift += 7;
  }
  return 0;
}

// ZigZag-encoded signed varint.
static inline int64_t ReadZigZagVarint(const uint8_t *&ptr,
                                       const uint8_t *end) {
  if (ptr >= end) return 0;
  uint64_t result = 0;
  int shift = 0;
  while (ptr < end && shift < 64) {
    uint8_t byte = *ptr++;
    result |= ((uint64_t)(byte & 0x7F)) << shift;
    if ((byte & 0x80) == 0) {
      // ZigZag decode: (n >>> 1) ^ -(n & 1)
      return (int64_t)((result >> 1) ^ -(result & 1));
    }
    shift += 7;
  }
  return 0;
}
