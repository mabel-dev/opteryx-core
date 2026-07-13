#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

struct TInput {
  const uint8_t *p;
  const uint8_t *end;

  // Checked read — used at structure boundaries where EOF is meaningful.
  uint8_t readByte() {
    if (__builtin_expect(p >= end, 0))
      throw std::runtime_error("EOF");
    return *p++;
  }

  // Unchecked read — for use inside inner loops when the outer caller has
  // already validated that enough bytes remain.
  uint8_t readByteUnchecked() { return *p++; }
};

enum ThriftType {
  T_STOP = 0,
  T_BOOL_TRUE = 1,
  T_BOOL_FALSE = 2,
  T_BOOL = 3,
  T_BYTE = 4,
  T_I16 = 6,
  T_I32 = 8,
  T_I64 = 10,
  T_DOUBLE = 11,
  T_STRING = 12,
  T_STRUCT = 13,
  T_MAP = 14,
  T_SET = 15,
  T_LIST = 16,
};

// ------------------- Varint / ZigZag -------------------

// Optimized ReadVarint: fast path for the 1-byte and 2-byte cases (covers
// ~95% of calls in parquet footers — small field deltas, list sizes, and
// integers that fit in 7 or 14 bits).  Falls through to a loop for larger
// values.  Avoids the shift-increment and count-check overhead in the loop.
inline uint64_t ReadVarint(TInput &in) {
  if (__builtin_expect(in.p >= in.end, 0))
    throw std::runtime_error("EOF");
  uint8_t b = *in.p++;
  if (__builtin_expect(!(b & 0x80), 1))
    return b;                               // fast path: 1 byte

  if (__builtin_expect(in.p >= in.end, 0))
    throw std::runtime_error("EOF");
  uint64_t result = (uint64_t)(b & 0x7F);
  b = *in.p++;
  if (__builtin_expect(!(b & 0x80), 1))
    return result | ((uint64_t)b << 7);     // fast path: 2 bytes

  // 3-10 byte varints (rare for footer data).
  result |= (uint64_t)(b & 0x7F) << 7;
  int shift = 14;
  int count = 0;
  while (true) {
    if (__builtin_expect(count++ > 8, 0))
      throw std::runtime_error("Varint too long");
    b = in.readByte();
    result |= (uint64_t)(b & 0x7F) << shift;
    if (!(b & 0x80)) break;
    shift += 7;
  }
  return result;
}

inline int64_t ZigZagDecode(uint64_t n) { return (n >> 1) ^ -(int64_t)(n & 1); }

inline int64_t ReadI64(TInput &in) { return ZigZagDecode(ReadVarint(in)); }

inline int32_t ReadI32(TInput &in) {
  return (int32_t)ZigZagDecode(ReadVarint(in));
}

inline std::string ReadString(TInput &in) {
  uint64_t len = ReadVarint(in);
  uint64_t avail = (uint64_t)(in.end - in.p);
  if (__builtin_expect(len > avail, 0))
    throw std::runtime_error("Invalid string length");
  std::string s((const char *)in.p, (size_t)len);
  in.p += len;
  return s;
}

// Read a length-prefixed binary/string and return a view into the input
// buffer. The view is valid only as long as the underlying buffer outlives it.
inline std::string_view ReadStringView(TInput &in) {
  uint64_t len = ReadVarint(in);
  uint64_t avail = (uint64_t)(in.end - in.p);
  if (__builtin_expect(len > avail, 0))
    throw std::runtime_error("Invalid string length");
  std::string_view sv((const char *)in.p, (size_t)len);
  in.p += len;
  return sv;
}

// Skip a length-prefixed binary/string field without allocating.
inline void SkipBinary(TInput &in) {
  uint64_t len = ReadVarint(in);
  uint64_t avail = (uint64_t)(in.end - in.p);
  if (__builtin_expect(len > avail, 0))
    throw std::runtime_error("Invalid string length");
  in.p += len;
}

// Thrift Compact Protocol inlines a struct-field bool's value into the field
// header's type nibble itself (T_BOOL_TRUE=1 / T_BOOL_FALSE=2) — there is no
// separate value byte on the wire for it (unlike list/set/map bool elements,
// which are one byte each). Takes the field header's type, not the stream.
static inline bool ReadBool(uint8_t field_type) { return field_type == T_BOOL_TRUE; }

// ------------------- Compact Protocol Structs -------------------

struct FieldHeader {
  int16_t id;
  uint8_t type;
};

// Decode a field header (Thrift Compact Protocol).
// Hot path: delta-encoded header (modifier != 0) is a single byte — no varint.
inline FieldHeader ReadFieldHeader(TInput &in, int16_t &last_id) {
  if (__builtin_expect(in.p >= in.end, 0))
    throw std::runtime_error("EOF");
  uint8_t header = *in.p++;

  if (__builtin_expect(header == 0, 0))
    return {0, 0};   // STOP

  uint8_t type     = header & 0x0F;
  uint8_t modifier = header >> 4;

  if (__builtin_expect(modifier != 0, 1)) {
    // Fast path: delta from previous field id (no extra varint).
    last_id = static_cast<int16_t>(last_id + modifier);
    return {last_id, type};
  }

  // Slow path: absolute field id encoded as a zigzag varint.
  int16_t field_id = static_cast<int16_t>(ZigZagDecode(ReadVarint(in)));
  last_id = field_id;
  return {field_id, type};
}

// Compact list header
struct ListHeader {
  uint8_t elem_type;
  uint32_t size;
};

inline ListHeader ReadListHeader(TInput &in) {
  uint8_t first = in.readByte();
  uint32_t size  = first >> 4;
  uint8_t  elem_type = first & 0x0F;
  if (__builtin_expect(size == 15, 0)) {
    size = (uint32_t)ReadVarint(in);
  }
  return {elem_type, size};
}

inline void SkipField(TInput &in, uint8_t type) {
  switch (type) {
  case 0:
    return; // STOP
  case 1:
  case 2:
    return; // BOOL (encoded in the field header type byte, nothing extra)
  case 3:
    in.readByte();
    return; // BYTE
  case 4:
    (void)ReadI32(in);
    return; // I16 zigzag
  case 5:
    (void)ReadI32(in);
    return; // I32 zigzag
  case 6:
    (void)ReadI64(in);
    return; // I64 zigzag
  case 7: { // DOUBLE
    if ((size_t)(in.end - in.p) < 8)
      throw std::runtime_error("EOF");
    in.p += 8;
    return;
  }
  case 8:
    SkipBinary(in);
    return; // BINARY/STRING — no alloc needed for skip
  case 9: { // LIST
    auto lh = ReadListHeader(in);
    for (uint32_t i = 0; i < lh.size; i++) {
      if (lh.elem_type == 1 || lh.elem_type == 2) {
        in.readByte();
      } else {
        SkipField(in, lh.elem_type);
      }
    }
    return;
  }
  case 10: { // SET
    auto lh = ReadListHeader(in);
    for (uint32_t i = 0; i < lh.size; i++) {
      if (lh.elem_type == 1 || lh.elem_type == 2) {
        in.readByte();
      } else {
        SkipField(in, lh.elem_type);
      }
    }
    return;
  }
  case 11: { // MAP
    uint8_t first = in.readByte();
    uint32_t size = first >> 4;
    if (size == 0)
      return;
    if (size == 15)
      size = (uint32_t)ReadVarint(in);
    uint8_t types    = in.readByte();
    uint8_t key_type = types >> 4;
    uint8_t val_type = types & 0x0F;

    for (uint32_t i = 0; i < size; i++) {
      if (key_type == 1 || key_type == 2) in.readByte();
      else SkipField(in, key_type);
      if (val_type == 1 || val_type == 2) in.readByte();
      else SkipField(in, val_type);
    }
    return;
  }
  case 12: { // STRUCT
    int16_t last = 0;
    while (true) {
      auto fh = ReadFieldHeader(in, last);
      if (fh.type == 0) break;
      SkipField(in, fh.type);
    }
    return;
  }
  default:
    in.readByte(); // be forgiving: skip one byte
    return;
  }
}

static void SkipStruct(TInput &in) {
  int16_t last_id = 0;
  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0) break;
    SkipField(in, fh.type);
  }
}
