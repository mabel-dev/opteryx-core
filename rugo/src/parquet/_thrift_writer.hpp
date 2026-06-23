#pragma once
// Thrift Compact Protocol WRITER for the rugo parquet writer.
//
// This is the structural inverse of `thrift.hpp` (the reader's Compact
// Protocol reader). It is deliberately self-contained and lives in its own
// namespace so it never clashes with the reader's global `ThriftType` enum if
// both headers end up in one translation unit.
//
// See docs/PARQUET_WRITER_DESIGN.md, Phase 0.
//
// Compact Protocol reference (must match thrift.hpp on the read side):
//   - varint: little-endian base-128, high bit = continuation
//   - signed ints: zigzag, then varint
//   - field header: (delta << 4) | type when 1 <= delta <= 15, else
//                   type byte followed by zigzag-varint absolute id
//   - bool fields: value folded into the type nibble (TRUE=1, FALSE=2)
//   - struct: field headers terminated by a single 0x00 STOP byte
//   - field ids are delta-encoded relative to the previous field WITHIN the
//     current struct; entering a nested struct saves/zeroes the running id.

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace rugo_pq_write {

// Compact Protocol type ids (the wire nibble values). These mirror the
// reader's `ThriftType` enum in thrift.hpp; kept local to this namespace.
enum CType : uint8_t {
  CT_STOP = 0,
  CT_BOOL_TRUE = 1,
  CT_BOOL_FALSE = 2,
  CT_BYTE = 3,
  CT_I16 = 4,
  CT_I32 = 5,
  CT_I64 = 6,
  CT_DOUBLE = 7,
  CT_BINARY = 8, // also STRING
  CT_LIST = 9,
  CT_SET = 10,
  CT_MAP = 11,
  CT_STRUCT = 12,
};

inline uint64_t ZigZagEncode64(int64_t n) {
  return (static_cast<uint64_t>(n) << 1) ^ static_cast<uint64_t>(n >> 63);
}

inline uint32_t ZigZagEncode32(int32_t n) {
  return (static_cast<uint32_t>(n) << 1) ^ static_cast<uint32_t>(n >> 31);
}

// A growable byte sink with the low-level Compact Protocol primitives.
struct TCompactWriter {
  std::vector<uint8_t> buf;

  // Running field id for the struct currently being written. Nested structs
  // push the parent id here and reset to 0; structEnd restores it.
  int16_t last_field_id = 0;
  std::vector<int16_t> id_stack;

  void reserve(size_t n) { buf.reserve(n); }
  size_t size() const { return buf.size(); }

  void writeByte(uint8_t b) { buf.push_back(b); }

  void writeBytes(const void *src, size_t n) {
    const uint8_t *p = static_cast<const uint8_t *>(src);
    buf.insert(buf.end(), p, p + n);
  }

  // ---- varint / zigzag ----

  void writeVarint(uint64_t v) {
    while (v >= 0x80) {
      buf.push_back(static_cast<uint8_t>(v) | 0x80);
      v >>= 7;
    }
    buf.push_back(static_cast<uint8_t>(v));
  }

  // ---- struct framing ----

  void structBegin() {
    id_stack.push_back(last_field_id);
    last_field_id = 0;
  }

  void structEnd() {
    writeByte(CT_STOP);
    if (id_stack.empty())
      throw std::runtime_error("thrift writer: structEnd without structBegin");
    last_field_id = id_stack.back();
    id_stack.pop_back();
  }

  // Write a field header for a non-bool field, advancing the delta state.
  void writeFieldHeader(uint8_t type, int16_t id) {
    int delta = static_cast<int>(id) - static_cast<int>(last_field_id);
    if (delta > 0 && delta <= 15) {
      writeByte(static_cast<uint8_t>((delta << 4) | type));
    } else {
      writeByte(type);
      writeVarint(ZigZagEncode32(static_cast<int32_t>(id)));
    }
    last_field_id = id;
  }

  // ---- typed field writers ----

  void writeBoolField(int16_t id, bool value) {
    uint8_t type = value ? CT_BOOL_TRUE : CT_BOOL_FALSE;
    int delta = static_cast<int>(id) - static_cast<int>(last_field_id);
    if (delta > 0 && delta <= 15) {
      writeByte(static_cast<uint8_t>((delta << 4) | type));
    } else {
      writeByte(type);
      writeVarint(ZigZagEncode32(static_cast<int32_t>(id)));
    }
    last_field_id = id;
  }

  void writeI16Field(int16_t id, int16_t value) {
    writeFieldHeader(CT_I16, id);
    writeVarint(ZigZagEncode32(value));
  }

  void writeI32Field(int16_t id, int32_t value) {
    writeFieldHeader(CT_I32, id);
    writeVarint(ZigZagEncode32(value));
  }

  void writeI64Field(int16_t id, int64_t value) {
    writeFieldHeader(CT_I64, id);
    writeVarint(ZigZagEncode64(value));
  }

  void writeDoubleField(int16_t id, double value) {
    writeFieldHeader(CT_DOUBLE, id);
    // Compact protocol writes doubles as 8 raw bytes, little-endian.
    uint64_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    for (int i = 0; i < 8; i++) {
      buf.push_back(static_cast<uint8_t>(bits & 0xFF));
      bits >>= 8;
    }
  }

  void writeBinaryField(int16_t id, const void *data, size_t len) {
    writeFieldHeader(CT_BINARY, id);
    writeVarint(static_cast<uint64_t>(len));
    writeBytes(data, len);
  }

  void writeStringField(int16_t id, std::string_view s) {
    writeBinaryField(id, s.data(), s.size());
  }

  // ---- list framing ----
  //
  // Caller writes a list header, then exactly `size` elements of `elem_type`.
  // For bool elements use writeListBoolElem; for structs call structBegin/End
  // per element; for scalars use the raw value writers below.
  void writeListHeader(uint8_t elem_type, uint32_t size) {
    if (size <= 14) {
      writeByte(static_cast<uint8_t>((size << 4) | elem_type));
    } else {
      writeByte(static_cast<uint8_t>(0xF0 | elem_type));
      writeVarint(size);
    }
  }

  // Raw (non-field) value writers for use inside list bodies.
  void writeListI32(int32_t v) { writeVarint(ZigZagEncode32(v)); }
  void writeListI64(int64_t v) { writeVarint(ZigZagEncode64(v)); }
  void writeListBoolElem(bool v) { writeByte(v ? CT_BOOL_TRUE : CT_BOOL_FALSE); }
  void writeListBinary(const void *data, size_t len) {
    writeVarint(static_cast<uint64_t>(len));
    writeBytes(data, len);
  }
  void writeListString(std::string_view s) {
    writeListBinary(s.data(), s.size());
  }
};

} // namespace rugo_pq_write
