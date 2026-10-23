#pragma once
// decode_page.hpp
// Parquet page-header parsing.
//
// `ParsePageHeader` reads a Parquet page header from a Thrift input stream
// and extracts the fields needed by the column decoder.

#include "thrift.hpp"
#include <cstdint>
#include <string>

// Relevant fields extracted from a Parquet PageHeader Thrift struct.
struct PageHeader {
  int32_t page_type            = -1;  // 0=DATA_PAGE, 2=DICTIONARY_PAGE, …
  int32_t uncompressed_page_size = 0;
  int32_t compressed_page_size   = 0;
  int32_t num_values             = 0;
  // Parquet Encoding enum: PLAIN=0, PLAIN_DICTIONARY=2, RLE=3,
  // BIT_PACKED=4 (deprecated), DELTA_BINARY_PACKED=5, DELTA_LENGTH_BYTE_ARRAY=6,
  // DELTA_BYTE_ARRAY=7, RLE_DICTIONARY=8, BYTE_STREAM_SPLIT=9, ALP=10.
  int32_t encoding               = 0;
  bool dictionary_is_sorted      = false;

  // DATA_PAGE_V2 (page_type == 3) fields. is_v2 is set only by the field-8
  // (data_page_header_v2) parser; for V1/dictionary pages it stays false and the
  // remaining V2 fields are unused. In V2 the repetition/definition levels are
  // stored UNCOMPRESSED at the front of the page with EXPLICIT byte lengths (no
  // 4-byte prefix), and only the values region is compressed (iff is_compressed).
  bool    is_v2                          = false;
  int32_t num_rows                       = 0;
  int32_t definition_levels_byte_length  = 0;
  int32_t repetition_levels_byte_length  = 0;
  bool    is_compressed                  = true;  // V2 default is TRUE when absent
};

// Parse a PageHeader from the given Thrift compact-protocol input stream.
// The stream pointer advances past the header on return.
PageHeader ParsePageHeader(TInput &in);

// THE value-encoding admission rule: why `encoding` cannot decode values of
// `physical_type` (metadata.cpp's names: "int32", "float64", "byte_array", ...),
// or nullptr when it can. File admission (CanDecode), chunk admission and the
// per-page check in both page walks all ask this one function, so they cannot
// drift. It is type-aware because an encoding read for the wrong type is not a
// no-op: its bytes decode as PLAIN into plausible garbage with success=true.
//   0 PLAIN, 2 PLAIN_DICTIONARY, 8 RLE_DICTIONARY   every type
//   3 RLE                                           boolean
//   5 DELTA_BINARY_PACKED                           int32, int64
//   6 DELTA_LENGTH_BYTE_ARRAY                       byte_array
//   7 DELTA_BYTE_ARRAY                              byte_array (the spec also
//                                                   allows FLBA; not decoded)
//   9 BYTE_STREAM_SPLIT                             int32, int64, float32,
//                                                   float64, fixed_len_byte_array
//  10 ALP                                           float32, float64
// 1 GROUP_VAR_INT (obsolete) and 4 BIT_PACKED (levels only) never encode values.
inline const char *ValueEncodingRefusal(int32_t encoding, const std::string &physical_type) {
  const std::string &t = physical_type;
  switch (encoding) {
  case 0: case 2: case 8:
    return nullptr;
  case 3:
    return t == "boolean" ? nullptr : "RLE values are only defined for boolean";
  case 5:
    return (t == "int32" || t == "int64") ? nullptr
           : "DELTA_BINARY_PACKED is only defined for int32 and int64";
  case 6:
    return t == "byte_array" ? nullptr
           : "DELTA_LENGTH_BYTE_ARRAY is only defined for byte_array";
  case 7:
    return t == "byte_array" ? nullptr
           : "DELTA_BYTE_ARRAY is only decoded for byte_array";
  case 9:
    return (t == "int32" || t == "int64" || t == "float32" || t == "float64" ||
            t == "fixed_len_byte_array") ? nullptr
           : "BYTE_STREAM_SPLIT is only defined for int32, int64, float32, float64 "
             "and fixed_len_byte_array";
  case 10:
    return (t == "float32" || t == "float64") ? nullptr
           : "ALP is only defined for float32 and float64";
  case 1:
    return "GROUP_VAR_INT is obsolete and never encodes values";
  case 4:
    return "BIT_PACKED encodes levels only, never values";
  default:
    return "unknown encoding";
  }
}

