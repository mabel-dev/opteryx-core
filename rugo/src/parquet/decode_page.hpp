#pragma once
// decode_page.hpp
// Parquet page-header parsing.
//
// `ParsePageHeader` reads a Parquet page header from a Thrift input stream
// and extracts the fields needed by the column decoder.

#include "thrift.hpp"
#include <cstdint>

// Relevant fields extracted from a Parquet PageHeader Thrift struct.
struct PageHeader {
  int32_t page_type            = -1;  // 0=DATA_PAGE, 2=DICTIONARY_PAGE, …
  int32_t uncompressed_page_size = 0;
  int32_t compressed_page_size   = 0;
  int32_t num_values             = 0;
  // Parquet Encoding enum: PLAIN=0, PLAIN_DICTIONARY=2, RLE=3,
  // BIT_PACKED=4 (deprecated), DELTA_BINARY_PACKED=5, DELTA_LENGTH_BYTE_ARRAY=6,
  // DELTA_BYTE_ARRAY=7, RLE_DICTIONARY=8, BYTE_STREAM_SPLIT=9.
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
