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
};

// Parse a PageHeader from the given Thrift compact-protocol input stream.
// The stream pointer advances past the header on return.
PageHeader ParsePageHeader(TInput &in);
