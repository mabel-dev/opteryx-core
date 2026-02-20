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
  int32_t encoding               = 0;  // PLAIN=0, DELTA_BINARY_PACKED=4, …
};

// Parse a PageHeader from the given Thrift compact-protocol input stream.
// The stream pointer advances past the header on return.
PageHeader ParsePageHeader(TInput &in);
