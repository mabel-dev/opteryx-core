// decode_page.cpp
// Implementation of Parquet page-header parsing.

#include "decode_page.hpp"

PageHeader ParsePageHeader(TInput &in) {
  PageHeader header;
  int16_t last_id = 0;

  while (true) {
    auto fh = ReadFieldHeader(in, last_id);
    if (fh.type == 0) break;

    switch (fh.id) {
    case 1:  // page_type
      header.page_type = ReadI32(in);
      break;

    case 2:  // uncompressed_page_size
      header.uncompressed_page_size = ReadI32(in);
      break;

    case 3:  // compressed_page_size
      header.compressed_page_size = ReadI32(in);
      break;

    case 5: {  // data_page_header (struct)
      int16_t dph_last_id = 0;
      while (true) {
        auto dph_fh = ReadFieldHeader(in, dph_last_id);
        if (dph_fh.type == 0) break;
        switch (dph_fh.id) {
        case 1:  // num_values
          header.num_values = ReadI32(in);
          break;
        case 2:  // encoding
          header.encoding = ReadI32(in);
          break;
        // fields 3/4 (definition/repetition level encodings) are not needed.
        default:
          SkipField(in, dph_fh.type);
          break;
        }
      }
      break;
    }

    case 7: {  // dictionary_page_header (struct)
      int16_t dph_last_id = 0;
      while (true) {
        auto dph_fh = ReadFieldHeader(in, dph_last_id);
        if (dph_fh.type == 0) break;
        switch (dph_fh.id) {
        case 1:  // num_values
          header.num_values = ReadI32(in);
          break;
        case 2:  // encoding
          header.encoding = ReadI32(in);
          break;
        case 3:  // is_sorted (dictionary entries in ascending order)
          if (dph_fh.type == T_BOOL_TRUE) {
            header.dictionary_is_sorted = true;
          } else if (dph_fh.type == T_BOOL_FALSE) {
            header.dictionary_is_sorted = false;
          } else {
            header.dictionary_is_sorted = ReadBool(dph_fh.type);
          }
          break;
        default:
          SkipField(in, dph_fh.type);
          break;
        }
      }
      break;
    }

    default:
      SkipField(in, fh.type);
      break;
    }
  }

  return header;
}
