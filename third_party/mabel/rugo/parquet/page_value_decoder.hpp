#pragma once

#include "page_decode_context.hpp"

// Forward declaration
struct DecodedColumn;

namespace rugo::parquet {

// Decode values from a single page
// Called by parallel page decode tasks
// Handles RLE level decoding, dictionary expansion, and plain value decoding
void decode_page_values(const PageDecodeContext& ctx,
                        DecodedColumn& result);

}  // namespace rugo::parquet
