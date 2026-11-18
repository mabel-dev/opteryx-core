// RUGO-1 spike, size probe only: what the page decompressors (needed by
// preview/validate) add to the bundle. Exposes rugo's DecompressIntoRaw as-is.
#include <cstdint>
#include <exception>
#include "compression.hpp"

extern "C" int rugo_decompress(int32_t codec, const uint8_t *in, uint32_t n,
                               uint8_t *out, uint32_t out_n) {
  try {
    using namespace rugo::compression;
    DecompressIntoRaw(in, n, out_n, CodecFromInt(codec), out);
    return 0;
  } catch (const std::exception &) {
    return 1;
  }
}
