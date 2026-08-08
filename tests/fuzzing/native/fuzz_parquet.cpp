// Fuzz rugo's Parquet reader: arbitrary bytes in, no crash out.
//
// Parquet files come from storage and from other writers, so the footer is a
// thrift-encoded structure that a hostile or merely broken producer controls
// completely — offsets, lengths, encodings, compressed sizes and the page
// headers they point at. The decoder trusts those to size buffers and to walk
// pages, which is where an out-of-bounds read lives if a bound is checked in
// the wrong order or not at all.
//
// Two entry points, because they fail differently:
//   * ReadParquetMetadataFromBuffer — the footer/thrift parse, reached by any
//     read at all, including a plan-time statistics fetch that never decodes a
//     single page.
//   * ReadParquet — the full decode, which additionally exercises page
//     headers, the encodings, and every decompressor.
//
// The oracle is the sanitizer, not the return value: rejecting a malformed file
// is the reader working. A crash, a hang, or an ASan/UBSan report is a failure.

#include <cstddef>
#include <cstdint>
#include <exception>

#include "parquet/decode.hpp"
#include "parquet/metadata.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // These readers report malformed input by throwing, so the harness has to
    // absorb that to keep fuzzing. This is NOT flow control hiding a failure:
    // the sanitizer, not this catch, decides whether the input was handled
    // safely, and a memory error fires before any exception could be thrown.
    try {
        ReadParquetMetadataFromBuffer(data, size);
    } catch (const std::exception&) {
    } catch (...) {
    }

    try {
        ReadParquet(data, size);
    } catch (const std::exception&) {
    } catch (...) {
    }

    return 0;
}
