// Fuzz rugo's CSV scanner: arbitrary bytes in, no crash out.
//
// The interesting code is the quote finite-state machine and the parallel
// safe-split discovery built on it. `find_safe_splits_parallel` scans chunks
// independently, runs the FSM four times per chunk (once per possible entry
// state), then composes the true states serially — so a buffer that ends inside
// a quoted field, or whose quotes never balance, is precisely the input that
// makes the composition disagree with reality. Splits are then used as byte
// ranges handed to threads, which turns a wrong offset into an out-of-bounds
// read rather than a wrong answer.
//
// The oracle is the sanitizer, not the return value.

#include <cstddef>
#include <cstdint>
#include <exception>
#include <vector>

#include "csv/core/csv_parse_context.hpp"
#include "csv/core/csv_scan.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    using namespace rugo::_csv;

    // The delimiter is part of the attack surface, not a constant: a file
    // declaring an unusual one drives a different path through the scanner.
    CsvParseContext ctx;
    if (size > 0) {
        ctx.delimiter = data[0];
    }

    try {
        std::vector<CsvMarkerPosition> markers = scan_csv_markers(data, size, ctx);
        find_safe_splits(data, size, ctx, markers);
        // Serial and parallel discovery must both survive; the parallel one has
        // the chunk-composition logic the serial one does not.
        for (size_t threads : {size_t(1), size_t(2), size_t(8)}) {
            find_safe_splits_parallel(data, size, ctx, threads);
        }
    } catch (const std::exception&) {
    } catch (...) {
    }

    return 0;
}
