// Fuzz skene's reader: arbitrary bytes in, no crash out.
//
// skene reads objects from storage, so its input is attacker-controllable in
// exactly the way a SQL string is not. The reader's own contract says why this
// matters more here than elsewhere: it "memcpys buffers and rebuilds absolute
// pointers from stored offsets, so continuing past a detected inconsistency is
// memory corruption rather than a wrong answer" (skene/status.h). Every
// structural claim a file makes about itself — extents, checksums, code bounds,
// arena invariants, array offset monotonicity — is a chance to get that wrong.
//
// The oracle is the sanitizer, not the return value. A `Status` rejection is a
// PASS: refusing a malformed file is the reader working. The only failures are
// a crash, a hang, or an ASan/UBSan report.

#include <cstddef>
#include <cstdint>

#include <vector>

#include "skene/format.h"
#include "skene/reader.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    // Footer parse. Cheap, and the gate every other read passes through.
    skene::FileMetadata metadata;
    skene::read_metadata(data, size, &metadata);

    // Full reconstruction, per row group. Validates and then builds real
    // buffers, so this is where a trusted-but-wrong offset turns into an
    // out-of-bounds write.
    //
    // Row group 0 always, because a file with none is malformed and the reader
    // must say so rather than fall through. Then whatever row group count the
    // input CLAIMS, bounded — a file declaring a thousand row groups it does not
    // contain is the interesting case, and it is reached by asking for the ones
    // past the end. The claimed count is taken from the parsed metadata rather
    // than from a fixed sweep so the fuzzer follows the file's own lie.
    CxxMorsel morsel;  // draken-owned type, global namespace
    skene::read_morsel(data, size, 0, &morsel);

    const size_t groups = metadata.row_groups.size() < 8 ? metadata.row_groups.size() : 8;
    for (uint32_t g = 1; g <= groups; ++g) {
        CxxMorsel other;
        skene::read_morsel(data, size, g, &other);
        skene::RowGroupMetadata detail;
        skene::read_row_group_metadata(data, size, g, &detail);
    }

    // The v3 RANGED path the engine runs: open from tail + footer, then the
    // directory blocks — planned through block 0, as the engine plans them —
    // then decode from the fetched ranges alone. The directory blocks are parsed
    // HERE, not at footer parse, so this is the surface a whole-buffer read
    // never reaches in the same order. Ranges point into the input: a planned
    // range the input does not hold is skipped, and the reader must then refuse
    // rather than read past what it was given.
    uint64_t footer_offset = 0;
    uint64_t footer_bytes = 0;
    if (size >= skene::kFileTailBytes &&
        skene::footer_extent(data + size - skene::kFileTailBytes, skene::kFileTailBytes,
                             static_cast<uint64_t>(size), &footer_offset, &footer_bytes)
            .is_ok() &&
        footer_offset + footer_bytes <= size) {
        skene::FileReader ranged;
        if (skene::open_reader_ranged(data + size - skene::kFileTailBytes,
                                      skene::kFileTailBytes, data + footer_offset,
                                      static_cast<size_t>(footer_bytes), footer_offset,
                                      static_cast<uint64_t>(size), &ranged)
                .is_ok()) {
            auto to_fetched = [&](const std::vector<skene::ByteRange>& plan,
                                  std::vector<skene::FetchedRange>* out) {
                for (const skene::ByteRange& r : plan)
                    if (r.offset <= size && r.bytes <= size - r.offset)
                        out->push_back(skene::FetchedRange{r.offset, r.bytes, data + r.offset});
            };
            std::vector<skene::ByteRange> plan;
            std::vector<skene::FetchedRange> fetched;
            if (skene::plan_directory_fetch(ranged, {}, true, skene::FetchPolicy{}, &plan)
                    .is_ok()) {
                to_fetched(plan, &fetched);
                if (skene::attach_directories(&ranged, {}, fetched).is_ok()) {
                    const size_t rgs = ranged.metadata().row_groups.size();
                    const uint32_t reads = rgs < 8 ? static_cast<uint32_t>(rgs) : 8u;
                    for (uint32_t g = 0; g < reads; ++g) {
                        std::vector<skene::FetchedRange> ranges = fetched;
                        if (skene::plan_fetch(ranged, {}, {g}, skene::FetchPolicy{}, &plan)
                                .is_ok())
                            to_fetched(plan, &ranges);
                        CxxMorsel from_ranges;
                        skene::read_morsel(ranged, g, skene::ReadOptions(), ranges,
                                           &from_ranges);
                    }
                }
            }
        }
    }

    // The remote-read path: the tail of an object plus a claimed total size.
    // `file_bytes` is what a remote store reported, so it does NOT have to
    // agree with the buffer in hand — feeding it a size that disagrees is the
    // realistic hostile case, not an artificial one.
    uint64_t offset = 0;
    uint64_t bytes = 0;
    skene::footer_extent(data, size, static_cast<uint64_t>(size), &offset, &bytes);
    if (size >= sizeof(uint64_t)) {
        uint64_t claimed = 0;
        __builtin_memcpy(&claimed, data, sizeof(claimed));
        skene::footer_extent(data, size, claimed, &offset, &bytes);
    }

    return 0;
}
