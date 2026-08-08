// Can the slot array be laid out better before a compressor ever sees it?
//
// STRING_SLOTS is 10.5 MB of the 35 MB measured across TPC-H and reaches only
// 0.43x under zstd-1 — much worse than the arena's 0.25x. That is suspicious,
// because a slot is more structured than text, not less.
//
// A 16-byte long slot is four u32 fields with nothing in common:
//
//   length        small, low entropy, often near-constant within a column
//   prefix        first 4 bytes of the string, big-endian — text-like
//   hash32        DEAD. Always zero since E37 removed the equality fast-reject.
//   arena_offset  monotonically increasing in arena order
//
// Interleaved, every 4 bytes changes distribution, which is close to the worst
// input a general compressor can be handed. This measures four layouts on real
// slot arrays, all through the same zstd-1 the format already uses:
//
//   as-is       what skene writes today
//   stripped    the dead hash32 removed (inline slots keep all 16 bytes)
//   planed      the fields separated into runs of like-with-like
//   planed+d    planed, with offsets delta-encoded before compression
//
// Nothing here changes the format. It sizes the prize.
//
// NOT part of libskene.a. See bench/README.md.

#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "skene/file_io.h"
#include "skene/reader.h"

#include "zstd.h"

using namespace skene;

namespace {

uint64_t zstd1(const std::vector<uint8_t>& plain) {
    if (plain.empty()) return 0;
    std::vector<uint8_t> out(ZSTD_compressBound(plain.size()));
    const size_t got = ZSTD_compress(out.data(), out.size(), plain.data(),
                                     plain.size(), 1);
    return ZSTD_isError(got) ? plain.size() : got;
}

void append(std::vector<uint8_t>* into, const void* bytes, size_t n) {
    const uint8_t* at = static_cast<const uint8_t*>(bytes);
    into->insert(into->end(), at, at + n);
}

struct Tally {
    uint64_t slots = 0, plain = 0;
    uint64_t as_is = 0, stripped = 0, planed = 0, planed_delta = 0;
};

void measure_column(const std::string& name, const DrakenVector& v, Tally* tally) {
    const DrakenStringArena* arena = static_cast<const DrakenStringArena*>(v.data);
    if (arena == nullptr || v.data_length == 0) return;

    const uint32_t n = v.data_length;
    const DrakenStringSlot* slots = arena->slots;

    // ── as-is: the 16-byte slot array, verbatim ──
    std::vector<uint8_t> as_is;
    append(&as_is, slots, static_cast<size_t>(n) * sizeof(DrakenStringSlot));

    // ── stripped: drop the dead hash32 from LONG slots only ──
    //
    // An inline slot uses all 16 bytes for payload, so the 4 dead bytes exist
    // only on long slots. Which a slot is follows from its length, which is
    // written first — so a reader rebuilds this without a flag.
    std::vector<uint8_t> stripped;
    for (uint32_t i = 0; i < n; ++i) {
        append(&stripped, &slots[i].inl.length, 4);
        if (str_is_inline(&slots[i])) {
            append(&stripped, slots[i].inl.data, 12);
        } else {
            append(&stripped, &slots[i].ext.prefix, 4);
            append(&stripped, &slots[i].ext.arena_offset, 4);
        }
    }

    // ── planed: like with like ──
    std::vector<uint32_t> lengths, prefixes, offsets;
    std::vector<uint8_t>  inline_bytes;
    for (uint32_t i = 0; i < n; ++i) {
        lengths.push_back(slots[i].inl.length);
        if (str_is_inline(&slots[i])) {
            append(&inline_bytes, slots[i].inl.data, 12);
        } else {
            prefixes.push_back(slots[i].ext.prefix);
            offsets.push_back(slots[i].ext.arena_offset);
        }
    }

    std::vector<uint8_t> planed;
    append(&planed, lengths.data(), lengths.size() * 4);
    append(&planed, prefixes.data(), prefixes.size() * 4);
    append(&planed, offsets.data(), offsets.size() * 4);
    planed.insert(planed.end(), inline_bytes.begin(), inline_bytes.end());

    // ── planed + delta on offsets ──
    //
    // Offsets are the one plane with a known shape: strings were appended to the
    // arena in slot order, so successive offsets differ by the previous string's
    // length. Wrapping unsigned subtraction, the same rule the format already
    // uses for delta sections.
    std::vector<uint32_t> deltas(offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i)
        deltas[i] = i == 0 ? offsets[0] : offsets[i] - offsets[i - 1];

    std::vector<uint8_t> planed_delta;
    append(&planed_delta, lengths.data(), lengths.size() * 4);
    append(&planed_delta, prefixes.data(), prefixes.size() * 4);
    append(&planed_delta, deltas.data(), deltas.size() * 4);
    planed_delta.insert(planed_delta.end(), inline_bytes.begin(), inline_bytes.end());

    const uint64_t a = zstd1(as_is), b = zstd1(stripped);
    const uint64_t c = zstd1(planed), d = zstd1(planed_delta);
    const double base = static_cast<double>(as_is.size());

    std::printf("  %-16s %8u %10zu %8" PRIu64 " %6.2fx %8" PRIu64 " %6.2fx %8" PRIu64
                " %6.2fx %8" PRIu64 " %6.2fx\n",
                name.c_str(), n, as_is.size(),
                a, a / base, b, b / base, c, c / base, d, d / base);

    tally->slots += n;
    tally->plain += as_is.size();
    tally->as_is += a;
    tally->stripped += b;
    tally->planed += c;
    tally->planed_delta += d;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: slot_layout <file.skene> [more.skene ...]\n");
        return 1;
    }

    Tally tally;
    for (int a = 1; a < argc; ++a) {
        std::vector<uint8_t> bytes;
        Status st = read_file(argv[a], &bytes);
        if (!st.is_ok()) { std::fprintf(stderr, "%s\n", st.message().c_str()); return 1; }

        CxxMorsel morsel;
        st = read_morsel(bytes.data(), bytes.size(), 0, &morsel);
        if (!st.is_ok()) { std::fprintf(stderr, "%s\n", st.message().c_str()); return 1; }

        bool header = false;
        for (size_t c = 0; c < morsel.columns.size(); ++c) {
            if (!draken_type_is_string_storage(morsel.columns[c].view.type)) continue;
            if (!header) {
                std::printf("%s\n  %-16s %8s %10s %8s %7s %8s %7s %8s %7s %8s %7s\n",
                            argv[a], "column", "slots", "plain", "as-is", "", "strip",
                            "", "plane", "", "plane+d", "");
                header = true;
            }
            measure_column(morsel.names[c], morsel.columns[c].view, &tally);
        }
    }

    if (tally.plain == 0) { std::fprintf(stderr, "no string columns found\n"); return 1; }

    const double base = static_cast<double>(tally.plain);
    std::printf("\n%" PRIu64 " slots, %" PRIu64 " bytes of slot array\n",
                tally.slots, tally.plain);
    std::printf("  %-12s %12s %8s %s\n", "layout", "zstd-1", "ratio", "vs as-is");
    struct Row { const char* label; uint64_t bytes; };
    const Row rows[] = {{"as-is", tally.as_is}, {"stripped", tally.stripped},
                        {"planed", tally.planed}, {"planed+delta", tally.planed_delta}};
    for (const Row& row : rows)
        std::printf("  %-12s %12" PRIu64 " %7.2fx %+7.1f%%\n", row.label, row.bytes,
                    row.bytes / base,
                    100.0 * (static_cast<double>(row.bytes) - static_cast<double>(tally.as_is))
                        / static_cast<double>(tally.as_is));
    return 0;
}
