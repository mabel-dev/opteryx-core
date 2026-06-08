#include "csv_scan.hpp"
#include <cstring>
#include <future>
#include "BS_thread_pool.hpp"

namespace rugo::_csv {

std::vector<CsvMarkerPosition> scan_csv_markers(
    const uint8_t*        data,
    size_t                length,
    const CsvParseContext& ctx)
{
    std::vector<CsvMarkerPosition> markers;
    markers.reserve(length / 32);
    scan_structural_csv(data, length, ctx, [&](uint32_t pos, CsvMarkerType type) {
        markers.push_back({pos, type});
    });
    return markers;
}

// Walk the marker stream tracking quote FSM state to find \n positions that occur
// outside quoted fields. Only those are safe to use as thread-range boundaries.
std::vector<uint32_t> find_safe_splits(
    const uint8_t*                        data,
    size_t                                length,
    const CsvParseContext&                ctx,
    const std::vector<CsvMarkerPosition>& input_markers)
{
    std::vector<CsvMarkerPosition> owned;
    const std::vector<CsvMarkerPosition>* markers = &input_markers;
    if (input_markers.empty() && length > 0) {
        owned = scan_csv_markers(data, length, ctx);
        markers = &owned;
    }

    std::vector<uint32_t> safe;

    enum class F { UNQUOTED, QUOTED, ESCAPE_IN_QUOTED, DOUBLE_QUOTE_PENDING };
    F state = F::UNQUOTED;

    for (const auto& m : *markers) {
        switch (state) {
            case F::UNQUOTED:
                if (m.type == CsvMarkerType::NEWLINE) {
                    safe.push_back(m.position);
                } else if (m.type == CsvMarkerType::QUOTE) {
                    state = F::QUOTED;
                }
                // CR, DELIMITER, BACKSLASH: don't affect quote state in unquoted context
                break;

            case F::QUOTED:
                if (m.type == CsvMarkerType::BACKSLASH) {
                    state = F::ESCAPE_IN_QUOTED;
                } else if (m.type == CsvMarkerType::QUOTE) {
                    state = F::DOUBLE_QUOTE_PENDING;
                }
                // NEWLINE/DELIMITER inside a quoted field: skip (NOT safe splits)
                break;

            case F::ESCAPE_IN_QUOTED:
                state = F::QUOTED;  // next marker was literal; back to quoted
                break;

            case F::DOUBLE_QUOTE_PENDING:
                if (m.type == CsvMarkerType::QUOTE) {
                    state = F::QUOTED;           // "" → still inside quoted field
                } else if (m.type == CsvMarkerType::NEWLINE) {
                    state = F::UNQUOTED;
                    safe.push_back(m.position);  // closing quote was real, row ended
                } else {
                    state = F::UNQUOTED;          // closed by delimiter/CR/other
                }
                break;
        }
    }

    return safe;
}

// ---------------------------------------------------------------------------
// find_safe_splits_parallel — prefix-sum FSM
// ---------------------------------------------------------------------------

namespace {

// FSM state codes
constexpr uint8_t kUnquoted = 0;
constexpr uint8_t kQuoted   = 1;
constexpr uint8_t kEscape   = 2;
constexpr uint8_t kDqPend   = 3;

// Transition table [state][marker_type] -> new_state.
// CsvMarkerType ordinals: NEWLINE=0, CR=1, DELIMITER=2, QUOTE=3, BACKSLASH=4
constexpr uint8_t kFsmT[4][5] = {
    // UNQUOTED:  NL         CR         DELIM      QUOTE     BSLASH
    {kUnquoted, kUnquoted, kUnquoted, kQuoted,   kUnquoted},
    // QUOTED:    NL         CR         DELIM      QUOTE     BSLASH
    {kQuoted,   kQuoted,   kQuoted,   kDqPend,   kEscape  },
    // ESCAPE:    NL         CR         DELIM      QUOTE     BSLASH
    {kQuoted,   kQuoted,   kQuoted,   kQuoted,   kQuoted  },
    // DQPEND:    NL         CR         DELIM      QUOTE     BSLASH
    {kUnquoted, kUnquoted, kUnquoted, kQuoted,   kUnquoted},
};

// Transfer function for one chunk: for each possible initial state, the final
// state and the chunk-relative offsets of safe \n positions.
struct ChunkXfer {
    uint8_t               end[4];
    std::vector<uint32_t> safe[4];
};

static ChunkXfer process_one_chunk(
    const uint8_t*         chunk,
    size_t                 chunk_len,
    const CsvParseContext& ctx)
{
    // Materialise markers (chunk-relative positions)
    std::vector<std::pair<uint32_t, uint8_t>> markers;
    markers.reserve(chunk_len / 32);
    scan_structural_csv(chunk, chunk_len, ctx, [&](uint32_t pos, CsvMarkerType t) {
        markers.emplace_back(pos, static_cast<uint8_t>(t));
    });

    ChunkXfer x;
    for (uint8_t s0 = 0; s0 < 4; ++s0) {
        uint8_t s = s0;
        for (const auto& [pos, mt] : markers) {
            // Emit safe \n before transitioning: both UNQUOTED and DQPEND yield a
            // safe row boundary on NEWLINE.
            if (mt == 0 /* NEWLINE */ && (s == kUnquoted || s == kDqPend))
                x.safe[s0].push_back(pos);
            s = kFsmT[s][mt];
        }
        x.end[s0] = s;
    }
    return x;
}

}  // anonymous namespace

std::vector<uint32_t> find_safe_splits_parallel(
    const uint8_t*         data,
    size_t                 length,
    const CsvParseContext& ctx,
    size_t                 nt)
{
    if (nt <= 1 || length == 0)
        return find_safe_splits(data, length, ctx);

    // Divide body into nt equal byte-chunks (last chunk may be smaller).
    const size_t chunk_sz  = (length + nt - 1) / nt;
    size_t       actual_nt = 0;
    for (size_t off = 0; off < length; off += chunk_sz) ++actual_nt;

    std::vector<size_t> offsets(actual_nt);
    std::vector<size_t> lens(actual_nt);
    for (size_t i = 0; i < actual_nt; ++i) {
        offsets[i] = i * chunk_sz;
        lens[i]    = std::min(chunk_sz, length - offsets[i]);
    }

    // Parallel: scan + 4-way FSM for every chunk
    std::vector<ChunkXfer> xfers(actual_nt);
    {
        BS::thread_pool<> pool(nt);
        std::vector<std::future<void>> futs;
        futs.reserve(actual_nt);
        for (size_t i = 0; i < actual_nt; ++i) {
            futs.push_back(pool.submit_task([&, i]() {
                xfers[i] = process_one_chunk(data + offsets[i], lens[i], ctx);
            }));
        }
        for (auto& f : futs) f.get();
    }

    // Serial O(nt): prefix scan to resolve true initial state per chunk
    std::vector<uint8_t> true_init(actual_nt);
    true_init[0] = kUnquoted;
    for (size_t i = 1; i < actual_nt; ++i)
        true_init[i] = xfers[i - 1].end[true_init[i - 1]];

    // Collect safe \n positions in body-relative order
    std::vector<uint32_t> result;
    for (size_t i = 0; i < actual_nt; ++i) {
        const uint8_t       s0  = true_init[i];
        const uint32_t      off = static_cast<uint32_t>(offsets[i]);
        for (uint32_t pos : xfers[i].safe[s0])
            result.push_back(off + pos);
    }

    return result;
}

}  // namespace rugo::_csv
