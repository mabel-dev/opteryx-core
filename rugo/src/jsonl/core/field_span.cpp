#include "field_span.hpp"
#include "interpreter.hpp"
#include "structural_scan.hpp"
#include "value_parser.hpp"
#include <algorithm>
#include <map>
#include <cstring>
#include <cctype>
#include <utility>
#include <thread>
#include <future>
#include "BS_thread_pool.hpp"

namespace rugo::_jsonl {

// OrdinalPredictor implementation
std::vector<uint16_t> OrdinalPredictor::get_candidates(const std::string& key) const {
    auto it = histories.find(key);
    if (it == histories.end()) {
        return {};  // No history yet, no prediction
    }

    const auto& history = it->second;
    if (history.disabled) {
        return {};  // Prediction disabled for this key
    }

    // Count occurrences of each ordinal in the history
    std::map<uint16_t, uint8_t> ordinal_counts;
    std::map<uint16_t, uint8_t> ordinal_recency;  // Position in circular buffer (higher = more recent)

    for (size_t i = 0; i < HISTORY_SIZE; ++i) {
        uint16_t ord = history.ordinals[i];
        if (ord != 0xFFFF) {  // 0xFFFF means not found
            ordinal_counts[ord]++;
            ordinal_recency[ord] = (history.position >= i) ?
                                    (history.position - i) :
                                    (history.position + HISTORY_SIZE - i);
        }
    }

    // Build candidate list based on heuristics
    std::vector<uint16_t> candidates;

    // First: ordinals appearing 5+ times (very stable)
    for (const auto& [ord, count] : ordinal_counts) {
        if (count >= 5) {
            candidates.push_back(ord);
        }
    }

    // Second: ordinals appearing 3-4 times (reasonably stable), sorted by recency
    std::vector<uint16_t> secondary;
    for (const auto& [ord, count] : ordinal_counts) {
        if (count >= 3 && count < 5) {
            secondary.push_back(ord);
        }
    }
    std::sort(secondary.begin(), secondary.end(),
              [&ordinal_recency](uint16_t a, uint16_t b) {
                  return ordinal_recency[a] > ordinal_recency[b];
              });
    candidates.insert(candidates.end(), secondary.begin(), secondary.end());

    // If we have no candidates (entropy), return empty and let caller brute force
    if (candidates.empty()) {
        return {};
    }

    return candidates;
}

void OrdinalPredictor::update_history(const std::string& key, uint16_t ordinal) {
    auto& history = histories[key];

    // Add ordinal to circular buffer
    history.ordinals[history.position] = ordinal;
    history.position = (history.position + 1) % HISTORY_SIZE;

    // Track brute-force fallbacks (ordinal = 0xFFFF means not found)
    // TODO: Phase 4 - count consecutive not-found to disable prediction
}

void OrdinalPredictor::disable_key(const std::string& key) {
    if (auto it = histories.find(key); it != histories.end()) {
        it->second.disabled = true;
    }
}

// Apply projection/predicates to the document map and move surviving records into
// result.all_records. Shared by the markers and fused interpret entry points; the
// caller sets result.bytes_consumed.
static void finalize_records(
    InterpreterResult& result,
    RecordSet& all_records,
    const uint8_t* buffer_data,
    const ParseContext& context,
    const std::vector<Predicate>& predicates) {

    // Fast path: no projection and no predicates — build_map already produced the final
    // arena (empties dropped, no filtering), so move it wholesale.
    if (predicates.empty() && context.projected_columns.empty()) {
        result.num_records_passed = all_records.num_records();
        result.all_records = std::move(all_records);
        return;
    }

    // General path: predicates were already applied inline by build_map (failing rows never
    // reach here); we re-resolve defensively and project to the requested column ORDER,
    // dropping predicate-only columns. Records hold only the wanted subset (few fields), so
    // the per-field scan is tiny. Output is built into a fresh flat arena.
    struct Col { const char* name; uint32_t len; uint8_t first; };
    auto make_cols = [](const auto& names) {
        std::vector<Col> v; v.reserve(names.size());
        for (const auto& n : names)
            v.push_back({n.data(), static_cast<uint32_t>(n.size()), n.empty() ? uint8_t(0) : uint8_t(n[0])});
        return v;
    };
    std::vector<Col> pcols; pcols.reserve(predicates.size());
    for (const auto& p : predicates)
        pcols.push_back({p.column.data(), static_cast<uint32_t>(p.column.size()),
                         p.column.empty() ? uint8_t(0) : uint8_t(p.column[0])});
    std::vector<Col> jcols = make_cols(context.projected_columns);

    auto find = [&](const RecordView& rec, const Col& c) -> const FieldSpan* {
        for (const auto& f : rec)
            if (f.key_width == c.len && buffer_data[f.key_start] == c.first &&
                std::memcmp(buffer_data + f.key_start, c.name, c.len) == 0)
                return &f;
        return nullptr;
    };

    RecordSet& out = result.all_records;
    out.offsets.clear();
    out.offsets.push_back(0);
    out.malformed = all_records.malformed;
    out.malformed_pos = all_records.malformed_pos;
    out.spans.reserve(all_records.spans.size());
    const size_t nrec = all_records.num_records();
    for (size_t r = 0; r < nrec; ++r) {
        const RecordView rec = all_records[r];

        bool passes = true;
        for (size_t i = 0; i < predicates.size(); ++i) {
            const FieldSpan* f = find(rec, pcols[i]);
            if (f == nullptr || !evaluate_predicate(buffer_data, *f, predicates[i])) {
                passes = false;
                break;
            }
        }
        if (!passes) continue;

        if (context.projected_columns.empty()) {
            for (const auto& f : rec) out.spans.push_back(f);  // predicates only — keep all cols
        } else {
            for (size_t i = 0; i < context.projected_columns.size(); ++i) {
                const FieldSpan* f = find(rec, jcols[i]);
                if (f != nullptr) out.spans.push_back(*f);
            }
        }
        // A record that passed predicates is kept even if none of the projected columns
        // are present on it (all-null row) — dropping it here would desync every column's
        // row count from the others (see rugo #jsonl-single-col-projection-drop).
        out.offsets.push_back(static_cast<uint32_t>(out.spans.size()));
        ++result.num_records_passed;
    }
}

// Markers-based entry: build_map over a pre-materialised marker array.
InterpreterResult interpret_jsonl(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const std::vector<MarkerPosition>& markers,
    const ParseContext& context,
    OrdinalPredictor& /*predictor*/) {

    InterpreterResult result;
    if (buffer_length == 0) { result.bytes_consumed = 0; return result; }

    // Predicate literals are parsed to int64/float64 ONCE here — not per row evaluated.
    // interpret_jsonl runs once per newline-range (interpret_jsonl_threaded calls it once
    // per thread chunk, typically single-digit count), so this is O(threads), never O(rows).
    std::vector<Predicate> prepared_predicates = context.predicates;
    for (auto& p : prepared_predicates) prepare_predicate(p);

    // Minimal-extent projection: when columns/predicates are named, build the map for ONLY
    // the projected ∪ predicate columns (exact bytes, no hashing) and stop scanning each
    // record once they are found. With nothing named, build the full data-blind map.
    // Predicate filtering and final column ordering happen afterwards in finalize_records.
    std::vector<WantedColumn> wanted_cols;
    MapProjection projbundle;
    const MapProjection* proj_ptr = nullptr;
    if (!context.projected_columns.empty() || !prepared_predicates.empty()) {
        auto find_col = [&](const char* n, size_t l) -> int {
            for (size_t k = 0; k < wanted_cols.size(); ++k)
                if (wanted_cols[k].len == l && std::memcmp(wanted_cols[k].name, n, l) == 0)
                    return static_cast<int>(k);
            return -1;
        };
        for (const auto& c : context.projected_columns)
            if (find_col(c.data(), c.size()) < 0)
                wanted_cols.push_back({c.data(), static_cast<uint32_t>(c.size()),
                                       c.empty() ? uint8_t(0) : uint8_t(c[0]), -1});
        // A predicate column joins the wanted set (reusing an existing projected entry)
        // and carries its predicate index for inline evaluation.
        for (size_t i = 0; i < prepared_predicates.size(); ++i) {
            const std::string& pc = prepared_predicates[i].column;
            int k = find_col(pc.data(), pc.size());
            if (k >= 0) { if (wanted_cols[k].pred_idx < 0) wanted_cols[k].pred_idx = static_cast<int>(i); }
            else wanted_cols.push_back({pc.data(), static_cast<uint32_t>(pc.size()),
                                        pc.empty() ? uint8_t(0) : uint8_t(pc[0]), static_cast<int>(i)});
        }

        // Wide-projection guard. The minimal-extent projection runs an O(num_wanted)
        // memcmp-gate on every key of every record, so its cost scales as
        // num_wanted × fields_per_row. For a pure projection that covers a large fraction
        // of a wide row, the full data-blind map (one pass, no per-key gate) is cheaper —
        // let finalize_records do the projection afterwards. The guard does NOT apply when
        // there are predicates: a predicate short-circuits failing rows inline (record_dead
        // → skip the tail), so the gate only runs to completion on rows that pass — the
        // N×M blow-up never materialises and inline pushdown is the bigger win. Field count
        // is estimated from the first record's COLON markers (interior/nested/string colons
        // only inflate it, biasing conservatively toward keeping the projection).
        bool wide_projection = false;
        if (prepared_predicates.empty()) {
            size_t first_record_fields = 0;
            for (const auto& m : markers) {
                if (m.marker_type == static_cast<uint8_t>(MarkerType::NEWLINE)) break;
                if (m.marker_type == static_cast<uint8_t>(MarkerType::COLON)) ++first_record_fields;
            }
            wide_projection =
                first_record_fields > 0 && wanted_cols.size() * 2 > first_record_fields;
        }

        if (!wide_projection) {
            projbundle.columns    = &wanted_cols;
            projbundle.num_wanted = wanted_cols.size();
            projbundle.predicates = &prepared_predicates;
            proj_ptr = &projbundle;
        }
    }

    auto all_records = build_map(buffer_data, buffer_length, markers, proj_ptr);

    // bytes_consumed = byte after the last newline (backward scan — newline near the end).
    result.bytes_consumed = 0;
    for (size_t i = markers.size(); i-- > 0; ) {
        if (markers[i].marker_type == static_cast<uint8_t>(MarkerType::NEWLINE)) {
            result.bytes_consumed = markers[i].position + 1;
            break;
        }
    }
    if (result.bytes_consumed == 0 && all_records.num_records() > 0) result.bytes_consumed = buffer_length;

    finalize_records(result, all_records, buffer_data, context, prepared_predicates);
    return result;
}

// Multithreaded entry: split the buffer into newline-aligned ranges and run
// scan + interpret on each in parallel, then merge the per-range records in order.
// All threads share the one read-only buffer; FieldSpan positions are absolute, so
// the merged records reference that single buffer (no per-chunk copies). max_threads
// == 0 means "use hardware_concurrency".
InterpreterResult interpret_jsonl_threaded(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const ParseContext& context,
    OrdinalPredictor& predictor,
    size_t max_threads) {

    InterpreterResult result;
    if (buffer_length == 0) { result.bytes_consumed = 0; return result; }

    // Adaptive masking: the masked scan drops in-string structurals (fewer FSM steps) but
    // costs ~1.4× scan, so it only nets out at high in-string density (stringified-JSON-ish
    // fields). Sample the head once and decide; correctness is identical either way (the FSM
    // handles escapes when unmasked; unescaping at extract is independent of this choice).
    const size_t sample = std::min<size_t>(buffer_length, static_cast<size_t>(256) << 10);
    const bool use_masked = sample_instring_density(buffer_data, sample) >= 0.40;

    size_t hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 1;
    size_t nt = std::min(hw, max_threads ? max_threads : hw);

    // Don't over-split: aim for at least a few MB of work per thread so the
    // per-task overhead and the serial merge don't dominate.
    const size_t MIN_CHUNK = static_cast<size_t>(4) << 20;  // 4 MB
    size_t max_chunks = std::max<size_t>(1, buffer_length / MIN_CHUNK);
    nt = std::min(nt, max_chunks);

    if (nt <= 1) {
        // Small input — single-threaded scan + interpret.
        auto markers = scan_structural_markers(buffer_data, buffer_length, use_masked);
        return interpret_jsonl(buffer_data, buffer_length, markers, context, predictor);
    }

    // Newline-aligned ranges. Each range ends just after a newline, so every range
    // holds complete records and the next range starts at a record boundary.
    std::vector<std::pair<size_t, size_t>> ranges;
    ranges.reserve(nt);
    size_t start = 0;
    for (size_t i = 1; i < nt && start < buffer_length; ++i) {
        size_t target = buffer_length * i / nt;
        if (target <= start) continue;
        size_t p = target;
        while (p < buffer_length && buffer_data[p] != '\n') ++p;
        if (p >= buffer_length) break;  // no more newlines; last range takes the rest
        ranges.push_back({start, p + 1});
        start = p + 1;
    }
    if (start < buffer_length) ranges.push_back({start, buffer_length});

    const size_t nc = ranges.size();
    std::vector<InterpreterResult> partial(nc);

    {
        BS::thread_pool<> pool(nt);
        std::vector<std::future<void>> futs;
        futs.reserve(nc);
        for (size_t c = 0; c < nc; ++c) {
            futs.push_back(pool.submit_task([&, c]() {
                const size_t s = ranges[c].first;
                const size_t e = ranges[c].second;
                // Scan this range with ABSOLUTE positions into the shared buffer.
                std::vector<MarkerPosition> markers;
                markers.reserve((e - s) / 3);
                const uint8_t* lut = structural_lut();
                auto emit = [&](uint32_t pos, uint8_t ch) {
                    markers.push_back(MarkerPosition(static_cast<uint32_t>(pos + s),
                                                     static_cast<MarkerType>(lut[ch] - 1)));
                };
                if (use_masked) scan_structural_masked(buffer_data + s, e - s, emit);
                else            scan_structural(buffer_data + s, e - s, emit);
                OrdinalPredictor local_pred;  // interpret does not use it; keep thread-local
                partial[c] = interpret_jsonl(buffer_data, buffer_length, markers, context, local_pred);
            }));
        }
        for (auto& f : futs) f.get();
    }

    // Merge in chunk order: concatenate each range's flat arena (offsets rebased).
    size_t total_spans = 0, total_recs = 0;
    for (const auto& p : partial) { total_spans += p.all_records.spans.size(); total_recs += p.all_records.num_records(); }
    result.all_records.spans.reserve(total_spans);
    result.all_records.offsets.reserve(total_recs + 1);
    for (auto& p : partial) {
        result.all_records.append(p.all_records);
        result.num_records_passed += p.num_records_passed;
    }
    result.bytes_consumed = buffer_length;
    return result;
}

}  // namespace rugo::_jsonl
