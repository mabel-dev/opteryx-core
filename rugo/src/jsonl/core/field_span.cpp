#include "field_span.hpp"
#include "interpreter.hpp"
#include "nested_column.hpp"
#include "structural_scan.hpp"
#include "value_parser.hpp"
#include <algorithm>
#include <map>
#include <cstring>
#include <cctype>
#include <utility>
#include <thread>
#include <future>
#include <exception>
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
    // Resolved through ColumnKey (nested_column.hpp): a top-level column matches by key
    // bytes among slot-0 spans, a nested column by its slot.
    std::vector<ColumnKey> pcols; pcols.reserve(predicates.size());
    for (const auto& p : predicates) pcols.push_back(column_key(context, p.column));
    std::vector<ColumnKey> jcols; jcols.reserve(context.projected_columns.size());
    for (const auto& c : context.projected_columns) jcols.push_back(column_key(context, c));

    auto find = [&](const RecordView& rec, const ColumnKey& c) -> const FieldSpan* {
        for (const auto& f : rec)
            if (c.matches(buffer_data, f)) return &f;
        return nullptr;
    };

    // Per-projected-column ordinal cache. A bare `find` per column is O(fields), making the
    // projection loop below O(wanted x fields) per record — the dominant cost whenever
    // interpret_jsonl's wide_projection guard sends a full data-blind map here. Measured on
    // a 197MB/105-column file: explicitly projecting all 105 columns cost 37% MORE than
    // projecting nothing at all (91.6ms vs 66.7ms), for byte-identical output. NDJSON holds
    // key order stable across records, so remembering each column's last span index makes
    // the steady state one memcmp per column. Same ordinal-stability assumption — and the
    // same "first match wins only while layout is stable" caveat under duplicate keys in one
    // object — as extract_column's predictor in column_builder.cpp.
    constexpr uint32_t NO_ORD = 0xFFFFFFFFu;
    std::vector<uint32_t> col_ord(jcols.size(), NO_ORD);

    auto find_projected = [&](const RecordView& rec, size_t j) -> const FieldSpan* {
        const ColumnKey& c = jcols[j];
        const uint32_t hint = col_ord[j];
        if (hint < rec.size() && c.matches(buffer_data, rec[hint])) return &rec[hint];
        for (uint32_t i = 0; i < rec.size(); ++i) {
            if (c.matches(buffer_data, rec[i])) {
                col_ord[j] = i;
                return &rec[i];
            }
        }
        return nullptr;
    };

    RecordSet& out = result.all_records;
    out.offsets.clear();
    out.offsets.push_back(0);
    out.malformed = all_records.malformed;
    out.malformed_pos = all_records.malformed_pos;
    out.malformed_count = all_records.malformed_count;
    out.spans.reserve(all_records.spans.size());
    const size_t nrec = all_records.num_records();
    for (size_t r = 0; r < nrec; ++r) {
        const RecordView rec = all_records[r];

        bool passes = true;
        for (size_t i = 0; i < predicates.size(); ++i) {
            const FieldSpan* f = find(rec, pcols[i]);
            // An absent key is a NULL cell: it passes only the predicates that accept NULL.
            if (f == nullptr ? !predicate_accepts_absent(predicates[i])
                             : !evaluate_predicate(buffer_data, *f, predicates[i])) {
                passes = false;
                break;
            }
        }
        if (!passes) continue;

        if (context.projected_columns.empty()) {
            for (const auto& f : rec) out.spans.push_back(f);  // predicates only — keep all cols
        } else {
            for (size_t i = 0; i < context.projected_columns.size(); ++i) {
                const FieldSpan* f = find_projected(rec, i);
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
    OrdinalPredictor& /*predictor*/,
    size_t range_start) {

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
    // Predicates with no projection keep every field (MapProjection::keep_unwanted).
    // Predicate filtering and final column ordering happen afterwards in finalize_records.
    std::vector<WantedColumn> wanted_cols;
    std::vector<ColumnSpec> specs;                 // owns the bytes wanted_cols points into
    std::vector<const std::string*> wanted_names;  // full requested name per wanted column
    MapProjection projbundle;
    const MapProjection* proj_ptr = nullptr;
    if (!context.projected_columns.empty() || !prepared_predicates.empty()) {
        // Wanted set: projected columns, then predicate-only columns, de-duplicated by
        // their FULL name (`commit` and `commit->>'a'` are different columns that share a
        // key). `specs` owns the parsed key/sub bytes the WantedColumns point into; it is
        // sized up front so those pointers never move.
        specs.reserve(context.projected_columns.size() + prepared_predicates.size());
        auto find_col = [&](const std::string& name) -> int {
            for (size_t k = 0; k < wanted_names.size(); ++k)
                if (*wanted_names[k] == name) return static_cast<int>(k);
            return -1;
        };
        auto add_col = [&](const std::string& name, int pred_idx) {
            specs.push_back(parse_column_spec(name));
            const ColumnSpec& sp = specs.back();
            WantedColumn w{sp.key.data(), static_cast<uint32_t>(sp.key.size()),
                           sp.key.empty() ? uint8_t(0) : uint8_t(sp.key[0]), pred_idx};
            if (sp.nested) {
                w.sub       = sp.sub.data();
                w.sub_len   = static_cast<uint32_t>(sp.sub.size());
                w.sub_first = static_cast<uint8_t>(sp.sub[0]);
                w.slot      = nested_slot(context, name);
            }
            wanted_cols.push_back(w);
            wanted_names.push_back(&name);
        };
        for (const auto& c : context.projected_columns)
            if (find_col(c) < 0) add_col(c, -1);
        // A predicate column joins the wanted set (reusing an existing projected entry)
        // and carries its predicate index for inline evaluation.
        for (size_t i = 0; i < prepared_predicates.size(); ++i) {
            const std::string& pc = prepared_predicates[i].column;
            // A `->` column is JSON, not text: comparing its rendering with a literal is
            // not what SQL means by it. Refused rather than given a meaning here.
            if (parse_column_spec(pc).as_json)
                throw std::invalid_argument(
                    "read_jsonl: predicate on `->` column '" + pc +
                    "' is not supported; compare the `->>` text instead");
            const int k = find_col(pc);
            if (k >= 0) { if (wanted_cols[k].pred_idx < 0) wanted_cols[k].pred_idx = static_cast<int>(i); }
            else add_col(pc, static_cast<int>(i));
        }
        // Chain wanted columns that share a top-level key, so the walk matches the key once
        // (it takes the FIRST match) and serves the whole chain from one value.
        for (size_t k = 0; k < wanted_cols.size(); ++k)
            for (size_t j = k + 1; j < wanted_cols.size(); ++j)
                if (wanted_cols[j].len == wanted_cols[k].len &&
                    std::memcmp(wanted_cols[j].name, wanted_cols[k].name, wanted_cols[k].len) == 0) {
                    wanted_cols[k].next = static_cast<int>(j);
                    break;
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
        //
        // A NESTED column also keeps the projection: only the projected walk reads inside a
        // container, so the data-blind map would hand every nested column back as NULL.
        bool any_nested = false;
        for (const auto& sp : specs) any_nested |= sp.nested;
        bool wide_projection = false;
        if (prepared_predicates.empty() && !any_nested) {
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
            // No projection = every column: predicates-only must not narrow the map.
            projbundle.keep_unwanted = context.projected_columns.empty();
            proj_ptr = &projbundle;
        }
    }

    auto all_records = build_map(buffer_data, buffer_length, markers, proj_ptr, range_start);

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
    // holds whole lines. Any newline is a sound split, including a raw newline inside a
    // string (the JSONBench defect, tests/performance/jsonbench/README.md): build_map
    // judges every line on its own, so the two halves of such a record are each rejected
    // as malformed whichever range they land in.
    std::vector<std::pair<size_t, size_t>> ranges;
    ranges.reserve(nt);
    size_t start = 0;
    for (size_t i = 1; i < nt && start < buffer_length; ++i) {
        size_t target = buffer_length * i / nt;
        if (target <= start) continue;
        size_t p = target;
        while (p < buffer_length && buffer_data[p] != '\n') ++p;
        if (p >= buffer_length) break;  // no more newlines; last range takes the rest
        const size_t split = p + 1;
        if (split >= buffer_length) break;  // rest of the buffer is one final range
        ranges.push_back({start, split});
        start = split;
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
                // [s, e) is this range: build_map judges its first and last lines against
                // the range bounds, not the whole buffer's.
                partial[c] = interpret_jsonl(buffer_data, e, markers, context, local_pred, s);
            }));
        }
        // Drain EVERY future before propagating: a range can throw (a predicate literal
        // that does not fit a value — evaluate_predicate), and rethrowing on the first
        // while other ranges still run would leave them reading this frame's locals
        // (ranges, partial, context) after it unwinds.
        std::exception_ptr first_exc;
        for (auto& f : futs) {
            try {
                f.get();
            } catch (...) {
                if (!first_exc) first_exc = std::current_exception();
            }
        }
        if (first_exc) std::rethrow_exception(first_exc);
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
