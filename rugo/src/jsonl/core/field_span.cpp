#include "field_span.hpp"
#include "interpreter.hpp"
#include "jsonl_reader.hpp"   // choose_prefilter_plan, prefilter_plan_segments
#include "nested_column.hpp"
#include "value_parser.hpp"
#include <algorithm>
#include <cstring>
#include <cctype>
#include <utility>
#include <thread>
#include <future>
#include <exception>
#include "BS_thread_pool.hpp"

namespace rugo::_jsonl {

namespace {

// build_columns over [range_start, buffer_length), with the copied columns decided by the
// caller (head_copy_columns) — over the whole input, so every chunk decides the same.
InterpreterResult interpret_range(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const ParseContext& context,
    const std::vector<std::string>& columns,
    size_t range_start,
    const std::vector<LineSpan>* lines,
    const std::vector<uint8_t>& copy) {

    InterpreterResult result;
    if (buffer_length == 0) { result.bytes_consumed = 0; return result; }

    // Predicate literals are parsed to int64/float64 ONCE here — not per row evaluated.
    // interpret_jsonl runs once per newline-range (interpret_jsonl_threaded calls it once
    // per thread chunk, typically single-digit count), so this is O(threads), never O(rows).
    std::vector<Predicate> prepared_predicates = context.predicates;
    for (auto& p : prepared_predicates) prepare_predicate(p);

    // Wanted set: every output column (capture slot = its output index), then each
    // predicate column not already among them (capture slots past the outputs). `specs`
    // owns the parsed key/sub bytes the WantedColumns point into; it is sized up front so
    // those pointers never move.
    std::vector<WantedColumn> wanted_cols;
    std::vector<ColumnSpec> specs;
    std::vector<const std::string*> wanted_names;  // full requested name per wanted column
    specs.reserve(columns.size() + prepared_predicates.size());
    auto find_col = [&](const std::string& name) -> int {
        for (size_t k = 0; k < wanted_names.size(); ++k)
            if (*wanted_names[k] == name) return static_cast<int>(k);
        return -1;
    };
    auto add_col = [&](const std::string& name, int pred_idx, uint32_t out) {
        specs.push_back(parse_column_spec(name));
        const ColumnSpec& sp = specs.back();
        WantedColumn w{sp.key.data(), static_cast<uint32_t>(sp.key.size()),
                       sp.key.empty() ? uint8_t(0) : uint8_t(sp.key[0]), pred_idx, out};
        if (sp.nested) {
            w.sub       = sp.sub.data();
            w.sub_len   = static_cast<uint32_t>(sp.sub.size());
            w.sub_first = static_cast<uint8_t>(sp.sub[0]);
            w.slot      = nested_slot(context, name);
        }
        wanted_cols.push_back(w);
        wanted_names.push_back(&name);
    };
    for (size_t c = 0; c < columns.size(); ++c) add_col(columns[c], -1, static_cast<uint32_t>(c));
    // A predicate column joins the wanted set (reusing an output column of the same name)
    // and carries its predicate index for inline evaluation. Every predicate is judged
    // again on the record's captured values when it closes (bank_record), so a column
    // carrying several predicates, or none present, is handled there.
    std::vector<uint32_t> pred_slot(prepared_predicates.size());
    uint32_t next_slot = static_cast<uint32_t>(columns.size());
    for (size_t i = 0; i < prepared_predicates.size(); ++i) {
        const std::string& pc = prepared_predicates[i].column;
        // A `->` column is JSON, not text: comparing its rendering with a literal is
        // not what SQL means by it. Refused rather than given a meaning here.
        if (parse_column_spec(pc).as_json)
            throw std::invalid_argument(
                "read_jsonl: predicate on `->` column '" + pc +
                "' is not supported; compare the `->>` text instead");
        const int k = find_col(pc);
        if (k >= 0) {
            if (wanted_cols[k].pred_idx < 0) wanted_cols[k].pred_idx = static_cast<int>(i);
            pred_slot[i] = wanted_cols[k].out;
        } else {
            add_col(pc, static_cast<int>(i), next_slot);
            pred_slot[i] = next_slot++;
        }
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

    const MapProjection proj{&wanted_cols, &prepared_predicates, &pred_slot, columns.size(), &copy,
                             /*early_exit=*/!context.projected_columns.empty()};
    ColumnMap map = build_columns(buffer_data, buffer_length, proj, range_start, lines);

    // bytes_consumed = byte after the range's last newline (backward scan — newline near
    // the end).
    result.bytes_consumed = 0;
    for (size_t p = buffer_length; p-- > range_start; ) {
        if (buffer_data[p] == '\n') { result.bytes_consumed = p + 1; break; }
    }
    if (result.bytes_consumed == 0 && map.num_records() > 0) result.bytes_consumed = buffer_length;
    // Prefiltered: the skipped lines were consumed too — the range is done.
    if (lines) result.bytes_consumed = buffer_length;

    result.num_records_passed = map.rows;
    result.all_records = std::move(map);
    return result;
}

}  // namespace

// Single-range entry: build_columns over [range_start, buffer_length).
InterpreterResult interpret_jsonl(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const ParseContext& context,
    const std::vector<std::string>& columns,
    size_t range_start,
    const std::vector<LineSpan>* lines) {
    require_chunk_length(buffer_length, "interpret_jsonl");
    if (buffer_length == 0) return InterpreterResult{};
    return interpret_range(buffer_data, buffer_length, context, columns, range_start, lines,
                           head_copy_columns(buffer_data, buffer_length, columns, context));
}

// Multithreaded entry: split the buffer into newline-aligned ranges and map each in
// parallel, then append the per-range columns in order. All threads share the one
// read-only buffer; FieldSpan positions are offsets from the start of the range's chunk
// (ColumnMap), so the merged columns reference that single buffer (no per-chunk copies).
// max_threads == 0 means "use hardware_concurrency".
InterpreterResult interpret_jsonl_threaded(
    const uint8_t* buffer_data,
    size_t buffer_length,
    const ParseContext& context,
    const std::vector<std::string>& columns,
    size_t max_threads,
    bool use_prefilter) {

    InterpreterResult result;
    if (buffer_length == 0) { result.bytes_consumed = 0; return result; }

    // The prefilter gate decides ONCE, from bounded samples of the head; each range task
    // then finds its own surviving lines (see run_range).
    PrefilterPlan plan;
    const bool prefilter =
        use_prefilter && choose_prefilter_plan(buffer_data, buffer_length, context, plan);

    size_t hw = std::thread::hardware_concurrency();
    if (hw == 0) hw = 1;
    size_t nt = std::min(hw, max_threads ? max_threads : hw);

    // Don't over-split: aim for at least a few MB of work per thread so the
    // per-task overhead and the serial merge don't dominate.
    const size_t MIN_CHUNK = static_cast<size_t>(4) << 20;  // 4 MB
    size_t max_chunks = std::max<size_t>(1, buffer_length / MIN_CHUNK);
    nt = std::min(nt, max_chunks);

    // Chunks: the buffer is read in pieces of at most kMaxChunkBytes, each cut after a
    // newline (chunk_end), because every position the parser stores is a uint32_t offset
    // from the start of what it walks. Each range below lies inside one chunk and is walked
    // from that chunk's start, so its spans are chunk-relative; the merge records where each
    // chunk's rows begin (ColumnMap::chunk_row). An input of at most kMaxChunkBytes is one
    // chunk at 0 — exactly the unchunked read.
    std::vector<size_t> chunk_starts{0};
    for (size_t at = chunk_end(buffer_data, 0, buffer_length); at < buffer_length;
         at = chunk_end(buffer_data, at, buffer_length))
        chunk_starts.push_back(at);
    auto chunk_of = [&](size_t pos) {
        return static_cast<size_t>(
            std::upper_bound(chunk_starts.begin(), chunk_starts.end(), pos) - chunk_starts.begin() - 1);
    };

    // Which columns build_columns copies, decided once from the head of the whole input so
    // every range — in every chunk — decides the same (head_copy_columns).
    const std::vector<uint8_t> copy = head_copy_columns(buffer_data, buffer_length, columns, context);

    // Interpret one newline-aligned range [s, e) of the shared buffer, inside chunk k
    // (build_columns scans it window by window). Prefiltered: the range is cut into
    // segments — a filtered one is scanned over its surviving lines only (each begun at its
    // own start, so the skipped bytes are never parsed or judged), an unfiltered
    // (non-selective) one by the normal path. One partial result per segment, merged in
    // order below.
    auto run_range = [&](size_t k, size_t s, size_t e) -> std::vector<InterpreterResult> {
        const size_t c0 = chunk_starts[k];
        const uint8_t* chunk = buffer_data + c0;
        std::vector<InterpreterResult> parts;
        if (!prefilter) {
            // [s, e) is this range: build_columns judges its first and last lines against
            // the range bounds, not the whole buffer's.
            parts.push_back(interpret_range(chunk, e - c0, context, columns, s - c0, nullptr, copy));
            return parts;
        }
        for (const PrefilterSegment& seg : prefilter_plan_segments(chunk, s - c0, e - c0, plan))
            parts.push_back(interpret_range(chunk, seg.to, context, columns, seg.from,
                                            seg.filtered ? &seg.lines : nullptr, copy));
        return parts;
    };

    // Newline-aligned ranges. Each range ends just after a newline, so every range holds
    // whole lines. Any newline is a sound split, including a raw newline inside a string
    // (the JSONBench defect, tests/performance/jsonbench/README.md): build_columns judges
    // every line on its own, so the two halves of such a record are each rejected as
    // malformed whichever range they land in. A range that crosses a chunk start is cut
    // there.
    std::vector<std::pair<size_t, size_t>> ranges;
    {
        std::vector<std::pair<size_t, size_t>> even;
        size_t start = 0;
        for (size_t i = 1; i < nt && start < buffer_length; ++i) {
            size_t target = buffer_length * i / nt;
            if (target <= start) continue;
            size_t p = target;
            while (p < buffer_length && buffer_data[p] != '\n') ++p;
            if (p >= buffer_length) break;  // no more newlines; last range takes the rest
            const size_t split = p + 1;
            if (split >= buffer_length) break;  // rest of the buffer is one final range
            even.push_back({start, split});
            start = split;
        }
        if (start < buffer_length) even.push_back({start, buffer_length});
        for (const auto& [s, e] : even) {
            size_t a = s;
            for (size_t k = chunk_of(s) + 1; k < chunk_starts.size() && chunk_starts[k] < e; ++k) {
                ranges.push_back({a, chunk_starts[k]});
                a = chunk_starts[k];
            }
            ranges.push_back({a, e});
        }
    }

    std::vector<InterpreterResult> partial;
    std::vector<size_t> partial_chunk;   // the chunk each partial result lies in
    if (ranges.size() == 1) {
        // Small input — single-threaded scan + interpret; one segment needs no merge.
        partial = run_range(0, 0, buffer_length);
        if (partial.size() == 1) return std::move(partial[0]);
        partial_chunk.assign(partial.size(), 0);
    } else {
        std::vector<std::vector<InterpreterResult>> per_range(ranges.size());

        {
            BS::thread_pool<> pool(nt);
            std::vector<std::future<void>> futs;
            futs.reserve(ranges.size());
            for (size_t c = 0; c < ranges.size(); ++c) {
                futs.push_back(pool.submit_task([&, c]() {
                    per_range[c] = run_range(chunk_of(ranges[c].first), ranges[c].first, ranges[c].second);
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
        for (size_t c = 0; c < ranges.size(); ++c)
            for (auto& p : per_range[c]) {
                partial.push_back(std::move(p));
                partial_chunk.push_back(chunk_of(ranges[c].first));
            }
    }
    const size_t nc = partial.size();
    const size_t nchunks = chunk_starts.size();

    // Merge in range order, in parallel: every range's spans and arena bytes go straight
    // to their final offsets (prefix sums over the ranges), value_start rebased by the
    // range's arena base. A serial append copied every column's bytes on one thread.
    // Rebased relative to the range's CHUNK: a copied span indexes the arena from where its
    // chunk's bytes begin (chunk_arena), as an uncopied one indexes the source from where
    // its chunk begins (chunk_src).
    const size_t ncol = columns.size();
    std::vector<size_t> row_base(nc + 1, 0);
    std::vector<std::vector<size_t>> arena_base(ncol, std::vector<size_t>(nc + 1, 0));
    for (size_t k = 0; k < nc; ++k) {
        row_base[k + 1] = row_base[k] + partial[k].all_records.rows;
        for (size_t c = 0; c < ncol; ++c)
            arena_base[c][k + 1] = arena_base[c][k] + partial[k].all_records.arena[c].size();
    }
    ColumnMap& out = result.all_records;
    // A chunk's first partial: where its rows and arena bytes begin.
    std::vector<size_t> first_part(nchunks, nc);
    for (size_t k = nc; k-- > 0; ) first_part[partial_chunk[k]] = k;
    if (nchunks > 1) {
        out.chunk_src = chunk_starts;
        out.chunk_row.resize(nchunks);
        out.chunk_arena.assign(ncol, std::vector<size_t>(nchunks, 0));
        for (size_t h = 0; h < nchunks; ++h) {
            const size_t k = first_part[h];  // nc: a chunk with no segment holds no rows
            out.chunk_row[h] = row_base[k];
            for (size_t c = 0; c < ncol; ++c) out.chunk_arena[c][h] = arena_base[c][k];
        }
    }
    out.rows = row_base[nc];
    out.cols.resize(ncol);
    out.arena.resize(ncol);
    out.copied = partial[0].all_records.copied;
    for (size_t c = 0; c < ncol; ++c) {
        out.cols[c].resize(out.rows);
        out.arena[c].resize(arena_base[c][nc]);
    }
    {
        BS::thread_pool<> pool(nt);
        std::vector<std::future<void>> futs;
        for (size_t k = 0; k < nc; ++k) {
            futs.push_back(pool.submit_task([&, k]() {
                ColumnMap& p = partial[k].all_records;
                const size_t h = partial_chunk[k];
                for (size_t c = 0; c < ncol; ++c) {
                    if (!p.arena[c].empty())
                        std::memcpy(out.arena[c].data() + arena_base[c][k], p.arena[c].data(), p.arena[c].size());
                    // Within one chunk, so it fits uint32_t (a chunk's copied bytes are
                    // bytes of the chunk).
                    const uint32_t ab = static_cast<uint32_t>(arena_base[c][k] - arena_base[c][first_part[h]]);
                    FieldSpan* dst = out.cols[c].data() + row_base[k];
                    for (size_t r = 0; r < p.rows; ++r) {
                        FieldSpan f = p.cols[c][r];
                        if (p.copied[c] && !span_absent(f)) f.value_start += ab;
                        dst[r] = f;
                    }
                    // Done with this range's column: free it here, on the worker, rather
                    // than one range after another when `partial` is destroyed.
                    std::vector<FieldSpan>().swap(p.cols[c]);
                    ByteArena().swap(p.arena[c]);
                }
            }));
        }
        for (auto& f : futs) f.get();
    }
    for (size_t k = 0; k < nc; ++k) {
        const ColumnMap& m = partial[k].all_records;
        out.malformed_count += m.malformed_count;
        const size_t pos = chunk_starts[partial_chunk[k]] + m.malformed_pos;
        if (m.malformed && (!out.malformed || pos < out.malformed_pos)) {
            out.malformed = true;
            out.malformed_pos = pos;
        }
        result.num_records_passed += partial[k].num_records_passed;
    }
    result.bytes_consumed = buffer_length;
    return result;
}

}  // namespace rugo::_jsonl
