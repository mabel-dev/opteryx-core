/**
 * json_extract_bench — draken-level microbenchmark for `->` (BC_EXTR_JSON_PTR) and
 * `->>` (BC_EXTR_JSON_KEY).
 *
 * WHY THIS EXISTS
 * ---------------
 * `make jsonbench` times whole SQL queries: JSONL scan + decode + extraction +
 * filter + group. That is the right end-to-end number, but it cannot tell us what
 * share of it extraction owns, nor which STAGE of extraction owns that share. This
 * binary measures `draken_json_extract` alone, in ns/row, with no scan and no
 * Python anywhere — so a change to the kernel can be A/B'd against exactly one
 * variable.
 *
 * THE PHASE SPLIT
 * ---------------
 * Rather than instrument the production hot loop with timers (which would cost
 * something and would have to be compiled out again), the split is derived by
 * DIFFERENCING four loops that each do strictly more than the last:
 *
 *   L0  parse                  ReadPool::read + yyjson_doc_free
 *   L1  parse + navigate       ... + nav_tokens
 *   L2  parse + nav + emit     ... + yyjson_val_write (or the zero-copy branch)
 *   L3  the real kernel        draken_json_extract through the C ABI
 *
 *   parse        = L0
 *   navigate     = L1 - L0
 *   serialize    = L2 - L1
 *   output+ABI   = L3 - L2      (slot init, arena build, consolidation, ctx deref)
 *
 * L0-L2 are a MEASUREMENT SCAFFOLD for the per-row stages of
 * draken::ops::extract_rows (draken/ops/json_extract.h). They call that header's
 * OWN helpers — the same ReadPool (so the same allocator and read flags) and the
 * same nav_tokens walk over a path resolved by the same bind-time code — so each
 * level differs from the one below it by exactly one added stage and nothing else.
 *
 *   >>> IF extract_rows GAINS OR LOSES A STAGE, THESE LEVELS MUST FOLLOW. <<<
 *   A scaffold that has drifted from production does not report a phase split —
 *   it reports a fiction. The kernel row (L3) always measures the real thing, so
 *   a large negative L3-L2 delta is the drift alarm.
 *
 * --verify runs a differential check of the navigation instead of a measurement:
 * nav_tokens against yyjson_ptr_getn, on pointer identity, over both a table of
 * edge cases and the whole loaded corpus.
 *
 * Input is real data: Bluesky `commit` documents lifted out of the JSONBench NDJSON
 * shards (the same values the `commit ->> 'collection'` queries in
 * tests/performance/jsonbench/opteryx/runner.py extract from). Synthetic documents
 * are available behind an explicit flag for portability, and are labelled as such in
 * the output — they are NOT a substitute for the real distribution.
 *
 * Build + run:  make json-extract-bench
 */

#include "ops/kernels/extraction_kernels.h"
#include "ops/kernels/kernel_context.h"
#include "ops/kernels/kernel_registry.h"
#include "ops/kernels/result_helpers.h"   // vecresult_from_string_buffers (fusion arm)
#include "ops/json_extract.h"   // the production loop's own helpers: ReadPool, nav_tokens
#include "ops/string_result.h"
#include "ops/vec_result.h"
#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"

#include "yyjson.h"

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

namespace {

// ---------------------------------------------------------------------------
// Anti-DCE sink. Every level folds something derived from its result in here and
// the value is printed at the end, so no level can be optimised away wholesale.
// ---------------------------------------------------------------------------
volatile uint64_t g_sink = 0u;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------
struct Options {
    std::string file =
        "testdata/_downloads/jsonbench/decompressed/file_0001.jsonl";
    std::string field = "commit";   // lift this field out of each NDJSON record
    std::vector<std::string> paths; // extraction paths to bench (default below)
    uint32_t rows    = 200000u;
    int      iters   = 5;
    int      warmup  = 1;
    bool     synthetic = false;
    bool     verify    = false;     // differential-check navigation, do not time
    bool     fusion    = false;     // measure the sibling-fusion ceiling (item 6)
    bool     ptr_mode  = true;      // bench `->`
    bool     key_mode  = true;      // bench `->>`
    std::string csv;                // optional append-target for machine-readable rows
    std::string label = "baseline"; // tag written into the CSV
};

[[noreturn]] void die(const std::string& msg) {
    std::fprintf(stderr, "json_extract_bench: %s\n", msg.c_str());
    std::exit(1);
}

void usage() {
    std::printf(
        "usage: json_extract_bench [options]\n"
        "  --file PATH      NDJSON shard to read documents from\n"
        "                   (default: testdata/_downloads/jsonbench/decompressed/file_0001.jsonl)\n"
        "  --field NAME     lift this top-level field from each record (default: commit;\n"
        "                   pass '' to use the whole record)\n"
        "  --path P         extraction path to bench; repeatable\n"
        "                   (default: collection, operation, record.langs[0], nosuchkey)\n"
        "  --rows N         documents to bench over (default 200000)\n"
        "  --iters N        timed repeats; the MINIMUM is reported (default 5)\n"
        "  --warmup N       untimed repeats before timing (default 1)\n"
        "  --mode ptr|key|both   `->` only, `->>` only, or both (default both)\n"
        "  --synthetic      generate documents instead of reading a shard\n"
        "  --verify         differential-check the token walk against yyjson_ptr_getn\n"
        "                   and exit; runs no timings\n"
        "  --fusion         measure the ceiling of sibling-extraction fusion (one\n"
        "                   parse for N paths) against N separate kernel calls\n"
        "  --csv PATH       append machine-readable rows to PATH (for before/after diffing)\n"
        "  --label TAG      label written into the CSV (default 'baseline')\n");
}

Options parse_args(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&](const char* what) -> std::string {
            if (i + 1 >= argc) die(std::string("missing value for ") + what);
            return argv[++i];
        };
        if      (a == "--file")   o.file = next("--file");
        else if (a == "--field")  o.field = next("--field");
        else if (a == "--path")   o.paths.push_back(next("--path"));
        else if (a == "--rows")   o.rows = static_cast<uint32_t>(std::strtoul(next("--rows").c_str(), nullptr, 10));
        else if (a == "--iters")  o.iters = std::atoi(next("--iters").c_str());
        else if (a == "--warmup") o.warmup = std::atoi(next("--warmup").c_str());
        else if (a == "--csv")    o.csv = next("--csv");
        else if (a == "--label")  o.label = next("--label");
        else if (a == "--synthetic") o.synthetic = true;
        else if (a == "--verify") o.verify = true;
        else if (a == "--fusion") o.fusion = true;
        else if (a == "--mode") {
            const std::string m = next("--mode");
            if      (m == "ptr")  { o.ptr_mode = true;  o.key_mode = false; }
            else if (m == "key")  { o.ptr_mode = false; o.key_mode = true;  }
            else if (m == "both") { o.ptr_mode = true;  o.key_mode = true;  }
            else die("--mode must be ptr, key or both");
        }
        else if (a == "-h" || a == "--help") { usage(); std::exit(0); }
        else die("unknown option: " + a);
    }
    if (o.paths.empty())
        o.paths = {"collection", "operation", "record.langs[0]", "nosuchkey"};
    if (o.rows == 0u)  die("--rows must be > 0");
    if (o.iters < 1)   die("--iters must be >= 1");
    return o;
}

// ---------------------------------------------------------------------------
// Document loading
//
// Each NDJSON record is parsed once here (UNTIMED) and the requested field is
// serialized back to text — that text is the document the kernel then sees, which
// is exactly what READ_JSONL hands `->>` for a VARIANT column.
//
// Records that do not parse are skipped and COUNTED, never silently dropped: the
// upstream Bluesky dump has a handful of genuinely malformed lines (see
// tests/performance/jsonbench/README.md), and pretending they weren't there would
// misreport the row count the timings are divided by.
// ---------------------------------------------------------------------------
struct Corpus {
    std::vector<std::string> docs;
    size_t bytes       = 0u;
    size_t skipped_bad = 0u;   // line did not parse
    size_t skipped_missing = 0u; // record parsed but had no such field / it was null
    bool   synthetic   = false;
};

Corpus load_from_file(const Options& o) {
    std::ifstream in(o.file, std::ios::binary);
    if (!in)
        die("cannot open " + o.file +
            "\n  fetch it with: python tests/performance/jsonbench/fetch_data.py --size 1"
            "\n  or run with --synthetic for a portability smoke test (NOT a real measurement)");

    Corpus c;
    c.docs.reserve(o.rows);
    std::string line;
    while (c.docs.size() < o.rows && std::getline(in, line)) {
        if (line.empty()) continue;
        yyjson_doc* doc = yyjson_read(line.data(), line.size(), 0u);
        if (!doc) { ++c.skipped_bad; continue; }
        yyjson_val* root = yyjson_doc_get_root(doc);
        yyjson_val* val  = root;
        if (!o.field.empty())
            val = (root && yyjson_is_obj(root))
                      ? yyjson_obj_getn(root, o.field.data(), o.field.size())
                      : nullptr;
        if (!val || yyjson_is_null(val)) {
            ++c.skipped_missing;
            yyjson_doc_free(doc);
            continue;
        }
        size_t len = 0u;
        char*  txt = yyjson_val_write(val, 0u, &len);
        if (txt) {
            c.docs.emplace_back(txt, len);
            c.bytes += len;
            std::free(txt);
        } else {
            ++c.skipped_missing;
        }
        yyjson_doc_free(doc);
    }
    if (c.docs.empty()) die("no documents loaded from " + o.file);
    return c;
}

// Portability smoke corpus. Shaped loosely like a Bluesky `commit` — a handful of
// top-level keys, one nested object with an array — but it is NOT the real
// distribution and every report says so.
Corpus make_synthetic(const Options& o) {
    Corpus c;
    c.synthetic = true;
    c.docs.reserve(o.rows);
    static const char* kCollections[] = {
        "app.bsky.feed.post", "app.bsky.feed.like", "app.bsky.feed.repost",
        "app.bsky.graph.follow", "app.bsky.actor.profile"};
    char buf[512];
    for (uint32_t i = 0u; i < o.rows; ++i) {
        const int n = std::snprintf(
            buf, sizeof(buf),
            "{\"rev\":\"3l%06u\",\"operation\":\"%s\",\"collection\":\"%s\","
            "\"rkey\":\"3kx%06u\",\"record\":{\"type\":\"app.bsky.feed.post\","
            "\"langs\":[\"en\"],\"text\":\"synthetic row %u\"},\"cid\":\"bafyrei%06u\"}",
            i, (i % 4u) ? "create" : "delete", kCollections[i % 5u], i, i, i);
        c.docs.emplace_back(buf, static_cast<size_t>(n));
        c.bytes += static_cast<size_t>(n);
    }
    return c;
}

// ---------------------------------------------------------------------------
// Build a dense VARIANT DrakenVector over the corpus.
//
// VARIANT (not VARCHAR) because that is what READ_JSONL produces for a nested
// object column, and it is what the arrows actually run against in production.
// Dense shape: selection is the identity permutation, data_length == length.
// ---------------------------------------------------------------------------
struct OwnedVector {
    DrakenVector*      vec   = nullptr;
    DrakenStringArena* arena = nullptr;

    ~OwnedVector() {
        if (arena) { std::free(arena->arena); std::free(arena->slots); std::free(arena); }
        if (vec)   { std::free(const_cast<uint32_t*>(vec->selection)); std::free(vec); }
    }
};

void build_vector(const Corpus& c, OwnedVector& out) {
    const uint32_t n = static_cast<uint32_t>(c.docs.size());

    auto* sa = static_cast<DrakenStringArena*>(std::malloc(sizeof(DrakenStringArena)));
    if (!sa) die("out of memory building the input vector");
    std::memset(sa, 0, sizeof(DrakenStringArena));

    auto* slots = static_cast<DrakenStringSlot*>(std::malloc((size_t)n * sizeof(DrakenStringSlot)));
    if (!slots) die("out of memory building the input vector");
    std::memset(slots, 0, (size_t)n * sizeof(DrakenStringSlot));

    size_t arena_len = 0u;
    for (const auto& d : c.docs)
        if (d.size() > STR_INLINE_MAX) arena_len += d.size();

    uint8_t* arena = arena_len ? static_cast<uint8_t*>(std::malloc(arena_len)) : nullptr;
    if (arena_len && !arena) die("out of memory building the input vector");

    size_t pos = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        const auto* s = reinterpret_cast<const uint8_t*>(c.docs[i].data());
        const uint32_t len = static_cast<uint32_t>(c.docs[i].size());
        if (len <= STR_INLINE_MAX) {
            str_init_inline(&slots[i], s, len);
        } else {
            std::memcpy(arena + pos, s, len);
            str_init_extern(&slots[i], arena + pos, len, static_cast<uint32_t>(pos));
            pos += len;
        }
    }

    sa->slots = slots; sa->arena = arena; sa->length = n;
    sa->arena_used = arena_len; sa->arena_cap = arena_len;
    sa->owns_buffers = 0; sa->type = DRAKEN_VARIANT; sa->payloads_elided = 0;

    auto* vec = static_cast<DrakenVector*>(std::malloc(sizeof(DrakenVector)));
    auto* sel = static_cast<uint32_t*>(std::malloc((size_t)n * sizeof(uint32_t)));
    if (!vec || !sel) die("out of memory building the input vector");
    for (uint32_t i = 0u; i < n; ++i) sel[i] = i;

    vec->data = sa; vec->selection = sel; vec->data_length = n; vec->length = n;
    vec->validity = nullptr; vec->type = DRAKEN_VARIANT; vec->flags = DRAKEN_SEL_IDENTITY;

    out.vec = vec; out.arena = sa;
}

// ---------------------------------------------------------------------------
// The measurement scaffold — L0/L1/L2.
//
// MIRRORS draken::ops::extract_rows (draken/ops/json_extract.h). Read that loop
// alongside this one; they must stay in step. `stage` selects how far to go:
//   0 = parse, 1 = parse+navigate, 2 = parse+navigate+emit.
//
// It calls the production helpers directly — the same ReadPool (so the same
// allocator and the same read flags) and the same nav_tokens walk over the same
// bind-time-resolved path — so a level differs from the one below it by exactly
// one added stage and by nothing else.
// ---------------------------------------------------------------------------
uint64_t scaffold_pass(const DrakenVector* dv, const draken::ops::JsonNav& nav,
                       bool text_mode, int stage) {
    const auto* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;
    uint64_t acc = 0u;

    draken::ops::ReadPool pool(draken::ops::max_slot_length(dv));

    for (uint32_t i = 0u; i < n; ++i) {
        if (!draken::ops::sr_row_is_valid(dv, i)) continue;

        const DrakenStringSlot* src_slot = &sa->slots[dv->selection[i]];
        const uint8_t* json_bytes = str_data(src_slot, sa->arena);
        const uint32_t json_len   = str_length(src_slot);

        yyjson_read_err parse_err;
        yyjson_doc* doc = pool.read(reinterpret_cast<const char*>(json_bytes),
                                    static_cast<size_t>(json_len), &parse_err);
        if (!doc) die(std::string("invalid JSON at row ") + std::to_string(i) + ": " +
                      (parse_err.msg ? parse_err.msg : "unknown error"));

        if (stage == 0) {
            acc += static_cast<uint64_t>(yyjson_doc_get_read_size(doc));
            yyjson_doc_free(doc);
            continue;
        }

        yyjson_val* val = draken::ops::nav_tokens(yyjson_doc_get_root(doc), nav);

        if (stage == 1 || !val || yyjson_is_null(val)) {
            acc += val ? static_cast<uint64_t>(yyjson_get_tag(val)) : 1u;
            yyjson_doc_free(doc);
            continue;
        }

        if (yyjson_is_raw(val)) {
            acc += static_cast<uint64_t>(yyjson_get_len(val));
        } else if (text_mode && yyjson_is_str(val)) {
            acc += static_cast<uint64_t>(yyjson_get_len(val));
        } else {
            size_t out_len = 0u;
            char*  txt = yyjson_val_write(val, 0u, &out_len);
            if (!txt) die("yyjson_val_write failed at row " + std::to_string(i));
            acc += static_cast<uint64_t>(out_len);
            std::free(txt);
        }
        yyjson_doc_free(doc);
    }
    return acc;
}

// ---------------------------------------------------------------------------
// L3 — the production kernel, through the C ABI, exactly as the VM calls it
// (_dv_extraction_kernel_c in opteryx/expression/evaluator/evaluation.pyx).
// ---------------------------------------------------------------------------
struct KernelStats {
    uint64_t acc     = 0u;
    uint32_t non_null = 0u;
};

KernelStats kernel_pass(const extraction_ctx* ctx, const DrakenVector* dv, bool count_nulls) {
    KernelStats st;
    VecResult r = draken_json_extract(const_cast<extraction_ctx*>(ctx), dv, nullptr);
    if (r.data == nullptr)
        die(std::string("kernel error: ") + (r.error_msg ? r.error_msg : "(no message)"));

    st.acc += r.length;
    if (count_nulls) {
        const auto* sa = static_cast<const DrakenStringArena*>(r.data);
        for (uint32_t i = 0u; i < r.length; ++i) {
            const bool null_row =
                r.validity && ((r.validity[i >> 3] >> (i & 7u)) & 1u) == 0u;
            if (!null_row) {
                ++st.non_null;
                st.acc += str_length(&sa->slots[r.selection[i]]);
            }
        }
    } else {
        st.acc += static_cast<uint64_t>(reinterpret_cast<uintptr_t>(r.data) & 0xFFu);
    }

    // String results are one consolidated block with the bitmap embedded in it
    // (vecresult_from_string_buffers); selection points at the global identity
    // array and is never owned. One free.
    if (r.owns_selection) draken_free(const_cast<uint32_t*>(r.selection));
    draken_free(r.data);
    return st;
}

// ---------------------------------------------------------------------------
// Timing
// ---------------------------------------------------------------------------
using Clock = std::chrono::steady_clock;

double ns_now() {
    return std::chrono::duration<double, std::nano>(Clock::now().time_since_epoch()).count();
}

// Runs `fn` warmup+iters times, returns the MINIMUM elapsed ns. Minimum, not mean:
// on a shared machine every source of noise adds time, so the fastest run is the
// closest estimate of the true cost. Median is reported alongside for spread.
struct Timing { double min_ns = 0.0; double med_ns = 0.0; };

template <typename F>
Timing time_it(const Options& o, F&& fn) {
    for (int w = 0; w < o.warmup; ++w) g_sink += fn();

    std::vector<double> samples;
    samples.reserve(static_cast<size_t>(o.iters));
    for (int it = 0; it < o.iters; ++it) {
        const double t0 = ns_now();
        const uint64_t acc = fn();
        const double t1 = ns_now();
        g_sink += acc;
        samples.push_back(t1 - t0);
    }
    std::sort(samples.begin(), samples.end());
    Timing t;
    t.min_ns = samples.front();
    t.med_ns = samples[samples.size() / 2u];
    return t;
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------
struct Row {
    std::string op;        // "->" or "->>"
    std::string path;
    uint32_t    rows;
    uint32_t    non_null;
    double      parse_ns;
    double      nav_ns;
    double      emit_ns;
    double      out_ns;
    double      total_ns;
    double      total_med_ns;
};

void print_header(const Options& o, const Corpus& c) {
    std::printf("\n");
    std::printf("json_extract_bench — draken `->` / `->>` kernel, ns per row\n");
    std::printf("-----------------------------------------------------------------------------\n");
    if (c.synthetic) {
        std::printf("  source        SYNTHETIC documents — NOT the real distribution;\n");
        std::printf("                use --file with a real shard for any number you act on\n");
    } else {
        std::printf("  source        %s\n", o.file.c_str());
        std::printf("  field         %s\n", o.field.empty() ? "(whole record)" : o.field.c_str());
    }
    const double mean_bytes = static_cast<double>(c.bytes) / static_cast<double>(c.docs.size());
    std::printf("  documents     %zu  (mean %.0f bytes, %.1f MB total)\n",
                c.docs.size(), mean_bytes, static_cast<double>(c.bytes) / (1024.0 * 1024.0));
    if (c.skipped_bad || c.skipped_missing)
        std::printf("  skipped       %zu unparseable, %zu missing/null '%s'\n",
                    c.skipped_bad, c.skipped_missing, o.field.c_str());
    std::printf("  iterations    %d timed (min reported), %d warmup\n", o.iters, o.warmup);
    std::printf("\n");
}

void print_rows(const std::vector<Row>& rows, const Corpus& c) {
    std::printf("  %-4s %-18s %7s %6s | %8s %6s %8s %7s | %8s %8s %7s\n",
                "op", "path", "rows", "hit%", "parse", "nav", "emit", "out+abi",
                "TOTAL", "median", "MB/s");
    std::printf("  ---------------------------------------------------------------"
                "----------------------------------\n");
    for (const auto& r : rows) {
        const double hit = 100.0 * static_cast<double>(r.non_null) / static_cast<double>(r.rows);
        const double mbps =
            (static_cast<double>(c.bytes) / (1024.0 * 1024.0)) /
            ((r.total_ns * static_cast<double>(r.rows)) / 1e9);
        std::printf("  %-4s %-18s %7u %5.1f%% | %8.1f %6.1f %8.1f %7.1f | %8.1f %8.1f %7.0f\n",
                    r.op.c_str(), r.path.c_str(), r.rows, hit,
                    r.parse_ns, r.nav_ns, r.emit_ns, r.out_ns,
                    r.total_ns, r.total_med_ns, mbps);
    }
    std::printf("\n");
    std::printf("  parse   = L0 (ReadPool::read + doc_free)\n");
    std::printf("  nav     = L1-L0 (nav_tokens over the bind-time-resolved path)\n");
    std::printf("  emit    = L2-L1 (yyjson_val_write, or the zero-copy raw/string branch)\n");
    std::printf("  out+abi = L3-L2 (slot init, arena build, block consolidation, ctx deref)\n");
    std::printf("  TOTAL   = L3, the real kernel.\n");
    std::printf("\n");
    std::printf("  A small +/- on `emit` is measurement noise: for a path that lands on a\n");
    std::printf("  string (->>) or a number, the emit stage is a length read and copies\n");
    std::printf("  nothing. A LARGE negative delta means the scaffold has drifted from\n");
    std::printf("  extract_rows — fix that before trusting the split.\n");
    std::printf("\n");
}

void write_csv(const Options& o, const Corpus& c, const std::vector<Row>& rows) {
    if (o.csv.empty()) return;
    const bool exists = std::ifstream(o.csv).good();
    std::ofstream out(o.csv, std::ios::app);
    if (!out) die("cannot write " + o.csv);
    if (!exists)
        out << "label,synthetic,docs,mean_bytes,op,path,rows,non_null,"
               "parse_ns,nav_ns,emit_ns,out_ns,total_ns,total_med_ns\n";
    const double mean_bytes = static_cast<double>(c.bytes) / static_cast<double>(c.docs.size());
    for (const auto& r : rows) {
        out << o.label << ',' << (c.synthetic ? 1 : 0) << ',' << c.docs.size() << ','
            << mean_bytes << ',' << r.op << ',' << r.path << ',' << r.rows << ','
            << r.non_null << ',' << r.parse_ns << ',' << r.nav_ns << ',' << r.emit_ns
            << ',' << r.out_ns << ',' << r.total_ns << ',' << r.total_med_ns << '\n';
    }
    std::printf("  appended %zu row(s) to %s (label=%s)\n\n", rows.size(), o.csv.c_str(),
                o.label.c_str());
}

// ---------------------------------------------------------------------------
// --verify — differential check of the bind-time token walk against yyjson_ptr_getn.
//
// nav_tokens (json_extract.h) replaced yyjson_ptr_getn in the row loop: the same
// navigation, but split/unescaped/index-parsed once at bind time instead of once
// per row. "The same navigation" is a correctness claim, so it gets checked rather
// than asserted.
//
// The check is POINTER IDENTITY: both walks run against one parsed document, and
// must land on the very same yyjson_val* (or both miss). That compares navigation
// alone — no rendering, no serialization, nothing that the number-as-raw flag
// could confuse.
// ---------------------------------------------------------------------------
struct VerifyCase {
    const char* doc;
    const char* path;
    const char* what;
};

// Edge cases the corpus cannot be relied on to contain.
const VerifyCase kVerifyCases[] = {
    {R"({"a":{"b":"x"}})",              "$.a.b",      "nested object"},
    {R"({"a":{"b":"x"}})",              "a.b",        "nested, no $ prefix"},
    {R"({"a":{"b":"x"}})",              "/a/b",       "already a pointer"},
    {R"({"a":[10,20,30]})",             "$.a[1]",     "array index"},
    {R"({"a":[10,20,30]})",             "$.a[9]",     "index past the end"},
    {R"({"a":[10,20,30]})",             "$.a[0]",     "index zero"},
    {R"({"a":{"0":"obj","x":1}})",      "$.a.0",      "numeric key on an object"},
    {R"({"a":{"01":"leading-zero"}})",  "$.a.01",     "leading-zero key (not an index)"},
    {R"({"a":[1,2]})",                  "/a/01",      "leading-zero index is a miss"},
    {R"({"a":[1,2]})",                  "/a/-",       "the '-' past-the-end token"},
    {R"({"a~b":"tilde"})",              "/a~0b",      "~0 escape"},
    {R"({"a/b":"slash"})",              "/a~1b",      "~1 escape"},
    {R"({"a~1b":"literal"})",           "/a~01b",     "~01 is '~' then '1'"},
    {R"({"a":"scalar"})",               "$.a.b",      "token applied to a scalar"},
    {R"({"a":null})",                   "$.a",        "JSON null"},
    {R"({"a":1})",                      "$.missing",  "absent key"},
    {R"({"a":{"b":{"c":[{"d":7}]}}})",  "$.a.b.c[0].d", "deep mixed path"},
    {R"({"":"empty-key"})",             "/",          "the empty key"},
    {R"({"a":1})",                      "",           "empty path is the root"},
    {R"([1,2,3])",                      "/1",         "array at the root"},
};

// Returns the number of disagreements found.
size_t verify_one(const std::string& path_in, const std::string& doc_text,
                  const char* label, bool loud) {
    const std::string ptr = draken::ops::dotpath_to_jsonptr(path_in.data(), path_in.size());
    const draken::ops::JsonPtrPath path =
        draken::ops::tokenize_jsonptr(ptr.data(), ptr.size());

    draken::ops::JsonNav nav;
    nav.tokens  = path.tokens.data();
    nav.ntokens = static_cast<uint32_t>(path.tokens.size());
    nav.blob    = path.blob.data();
    nav.mode    = 0;

    yyjson_doc* doc = yyjson_read(doc_text.data(), doc_text.size(), 0u);
    if (!doc) die(std::string("verify: unparseable document for ") + label);
    yyjson_val* root = yyjson_doc_get_root(doc);

    yyjson_val* want = ptr.empty() ? root
                                   : yyjson_ptr_getn(root, ptr.data(), ptr.size());
    yyjson_val* got  = draken::ops::nav_tokens(root, nav);

    const bool ok = (want == got);
    if (!ok && loud) {
        std::printf("  MISMATCH  %-28s path=%-14s ptr=%-12s yyjson_ptr=%s nav_tokens=%s\n",
                    label, path_in.c_str(), ptr.c_str(),
                    want ? "hit" : "miss", got ? "hit" : "miss");
    }
    yyjson_doc_free(doc);
    return ok ? 0u : 1u;
}

int run_verify(const Options& o, const Corpus& c) {
    std::printf("verify — nav_tokens vs yyjson_ptr_getn (pointer identity)\n");
    std::printf("-----------------------------------------------------------------------------\n");

    size_t fails = 0u;
    for (const auto& vc : kVerifyCases)
        fails += verify_one(vc.path, vc.doc, vc.what, /*loud=*/true);
    std::printf("  edge cases    %zu checked, %zu mismatched\n",
                sizeof(kVerifyCases) / sizeof(kVerifyCases[0]), fails);

    size_t corpus_checks = 0u;
    for (const auto& p : o.paths) {
        size_t path_fails = 0u;
        for (const auto& d : c.docs) {
            path_fails += verify_one(p, d, p.c_str(), /*loud=*/path_fails == 0u);
            ++corpus_checks;
        }
        fails += path_fails;
    }
    std::printf("  corpus        %zu checked across %zu path(s), %zu mismatched\n",
                corpus_checks, o.paths.size(), fails);

    if (fails) {
        std::printf("\n  FAILED — the token walk does not agree with yyjson_ptr_getn.\n\n");
        return 1;
    }
    std::printf("\n  OK — every navigation landed on the same value.\n\n");
    return 0;
}

// ---------------------------------------------------------------------------
// --fusion — measure the CEILING of "parse once, extract all needed paths"
// (sibling-extraction fusion) WITHOUT building the planner/compiler change.
//
// The fused arm below is a PROTOTYPE KERNEL living only in this bench: the
// production row loop (extract_rows) generalized to N paths sharing one parse.
// It produces N consolidated VecResults through the very same output machinery
// the real kernel uses (same slot init, same arena build, same
// vecresult_from_string_buffers), so the comparison against N separate
// draken_json_extract calls isolates exactly one variable — the shared parse.
// A fused arm that skipped output work would manufacture a fake win
// (matched-wrappers rule); the N=1 row of the report is the sanity check that
// it doesn't: fused at N=1 must be ~1.00x of the real kernel.
//
// Before timing, the fused outputs are byte-compared against the real kernel's
// outputs for every path — a prototype that answers faster but differently
// would be worthless.
// ---------------------------------------------------------------------------
std::string vr_row(const VecResult& r, uint32_t i) {
    const auto* sa = static_cast<const DrakenStringArena*>(r.data);
    const DrakenStringSlot* s = &sa->slots[r.selection[i]];
    return std::string(reinterpret_cast<const char*>(str_data(s, sa->arena)),
                       str_length(s));
}
bool vr_null(const VecResult& r, uint32_t i) {
    return r.validity && ((r.validity[i >> 3] >> (i & 7u)) & 1u) == 0u;
}
void vr_free(VecResult& r) {
    if (r.owns_selection) draken_free(const_cast<uint32_t*>(r.selection));
    draken_free(r.data);
    r.data = nullptr;
}

// One parse per row, N navigations+emits, N outputs. `keep`, when non-null,
// receives the N VecResults (for the equality check); otherwise they are freed
// and only the anti-DCE accumulator escapes.
uint64_t fused_pass(const DrakenVector* dv,
                    const draken::ops::JsonNav* navs, size_t np,
                    bool text_mode,
                    std::vector<VecResult>* keep) {
    const auto* sa = static_cast<const DrakenStringArena*>(dv->data);
    const uint32_t n = dv->length;

    struct Out {
        draken::ops::StringRows rows;
        std::vector<uint8_t>    arena_buf;
        bool                    any_null = false;
    };
    std::vector<Out> outs(np);
    for (auto& o : outs) {
        o.rows.length = n;
        o.rows.type   = text_mode ? DRAKEN_NVARCHAR : DRAKEN_VARIANT;
        o.rows.slots  = draken::ops::sr_alloc_slots(n);
        o.arena_buf.reserve(static_cast<size_t>(n) * 32u);
    }

    draken::ops::ReadPool pool(draken::ops::max_slot_length(dv));

    for (uint32_t i = 0u; i < n; ++i) {
        if (!draken::ops::sr_row_is_valid(dv, i)) {
            for (auto& o : outs) { draken::ops::sr_mark_null(o.rows, i); o.any_null = true; }
            continue;
        }

        const DrakenStringSlot* src_slot = &sa->slots[dv->selection[i]];
        const uint8_t* json_bytes = str_data(src_slot, sa->arena);
        const uint32_t json_len   = str_length(src_slot);

        yyjson_read_err parse_err;
        yyjson_doc* doc = pool.read(reinterpret_cast<const char*>(json_bytes),
                                    static_cast<size_t>(json_len), &parse_err);
        if (!doc) die(std::string("fused: invalid JSON at row ") + std::to_string(i) +
                      ": " + (parse_err.msg ? parse_err.msg : "unknown error"));
        draken::ops::JDocGuard doc_guard(doc);
        yyjson_val* root = yyjson_doc_get_root(doc);

        for (size_t j = 0u; j < np; ++j) {
            Out& o = outs[j];
            yyjson_val* val = draken::ops::nav_tokens(root, navs[j]);
            if (!val || yyjson_is_null(val)) {
                draken::ops::sr_mark_null(o.rows, i);
                o.any_null = true;
                continue;
            }

            struct MallocGuard {
                char* p;
                ~MallocGuard() { if (p) std::free(p); }
            } mg{nullptr};

            const char* out_str = nullptr;
            size_t      out_len = 0u;
            if (yyjson_is_raw(val)) {
                out_str = yyjson_get_raw(val);
                out_len = yyjson_get_len(val);
            } else if (text_mode && yyjson_is_str(val)) {
                out_str = yyjson_get_str(val);
                out_len = yyjson_get_len(val);
            } else {
                mg.p = yyjson_val_write(val, 0u, &out_len);
                if (!mg.p) die("fused: yyjson_val_write failed at row " + std::to_string(i));
                out_str = mg.p;
            }

            if (out_len <= STR_INLINE_MAX) {
                str_init_inline(&o.rows.slots[i],
                                reinterpret_cast<const uint8_t*>(out_str),
                                static_cast<uint32_t>(out_len));
            } else {
                const uint32_t off = static_cast<uint32_t>(o.arena_buf.size());
                o.arena_buf.insert(o.arena_buf.end(),
                                   reinterpret_cast<const uint8_t*>(out_str),
                                   reinterpret_cast<const uint8_t*>(out_str) + out_len);
                draken_build_string_slot(&o.rows.slots[i], o.arena_buf.data() + off,
                                         static_cast<uint32_t>(out_len), off);
            }
        }
    }

    // Finalize exactly as the kernel does: arena copy, then the consolidated block.
    uint64_t acc = 0u;
    for (auto& o : outs) {
        o.rows.arena_len = o.arena_buf.size();
        if (o.rows.arena_len > 0u) {
            o.rows.arena = static_cast<uint8_t*>(draken_malloc(o.rows.arena_len));
            if (!o.rows.arena) die("fused: out of memory");
            std::memcpy(o.rows.arena, o.arena_buf.data(), o.rows.arena_len);
        }
        VecResult r = vecresult_from_string_buffers(
            o.rows.slots, o.rows.arena, o.rows.arena_len,
            o.rows.validity, n, o.rows.type);
        if (r.data == nullptr) die("fused: consolidation failed");
        acc += r.length + static_cast<uint64_t>(reinterpret_cast<uintptr_t>(r.data) & 0xFFu);
        if (keep) keep->push_back(r);
        else      vr_free(r);
    }
    return acc;
}

int run_fusion(const Options& o, const Corpus& c, const OwnedVector& ov) {
    const bool text_mode = o.key_mode;   // `->>` unless --mode ptr
    const uint16_t sub_op = text_mode ? 4u : 3u;
    const size_t np = o.paths.size();
    const DrakenVector* dv = ov.vec;

    // Bind once per path, exactly as the planner does.
    std::vector<extraction_ctx*> ctxs;
    std::vector<draken::ops::JsonNav> navs;
    for (const auto& p : o.paths) {
        extraction_ctx* ctx = kernel_alloc_extraction_ctx(sub_op, p.data(), p.size(), 0);
        if (!ctx) die("kernel_alloc_extraction_ctx returned null");
        draken::ops::JsonNav nav;
        nav.tokens  = reinterpret_cast<const draken::ops::JsonPtrToken*>(extraction_ctx_tokens(ctx));
        nav.ntokens = ctx->ntokens;
        nav.blob    = extraction_ctx_blob(ctx);
        nav.mode    = 0;
        ctxs.push_back(ctx);
        navs.push_back(nav);
    }

    std::printf("\n");
    std::printf("fusion ceiling — one parse for N sibling `%s` extractions vs N kernel calls\n",
                text_mode ? "->>" : "->");
    std::printf("-----------------------------------------------------------------------------\n");
    std::printf("  documents     %zu  (%s)\n", c.docs.size(),
                c.synthetic ? "SYNTHETIC — not a real measurement" : o.file.c_str());
    std::printf("\n");

    // Equality check first: the fused prototype must produce byte-identical
    // columns to the real kernel, or its timing is meaningless.
    {
        std::vector<VecResult> fused;
        g_sink += fused_pass(dv, navs.data(), np, text_mode, &fused);
        size_t mismatches = 0u;
        for (size_t j = 0u; j < np; ++j) {
            VecResult want = draken_json_extract(ctxs[j], dv, nullptr);
            if (want.data == nullptr) die("kernel error during fusion equality check");
            for (uint32_t i = 0u; i < dv->length; ++i) {
                const bool wn = vr_null(want, i), gn = vr_null(fused[j], i);
                if (wn != gn || (!wn && vr_row(want, i) != vr_row(fused[j], i))) {
                    if (mismatches == 0u)
                        std::printf("  MISMATCH path=%s row=%u\n", o.paths[j].c_str(), i);
                    ++mismatches;
                }
            }
            vr_free(want);
        }
        for (auto& r : fused) vr_free(r);
        if (mismatches) {
            std::printf("\n  FAILED — fused output differs from the kernel (%zu rows).\n\n",
                        mismatches);
            for (auto* ctx : ctxs) kernel_free_context(ctx);
            return 1;
        }
        std::printf("  equality      OK — fused output byte-identical to the kernel, all paths\n");
        std::printf("\n");
    }

    std::printf("  %2s  %-40s | %9s %9s | %7s\n",
                "N", "paths", "N-kernels", "fused", "speedup");
    std::printf("  --------------------------------------------------------------------------\n");

    const double nrows = static_cast<double>(dv->length);
    std::string path_list;
    for (size_t np_now = 1u; np_now <= np; ++np_now) {
        path_list += (np_now == 1u ? "" : "+") + o.paths[np_now - 1u];

        const Timing sep = time_it(o, [&] {
            uint64_t acc = 0u;
            for (size_t j = 0u; j < np_now; ++j)
                acc += kernel_pass(ctxs[j], dv, false).acc;
            return acc;
        });
        const Timing fus = time_it(o, [&] {
            return fused_pass(dv, navs.data(), np_now, text_mode, nullptr);
        });

        std::printf("  %2zu  %-40s | %8.1f %9.1f | %6.2fx%s\n",
                    np_now, path_list.c_str(),
                    sep.min_ns / nrows, fus.min_ns / nrows,
                    sep.min_ns / fus.min_ns,
                    np_now == 1u ? "   <- sanity: must be ~1.00x" : "");
    }

    std::printf("\n");
    std::printf("  N-kernels = N separate draken_json_extract calls (production today)\n");
    std::printf("  fused     = prototype: one parse per row, N navigations+emits, N output\n");
    std::printf("              columns through the kernel's own consolidation\n");
    std::printf("  jsonbench context: Q2/Q4/Q5 have N=2, Q3 has N=3 (4 counting the\n");
    std::printf("  WHERE/SELECT duplicate); Q1 has N=1 and fusion cannot help it.\n\n");

    for (auto* ctx : ctxs) kernel_free_context(ctx);
    return 0;
}

// One (op, path) cell: four timed levels over the same vector.
Row bench_one(const Options& o, const OwnedVector& ov, const std::string& path,
              bool text_mode) {
    // Bind-time work, done ONCE — exactly as the planner does it. This is also
    // what makes the scaffold and the kernel navigate the same bytes.
    const uint16_t sub_op = text_mode ? 4u /* BC_EXTR_JSON_KEY */
                                      : 3u /* BC_EXTR_JSON_PTR */;
    extraction_ctx* ctx =
        kernel_alloc_extraction_ctx(sub_op, path.data(), path.size(), 0);
    if (!ctx) die("kernel_alloc_extraction_ctx returned null");

    // The scaffold navigates through the ctx the kernel itself will use — same
    // tokens, same blob, resolved by the same bind-time code.
    draken::ops::JsonNav nav;
    nav.tokens  = reinterpret_cast<const draken::ops::JsonPtrToken*>(extraction_ctx_tokens(ctx));
    nav.ntokens = ctx->ntokens;
    nav.blob    = extraction_ctx_blob(ctx);
    nav.mode    = 0;

    const DrakenVector* dv = ov.vec;

    const Timing l0 = time_it(o, [&] { return scaffold_pass(dv, nav, text_mode, 0); });
    const Timing l1 = time_it(o, [&] { return scaffold_pass(dv, nav, text_mode, 1); });
    const Timing l2 = time_it(o, [&] { return scaffold_pass(dv, nav, text_mode, 2); });
    const Timing l3 = time_it(o, [&] { return kernel_pass(ctx, dv, false).acc; });

    // Hit rate is a property of the data, not of a timed run — measure it once,
    // outside the timed region, so counting nulls never lands in the L3 number.
    const uint32_t non_null = kernel_pass(ctx, dv, true).non_null;

    kernel_free_context(ctx);

    const double n = static_cast<double>(dv->length);
    Row r;
    r.op       = text_mode ? "->>" : "->";
    r.path     = path;
    r.rows     = dv->length;
    r.non_null = non_null;
    r.parse_ns = l0.min_ns / n;
    r.nav_ns   = (l1.min_ns - l0.min_ns) / n;
    r.emit_ns  = (l2.min_ns - l1.min_ns) / n;
    r.out_ns   = (l3.min_ns - l2.min_ns) / n;
    r.total_ns = l3.min_ns / n;
    r.total_med_ns = l3.med_ns / n;
    return r;
}

}  // namespace

int main(int argc, char** argv) {
    const Options o = parse_args(argc, argv);

    const Corpus corpus = o.synthetic ? make_synthetic(o) : load_from_file(o);
    OwnedVector ov;
    build_vector(corpus, ov);

    if (o.verify) return run_verify(o, corpus);
    if (o.fusion) return run_fusion(o, corpus, ov);

    print_header(o, corpus);

    std::vector<Row> rows;
    for (const auto& path : o.paths) {
        if (o.key_mode) rows.push_back(bench_one(o, ov, path, /*text_mode=*/true));
        if (o.ptr_mode) rows.push_back(bench_one(o, ov, path, /*text_mode=*/false));
    }

    print_rows(rows, corpus);
    write_csv(o, corpus, rows);

    std::printf("  (sink %" PRIu64 ")\n\n", g_sink);
    return 0;
}
