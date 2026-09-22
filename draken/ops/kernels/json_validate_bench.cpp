/**
 * json_validate_bench — draken-level microbenchmark for `IS [NOT] JSON`
 * (draken/ops/kernels/function_json_validate.cpp over draken/ops/json_validate.h).
 *
 * WHAT IT MEASURES
 * ----------------
 * IS JSON's cost per row over real JSON text, and how that cost moves as a
 * fraction of the rows are truncated. IS JSON is answered by a yyjson parse behind
 * first/last-byte gates (json_validate.h). History, 2026-09-22 on this bench:
 *   - the last-byte gate on the old hand-written walker: no cost on valid input,
 *     -32% ns/row at 50% truncated;
 *   - yyjson + pool behind the gates vs that walker: -33% ns/row on every mix
 *     (190 -> 127 ns at 0%, 99 -> 68 ns at 50%), so the walker was replaced.
 *
 * ARMS (identical bitmaps; verified per mix before any timing)
 *   K   kernel   — the production C-ABI kernel (draken_is_json_<shape>), result
 *                  bitmap allocated and freed each pass. The real number.
 *   A   scaffold — MIRRORS jk_is_json's row loop (function_json_validate.cpp):
 *                  an IsJsonReadPool sized per pass, json_is_wellformed per row
 *                  into a preallocated bitmap. The arm to edit when A/B-ing a
 *                  change to the row loop.
 *   A'  control  — A again, byte-identical. It CANNOT differ from A, so A'/A is
 *                  the harness's own bias. Read it FIRST: if it is not ~1.00, no
 *                  other ratio in the table means anything.
 *
 *   >>> IF jk_is_json's ROW LOOP CHANGES, A MUST FOLLOW. <<<  K/A is the drift
 *   alarm: it should sit at ~1.00 (K also pays the bitmap alloc + C ABI).
 *
 * MIXES
 *   Base documents are raw NDJSON lines from a JSONBench shard (the bytes as they
 *   arrive, not re-serialized). Each mix truncates a deterministic fraction of rows
 *   at a uniformly random byte in [1, len): valid (0%), and --trunc percentages.
 *   A truncation can land just after an inner `}`; such a row survives the
 *   last-byte gate and pays for a partial parse.
 *
 * METHOD
 *   Arms are interleaved within each round and the order is REVERSED on odd rounds
 *   (a fixed within-round order biased the second arm 1-4% in dev/bench_unnest.py).
 *   Warmup rounds are discarded; the MEDIAN over rounds is reported.
 *
 * Build + run:  make json-validate-bench JSON_VALIDATE_BENCH_ARGS="..."
 */

#include "ops/json_validate.h"
#include "ops/vec_result.h"
#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <random>
#include <string>
#include <vector>

extern "C" {
VecResult draken_is_json_value(void*, const DrakenVector* const*, uint32_t);
VecResult draken_is_json_object(void*, const DrakenVector* const*, uint32_t);
VecResult draken_is_json_array(void*, const DrakenVector* const*, uint32_t);
VecResult draken_is_json_scalar(void*, const DrakenVector* const*, uint32_t);
}

namespace {

using draken::ops::IsJsonReadPool;
using draken::ops::json_is_wellformed;

volatile uint64_t g_sink = 0u;

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------
struct Options {
    std::string file = "testdata/_downloads/jsonbench/decompressed/file_0001.jsonl";
    uint32_t rows = 200000u;
    int rounds = 15;
    int warmup = 2;
    uint8_t shape = draken::ops::JSON_SHAPE_VALUE;
    const char* shape_name = "value";
    std::vector<int> trunc = {0, 1, 10, 50};  // percent of rows truncated, per mix
    uint64_t seed = 0x5eedULL;
};

[[noreturn]] void die(const std::string& msg) {
    std::fprintf(stderr, "json_validate_bench: %s\n", msg.c_str());
    std::exit(1);
}

void usage() {
    std::printf(
        "json_validate_bench [options]\n"
        "  --file PATH     NDJSON shard; each line is one document\n"
        "                  (default: testdata/_downloads/jsonbench/decompressed/file_0001.jsonl)\n"
        "  --rows N        documents per mix (default 200000)\n"
        "  --rounds N      timed rounds, median reported (default 15)\n"
        "  --warmup N      discarded rounds (default 2)\n"
        "  --shape S       value|object|array|scalar (default value)\n"
        "  --trunc LIST    comma-separated truncation percentages (default 0,1,10,50)\n"
        "  --seed N        truncation RNG seed (default 0x5eed)\n");
}

Options parse_args(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) die(std::string(flag) + " needs a value");
            return argv[++i];
        };
        if (a == "--file") o.file = next("--file");
        else if (a == "--rows") o.rows = static_cast<uint32_t>(std::strtoul(next("--rows").c_str(), nullptr, 10));
        else if (a == "--rounds") o.rounds = std::atoi(next("--rounds").c_str());
        else if (a == "--warmup") o.warmup = std::atoi(next("--warmup").c_str());
        else if (a == "--seed") o.seed = std::strtoull(next("--seed").c_str(), nullptr, 0);
        else if (a == "--shape") {
            const std::string s = next("--shape");
            if (s == "value") { o.shape = draken::ops::JSON_SHAPE_VALUE; o.shape_name = "value"; }
            else if (s == "object") { o.shape = draken::ops::JSON_SHAPE_OBJECT; o.shape_name = "object"; }
            else if (s == "array") { o.shape = draken::ops::JSON_SHAPE_ARRAY; o.shape_name = "array"; }
            else if (s == "scalar") { o.shape = draken::ops::JSON_SHAPE_SCALAR; o.shape_name = "scalar"; }
            else die("--shape must be value, object, array or scalar");
        } else if (a == "--trunc") {
            o.trunc.clear();
            const std::string s = next("--trunc");
            size_t p = 0u;
            while (p <= s.size()) {
                const size_t q = s.find(',', p);
                const std::string tok = s.substr(p, q == std::string::npos ? std::string::npos : q - p);
                const int v = std::atoi(tok.c_str());
                if (tok.empty() || v < 0 || v > 100) die("--trunc entries must be 0..100");
                o.trunc.push_back(v);
                if (q == std::string::npos) break;
                p = q + 1u;
            }
        } else if (a == "-h" || a == "--help") { usage(); std::exit(0); }
        else die("unknown argument " + a);
    }
    if (o.rows == 0u) die("--rows must be > 0");
    if (o.rounds < 1) die("--rounds must be >= 1");
    if (o.warmup < 0) die("--warmup must be >= 0");
    return o;
}

using KernelFn = VecResult (*)(void*, const DrakenVector* const*, uint32_t);

KernelFn kernel_for(uint8_t shape) {
    switch (shape) {
        case draken::ops::JSON_SHAPE_OBJECT: return draken_is_json_object;
        case draken::ops::JSON_SHAPE_ARRAY: return draken_is_json_array;
        case draken::ops::JSON_SHAPE_SCALAR: return draken_is_json_scalar;
        default: return draken_is_json_value;
    }
}

// ---------------------------------------------------------------------------
// Corpus
// ---------------------------------------------------------------------------
std::vector<std::string> load_lines(const Options& o) {
    std::ifstream in(o.file, std::ios::binary);
    if (!in)
        die("cannot open " + o.file +
            "\n  fetch it with: python tests/performance/jsonbench/fetch_data.py --size 1");
    std::vector<std::string> docs;
    docs.reserve(o.rows);
    std::string line;
    while (docs.size() < o.rows && std::getline(in, line)) {
        if (line.empty()) continue;
        docs.push_back(line);
    }
    if (docs.size() < o.rows)
        die("shard has only " + std::to_string(docs.size()) + " non-empty lines; lower --rows");
    return docs;
}

// Truncates `pct`% of rows at a uniform byte in [1, len). Deterministic per seed+pct
// so every arm and every run sees the same rows.
std::vector<std::string> make_mix(const std::vector<std::string>& base, int pct, uint64_t seed) {
    std::vector<std::string> docs = base;
    std::mt19937_64 rng(seed ^ (static_cast<uint64_t>(pct) * 0x9E3779B97F4A7C15ULL));
    std::uniform_int_distribution<int> coin(0, 99);
    for (auto& d : docs) {
        if (d.size() < 2u || coin(rng) >= pct) continue;
        std::uniform_int_distribution<size_t> cut(1u, d.size() - 1u);
        d.resize(cut(rng));
    }
    return docs;
}

// Dense VARCHAR vector over the docs — what READ_JSONL/a text column hands IS JSON.
struct OwnedVector {
    DrakenVector* vec = nullptr;
    DrakenStringArena* arena = nullptr;
    size_t bytes = 0u;
    OwnedVector() = default;
    OwnedVector(const OwnedVector&) = delete;
    OwnedVector& operator=(const OwnedVector&) = delete;
    ~OwnedVector() {
        if (arena) { std::free(arena->arena); std::free(arena->slots); std::free(arena); }
        if (vec) { std::free(const_cast<uint32_t*>(vec->selection)); std::free(vec); }
    }
};

void build_vector(const std::vector<std::string>& docs, OwnedVector& out) {
    const uint32_t n = static_cast<uint32_t>(docs.size());
    auto* sa = static_cast<DrakenStringArena*>(std::calloc(1u, sizeof(DrakenStringArena)));
    auto* slots = static_cast<DrakenStringSlot*>(std::calloc(n, sizeof(DrakenStringSlot)));
    if (!sa || !slots) die("out of memory building the input vector");

    size_t arena_len = 0u;
    for (const auto& d : docs) {
        out.bytes += d.size();
        if (d.size() > STR_INLINE_MAX) arena_len += d.size();
    }
    uint8_t* arena = arena_len ? static_cast<uint8_t*>(std::malloc(arena_len)) : nullptr;
    if (arena_len && !arena) die("out of memory building the input vector");

    size_t pos = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        const auto* s = reinterpret_cast<const uint8_t*>(docs[i].data());
        const uint32_t len = static_cast<uint32_t>(docs[i].size());
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
    sa->owns_buffers = 0; sa->type = DRAKEN_VARCHAR; sa->payloads_elided = 0;

    auto* vec = static_cast<DrakenVector*>(std::malloc(sizeof(DrakenVector)));
    auto* sel = static_cast<uint32_t*>(std::malloc(static_cast<size_t>(n) * sizeof(uint32_t)));
    if (!vec || !sel) die("out of memory building the input vector");
    for (uint32_t i = 0u; i < n; ++i) sel[i] = i;
    vec->data = sa; vec->selection = sel; vec->data_length = n; vec->length = n;
    vec->validity = nullptr; vec->type = DRAKEN_VARCHAR; vec->flags = DRAKEN_SEL_IDENTITY;
    out.vec = vec; out.arena = sa;
}

// ---------------------------------------------------------------------------
// Arms
// ---------------------------------------------------------------------------

// A / A': mirrors jk_is_json's loop (no validity — the corpus has no NULLs).
uint64_t scaffold_pass(const DrakenVector* v, uint8_t shape, uint8_t* out) {
    const auto* sa = static_cast<const DrakenStringArena*>(v->data);
    const uint32_t n = v->length;
    std::memset(out, 0, (static_cast<size_t>(n) + 7u) / 8u);
    IsJsonReadPool pool(draken::ops::max_slot_length(v));
    uint64_t hits = 0u;
    for (uint32_t i = 0u; i < n; ++i) {
        const DrakenStringSlot* slot = &sa->slots[v->selection[i]];
        if (json_is_wellformed(str_data(slot, sa->arena), str_length(slot), shape, pool)) {
            out[i >> 3] |= static_cast<uint8_t>(1u << (i & 7u));
            ++hits;
        }
    }
    return hits;
}

uint64_t kernel_pass(KernelFn fn, const DrakenVector* v, std::vector<uint8_t>* copy_out) {
    const DrakenVector* args[1] = {v};
    VecResult r = fn(nullptr, args, 1u);
    if (r.type != DRAKEN_BOOL || r.data == nullptr) die("kernel returned an error sentinel");
    const auto* bits = static_cast<const uint8_t*>(r.data);
    const size_t nb = (static_cast<size_t>(r.length) + 7u) / 8u;
    uint64_t hits = 0u;
    for (size_t b = 0u; b < nb; ++b) hits += static_cast<uint64_t>(__builtin_popcount(bits[b]));
    if (copy_out) copy_out->assign(bits, bits + nb);
    draken_free(r.data);
    return hits;
}

double ns_now() {
    return std::chrono::duration<double, std::nano>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

double median(std::vector<double> xs) {
    std::sort(xs.begin(), xs.end());
    const size_t m = xs.size() / 2u;
    return xs.size() % 2u ? xs[m] : 0.5 * (xs[m - 1u] + xs[m]);
}

}  // namespace

int main(int argc, char** argv) {
    const Options o = parse_args(argc, argv);
    const std::vector<std::string> base = load_lines(o);
    const KernelFn kfn = kernel_for(o.shape);

    std::printf("json_validate_bench  shape=%s  rows=%u  rounds=%d (+%d warmup)  file=%s\n",
                o.shape_name, o.rows, o.rounds, o.warmup, o.file.c_str());
    std::printf("median ns/row per arm; ratios are vs A. Read A'/A first — it is the harness bias.\n\n");
    std::printf("%-6s %8s %9s | %8s %8s %8s | %6s %6s\n", "trunc%", "MB", "matched%",
                "K", "A", "A'", "A'/A", "K/A");
    std::printf("---------------------------------------------------------------------\n");

    for (const int pct : o.trunc) {
        const std::vector<std::string> docs = make_mix(base, pct, o.seed);
        OwnedVector ov;
        build_vector(docs, ov);
        const DrakenVector* v = ov.vec;
        const uint32_t n = v->length;
        const size_t nb = (static_cast<size_t>(n) + 7u) / 8u;

        // Verify: every arm must produce the kernel's exact bitmap.
        std::vector<uint8_t> kbits, abits(nb);
        const uint64_t khits = kernel_pass(kfn, v, &kbits);
        const uint64_t ahits = scaffold_pass(v, o.shape, abits.data());
        if (kbits != abits || khits != ahits) die("scaffold A disagrees with the kernel — A has drifted");

        // Timed rounds. Arm order is reversed on odd rounds.
        enum { K = 0, A = 1, A2 = 2, NARMS = 3 };
        std::vector<double> samples[NARMS];
        std::vector<uint8_t> scratch(nb);
        auto run = [&](int arm) -> double {
            const double t0 = ns_now();
            uint64_t acc;
            switch (arm) {
                case K: acc = kernel_pass(kfn, v, nullptr); break;
                default: acc = scaffold_pass(v, o.shape, scratch.data()); break;  // A, A'
            }
            const double t1 = ns_now();
            g_sink += acc;
            return (t1 - t0) / static_cast<double>(n);
        };
        for (int r = 0; r < o.warmup + o.rounds; ++r) {
            for (int j = 0; j < NARMS; ++j) {
                const int arm = (r % 2 == 0) ? j : (NARMS - 1 - j);
                const double ns = run(arm);
                if (r >= o.warmup) samples[arm].push_back(ns);
            }
        }
        const double mk = median(samples[K]), ma = median(samples[A]), ma2 = median(samples[A2]);
        std::printf("%-6d %8.1f %8.2f%% | %8.1f %8.1f %8.1f | %6.3f %6.3f\n", pct,
                    static_cast<double>(ov.bytes) / 1e6, 100.0 * static_cast<double>(khits) / n,
                    mk, ma, ma2, ma2 / ma, mk / ma);
    }
    std::printf("\n(sink %llu)\n", static_cast<unsigned long long>(g_sink));
    return 0;
}
