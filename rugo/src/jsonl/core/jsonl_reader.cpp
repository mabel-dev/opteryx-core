#include "jsonl_reader.hpp"
#include "volnitsky.h"     // simd_contains_cs (confirm step)
#include "interpreter.hpp"  // require_chunk_length
#include <cstring>
#include <algorithm>
#include <memory>
#include <utility>

namespace rugo::_jsonl {

namespace {

// [start, end) of the line holding byte `at`, within [from, to): `from` is a line start,
// `end` is the line's newline or `to`.
inline LineSpan line_at(const uint8_t* buffer, size_t from, size_t to, size_t at) {
    size_t ls = at; while (ls > from && buffer[ls - 1] != '\n') --ls;
    size_t le = at; while (le < to && buffer[le] != '\n') ++le;
    return {static_cast<uint32_t>(ls), static_cast<uint32_t>(le)};
}

// Every line of [from, to) containing a `\u` escape, in buffer order. memchr-driven: the
// only byte examined per non-backslash run is the run's end, and a hit skips to the next line.
std::vector<LineSpan> unicode_escape_lines(const uint8_t* buffer, size_t from, size_t to) {
    std::vector<LineSpan> lines;
    size_t p = from;
    while (p + 1 < to) {
        const void* hit = std::memchr(buffer + p, '\\', to - p - 1);
        if (hit == nullptr) break;
        const size_t b = static_cast<size_t>(static_cast<const uint8_t*>(hit) - buffer);
        if (buffer[b + 1] != 'u') { p = b + 1; continue; }
        lines.push_back(line_at(buffer, from, to, b));
        p = static_cast<size_t>(lines.back().end) + 1;
    }
    return lines;
}

}  // namespace

namespace {

// Multi-pattern Volnitsky over a clause's needles. W = the shortest needle's length (capped
// at 256). EVERY bigram in the first W bytes of every needle is registered with its offset
// (chained, not just the rightmost), so after a window's candidates are all verified the
// scan may stride W-1 whatever the outcome: an occurrence at s covers bigram end positions
// s+1 .. s+W-1, W-1 consecutive positions, and the stride lands on one of them.
struct MultiTable {
    struct Entry { uint32_t next; uint32_t needle; uint32_t offset; };
    std::vector<uint32_t> head;   // bigram -> 1-based index into entries (0 = absent)
    std::vector<Entry> entries;
    size_t window = 0;
    explicit MultiTable(const std::vector<std::string>& needles) : head(65536, 0) {
        window = SIZE_MAX;
        for (const std::string& n : needles) window = n.size() < window ? n.size() : window;
        if (window > 256) window = 256;
        for (uint32_t j = 0; j < needles.size(); ++j) {
            const uint8_t* n = reinterpret_cast<const uint8_t*>(needles[j].data());
            for (uint32_t i = 0; i + 1 < window; ++i) {
                const uint16_t h = (static_cast<uint16_t>(n[i]) << 8) | n[i + 1];
                entries.push_back({head[h], j, i});
                head[h] = static_cast<uint32_t>(entries.size());
            }
        }
    }
};

inline bool line_has_unicode_escape(const uint8_t* b, size_t ls, size_t le) {
    for (size_t p = ls; p + 1 < le; ) {
        const void* hit = std::memchr(b + p, '\\', le - p - 1);
        if (hit == nullptr) return false;
        const size_t q = static_cast<size_t>(static_cast<const uint8_t*>(hit) - b);
        if (b[q + 1] == 'u') return true;
        p = q + 1;   // as unicode_escape_lines: any `\u` byte pair counts (conservative)
    }
    return false;
}

inline bool line_has(const uint8_t* b, size_t ls, size_t le, const std::string& n) {
    return simd_contains_cs(b + ls, le - ls, reinterpret_cast<const uint8_t*>(n.data()), n.size());
}

// Does line [ls, le) satisfy every clause after the driver?
inline bool confirm_line(const uint8_t* b, size_t ls, size_t le, const PrefilterPlan& plan) {
    for (size_t c = 1; c < plan.clauses.size(); ++c) {
        const PrefilterClause& cl = plan.clauses[c];
        bool ok = false;
        for (const std::string& n : cl.needles) if (line_has(b, ls, le, n)) { ok = true; break; }
        if (!ok && cl.keep_unicode_escapes) ok = line_has_unicode_escape(b, ls, le);
        if (!ok) return false;
    }
    return true;
}

}  // namespace

// SIMD sieve driver: per needle, compare the two sieve bytes (the rarest on the sample) at
// their offsets for 64 candidate starts at a time; verify candidates with memcmp. Touches
// every byte (like the newline floor) at vector width, independent of needle length — the
// better driver when needles are short and Volnitsky's stride (shortest needle - 1) is small.
static void simd_driver(const uint8_t* buffer, size_t from, size_t to, const PrefilterPlan& plan,
                        std::vector<LineSpan>& hits) {
    const PrefilterClause& driver = plan.clauses[0];
    const size_t k = driver.needles.size();
    size_t maxlen = 0;
    for (const auto& n : driver.needles) maxlen = n.size() > maxlen ? n.size() : maxlen;
    // Verify a candidate start; on a hit decide its line and return the resume position.
    auto verify = [&](size_t start, size_t& resume) -> bool {
        for (const std::string& n : driver.needles)
            if (start + n.size() <= to && std::memcmp(buffer + start, n.data(), n.size()) == 0) {
                const LineSpan line = line_at(buffer, from, to, start);
                if (confirm_line(buffer, line.start, line.end, plan)) hits.push_back(line);
                resume = static_cast<size_t>(line.end) + 1;
                return true;
            }
        return false;
    };
    size_t s = from;
#if defined(__ARM_NEON)
    while (s + 64 + maxlen <= to) {
        uint8x16_t acc[4] = {vdupq_n_u8(0), vdupq_n_u8(0), vdupq_n_u8(0), vdupq_n_u8(0)};
        for (size_t j = 0; j < k; ++j) {
            const uint8_t* n = reinterpret_cast<const uint8_t*>(driver.needles[j].data());
            const uint32_t a = plan.sieve[j].first, b = plan.sieve[j].second;
            const uint8x16_t va = vdupq_n_u8(n[a]), vb = vdupq_n_u8(n[b]);
            for (int q = 0; q < 4; ++q)
                acc[q] = vorrq_u8(acc[q], vandq_u8(vceqq_u8(vld1q_u8(buffer + s + 16 * q + a), va),
                                                   vceqq_u8(vld1q_u8(buffer + s + 16 * q + b), vb)));
        }
        if (vmaxvq_u8(vorrq_u8(vorrq_u8(acc[0], acc[1]), vorrq_u8(acc[2], acc[3]))) == 0) {
            s += 64;
            continue;
        }
        size_t resume = s + 64;
        bool jumped = false;
        for (int q = 0; q < 4 && !jumped; ++q) {
            uint64_t m = vget_lane_u64(
                vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(acc[q]), 4)), 0);
            while (m) {
                const unsigned L = static_cast<unsigned>(__builtin_ctzll(m) >> 2);
                m &= ~(0xFull << (L << 2));
                if (verify(s + 16 * q + L, resume)) { jumped = true; break; }
            }
        }
        s = resume;
    }
#endif
    // Tail (and non-NEON builds): scalar sieve, one start at a time.
    while (s < to) {
        bool cand = false;
        for (size_t j = 0; j < k && !cand; ++j) {
            const std::string& n = driver.needles[j];
            cand = s + n.size() <= to &&
                   buffer[s + plan.sieve[j].first] == static_cast<uint8_t>(n[plan.sieve[j].first]) &&
                   buffer[s + plan.sieve[j].second] == static_cast<uint8_t>(n[plan.sieve[j].second]);
        }
        size_t resume = s + 1;
        if (cand) verify(s, resume);
        s = resume;
    }
}

// The plan's surviving lines of [from, to) — one driver pass (SIMD sieve when `table` is
// null, else Volnitsky over `table`), every other clause confirmed per line, then the
// driver's `\u` lines merged in. `from` must be a line start.
static std::vector<LineSpan> prefilter_span(
    const uint8_t* buffer, size_t from, size_t to, const PrefilterPlan& plan,
    const MultiTable* table) {
    std::vector<LineSpan> hits;
    const PrefilterClause& driver = plan.clauses[0];
    if (table == nullptr) {
        simd_driver(buffer, from, to, plan, hits);
        goto escapes_merge;
    }
    {
    const MultiTable& t = *table;
    const size_t W = t.window;
    const size_t stride = W - 1;   // W >= 2: the gate never admits a needle under kMinNeedle

    for (size_t p = from + stride; p < to; ) {
        const uint16_t h = (static_cast<uint16_t>(buffer[p - 1]) << 8) | buffer[p];
        uint32_t e = t.head[h];
        bool found = false;
        size_t hs = 0;
        while (e) {
            const MultiTable::Entry& en = t.entries[e - 1];
            e = en.next;
            const size_t start = p - 1 - en.offset;
            const std::string& n = driver.needles[en.needle];
            if (start >= from && start + n.size() <= to &&
                std::memcmp(buffer + start, n.data(), n.size()) == 0) {
                found = true; hs = start; break;
            }
        }
        if (!found) { p += stride; continue; }
        const LineSpan line = line_at(buffer, from, to, hs);
        if (confirm_line(buffer, line.start, line.end, plan)) hits.push_back(line);
        p = static_cast<size_t>(line.end) + 1 + stride;   // past the decided line
    }
    }
escapes_merge:
    if (!driver.keep_unicode_escapes) return hits;

    // Lines the driver keeps for their `\u` escapes, still subject to every other clause;
    // merged in buffer order, one entry per line.
    std::vector<LineSpan> escapes = unicode_escape_lines(buffer, from, to);
    size_t kept = 0;
    for (const LineSpan& l : escapes)
        if (confirm_line(buffer, l.start, l.end, plan)) escapes[kept++] = l;
    escapes.resize(kept);
    if (escapes.empty()) return hits;
    std::vector<LineSpan> merged;
    merged.reserve(hits.size() + escapes.size());
    size_t i = 0, j = 0;
    while (i < hits.size() || j < escapes.size()) {
        if (j == escapes.size() || (i < hits.size() && hits[i].start < escapes[j].start)) {
            merged.push_back(hits[i++]);
        } else if (i == hits.size() || escapes[j].start < hits[i].start) {
            merged.push_back(escapes[j++]);
        } else {
            merged.push_back(hits[i++]);
            ++j;
        }
    }
    return merged;
}

namespace {

// Line-aligned end at or after `target` within [.., to]: one past the newline, or `to`.
inline size_t line_end_after(const uint8_t* buffer, size_t target, size_t to) {
    if (target >= to) return to;
    const void* nl = std::memchr(buffer + target, '\n', to - target);
    return nl ? static_cast<size_t>(static_cast<const uint8_t*>(nl) - buffer) + 1 : to;
}

constexpr size_t kWindow = static_cast<size_t>(4) << 20;   // re-decided every 4MB
constexpr size_t kProbe  = static_cast<size_t>(256) << 10; // the decision's sample per window

}  // namespace

std::vector<PrefilterSegment> prefilter_plan_segments(
    const uint8_t* buffer, size_t from, size_t to, const PrefilterPlan& plan) {
    require_chunk_length(to, "prefilter_plan_segments");   // LineSpan positions are uint32_t
    const PrefilterClause& driver = plan.clauses[0];
    // Driver: the SIMD sieve when the shortest needle is <= 16 bytes (Volnitsky's stride,
    // shortest - 1, is then small), Volnitsky otherwise. NEON builds only: the crossover is
    // measured on Apple Silicon (2026-10-05); no other target has data, so they keep
    // Volnitsky.
    size_t shortest = SIZE_MAX;
    for (const auto& n : driver.needles) shortest = n.size() < shortest ? n.size() : shortest;
#if defined(__ARM_NEON)
    const bool auto_simd = shortest <= 16;
#else
    const bool auto_simd = false;
#endif
    const bool use_simd = !plan.sieve.empty() && auto_simd;
    std::unique_ptr<MultiTable> table;
    if (!use_simd) table = std::make_unique<MultiTable>(driver.needles);

    // The gate judged the buffer's HEAD; data may be skewed (sorted, clustered). So each
    // line-aligned 4MB window is re-judged on its own first 256KB: if the probe's survivors
    // exceed 30% of its bytes the window is left UNFILTERED (parsed by the normal path)
    // — a non-selective region costs one probe, not a driver pass plus the per-line parse.
    // Adjacent windows of the same kind are coalesced into one segment.
    std::vector<PrefilterSegment> out;
    for (size_t wf = from; wf < to; ) {
        const size_t we = line_end_after(buffer, wf + kWindow, to);
        const size_t pe = line_end_after(buffer, wf + kProbe, we);
        std::vector<LineSpan> probe = prefilter_span(buffer, wf, pe, plan, table.get());
        size_t kept = 0;
        for (const LineSpan& l : probe) kept += l.end - l.start + 1;
        const bool filtered = kept * 10 <= (pe - wf) * 3;
        if (out.empty() || out.back().filtered != filtered) out.push_back({wf, we, filtered, {}});
        PrefilterSegment& seg = out.back();
        seg.to = we;
        if (filtered) {
            seg.lines.insert(seg.lines.end(), probe.begin(), probe.end());
            if (pe < we) {
                const std::vector<LineSpan> rest = prefilter_span(buffer, pe, we, plan, table.get());
                seg.lines.insert(seg.lines.end(), rest.begin(), rest.end());
            }
        }
        wf = we;
    }
    return out;
}

}  // namespace rugo::_jsonl

// ---------------------------------------------------------------------------------
// Shared by the Cython read_jsonl edge and the engine's native JSONL scan Source
// (src/cpp/engine/native_jsonl_scan_source.hpp): one implementation of the prefilter
// gate and of the malformed-record message, so the two readers cannot drift.
// ---------------------------------------------------------------------------------

#include "predicate_literal.hpp"   // LITERAL_STRING
#include "nested_column.hpp"       // parse_column_spec

namespace rugo::_jsonl {

namespace {

constexpr uint8_t kOpEq = 0;   // parse_context.hpp Predicate::op
constexpr uint8_t kOpIn = 6;
// Shortest needle armed: a quoted 2-byte value. Volnitsky needs a bigram, and a needle this
// short strides 3 bytes — whether that beats parsing is the selectivity sample's call.
constexpr size_t kMinNeedle = 4;

// A nested `->>` predicate compares the field's DECODED text (value_parser.cpp
// evaluate_nested_text): a JSON string unescaped, a number/boolean as its source token.
// A literal made only of these bytes — none of which JSON requires escaping, and whose
// only alternative spelling is a `\uXXXX` escape — therefore appears verbatim in every
// matching record that has no `\u` escape, whichever JSON type holds it. Length is not
// judged here: kMinNeedle bounds the needle, and the selectivity sample decides the payoff.
bool verbatim_safe(const std::string& v) {
    if (v.empty()) return false;
    for (const unsigned char c : v) {
        const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                        (c >= '0' && c <= '9') || c == '.' || c == '_' || c == ':' || c == '-';
        if (!ok) return false;
    }
    return true;
}

// Could `v` be the source token of a JSON number or boolean? `->>` renders those as their
// token, so a matching record may hold the value UNQUOTED. Anything else can only match a
// JSON string, whose quotes then sit directly around the value (verbatim_safe bytes need
// no escaping), so the quoted value is a sound — and far more selective — needle: it does
// not hit the value embedded in a longer string (a URI, a path). JSON null is absent here
// on purpose: `->>` of null is NULL, never the text "null".
bool json_scalar_token(const std::string& v) {
    if (v == "true" || v == "false") return true;
    size_t i = 0;
    const size_t n = v.size();
    auto digit = [&](size_t k) { return k < n && v[k] >= '0' && v[k] <= '9'; };
    if (i < n && v[i] == '-') ++i;
    if (!digit(i)) return false;
    if (v[i] == '0') ++i; else while (digit(i)) ++i;
    if (i < n && v[i] == '.') { ++i; if (!digit(i)) return false; while (digit(i)) ++i; }
    if (i < n && (v[i] == 'e' || v[i] == 'E')) {
        ++i;
        if (i < n && (v[i] == '+' || v[i] == '-')) ++i;
        if (!digit(i)) return false;
        while (digit(i)) ++i;
    }
    return i == n;
}

// Find `needle` in [hay, hay + n); SIZE_MAX if absent.
size_t find_bytes(const uint8_t* hay, size_t n, const uint8_t* needle, size_t m) {
    if (m == 0) return 0;
    if (m > n) return SIZE_MAX;
    for (size_t i = 0; i + m <= n; ++i) {
        const void* p = std::memchr(hay + i, needle[0], n - m - i + 1);
        if (p == nullptr) return SIZE_MAX;
        i = static_cast<size_t>(static_cast<const uint8_t*>(p) - hay);
        if (std::memcmp(hay + i, needle, m) == 0) return i;
    }
    return SIZE_MAX;
}

// One code point from UTF-8 under Python's errors='replace' (Unicode "maximal subpart"
// practice, which CPython follows): an ill-formed sequence becomes ONE U+FFFD covering the
// bytes consumed before the first byte that cannot continue it, and that byte is re-read.
uint32_t decode_one(const uint8_t* s, size_t n, size_t& i) {
    const uint8_t b0 = s[i];
    if (b0 < 0x80) { ++i; return b0; }
    size_t need;
    uint32_t cp;
    uint8_t lo = 0x80, hi = 0xBF;   // bounds on the SECOND byte
    if (b0 >= 0xC2 && b0 <= 0xDF)      { need = 1; cp = b0 & 0x1F; }
    else if (b0 >= 0xE0 && b0 <= 0xEF) { need = 2; cp = b0 & 0x0F;
        if (b0 == 0xE0) lo = 0xA0; else if (b0 == 0xED) hi = 0x9F; }
    else if (b0 >= 0xF0 && b0 <= 0xF4) { need = 3; cp = b0 & 0x07;
        if (b0 == 0xF0) lo = 0x90; else if (b0 == 0xF4) hi = 0x8F; }
    else { ++i; return 0xFFFD; }
    size_t j = i + 1;
    for (size_t k = 0; k < need; ++k, ++j) {
        if (j >= n) { i = j; return 0xFFFD; }
        const uint8_t b = s[j];
        const uint8_t l = (k == 0) ? lo : 0x80, h = (k == 0) ? hi : 0xBF;
        if (b < l || b > h) { i = j; return 0xFFFD; }
        cp = (cp << 6) | (b & 0x3F);
    }
    i = j;
    return cp;
}

// str.isprintable() for a non-ASCII code point. Python prints every code point outside
// categories Cc/Cf/Cs/Co/Cn/Zl/Zp/Zs; the ranges below are the assigned members of those
// categories that real data carries (C1 controls, the space separators, the format
// characters, private use). An UNASSIGNED code point (Cn) is printed raw here where repr()
// would escape it — the one place this can differ, and only in an error message.
bool printable_non_ascii(uint32_t c) {
    if (c <= 0xA0) return false;                       // C1 controls + NBSP
    if (c == 0xAD) return false;                       // soft hyphen
    if (c >= 0x600 && c <= 0x605) return false;
    if (c == 0x61C || c == 0x6DD || c == 0x70F || c == 0x180E) return false;
    if (c == 0x1680) return false;
    if (c >= 0x2000 && c <= 0x200F) return false;
    if (c >= 0x2028 && c <= 0x202F) return false;
    if (c >= 0x205F && c <= 0x206F) return false;
    if (c == 0x3000) return false;
    if (c >= 0xD800 && c <= 0xF8FF) return false;      // surrogates + private use
    if (c == 0xFEFF) return false;
    if (c >= 0xFFF9 && c <= 0xFFFB) return false;
    if (c == 0xFFFE || c == 0xFFFF) return false;
    if (c >= 0xF0000) return false;                    // supplementary private use
    return true;
}

void append_utf8(std::string& out, uint32_t c) {
    if (c < 0x80) { out.push_back(static_cast<char>(c)); }
    else if (c < 0x800) {
        out.push_back(static_cast<char>(0xC0 | (c >> 6)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    } else if (c < 0x10000) {
        out.push_back(static_cast<char>(0xE0 | (c >> 12)));
        out.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    } else {
        out.push_back(static_cast<char>(0xF0 | (c >> 18)));
        out.push_back(static_cast<char>(0x80 | ((c >> 12) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | ((c >> 6) & 0x3F)));
        out.push_back(static_cast<char>(0x80 | (c & 0x3F)));
    }
}

void append_hex_escape(std::string& out, uint32_t c) {
    static const char* hex = "0123456789abcdef";
    int digits;
    if (c < 0x100) { out += "\\x"; digits = 2; }
    else if (c < 0x10000) { out += "\\u"; digits = 4; }
    else { out += "\\U"; digits = 8; }
    for (int d = digits - 1; d >= 0; --d) out.push_back(hex[(c >> (4 * d)) & 0xF]);
}

}  // namespace

std::string py_str_repr(const uint8_t* bytes, size_t length) {
    std::vector<uint32_t> cps;
    cps.reserve(length);
    bool has_single = false, has_double = false;
    for (size_t i = 0; i < length;) {
        const uint32_t c = decode_one(bytes, length, i);
        has_single |= (c == '\'');
        has_double |= (c == '"');
        cps.push_back(c);
    }
    const char quote = (has_single && !has_double) ? '"' : '\'';
    std::string out;
    out.reserve(length + 2);
    out.push_back(quote);
    for (const uint32_t c : cps) {
        if (c == static_cast<uint32_t>(quote) || c == '\\') { out.push_back('\\'); out.push_back(static_cast<char>(c)); }
        else if (c == '\t') out += "\\t";
        else if (c == '\n') out += "\\n";
        else if (c == '\r') out += "\\r";
        else if (c < 0x20 || c == 0x7F) append_hex_escape(out, c);
        else if (c < 0x80) out.push_back(static_cast<char>(c));
        else if (printable_non_ascii(c)) append_utf8(out, c);
        else append_hex_escape(out, c);
    }
    out.push_back(quote);
    return out;
}

std::string malformed_error_message(const uint8_t* buffer, size_t length, size_t offset) {
    const size_t limit = offset < length ? offset : length;
    size_t line = 1;
    for (size_t i = 0; i < limit; ++i)
        if (buffer[i] == '\n') ++line;
    size_t snippet_end = offset;
    while (snippet_end < length && buffer[snippet_end] != '\n') ++snippet_end;
    const size_t start = offset < length ? offset : length;
    size_t snippet_len = snippet_end > start ? snippet_end - start : 0;
    if (snippet_len > 200) snippet_len = 200;
    return "Malformed JSONL at line " + std::to_string(line) + " (byte offset " +
           std::to_string(offset) + "): " + py_str_repr(buffer + start, snippet_len);
}

// The needle for string literal `v` on column `pred_column`, or false when it is not a
// sound needle (see choose_prefilter_plan).
static bool literal_needle(const uint8_t* buffer, size_t first_len, const std::string& column,
                           const ColumnSpec& spec, const std::string& v, std::string& needle) {
    if (spec.nested) {
        // `->` yields JSON, never compared with a string literal here; `->>` compares
        // decoded text — see verbatim_safe for why the unquoted value is sound.
        if (spec.as_json || !verbatim_safe(v)) return false;
        // Quoted unless a number/boolean token could hold it — see json_scalar_token.
        needle = json_scalar_token(v) ? v : "\"" + v + "\"";
        return needle.size() >= kMinNeedle;
    }
    // Top-level: only when the first record stores the column as a quoted (string) value.
    // A bare numeric/bool value isn't quoted, so a quoted needle would false-negative.
    std::string key;
    key.reserve(column.size() + 3);
    key.push_back('"');
    key += column;
    key += "\":";
    const size_t ki = find_bytes(buffer, first_len,
                                 reinterpret_cast<const uint8_t*>(key.data()), key.size());
    if (ki == SIZE_MAX) return false;            // key absent / non-compact formatting
    const size_t vpos = ki + key.size();
    if (vpos >= first_len || buffer[vpos] != '"') return false;   // bare value: numeric hazard
    needle.clear();
    needle.reserve(v.size() + 2);
    needle.push_back('"');
    needle += v;
    needle.push_back('"');
    return needle.size() >= kMinNeedle;
}

bool choose_prefilter_plan(const uint8_t* buffer, size_t length, const ParseContext& context,
                           PrefilterPlan& out) {
    // The first record, for the top-level stored-as-string probe. Bounded to 4KB — real
    // JSONL lines are far shorter than that.
    const size_t first_window = length < 4096 ? length : 4096;
    const void* nl = std::memchr(buffer, '\n', first_window);
    const size_t first_len = nl ? static_cast<size_t>(static_cast<const uint8_t*>(nl) - buffer)
                                : first_window;

    std::vector<PrefilterClause> clauses;
    for (const Predicate& pred : context.predicates) {
        // `=` (one literal) or IN (any of its members). Every literal must be a string: a
        // non-string literal is a type mismatch against a string column, and prefiltering
        // on it would drop every record before evaluate_predicate could raise.
        std::vector<const Predicate*> literals;
        if (pred.op == kOpEq && pred.members.empty()) literals.push_back(&pred);
        else if (pred.op == kOpIn && !pred.members.empty())
            for (const Predicate& m : pred.members) literals.push_back(&m);
        else continue;

        const ColumnSpec spec = parse_column_spec(pred.column);
        PrefilterClause clause;
        clause.keep_unicode_escapes = spec.nested;
        bool ok = true;
        for (const Predicate* lit : literals) {
            std::string needle;
            if (lit->kind != LITERAL_STRING ||
                !literal_needle(buffer, first_len, pred.column, spec, lit->value, needle)) {
                ok = false; break;
            }
            clause.needles.push_back(std::move(needle));
        }
        if (ok) clauses.push_back(std::move(clause));
    }
    if (clauses.empty()) return false;

    // Selectivity sample on the first ~1MB: the clause hitting the fewest sampled records
    // drives, the rest confirm; if the whole plan still keeps >30% there is little to skip —
    // run the normal path instead of prefiltering.
    const size_t sample_len = length < 1000000 ? length : 1000000;
    size_t sample_lines = 0;
    for (size_t i = 0; i < sample_len; ++i) sample_lines += (buffer[i] == '\n');
    std::vector<std::pair<size_t, size_t>> rank;   // (sampled hits, clause index)
    for (size_t c = 0; c < clauses.size(); ++c) {
        PrefilterPlan one;
        one.clauses.push_back(clauses[c]);
        const MultiTable t(one.clauses[0].needles);
        rank.push_back({prefilter_span(buffer, 0, sample_len, one, &t).size(), c});
    }
    std::sort(rank.begin(), rank.end());
    PrefilterPlan plan;
    for (const auto& r : rank) plan.clauses.push_back(std::move(clauses[r.second]));
    {
        size_t hist[256] = {0};
        for (size_t i = 0; i < sample_len; ++i) ++hist[buffer[i]];
        for (const std::string& n : plan.clauses[0].needles) {
            uint32_t a = 0, b = 1;
            // two distinct positions with the lowest sample frequency
            std::vector<uint32_t> idx(n.size());
            for (uint32_t i = 0; i < n.size(); ++i) idx[i] = i;
            std::sort(idx.begin(), idx.end(), [&](uint32_t x, uint32_t y) {
                return hist[static_cast<uint8_t>(n[x])] < hist[static_cast<uint8_t>(n[y])]; });
            a = idx[0];
            for (size_t i = 1; i < idx.size(); ++i) if (n[idx[i]] != n[a] || i + 1 == idx.size()) { b = idx[i]; break; }
            plan.sieve.push_back({a, b});
        }
    }
    const size_t matched = plan.clauses.size() == 1
        ? rank[0].first
        : [&] { const MultiTable t(plan.clauses[0].needles);
                return prefilter_span(buffer, 0, sample_len, plan, &t).size(); }();
    if (sample_lines > 0 && matched * 10 > sample_lines * 3) return false;
    out = std::move(plan);
    return true;
}

bool maybe_prefilter(const uint8_t* buffer, size_t length, const ParseContext& context,
                     std::vector<uint8_t>& out) {
    PrefilterPlan plan;
    if (!choose_prefilter_plan(buffer, length, context, plan)) return false;
    out.clear();
    out.reserve(length / 16);
    for (const PrefilterSegment& seg : prefilter_plan_segments(buffer, 0, length, plan)) {
        if (!seg.filtered) {   // non-selective window(s): every byte, whole lines
            out.insert(out.end(), buffer + seg.from, buffer + seg.to);
            if (buffer[seg.to - 1] != '\n') out.push_back('\n');
            continue;
        }
        for (const LineSpan& line : seg.lines) {
            out.insert(out.end(), buffer + line.start, buffer + line.end);
            out.push_back('\n');
        }
    }
    return true;
}

}  // namespace rugo::_jsonl
