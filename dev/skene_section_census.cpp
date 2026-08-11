// Section census for a .skene file — what the writer's compression gates
// actually cost, measured per section rather than argued.
//
// writer.cpp's emit_encoded puts four gates in front of the codec, and each one
// silently declines some bytes. This walks the REAL section directory (not a
// blind chunking of the file, which is what dev/skene_codec_bench.cpp does) and
// answers, for one file:
//
//   - what is stored compressed today, and at what ratio
//   - for every section the gates DECLINED, which gate declined it and what LZ4
//     would actually have done to those bytes
//   - how the recoverable total moves as kCompressMinBytes is swept down
//   - what decompress CPU each recovery would add, at this machine's measured
//     LZ4 rate rather than a quoted one
//
// It reads only the directory and the section bodies it tests; it never decodes
// a column and never writes anything.
//
// Section bodies are compressed VERBATIM as stored — for a kBitpack section that
// means the BitpackHeader plus the packed body, which is exactly what a writer
// admitting bit-packed sections would hand the codec.
//
// ⚠ The encoding gate cannot simply be loosened. SectionEntry.encoding is ONE
// field holding mutually exclusive values, so there is no way to spell
// "bit-packed AND then LZ4'd" in v1 — admitting those sections is a format
// change (a new encoding on a REQUIRED section bumps kVersion, format.h). This
// tool sizes the prize BEFORE anyone pays that price; it does not assume the
// change is free.
//
// Dev tooling only — never imported by production code (repo rules §5).

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "lz4.h"
#include "skene/format.h"

using namespace skene;
using Clock = std::chrono::steady_clock;

static double ms_between(Clock::time_point a, Clock::time_point b) {
    return std::chrono::duration<double, std::milli>(b - a).count();
}

[[noreturn]] static void die(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    std::fprintf(stderr, "skene_section_census: ");
    std::vfprintf(stderr, fmt, ap);
    std::fprintf(stderr, "\n");
    va_end(ap);
    std::exit(1);
}

// ─── Bounds-checked cursor ──────────────────────────────────────────────────
//
// The format's rule is that nothing is interpreted before it is validated
// (format.h §"Validation order is TOTAL"). This is a read-only census, so it
// does not verify checksums — but every take() is bounds-checked, because a
// corrupt length read as a real one is an out-of-bounds read, not a wrong number.
class Cursor {
  public:
    Cursor(const uint8_t* base, size_t bytes) : p_(base), end_(base + bytes) {}

    template <typename T>
    bool take(T* out) {
        if (static_cast<size_t>(end_ - p_) < sizeof(T)) return false;
        std::memcpy(out, p_, sizeof(T));
        p_ += sizeof(T);
        return true;
    }

    const uint8_t* raw(size_t n) {
        if (static_cast<size_t>(end_ - p_) < n) return nullptr;
        const uint8_t* r = p_;
        p_ += n;
        return r;
    }

    size_t remaining() const { return static_cast<size_t>(end_ - p_); }

  private:
    const uint8_t* p_;
    const uint8_t* end_;
};

// ─── Names for the enums, so the report reads like the format ───────────────

static const char* kind_name(uint16_t k) {
    switch (static_cast<SectionKind>(k)) {
        case SectionKind::kData:        return "DATA";
        case SectionKind::kSelection:   return "SELECTION";
        case SectionKind::kValidity:    return "VALIDITY";
        case SectionKind::kStringSlots: return "STRING_SLOTS";
        case SectionKind::kStringArena: return "STRING_ARENA";
        case SectionKind::kBloom:       return "BLOOM";
        case SectionKind::kPermutation: return "PERMUTATION";
        case SectionKind::kZoneMap:     return "ZONE_MAP";
    }
    return "UNKNOWN";
}

static const char* encoding_name(uint16_t e) {
    switch (static_cast<Encoding>(e)) {
        case Encoding::kPlain:        return "PLAIN";
        case Encoding::kBitpack:      return "BITPACK";
        case Encoding::kDeltaBitpack: return "DELTA_BITPACK";
        case Encoding::kZstd:         return "ZSTD";
        case Encoding::kLz4:          return "LZ4";
    }
    return "UNKNOWN";
}

// Why a section is not stored compressed. Mirrors emit_encoded's gate ORDER, so
// each section is attributed to the FIRST gate that would have rejected it —
// the same one that actually did.
enum class Decline { kNotDeclined = 0, kEncoding, kKind, kSize, kResult, kCount };

static const char* decline_name(Decline d) {
    switch (d) {
        case Decline::kNotDeclined: return "compressed";
        case Decline::kEncoding:    return "encoding gate";
        case Decline::kKind:        return "kind gate";
        case Decline::kSize:        return "size gate";
        case Decline::kResult:      return "result gate";
        case Decline::kCount:       break;
    }
    return "?";
}

struct Section {
    SectionEntry entry{};
    std::string  column;        // owning column, "" if unattributed
    uint32_t     row_group = 0;
    Decline      decline = Decline::kNotDeclined;
    uint64_t     lz4_bytes = 0; // what LZ4 makes of stored_bytes
    bool         tested = false;
};

// ─── Footer parsing (mirrors reader_v1.cpp's order exactly) ─────────────────

// child_count is trusted only as a loop bound — the cursor is checked on every
// read, so a corrupt count truncates rather than escapes.
static void parse_column(Cursor& cursor,
                         std::vector<std::pair<std::string, ColumnEntryHead>>* out,
                         int depth) {
    if (depth > 32) die("column nesting exceeds 32 levels");

    ColumnEntryHead head{};
    if (!cursor.take(&head)) die("footer ends inside a column directory entry");

    const uint8_t* name = cursor.raw(head.name_bytes);
    if (name == nullptr)
        die("column name claims %u bytes, only %zu remain", head.name_bytes, cursor.remaining());
    std::string column_name(reinterpret_cast<const char*>(name), head.name_bytes);

    if (head.logical_present) {
        LogicalTypeDescriptor logical{};
        if (!cursor.take(&logical)) die("footer ends inside a logical type descriptor");
    }

    const uint32_t children = head.child_count;
    out->emplace_back(std::move(column_name), head);
    for (uint32_t i = 0; i < children; ++i) parse_column(cursor, out, depth + 1);
}

// Statistics blobs are located by ORDER, not by offset, and must be consumed to
// keep the cursor aligned — even though the census does not use them.
static void skip_statistics(Cursor& cursor,
                            const std::vector<std::pair<std::string, ColumnEntryHead>>& columns) {
    for (const auto& c : columns) {
        const uint32_t declared = c.second.stats_bytes;
        if (declared == 0) continue;
        if (cursor.raw(declared) == nullptr)
            die("footer ends inside column '%s' statistics", c.first.c_str());
    }
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr,
            "usage: skene_section_census <file.skene> [--floor PCT] [--top N]\n"
            "  --floor PCT  keep-compressed floor, percent of original (default 85)\n"
            "  --top N      list the N largest recovery opportunities (default 15)\n");
        return 1;
    }

    const char* path = argv[1];
    double floor_pct = 85.0;
    int top_n = 15;
    for (int i = 2; i < argc; ++i) {
        if (std::strcmp(argv[i], "--floor") == 0 && i + 1 < argc) floor_pct = std::atof(argv[++i]);
        else if (std::strcmp(argv[i], "--top") == 0 && i + 1 < argc) top_n = std::atoi(argv[++i]);
        else die("unrecognised argument '%s'", argv[i]);
    }
    const double floor_ratio = floor_pct / 100.0;

    // ── map the file ────────────────────────────────────────────────────────
    int fd = open(path, O_RDONLY);
    if (fd < 0) die("cannot open %s", path);
    struct stat st {};
    if (fstat(fd, &st) != 0) die("cannot stat %s", path);
    const size_t file_bytes = static_cast<size_t>(st.st_size);
    if (file_bytes < kMinFileBytes) die("%s is too small to be a .skene file", path);

    const uint8_t* file = static_cast<const uint8_t*>(
        mmap(nullptr, file_bytes, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    if (file == MAP_FAILED) die("cannot mmap %s", path);

    // ── head / tail ─────────────────────────────────────────────────────────
    FileHead head{};
    std::memcpy(&head, file, sizeof(head));
    if (head.magic != kMagic) die("%s: bad head magic (not a .skene file)", path);
    if (!version_is_supported(head.version))
        die("%s: format version %u, this tool understands %u..%u",
            path, head.version, kMinReadVersion, kVersion);

    FileTail tail{};
    std::memcpy(&tail, file + file_bytes - kFileTailBytes, sizeof(tail));
    if (tail.magic != kMagic) die("%s: bad tail magic", path);
    if (tail.version != head.version) die("%s: head/tail version disagree", path);

    const size_t footer_bytes = tail.footer_bytes;
    if (footer_bytes + kFileTailBytes > file_bytes)
        die("%s: file footer length %zu does not fit the object", path, footer_bytes);
    const size_t footer_offset = file_bytes - kFileTailBytes - footer_bytes;

    // ── file footer: row group directory ────────────────────────────────────
    Cursor fc(file + footer_offset, footer_bytes);
    FileFooterHeader fh{};
    if (!fc.take(&fh)) die("file footer too small for its header");
    if (fh.footer_magic != kFileFooterMagic)
        die("%s: file footer magic is 0x%08X, not 0x%08X — regenerate this file "
            "(it predates the multi-row-group layout)", path, fh.footer_magic, kFileFooterMagic);
    if (fc.raw(fh.writer_tag_bytes) == nullptr) die("file footer ends inside the writer tag");

    std::vector<RowGroupEntry> row_groups(fh.row_group_count);
    for (uint32_t i = 0; i < fh.row_group_count; ++i)
        if (!fc.take(&row_groups[i])) die("file footer ends inside the row group directory");

    // ── walk every row group's section directory ────────────────────────────
    std::vector<Section> sections;
    for (uint32_t g = 0; g < row_groups.size(); ++g) {
        const RowGroupEntry& rg = row_groups[g];
        if (rg.footer_offset + rg.footer_bytes > file_bytes)
            die("row group %u footer extends past the end of the file", g);

        Cursor rc(file + rg.footer_offset, rg.footer_bytes);
        RowGroupFooterHeader rh{};
        if (!rc.take(&rh)) die("row group %u footer too small for its header", g);
        if (rc.raw(rh.writer_tag_bytes) == nullptr)
            die("row group %u footer ends inside the writer tag", g);

        std::vector<std::pair<std::string, ColumnEntryHead>> columns;
        for (uint32_t i = 0; i < rh.column_count; ++i) parse_column(rc, &columns, 0);

        std::vector<SectionEntry> entries(rh.section_count);
        for (uint32_t i = 0; i < rh.section_count; ++i)
            if (!rc.take(&entries[i]))
                die("row group %u footer ends inside the section directory", g);

        skip_statistics(rc, columns);

        // Attribute each section to its owning column via the two directory
        // slices the column entry carries: required sections live in the data
        // region, optional ones in the index region.
        std::vector<std::string> owner(entries.size());
        for (const auto& c : columns) {
            const ColumnEntryHead& h = c.second;
            for (uint32_t i = 0; i < h.section_count; ++i) {
                const uint64_t idx = static_cast<uint64_t>(h.section_index) + i;
                if (idx < owner.size()) owner[idx] = c.first;
            }
            for (uint32_t i = 0; i < h.index_section_count; ++i) {
                const uint64_t idx = static_cast<uint64_t>(h.index_section_index) + i;
                if (idx < owner.size()) owner[idx] = c.first;
            }
        }

        for (size_t i = 0; i < entries.size(); ++i) {
            if (entries[i].offset + entries[i].stored_bytes > file_bytes)
                die("row group %u section %zu extends past the end of the file", g, i);
            Section s;
            s.entry     = entries[i];
            s.column    = owner[i];
            s.row_group = g;
            sections.push_back(std::move(s));
        }
    }

    // ── classify: which gate declined each uncompressed section ─────────────
    size_t codec_sections = 0;
    for (Section& s : sections) {
        const uint16_t enc = s.entry.encoding;
        if (enc == static_cast<uint16_t>(Encoding::kZstd) ||
            enc == static_cast<uint16_t>(Encoding::kLz4)) {
            ++codec_sections;
            continue;
        }
        if (enc != static_cast<uint16_t>(Encoding::kPlain)) s.decline = Decline::kEncoding;
        else if (!kind_is_compressible(s.entry.kind))       s.decline = Decline::kKind;
        else if (s.entry.stored_bytes < kCompressMinBytes)  s.decline = Decline::kSize;
        else                                                s.decline = Decline::kResult;
    }

    // ── measure: what LZ4 does to every declined section ────────────────────
    //
    // One compression pass. Sections that clear the floor have their compressed
    // bytes RETAINED, so the decompress timing below reuses them instead of
    // compressing the corpus a second time.
    std::vector<uint8_t> scratch;
    std::vector<std::pair<std::vector<uint8_t>, uint64_t>> corpus;  // {packed, plain_bytes}
    double compress_ms = 0.0;
    uint64_t tested_bytes = 0, kept_plain_bytes = 0;

    for (Section& s : sections) {
        if (s.decline == Decline::kNotDeclined) continue;
        const uint64_t n = s.entry.stored_bytes;
        if (n == 0 || n > static_cast<uint64_t>(LZ4_MAX_INPUT_SIZE)) continue;

        const int bound = LZ4_compressBound(static_cast<int>(n));
        if (bound <= 0) continue;
        if (scratch.size() < static_cast<size_t>(bound)) scratch.resize(bound);

        const auto t0 = Clock::now();
        const int produced = LZ4_compress_default(
            reinterpret_cast<const char*>(file + s.entry.offset),
            reinterpret_cast<char*>(scratch.data()), static_cast<int>(n), bound);
        compress_ms += ms_between(t0, Clock::now());
        if (produced <= 0) continue;

        s.lz4_bytes = static_cast<uint64_t>(produced);
        s.tested = true;
        tested_bytes += n;

        if (static_cast<double>(s.lz4_bytes) < floor_ratio * static_cast<double>(n)) {
            corpus.emplace_back(std::vector<uint8_t>(scratch.begin(), scratch.begin() + produced), n);
            kept_plain_bytes += n;
        }
    }

    // ── measure this machine's LZ4 decompress rate on that exact corpus ─────
    double decomp_ms = 0.0, decomp_mbs = 0.0;
    if (kept_plain_bytes > 0) {
        size_t widest = 0;
        for (const auto& c : corpus) widest = std::max<size_t>(widest, static_cast<size_t>(c.second));
        std::vector<uint8_t> out(widest);
        double best = 1e30;
        for (int rep = 0; rep < 3; ++rep) {
            const auto t0 = Clock::now();
            for (const auto& c : corpus)
                LZ4_decompress_safe(reinterpret_cast<const char*>(c.first.data()),
                                    reinterpret_cast<char*>(out.data()),
                                    static_cast<int>(c.first.size()),
                                    static_cast<int>(c.second));
            best = std::min(best, ms_between(t0, Clock::now()));
        }
        decomp_ms = best;
        decomp_mbs = (kept_plain_bytes / 1e6) / (decomp_ms / 1e3);
    }

    // ── report ──────────────────────────────────────────────────────────────
    std::printf("skene section census — %s\n", path);
    std::printf("  %u row groups, %" PRIu64 " rows, %u columns, %zu sections, %.1f MB on disk\n\n",
                fh.row_group_count, fh.row_count, fh.column_count, sections.size(), file_bytes / 1e6);

    if (codec_sections == 0)
        std::printf("  ! NO section in this file is stored under a codec — it was written with\n"
                    "    SectionCodec::kNone. The gate attribution below therefore describes what\n"
                    "    the gates WOULD decline, not what they did.\n\n");

    {   // Inventory by encoding, as written.
        struct Agg { size_t count = 0; uint64_t stored = 0, plain = 0; };
        Agg by_enc[8];
        for (const Section& s : sections) {
            const uint16_t e = s.entry.encoding < 8 ? s.entry.encoding : 0;
            by_enc[e].count++;
            by_enc[e].stored += s.entry.stored_bytes;
            by_enc[e].plain  += s.entry.plain_bytes;
        }
        std::printf("SECTION INVENTORY (as written)\n");
        std::printf("  %-15s %8s %12s %12s %8s\n", "encoding", "count", "stored MB", "plain MB", "ratio");
        for (int e = 0; e < 8; ++e) {
            if (by_enc[e].count == 0) continue;
            const double ratio = by_enc[e].stored ? (double)by_enc[e].plain / (double)by_enc[e].stored : 0.0;
            std::printf("  %-15s %8zu %12.1f %12.1f %7.2fx\n",
                        encoding_name(static_cast<uint16_t>(e)), by_enc[e].count,
                        by_enc[e].stored / 1e6, by_enc[e].plain / 1e6, ratio);
        }
        std::printf("\n");
    }

    {   // What each gate declined, and what recovering it is worth.
        struct Agg { size_t count = 0, kept = 0; uint64_t stored = 0, kept_plain = 0, kept_packed = 0; };
        Agg by_gate[static_cast<int>(Decline::kCount)];
        for (const Section& s : sections) {
            if (s.decline == Decline::kNotDeclined) continue;
            Agg& a = by_gate[static_cast<int>(s.decline)];
            a.count++;
            a.stored += s.entry.stored_bytes;
            if (!s.tested) continue;
            if (static_cast<double>(s.lz4_bytes) >= floor_ratio * (double)s.entry.stored_bytes) continue;
            a.kept++;
            a.kept_plain  += s.entry.stored_bytes;
            a.kept_packed += s.lz4_bytes;
        }
        std::printf("WHAT THE GATES DECLINED  (LZ4 measured on the stored bodies, floor %.0f%%)\n", floor_pct);
        std::printf("  %-15s %8s %12s %10s %12s %8s %10s\n",
                    "gate", "sections", "declined MB", "clear fl.", "SAVED MB", "ratio", "decomp ms");
        for (int d = 1; d < static_cast<int>(Decline::kCount); ++d) {
            const Agg& a = by_gate[d];
            if (a.count == 0) continue;
            const double ratio = a.kept_packed ? (double)a.kept_plain / (double)a.kept_packed : 0.0;
            const double added_ms = decomp_mbs > 0 ? (a.kept_plain / 1e6) / decomp_mbs * 1e3 : 0.0;
            std::printf("  %-15s %8zu %12.1f %10zu %12.1f %7.2fx %10.1f\n",
                        decline_name(static_cast<Decline>(d)), a.count, a.stored / 1e6,
                        a.kept, (a.kept_plain - a.kept_packed) / 1e6, ratio, added_ms);
        }
        std::printf("\n  SAVED counts ONLY sections that clear the %.0f%% floor; 'decomp ms' is the\n"
                    "  read-side CPU those bytes would add per full scan of this file.\n\n", floor_pct);

        // Which KINDS the kind gate is actually turning away. This decides the
        // cost of admitting them: a PLAIN section needs only kind_is_compressible
        // to change, whereas a bit-packed one cannot be spelled compressed at all
        // in v1 (SectionEntry.encoding is one field) and needs a format change.
        std::printf("  kind gate, broken out by kind and encoding:\n");
        std::printf("    %-14s %-14s %8s %12s %12s %8s %9s %9s %9s\n",
                    "kind", "encoding", "sections", "declined MB", "SAVED MB", "ratio",
                    "min KB", "med KB", "max KB");
        struct KeyAgg {
            size_t count = 0; uint64_t stored = 0, kept_plain = 0, kept_packed = 0;
            std::vector<uint64_t> sizes;  // to tell a uniform population from a varied one
        };
        std::vector<std::pair<std::pair<uint16_t, uint16_t>, KeyAgg>> rows;
        for (const Section& s : sections) {
            if (s.decline != Decline::kKind) continue;
            const auto key = std::make_pair(s.entry.kind, s.entry.encoding);
            auto it = std::find_if(rows.begin(), rows.end(),
                                   [&](const auto& r) { return r.first == key; });
            if (it == rows.end()) { rows.push_back({key, KeyAgg{}}); it = rows.end() - 1; }
            it->second.count++;
            it->second.stored += s.entry.stored_bytes;
            it->second.sizes.push_back(s.entry.stored_bytes);
            if (s.tested && static_cast<double>(s.lz4_bytes) < floor_ratio * (double)s.entry.stored_bytes) {
                it->second.kept_plain  += s.entry.stored_bytes;
                it->second.kept_packed += s.lz4_bytes;
            }
        }
        std::sort(rows.begin(), rows.end(), [](const auto& a, const auto& b) {
            return (a.second.kept_plain - a.second.kept_packed) >
                   (b.second.kept_plain - b.second.kept_packed);
        });
        for (auto& r : rows) {
            const double ratio = r.second.kept_packed
                               ? (double)r.second.kept_plain / (double)r.second.kept_packed : 0.0;
            auto& z = r.second.sizes;
            std::sort(z.begin(), z.end());
            std::printf("    %-14s %-14s %8zu %12.1f %12.1f %7.2fx %9.1f %9.1f %9.1f\n",
                        kind_name(r.first.first), encoding_name(r.first.second), r.second.count,
                        r.second.stored / 1e6,
                        (r.second.kept_plain - r.second.kept_packed) / 1e6, ratio,
                        z.front() / 1e3, z[z.size() / 2] / 1e3, z.back() / 1e3);
        }
        std::printf("\n");
    }

    {   // kCompressMinBytes sweep, over the exact population the size gate governs.
        std::printf("kCompressMinBytes SWEEP  (kind-eligible PLAIN sections, floor %.0f%%)\n", floor_pct);
        std::printf("  %10s %10s %12s %12s\n", "threshold", "sections", "SAVED MB", "decomp ms");
        const uint64_t thresholds[] = {kCompressMinBytes, 8192, 4096, 2048, 1024, 512, 256, 0};
        for (uint64_t t : thresholds) {
            size_t n = 0;
            uint64_t saved = 0, plain = 0;
            for (const Section& s : sections) {
                if (!s.tested) continue;
                if (s.entry.encoding != static_cast<uint16_t>(Encoding::kPlain)) continue;
                if (!kind_is_compressible(s.entry.kind)) continue;
                if (s.entry.stored_bytes < t) continue;
                if (static_cast<double>(s.lz4_bytes) >= floor_ratio * (double)s.entry.stored_bytes) continue;
                ++n;
                saved += s.entry.stored_bytes - s.lz4_bytes;
                plain += s.entry.stored_bytes;
            }
            const double added_ms = decomp_mbs > 0 ? (plain / 1e6) / decomp_mbs * 1e3 : 0.0;
            std::printf("  %10" PRIu64 " %10zu %12.1f %12.1f\n", t, n, saved / 1e6, added_ms);
        }
        std::printf("\n  The current threshold is the first row. Every row below it is what\n"
                    "  lowering the gate to that value would buy, and cost.\n\n");
    }

    {   // The largest single opportunities, so a per-column decision is possible.
        std::vector<const Section*> ranked;
        for (const Section& s : sections)
            if (s.tested && static_cast<double>(s.lz4_bytes) < floor_ratio * (double)s.entry.stored_bytes)
                ranked.push_back(&s);
        std::sort(ranked.begin(), ranked.end(), [](const Section* a, const Section* b) {
            return (a->entry.stored_bytes - a->lz4_bytes) > (b->entry.stored_bytes - b->lz4_bytes);
        });
        std::printf("TOP %d RECOVERY OPPORTUNITIES\n", top_n);
        std::printf("  %-26s %-13s %-14s %-14s %10s %10s %7s\n",
                    "column", "kind", "encoding", "declined by", "stored KB", "saved KB", "ratio");
        for (int i = 0; i < top_n && i < static_cast<int>(ranked.size()); ++i) {
            const Section* s = ranked[i];
            std::printf("  %-26.26s %-13s %-14s %-14s %10.1f %10.1f %6.2fx\n",
                        s->column.empty() ? "(unattributed)" : s->column.c_str(),
                        kind_name(s->entry.kind), encoding_name(s->entry.encoding),
                        decline_name(s->decline),
                        s->entry.stored_bytes / 1e3,
                        (s->entry.stored_bytes - s->lz4_bytes) / 1e3,
                        (double)s->entry.stored_bytes / (double)s->lz4_bytes);
        }
        std::printf("\n");
    }

    if (kept_plain_bytes > 0) {
        std::printf("COST SUMMARY (this machine, decompress best of 3)\n");
        std::printf("  compress    %8.1f MB tested at %7.0f MB/s   (write-side, paid once)\n",
                    tested_bytes / 1e6, (tested_bytes / 1e6) / (compress_ms / 1e3));
        std::printf("  decompress  %8.1f MB kept   at %7.0f MB/s   = %.1f ms per full read of this file\n",
                    kept_plain_bytes / 1e6, decomp_mbs, decomp_ms);
        std::printf("  net bytes   %8.1f MB saved on disk\n\n",
                    (kept_plain_bytes - [&] {
                        uint64_t p = 0; for (const auto& c : corpus) p += c.first.size(); return p;
                    }()) / 1e6);
    }

    munmap(const_cast<uint8_t*>(file), file_bytes);
    return 0;
}

// Build (from the repo root):
//
//   c++ -std=c++17 -O2 -DNDEBUG -o /tmp/skene_section_census \
//     dev/skene_section_census.cpp third_party/lz4/lz4.c \
//     -I third_party/lz4 -I skene/include
//
//   /tmp/skene_section_census scratch/hits_skene/hits_skene-0000.skene
//
// Dev tooling only — never imported by production code (repo rules §5).
