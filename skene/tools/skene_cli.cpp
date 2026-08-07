// skene — command line tool.
//
//   skene version              what this build writes and reads
//   skene info    <file>       inspect a file without materializing its columns
//   skene demo    <file>       write a sample file exercising every family
//
// `info` is the tool the migration story needs: it identifies a file whose
// version this build cannot read, because that is exactly when an operator has
// to work out which retained binary to fetch.

#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include "skene/file_io.h"
#include "skene/format.h"
#include "skene/probe.h"
#include "skene/reader.h"
#include "skene/writer.h"

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/vector_owner.h"
#include "logical_type.h"

using namespace skene;

// ─── Display helpers ────────────────────────────────────────────────────────

static const char* type_name(uint32_t type) {
    switch (static_cast<DrakenType>(type)) {
        case DRAKEN_INT8:        return "INT8";
        case DRAKEN_INT16:       return "INT16";
        case DRAKEN_INT32:       return "INT32";
        case DRAKEN_INT64:       return "INT64";
        case DRAKEN_UINT8:       return "UINT8";
        case DRAKEN_UINT16:      return "UINT16";
        case DRAKEN_UINT32:      return "UINT32";
        case DRAKEN_UINT64:      return "UINT64";
        case DRAKEN_FLOAT32:     return "FLOAT32";
        case DRAKEN_FLOAT64:     return "FLOAT64";
        case DRAKEN_DECIMAL:     return "DECIMAL";
        case DRAKEN_DECIMAL128:  return "DECIMAL128";
        case DRAKEN_DATE32:      return "DATE32";
        case DRAKEN_TIMESTAMP64: return "TIMESTAMP64";
        case DRAKEN_TIME32:      return "TIME32";
        case DRAKEN_TIME64:      return "TIME64";
        case DRAKEN_INTERVAL:    return "INTERVAL";
        case DRAKEN_BOOL:        return "BOOL";
        case DRAKEN_VARCHAR:     return "VARCHAR";
        case DRAKEN_NVARCHAR:    return "NVARCHAR";
        case DRAKEN_VARBINARY:   return "VARBINARY";
        case DRAKEN_VARIANT:     return "VARIANT";
        case DRAKEN_ARRAY:       return "ARRAY";
        case DRAKEN_NULL:        return "NULL";
        case DRAKEN_VECTOR_FP16: return "VECTOR_FP16";
        default:                 return "UNKNOWN";
    }
}

// The whole point of the format: a UINT32 that is an IPv4 address says so here,
// where Parquet would have nothing to say.
static std::string logical_name(const ColumnMetadata& column) {
    if (!column.logical_present) return "-";
    char buffer[64];
    switch (static_cast<LogicalKind>(column.logical.kind)) {
        case LogicalKind::TIMESTAMP:
            std::snprintf(buffer, sizeof(buffer), "TIMESTAMP(unit=%u, offset=%d)",
                          column.logical.unit, column.logical.offset_minutes);
            return buffer;
        case LogicalKind::TIME:
            std::snprintf(buffer, sizeof(buffer), "TIME(unit=%u)", column.logical.unit);
            return buffer;
        case LogicalKind::DECIMAL:
            std::snprintf(buffer, sizeof(buffer), "DECIMAL(%u,%u)",
                          column.logical.precision, column.logical.scale);
            return buffer;
        case LogicalKind::VECTOR:
            std::snprintf(buffer, sizeof(buffer), "VECTOR(%u)", column.logical.dimension);
            return buffer;
        case LogicalKind::IPV4:  return "IPV4";
        case LogicalKind::NONE:  return "NONE";
    }
    return "?";
}

static const char* selection_name(SelectionKind kind) {
    switch (kind) {
        case SelectionKind::kConstant: return "constant";
        case SelectionKind::kIdentity: return "identity";
        case SelectionKind::kStored:   return "stored";
    }
    return "?";
}

static void print_column(const ColumnMetadata& column, int indent) {
    std::printf("%*s%-24s %-12s %-24s rows=%-8u distinct=%-8u %-9s %s\n",
                indent, "", column.name.c_str(), type_name(column.type),
                logical_name(column).c_str(), column.length, column.data_length,
                selection_name(column.selection_kind),
                column.value_order == ValueOrder::kAscending ? "ascending" : "as-written");

    std::printf("%*s  bytes [%" PRIu64 ", %" PRIu64 ")  field_id=%u  flags=0x%02x\n",
                indent, "", column.byte_offset,
                column.byte_offset + column.byte_bytes, column.field_id,
                column.vector_flags);

    if (column.zone_map.present()) {
        // Skips all-null chunks, which carry an empty range by design and would
        // otherwise drag the reported span out to the full int64 domain.
        int64_t lo = INT64_MAX, hi = INT64_MIN;
        size_t empty = 0;
        for (const ZoneMapEntry& e : column.zone_map.chunks) {
            if (e.min_ordinal > e.max_ordinal) { ++empty; continue; }
            if (e.min_ordinal < lo) lo = e.min_ordinal;
            if (e.max_ordinal > hi) hi = e.max_ordinal;
        }
        if (empty == column.zone_map.chunks.size())
            std::printf("%*s  zone map: %zu chunks of %u rows, all null\n",
                        indent, "", column.zone_map.chunks.size(),
                        column.zone_map.chunk_rows);
        else
            std::printf("%*s  zone map: %zu chunks of %u rows, ordinals "
                        "[%lld, %lld]%s\n",
                        indent, "", column.zone_map.chunks.size(),
                        column.zone_map.chunk_rows,
                        static_cast<long long>(lo), static_cast<long long>(hi),
                        empty > 0 ? " (some all-null)" : "");
    }

    if (column.has_statistics) {
        const ColumnStatistics& stats = column.statistics;
        std::printf("%*s  stats:", indent, "");
        if (stats.flags & kStatNullCount)
            std::printf(" nulls=%" PRIu64, stats.null_count);
        if (stats.flags & kStatMin)
            std::printf(" min_ordinal=%" PRId64, stats.min_ordinal);
        if (stats.flags & kStatMax)
            std::printf(" max_ordinal=%" PRId64, stats.max_ordinal);
        if (stats.flags & kStatSum) {
            const __int128 total = (static_cast<__int128>(stats.sum_high) << 64)
                                 | static_cast<uint64_t>(stats.sum_low);
            // Printed as two halves: there is no portable printf for __int128,
            // and the exact 128-bit value is what a consumer needs, not a
            // truncation of it.
            std::printf(" sum=(hi=%" PRId64 ",lo=%" PRIu64 ")",
                        static_cast<int64_t>(total >> 64),
                        static_cast<uint64_t>(total));
        }
        if (stats.flags & kStatRowSorted)
            std::printf(" row_sorted=%s",
                        (stats.flags & kStatRowSortedDescending) ? "desc" : "asc");
        std::printf("\n");
    }

    for (const ColumnMetadata& child : column.children)
        print_column(child, indent + 4);
}

// ─── Commands ───────────────────────────────────────────────────────────────

static int command_info(const char* path) {
    std::vector<uint8_t> bytes;
    Status st = read_file(path, &bytes);
    if (!st.is_ok()) { std::fprintf(stderr, "error: %s\n", st.message().c_str()); return 1; }

    // Probe FIRST. This succeeds for versions this build cannot read, which is
    // the case where an operator most needs the answer.
    uint16_t version = 0;
    st = probe_version(bytes.data(), bytes.size(), &version);
    if (!st.is_ok()) { std::fprintf(stderr, "error: %s\n", st.message().c_str()); return 1; }

    std::printf("file      %s\n", path);
    std::printf("size      %zu bytes\n", bytes.size());
    std::printf("version   %u\n", version);
    std::printf("this build %s\n", supported_versions_string());

    if (!version_is_supported(version)) {
        char advice[448];
        migration_advice(version, advice, sizeof(advice));
        std::printf("\n%s\n", advice);
        return 2;
    }

    FileMetadata meta;
    st = read_metadata(bytes.data(), bytes.size(), &meta);
    if (!st.is_ok()) { std::fprintf(stderr, "error: %s\n", st.message().c_str()); return 1; }

    std::printf("rows      %" PRIu64 "\n", meta.row_count);
    std::printf("columns   %zu\n", meta.columns.size());
    if (!meta.writer_tag.empty())
        std::printf("writer    %s\n", meta.writer_tag.c_str());
    std::printf("\n");
    for (const ColumnMetadata& column : meta.columns) print_column(column, 0);
    return 0;
}

// A sample file covering every family the format claims to support, so there is
// something real on disk to inspect, diff and hand to another tool.
static int command_demo(const char* path) {
    CxxMorsel morsel;

    auto add = [&](const char* name, DrakenVector view,
                   OwnedBuffer<void> data, OwnedBuffer<uint8_t> validity,
                   const LogicalType* logical) {
        CxxColumn column;
        column.view = view;
        column.own  = std::make_shared<VectorOwner>(view, std::move(data),
                                                    std::move(validity));
        column.own->logical_type = logical;
        column.view = column.own->vec;
        morsel.columns.push_back(std::move(column));
        morsel.names.push_back(name);
    };

    constexpr uint32_t kRows = 4;


    // INT64 with nulls.
    {
        int64_t* values = static_cast<int64_t*>(draken_malloc(kRows * sizeof(int64_t)));
        const int64_t source[kRows] = {30, 10, 20, 10};
        std::memcpy(values, source, sizeof(source));
        uint8_t* validity = static_cast<uint8_t*>(draken_malloc(8));
        std::memset(validity, 0, 8);
        validity[0] = 0b00001101;   // row 1 is null
        add("n", draken_vector_from_dense(values, kRows, DRAKEN_INT64, validity),
            OwnedBuffer<void>(values), OwnedBuffer<uint8_t>(validity), nullptr);
    }

    // IPv4 — a UINT32 refined by an IPV4 descriptor. Parquet cannot say this.
    {
        LogicalType lt;
        lt.kind = LogicalKind::IPV4;
        uint32_t* values = static_cast<uint32_t*>(draken_malloc(kRows * sizeof(uint32_t)));
        const uint32_t source[kRows] = {0xC0A80101u, 0x08080808u, 0x7F000001u, 0x0A000001u};
        std::memcpy(values, source, sizeof(source));
        add("client_ip", draken_vector_from_dense(values, kRows, DRAKEN_UINT32, nullptr),
            OwnedBuffer<void>(values), OwnedBuffer<uint8_t>(nullptr),
            logical_type_intern(lt));
    }

    // VARCHAR mixing inline and arena payloads.
    {
        const char* source[kRows] = {
            "alpha", "a considerably longer value that lives in the arena",
            "gamma", "alpha"};
        size_t arena_bytes = 0;
        for (uint32_t i = 0; i < kRows; ++i) {
            const size_t n = std::strlen(source[i]);
            if (n > STR_INLINE_MAX) arena_bytes += n;
        }
        const size_t struct_end = sizeof(DrakenStringArena);
        const size_t slots_bytes = kRows * sizeof(DrakenStringSlot);
        uint8_t* block = static_cast<uint8_t*>(
            draken_malloc(struct_end + slots_bytes + arena_bytes));
        std::memset(block, 0, struct_end + slots_bytes + arena_bytes);

        DrakenStringArena* arena = reinterpret_cast<DrakenStringArena*>(block);
        DrakenStringSlot* slots =
            reinterpret_cast<DrakenStringSlot*>(block + struct_end);
        uint8_t* payload = arena_bytes > 0 ? block + struct_end + slots_bytes : nullptr;

        size_t used = 0;
        for (uint32_t i = 0; i < kRows; ++i) {
            const uint8_t* text = reinterpret_cast<const uint8_t*>(source[i]);
            const uint32_t n = static_cast<uint32_t>(std::strlen(source[i]));
            if (n <= STR_INLINE_MAX) {
                str_init_inline(&slots[i], text, n);
            } else {
                std::memcpy(payload + used, text, n);
                str_init_extern(&slots[i], text, n, static_cast<uint32_t>(used));
                used += n;
            }
        }
        arena->slots = slots;   arena->arena = payload;
        arena->length = kRows;  arena->arena_used = used;
        arena->arena_cap = arena_bytes;
        arena->null_bitmap = nullptr; arena->owns_buffers = 0;
        arena->payloads_elided = 0;   arena->type = DRAKEN_VARCHAR;

        add("label", draken_vector_from_dense(block, kRows, DRAKEN_VARCHAR, nullptr),
            OwnedBuffer<void>(block), OwnedBuffer<uint8_t>(nullptr), nullptr);
    }

    // A constant column — stores one value and no selection at all.
    {
        int64_t* value = static_cast<int64_t*>(draken_malloc(sizeof(int64_t)));
        *value = 7;
        add("constant", draken_vector_from_constant(value, kRows, DRAKEN_INT64, nullptr),
            OwnedBuffer<void>(value), OwnedBuffer<uint8_t>(nullptr), nullptr);
    }

    WriteOptions options;
    options.read_acceleration = true;
    options.writer_tag  = "skene-cli/demo";

    std::vector<uint8_t> bytes;
    Status st = write_morsel(morsel, options, &bytes);
    if (!st.is_ok()) { std::fprintf(stderr, "error: %s\n", st.message().c_str()); return 1; }

    st = write_file(path, bytes);
    if (!st.is_ok()) { std::fprintf(stderr, "error: %s\n", st.message().c_str()); return 1; }

    std::printf("wrote %s (%zu bytes, %u rows, %zu columns)\n",
                path, bytes.size(), kRows, morsel.names.size());
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr,
            "usage: skene <command> [args]\n"
            "  version           what this build writes and reads\n"
            "  info <file>       inspect a .skene file\n"
            "  demo <file>       write a sample .skene file\n");
        return 1;
    }
    const std::string command = argv[1];

    if (command == "version") { std::printf("%s\n", supported_versions_string()); return 0; }
    if (command == "info" && argc == 3) return command_info(argv[2]);
    if (command == "demo" && argc == 3) return command_demo(argv[2]);

    std::fprintf(stderr, "unknown command or wrong arguments: %s\n", command.c_str());
    return 1;
}
