// Parquet -> .skene converter.
//
// The chain, all native:
//
//   rugo pipeline -> MorselRef
//        |- direct columns: buffers already draken-allocated, moved not copied
//        '- DK_POOL columns: IPC blob -> opteryx's deserialize_fixed_column
//   -> CxxMorsel -> skene writer -> one .skene per row group
//
// LOGICAL TYPES COME FROM THE PARQUET FOOTER, not from a query plan. That matters:
// TPC-H's money columns are int64-backed DECIMAL(15,2), and rugo hands them back
// as plain int64. Writing them as INT64 would lose the precision and scale — the
// exact class of loss this format exists to prevent — so the converter reads
// `decimal(p,s)` / `date32[day]` / `timestamp[unit]` from the schema and attaches
// the descriptor. A converter that skipped this would produce files that prove
// nothing about the format's central claim.
//
// NOT part of libskene.a. See bench/README.md.

#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "skene/file_io.h"
#include "skene/reader.h"
#include "skene/writer.h"

#include "core/alloc.h"
#include "core/buffers.h"
#include "core/string_slot.h"
#include "core/vector_alloc.h"
#include "core/vector_owner.h"
#include "logical_type.h"

#include "zstd.h"

#include "parquet/io_pipeline.hpp"
#include "parquet/metadata.hpp"

#include "ipc_deserialize.hpp"   // opteryx: the DK_POOL blob decoder

using namespace skene;
using Clock = std::chrono::steady_clock;

namespace {

// ─── Parquet logical type -> draken ─────────────────────────────────────────

struct LogicalSpec {
    DrakenType         physical_override = static_cast<DrakenType>(0);  // 0 = keep
    const LogicalType* logical = nullptr;
};

// Parses rugo's logical-type spellings (metadata.cpp): "decimal(p,s)",
// "date32[day]", "timestamp[us]", "time[...]".
LogicalSpec logical_spec_for(const std::string& text) {
    LogicalSpec spec;
    if (text.rfind("decimal(", 0) == 0) {
        int precision = 0, scale = 0;
        if (std::sscanf(text.c_str(), "decimal(%d,%d)", &precision, &scale) == 2) {
            LogicalType lt;
            lt.kind      = LogicalKind::DECIMAL;
            lt.precision = static_cast<uint8_t>(precision);
            lt.scale     = static_cast<uint8_t>(scale);
            // p <= 18 is int64-backed; beyond that it is int128.
            spec.physical_override = precision <= 18 ? DRAKEN_DECIMAL : DRAKEN_DECIMAL128;
            spec.logical = logical_type_intern(lt);
        }
        return spec;
    }
    if (text.rfind("date32", 0) == 0) {
        spec.physical_override = DRAKEN_DATE32;
        return spec;   // DATE32 carries no parameters
    }
    if (text.rfind("timestamp[", 0) == 0) {
        LogicalType lt;
        lt.kind = LogicalKind::TIMESTAMP;
        lt.unit = text.find("[s") != std::string::npos   ? TimestampUnit::SECONDS
                : text.find("[ms") != std::string::npos  ? TimestampUnit::MILLISECONDS
                : text.find("[ns") != std::string::npos  ? TimestampUnit::NANOSECONDS
                                                        : TimestampUnit::MICROSECONDS;
        spec.physical_override = DRAKEN_TIMESTAMP64;
        spec.logical = logical_type_intern(lt);
    }
    return spec;
}

DrakenType type_for_direct_kind(int kind) {
    switch (kind) {
        case rugo::DK_INT64:      case rugo::DK_INT64_DICT:     return DRAKEN_INT64;
        case rugo::DK_INT32:      case rugo::DK_INT32_DICT:     return DRAKEN_INT32;
        case rugo::DK_INT16:      case rugo::DK_INT16_DICT:     return DRAKEN_INT16;
        case rugo::DK_INT8:       case rugo::DK_INT8_DICT:      return DRAKEN_INT8;
        case rugo::DK_UINT64:     case rugo::DK_UINT64_DICT:    return DRAKEN_UINT64;
        case rugo::DK_UINT32:     case rugo::DK_UINT32_DICT:    return DRAKEN_UINT32;
        case rugo::DK_UINT16:     case rugo::DK_UINT16_DICT:    return DRAKEN_UINT16;
        case rugo::DK_UINT8:      case rugo::DK_UINT8_DICT:     return DRAKEN_UINT8;
        case rugo::DK_FLOAT64:    case rugo::DK_FLOAT64_DICT:   return DRAKEN_FLOAT64;
        case rugo::DK_FLOAT32:    case rugo::DK_FLOAT32_DICT:   return DRAKEN_FLOAT32;
        case rugo::DK_BOOL:                                     return DRAKEN_BOOL;
        case rugo::DK_DECIMAL128:                               return DRAKEN_DECIMAL128;
        case rugo::DK_VARCHAR:    case rugo::DK_VARCHAR_DICT:   return DRAKEN_VARCHAR;
        default:                                                return static_cast<DrakenType>(0);
    }
}

bool kind_is_dict(int kind) {
    switch (kind) {
        case rugo::DK_INT64_DICT:  case rugo::DK_INT32_DICT:
        case rugo::DK_INT16_DICT:  case rugo::DK_INT8_DICT:
        case rugo::DK_UINT64_DICT: case rugo::DK_UINT32_DICT:
        case rugo::DK_UINT16_DICT: case rugo::DK_UINT8_DICT:
        case rugo::DK_FLOAT64_DICT: case rugo::DK_FLOAT32_DICT:
        case rugo::DK_VARCHAR_DICT:
            return true;
        default:
            return false;
    }
}

// ─── Buffer helpers ─────────────────────────────────────────────────────────

// Validity bitmaps get padded to 8 bytes, matching what draken's own allocators
// do so SIMD bitmap ops can read whole words.
OwnedBuffer<uint8_t> adopt_validity(uint8_t* rugo_validity) {
    return OwnedBuffer<uint8_t>(rugo_validity);
}

// The string family: rugo hands back the slot array and the arena as SEPARATE
// allocations, but draken expects one DrakenStringArena whose slots and arena
// pointers live inside a single owned block. So this copies both into the same
// block layout the reader builds — one extra copy, in a converter, to keep the
// ownership model exactly as draken defines it rather than inventing a variant.
OwnedBuffer<void> build_string_block(const DrakenStringSlot* slots, uint64_t slot_count,
                                     const uint8_t* arena, uint64_t arena_bytes,
                                     DrakenType type, uint8_t* validity) {
    const size_t struct_end  = sizeof(DrakenStringArena);
    const size_t slots_bytes = static_cast<size_t>(slot_count > 0 ? slot_count : 1)
                             * sizeof(DrakenStringSlot);
    uint8_t* block = static_cast<uint8_t*>(
        draken_malloc(struct_end + slots_bytes + static_cast<size_t>(arena_bytes)));
    std::memset(block, 0, struct_end + slots_bytes + static_cast<size_t>(arena_bytes));

    DrakenStringSlot* destination =
        reinterpret_cast<DrakenStringSlot*>(block + struct_end);
    if (slot_count > 0)
        std::memcpy(destination, slots,
                    static_cast<size_t>(slot_count) * sizeof(DrakenStringSlot));

    uint8_t* payload = nullptr;
    if (arena_bytes > 0) {
        payload = block + struct_end + slots_bytes;
        std::memcpy(payload, arena, static_cast<size_t>(arena_bytes));
    }

    DrakenStringArena* header = reinterpret_cast<DrakenStringArena*>(block);
    header->slots           = destination;
    header->arena           = payload;
    header->length          = slot_count;
    header->arena_used      = arena_bytes;
    header->arena_cap       = arena_bytes;
    header->null_bitmap     = validity;
    header->owns_buffers    = 0;
    header->payloads_elided = 0;
    header->type            = type;
    return OwnedBuffer<void>(block);
}

// ─── Pool (DK_POOL blobs) ───────────────────────────────────────────────────

struct Pool {
    std::mutex mutex;
    std::vector<std::vector<uint8_t>> blobs;
};

// A DK_POOL column arrives as an IPC blob. opteryx's C++ deserializer covers the
// fixed-width tags, which is every pooled column TPC-H produces (all tag 1).
// Anything it cannot handle fails LOUD — silently dropping a column would make a
// converted file quietly incomplete.
bool decode_pool_column(Pool& pool, const rugo::ColumnOut& column, const char* name,
                        uint32_t rows, DrakenType* out_type,
                        OwnedBuffer<void>* out_data, OwnedBuffer<uint8_t>* out_validity) {
    std::vector<uint8_t>* blob = nullptr;
    {
        std::lock_guard<std::mutex> lock(pool.mutex);
        if (column.ref_id < 0 || static_cast<size_t>(column.ref_id) >= pool.blobs.size()) {
            std::fprintf(stderr, "column '%s': pool ref %lld is out of range\n",
                         name, static_cast<long long>(column.ref_id));
            return false;
        }
        blob = &pool.blobs[column.ref_id];
    }

    opteryx::DecodedFixedColumn decoded{};
    opteryx::deserialize_fixed_column(blob->data(),
                                      static_cast<int64_t>(blob->size()), decoded);
    if (decoded.status != opteryx::kStatusOk) {
        std::fprintf(stderr,
                     "column '%s': IPC tag %u could not be decoded by the C++ path "
                     "(status %d). This converter has no Cython fallback.\n",
                     name, decoded.tag, decoded.status);
        return false;
    }

    size_t element = 0;
    switch (decoded.kind) {
        case opteryx::IpcKind::Int64:   *out_type = DRAKEN_INT64;   element = 8; break;
        case opteryx::IpcKind::Float64: *out_type = DRAKEN_FLOAT64; element = 8; break;
        case opteryx::IpcKind::Float32: *out_type = DRAKEN_FLOAT32; element = 4; break;
        case opteryx::IpcKind::Int128:  *out_type = DRAKEN_DECIMAL128; element = 16; break;
        case opteryx::IpcKind::Bool:    *out_type = DRAKEN_BOOL;    element = 0; break;
    }

    // The IPC value stream is COMPACT — Parquet omits null rows — so a nullable
    // column arrives with fewer values than rows and must be scattered back into
    // positional slots before it is a draken vector. Copying anyway, because
    // these buffers are std::malloc'd and draken frees with draken_free.
    if (decoded.kind == opteryx::IpcKind::Bool) {
        const size_t bytes = (static_cast<size_t>(rows) + 7u) / 8u;
        uint8_t* data = static_cast<uint8_t*>(draken_malloc(bytes > 0 ? bytes : 1));
        std::memset(data, 0, bytes > 0 ? bytes : 1);
        if (decoded.data != nullptr) std::memcpy(data, decoded.data, bytes);
        out_data->reset(data);
    } else {
        const size_t positional = static_cast<size_t>(rows) * element;
        uint8_t* data = static_cast<uint8_t*>(draken_malloc(positional > 0 ? positional : 1));
        std::memset(data, 0, positional > 0 ? positional : 1);

        const size_t present = decoded.data_len / (element > 0 ? element : 1);
        if (present == rows || decoded.null_bitmap == nullptr) {
            std::memcpy(data, decoded.data, decoded.data_len);
        } else {
            // Scatter compact -> positional against the validity bitmap.
            const uint8_t* source = static_cast<const uint8_t*>(decoded.data);
            size_t taken = 0;
            for (uint32_t row = 0; row < rows; ++row) {
                const bool valid =
                    (decoded.null_bitmap[row >> 3] & (1u << (row & 7u))) != 0;
                if (!valid) continue;
                std::memcpy(data + static_cast<size_t>(row) * element,
                            source + taken * element, element);
                ++taken;
            }
        }
        out_data->reset(data);
    }

    if (decoded.null_bitmap != nullptr) {
        const size_t bytes = (static_cast<size_t>(rows) + 7u) / 8u;
        const size_t padded = (bytes + 7u) & ~size_t{7};
        uint8_t* validity = static_cast<uint8_t*>(draken_malloc(padded > 0 ? padded : 8));
        std::memset(validity, 0, padded > 0 ? padded : 8);
        std::memcpy(validity, decoded.null_bitmap, bytes);
        out_validity->reset(validity);
    }

    std::free(decoded.data);
    std::free(decoded.null_bitmap);
    return true;
}

// ─── MorselRef -> CxxMorsel ─────────────────────────────────────────────────

bool build_morsel(Pool& pool, rugo::MorselRef& result,
                  const std::map<std::string, std::string>& logical_by_name,
                  CxxMorsel* out) {
    for (size_t i = 0; i < result.columns.size(); ++i) {
        rugo::ColumnOut& column = result.columns[i];
        const std::string name =
            i < result.column_names.size() ? result.column_names[i] : "?";

        const uint32_t rows = column.length > 0
            ? column.length
            : (out->columns.empty() ? 0u : out->columns.front().view.length);

        DrakenType type = static_cast<DrakenType>(0);
        OwnedBuffer<void>    data_buf(nullptr);
        OwnedBuffer<uint8_t> validity_buf(nullptr);
        OwnedBuffer<void>    codes_buf(nullptr);
        uint32_t data_length = rows;
        const uint32_t* codes = nullptr;

        if (column.direct_kind == rugo::DK_POOL) {
            if (!decode_pool_column(pool, column, name.c_str(), rows, &type,
                                    &data_buf, &validity_buf))
                return false;
            data_length = rows;
        } else {
            type = type_for_direct_kind(column.direct_kind);
            if (type == static_cast<DrakenType>(0)) {
                std::fprintf(stderr, "column '%s': direct kind %d is not mapped\n",
                             name.c_str(), column.direct_kind);
                return false;
            }

            uint8_t* validity = nullptr;
            void* raw = rugo::morsel_take_direct(result, i, &validity);
            validity_buf.reset(validity);

            void* arena = nullptr;
            void* raw_codes = nullptr;
            rugo::morsel_take_string(result, i, &arena, &raw_codes);

            if (draken_type_is_string_storage(type)) {
                const uint64_t slot_count = kind_is_dict(column.direct_kind)
                    ? column.data_length : rows;
                data_buf = build_string_block(
                    static_cast<const DrakenStringSlot*>(raw), slot_count,
                    static_cast<const uint8_t*>(arena), column.arena_len,
                    type, validity_buf.get());
                // rugo allocated the slots and arena separately; the block above
                // is a copy, so both originals are released here.
                draken_free(raw);
                draken_free(arena);
                data_length = static_cast<uint32_t>(slot_count);
            } else {
                data_buf.reset(raw);
                if (arena != nullptr) draken_free(arena);
                data_length = kind_is_dict(column.direct_kind)
                    ? column.data_length : rows;
            }

            if (kind_is_dict(column.direct_kind)) {
                codes_buf.reset(raw_codes);
                codes = static_cast<const uint32_t*>(raw_codes);
            } else if (raw_codes != nullptr) {
                draken_free(raw_codes);
            }
        }

        // Logical type from the FOOTER. Without this an int64-backed DECIMAL is
        // written as a bare integer and the file loses exactly what the format
        // exists to carry.
        const LogicalType* logical = nullptr;
        const auto found = logical_by_name.find(name);
        if (found != logical_by_name.end()) {
            const LogicalSpec spec = logical_spec_for(found->second);
            if (spec.physical_override != static_cast<DrakenType>(0)
                    && !draken_type_is_string_storage(type))
                type = spec.physical_override;
            logical = spec.logical;
        }

        DrakenVector vector = codes != nullptr
            ? draken_vector_from_dict(data_buf.get(), data_length, codes, rows,
                                      type, validity_buf.get())
            : draken_vector_from_dense(data_buf.get(), rows, type, validity_buf.get());

        CxxColumn built;
        built.own = std::make_shared<VectorOwner>(vector, std::move(data_buf),
                                                  std::move(validity_buf),
                                                  std::move(codes_buf));
        built.own->logical_type = logical;
        built.view = built.own->vec;

        out->columns.push_back(std::move(built));
        out->names.push_back(name);
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        std::fprintf(stderr,
            "usage: convert_parquet <in.parquet> <out-prefix> [max_row_groups]\n"
            "  writes <out-prefix>.rgNNN.skene, one per row group\n");
        return 1;
    }
    const std::string input = argv[1];

    // Source size, for the only size comparison that is not self-referential:
    // skene against the Parquet file it was built from.
    uint64_t source_bytes = 0;
    {
        std::vector<uint8_t> probe;
        Status st = read_file(input.c_str(), &probe);
        if (!st.is_ok()) { std::fprintf(stderr, "%s\n", st.message().c_str()); return 1; }
        source_bytes = probe.size();
    }
    const std::string prefix = argv[2];
    const int limit = (argc > 3) ? std::atoi(argv[3]) : -1;

    const FileStats stats = ReadParquetMetadataC(input.c_str());
    if (stats.row_groups.empty()) {
        std::fprintf(stderr, "no row groups in %s\n", input.c_str());
        return 1;
    }

    std::map<std::string, std::string> logical_by_name;
    for (const SchemaField& field : stats.schema_columns)
        logical_by_name[field.name] = field.logical_type;

    std::vector<std::string> wanted;
    for (const ColumnStats& c : stats.row_groups[0].columns) wanted.push_back(c.name);

    const int row_groups = (limit > 0 && limit < static_cast<int>(stats.row_groups.size()))
        ? limit : static_cast<int>(stats.row_groups.size());

    Pool pool;
    rugo::ParquetIOPipeline pipeline(4);
    rugo::PoolSink sink;
    sink.ctx = &pool;
    sink.draken_alloc = [](size_t n) -> void* { return draken_malloc(n); };
    sink.draken_free  = [](void* p) { draken_free(p); };
    sink.reserve = [](void* ctx, int64_t size, void** out_ptr) -> int64_t {
        Pool* p = static_cast<Pool*>(ctx);
        std::lock_guard<std::mutex> lock(p->mutex);
        p->blobs.emplace_back(static_cast<size_t>(size));
        *out_ptr = p->blobs.back().data();
        return static_cast<int64_t>(p->blobs.size()) - 1;
    };
    sink.finalize = [](void*, int64_t, int64_t) {};
    pipeline.set_pool_sink(sink);

    WriteOptions options;
    options.read_acceleration = true;
    options.writer_tag = "skene-convert";
    // Per-section compression, set from the environment so the same converter
    // measures every posture against identical input. SKENE_ZSTD carries a zstd
    // level; SKENE_LZ4 selects lz4 instead. Setting both is a contradiction and
    // is refused rather than resolved — a bake-off that silently measured a
    // codec other than the one asked for would be worse than no measurement.
    const char* zstd_level = std::getenv("SKENE_ZSTD");
    const bool  want_lz4   = std::getenv("SKENE_LZ4") != nullptr;
    if (zstd_level != nullptr && want_lz4) {
        std::fprintf(stderr, "SKENE_ZSTD and SKENE_LZ4 are both set — pick one\n");
        return 1;
    }
    if (zstd_level != nullptr) {
        options.codec = SectionCodec::kZstd;
        options.zstd_level = std::atoi(zstd_level);
    } else if (want_lz4) {
        options.codec = SectionCodec::kLz4;
    }

    uint64_t total_rows = 0, total_skene = 0, total_skene_zstd = 0;
    double write_ms = 0, read_back_ms = 0, parquet_read_ms = 0;
    const auto started = Clock::now();

    for (int rg = 0; rg < row_groups; ++rg) {
        // rugo's read, timed for the comparison. It is a THREADED pipeline while
        // skene's read is single-threaded, so this favours Parquet — which is the
        // right direction for a number we are quoting in our own favour.
        const auto parquet_started = Clock::now();
        pipeline.submit_row_group(input, rg, wanted, stats.row_groups[rg].columns);

        rugo::MorselRef result;
        if (!pipeline.wait_and_get_result(result) || !result.success) {
            std::fprintf(stderr, "row group %d: %s\n", rg,
                         result.error.empty() ? "no result" : result.error.c_str());
            return 1;
        }

        parquet_read_ms += std::chrono::duration<double, std::milli>(
            Clock::now() - parquet_started).count();

        CxxMorsel morsel;
        if (!build_morsel(pool, result, logical_by_name, &morsel)) return 1;

        std::vector<uint8_t> bytes;
        const auto write_started = Clock::now();
        Status st = write_morsel(morsel, options, &bytes);
        write_ms += std::chrono::duration<double, std::milli>(
            Clock::now() - write_started).count();
        if (!st.is_ok()) {
            std::fprintf(stderr, "row group %d: write failed: %s\n", rg,
                         st.message().c_str());
            return 1;
        }

        char path[1024];
        std::snprintf(path, sizeof(path), "%s.rg%03d.skene", prefix.c_str(), rg);
        st = write_file(path, bytes);
        if (!st.is_ok()) {
            std::fprintf(stderr, "row group %d: %s\n", rg, st.message().c_str());
            return 1;
        }

        // Read every file back. A converter that writes files nothing can read
        // has converted nothing, and finding that out later costs more than
        // checking now.
        const auto read_started = Clock::now();
        CxxMorsel verify;
        st = read_morsel(bytes.data(), bytes.size(), 0, &verify);
        read_back_ms += std::chrono::duration<double, std::milli>(
            Clock::now() - read_started).count();
        if (!st.is_ok()) {
            std::fprintf(stderr, "row group %d: read-back failed: %s\n", rg,
                         st.message().c_str());
            return 1;
        }
        if (verify.num_rows() != morsel.num_rows()
                || verify.num_columns() != morsel.num_columns()) {
            std::fprintf(stderr, "row group %d: read-back shape differs\n", rg);
            return 1;
        }

        total_rows += morsel.num_rows();
        total_skene += bytes.size();

        // skene stores raw bytes, and the TPC-H sources are ZSTD Parquet — so a
        // raw-vs-compressed comparison answers a different question from
        // compressed-vs-compressed. Both are reported rather than picking the
        // flattering one.
        const size_t bound = ZSTD_compressBound(bytes.size());
        std::vector<uint8_t> packed(bound);
        const size_t packed_bytes =
            ZSTD_compress(packed.data(), bound, bytes.data(), bytes.size(), 2);
        if (!ZSTD_isError(packed_bytes)) total_skene_zstd += packed_bytes;

        if ((rg % 16) == 0 || rg + 1 == row_groups)
            std::fprintf(stderr, "\r  row group %d/%d", rg + 1, row_groups);
    }
    std::fprintf(stderr, "\r%*s\r", 40, "");

    const double total_ms =
        std::chrono::duration<double, std::milli>(Clock::now() - started).count();

    std::printf("%s\n", input.c_str());
    std::printf("  row groups   %d\n", row_groups);
    std::printf("  rows         %" PRIu64 "\n", total_rows);
    std::printf("  skene bytes  %" PRIu64 "\n", total_skene);
    std::printf("  +zstd-2      %" PRIu64 "\n", total_skene_zstd);
    std::printf("  write        %.0f ms   read-back %.0f ms   total %.0f ms\n",
                write_ms, read_back_ms, total_ms);
    std::printf("  parquet read %.0f ms (rugo, threaded)\n", parquet_read_ms);

    // One machine-readable line, so a driver script does not have to scrape the
    // human output above and quietly mis-parse it.
    std::printf("TSV\t%s\t%" PRIu64 "\t%" PRIu64 "\t%" PRIu64 "\t%.1f\t%.1f\t%.1f\n",
                argv[1], total_rows, source_bytes, total_skene,
                write_ms, read_back_ms, parquet_read_ms);
    return 0;
}
