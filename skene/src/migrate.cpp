#include "skene/migrate.h"

#include <cstdarg>
#include <cstdio>
#include <cstring>

#include "skene/format.h"
#include "skene/probe.h"
#include "skene/reader.h"

namespace skene {
namespace {

Status fail(Code code, const char* fmt, ...) __attribute__((format(printf, 2, 3)));
Status fail(Code code, const char* fmt, ...) {
    char buffer[640];
    va_list args;
    va_start(args, fmt);
    std::vsnprintf(buffer, sizeof(buffer), fmt, args);
    va_end(args);
    return Status(code, buffer);
}

bool uuid_is_set(const uint8_t uuid[16]) {
    for (int i = 0; i < 16; ++i)
        if (uuid[i] != 0) return true;
    return false;
}

}  // namespace

Status migrate_file(const void* file, size_t file_bytes,
                    const WriteOptions& posture, std::vector<uint8_t>* out) {
    if (out == nullptr)
        return fail(Code::kMalformed, "migrate_file: out is null");

    // Provenance is carried from the source; a posture that also sets it is a
    // caller holding a second copy of a fact this function owns.
    if (uuid_is_set(posture.file_uuid) || posture.created_at_unix_us != 0
            || !posture.writer_tag.empty())
        return fail(Code::kMalformed,
                    "migrate_file: file_uuid / created_at_unix_us / writer_tag "
                    "are carried from the source file; leave them unset on the "
                    "posture");
    if (!posture.field_ids.empty())
        return fail(Code::kMalformed,
                    "migrate_file: field_ids are carried from the source file's "
                    "schema; leave them unset on the posture");

    uint16_t version = 0;
    SKENE_RETURN_IF_ERROR(probe_version(file, file_bytes < kProbeBytes
                                                  ? file_bytes : kProbeBytes,
                                        &version));
    if (version == kVersion)
        return fail(Code::kMalformed,
                    "file is already version %u; there is nothing to migrate",
                    static_cast<unsigned>(kVersion));
    if (!version_is_migratable(version)) {
        char advice[448];
        migration_advice(version, advice, sizeof(advice));
        return Status(Code::kUnsupportedVersion, advice);
    }

    FileMetadata metadata;
    SKENE_RETURN_IF_ERROR(read_metadata(file, file_bytes, &metadata));

    WriteOptions options = posture;
    options.created_at_unix_us = metadata.created_at_unix_us;
    options.writer_tag         = metadata.writer_tag;
    std::memcpy(options.file_uuid, metadata.file_uuid, sizeof(options.file_uuid));
    options.field_ids.reserve(metadata.columns.size());
    for (const ColumnSchema& column : metadata.columns)
        options.field_ids.push_back(column.field_id);

    FileWriter writer;
    SKENE_RETURN_IF_ERROR(writer.begin(options, out));
    for (uint32_t rg = 0; rg < metadata.row_groups.size(); ++rg) {
        CxxMorsel morsel;
        SKENE_RETURN_IF_ERROR(
            read_morsel(file, file_bytes, rg, ReadOptions(), &morsel));
        SKENE_RETURN_IF_ERROR(writer.add_row_group(morsel));
    }
    return writer.finish();
}

}  // namespace skene
