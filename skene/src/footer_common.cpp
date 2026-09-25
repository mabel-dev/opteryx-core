// Footer records shared by the v2 and v3 readers. See footer_common.h.
// Moved here from reader_v2.cpp at the v3 bump (2026-09-24), logic unchanged.

#include "footer_common.h"

#include <cstdarg>
#include <cstdio>

// draken — imported, never copied.
#include "core/buffers.h"

namespace skene {
namespace footer {
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

}  // namespace

Status parse_schema(Cursor& cursor, ParsedSchema* out, int depth) {
    if (depth > 32)
        return fail(Code::kMalformed,
                    "schema nesting exceeds 32 levels; refusing to recurse further");

    if (!cursor.take(&out->head, sizeof(SchemaEntryHead)))
        return fail(Code::kTruncated, "file footer ends inside a schema entry");

    const uint8_t* name = cursor.raw(out->head.name_bytes);
    if (name == nullptr)
        return fail(Code::kTruncated,
                    "schema entry name claims %u bytes but only %zu remain in "
                    "the file footer", out->head.name_bytes, cursor.remaining());
    out->name.assign(reinterpret_cast<const char*>(name), out->head.name_bytes);

    if (out->head.logical_present) {
        if (!cursor.take(&out->logical, sizeof(LogicalTypeDescriptor)))
            return fail(Code::kTruncated,
                        "schema entry '%s' declares a logical type descriptor "
                        "but the file footer ends before it", out->name.c_str());
    }

    const bool is_array = out->head.type == static_cast<uint32_t>(DRAKEN_ARRAY);
    if (is_array && out->head.child_count != 1u)
        return fail(Code::kMalformed,
                    "schema entry '%s' is ARRAY with child_count %u; exactly one "
                    "child is required", out->name.c_str(), out->head.child_count);
    if (!is_array && out->head.child_count != 0u)
        return fail(Code::kMalformed,
                    "schema entry '%s' has child_count %u but type %u is not "
                    "ARRAY", out->name.c_str(), out->head.child_count,
                    out->head.type);

    out->children.resize(out->head.child_count);
    for (uint32_t i = 0; i < out->head.child_count; ++i)
        SKENE_RETURN_IF_ERROR(parse_schema(cursor, &out->children[i], depth + 1));
    return Status::ok();
}

uint32_t count_schema_nodes(const ParsedSchema& node) {
    uint32_t total = 1;
    for (const ParsedSchema& child : node.children) total += count_schema_nodes(child);
    return total;
}

Status parse_cluster_spec(Cursor& cursor, uint32_t column_count,
                          std::vector<SortKey>* out) {
    // ── Cluster spec (v2) ── a PROMISE consumers act on, so it is validated
    // structurally here: ordinals inside the schema, reserved bytes zero, the
    // null rule consistent. Whether the rows genuinely have this order was the
    // writer's obligation; a reader can only check that the record is coherent.
    {
        ClusterSpecHeader spec{};
        if (!cursor.take(&spec, sizeof(spec)))
            return fail(Code::kTruncated,
                        "file footer ends inside the cluster spec header");
        if (spec.reserved != 0)
            return fail(Code::kMalformed,
                        "cluster spec reserved bytes are %u, not 0", spec.reserved);
        if (static_cast<uint64_t>(spec.key_count) * sizeof(SortKey)
                > cursor.remaining())
            return fail(Code::kMalformed,
                        "cluster spec claims %u keys, which cannot fit in the "
                        "remaining %zu footer bytes", spec.key_count,
                        cursor.remaining());
        (*out).resize(spec.key_count);
        for (uint16_t k = 0; k < spec.key_count; ++k) {
            SortKey& key = (*out)[k];
            if (!cursor.take(&key, sizeof(SortKey)))
                return fail(Code::kTruncated,
                            "file footer ends inside the cluster spec keys");
            if (key.reserved != 0)
                return fail(Code::kMalformed,
                            "cluster key %u: reserved bytes are %u, not 0", k,
                            key.reserved);
            if (key.column_ordinal >= column_count)
                return fail(Code::kMalformed,
                            "cluster key %u names column ordinal %u but the "
                            "schema has %u top-level columns", k,
                            key.column_ordinal, column_count);
            const bool expected_nulls_first = key.descending == 0;
            if ((key.nulls_first != 0) != expected_nulls_first)
                return fail(Code::kMalformed,
                            "cluster key %u: nulls_first=%u with descending=%u "
                            "violates draken's sort rule", k, key.nulls_first,
                            key.descending);
        }
    }
    return Status::ok();
}

void fill_schema(const ParsedSchema& parsed, ColumnSchema* out) {
    out->name            = parsed.name;
    out->field_id        = parsed.head.field_id;
    out->type            = parsed.head.type;
    out->logical_present = parsed.head.logical_present != 0;
    out->logical         = parsed.logical;
    out->children.resize(parsed.children.size());
    for (size_t i = 0; i < parsed.children.size(); ++i)
        fill_schema(parsed.children[i], &out->children[i]);
}

}  // namespace footer
}  // namespace skene
