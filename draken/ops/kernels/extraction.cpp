#include "ops/kernels/extraction_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/result_helpers.h"
#include "ops/json_extract.h"
#include "ops/string_subscript.h"

/**
 * BC_EXTRACTION kernels.
 *
 * The row loops live in draken/ops/{json_extract,string_subscript}.h and are the
 * SAME code the nanobind bindings call — these entry points only translate the C
 * ABI (ctx in, VecResult out) and consolidate the produced component buffers.
 *
 * All bind-time parameters (navigation path, subscript index) arrive in
 * extraction_ctx; the ABI's `key` operand is unused, so BC_EXTRACTION pops
 * exactly one vector. For JSON sub-ops the ctx path is ALREADY an RFC 6901
 * pointer (converted once in kernel_alloc_extraction_ctx).
 *
 * Errors — invalid JSON, unsupported operand type, OOM — surface as
 * draken_error_sentinel (data == nullptr). No silent nulls, no fallback.
 */

namespace {

// Sub-op codes; mirror BCExtractionOpCode in compiled_expression.pxd.
// Kept in sync with kernel_alloc_extraction_ctx's JSON test in kernel_registry.cpp.
constexpr int32_t kExtrJsonPtr = 3;
constexpr int32_t kExtrJsonKey = 4;

inline bool is_string_family(DrakenType t) {
    return t == DRAKEN_VARCHAR || t == DRAKEN_NVARCHAR || t == DRAKEN_VARBINARY;
}

// Consolidate a produced StringRows into the single owned block a string
// DrakenVector requires. CONSUMES the buffers (freed inside, success or failure).
inline VecResult finalize(draken::ops::StringRows& r) {
    return vecresult_from_string_buffers(r.slots, r.arena, r.arena_len,
                                         r.validity, r.length, r.type);
}

}  // namespace

extern "C" {

// `str[i]` — character subscript. Index in ctx->index.
VecResult draken_map_access_string(void* ctx, const DrakenVector* vector, const DrakenVector*) {
    DRAKEN_KERNEL_TRY({
        auto* c = static_cast<const extraction_ctx*>(ctx);
        if (!c || !vector)
            return draken_error_sentinel("draken_map_access_string: null ctx or operand");
        if (!is_string_family(vector->type))
            return draken_error_sentinel(
                "draken_map_access_string: operand must be VARCHAR/NVARCHAR/VARBINARY");
        auto rows = draken::ops::char_subscript_rows(vector, c->index);
        return finalize(rows);
    });
}

// `arr[i]` — element subscript on DRAKEN_ARRAY.
//
// STILL A STUB, and deliberately unreachable: the ARRAY child vector hangs off the
// VectorOwner (vector_owner.h), not off DrakenVector, so this signature cannot
// reach it. Going native requires the BC_C_NATIVE_CHILD plumbing the ARRAY→VARCHAR
// cast uses (a 3-arg kernel whose child operand the VM resolves from the morsel's
// column owner). Until then the binder does NOT set BC_INSTR_C_NATIVE for
// BC_EXTR_MAP_ARRAY, so the gate routes it to the GIL VM and this is never called.
VecResult draken_array_map_access(void* ctx, const DrakenVector* vector, const DrakenVector* key) {
    (void)ctx; (void)vector; (void)key;
    DRAKEN_KERNEL_TRY({
        return draken_error_sentinel(
            "draken_array_map_access: ARRAY subscript is not a C-native kernel "
            "(needs the owner-held child vector); this kernel must not be dispatched");
    });
}

// `->` (VARIANT) and `->>` (NVARCHAR). Path in ctx, already an RFC 6901 pointer.
VecResult draken_json_extract(void* ctx, const DrakenVector* json_vec, const DrakenVector*) {
    DRAKEN_KERNEL_TRY({
        auto* c = static_cast<const extraction_ctx*>(ctx);
        if (!c || !json_vec)
            return draken_error_sentinel("draken_json_extract: null ctx or operand");
        // VARIANT is accepted so chains compose: (x -> 'a') -> 'b'.
        if (!is_string_family(json_vec->type) && json_vec->type != DRAKEN_VARIANT)
            return draken_error_sentinel(
                "draken_json_extract: operand must be a string-family or VARIANT vector");
        if (c->sub_op_code != kExtrJsonPtr && c->sub_op_code != kExtrJsonKey)
            return draken_error_sentinel("draken_json_extract: unexpected sub-op code");

        const bool text_mode = (c->sub_op_code == kExtrJsonKey);
        auto rows = draken::ops::extract_rows(
            json_vec, extraction_ctx_nav(c), static_cast<size_t>(c->nav_len),
            /*mode=*/0, text_mode,
            text_mode ? "vector_json_extract_text" : "vector_json_extract");
        return finalize(rows);
    });
}

// Top-level object key via yyjson_obj_get. Registered but currently unreferenced by
// the binder (no sub-op maps to it); kept as a real kernel rather than a stub so a
// future MapAccess-on-JSON lowering has a correct target.
VecResult draken_pointer_extract(void* ctx, const DrakenVector* ptr_vec, const DrakenVector*) {
    DRAKEN_KERNEL_TRY({
        auto* c = static_cast<const extraction_ctx*>(ctx);
        if (!c || !ptr_vec)
            return draken_error_sentinel("draken_pointer_extract: null ctx or operand");
        if (!is_string_family(ptr_vec->type) && ptr_vec->type != DRAKEN_VARIANT)
            return draken_error_sentinel(
                "draken_pointer_extract: operand must be a string-family or VARIANT vector");
        auto rows = draken::ops::extract_rows(
            ptr_vec, extraction_ctx_nav(c), static_cast<size_t>(c->nav_len),
            /*mode=*/1, /*text_mode=*/false, "vector_map_access");
        return finalize(rows);
    });
}

}  // extern "C"
