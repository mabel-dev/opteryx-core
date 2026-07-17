#include "ops/kernels/extraction_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/result_helpers.h"
#include "ops/json_extract.h"
#include "ops/string_subscript.h"
#include "ops/array_subscript.h"

/**
 * BC_EXTRACTION kernels.
 *
 * The row loops live in draken/ops/{json_extract,string_subscript,array_subscript}.h
 * — these entry points only translate the C ABI (ctx in, VecResult out) and
 * consolidate the produced component buffers.
 *
 * All bind-time parameters (navigation path, subscript index) arrive in
 * extraction_ctx, so no kernel here pops a key operand. For JSON sub-ops the ctx
 * path is ALREADY an RFC 6901 pointer (converted once in
 * kernel_alloc_extraction_ctx).
 *
 * That leaves the ABI's second operand slot free, and BC_EXTR_MAP_ARRAY uses it to
 * receive the ARRAY's CHILD vector — which hangs off the VectorOwner and is not
 * reachable from the parent DrakenVector (BC_C_NATIVE_CHILD; see
 * draken_array_map_access). Every other sub-op is handed NULL there and ignores it.
 *
 * Errors — invalid JSON, unsupported operand type, OOM — surface as
 * draken_error_sentinel (data == nullptr). No silent nulls, no fallback.
 */

namespace {

// Sub-op codes; mirror BCExtractionOpCode in compiled_expression.pxd.
// Kept in sync with kernel_alloc_extraction_ctx's JSON test in kernel_registry.cpp.
constexpr int32_t kExtrMapArray = 2;
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

// `arr[i]` — element subscript on DRAKEN_ARRAY. Index in ctx->index.
//
// The ARRAY child vector hangs off the VectorOwner (vector_owner.h), not off
// DrakenVector, so it cannot be reached from the parent operand alone. It arrives
// in the ABI's second slot instead: BC_EXTR_MAP_ARRAY binds no key operand (the
// index is a bind-time constant in ctx), so the VM reuses that slot to hand over
// the child it resolved from the morsel's column owner — the BC_C_NATIVE_CHILD
// mechanism the ARRAY→VARCHAR cast and SORT already use. Hence `child`, not `key`.
//
// The result's type is the CHILD's type, so unlike every other kernel in this file
// this one is not string-only; array_subscript.h dispatches the element type.
VecResult draken_array_map_access(void* ctx, const DrakenVector* vector,
                                  const DrakenVector* child) {
    DRAKEN_KERNEL_TRY({
        auto* c = static_cast<const extraction_ctx*>(ctx);
        if (!c || !vector)
            return draken_error_sentinel("draken_array_map_access: null ctx or operand");
        if (vector->type != DRAKEN_ARRAY)
            return draken_error_sentinel("draken_array_map_access: operand must be ARRAY");
        if (c->sub_op_code != kExtrMapArray)
            return draken_error_sentinel("draken_array_map_access: unexpected sub-op code");
        if (!child)
            return draken_error_sentinel(
                "draken_array_map_access: no child vector — the array operand must be a "
                "column the VM can resolve a child from (the compiler projects a computed "
                "array into one first)");

        const int64_t idx = c->index;
        switch (child->type) {
            case DRAKEN_BOOL:
                return draken::ops::array_subscript_bool(vector, child, idx);
            case DRAKEN_INT8:
                return draken::ops::array_subscript_fixed<int8_t>(vector, child, idx, DRAKEN_INT8);
            case DRAKEN_INT16:
                return draken::ops::array_subscript_fixed<int16_t>(vector, child, idx, DRAKEN_INT16);
            case DRAKEN_INT32:
                return draken::ops::array_subscript_fixed<int32_t>(vector, child, idx, DRAKEN_INT32);
            case DRAKEN_DATE32:
                return draken::ops::array_subscript_fixed<int32_t>(vector, child, idx, DRAKEN_DATE32);
            case DRAKEN_UINT8:
                return draken::ops::array_subscript_fixed<uint8_t>(vector, child, idx, DRAKEN_UINT8);
            case DRAKEN_UINT16:
                return draken::ops::array_subscript_fixed<uint16_t>(vector, child, idx, DRAKEN_UINT16);
            case DRAKEN_UINT32:
                return draken::ops::array_subscript_fixed<uint32_t>(vector, child, idx, DRAKEN_UINT32);
            case DRAKEN_UINT64:
                return draken::ops::array_subscript_fixed<uint64_t>(vector, child, idx, DRAKEN_UINT64);
            case DRAKEN_INT64:
                return draken::ops::array_subscript_fixed<int64_t>(vector, child, idx, DRAKEN_INT64);
            case DRAKEN_FLOAT32:
                return draken::ops::array_subscript_fixed<float>(vector, child, idx, DRAKEN_FLOAT32);
            case DRAKEN_FLOAT64:
                return draken::ops::array_subscript_fixed<double>(vector, child, idx, DRAKEN_FLOAT64);
            // Descriptor-carrying types. A subscript COPIES the element, it does not
            // interpret it, so the raw payload is the whole answer: the VM's model is
            // raw-domain for DECIMAL/TIMESTAMP/TIME, and the plan-declared scale/unit
            // is re-attached at the ExprProject boundary (compiler.py _add_computed),
            // never recovered from the vector. Passing the raw value through is
            // therefore correct — the same contract every other raw-domain kernel has.
            case DRAKEN_DECIMAL:
                return draken::ops::array_subscript_fixed<int64_t>(vector, child, idx, DRAKEN_DECIMAL);
            case DRAKEN_TIMESTAMP64:
                return draken::ops::array_subscript_fixed<int64_t>(vector, child, idx,
                                                                   DRAKEN_TIMESTAMP64);
            case DRAKEN_TIME64:
                return draken::ops::array_subscript_fixed<int64_t>(vector, child, idx, DRAKEN_TIME64);
            case DRAKEN_TIME32:
                return draken::ops::array_subscript_fixed<int32_t>(vector, child, idx, DRAKEN_TIME32);
            case DRAKEN_VARCHAR:
            case DRAKEN_NVARCHAR:
            case DRAKEN_VARBINARY:
            case DRAKEN_VARIANT: {
                // VARIANT is German-string storage too (JSON text), so it takes the
                // same row loop — this is what makes JSONB_OBJECT_KEYS(x)[0] work.
                auto rows = draken::ops::array_subscript_rows(vector, child, idx);
                return finalize(rows);
            }
            case DRAKEN_DECIMAL128:
                // int128-backed; mirrors the explicit refusal the other kernels give
                // it rather than silently truncating to 64 bits.
                return draken_error_sentinel(
                    "draken_array_map_access: DECIMAL128 (precision > 18) ARRAY elements "
                    "are not supported by this kernel");
            default:
                // VECTOR_FP16 elements are dimension-width, not fixed-width, so a
                // per-element copy has no single stride to use — fail loud rather
                // than answer with a stripped or misread value.
                return draken_error_sentinel_fmt(
                    "draken_array_map_access: unsupported ARRAY element type %d",
                    (int)child->type);
        }
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
