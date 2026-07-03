#include "ops/kernels/cast_kernels.h"
#include "ops/kernels/error_handling.h"
#include "ops/kernels/kernel_context.h"
#include "ops/kernels/result_helpers.h"
#include "core/alloc.h"
#include "core/vector_alloc.h"
#include "core/string_slot.h"
#include "logical_type.h"
#include <cstring>

/**
 * Cast kernels: temporal conversions (Phase 9c).
 *
 * Compute extracted from opteryx/compiled/nanobind/vector_casts.cpp.
 * int64->timestamp carries its unit via VecResult.ts_unit (see vec_result.h);
 * vecresult_to_owner interns it into the VectorOwner's LogicalType.
 */

extern "C" {

// Gregorian calendar: days-since-epoch (1970-01-01) → (year, month, day).
// Howard Hinnant's algorithm.
static void days_to_ymd(int32_t days, int32_t& y, int32_t& m, int32_t& d) noexcept {
    const int32_t z   = days + 719468;
    const int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    const int32_t doe = z - era * 146097;
    const int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    const int32_t yr  = yoe + era * 400;
    const int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    const int32_t mp  = (5*doy + 2) / 153;
    d = doy - (153*mp + 2)/5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y = yr + (m <= 2 ? 1 : 0);
}

VecResult draken_cast_date32_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_DATE32)
            return draken_error_sentinel_fmt("cast date32->int64: expected DATE32, got %d", v->type);
        const uint32_t k = v->data_length;
        const int32_t* src = static_cast<const int32_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j) out[j] = static_cast<int64_t>(src[j]);
        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

VecResult draken_cast_timestamp_to_int64(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_TIMESTAMP64)
            return draken_error_sentinel_fmt("cast timestamp->int64: expected TIMESTAMP64, got %d", v->type);
        const uint32_t k = v->data_length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((k > 0u ? k : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < k; ++j) out[j] = src[j];
        VecResult r;
        r.data = out; r.type = DRAKEN_INT64; r.validity_embedded = 0u; r.ts_unit = 0xFFu;
        kernel_preserve_shape(r, v);
        return r;
    });
}

// INT64 → TIMESTAMP64. Unit from cast_timestamp_ctx (0/none=us default).
// ctx codes: 1=ns,2=us,3=ms,4=s,5=days. "days" scales epoch-days → epoch-µs.
VecResult draken_cast_int64_to_timestamp(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_INT64)
            return draken_error_sentinel_fmt("cast int64->timestamp: expected INT64, got %d", v->type);

        int unit_code = 2;  // default microseconds
        if (ctx) unit_code = static_cast<const cast_timestamp_ctx*>(ctx)->unit;
        if (unit_code == 0) unit_code = 2;

        TimestampUnit ts_unit;
        bool scale_days = false;
        switch (unit_code) {
            case 1: ts_unit = TimestampUnit::NANOSECONDS;  break;
            case 2: ts_unit = TimestampUnit::MICROSECONDS; break;
            case 3: ts_unit = TimestampUnit::MILLISECONDS; break;
            case 4: ts_unit = TimestampUnit::SECONDS;      break;
            case 5: ts_unit = TimestampUnit::MICROSECONDS; scale_days = true; break;
            default: return draken_error_sentinel_fmt("cast int64->timestamp: bad unit %d", unit_code);
        }

        // DENSE emit (gather through selection, identity-selection output) — a
        // physical-domain (data_length-indexed) shape-preserving emit here
        // caused a silent wrong-answer bug in the sibling DATE32->TIMESTAMP
        // kernel on dict-shaped columns (see draken_cast_date32_to_timestamp).
        const uint32_t n = v->length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < n; ++j) {
            int64_t raw = src[v->selection[j]];
            out[j] = scale_days ? raw * 86'400'000'000LL : raw;
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_TIMESTAMP64; r.validity_embedded = 0u;
        r.ts_unit = static_cast<uint8_t>(ts_unit);
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    });
}

// DATE32 → TIMESTAMP64: midnight of the date, in the target unit. DATE32 is int32
// days-since-epoch; result = days * 86400 * (ticks per second for the unit).
VecResult draken_cast_date32_to_timestamp(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_DATE32)
            return draken_error_sentinel_fmt("cast date32->timestamp: expected DATE32, got %d", v->type);
        int unit_code = 2;   // default microseconds
        if (ctx) unit_code = static_cast<const cast_timestamp_ctx*>(ctx)->unit;
        if (unit_code == 0 || unit_code == 5) unit_code = 2;   // 5=days-scaled→us here
        TimestampUnit ts_unit;
        int64_t per_sec;
        switch (unit_code) {
            case 1: ts_unit = TimestampUnit::NANOSECONDS;  per_sec = 1000000000LL; break;
            case 2: ts_unit = TimestampUnit::MICROSECONDS; per_sec = 1000000LL;    break;
            case 3: ts_unit = TimestampUnit::MILLISECONDS; per_sec = 1000LL;       break;
            case 4: ts_unit = TimestampUnit::SECONDS;      per_sec = 1LL;          break;
            default: return draken_error_sentinel_fmt("cast date32->timestamp: bad unit %d", unit_code);
        }
        const int64_t mult = 86400LL * per_sec;
        // DENSE output: gather through selection (uniform contract) and emit an
        // identity-selection result — unambiguous for every downstream consumer.
        const uint32_t n = v->length;
        const int32_t* src = static_cast<const int32_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < n; ++j)
            out[j] = static_cast<int64_t>(src[v->selection[j]]) * mult;
        VecResult r;
        r.data = out; r.type = DRAKEN_TIMESTAMP64; r.validity_embedded = 0u;
        r.ts_unit = static_cast<uint8_t>(ts_unit);
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    });
}

// DATE32 → VARCHAR: "YYYY-MM-DD" (10 chars, always inline). Fixed-width: every
// output is 10 bytes, so no arena and the block size is known up front — write
// slots straight into the consolidated block (no separate-buffers consolidation).
VecResult draken_cast_date_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_DATE32)
            return draken_error_sentinel_fmt("cast date->string: expected DATE32, got %d", v->type);
        const uint32_t k = v->data_length;
        const int32_t* src = static_cast<const int32_t*>(v->data);

        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* vunused;
        uint8_t* block = vecresult_string_block_alloc(k, 0u, 0, &slots, &arena, &vunused);
        if (!block) return draken_error_sentinel("Allocation failed");
        (void)arena; (void)vunused;  // all-inline: no arena; validity preserved separately

        uint8_t buf[10];
        for (uint32_t j = 0u; j < k; ++j) {
            int32_t y; int32_t m; int32_t d;
            days_to_ymd(src[j], y, m, d);
            buf[0] = static_cast<uint8_t>('0' + (y / 1000) % 10);
            buf[1] = static_cast<uint8_t>('0' + (y / 100)  % 10);
            buf[2] = static_cast<uint8_t>('0' + (y / 10)   % 10);
            buf[3] = static_cast<uint8_t>('0' + y % 10);
            buf[4] = '-';
            buf[5] = static_cast<uint8_t>('0' + m / 10);
            buf[6] = static_cast<uint8_t>('0' + m % 10);
            buf[7] = '-';
            buf[8] = static_cast<uint8_t>('0' + d / 10);
            buf[9] = static_cast<uint8_t>('0' + d % 10);
            draken_build_string_slot(&slots[j], buf, 10u, 0u);
        }
        VecResult r = vecresult_from_string_block(block, k, 0u, 0, DRAKEN_VARCHAR);
        kernel_preserve_shape(r, v);
        return r;
    });
}

// TIMESTAMP64 → VARCHAR: "YYYY-MM-DD HH:MM:SS.ffffff+0000" (31 chars, extern).
// Compression-aware: format the K physical values into a K-slot value block, then
// preserve the input's selection + validity (dict timestamp → dict string).
VecResult draken_cast_timestamp_to_string(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        if (v->type != DRAKEN_TIMESTAMP64)
            return draken_error_sentinel_fmt("cast timestamp->string: expected TIMESTAMP64, got %d", v->type);
        const uint32_t k = v->data_length;
        const int64_t* src = static_cast<const int64_t*>(v->data);

        // Every physical value formats to exactly 31 bytes (extern), so the K-slot
        // value block size is known up front: K slots + K*31 arena, no embedded validity.
        const size_t total_arena = static_cast<size_t>(k) * 31u;
        DrakenStringSlot* slots;
        uint8_t* arena;
        uint8_t* vunused;
        uint8_t* block = vecresult_string_block_alloc(k, total_arena, 0, &slots, &arena, &vunused);
        if (!block) return draken_error_sentinel("Allocation failed");
        (void)vunused;

        size_t arena_used = 0u;
        uint8_t buf[31];
        for (uint32_t j = 0u; j < k; ++j) {
            int64_t us = src[j];
            int64_t sec = us / 1000000LL;
            int32_t usec = static_cast<int32_t>(us % 1000000LL);
            if (usec < 0) { sec -= 1; usec += 1000000; }
            int64_t days64 = sec / 86400LL;
            int32_t tod = static_cast<int32_t>(sec % 86400LL);
            if (tod < 0) { days64 -= 1; tod += 86400; }
            int32_t y; int32_t m; int32_t d;
            days_to_ymd(static_cast<int32_t>(days64), y, m, d);
            const int32_t hh = tod / 3600;
            const int32_t mm = (tod % 3600) / 60;
            const int32_t ss = tod % 60;
            buf[0]=static_cast<uint8_t>('0'+(y/1000)%10); buf[1]=static_cast<uint8_t>('0'+(y/100)%10);
            buf[2]=static_cast<uint8_t>('0'+(y/10)%10);   buf[3]=static_cast<uint8_t>('0'+y%10);
            buf[4]='-'; buf[5]=static_cast<uint8_t>('0'+m/10); buf[6]=static_cast<uint8_t>('0'+m%10);
            buf[7]='-'; buf[8]=static_cast<uint8_t>('0'+d/10); buf[9]=static_cast<uint8_t>('0'+d%10);
            buf[10]=' '; buf[11]=static_cast<uint8_t>('0'+hh/10); buf[12]=static_cast<uint8_t>('0'+hh%10);
            buf[13]=':'; buf[14]=static_cast<uint8_t>('0'+mm/10); buf[15]=static_cast<uint8_t>('0'+mm%10);
            buf[16]=':'; buf[17]=static_cast<uint8_t>('0'+ss/10); buf[18]=static_cast<uint8_t>('0'+ss%10);
            buf[19]='.';
            buf[20]=static_cast<uint8_t>('0'+(usec/100000)%10); buf[21]=static_cast<uint8_t>('0'+(usec/10000)%10);
            buf[22]=static_cast<uint8_t>('0'+(usec/1000)%10);   buf[23]=static_cast<uint8_t>('0'+(usec/100)%10);
            buf[24]=static_cast<uint8_t>('0'+(usec/10)%10);     buf[25]=static_cast<uint8_t>('0'+usec%10);
            buf[26]='+'; buf[27]='0'; buf[28]='0'; buf[29]='0'; buf[30]='0';
            std::memcpy(arena + arena_used, buf, 31u);
            draken_build_string_slot(&slots[j], buf, 31u, static_cast<uint32_t>(arena_used));
            arena_used += 31u;
        }
        VecResult r = vecresult_from_string_block(block, k, total_arena, 0, DRAKEN_VARCHAR);
        kernel_preserve_shape(r, v);
        return r;
    });
}

namespace {
// Kept OUTSIDE the DRAKEN_KERNEL_TRY macro body below: an in-body brace-init
// array literal's commas are seen as macro-argument separators (the
// preprocessor tracks () nesting, not {}), which breaks the macro call with
// "too many arguments". A plain function call has none of that ambiguity.
inline int64_t fk_ts_ticks_per_sec(unsigned unit) {
    switch (unit & 3) {
        case 1: return 1000;
        case 2: return 1000000;
        case 3: return 1000000000;
        default: return 1;   // 0 = seconds
    }
}
}  // namespace

// TIMESTAMP64 → TIMESTAMP64 unit rescale (e.g. `EventTime::TIMESTAMP[s]` where
// EventTime is already TIMESTAMP64 at a different unit). ctx = binary_op_ctx:
// left_unit = source TimestampUnit, right_unit = target TimestampUnit (both
// 0=s,1=ms,2=us,3=ns — the SAME convention draken_date_part/draken_date_trunc
// use, NOT cast_timestamp_ctx's older 1=ns..5=days numbering). Narrowing
// (source finer than target, e.g. us->s) floors toward -inf for correctness on
// pre-epoch timestamps; widening (source coarser, e.g. s->us) is an exact
// multiply. Identity (src==dst) degrades to a copy. DENSE emit (gather through
// selection) per the date32-cast lesson: shape-preserving output caused a
// silent wrong-answer bug there.
VecResult draken_cast_timestamp_rescale(void* ctx, const DrakenVector* v) {
    DRAKEN_KERNEL_TRY({
        if (!v) return draken_error_sentinel("Input vector is null");
        // INT64 accepted alongside TIMESTAMP64: same 8-byte payload — an INT64
        // source is "a timestamp at the ctx-declared source unit" (the SQL
        // suffix's interpretation), rescaled to the declared result unit.
        if (v->type != DRAKEN_TIMESTAMP64 && v->type != DRAKEN_INT64)
            return draken_error_sentinel_fmt("cast timestamp rescale: expected TIMESTAMP64/INT64, got %d", v->type);
        if (!ctx) return draken_error_sentinel("cast timestamp rescale: missing ctx (units)");
        const auto* c = static_cast<const binary_op_ctx*>(ctx);
        const int64_t src_ticks = fk_ts_ticks_per_sec(c->left_unit);
        const int64_t dst_ticks = fk_ts_ticks_per_sec(c->right_unit);
        const TimestampUnit dst_unit = static_cast<TimestampUnit>(c->right_unit & 3);

        const uint32_t n = v->length;
        const int64_t* src = static_cast<const int64_t*>(v->data);
        int64_t* out = static_cast<int64_t*>(draken_malloc((n > 0u ? n : 1u) * sizeof(int64_t)));
        if (!out) return draken_error_sentinel("Allocation failed");
        for (uint32_t j = 0u; j < n; ++j) {
            int64_t raw = src[v->selection[j]];
            if (src_ticks >= dst_ticks) {
                int64_t factor = src_ticks / dst_ticks;
                if (factor == 1) {
                    out[j] = raw;
                } else {
                    int64_t q = raw / factor;
                    int64_t r = raw % factor;
                    out[j] = (r != 0 && ((r < 0) != (factor < 0))) ? q - 1 : q;
                }
            } else {
                out[j] = raw * (dst_ticks / src_ticks);
            }
        }
        VecResult r;
        r.data = out; r.type = DRAKEN_TIMESTAMP64; r.validity_embedded = 0u;
        r.ts_unit = static_cast<uint8_t>(dst_unit);
        r.length = n; r.data_length = n;
        r.selection = draken_identity_sel(n);
        r.owns_selection = false;
        r.validity = kernel_copy_validity(v);
        r.flags = DRAKEN_SEL_IDENTITY | DRAKEN_SEL_PERMUTATION;
        return r;
    });
}

// DATE32 → TIMESTAMP64 is implemented above (real, registered). timestamp→date32
// remains a stub.
VecResult draken_cast_timestamp_to_date32(void* ctx, const DrakenVector* vector) {
    DRAKEN_KERNEL_TRY({ return draken_error_sentinel("cast timestamp->date32 not yet implemented"); });
}

}  // extern "C"
