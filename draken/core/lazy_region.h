#pragma once
// draken/core/lazy_region.h — row selection, narrowing and scatter for LAZY
// branch evaluation in the expression VM.
//
// A guarded operand (the right side of AND/OR, a CASE/IIF branch, a COALESCE
// argument) must only be evaluated on the rows that need it: a checked-arithmetic
// or cast error on a row the guard excludes must never surface. The VM therefore
//   1. asks draken_lz_rows for the rows the guard admits,
//   2. narrows every column the branch loads to those rows (draken_lz_narrow),
//   3. runs the branch over k rows,
//   4. scatters the k-row result back to full length (draken_lz_scatter), the
//      excluded rows NULL, so the ordinary blend / Kleene kernels combine it.
//
// Every function returns/uses owned draken_malloc buffers (VecResult) for the VM to
// adopt into its frame arena; a VecResult with data == nullptr is an error and
// carries error_msg. Unsupported operand types (ARRAY, VECTOR_FP16, NULL) fail
// loud — never a silent fallback.

#include <stdint.h>
#include "core/buffers.h"
#include "ops/vec_result.h"

// Which rows a guard admits. `guards` are `nguards` already-evaluated vectors on
// the VM stack; row i is admitted when:
enum {
    DRAKEN_LZ_NOT_FALSE = 1,  // NO guard is FALSE (valid and false)         — AND, DNF term j
    DRAKEN_LZ_NOT_TRUE  = 2,  // NO guard is TRUE  (valid and true)          — OR, CNF term j, CASE ELSE
    DRAKEN_LZ_TRUE      = 3,  // the guard is TRUE                           — CASE THEN
    DRAKEN_LZ_ALL_NULL  = 4,  // EVERY guard is NULL                         — COALESCE argument j
    DRAKEN_LZ_VALID     = 5   // the guard is not NULL                       — IFNOTNULL result
};

#ifdef __cplusplus
extern "C" {
#endif

// Fills out_rows (capacity >= n) with the admitted row indices, ascending, and
// returns how many (k). Guards of type DRAKEN_NULL are all-NULL.
uint32_t draken_lz_rows(int kind, const DrakenVector* const* guards, uint32_t nguards,
                        uint32_t n, uint32_t* out_rows);

// The k-row vector whose row j is row rows[j] of v (dense, identity selection).
VecResult draken_lz_narrow(const DrakenVector* v, const uint32_t* rows, uint32_t k);

// The n-row vector whose row rows[j] is row j of `compact` and whose every other
// row is NULL (dense, identity selection). k >= 1.
VecResult draken_lz_scatter(const DrakenVector* compact, const uint32_t* rows,
                            uint32_t k, uint32_t n);

#ifdef __cplusplus
}
#endif
