/*
 * bytecode_worker.h — C-callable interface for Opteryx's nogil bytecode VM.
 *
 * This header is shared between the Cython evaluator module and any C++ code
 * that needs to dispatch predicate evaluation from worker threads without
 * touching the Python interpreter.
 *
 * Lifecycle:
 *   1. Python imports opteryx.expression.evaluator — the Cython module init
 *      calls opteryx_set_worker_fn() to wire up the trampoline.
 *   2. C++ worker threads call through opteryx_worker_fn(&item) directly,
 *      with no GIL held. The trampoline calls c_execute_bytecode_inner().
 *   3. error_code == 0: result bitmap is at item.bitmaps[0].
 *      error_code != 0: unexpected opcode; caller must fall back to the GIL
 *      path (re-run via execute_bytecode from a GIL-held thread).
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif


/* All pointer fields are pre-prepared by the Python thread during the GIL-held
 * pre-pass (_execute_bytecode_prepass). Worker threads treat them as opaque. */
typedef struct BytecodeWorkerItem {
    /* Bytecode — set once at build time, immutable during execution. */
    const void*  instrs;       /* BytecodeInstr* — opaque to C++ */
    size_t       n_instrs;

    /* Column cache — pre-resolved BoolVector bitmap pointers per instruction. */
    const void*  col_cache;    /* ColCache* — opaque to C++ */

    /* Scratch bitmap pools — allocated in the pre-pass, freed by the caller.
     * bitmaps[0..n_slots-1] = stack slots; [n_slots] and [n_slots+1] = scratch. */
    uint8_t**    bitmaps;
    uint8_t**    null_bitmaps;
    int8_t*      slot_has_null;
    size_t       n_slots;      /* max stack depth (== bc.max_stack_depth) */

    /* Row geometry — fixed per morsel. */
    size_t       nbytes;       /* ceil(n_rows / 8) */
    size_t       n_rows;

    /* Output — written by the worker.
     * 0 = success (result bitmap at bitmaps[0]).
     * 1 = unexpected opcode encountered (bc.is_pure_bitmap was wrong);
     *     caller must re-run with GIL held. */
    int          error_code;
} BytecodeWorkerItem;


/* Function pointer type for the worker callable. */
typedef int (*opteryx_worker_fn_t)(BytecodeWorkerItem*);


/* Global function pointer — NULL until the Cython module has been imported.
 * Set by opteryx_set_worker_fn() during module init. Thread-safe after that
 * (pointer write is atomic on all supported architectures; workers only read
 * after module init completes). */
extern opteryx_worker_fn_t opteryx_worker_fn;


/* Called exactly once by the Cython evaluator module at import time. */
void opteryx_set_worker_fn(opteryx_worker_fn_t fn);

#ifdef __cplusplus
} /* extern "C" */
#endif
