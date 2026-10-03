#pragma once
// draken/core/draken_capi.h — the Python-free C ABI over VectorOwner / VecResult.
//
// No <Python.h>, no nanobind: native C++ (the opteryx engine, the planner's file
// statistics) includes THIS header, never the Python bridge
// (vectors/_vector_bridge.h). Draken must execute without Python (CLAUDE.md
// §1/§2). Implementations live in morsels/cxx_morsel_ops.cpp, compiled into
// draken_native.so and resolved by other .so's through the RTLD_GLOBAL load in
// draken/__init__.py.
//
// C++ only: VectorOwner and VecResult are C++ types.

#include <stddef.h>
#include <stdint.h>

#include "core/buffers.h"
#include "ops/vec_result.h"

struct VectorOwner;   // full definition: core/vector_owner.h (included by callers)

extern "C" {

// draken_hash_rows — each logical row's Vector.hash_shaped() value into
// out[0..v->vec.length), through the binding's own implementation. Pure C++,
// GIL-free. 0, or -1 with the reason written to `error`.
int draken_hash_rows(const VectorOwner* v, uint64_t* out, char* error, size_t error_len);

// draken_vecresult_child_owner_new_c — heap-allocate a VectorOwner from a VecResult,
// with NO Python object created (unlike draken_vecresult_own_c). For embedding as
// ANOTHER VectorOwner's child_owner (an ARRAY result's element vector), where a
// Python handle would be wasted and wrong — child_owner wants sole C++ ownership,
// not a refcounted Python wrapper.
//
// A raw pointer (not VectorOwner by value) crosses this C-linkage boundary
// deliberately: VectorOwner is move-only (unique_ptr members), and a non-trivial
// C++ type returned by value through `extern "C"` is compiler-ABI-dependent, not a
// portable C contract — see draken_vecresult_own_c's comment on why VecResult
// (also a C++ struct) gets a dedicated C-linkage alias instead of relying on the
// mangled C++ symbol resolving cross-.so. A pointer has no such ambiguity.
//
// MOVES ownership from res, identical to draken_vecresult_own_c. Recurses through
// res.child (freed after adoption — same contract as vecresult_to_owner).
// Returns a NEW heap allocation; the caller adopts it into a
// std::unique_ptr<VectorOwner> (default `delete` is correct — allocated with
// plain `new`, both sides are C++ compiled by the same toolchain).
VectorOwner* draken_vecresult_child_owner_new_c(VecResult res);

// draken_vecresult_discard_c — free a heap-boxed child VecResult* (as produced by
// a kernel's `new VecResult(...)`, e.g. VecResult::child) WITHOUT adopting it
// anywhere. For error paths that received a valid ARRAY result but cannot use it
// (see evaluation.pyx's evaluate_c_native) — frees data/validity/selection/nested
// child correctly via the same vecresult_to_owner RAII teardown, then the box
// itself. `res` may be NULL (no-op). Safe to call exactly once per pointer.
void draken_vecresult_discard_c(VecResult* res);

}  // extern "C"
