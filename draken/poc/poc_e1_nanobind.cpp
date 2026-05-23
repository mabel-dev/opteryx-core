// Milestone E.1 — nanobind Python edge for the typed kernel POC.
//
// Pattern:
//   .pyx  (poc_e1_kernel.pyx)  — cdef kernels; zero object; zero Python.
//   .cpp  (this file)          — the ONLY Python-visible entry points.
//
// draken_vector_unwrap is compiled into draken_native.so and resolved at
// runtime via -undefined dynamic_lookup (macOS) / --allow-shlib-undefined
// (Linux). run_poc_e1.py loads draken_native with RTLD_GLOBAL first.
//
// The cdef public functions from poc_e1_kernel.pyx are linked directly into
// poc_e1.so because both files are listed as sources for the same Extension.
// They are declared below as extern "C" — no Cython-generated header needed.

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>

#include "core/buffers.h"
#include "core/draken_bridge.h"

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Forward declarations for the typed cdef kernels from poc_e1_kernel.pyx.
// Cython's `cdef public` in C++ mode uses __PYX_EXTERN_C = extern "C++"
// (not plain C), so the generated symbols have C++ name mangling.
// Match that here with plain C++ declarations (no extern "C" block).
// ---------------------------------------------------------------------------
int64_t poc_e1_sum_kernel(const DrakenVector* dv, uint32_t* nonnull);
int64_t poc_e1_min_kernel(const DrakenVector* dv, uint32_t* nonnull);
int64_t poc_e1_max_kernel(const DrakenVector* dv, uint32_t* nonnull);

// ---------------------------------------------------------------------------
// Python entry points — nb::object is the ONLY place boxing happens.
// ---------------------------------------------------------------------------

NB_MODULE(poc_e1, m) {
    m.attr("__doc__") = (
        "Milestone E.1 POC — nanobind edge + typed cdef kernel.\n"
        "\n"
        "Proves the architecture: .pyx has zero object; Python edge is C++.\n"
        "draken_vector_unwrap type-checks; raises TypeError on non-Vector.\n"
    );

    // sum_kernel: unwrap → cdef kernel (nogil) → return Python int.
    m.def("sum_kernel",
        [](nb::object vec) -> int64_t {
            const DrakenVector* dv = draken_vector_unwrap(vec.ptr());
            if (!dv) throw nb::python_error();  // TypeError already set
            uint32_t nonnull = 0;
            int64_t result = poc_e1_sum_kernel(dv, &nonnull);
            return result;
        },
        nb::arg("vec"),
        "Sum all non-null int64 values. Raises TypeError on non-Vector."
    );

    // min_kernel
    m.def("min_kernel",
        [](nb::object vec) -> int64_t {
            const DrakenVector* dv = draken_vector_unwrap(vec.ptr());
            if (!dv) throw nb::python_error();
            uint32_t nonnull = 0;
            int64_t result = poc_e1_min_kernel(dv, &nonnull);
            return result;
        },
        nb::arg("vec"),
        "Minimum non-null int64 value. Raises TypeError on non-Vector."
    );

    // max_kernel
    m.def("max_kernel",
        [](nb::object vec) -> int64_t {
            const DrakenVector* dv = draken_vector_unwrap(vec.ptr());
            if (!dv) throw nb::python_error();
            uint32_t nonnull = 0;
            int64_t result = poc_e1_max_kernel(dv, &nonnull);
            return result;
        },
        nb::arg("vec"),
        "Maximum non-null int64 value. Raises TypeError on non-Vector."
    );
}
