// opteryx/compiled/nanobind/vector_math.cpp — Milestone E.3, Part B + E.19.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, five functions.
//
// Each function:
//   1. Receives nb::object operand(s).
//   2. Calls draken_vector_unwrap (declared in draken_bridge.h,
//      implemented in draken_native.so — resolved at import time via
//      RTLD_GLOBAL set in draken/__init__.py).
//   3. Calls draken::ops::float_* from draken/ops/float_math.h.
//   4. Wraps the VecResult via draken_vector_own_raw → returns new Vector.
//
// Type-check contract: draken_vector_unwrap raises TypeError and returns
// nullptr on non-Vector input — never segfaults.
//
// ROUND semantics: half-to-even via 2^52 trick (see float_math.h).
//   boost::math::round is half-away-from-zero, same as std::round —
//   it does NOT provide half-to-even.  The 2^52 trick uses the hardware
//   FP rounding mode (FE_TONEAREST = half-to-even, IEEE 754 default).
//
// Replaces: opteryx/compiled/vector_ops/vector_{abs,sign,sqrt,round}.pyx
//           (deleted as part of E.3).
//           opteryx/compiled/vector_ops/vector_math.pyx (ceil/floor/trunc/power/random)
//           (deleted as part of E.19).

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cmath>
#include <random>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/draken_bridge.h"  // draken_vector_unwrap, draken_vector_own_raw
#include "ops/float_math.h"      // draken::ops::float_*

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// RNG state for RANDOM() / RANDOM_NORMAL()
// Module-level statics; not thread-safe (matches prior Cython behaviour).
// ---------------------------------------------------------------------------

static std::mt19937_64 rng_uniform(std::random_device{}());
static std::mt19937_64 rng_normal(674162347314ULL);
static std::uniform_real_distribution<double> dist_uniform(0.0, 1.0);

// ---------------------------------------------------------------------------
// Helpers (mirror vector_bitwise.cpp)
// ---------------------------------------------------------------------------

static const DrakenVector* unwrap(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    return dv;
}

static nb::object wrap(VecResult res) {
    PyObject* out = draken_vector_own_raw(res.data, res.validity, res.length, res.type);
    if (!out) throw nb::python_error();
    return nb::steal<nb::object>(out);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_math, m) {

    m.def("vector_abs",
        [](nb::object v) -> nb::object {
            return wrap(draken::ops::float_abs(*unwrap(v)));
        },
        nb::arg("v"),
        "ABS(v): element-wise absolute value. "
        "Integer types: same type, INT*_MIN wraps (C convention). "
        "Float types → FLOAT64. NULL propagates.");

    m.def("vector_sign",
        [](nb::object v) -> nb::object {
            return wrap(draken::ops::float_sign(*unwrap(v)));
        },
        nb::arg("v"),
        "SIGN(v): element-wise sign → INT8 ∈ {-1, 0, 1}. "
        "NaN input rows become null. NULL propagates.");

    m.def("vector_sqrt",
        [](nb::object v) -> nb::object {
            return wrap(draken::ops::float_sqrt(*unwrap(v)));
        },
        nb::arg("v"),
        "SQRT(v): element-wise square root → FLOAT64. "
        "Negative integer input raises std::invalid_argument. "
        "Negative float input → NaN (IEEE 754). NULL propagates.");

    m.def("vector_round",
        [](nb::object v) -> nb::object {
            return wrap(draken::ops::float_round(*unwrap(v), 0));
        },
        nb::arg("v"),
        "ROUND(v): round to nearest integer, half-to-even (banker's rounding). "
        "Integer types: identity (already integers, same type). "
        "Float types → FLOAT64. NULL propagates.");

    m.def("vector_round_digits",
        [](nb::object v, int digits) -> nb::object {
            return wrap(draken::ops::float_round(*unwrap(v), digits));
        },
        nb::arg("v"), nb::arg("digits"),
        "ROUND(v, digits): round to `digits` decimal places, half-to-even. "
        "Negative digits round to powers of ten (e.g. digits=-1 rounds to nearest 10). "
        "Integer types: identity (digits ignored for exact integers). "
        "Float types → FLOAT64. NULL propagates.");

    // -----------------------------------------------------------------------
    // E.19: CEIL / FLOOR / TRUNC / POWER / RANDOM / RANDOM_NORMAL
    // -----------------------------------------------------------------------

    m.def("vector_ceil",
        [](nb::object v, int scale) -> nb::object {
            return wrap(draken::ops::float_ceil(*unwrap(v), scale));
        },
        nb::arg("v"), nb::arg("scale") = 0,
        "CEILING(v [, scale]): element-wise ceiling with optional scale factor. "
        "scale > 0: decimal places. scale < 0: rounds to nearest 10^|scale|. "
        "All numeric types → FLOAT64. NULL propagates.");

    m.def("vector_floor",
        [](nb::object v, int scale) -> nb::object {
            return wrap(draken::ops::float_floor(*unwrap(v), scale));
        },
        nb::arg("v"), nb::arg("scale") = 0,
        "FLOOR(v [, scale]): element-wise floor with optional scale factor. "
        "All numeric types → FLOAT64. NULL propagates.");

    m.def("vector_trunc",
        [](nb::object v, int scale) -> nb::object {
            return wrap(draken::ops::float_trunc(*unwrap(v), scale));
        },
        nb::arg("v"), nb::arg("scale") = 0,
        "TRUNCATE(v [, scale]): element-wise truncation toward zero. "
        "All numeric types → FLOAT64. NULL propagates.");

    m.def("vector_power",
        [](nb::object v, double exponent) -> nb::object {
            return wrap(draken::ops::float_power(*unwrap(v), exponent));
        },
        nb::arg("v"), nb::arg("exponent"),
        "POWER(v, exponent): element-wise pow(v, exponent). "
        "All numeric types → FLOAT64. NULL propagates.");

    m.def("vector_random",
        [](uint32_t n) -> nb::object {
            double* dst = static_cast<double*>(draken_malloc(n * sizeof(double)));
            if (!dst) throw std::bad_alloc();
            for (uint32_t i = 0; i < n; ++i)
                dst[i] = dist_uniform(rng_uniform);
            PyObject* out = draken_vector_own_raw(dst, nullptr, n, DRAKEN_FLOAT64);
            if (!out) { draken_free(dst); throw nb::python_error(); }
            return nb::steal<nb::object>(out);
        },
        nb::arg("n"),
        "RANDOM(): generate n uniform random FLOAT64 values in [0, 1).");

    m.def("vector_random_normal",
        [](uint32_t n) -> nb::object {
            double* dst = static_cast<double*>(draken_malloc(n * sizeof(double)));
            if (!dst) throw std::bad_alloc();
            constexpr double TWO_PI = 6.283185307179586;
            constexpr double EPS    = 1e-300;
            std::uniform_real_distribution<double> dist01(0.0, 1.0);
            const uint32_t pairs = n >> 1;
            for (uint32_t i = 0; i < pairs; ++i) {
                double u1, u2;
                do { u1 = dist01(rng_normal); } while (u1 < EPS);
                u2  = dist01(rng_normal);
                double mag = std::sqrt(-2.0 * std::log(u1));
                dst[2 * i]     = mag * std::cos(TWO_PI * u2);
                dst[2 * i + 1] = mag * std::sin(TWO_PI * u2);
            }
            if (n & 1u) {
                double u1, u2;
                do { u1 = dist01(rng_normal); } while (u1 < EPS);
                u2 = dist01(rng_normal);
                dst[n - 1] = std::sqrt(-2.0 * std::log(u1)) * std::cos(TWO_PI * u2);
            }
            PyObject* out = draken_vector_own_raw(dst, nullptr, n, DRAKEN_FLOAT64);
            if (!out) { draken_free(dst); throw nb::python_error(); }
            return nb::steal<nb::object>(out);
        },
        nb::arg("n"),
        "RANDOM_NORMAL(): generate n standard-normal FLOAT64 values via Box-Muller. "
        "Fixed seed (674162347314) for reproducibility across calls.");
}
