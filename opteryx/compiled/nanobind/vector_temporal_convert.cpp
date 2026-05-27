// opteryx/compiled/nanobind/vector_temporal_convert.cpp — Milestone E.12, Phase 11, C′.
//
// C′ pattern: pure nanobind C++, zero Cython.  One NB_MODULE, four functions:
//
//   vector_date32_to_timestamp(v, [unit])      — DATE32 → TIMESTAMP64 + mandatory unit.
//   vector_timestamp_to_date32(v)              — TIMESTAMP64 → DATE32 (floor to day boundary).
//   vector_unixtime(v)                         — TIMESTAMP64|DATE32 → INT64 unix seconds.
//   vector_floor_temporal(v, magnitude, units) — TIMESTAMP64 → TIMESTAMP64 (same unit, floored).
//
// Unit handling:
//   Functions that need to read the input's unit call PyObject_GetAttrString on the
//   "logical_type_unit" attribute (exposed on draken_native.Vector by D.8).
//   This is a function-level call (not per-row); the unit is hoisted before the loop.
//
// floor_div: all integer division uses floor semantics (toward −∞) matching Python //.
//   C++ / truncates toward zero — the correction is applied for negative timestamps.
//
// Supported floor units: second, minute, hour, day (singular or plural, case-insensitive).
//   Month/year involve non-uniform calendar math and are out of scope for this batch.
//
// Null TVL: null input row → null output row; validity bitmap copied from input.
// Fails loud on non-Vector input, wrong DrakenType, missing unit descriptor.
//
// Replaces: opteryx/compiled/vector_ops/vector_date32_to_timestamp.pyx
//           opteryx/compiled/vector_ops/vector_timestamp_to_date32.pyx
//           opteryx/compiled/vector_ops/vector_unixtime.pyx
//           opteryx/compiled/vector_ops/vector_floor_temporal.pyx

#include <Python.h>
#include <nanobind/nanobind.h>
#include <cstdint>
#include <cstring>
#include <cctype>
#include <stdexcept>

#include "core/buffers.h"
#include "core/alloc.h"
#include "core/draken_bridge.h"
#include "logical_type.h"   // TimestampUnit (SECONDS/MILLISECONDS/MICROSECONDS/NANOSECONDS)

namespace nb = nanobind;

// ---------------------------------------------------------------------------
// Row-level helpers and scalar extraction
// ---------------------------------------------------------------------------

static inline bool row_is_null(const DrakenVector* dv, uint32_t i) noexcept {
    if (!dv->validity) return false;
    return !((dv->validity[i >> 3] >> (i & 7u)) & 1u);
}

// Extract scalar integer from a Python sequence-like object.
static int64_t extract_scalar_int(nb::object seq) {
    try {
        nb::object first = seq[0];
        return nb::cast<int64_t>(first);
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_TypeError, "Failed to extract integer scalar from constant vector");
        throw nb::python_error();
    }
}

// Extract scalar string from a Python sequence-like object.
static const char* extract_scalar_string(nb::object seq) {
    try {
        nb::object first = seq[0];
        if (PyUnicode_Check(first.ptr())) {
            const char* s = PyUnicode_AsUTF8(first.ptr());
            if (!s) throw nb::python_error();
            return s;
        } else if (PyBytes_Check(first.ptr())) {
            const char* s = PyBytes_AS_STRING(first.ptr());
            if (!s) throw nb::python_error();
            return s;
        } else {
            // Try str() conversion
            nb::object str_obj = nb::cast<nb::object>(nb::str(first));
            const char* s = PyUnicode_AsUTF8(str_obj.ptr());
            if (!s) throw nb::python_error();
            return s;
        }
    } catch (const std::exception& e) {
        PyErr_SetString(PyExc_TypeError, "Failed to extract string scalar from constant vector");
        throw nb::python_error();
    }
}

// Deep-copy the logical-row validity bitmap.  Returns nullptr when all-valid.
static uint8_t* copy_validity(const DrakenVector* dv) {
    if (!dv->validity) return nullptr;
    const uint32_t bm     = (dv->length + 7u) >> 3;
    const uint32_t padded = (bm + 7u) & ~7u;
    const size_t   vbytes = padded > 0u ? padded : 8u;
    uint8_t* out = static_cast<uint8_t*>(draken_malloc(vbytes));
    if (!out) throw std::bad_alloc();
    std::memcpy(out, dv->validity, vbytes);
    return out;
}

// ---------------------------------------------------------------------------
// Timestamp unit helpers
// ---------------------------------------------------------------------------

// Read the unit of a DRAKEN_TIMESTAMP64 vector from its logical_type_unit attribute.
// Raises TypeError if attribute is absent/None, ValueError for unknown unit strings.
static TimestampUnit get_ts_unit(nb::object obj) {
    PyObject* raw = PyObject_GetAttrString(obj.ptr(), "logical_type_unit");
    if (!raw) throw nb::python_error();
    nb::object unit_obj = nb::steal<nb::object>(raw);
    if (unit_obj.is_none())
        throw nb::type_error(
            "TIMESTAMP64 vector is missing mandatory logical_type_unit descriptor");
    const char* s = PyUnicode_AsUTF8(unit_obj.ptr());
    if (!s) throw nb::python_error();
    if (std::strcmp(s, "us") == 0) return TimestampUnit::MICROSECONDS;
    if (std::strcmp(s, "ms") == 0) return TimestampUnit::MILLISECONDS;
    if (std::strcmp(s, "ns") == 0) return TimestampUnit::NANOSECONDS;
    if (std::strcmp(s, "s")  == 0) return TimestampUnit::SECONDS;
    PyErr_Format(PyExc_ValueError,
        "vector_temporal: unknown timestamp unit '%s'; expected 's', 'ms', 'us', or 'ns'", s);
    throw nb::python_error();
}

// Convert TimestampUnit back to the string accepted by draken_vector_own_timestamp.
static const char* unit_to_str(TimestampUnit u) noexcept {
    switch (u) {
        case TimestampUnit::SECONDS:      return "s";
        case TimestampUnit::MILLISECONDS: return "ms";
        case TimestampUnit::MICROSECONDS: return "us";
        case TimestampUnit::NANOSECONDS:  return "ns";
        default:                          return "us";
    }
}

// Ticks per day in the given timestamp unit.
static int64_t ticks_per_day(TimestampUnit u) noexcept {
    switch (u) {
        case TimestampUnit::SECONDS:      return      86400LL;
        case TimestampUnit::MILLISECONDS: return   86400000LL;
        case TimestampUnit::MICROSECONDS: return 86400000000LL;
        case TimestampUnit::NANOSECONDS:  return 86400000000000LL;
        default:                          return 86400000000LL;
    }
}

// Ticks per second in the given timestamp unit.
static int64_t ticks_per_second(TimestampUnit u) noexcept {
    switch (u) {
        case TimestampUnit::SECONDS:      return          1LL;
        case TimestampUnit::MILLISECONDS: return       1000LL;
        case TimestampUnit::MICROSECONDS: return    1000000LL;
        case TimestampUnit::NANOSECONDS:  return 1000000000LL;
        default:                          return    1000000LL;
    }
}

// Floor division toward −∞ (matches Python //; C++ / truncates toward zero).
static inline int64_t floor_div(int64_t a, int64_t b) noexcept {
    const int64_t q = a / b;
    // Subtract 1 when the signs differ and there is a non-zero remainder.
    return q - (((a ^ b) < 0) & (q * b != a));
}

// ---------------------------------------------------------------------------
// vector_date32_to_timestamp
// ---------------------------------------------------------------------------

static nb::object impl_date32_to_timestamp(nb::object obj, const char* out_unit_str) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_DATE32)
        throw nb::type_error("vector_date32_to_timestamp: expected DRAKEN_DATE32 Vector");

    TimestampUnit out_unit = TimestampUnit::MICROSECONDS;
    if      (std::strcmp(out_unit_str, "ms") == 0) out_unit = TimestampUnit::MILLISECONDS;
    else if (std::strcmp(out_unit_str, "ns") == 0) out_unit = TimestampUnit::NANOSECONDS;
    else if (std::strcmp(out_unit_str, "s")  == 0) out_unit = TimestampUnit::SECONDS;
    // "us" leaves the default MICROSECONDS; any other value is treated as "us".

    const int64_t  scale = ticks_per_day(out_unit);
    const uint32_t n     = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    g.v = copy_validity(dv);

    const int32_t* src = static_cast<const int32_t*>(dv->data);
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }
        out[i] = static_cast<int64_t>(src[dv->selection[i]]) * scale;
    }

    uint8_t* validity = g.v;
    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_timestamp(out, validity, n, out_unit_str);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// vector_timestamp_to_date32
// ---------------------------------------------------------------------------

static nb::object impl_timestamp_to_date32(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_TIMESTAMP64)
        throw nb::type_error("vector_timestamp_to_date32: expected DRAKEN_TIMESTAMP64 Vector");

    const TimestampUnit in_unit = get_ts_unit(obj);
    const int64_t  scale = ticks_per_day(in_unit);
    const uint32_t n     = dv->length;

    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int32_t);
    int32_t* out = static_cast<int32_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int32_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    g.v = copy_validity(dv);

    const int64_t* src = static_cast<const int64_t*>(dv->data);
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }
        out[i] = static_cast<int32_t>(floor_div(src[dv->selection[i]], scale));
    }

    uint8_t* validity = g.v;
    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, validity, n, DRAKEN_DATE32);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// vector_unixtime
// ---------------------------------------------------------------------------

static nb::object impl_unixtime(nb::object obj) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();

    const uint32_t n = dv->length;
    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    g.v = copy_validity(dv);

    if (dv->type == DRAKEN_TIMESTAMP64) {
        const TimestampUnit in_unit = get_ts_unit(obj);
        const int64_t scale = ticks_per_second(in_unit);
        const int64_t* src  = static_cast<const int64_t*>(dv->data);
        for (uint32_t i = 0u; i < n; ++i) {
            if (row_is_null(dv, i)) { out[i] = 0; continue; }
            out[i] = floor_div(src[dv->selection[i]], scale);
        }
    } else if (dv->type == DRAKEN_DATE32) {
        const int32_t* src = static_cast<const int32_t*>(dv->data);
        for (uint32_t i = 0u; i < n; ++i) {
            if (row_is_null(dv, i)) { out[i] = 0; continue; }
            out[i] = static_cast<int64_t>(src[dv->selection[i]]) * 86400LL;
        }
    } else {
        PyErr_Format(PyExc_TypeError,
            "vector_unixtime: expected TIMESTAMP64 or DATE32 Vector, got DrakenType %d",
            static_cast<int>(dv->type));
        throw nb::python_error();
    }

    uint8_t* validity = g.v;
    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_raw(out, validity, n, DRAKEN_INT64);
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// vector_floor_temporal
// ---------------------------------------------------------------------------

// Map floor unit name → seconds per unit.  Raises ValueError on unknown names.
static int64_t seconds_per_floor_unit(const char* units) {
    // Case-insensitive comparison.
    auto ci_eq = [](const char* a, const char* b) noexcept -> bool {
        while (*a && *b) {
            if (std::tolower(static_cast<unsigned char>(*a)) !=
                std::tolower(static_cast<unsigned char>(*b))) return false;
            ++a; ++b;
        }
        return *a == '\0' && *b == '\0';
    };
    if (ci_eq(units, "second")  || ci_eq(units, "seconds"))  return         1LL;
    if (ci_eq(units, "minute")  || ci_eq(units, "minutes"))  return        60LL;
    if (ci_eq(units, "hour")    || ci_eq(units, "hours"))    return      3600LL;
    if (ci_eq(units, "day")     || ci_eq(units, "days"))     return     86400LL;
    PyErr_Format(PyExc_ValueError,
        "vector_floor_temporal: unsupported unit '%s'; "
        "supported units: second, minute, hour, day (singular or plural)", units);
    throw nb::python_error();
}

static nb::object impl_floor_temporal(nb::object obj, int64_t magnitude, const char* units) {
    const DrakenVector* dv = draken_vector_unwrap(obj.ptr());
    if (!dv) throw nb::python_error();
    if (dv->type != DRAKEN_TIMESTAMP64)
        throw nb::type_error("vector_floor_temporal: expected DRAKEN_TIMESTAMP64 Vector");
    if (magnitude <= 0) {
        PyErr_Format(PyExc_ValueError,
            "vector_floor_temporal: magnitude must be positive, got %lld",
            static_cast<long long>(magnitude));
        throw nb::python_error();
    }

    const TimestampUnit in_unit      = get_ts_unit(obj);
    const int64_t secs_per_unit      = seconds_per_floor_unit(units);
    const int64_t native_per_unit    = secs_per_unit * ticks_per_second(in_unit);
    const int64_t floor_period       = native_per_unit * magnitude;

    const uint32_t n = dv->length;
    const size_t data_sz = (n > 0u ? n : 1u) * sizeof(int64_t);
    int64_t* out = static_cast<int64_t*>(draken_malloc(data_sz));
    if (!out) throw std::bad_alloc();

    struct Guard { int64_t* d; uint8_t* v;
        ~Guard() { if (d) draken_free(d); if (v) draken_free(v); } } g{out, nullptr};

    g.v = copy_validity(dv);

    const int64_t* src = static_cast<const int64_t*>(dv->data);
    for (uint32_t i = 0u; i < n; ++i) {
        if (row_is_null(dv, i)) { out[i] = 0; continue; }
        out[i] = floor_div(src[dv->selection[i]], floor_period) * floor_period;
    }

    uint8_t* validity = g.v;
    g.d = nullptr; g.v = nullptr;
    PyObject* result = draken_vector_own_timestamp(out, validity, n, unit_to_str(in_unit));
    if (!result) throw nb::python_error();
    return nb::steal<nb::object>(result);
}

// ---------------------------------------------------------------------------
// Dispatch wrappers (constant vector unwrapping)
// These are called from the Python dispatch layer and handle scalar extraction.
// ---------------------------------------------------------------------------

static nb::object dispatch_date_floor(nb::object dates, nb::object magnitude_seq, nb::object units_seq) {
    const int64_t magnitude = extract_scalar_int(magnitude_seq);
    const char* units = extract_scalar_string(units_seq);
    return impl_floor_temporal(dates, magnitude, units);
}

static nb::object dispatch_unixtime(nb::object array) {
    return impl_unixtime(array);
}

// Pure-Python function: convert Unix timestamps to UTC datetime objects.
// Unlike the vector functions, this operates on a Python iterable and returns a list.
#include <ctime>
static nb::object dispatch_from_unixtimestamp(nb::object values) {
    nb::list result;
    for (nb::handle v : values) {
        try {
            const time_t ts = nb::cast<time_t>(v);
            // Python datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)
            PyObject* dt_module = PyImport_ImportModule("datetime");
            if (!dt_module) throw nb::python_error();
            nb::object datetime_class = nb::steal<nb::object>(PyObject_GetAttrString(dt_module, "datetime"));
            Py_DECREF(dt_module);
            if (!datetime_class) throw nb::python_error();

            nb::object tz_module = nb::steal<nb::object>(PyImport_ImportModule("datetime"));
            nb::object timezone_class = nb::steal<nb::object>(PyObject_GetAttrString(tz_module.ptr(), "timezone"));
            nb::object utc = nb::steal<nb::object>(PyObject_GetAttrString(timezone_class.ptr(), "utc"));

            nb::object dt = datetime_class.attr("fromtimestamp")(nb::cast<double>(ts), utc);
            result.append(dt);
        } catch (const std::exception& e) {
            PyErr_SetString(PyExc_TypeError, "Failed to convert timestamp to datetime");
            throw nb::python_error();
        }
    }
    return nb::cast<nb::object>(result);
}

// ---------------------------------------------------------------------------
// NB_MODULE
// ---------------------------------------------------------------------------

NB_MODULE(vector_temporal_convert, m) {

    m.def("vector_date32_to_timestamp",
        [](nb::object v, nb::object unit_obj) -> nb::object {
            const char* u = "us";
            if (!unit_obj.is_none()) {
                if (!PyUnicode_Check(unit_obj.ptr()))
                    throw nb::type_error("unit must be a str or None");
                u = PyUnicode_AsUTF8(unit_obj.ptr());
                if (!u) throw nb::python_error();
            }
            return impl_date32_to_timestamp(v, u);
        },
        nb::arg("v"), nb::arg("unit") = nb::none(),
        "DATE32 → TIMESTAMP64. "
        "unit: output unit ('s', 'ms', 'us' (default), 'ns'). "
        "Math: days × ticks_per_day(unit). "
        "Mandatory LogicalType descriptor attached. Null rows propagate as null. "
        "Raises TypeError on non-DATE32 input.");

    m.def("vector_timestamp_to_date32",
        [](nb::object v) -> nb::object {
            return impl_timestamp_to_date32(v);
        },
        nb::arg("v"),
        "TIMESTAMP64 → DATE32 (floor to day boundary in input unit). "
        "Reads input unit from logical_type_unit attribute (mandatory; raises TypeError if absent). "
        "Math: floor(ts / ticks_per_day(unit)). Handles pre-epoch (negative) timestamps. "
        "Null rows propagate as null. Raises TypeError on non-TIMESTAMP64 input.");

    m.def("vector_unixtime",
        [](nb::object v) -> nb::object {
            return impl_unixtime(v);
        },
        nb::arg("v"),
        "TIMESTAMP64|DATE32 → INT64 unix seconds since epoch. "
        "TIMESTAMP64: floor(ts / ticks_per_second(unit)). "
        "DATE32: days × 86400. "
        "Handles pre-epoch (negative) timestamps via floor division. "
        "Null rows propagate as null. Raises TypeError on other types.");

    m.def("vector_floor_temporal",
        [](nb::object v, int64_t magnitude, nb::object units_obj) -> nb::object {
            if (!PyUnicode_Check(units_obj.ptr()))
                throw nb::type_error("units must be a str");
            const char* units = PyUnicode_AsUTF8(units_obj.ptr());
            if (!units) throw nb::python_error();
            return impl_floor_temporal(v, magnitude, units);
        },
        nb::arg("v"), nb::arg("magnitude"), nb::arg("units"),
        "Floor TIMESTAMP64 to a unit boundary. "
        "magnitude: positive int (e.g. 5 for '5 minutes'). "
        "units: 'second'/'seconds', 'minute'/'minutes', 'hour'/'hours', 'day'/'days' "
        "(case-insensitive). Month/year are out of scope (non-uniform calendar math). "
        "Output type: TIMESTAMP64, same unit as input. "
        "Null rows propagate as null. Raises TypeError on non-TIMESTAMP64 input, "
        "ValueError on unsupported units or non-positive magnitude.");

    // Dispatch wrappers that handle constant vector unwrapping (called from Python layer).
    m.def("date_floor", &dispatch_date_floor, nb::arg("dates"), nb::arg("magnitude"), nb::arg("units"),
        "Floor timestamp to unit boundary (dispatcher for constant-wrapped magnitude and units).");

    m.def("unixtime", &dispatch_unixtime, nb::arg("array"),
        "Convert timestamp or date to Unix seconds (dispatcher wrapper).");

    m.def("from_unixtimestamp", &dispatch_from_unixtimestamp, nb::arg("values"),
        "Convert iterable of Unix timestamps to list of UTC datetime objects.");
}
