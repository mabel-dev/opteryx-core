#pragma once
// rugo/src/csv/_csv_column_wrap.hpp — Python edge of the CSV column builder.
//
// core/ is pure C++ (rugo runs without Python): build_columns_streaming produces
// ParsedCsvColumn buffers with no Python involved. Turning one into a Draken Vector
// handle is the only CPython step, so it lives here, beside the Cython wrapper,
// and is compiled into rugo_native only.

#include <Python.h>

#include "core/csv_column_builder.hpp"  // ParsedCsvColumn

namespace rugo::_csv {

// Wrap a ParsedCsvColumn into an owned Draken Vector. Creates a Python object — call
// under the GIL. Returns a NEW reference, or NULL with an exception set on failure.
PyObject* wrap_csv_column(ParsedCsvColumn& pc);

}  // namespace rugo::_csv
