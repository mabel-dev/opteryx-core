#pragma once
// rugo/src/parquet/_parquet_column_wrap.hpp — Python edge of the parquet column materializer.
//
// column_materialize.{hpp,cpp} is pure C++ (rugo runs without Python): it turns a
// DecodedColumn into owned Draken buffers with no Python involved. Turning those
// buffers into a Draken Vector handle is the only CPython step, so it lives here,
// beside the Cython wrapper, and is compiled into rugo_native only.

#include <Python.h>

#include "column_materialize.hpp"  // MaterializedColumn

namespace rugo::_parquet {

// Wrap a MaterializedColumn into an owned Draken Vector. Every buffer is consumed,
// on success or failure. Creates a Python object — call under the GIL. Returns a
// NEW reference, or NULL with an exception set on failure.
PyObject* wrap_parquet_column(MaterializedColumn& mc);

}  // namespace rugo::_parquet
