#pragma once
// rugo/src/jsonl/_jsonl_column_wrap.hpp — Python edge of the JSONL column builder.
//
// core/ is pure C++ (rugo runs without Python): parse_all_columns produces
// ParsedColumn buffers with no Python involved. Turning one into a Draken Vector
// handle is the only CPython step, so it lives here, beside the Cython wrapper,
// and is compiled into rugo_native only.

#include <Python.h>

#include "core/column_builder.hpp"  // ParsedColumn

namespace rugo::_jsonl {

// Wrap a ParsedColumn into an owned Draken Vector. Creates a Python object — call under
// the GIL. Returns a NEW reference, or NULL with an exception set on failure.
PyObject* wrap_column(ParsedColumn& pc);

}  // namespace rugo::_jsonl
