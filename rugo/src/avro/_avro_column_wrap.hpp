#pragma once
// rugo/src/avro/_avro_column_wrap.hpp — Python edge of the Avro reader.
//
// avro_reader.cpp is pure C++ (rugo runs without Python): it produces AvroColumn
// buffers. Turning one into a Draken Vector is the only CPython step, so it lives
// here and is compiled into rugo_native only.

#include <Python.h>

#include "avro_reader.hpp"

namespace rugo::avro {

// Wrap `col` into an owned Draken Vector; its buffers are transferred (col is left
// empty either way). Call under the GIL. NEW reference, or NULL with an exception set.
PyObject* wrap_avro_column(AvroColumn& col);

}  // namespace rugo::avro
