#include "_avro_column_wrap.hpp"

#include "core/alloc.h"

// Producer surface (resolved at load via RTLD_GLOBAL from draken_native.so).
#include "vectors/_vector_bridge.h"

namespace rugo::avro {

PyObject* wrap_avro_column(AvroColumn& c) {
    // Every draken_vector_own_* takes ownership on entry, success or failure.
    PyObject* v = nullptr;
    switch (c.kind) {
        case OutKind::Raw:
            v = draken_vector_own_raw(c.data, c.validity, c.length, c.type);
            break;
        case OutKind::String:
            v = draken_vector_own_string(c.slots, c.arena, c.arena_len, c.validity, c.length, c.type);
            break;
        case OutKind::Dict:
            v = draken_vector_own_string_dict(c.slots, c.arena, c.arena_len, c.codes, c.dict_len,
                                              c.validity, c.length, c.type);
            break;
        case OutKind::Decimal64:
            v = draken_vector_own_decimal(c.data, c.validity, c.length, c.precision, c.scale);
            break;
        case OutKind::Decimal128:
            v = draken_vector_own_decimal128(c.data, c.validity, c.length, c.precision, c.scale);
            break;
        case OutKind::Timestamp:
            v = draken_vector_own_timestamp(c.data, c.validity, c.length, "us");
            break;
        case OutKind::Time64:
            v = draken_vector_own_time64(c.data, c.validity, c.length, "us");
            break;
        case OutKind::Ipv4:
            // LogicalKind ordinal 5 = IPV4 (draken/logical_type.h); no unit / scale / dimension.
            v = draken_vector_own_raw_logical(c.data, c.validity, c.length, c.type, 5, 0, 0, 0, 0, 0);
            break;
        case OutKind::ConstRaw:
            if (c.type == DRAKEN_TIMESTAMP64 || c.type == DRAKEN_TIME64 || c.type == DRAKEN_DECIMAL ||
                c.type == DRAKEN_DECIMAL128) {
                // The engine builds these natively with their logical type attached;
                // no Python-edge producer takes value list + positions + a logical
                // descriptor (docs §19.4). `c` frees its buffers when it is destroyed.
                PyErr_SetString(PyExc_RuntimeError,
                                c.type == DRAKEN_TIMESTAMP64 || c.type == DRAKEN_TIME64
                                    ? "Avro: a constant TIME/TIMESTAMP column (a reader-schema field the file "
                                      "does not hold) has no draken producer that carries its logical type at "
                                      "the Python edge (docs §19.4)"
                                    : "Avro: a constant DECIMAL column (a reader-schema field the file does "
                                      "not hold) has no draken producer that carries its logical type at the "
                                      "Python edge (docs §19.4)");
                return nullptr;
            }
            v = draken_vector_own_dict(c.data, 1, c.codes, c.length, c.validity, c.type);
            break;
        case OutKind::ConstString:
            v = draken_vector_own_string_dict(c.slots, c.arena, c.arena_len, c.codes, 1,
                                              c.validity, c.length, c.type);
            break;
        case OutKind::Array:
            if (c.child_col) {
                // An array of arrays: wrap the child first (it takes its own buffers), then hang it
                // under this level's offsets; draken moves the child's owner subtree into the parent.
                PyObject* child = wrap_avro_column(*c.child_col);
                if (child == nullptr) {
                    draken_free(c.offsets);
                    draken_free(c.validity);
                    v = nullptr;
                    break;
                }
                v = draken_vector_own_array_child(c.offsets, child, c.validity, c.length);
                Py_DECREF(child);
                break;
            }
            if (c.child_type == DRAKEN_VARCHAR || c.child_type == DRAKEN_VARBINARY)
                v = draken_vector_own_array(c.offsets, c.slots, c.arena, c.arena_len, c.child_length,
                                            c.child_type, c.child_validity, c.validity, c.length);
            else
                v = draken_vector_own_array_numeric(c.offsets, c.data, c.child_validity, c.child_length,
                                                    c.child_type, c.validity, c.length);
            break;
    }
    c.disown();
    return v;
}

}  // namespace rugo::avro
