#include "_avro_column_wrap.hpp"

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
        case OutKind::ConstRaw:
            v = draken_vector_own_dict(c.data, 1, c.codes, c.length, c.validity, c.type);
            break;
        case OutKind::ConstString:
            v = draken_vector_own_string_dict(c.slots, c.arena, c.arena_len, c.codes, 1,
                                              c.validity, c.length, c.type);
            break;
        case OutKind::Array:
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
