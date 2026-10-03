#include "_parquet_column_wrap.hpp"

// Producer surface (definitions resolved at load via RTLD_GLOBAL from draken_native.so —
// rugo/__init__.py imports draken first for exactly this reason):
#include "vectors/_vector_bridge.h"  // draken_vector_own_raw/_string/_decimal/_decimal128

namespace rugo::_parquet {

// Wrap materialized buffers into an owned Draken Vector (creates a Python object — GIL).
PyObject* wrap_parquet_column(MaterializedColumn& mc) {
    switch (mc.type) {
        case DRAKEN_VARCHAR:
        case DRAKEN_VARBINARY:
            return draken_vector_own_string((DrakenStringSlot*)mc.data, mc.arena, mc.arena_len,
                                            mc.validity, mc.length, mc.type);
        // The DECIMAL precision/scale live on the Vector's owner, not in the frozen
        // DrakenVector, so they are attached at construction.
        case DRAKEN_DECIMAL:
            return draken_vector_own_decimal(mc.data, mc.validity, mc.length,
                                             mc.precision, mc.scale);
        case DRAKEN_DECIMAL128:
            return draken_vector_own_decimal128(mc.data, mc.validity, mc.length,
                                                mc.precision, mc.scale);
        default:
            return draken_vector_own_raw(mc.data, mc.validity, mc.length, mc.type);
    }
}

}  // namespace rugo::_parquet
