#include "_csv_column_wrap.hpp"

// Producer surface (definitions resolved at load via RTLD_GLOBAL from draken_native.so):
#include "vectors/_vector_bridge.h"  // draken_vector_own_string, draken_vector_own_raw(_logical)

namespace rugo::_csv {

// Wrap parsed buffers into an owned Draken Vector (creates a Python object — GIL).
PyObject* wrap_csv_column(ParsedCsvColumn& pc) {
    if (pc.is_string)
        return draken_vector_own_string(
            pc.slots, pc.arena, pc.arena_len,
            pc.validity, pc.length, pc.type,
            /*keyhash=*/nullptr);   // E37: csv producer = task #5
    // A declared IPV4/TIMESTAMP/DECIMAL column carries a logical-type descriptor,
    // which lives on the Vector's owner rather than in the frozen DrakenVector, so
    // it must be attached at construction. own_raw_logical is own_raw when the
    // kind is NONE — every sniffed column.
    if (pc.logical_kind != 0)
        return draken_vector_own_raw_logical(pc.data, pc.validity, pc.length, pc.type,
                                             pc.logical_kind, pc.unit, pc.offset_minutes,
                                             pc.precision, pc.scale, /*dimension=*/0u);
    return draken_vector_own_raw(pc.data, pc.validity, pc.length, pc.type);
}

}  // namespace rugo::_csv
