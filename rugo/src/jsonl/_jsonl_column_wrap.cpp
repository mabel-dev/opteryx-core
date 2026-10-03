#include "_jsonl_column_wrap.hpp"

// Producer surface (definitions resolved at load via RTLD_GLOBAL from draken_native.so):
#include "vectors/_vector_bridge.h"  // draken_vector_own_string, draken_vector_own_array(_numeric)

namespace rugo::_jsonl {

// Wrap parsed buffers into an owned Draken Vector (creates a Python object — GIL).
PyObject* wrap_column(ParsedColumn& pc) {
    if (pc.type == DRAKEN_ARRAY) {
        // String-family child iff fill_string_array_column populated slots;
        // fill_numeric_array_column never touches array_child_slots.
        if (pc.array_child_slots != nullptr) {
            return draken_vector_own_array(
                pc.array_parent_offsets, pc.array_child_slots, pc.array_child_arena,
                pc.array_child_arena_len, pc.array_child_length, pc.array_child_type,
                pc.array_child_validity, pc.validity, pc.length);
        }
        return draken_vector_own_array_numeric(
            pc.array_parent_offsets, pc.array_child_data, pc.array_child_validity,
            pc.array_child_length, pc.array_child_type, pc.validity, pc.length);
    }
    if (pc.is_string && pc.codes != nullptr)
        return draken_vector_own_string_dict(pc.slots, pc.arena, pc.arena_len, pc.codes,
                                             pc.data_length, pc.validity, pc.length, pc.type);
    if (pc.is_string)
        return draken_vector_own_string(pc.slots, pc.arena, pc.arena_len,
                                        pc.validity, pc.length, pc.type,
                                        /*keyhash=*/nullptr);   // E37: jsonl producer = task #5
    // A declared IPV4/TIMESTAMP/DECIMAL column carries a logical-type descriptor,
    // which lives on the Vector's owner rather than in the frozen DrakenVector —
    // so it has to be attached at construction. own_raw_logical is own_raw when
    // the kind is NONE, which is every inferred column.
    if (pc.logical_kind != 0)
        return draken_vector_own_raw_logical(pc.data, pc.validity, pc.length, pc.type,
                                             pc.logical_kind, pc.unit, pc.offset_minutes,
                                             pc.precision, pc.scale, /*dimension=*/0u);
    return draken_vector_own_raw(pc.data, pc.validity, pc.length, pc.type);
}

}  // namespace rugo::_jsonl
