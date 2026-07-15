// opteryx/compiled/nanobind/_vectors_module.cpp
//
// Single nanobind module entry point for the C′ vector-op kernels.
//
// Each opteryx/compiled/nanobind/vector_*.cpp contributes a
//   void register_<name>(nb::module_ &m)
// function (instead of its own NB_MODULE). This dispatcher owns the one
// NB_MODULE(vectors, ...) and calls every register function so that all
// translation units link into ONE shared object (opteryx.compiled.nanobind.vectors)
// rather than ~21 separate .so files. This eliminates the per-extension
// duplication of vector_alloc.cpp / nb_combined.cpp (the wheel-size bloat).
//
// Adding a new vector op file:
//   1. give it `void register_<name>(nb::module_ &m)` (no NB_MODULE)
//   2. add the source to the `vectors` Extension in setup.py
//   3. declare + call register_<name> below

#include <nanobind/nanobind.h>

namespace nb = nanobind;

// Forward declarations — one per vector_*.cpp translation unit.
void register_vector_accessors(nb::module_ &m);
void register_vector_array_reduce(nb::module_ &m);
void register_vector_bitwise(nb::module_ &m);
void register_vector_bool_ops(nb::module_ &m);
void register_vector_casts(nb::module_ &m);
void register_vector_codec(nb::module_ &m);
void register_vector_hash_codec(nb::module_ &m);
void register_vector_json(nb::module_ &m);
void register_vector_math(nb::module_ &m);
void register_vector_misc(nb::module_ &m);
void register_vector_selection_concat(nb::module_ &m);
void register_vector_sketch_reduce(nb::module_ &m);
void register_vector_special(nb::module_ &m);
void register_vector_split_native(nb::module_ &m);
void register_vector_string_case(nb::module_ &m);
void register_vector_string_misc(nb::module_ &m);
void register_vector_string_misc2(nb::module_ &m);
void register_vector_string_misc3(nb::module_ &m);
void register_vector_string_search(nb::module_ &m);
void register_vector_string_slice(nb::module_ &m);
void register_vector_temporal_arith(nb::module_ &m);
void register_vector_temporal_convert(nb::module_ &m);

NB_MODULE(vectors, m) {
    register_vector_accessors(m);
    register_vector_array_reduce(m);
    register_vector_bitwise(m);
    register_vector_bool_ops(m);
    register_vector_casts(m);
    register_vector_codec(m);
    register_vector_hash_codec(m);
    register_vector_json(m);
    register_vector_math(m);
    register_vector_misc(m);
    register_vector_selection_concat(m);
    register_vector_sketch_reduce(m);
    register_vector_special(m);
    register_vector_split_native(m);
    register_vector_string_case(m);
    register_vector_string_misc(m);
    register_vector_string_misc2(m);
    register_vector_string_misc3(m);
    register_vector_string_search(m);
    register_vector_string_slice(m);
    register_vector_temporal_arith(m);
    register_vector_temporal_convert(m);
}
