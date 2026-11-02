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
void register_vector_ann(nb::module_ &m);
void register_vector_json(nb::module_ &m);
void register_vector_sketch_reduce(nb::module_ &m);
void register_disk_reader(nb::module_ &m);  // src/cpp/disk_reader_native.cpp

NB_MODULE(vectors, m) {
    register_vector_ann(m);
    register_vector_json(m);
    register_vector_sketch_reduce(m);
    register_disk_reader(m);
}
