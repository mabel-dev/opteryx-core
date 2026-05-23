// Milestone A.3 smoke target: proves mimalloc links into the new draken
// extensions and is the live allocator on the dev (ARM) platform. It exercises
// the draken_malloc / draken_aligned_malloc / draken_free surface from alloc.h,
// touches the memory so a non-functional allocator would crash, checks the
// requested alignment, and reports mi_version(). A Python extension module (not
// a compile-only TU like _abi_guard) so it can actually RUN: build, then
//   python -c "import draken.core._mimalloc_smoke as m; print(m.run())"
#include <Python.h>

#include <cstring>

#include "core/alloc.h"

static PyObject* smoke_run(PyObject* /*self*/, PyObject* /*args*/) {
    // Plain allocation: write a pattern across the block, read it back.
    const size_t n = 4096;
    unsigned char* p = static_cast<unsigned char*>(draken_malloc(n));
    if (p == nullptr) {
        PyErr_SetString(PyExc_MemoryError, "draken_malloc returned NULL");
        return nullptr;
    }
    std::memset(p, 0xAB, n);
    if (p[0] != 0xAB || p[n - 1] != 0xAB) {
        draken_free(p);
        PyErr_SetString(PyExc_RuntimeError, "draken_malloc block not writable");
        return nullptr;
    }
    draken_free(p);

    // Aligned allocation: verify the pointer honours the requested alignment.
    const size_t align = 64;
    void* a = draken_aligned_malloc(n, align);
    if (a == nullptr) {
        PyErr_SetString(PyExc_MemoryError, "draken_aligned_malloc returned NULL");
        return nullptr;
    }
    if ((reinterpret_cast<uintptr_t>(a) % align) != 0) {
        draken_free(a);
        PyErr_SetString(PyExc_RuntimeError, "draken_aligned_malloc not aligned");
        return nullptr;
    }
    std::memset(a, 0xCD, n);
    draken_free(a);

    // Report the live mimalloc version (e.g. 332 for v3.3.2) so the caller can
    // confirm the vendored allocator, not a system shim, is in play.
    return Py_BuildValue("{s:i,s:i}", "mi_version", mi_version(), "ok", 1);
}

static PyMethodDef smoke_methods[] = {
    {"run", smoke_run, METH_NOARGS,
     "Exercise draken_malloc/draken_aligned_malloc/draken_free; return mimalloc version."},
    {nullptr, nullptr, 0, nullptr},
};

static PyModuleDef smoke_module = {
    PyModuleDef_HEAD_INIT, "_mimalloc_smoke",
    "Milestone A.3 mimalloc link/liveness smoke test.", -1, smoke_methods,
    nullptr, nullptr, nullptr, nullptr,
};

PyMODINIT_FUNC PyInit__mimalloc_smoke(void) { return PyModule_Create(&smoke_module); }
