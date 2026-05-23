"""
Milestone E.1 POC build script — typed cdef kernel + nanobind Python edge.

Builds poc_e1.so from:
  - poc_e1_kernel.pyx   → Cython compiles to poc_e1_kernel.cpp
  - poc_e1_nanobind.cpp → nanobind Python entry points
  - third_party/nanobind/src/nb_combined.cpp

draken_vector_unwrap / draken_vector_own_raw are in draken_native.so and
resolved at runtime (RTLD_GLOBAL). run_poc_e1.py loads draken_native first.

Usage (from repo root — draken_native must be built first via make compile):
    python draken/poc/setup_poc_e1.py build_ext --inplace --build-lib draken/poc

Or from the poc directory:
    cd draken/poc
    python setup_poc_e1.py build_ext --inplace
"""

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

from setuptools import setup, Extension
from Cython.Build import cythonize

POC_DIR = os.path.join(REPO_ROOT, "draken", "poc")

include_dirs = [
    REPO_ROOT,                                              # cimport draken.core.buffers
    os.path.join(REPO_ROOT, "draken"),                     # "core/buffers.h" etc.
    os.path.join(REPO_ROOT, "draken", "core"),             # "buffers.h" quote-include
    os.path.join(REPO_ROOT, "draken", "ops"),              # "int64_reductions.h"
    os.path.join(REPO_ROOT, "src", "cpp"),                 # simd_hash.h (nanobind dep)
    os.path.join(REPO_ROOT, "third_party", "mimalloc", "include"),  # <mimalloc.h>
    os.path.join(REPO_ROOT, "third_party", "nanobind"),              # <nanobind/nanobind.h>
    os.path.join(REPO_ROOT, "third_party", "nanobind", "src"),       # internal nb headers
    os.path.join(REPO_ROOT, "third_party", "nanobind", "ext", "robin_map", "include"),
    POC_DIR,  # so poc_e1_nanobind.cpp can find Cython-generated poc_e1_kernel.h
]

if sys.platform == "darwin":
    link_args = ["-undefined", "dynamic_lookup"]
else:
    link_args = ["-Wl,--allow-shlib-undefined"]

compiler_directives = {
    "language_level": "3",
    "nonecheck": False,
    "cdivision": True,
    "initializedcheck": False,
    "infer_types": True,
    "wraparound": False,
    "boundscheck": False,
}

# Cythonize the kernel .pyx to a .cpp, generating poc_e1_kernel.h alongside it.
kernel_ext_list = cythonize(
    [Extension(
        "poc_e1_kernel_cython_stub",   # stub name; not a real Python module
        sources=[os.path.join(POC_DIR, "poc_e1_kernel.pyx")],
        include_dirs=include_dirs,
        language="c++",
    )],
    compiler_directives=compiler_directives,
    include_path=[REPO_ROOT],
)
# Grab the generated .cpp path from the cythonized stub.
kernel_cpp = kernel_ext_list[0].sources[0]

ext = Extension(
    "poc_e1",
    sources=[
        kernel_cpp,
        os.path.join(POC_DIR, "poc_e1_nanobind.cpp"),
        os.path.join(REPO_ROOT, "third_party", "nanobind", "src", "nb_combined.cpp"),
    ],
    include_dirs=include_dirs,
    language="c++",
    extra_compile_args=[
        "-std=c++17", "-O2", "-fno-strict-aliasing",
        "-DNB_COMPACT_ASSERTIONS",
    ],
    extra_link_args=link_args,
)

setup(
    name="poc_e1",
    ext_modules=[ext],
)
