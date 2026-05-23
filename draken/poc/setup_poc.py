"""
Standalone build script for the Milestone E.0 binding POC.

Usage (from repo root):
    cd draken/poc
    python setup_poc.py build_ext --inplace

Or from repo root:
    python draken/poc/setup_poc.py build_ext --inplace --build-lib draken/poc

Requires: Cython installed (pip install cython).
Does NOT require: mimalloc.o — the POC uses plain malloc, not draken_malloc.
"""

import os, sys

# Allow running from either repo root or draken/poc/
REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
sys.path.insert(0, REPO_ROOT)

from setuptools import setup, Extension
from Cython.Build import cythonize

include_dirs = [
    REPO_ROOT,                              # for cimport draken.core.buffers
    os.path.join(REPO_ROOT, "draken"),      # for #include "core/buffers.h"
    os.path.join(REPO_ROOT, "draken/core"), # for #include "buffers.h" (quote)
    os.path.join(REPO_ROOT, "draken/ops"),  # for #include "hash.h" etc.
    os.path.join(REPO_ROOT, "src/cpp"),     # for simd_hash.h (pulled in by hash.h)
    os.path.join(REPO_ROOT, "third_party/mimalloc/include"),  # for <mimalloc.h>
]

ext = Extension(
    "binding_poc",
    sources=[
        os.path.join(os.path.dirname(__file__), "binding_poc.pyx"),
    ],
    include_dirs=include_dirs,
    language="c++",
    extra_compile_args=["-std=c++17", "-O2"],
)

setup(
    name="binding_poc",
    ext_modules=cythonize(
        [ext],
        compiler_directives={"language_level": "3"},
        include_path=include_dirs,
    ),
)
