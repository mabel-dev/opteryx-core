"""
Milestone E.7 POC build script — draken_vector_own_string bridge proof.

Builds poc_e7.so from poc_e7_nanobind.cpp (pure C++, no Cython).

draken_vector_own_string / draken_vector_own_raw / draken_vector_unwrap are compiled
into draken_native.so and resolved at runtime via RTLD_GLOBAL. run_poc_e7.py loads
draken_native first.

Usage (from repo root — draken_native must be built first via make compile):
    python draken/poc/setup_poc_e7.py build_ext --inplace --build-lib draken/poc
"""

import os
import sys

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, REPO_ROOT)

from setuptools import setup, Extension

POC_DIR = os.path.join(REPO_ROOT, "draken", "poc")

include_dirs = [
    REPO_ROOT,
    os.path.join(REPO_ROOT, "draken"),
    os.path.join(REPO_ROOT, "draken", "core"),
    os.path.join(REPO_ROOT, "draken", "ops"),
    os.path.join(REPO_ROOT, "src", "cpp"),
    os.path.join(REPO_ROOT, "third_party", "mimalloc", "include"),
    os.path.join(REPO_ROOT, "third_party", "nanobind"),
    os.path.join(REPO_ROOT, "third_party", "nanobind", "src"),
    os.path.join(REPO_ROOT, "third_party", "nanobind", "ext", "robin_map", "include"),
    os.path.join(REPO_ROOT, "third_party", "cyan4973"),  # xxhash.h via string_slot.h
    POC_DIR,
]

if sys.platform == "darwin":
    link_args = ["-undefined", "dynamic_lookup"]
else:
    link_args = ["-Wl,--allow-shlib-undefined"]

ext = Extension(
    "poc_e7",
    sources=[
        os.path.join(POC_DIR, "poc_e7_nanobind.cpp"),
        os.path.join(REPO_ROOT, "draken", "core", "vector_alloc.cpp"),
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
    name="poc_e7",
    ext_modules=[ext],
)
