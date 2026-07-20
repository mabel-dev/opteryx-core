"""Build the take A/B microbench extension in-place.

    python dev/take_ab/build.py build_ext --inplace

Uses the same include dirs as the draken build so `core/alloc.h` and the
draken cimports resolve. Run from repo root.
"""

import sys
from setuptools import Extension, setup
from Cython.Build import cythonize

include_dirs = [".", "draken", "draken/core", "src/cpp", "third_party/cyan4973"]

ext = Extension(
    "dev.take_ab.take_ab",
    sources=["dev/take_ab/take_ab.pyx"],
    include_dirs=include_dirs,
    language="c++",
    extra_compile_args=["-std=c++20", "-O3"],
    extra_link_args=(["-undefined", "dynamic_lookup"] if sys.platform == "darwin"
                     else ["-Wl,--allow-shlib-undefined"]),
)

setup(
    name="take_ab",
    ext_modules=cythonize([ext], compiler_directives={"language_level": "3"}),
)
