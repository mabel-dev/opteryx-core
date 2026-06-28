"""
Standalone build for the ``rugo`` wheel — the PyArrow/NumPy-free file engine
(draken + rugo), packaged from this same source tree *without* the Opteryx SQL
engine, for users who want fast Parquet/CSV/JSONL I/O with zero heavy deps.

This file lives in ``rugo/`` for orientation (people look for rugo's build here),
but its build *base* is the repo root: the wheel bundles ``draken``, a sibling
package, and every extension source path in ``build_common`` is relative to the
repo root. So this script re-roots to the repo top before doing anything.

The shared build machinery and the single-source draken/rugo extension
definitions live in ``build_common.py`` at the repo root — imported, never
duplicated, so this wheel and ``opteryx_core`` can never drift apart.

Run from the repo root:

    python rugo/setup.py bdist_wheel
"""

import os
import sys
from pathlib import Path

# rugo lives at <repo>/rugo; re-root to <repo> so relative source paths in
# build_common resolve and `build_common` itself is importable.
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

# Running `python rugo/setup.py` puts THIS script's directory (rugo/) on
# sys.path[0]. That makes rugo's subpackages shadow stdlib modules — e.g. the
# wheel tooling's `import csv` would resolve to rugo/csv and try to load
# rugo.csv._csv_reader before draken_native is available (undefined
# draken_vector_own_raw). Drop rugo/ from sys.path and root at the repo top.
_here = os.path.abspath(os.path.dirname(__file__))
sys.path[:] = [p for p in sys.path if os.path.abspath(p or ".") != _here]
sys.path.insert(0, str(ROOT))

from Cython.Build import cythonize  # noqa: E402
from setuptools import find_packages, setup  # noqa: E402
import setuptools.config.pyprojecttoml as _pyprojecttoml  # noqa: E402

# We build from the repo root so build_common's relative source paths resolve and
# draken (a sibling package) is reachable. The cost: setuptools would otherwise
# read the repo-root pyproject.toml's [project] table (opteryx_core) and apply it
# to THIS distribution — overriding our name/version and auto-discovering
# opteryx's packages. `rugo` is a separate distribution defined entirely by this
# setup.py, so neutralise that foreign pyproject application for this build only.
# opteryx_core's own build (`python setup.py`) is a separate process, unaffected,
# and the repo-root pyproject.toml is left completely untouched.
_pyprojecttoml.apply_configuration = (
    lambda dist, filepath, ignore_option_errors=False: dist
)

from build_common import (  # noqa: E402
    FREE_THREADED_BUILD,
    build_ext,
    draken_rugo_extensions,
)

# rugo's own version — single source of truth in rugo/__version__.py.
_version_ns = {}
with open(os.path.join(ROOT, "rugo", "__version__.py")) as _vf:
    exec(_vf.read(), _version_ns)
__version__ = _version_ns["__version__"]

with open("rugo/README.md", "r", encoding="UTF8") as f:
    long_description = f.read()


def discover_packages():
    """draken + rugo packages only (never opteryx).

    Mirrors the root setup.py's discover_packages: find_packages only treats a
    directory as a package when it has a literal __init__.py, but many __init__
    are compiled .pyx — so we additionally walk for __init__.pyx so pure-Python
    siblings still ship. Tests are excluded.
    """
    base = set(
        find_packages(
            include=["draken", "draken.*", "rugo", "rugo.*"],
            exclude=["draken.tests", "draken.tests.*", "rugo.tests", "rugo.tests.*"],
        )
    )
    for root_pkg in ("draken", "rugo"):
        for dirpath, _dirnames, filenames in os.walk(root_pkg):
            parts = dirpath.split(os.sep)
            if "tests" in parts:
                continue
            if "__init__.pyx" in filenames or "__init__.py" in filenames:
                base.add(".".join(parts))
    return sorted(base)


setup(
    name="rugo",
    version=__version__,
    description=(
        "Fast, dependency-free Parquet/CSV/JSONL reader and writer "
        "(no PyArrow, no NumPy)."
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Justin Joyce",
    author_email="justin.joyce@joocer.com",
    url="https://github.com/mabel-dev/opteryx-core",
    project_urls={
        "Source": "https://github.com/mabel-dev/opteryx-core/tree/main/rugo",
        "Bug Tracker": "https://github.com/mabel-dev/opteryx-core/issues",
        "Documentation": "https://github.com/mabel-dev/opteryx-core/blob/main/rugo/README.md",
    },
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Programming Language :: Cython",
        "Programming Language :: C++",
        "Topic :: Database",
        "Topic :: File Formats",
        "Topic :: Scientific/Engineering",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Development Status :: 4 - Beta",
    ],
    packages=discover_packages(),
    # rugo targets a broader range than opteryx_core (which is 3.14-only): GIL
    # 3.11–3.14 plus free-threaded 3.13t/3.14t. opteryx's >=3.13 floor does not
    # apply here — rugo is its own distribution.
    python_requires=">=3.11",
    ext_modules=cythonize(
        draken_rugo_extensions(parquet_created_by="rugo version %s" % __version__),
        compiler_directives={
            "language_level": "3",
            # Declare modules free-threading-safe under a free-threaded CPython,
            # gated on the build interpreter actually being free-threaded.
            "freethreading_compatible": FREE_THREADED_BUILD,
        },
    ),
    package_data={},
    cmdclass={"build_ext": build_ext},
    # Isolate rugo's build artifacts from opteryx_core's shared ./build dir.
    # bdist_wheel archives <build_base>/lib/*, and the shared build/lib is
    # contaminated with opteryx .so from `make c` — which would otherwise be
    # swept into the rugo wheel. A dedicated build base packages draken+rugo only.
    options={"build": {"build_base": "build/_rugo_wheel"}},
    zip_safe=False,
)
