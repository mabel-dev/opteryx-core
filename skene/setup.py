"""
Standalone build for the ``libskene`` wheel — the skene columnar file format
(draken + skene), packaged from this same source tree *without* the Opteryx
SQL engine and *without* rugo, for users who want lossless draken-vector
serialization with zero heavy deps.

PyPI distribution name is ``libskene`` (``skene`` is taken by an unrelated
project); the import package is ``skene`` — matching the C artifact the wheel
wraps (libskene.a).

This file lives in ``skene/`` for orientation, but its build *base* is the
repo root: the wheel bundles ``draken``, a sibling package, and every
extension source path in ``build_common`` is relative to the repo root. So
this script re-roots to the repo top before doing anything — the same
approach, isolation tricks included, as ``rugo/setup.py``.

Run from the repo root:

    python skene/setup.py bdist_wheel
"""

import os
import sys
from pathlib import Path

# skene lives at <repo>/skene; re-root to <repo> so relative source paths in
# build_common resolve and `build_common` itself is importable.
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

# Running `python skene/setup.py` puts THIS script's directory (skene/) on
# sys.path[0], letting anything under skene/ shadow stdlib/tooling imports
# (the exact failure rugo hit with its `csv` subpackage). Drop skene/ from
# sys.path and root at the repo top.
_here = os.path.abspath(os.path.dirname(__file__))
sys.path[:] = [p for p in sys.path if os.path.abspath(p or ".") != _here]
sys.path.insert(0, str(ROOT))

from Cython.Build import cythonize  # noqa: E402
from setuptools import find_packages, setup  # noqa: E402
import setuptools.config.pyprojecttoml as _pyprojecttoml  # noqa: E402

# We build from the repo root so build_common's relative source paths resolve
# and draken (a sibling package) is reachable. The cost: setuptools would
# otherwise read the repo-root pyproject.toml's [project] table (opteryx_core)
# and apply it to THIS distribution. `libskene` is a separate distribution
# defined entirely by this setup.py, so neutralise that foreign pyproject
# application for this build only — the repo-root files are left untouched.
_pyprojecttoml.apply_configuration = (
    lambda dist, filepath, ignore_option_errors=False: dist
)

from build_common import (  # noqa: E402
    FREE_THREADED_BUILD,
    build_ext,
    draken_rugo_extensions,
    skene_extensions,
)

# libskene's own version — single source of truth in skene/__version__.py.
_version_ns = {}
with open(os.path.join(ROOT, "skene", "__version__.py")) as _vf:
    exec(_vf.read(), _version_ns)
__version__ = _version_ns["__version__"]

with open("skene/README.md", "r", encoding="UTF8") as f:
    long_description = f.read()


def _draken_only_extensions():
    """draken's extensions without rugo's.

    build_common's single-source list builds draken and rugo together (they
    ship together in the other two wheels). skene is disjoint from rugo by
    design, so this wheel takes the draken subset — filtered by module name,
    which cannot drift: any new extension is either draken.* (wanted) or not.
    """
    all_extensions = draken_rugo_extensions(
        parquet_created_by="libskene version %s" % __version__
    )
    return [e for e in all_extensions if e.name.startswith("draken.")]


def discover_packages():
    """draken + skene packages only (never opteryx, never rugo).

    Mirrors rugo/setup.py's discover_packages: find_packages only treats a
    directory as a package when it has a literal __init__.py, but some draken
    __init__ are compiled .pyx — so we additionally walk for __init__.pyx so
    pure-Python siblings still ship. Tests are excluded (skene/tests is C++
    and has no __init__, so it is invisible here anyway).
    """
    base = set(
        find_packages(
            include=["draken", "draken.*", "skene", "skene.*"],
            exclude=["draken.tests", "draken.tests.*", "skene.tests", "skene.tests.*"],
        )
    )
    for root_pkg in ("draken", "skene"):
        for dirpath, _dirnames, filenames in os.walk(root_pkg):
            parts = dirpath.split(os.sep)
            if "tests" in parts:
                continue
            if "__init__.pyx" in filenames or "__init__.py" in filenames:
                base.add(".".join(parts))
    return sorted(base)


setup(
    name="libskene",
    version=__version__,
    description=(
        "Skene: a lossless columnar file format for draken vectors — "
        "exact logical types, restored dictionary encodings, value-ordered "
        "columns with exact distinct counts (no PyArrow, no NumPy)."
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Justin Joyce",
    author_email="justin.joyce@joocer.com",
    url="https://github.com/mabel-dev/opteryx-core",
    project_urls={
        "Source": "https://github.com/mabel-dev/opteryx-core/tree/main/skene",
        "Bug Tracker": "https://github.com/mabel-dev/opteryx-core/issues",
        "Format Specification": "https://github.com/mabel-dev/opteryx-core/blob/main/skene/FORMAT.md",
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
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers",
        # Alpha while format v1 is DRAFT (skene/FORMAT.md) — files written by
        # 0.x may need migration once v1 freezes.
        "Development Status :: 3 - Alpha",
    ],
    packages=discover_packages(),
    # Same interpreter range as the rugo wheel (its own distribution — the
    # opteryx_core 3.14-only floor does not apply).
    python_requires=">=3.11",
    ext_modules=cythonize(
        _draken_only_extensions() + skene_extensions(),
        compiler_directives={
            "language_level": "3",
            # Declare modules free-threading-safe under a free-threaded CPython,
            # gated on the build interpreter actually being free-threaded.
            "freethreading_compatible": FREE_THREADED_BUILD,
        },
    ),
    package_data={
        # Standalone mimalloc preload lib built by build_common (see
        # draken.preload_library_path); ships in the wheel, linked into nothing.
        "draken": ["libmimalloc.so", "libmimalloc.dylib"],
    },
    cmdclass={"build_ext": build_ext},
    # Isolate this wheel's build artifacts from the shared ./build dir, which
    # is contaminated with opteryx/rugo .so from `make c` — a dedicated build
    # base packages draken+skene only.
    options={"build": {"build_base": "build/_skene_wheel"}},
    zip_safe=False,
)
