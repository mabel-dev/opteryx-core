"""Vendor a header-only slice of boost::math into third_party/boost_math/.

Usage:
    python dev/vendor_boost_math.py [--tag boost-1.86.0]

Downloads the minimal set of modular boost repos needed for:
    #include <boost/math/special_functions/round.hpp>   // half-to-even rounding

Each module lives at https://github.com/boostorg/{module} and is pinned to the
same release tag (default: boost-1.86.0).  Only the include/ directory from each
module is extracted and merged into third_party/boost_math/.

The result is a header-only vendored slice — no compiled .so is produced or linked.
Include path to add to setup.py: "third_party/boost_math"

Transitive dependency set for boost::math::round (determined empirically):
    math            — the core library
    config          — boost/config.hpp, boost/cstdint.hpp
    core            — boost/core/*.hpp
    assert          — boost/assert.hpp
    throw_exception — boost/throw_exception.hpp
    type_traits     — boost/type_traits/*.hpp
    static_assert   — boost/static_assert.hpp
    preprocessor    — boost/preprocessor/*.hpp
    integer         — boost/integer.hpp, boost/integer/...
    predef          — boost/predef.h, boost/predef/...
    mp11            — boost/mp11/*.hpp (needed by newer type_traits)
    container_hash  — boost/container_hash/hash.hpp (pulled by some core paths)
    describe        — boost/describe/*.hpp (pulled by container_hash)
    variant2        — boost/variant2/*.hpp (used by math error paths)

Note: running this script is not automatic in CI.  The vendored headers are
checked into the repo for reproducible builds.  Run this script when you need
to update the pinned version.
"""

import argparse
import hashlib
import os
import shutil
import tarfile
from urllib.request import urlopen

BOOST_TAG = "boost-1.86.0"

MODULES = [
    "math",
    "config",
    "core",
    "assert",
    "throw_exception",
    "type_traits",
    "static_assert",
    "preprocessor",
    "integer",
    "predef",
    "mp11",
    "container_hash",
    "describe",
    "variant2",
]

GITHUB_ARCHIVE = "https://github.com/boostorg/{module}/archive/refs/tags/{tag}.tar.gz"


def download_module(module: str, tag: str, dest_include: str, tmp_dir: str) -> None:
    url = GITHUB_ARCHIVE.format(module=module, tag=tag)
    tmp_tar = os.path.join(tmp_dir, f"{module}.tar.gz")

    print(f"  Downloading {module} @ {tag} ...", end=" ", flush=True)
    with urlopen(url) as r, open(tmp_tar, "wb") as f:
        shutil.copyfileobj(r, f)
    print("done")

    extract_dir = os.path.join(tmp_dir, f"{module}_extract")
    os.makedirs(extract_dir, exist_ok=True)
    with tarfile.open(tmp_tar, "r:gz") as tar:
        tar.extractall(path=extract_dir)

    # Extracted as {module}-{tag}/include/  (tarball root changes dashes)
    # Glob for the include/ subdirectory
    for entry in os.listdir(extract_dir):
        module_root = os.path.join(extract_dir, entry)
        inc = os.path.join(module_root, "include")
        if os.path.isdir(inc):
            # Merge include/ into dest_include
            for root, dirs, files in os.walk(inc):
                rel = os.path.relpath(root, inc)
                target_dir = os.path.join(dest_include, rel)
                os.makedirs(target_dir, exist_ok=True)
                for fname in files:
                    src_file = os.path.join(root, fname)
                    dst_file = os.path.join(target_dir, fname)
                    if not os.path.exists(dst_file):
                        shutil.copy2(src_file, dst_file)
            return

    print(f"  WARNING: no include/ directory found for module {module}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", default=BOOST_TAG, help="Boost release tag (e.g. boost-1.86.0)")
    parser.add_argument("--dest", default="third_party/boost_math", help="Destination directory")
    args = parser.parse_args()

    dest = args.dest
    dest_include = dest  # headers land directly under dest/ (i.e. dest/boost/math/...)

    print(f"Vendoring boost::math slice @ {args.tag} into {dest}/")
    print(f"Modules: {', '.join(MODULES)}")
    print()

    if os.path.exists(dest):
        print(f"Removing existing {dest}/ ...")
        shutil.rmtree(dest)
    os.makedirs(dest_include, exist_ok=True)

    tmp_dir = "/tmp/boost_math_vendor"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir)

    for module in MODULES:
        download_module(module, args.tag, dest_include, tmp_dir)

    shutil.rmtree(tmp_dir)

    # Write a VERSION file for traceability
    version_file = os.path.join(dest, "BOOST_VERSION.txt")
    with open(version_file, "w") as f:
        f.write(f"boost-tag: {args.tag}\n")
        f.write(f"modules: {', '.join(MODULES)}\n")
        f.write("source: https://github.com/boostorg/{module}/archive/refs/tags/{tag}.tar.gz\n")
        f.write("purpose: boost::math::round (half-to-even) for draken/ops/float_math.h\n")
        f.write("include-path: third_party/boost_math\n")

    print()
    print(f"Done. Headers in {dest}/boost/")
    print(f"Version record: {version_file}")
    print()
    print("Smoke-test compilation:")
    print("  #include <boost/math/special_functions/round.hpp>")
    print("  static_assert(boost::math::round(0.5) == 0.0, \"half-to-even\");")


if __name__ == "__main__":
    main()
