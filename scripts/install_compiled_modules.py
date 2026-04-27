#!/usr/bin/env python3
"""Copy Meson-built extension modules to their Python package locations.

Meson builds .so files with long flat module names:
  build/opteryx/opteryx_compiled_structures_bloom_filter.cpython-313-darwin.so
  build/draken/opteryx_compiled_draken_interop_arrow.cpython-313-darwin.so
  build/rugo/opteryx_compiled_rugo_parquet.cpython-313-darwin.so

Python imports expect short names in package directories:
  opteryx/compiled/structures/bloom_filter.cpython-313-darwin.so
  opteryx/compiled/draken/interop/arrow.cpython-313-darwin.so
  opteryx/compiled/rugo/parquet.cpython-313-darwin.so

The module name maps exactly to the package path:
  opteryx_compiled_<part1>_<part2>_..._<name>
    → opteryx/compiled/<part1>/<part2>/.../<name>.cpython-*.so

We disambiguate path vs name components by matching against existing directories
under opteryx/compiled/ and opteryx/compiled/draken/ etc.
"""

import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
BUILD_DIR = REPO_ROOT / "build"
OPTERYX_COMPILED = REPO_ROOT / "opteryx" / "compiled"


def so_suffix(path: Path) -> str:
    """Extract .cpython-NNN-platform.so suffix from filename."""
    m = re.search(r"(\.cpython-\d+-[^.]+\.so)$", path.name)
    return m.group(1) if m else ".so"


def module_name_to_target(long_name: str, so_sfx: str) -> Path | None:
    """Convert a long Meson module name to the target path in the source tree.

    Strategy: strip 'opteryx_compiled_' prefix, then greedily match leading
    underscore-joined parts against known subdirectories under opteryx/compiled/.

    Handles leading underscores in module names (e.g. _decimal_vector):
    The Meson module name uses double underscore to represent a leading underscore:
      opteryx_compiled_draken_vectors__decimal_vector → _decimal_vector in vectors/
    """
    if not long_name.startswith("opteryx_compiled_"):
        return None

    remaining = long_name[len("opteryx_compiled_"):]

    # Walk forward, consuming directory components greedily.
    # We work on the remaining string directly to preserve leading underscores.
    base = OPTERYX_COMPILED
    pos = 0  # current position in remaining string

    while pos < len(remaining):
        # Find the next potential split points (underscore positions)
        # Try to match from pos to each subsequent underscore as a directory name
        found = False

        # Collect all underscore positions after pos
        rest = remaining[pos:]
        splits = [i for i, c in enumerate(rest) if c == "_"]

        # Try longest match first (rightmost split)
        for split_idx in reversed(splits):
            if split_idx == 0:
                # Leading underscore — can't be a directory name boundary here
                continue
            candidate_dir = rest[:split_idx]
            if not candidate_dir:
                continue
            if (base / candidate_dir).is_dir():
                base = base / candidate_dir
                pos += split_idx + 1  # consume dir + separator underscore
                found = True
                break

        if not found:
            break

    # Whatever remains (from pos) is the module name
    module_name = remaining[pos:]
    if not module_name:
        return None

    target = base / (module_name + so_sfx)
    return target


def install_modules(build_subdir: str, verbose: bool = True) -> int:
    """Install all .so files from build/<subdir> to their package locations."""
    src_dir = BUILD_DIR / build_subdir
    if not src_dir.exists():
        print(f"  Build directory not found: {src_dir}", file=sys.stderr)
        return 0

    copied = 0
    skipped = 0
    errors = 0

    for so_file in sorted(src_dir.glob("*.so")):
        sfx = so_suffix(so_file)
        long_name = so_file.name[: -len(sfx)]

        target = module_name_to_target(long_name, sfx)
        if target is None:
            print(f"  SKIP (no mapping): {so_file.name}")
            skipped += 1
            continue

        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(so_file, target)
        if verbose:
            rel_src = so_file.relative_to(REPO_ROOT)
            rel_dst = target.relative_to(REPO_ROOT)
            print(f"  {rel_src} → {rel_dst}")
        copied += 1

    return copied


def verify_imports(sample_modules: list[str]) -> bool:
    """Quick smoke-test: verify a sample of installed modules are importable."""
    ok = True
    for mod in sample_modules:
        try:
            __import__(mod)
        except ImportError as e:
            print(f"  IMPORT FAILED: {mod}: {e}", file=sys.stderr)
            ok = False
    return ok


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Install Meson-built extension modules")
    parser.add_argument("--verify", action="store_true", help="Run import smoke-tests after install")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file output")
    args = parser.parse_args()

    verbose = not args.quiet
    total = 0

    print("Installing Draken modules...")
    total += install_modules("draken", verbose=verbose)

    print("Installing Rugo modules...")
    total += install_modules("rugo", verbose=verbose)

    print("Installing Opteryx modules...")
    total += install_modules("opteryx", verbose=verbose)

    print(f"\nInstalled {total} extension modules.")

    if args.verify:
        print("\nRunning import smoke-tests...")
        smoke = [
            "opteryx.compiled.structures.bloom_filter",
            "opteryx.compiled.draken.vectors.float64_vector",
            "opteryx.compiled.rugo.parquet",
            "opteryx.compiled.io.csv_rows",
            "opteryx.compiled.vector_ops.vector_ops",
        ]
        sys.path.insert(0, str(REPO_ROOT))
        ok = verify_imports(smoke)
        if ok:
            print("All smoke-tests passed.")
        else:
            sys.exit(1)
