#!/usr/bin/env python3
"""Copy Meson-built extension modules to their Python package locations.

Meson builds .so files with long flat module names:
  build/opteryx/opteryx_compiled_structures_bloom_filter.cpython-313-darwin.so
  build/draken/opteryx_compiled_draken_interop_arrow.cpython-313-darwin.so
  build/rugo/opteryx_compiled_rugo_parquet.cpython-313-darwin.so

Python imports expect short names in package directories:
  opteryx/compiled/structures/bloom_filter.cpython-313-darwin.so
  draken/interop/arrow.cpython-313-darwin.so
  rugo/parquet.cpython-313-darwin.so

Routing:
  opteryx_compiled_<parts>  → opteryx/compiled/<parts>.so  (directory-matched)
  opteryx_compiled_draken_* → draken/<parts>.so             (directory-matched under draken/)
  opteryx_compiled_rugo_*   → rugo/<parts>.so               (directory-matched under rugo/)
"""

import re
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
BUILD_DIR = REPO_ROOT / "build"
OPTERYX_COMPILED = REPO_ROOT / "opteryx" / "compiled"
OPTERYX_ROOT = REPO_ROOT / "opteryx"
DRAKEN_ROOT = REPO_ROOT / "draken"
RUGO_ROOT = REPO_ROOT / "rugo"

# Explicit mapping for modules that don't follow the opteryx_compiled_ prefix convention.
# Keys are the Meson module name (short form, matches PyInit_<name>).
# Values are relative paths within opteryx/ (without .so suffix).
EXTRA_MODULE_MAP: dict[str, str] = {
    # third_party
    "xxhash": "third_party/cyan4973/xxhash",
    "zstd": "third_party/facebook/zstd",
    "fast_float": "third_party/fastfloat/fast_float",
    "fuzzy": "third_party/fuzzy",
    "lz4": "third_party/lz4/lz4",
    "base16": "third_party/mabel/base16",
    "base64": "third_party/mabel/base64",
    "base85": "third_party/mabel/base85",
    "distogram": "third_party/maki_nage/distogram",
    "mbleven": "third_party/mbleven",
    "ryu": "third_party/ulfjack/ryu",
    "cyyjson": "third_party/yyjson/cyyjson",
    # operators
    "_operators": "operators/_operators",
    # connectors
    "pool_reader": "connectors/parquet_io/pool_reader",
    # nanobind extensions
    "disk_reader": "compiled/io/disk_reader",
}


def so_suffix(path: Path) -> str:
    """Extract .cpython-NNN-platform.so suffix from filename."""
    m = re.search(r"(\.cpython-\d+-[^.]+\.so)$", path.name)
    return m.group(1) if m else ".so"


def _match_path_components(remaining: str, base: Path, so_sfx: str) -> Path | None:
    """Greedily match underscore-separated components against directories under base.

    Handles double underscore for leading underscores in module names:
      vectors__decimal_vector → _decimal_vector in vectors/
    """
    pos = 0
    while pos < len(remaining):
        rest = remaining[pos:]
        splits = [i for i, c in enumerate(rest) if c == "_"]
        found = False
        for split_idx in reversed(splits):
            if split_idx == 0:
                continue
            candidate_dir = rest[:split_idx]
            if not candidate_dir:
                continue
            if (base / candidate_dir).is_dir():
                base = base / candidate_dir
                pos += split_idx + 1
                found = True
                break
        if not found:
            break

    module_name = remaining[pos:]
    if not module_name:
        return None
    return base / (module_name + so_sfx)


def module_name_to_target(long_name: str, so_sfx: str) -> Path | None:
    """Convert a long Meson module name to the target path in the source tree."""
    if not long_name.startswith("opteryx_compiled_"):
        return None

    remaining = long_name[len("opteryx_compiled_"):]

    # Route draken modules to draken/ package root
    if remaining.startswith("draken_"):
        remaining = remaining[len("draken_"):]
        return _match_path_components(remaining, DRAKEN_ROOT, so_sfx)

    # Route rugo modules to rugo/ package root
    if remaining.startswith("rugo_"):
        remaining = remaining[len("rugo_"):]
        return _match_path_components(remaining, RUGO_ROOT, so_sfx)

    # All other opteryx_compiled_* modules go under opteryx/compiled/
    return _match_path_components(remaining, OPTERYX_COMPILED, so_sfx)


def extra_module_target(short_name: str, so_sfx: str) -> Path | None:
    """Look up install path for a non-opteryx_compiled_ module."""
    rel = EXTRA_MODULE_MAP.get(short_name)
    if rel is None:
        return None
    return OPTERYX_ROOT / (rel + so_sfx)


def install_modules(build_subdir: str, verbose: bool = True) -> int:
    """Install all .so files from build/<subdir> to their package locations."""
    src_dir = BUILD_DIR / build_subdir
    if not src_dir.exists():
        print(f"  Build directory not found: {src_dir}", file=sys.stderr)
        return 0

    copied = 0
    skipped = 0

    for so_file in sorted(src_dir.glob("*.so")):
        sfx = so_suffix(so_file)
        long_name = so_file.name[: -len(sfx)]

        target = module_name_to_target(long_name, sfx)

        if target is None:
            target = extra_module_target(long_name, sfx)

        if target is None:
            if verbose:
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


def install_rust_module(verbose: bool = True) -> int:
    """Copy the Rust compute extension to opteryx/compute.<suffix>.so."""
    import subprocess
    import sys

    ext_suffix = subprocess.check_output(
        [sys.executable, "-c", "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"],
        text=True,
    ).strip()

    dylib = REPO_ROOT / "target" / "release" / "libcompute.dylib"
    target = OPTERYX_ROOT / ("compute" + ext_suffix)

    if not dylib.exists():
        print(f"  Rust build artifact not found: {dylib}", file=sys.stderr)
        print("  Run 'cargo build --release' first.", file=sys.stderr)
        return 0

    shutil.copy2(dylib, target)
    if verbose:
        print(f"  target/release/libcompute.dylib → {target.relative_to(REPO_ROOT)}")
    return 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Install Meson-built extension modules")
    parser.add_argument("--verify", action="store_true", help="Run import smoke-tests after install")
    parser.add_argument("--quiet", action="store_true", help="Suppress per-file output")
    parser.add_argument("--no-rust", action="store_true", help="Skip Rust extension install")
    args = parser.parse_args()

    verbose = not args.quiet
    total = 0

    print("Installing Draken modules...")
    total += install_modules("draken", verbose=verbose)

    print("Installing Rugo modules...")
    total += install_modules("rugo", verbose=verbose)

    print("Installing Opteryx modules...")
    total += install_modules("opteryx", verbose=verbose)

    if not args.no_rust:
        print("Installing Rust modules...")
        total += install_rust_module(verbose=verbose)

    print(f"\nInstalled {total} extension modules.")

    if args.verify:
        print("\nRunning import smoke-tests...")
        smoke = [
            "opteryx.compiled.structures.bloom_filter",
            "draken.vectors.float64_vector",
            "rugo.parquet",
            "opteryx.compiled.io.csv_rows",
            "opteryx.compiled.vector_ops.vector_ops",
        ]
        sys.path.insert(0, str(REPO_ROOT))
        ok = verify_imports(smoke)
        if ok:
            print("All smoke-tests passed.")
        else:
            sys.exit(1)
