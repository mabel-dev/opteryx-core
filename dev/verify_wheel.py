#!/usr/bin/env python3
"""Download and inspect an opteryx wheel for compiled extensions.

Usage examples:
  python dev/verify_wheel.py https://files.pythonhosted.org/.../opteryx_core-0.6.0-...whl
  python dev/verify_wheel.py /path/to/opteryx_core-0.6.0.whl --no-docker

What it does:
  - downloads the wheel (if a URL is provided)
  - lists .so files contained in the wheel
  - searches for opteryx/compiled/vector_ops*.so
  - extracts that .so and runs `file` and (where possible) `ldd`/`readelf` to inspect dynamic deps
  - attempts to grep for the symbol `vector_contains_all` in the symbol table
  - optionally uses a manylinux Docker image to run Linux-native tools for inspection and to try `pip install` + import

This script is intended as a reproducible, easy-to-run diagnostic for the packaging/ABI issue.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent

# No hard-coded default wheel in the repository. Use either:
# - pass the wheel as an argument: python dev/verify_wheel.py <WHEEL_URL_OR_PATH>
# - set the WHEEL_URL env var: WHEEL_URL="https://...whl" python dev/verify_wheel.py
# This keeps the script disposable and avoids committing temporary URLs to git.


def run(cmd, capture=True, check=False, env=None):
    print(f"$ {cmd}")
    try:
        res = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.STDOUT if capture else None,
            env=env,
            check=check,
            text=True,
        )
        out = res.stdout if capture else ""
        if out:
            print(out)
        return res.returncode, out
    except FileNotFoundError as exc:  # pragma: no cover - defensive
        print(f"Command failed: {exc}")
        return 127, ""


def download_wheel(url: str, dest_dir: Path) -> Path:
    dest = dest_dir / Path(url).name
    print(f"Downloading wheel to {dest}")
    # Use curl -f to fail on HTTP error codes, and capture output for diagnostics.
    code, out = run(f"curl -f -L -o {dest} '{url}'")
    if code != 0 or not dest.exists():
        raise RuntimeError(f"Failed to download wheel (curl exit {code}). Check URL: {url}\n{out}")
    # sanity checks
    if dest.stat().st_size == 0:
        raise RuntimeError(f"Downloaded file is empty: {dest}. Check URL: {url}")
    import zipfile

    if not zipfile.is_zipfile(dest):
        raise RuntimeError(f"Downloaded file does not appear to be a valid wheel/zip: {dest}")
    return dest


def vector_so_files(wheel: Path) -> list[str]:
    # Escape the backslash in the regex to avoid SyntaxWarning on Python.
    code, out = run(f"unzip -l {wheel} | awk '/\\.so$/ {{print $4}}'")
    if code != 0:
        raise RuntimeError("Failed to list wheel contents (unzip missing or wheel invalid)")
    return [line.strip() for line in out.splitlines() if line.strip()]


def find_vector_ops_member(so_list: list[str]) -> str | None:
    for p in so_list:
        if "opteryx/compiled" in p and "vector_ops" in p and p.endswith(".so"):
            return p
    return None


def extract_member(wheel: Path, member: str, outpath: Path):
    print(f"Extracting {member} -> {outpath}")
    code, _ = run(f"unzip -p {wheel} '{member}' > {outpath}")
    if code != 0 or not outpath.exists():
        raise RuntimeError("Failed to extract member from wheel")


def inspect_local_binary(binpath: Path) -> dict:
    out = {}
    out['file'] = run(f"file {binpath}")[1]
    # show symbol table if readelf present
    rc, sym = run(f"readelf -Ws {binpath} | head -n 200", capture=True)
    out['readelf_symbols'] = sym if rc == 0 else "readelf not available or failed"
    # try ldd (Linux only)
    rc, lddout = run(f"ldd {binpath}")
    out['ldd'] = lddout if rc == 0 else "ldd not available or failed"
    return out


def inspect_with_docker(host_file: Path) -> dict:
    # Use manylinux2014 image to inspect an ELF .so reliably on macOS
    image = "quay.io/pypa/manylinux2014_x86_64"
    target = f"/work/{host_file.name}"
    cmd = (
        f"docker run --rm -v '{host_file.parent.resolve()}':/work -w /work {image} "
        f"bash -lc \"file {target} && readelf -Ws {target} | head -n 200 && ldd {target} || true\""
    )
    rc, out = run(cmd)
    return {"docker_inspect": out}


def try_import_in_docker(wheel_host_path: Path) -> dict:
    image = "quay.io/pypa/manylinux2014_x86_64"
    wheel_name = wheel_host_path.name
    cmd = (
        f"docker run --rm -v '{wheel_host_path.parent.resolve()}':/work -w /work {image} "
        f"bash -lc \"python -m pip install --upgrade pip setuptools wheel || true && pip install '{wheel_name}' && python - <<'PY'\nimport importlib, traceback, sys\ntry:\n    m = importlib.import_module('opteryx.compiled.vector_ops')\n    if not hasattr(m, 'vector_contains_any'):\n        print('MISSING_SYMBOL vector_contains_any')\n        sys.exit(2)\n    print('IMPORT_OK')\nexcept Exception:\n    print('IMPORT_ERR')\n    traceback.print_exc()\n    sys.exit(1)\nPY\""
    )
    rc, out = run(cmd)
    return {"docker_import": out}


def main():
    parser = argparse.ArgumentParser(description="Verify opteryx wheel compiled extensions")
    parser.add_argument("wheel", nargs="?", default=None, help="Wheel URL or local wheel path (optional; falls back to WHEEL_URL env var if set)")
    parser.add_argument("--no-docker", action="store_true", help="Don't attempt Docker-based checks")
    parser.add_argument("--out-dir", default=None, help="Directory to place artifacts (default: temp)")

    args = parser.parse_args()

    if args.out_dir:
        out_dir = Path(args.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
    else:
        out_dir = Path(tempfile.mkdtemp(prefix="opteryx_whl_"))

    try:
        # Accept wheel from arg or WHEEL_URL env var to keep the script disposable
        wheel_src = args.wheel or os.environ.get("WHEEL_URL")
        if not wheel_src:
            raise SystemExit(
                "No wheel provided. Pass the wheel as an argument or set the WHEEL_URL environment variable."
            )
        wheel_arg = Path(wheel_src)
        if str(wheel_src).startswith("http://") or str(wheel_src).startswith("https://"):
            wheel_path = download_wheel(wheel_src, out_dir)
        elif wheel_arg.exists():
            wheel_path = wheel_arg
        else:
            raise SystemExit("Wheel url or path not found: {wheel_src}")

        print('\n=== Wheel .so listing ===')
        so_list = vector_so_files(wheel_path)
        if not so_list:
            print("No .so files found in wheel")
        else:
            for s in so_list:
                print(s)

        print('\n=== Looking for vector_ops binary ===')
        member = find_vector_ops_member(so_list)
        if not member:
            print("No opteryx/compiled/vector_ops*.so member found in the wheel")
            sys.exit(2)

        print(f"Found member: {member}")
        out_so = out_dir / Path(member).name
        extract_member(wheel_path, member, out_so)

        print('\n=== Local inspection ===')
        local_info = inspect_local_binary(out_so)
        print(local_info['file'])
        if 'readelf not available' in local_info['readelf_symbols']:
            print('readelf not available locally; consider running with Docker to inspect symbols')
        else:
            # quick symbol check
            if 'vector_contains_all' in local_info['readelf_symbols']:
                print('\nSymbol `vector_contains_all` FOUND in readelf output')
            else:
                print('\nSymbol `vector_contains_all` NOT found in local readelf output')

        if not args.no_docker:
            print('\n=== Docker-based inspection (manylinux2014_x86_64) ===')
            if shutil.which('docker') is None:
                print('docker not found in PATH, skipping docker checks')
            else:
                docker_info = inspect_with_docker(out_so)
                print(docker_info['docker_inspect'])

                print('\n=== Attempting import inside manylinux docker ===')
                docker_import = try_import_in_docker(wheel_path)
                print(docker_import['docker_import'])

        print('\n=== Done. Artifacts placed in:')
        print(out_dir)

    finally:
        # don't auto-delete the temp dir so user can inspect artifacts; user can remove when done
        pass


if __name__ == '__main__':
    main()
