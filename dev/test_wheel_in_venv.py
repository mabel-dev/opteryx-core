#!/usr/bin/env python3
"""Create a venv, install a wheel, and test importing opteryx.compiled.list_ops.

Usage examples:
  WHEEL_URL="/path/or/url/to/opteryx_core.whl" python dev/test_wheel_in_venv.py
  python dev/test_wheel_in_venv.py /path/to/opteryx_core.whl --no-clean

What it does:
  - create a temporary venv
  - pip install the wheel
  - attempt to import opteryx.compiled.list_ops and check for `list_contains_all`
  - locate the installed .so file and run `file`, `nm`, and `ldd`/`readelf` where available

This is intended for quick reproduction on a developer machine or CI (Linux preferred).
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import tempfile
from pathlib import Path


def run(cmd, capture=True, check=False, env=None):
    print(f"$ {cmd}")
    res = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.STDOUT if capture else None,
        env=env,
        text=True,
    )
    out = res.stdout if capture else None
    if out:
        print(out)
    if check and res.returncode != 0:
        raise SystemExit(f"Command failed ({res.returncode}): {cmd}\n{out}")
    return res.returncode, out


def create_venv(path: Path):
    run(f"python -m venv '{path}'", check=True)


def pip_install(venv_python: Path, wheel: str):
    run(f"'{venv_python}' -m pip install --upgrade pip setuptools wheel", check=True)
    if wheel.startswith("http://") or wheel.startswith("https://"):
        run(f"'{venv_python}' -m pip install '{wheel}'", check=True)
    else:
        run(f"'{venv_python}' -m pip install '{wheel}'", check=True)


def run_import_test(venv_python: Path):
    py = str(venv_python)
    code = (
        "import importlib, traceback, sys, platform, os\n"
        "print('PY', sys.version)\n"
        "print('PLATFORM', platform.platform(), platform.machine())\n"
        "try:\n"
        "    m = importlib.import_module('opteryx.compiled.list_ops')\n"
        "    print('IMPORT_OK', hasattr(m, 'list_contains_all'))\n"
        "except Exception as e:\n"
        "    print('IMPORT_ERR')\n"
        "    traceback.print_exc()\n"
    )
    return run(f"'{py}' - <<'PY'\n{code}\nPY", check=False)


def find_installed_wheel_files(venv_python: Path) -> list[Path]:
    # Use small helper to print opteryx package path
    rc, out = run(f"'{venv_python}' -c \"import opteryx, sys, os; print(opteryx.__file__)\"", check=False)
    if rc != 0 or not out:
        return []
    pkg_file = out.strip().splitlines()[-1]
    pkg_dir = Path(pkg_file).resolve().parent
    files = list(pkg_dir.rglob('*.so'))
    return files


def inspect_binary(binpath: Path):
    print('\nBinary inspection for:', binpath)
    run(f"file '{binpath}' || true")
    run(f"nm '{binpath}' | grep -n 'list_contains_all' || true")
    # Try readelf / ldd where available
    run(f"readelf -Ws '{binpath}' | head -n 200 || true")
    run(f"ldd '{binpath}' || true")


# Hard-code the wheel URL for quick, disposable testing. Replace with the exact wheel if necessary.
DEFAULT_WHEEL = "https://files.pythonhosted.org/packages/01/43/d6ede06774e774d02953ca021ad7381a94f71cc4655d868d608a95bbb72b/opteryx_core-0.6.0-cp313-cp313-manylinux2014_x86_64.manylinux_2_17_x86_64.whl"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", nargs="?", default=os.environ.get("WHEEL_URL", DEFAULT_WHEEL), help="Wheel URL or path (or set WHEEL_URL env var)")
    parser.add_argument("--keep", dest="clean", action="store_false", help="Don't clean up the temporary venv (keeps it for inspection)")
    parser.add_argument("--venv-dir", default=None, help="Create venv at this path instead of a temp dir")
    args = parser.parse_args()

    if not args.wheel or "..." in str(args.wheel):
        raise SystemExit("No valid wheel provided. Edit DEFAULT_WHEEL in dev/test_wheel_in_venv.py or pass a wheel as an argument")

    tmpdir = Path(args.venv_dir) if args.venv_dir else Path(tempfile.mkdtemp(prefix="opteryx_venv_"))
    venv_dir = tmpdir / "venv"

    try:
        print('Creating venv at', venv_dir)
        create_venv(venv_dir)
        venv_python = venv_dir / ("Scripts" if os.name == 'nt' else "bin") / "python"

        print('\nInstalling wheel:', args.wheel)
        pip_install(venv_python, args.wheel)

        print('\n=== Import test ===')
        run_import_test(venv_python)

        so_files = find_installed_wheel_files(venv_python)
        if not so_files:
            print('\nNo .so files found in installed package; package may not have installed compiled extensions')
        else:
            for s in so_files:
                inspect_binary(s)

        print('\nDone. venv located at:', tmpdir)
        if not args.clean:
            print('Not removing venv for inspection')
    finally:
        if args.clean:
            print('Cleaning up', tmpdir)
            shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == '__main__':
    main()
