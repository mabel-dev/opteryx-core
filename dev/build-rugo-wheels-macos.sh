#!/bin/bash
# Build a macOS wheel for the standalone `rugo` distribution.
#
# Runs natively on a macOS runner (no docker). The interpreter is whatever
# `python` resolves to (set up by actions/setup-python, including free-threaded
# 3.13t/3.14t). delocate-wheel bundles any non-system dylibs, the macOS analog
# of auditwheel repair. rugo needs no openssl on macOS (parquet_link_args is
# empty on mac) and no Rust.
set -ex

# repo root = parent of this script's dir (dev/). rugo/setup.py self-roots here.
cd "$(dirname "$0")/.."

python -m pip install -U setuptools wheel cython delocate

# Confirm the ABI we were handed (free-threaded request must be free-threaded).
abiflags="$(python -c 'import sys; print(sys.abiflags)')"
echo "macOS build interpreter abiflags='${abiflags}', version=$(python -c 'import sys;print(sys.version)')"

NPROC=$(sysctl -n hw.ncpu 2>/dev/null || echo 1)
python rugo/setup.py build_ext --parallel "$NPROC" bdist_wheel

mkdir -p dist/delocated
for whl in dist/*.whl; do
  [ -f "$whl" ] || continue
  echo "delocate-wheel: $whl"
  delocate-wheel -w dist/delocated -v "$whl"
done

echo "=== rugo macOS wheels ==="
ls -lh dist/delocated/*.whl || { echo "No macOS wheels produced"; exit 1; }
