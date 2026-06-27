#!/bin/bash
# Build a macOS wheel for the standalone `rugo` distribution.
#
# Runs natively on a macOS runner (no docker, arm64 only — see release-rugo.yaml).
# The interpreter is whatever `python` resolves to (set up by actions/setup-python,
# including free-threaded 3.14t). delocate-wheel bundles any non-system dylibs, the
# macOS analog of auditwheel repair. rugo needs no openssl on macOS
# (parquet_link_args is empty on mac) and no Rust.
set -ex

# repo root = parent of this script's dir (dev/). rugo/setup.py self-roots here.
cd "$(dirname "$0")/.."

python -m pip install -U setuptools wheel cython delocate

# Confirm the ABI we were handed (free-threaded request must be free-threaded).
abiflags="$(python -c 'import sys; print(sys.abiflags)')"
echo "macOS build interpreter abiflags='${abiflags}', version=$(python -c 'import sys;print(sys.version)')"

# Force a NATIVE arm64 build. A universal2 CPython otherwise makes setuptools emit
# `-arch arm64 -arch x86_64`, but draken/rugo carry arch-specific SIMD (NEON on
# arm) and no x86_64 objects on an arm host, so the x86_64 slice fails to link.
# ARCHFLAGS pins the compile/link to arm64; _PYTHON_HOST_PLATFORM pins the wheel
# tag to macosx_*_arm64 (not universal2) so the tag matches the single-arch binary.
export ARCHFLAGS="-arch arm64"
export _PYTHON_HOST_PLATFORM="macosx-${MACOSX_DEPLOYMENT_TARGET:-11.0}-arm64"
export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-11.0}"

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
