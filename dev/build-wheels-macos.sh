#!/bin/bash
# Build a macOS wheel for the `opteryx_core` distribution.
#
# Runs natively on a macOS runner (no docker, arm64 only — see release.yaml).
# The interpreter is whatever `python` resolves to (set up by actions/setup-python).
# Unlike rugo, opteryx_core needs Rust (opteryx.compute) and libcurl (http_client).
# We use system libcurl via Homebrew + pkg-config rather than the vendored static
# build — that path exists for manylinux containers with no package manager, but
# on a macOS runner `brew install curl pkg-config` is simpler and is the path
# setup.py's own error message documents for macOS.
set -ex

# repo root = parent of this script's dir (dev/). setup.py self-roots here.
cd "$(dirname "$0")/.."

brew install curl pkg-config openssl

# Install Rust (pinned to match the Linux release build for consistency).
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain 1.83.0 -y
export PATH="$HOME/.cargo/bin:$PATH"

python -m pip install -U setuptools wheel cython setuptools-rust delocate

# Confirm the ABI we were handed (must be GIL-enabled — no free-threaded macOS build).
abiflags="$(python -c 'import sys; print(sys.abiflags)')"
echo "macOS build interpreter abiflags='${abiflags}', version=$(python -c 'import sys;print(sys.version)')"
if [[ "$abiflags" == *t* ]]; then
  echo "ABI MISMATCH: macOS release build must be the GIL interpreter, got free-threaded (abiflags='${abiflags}')"
  exit 1
fi

# Force a NATIVE arm64 build. A universal2 CPython otherwise makes setuptools emit
# `-arch arm64 -arch x86_64`, but draken/rugo/opteryx carry arch-specific SIMD
# (NEON on arm) and no x86_64 objects on an arm host, so the x86_64 slice fails to
# link. ARCHFLAGS pins the compile/link to arm64; _PYTHON_HOST_PLATFORM pins the
# wheel tag to macosx_*_arm64 (not universal2) so the tag matches the binary.
export ARCHFLAGS="-arch arm64"
export _PYTHON_HOST_PLATFORM="macosx-${MACOSX_DEPLOYMENT_TARGET:-11.0}-arm64"
export MACOSX_DEPLOYMENT_TARGET="${MACOSX_DEPLOYMENT_TARGET:-11.0}"

NPROC=$(sysctl -n hw.ncpu 2>/dev/null || echo 1)
python setup.py build_ext --parallel "$NPROC" bdist_wheel

mkdir -p dist/delocated
for whl in dist/*.whl; do
  [ -f "$whl" ] || continue
  echo "delocate-wheel: $whl"
  delocate-wheel -w dist/delocated -v "$whl"
done

echo "=== opteryx_core macOS wheels ==="
ls -lh dist/delocated/*.whl || { echo "No macOS wheels produced"; exit 1; }
