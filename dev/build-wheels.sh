#!/bin/bash
set -ex

# Install OpenSSL development headers inside the container
# Note: zstd/snappy are vendored into the project; we should not install
# zstd-devel/snappy-devel via yum inside the manylinux container (they may
# not be available on the base image and we compile vendor sources directly).
yum install -y openssl-devel

# Install Rust (required for building some Python packages with Rust extensions)
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain stable -y
export PATH="$HOME/.cargo/bin:$PATH"

cd $GITHUB_WORKSPACE/io
cd io

# Only build for the specified Python version
PYBIN="/opt/python/cp${PYTHON_VERSION//.}-cp${PYTHON_VERSION//.}/bin"

# Install necessary packages
"${PYBIN}/python" -m pip install -U setuptools wheel setuptools-rust numpy cython auditwheel=="6.4.2"

# Build the wheel (parallelize C/C++ extension compilation)
# Detect number of CPUs available inside the container
NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
echo "Using $NPROC parallel jobs for build_ext"
"${PYBIN}/python" setup.py build_ext --parallel "$NPROC" bdist_wheel

# Repair the wheel using auditwheel in parallel (capture failures and continue so we can collect diagnostics)
mkdir -p dist/diagnostics
BLACKLIST_RE='__cxa_thread_atexit_impl|__issignaling|pthread_getattr_default_np|pthread_setattr_default_np'
max_jobs="$NPROC"
for whl in dist/*.whl; do
    # limit background jobs to number of CPUs
    while [ "$(jobs -p | wc -l)" -ge "$max_jobs" ]; do
        sleep 1
    done
    (
        if ! auditwheel repair "$whl" -w dist/; then
            echo "FAILED_REPAIR: $whl" >> dist/diagnostics/auditwheel_failures.txt
        fi
    ) &
done
wait

# Generate symbol diagnostics for each wheel in parallel
for whl in dist/*.whl; do
    while [ "$(jobs -p | wc -l)" -ge "$max_jobs" ]; do
        sleep 1
    done
    (
        base="$(basename "$whl")"
        out="dist/diagnostics/${base}.symbols.txt"
        echo "Inspecting $whl" > "$out"
        tmpdir=$(mktemp -d)
        unzip -q "$whl" -d "$tmpdir"
        find "$tmpdir" -name '*.so' -print0 | while IFS= read -r -d '' so; do
            echo "=== $so ===" >> "$out"
            if command -v readelf >/dev/null 2>&1; then
                readelf -Ws "$so" | egrep -i "$BLACKLIST_RE" >> "$out" || true
            elif command -v objdump >/dev/null 2>&1; then
                objdump -T "$so" | egrep -i "$BLACKLIST_RE" >> "$out" || true
            elif command -v nm >/dev/null 2>&1; then
                nm -D "$so" | egrep -i "$BLACKLIST_RE" >> "$out" || true
            else
                echo "No symbol tool available" >> "$out"
            fi
        done
        # keep a copy of the (original) wheel for debugging
        cp -v "$whl" "dist/diagnostics/${base}"
        rm -rf "$tmpdir"
    ) &
done
wait