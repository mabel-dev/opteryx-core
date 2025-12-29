#!/bin/bash
set -ex

# Install OpenSSL development headers inside the container
# Note: zstd/snappy are vendored into the project; we should not install
# zstd-devel/snappy-devel via yum inside the manylinux container (they may
# not be available on the base image and we compile vendor sources directly).
yum install -y openssl-devel

# Install Rust 1.83.0 (pinned version to avoid GLIBC_2.18 symbols from newer compilers)
curl https://sh.rustup.rs -sSf | sh -s -- --default-toolchain 1.83.0 -y
export PATH="$HOME/.cargo/bin:$PATH"

cd $GITHUB_WORKSPACE/io
cd io

PYBIN="/opt/python/cp${PYTHON_VERSION//.}-cp${PYTHON_VERSION//.}/bin"

"${PYBIN}/python" -m pip install -U setuptools wheel setuptools-rust numpy cython auditwheel

NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
"${PYBIN}/python" setup.py build_ext --parallel "$NPROC" bdist_wheel

for whl in dist/*.whl; do
    auditwheel repair "$whl" -w dist/
done

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
            # Look for specific blacklisted symbol names
            if command -v readelf >/dev/null 2>&1; then
                readelf -Ws "$so" | egrep -i "$BLACKLIST_RE|@@GLIBC_2.18|GLIBC_2.18" >> "$out" || true
                readelf -V "$so" | egrep -i "GLIBC_2.18" >> "$out" || true
            fi
            # Check with objdump for versioned symbol names (e.g., symbol@@GLIBC_2.18)
            if command -v objdump >/dev/null 2>&1; then
                objdump -T "$so" | egrep -i "$BLACKLIST_RE|GLIBC_2.18|@@GLIBC_2.18" >> "$out" || true
            fi
            # Fallbacks
            if command -v nm >/dev/null 2>&1; then
                nm -D "$so" | egrep -i "$BLACKLIST_RE|GLIBC_2.18" >> "$out" || true
            fi
            # Last resort: search strings for the GLIBC version tag
            if command -v strings >/dev/null 2>&1; then
                strings "$so" | egrep -i "GLIBC_2.18|__cxa_thread_atexit_impl|__issignaling|pthread_getattr_default_np|pthread_setattr_default_np" >> "$out" || true
            fi
        done
        # keep a copy of the (original) wheel for debugging
        cp -v "$whl" "dist/diagnostics/${base}"
        rm -rf "$tmpdir"
    ) &
done
wait

# Show what wheels we have after repair attempts
echo "=== Wheels after auditwheel repair ==="
ls -lh dist/*.whl || echo "No wheels found in dist/"
echo "=== Manylinux wheels ==="
ls -lh dist/*manylinux*.whl || echo "No manylinux wheels found"
echo "=== Auditwheel failures ==="
cat dist/diagnostics/auditwheel_failures.txt 2>/dev/null || echo "No auditwheel_failures.txt found"