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

# Resolve /opt/python interpreter directories robustly for variants like cp314T, cp314t or cp314
py_tag="${PYTHON_VERSION//./}"
PYBIN_CANDIDATES=()

# If the tag ends with a T/t (free-threaded), try with and without the suffix and both cases
if [[ "$py_tag" =~ ^([0-9]+)([Tt])$ ]]; then
  base="${BASH_REMATCH[1]}"
  PYBIN_CANDIDATES+=("/opt/python/cp${py_tag}-cp${py_tag}/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}-cp${base}/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}T-cp${base}T/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}t-cp${base}t/bin")
else
  PYBIN_CANDIDATES+=("/opt/python/cp${py_tag}-cp${py_tag}/bin")
fi

# Pick the first candidate that exists
PYBIN=""
for c in "${PYBIN_CANDIDATES[@]}"; do
  if [ -x "${c}/python" ]; then
    PYBIN="$c"
    break
  fi
done

if [ -z "$PYBIN" ]; then
  echo "No matching /opt/python interpreter found for PYTHON_VERSION=${PYTHON_VERSION}"
  echo "Tried these candidates:"
  for c in "${PYBIN_CANDIDATES[@]}"; do echo "  - $c"; done
  echo "Available /opt/python entries:"; ls -1 /opt/python || true
  exit 1
fi

"${PYBIN}/python" -m pip install -U setuptools wheel setuptools-rust numpy cython auditwheel

NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
"${PYBIN}/python" setup.py build_ext --parallel "$NPROC" bdist_wheel

# Repair wheels with auditwheel, preserving the 't' suffix for free-threaded builds
for whl in dist/*.whl; do
    # Check if this is a free-threaded build (cp*-cp*t or cp*t-cp*t pattern)
    if [[ "$whl" =~ cp[0-9]+-cp[0-9]+t ]] || [[ "$whl" =~ cp[0-9]+t-cp[0-9]+t ]]; then
        echo "Free-threaded wheel detected: $whl"
        
        auditwheel repair "$whl" -w dist/
        
        # Rename the repaired wheel to restore the 't' suffix in ABI tag
        # PyArrow format: cp314-cp314t (no 't' on first, 't' on second)
        repaired=$(ls -t dist/*manylinux*.whl | head -n1)
        if [ -f "$repaired" ]; then
            # Restore 't' suffix: cp314-cp314 -> cp314-cp314t
            restored=$(echo "$repaired" | sed -E 's/-cp([0-9]+)-cp([0-9]+)-/-cp\1-cp\2t-/')
            if [ "$repaired" != "$restored" ]; then
                mv -v "$repaired" "$restored"
                echo "Restored free-threaded ABI tag: $restored"
            fi
        fi
    else
        echo "Standard build wheel: $whl"
        auditwheel repair "$whl" -w dist/
    fi
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