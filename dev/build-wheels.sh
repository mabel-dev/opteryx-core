#!/bin/bash
set -ex

# Install OpenSSL development headers inside the container
# Note: zstd/snappy/libcurl are vendored into the project; we should not install
# zstd-devel/snappy-devel via yum inside the manylinux container (they may
# not be available on the base image and we compile vendor sources directly).
# curl-devel is installed here purely to provide system headers as a fallback;
# the actual libcurl.a used at link time is still the vendored static build.
yum install -y openssl-devel curl-devel || dnf install -y openssl-devel libcurl-devel

# Install Rust 1.83.0 (pinned version to avoid GLIBC_2.18 symbols from newer compilers)
#
# This used to be a bare `curl ... | sh`. In a pipeline `set -e` only observes the
# exit status of the LAST command, so a failed fetch (empty stdout) fed `sh` an
# empty script, `sh` exited 0, and the build carried on for eight more minutes of
# C++/Cython compilation before dying at `build_rust` with "can't find Rust
# compiler". The real failure — the network — was invisible. Fetch to a file, check
# the fetch, and verify the toolchain is actually on PATH before continuing.
RUSTUP_SH="$(mktemp)"
rustup_fetched=false
for attempt in 1 2 3; do
  if curl --proto '=https' --tlsv1.2 -sSf --retry 2 --retry-connrefused \
      --connect-timeout 15 --max-time 120 https://sh.rustup.rs -o "${RUSTUP_SH}"; then
    rustup_fetched=true
    break
  fi
  echo "rustup fetch attempt ${attempt}/3 failed" >&2
  sleep $((attempt * 5))
done

if [ "${rustup_fetched}" != true ]; then
  echo "ERROR: could not fetch https://sh.rustup.rs after 3 attempts." >&2
  echo "       Rust is required for the opteryx_core compute extension; refusing" >&2
  echo "       to start a build that can only fail at link time." >&2
  exit 1
fi

sh "${RUSTUP_SH}" --default-toolchain 1.83.0 -y
rm -f "${RUSTUP_SH}"
export PATH="$HOME/.cargo/bin:$PATH"

# The installer can exit 0 having installed nothing usable. Verify before building.
if ! command -v cargo >/dev/null 2>&1 || ! command -v rustc >/dev/null 2>&1; then
  echo "ERROR: rustup ran but cargo/rustc are not on PATH (${PATH})." >&2
  exit 1
fi
rustc --version
cargo --version

cd $GITHUB_WORKSPACE/io
cd io

# Resolve /opt/python interpreter directories robustly for variants like cp314T, cp314t or cp314
py_tag="${PYTHON_VERSION//./}"
PYBIN_CANDIDATES=()

# If the tag ends with a T/t (free-threaded), try with and without the suffix and both cases
if [[ "$py_tag" =~ ^([0-9]+)([Tt])$ ]]; then
  base="${BASH_REMATCH[1]}"
  # manylinux installs free-threaded CPython as cp<ver>-cp<ver>t (no 't' on the
  # first segment, 't' on the second) — this is the canonical layout and MUST be
  # tried first. Never fall back to the GIL build (cp<ver>-cp<ver>): selecting it
  # silently produces a wrong-ABI wheel that won't import on a free-threaded runtime.
  PYBIN_CANDIDATES+=("/opt/python/cp${base}-cp${base}t/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}-cp${base}T/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${py_tag}-cp${py_tag}/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}t-cp${base}t/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}T-cp${base}T/bin")
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

# Fail loud if the resolved interpreter's ABI doesn't match what was requested.
# A free-threaded request (PYTHON_VERSION ending in t/T) MUST resolve to an
# interpreter whose abiflags contain 't'; otherwise we'd build a wrong-ABI wheel.
resolved_abiflags="$("${PYBIN}/python" -c 'import sys; print(sys.abiflags)')"
if [[ "$py_tag" =~ [Tt]$ ]]; then
  if [[ "$resolved_abiflags" != *t* ]]; then
    echo "ABI MISMATCH: requested free-threaded (${PYTHON_VERSION}) but ${PYBIN}/python is GIL-enabled (abiflags='${resolved_abiflags}')"
    exit 1
  fi
else
  if [[ "$resolved_abiflags" == *t* ]]; then
    echo "ABI MISMATCH: requested GIL build (${PYTHON_VERSION}) but ${PYBIN}/python is free-threaded (abiflags='${resolved_abiflags}')"
    exit 1
  fi
fi
echo "Using interpreter ${PYBIN}/python (abiflags='${resolved_abiflags}')"

"${PYBIN}/python" -m pip install -U setuptools wheel setuptools-rust cython auditwheel

NPROC=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
"${PYBIN}/python" setup.py build_ext --parallel "$NPROC" bdist_wheel

# Repair wheels with auditwheel, preserving the 't' suffix for free-threaded builds
# Check PYTHON_VERSION environment variable (e.g., "3.14t") to determine if this is free-threaded
IS_FREE_THREADED=false
if [[ "$PYTHON_VERSION" =~ [0-9]+\.[0-9]+[Tt]$ ]]; then
    IS_FREE_THREADED=true
    echo "Building free-threaded Python wheel (PYTHON_VERSION=$PYTHON_VERSION)"
fi

for whl in dist/*.whl; do
    [ -f "$whl" ] || continue
    echo "Processing wheel: $whl"

    # libonnxruntime is a user-level dependency (installed via onnxruntime pip package),
    # not something to vendor into the wheel. Exclude it from bundling.
    AUDITWHEEL_EXCLUDES="--exclude libonnxruntime.so.1"

    auditwheel repair "$whl" -w dist/ $AUDITWHEEL_EXCLUDES
    repaired=$(ls -t dist/*manylinux*.whl 2>/dev/null | head -n1)

    if [ "$IS_FREE_THREADED" = true ]; then
        echo "  -> Free-threaded build detected"
        # The wheel MUST already carry the free-threaded ABI tag (cp<ver>-cp<ver>t),
        # produced by building with the free-threaded interpreter. Do NOT rename to
        # force the tag — that masks an ABI mismatch (a GIL-built .so relabelled as
        # free-threaded installs cleanly but fails to import). Verify and fail loud.
        if [[ "$(basename "$repaired")" != *-cp[0-9]*t-* ]]; then
            echo "  -> ABI TAG ERROR: free-threaded build produced non-'t' wheel: $(basename "$repaired")"
            echo "     The build interpreter was not free-threaded. Aborting."
            exit 1
        fi
        echo "  -> Verified free-threaded ABI tag: $(basename "$repaired")"
    else
        echo "  -> Standard build"
        if [[ "$(basename "$repaired")" == *-cp[0-9]*t-* ]]; then
            echo "  -> ABI TAG ERROR: GIL build produced free-threaded ('t') wheel: $(basename "$repaired")"
            exit 1
        fi
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
