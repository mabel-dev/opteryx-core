#!/bin/bash
# Build manylinux wheels for the standalone `rugo` distribution.
#
# Unlike dev/build-wheels.sh (opteryx_core) this needs NO Rust, NO libcurl and
# NO OpenSSL. rugo's parquet sources reference http_client.hpp (remote footer/
# range reads), but that whole path is gated on the RUGO_ENABLE_HTTP macro —
# defined ONLY by the opteryx_core build. rugo/setup.py never defines it, so
# filesystem.hpp / io_pipeline.hpp compile with the HTTP code (and the
# <curl/curl.h> include) stripped out; remote paths fail loud at runtime.
#
# It follows that rugo references zero libcrypto symbols. It used to install
# openssl-devel anyway, because the shared parquet extension force-linked
# -lcrypto (--no-as-needed) to satisfy a CI ldd check — which made auditwheel
# vendor a 2.6MB libcrypto into this wheel and saddle it with a hard OpenSSL
# runtime dependency. The force-link is gone (build_common.parquet_link_args),
# so rugo now links nothing beyond libc/libm/pthread.
#
# Invoked inside a manylinux container with PYTHON_VERSION set (e.g. "3.14" or
# "3.14t"). Produces repaired manylinux wheels in dist/.
set -ex

# repo root = parent of this script's dir (dev/). rugo/setup.py self-roots here.
cd "$(dirname "$0")/.."

# Resolve /opt/python interpreter for PYTHON_VERSION (free-threaded aware) —
# same logic as dev/build-wheels.sh: a 't' request MUST resolve to a 't' ABI.
py_tag="${PYTHON_VERSION//./}"
PYBIN_CANDIDATES=()
if [[ "$py_tag" =~ ^([0-9]+)([Tt])$ ]]; then
  base="${BASH_REMATCH[1]}"
  PYBIN_CANDIDATES+=("/opt/python/cp${base}-cp${base}t/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}-cp${base}T/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${py_tag}-cp${py_tag}/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}t-cp${base}t/bin")
  PYBIN_CANDIDATES+=("/opt/python/cp${base}T-cp${base}T/bin")
else
  PYBIN_CANDIDATES+=("/opt/python/cp${py_tag}-cp${py_tag}/bin")
fi

PYBIN=""
for c in "${PYBIN_CANDIDATES[@]}"; do
  if [ -x "${c}/python" ]; then PYBIN="$c"; break; fi
done
if [ -z "$PYBIN" ]; then
  echo "No matching /opt/python interpreter found for PYTHON_VERSION=${PYTHON_VERSION}"
  for c in "${PYBIN_CANDIDATES[@]}"; do echo "  - $c"; done
  ls -1 /opt/python || true
  exit 1
fi

# Fail loud on ABI mismatch (a wrong-ABI wheel installs cleanly but won't import).
resolved_abiflags="$("${PYBIN}/python" -c 'import sys; print(sys.abiflags)')"
if [[ "$py_tag" =~ [Tt]$ ]]; then
  [[ "$resolved_abiflags" == *t* ]] || { echo "ABI MISMATCH: requested free-threaded but ${PYBIN}/python is GIL (abiflags='${resolved_abiflags}')"; exit 1; }
else
  [[ "$resolved_abiflags" != *t* ]] || { echo "ABI MISMATCH: requested GIL but ${PYBIN}/python is free-threaded (abiflags='${resolved_abiflags}')"; exit 1; }
fi
echo "Using interpreter ${PYBIN}/python (abiflags='${resolved_abiflags}')"

"${PYBIN}/python" -m pip install -U setuptools wheel cython auditwheel

NPROC=$(nproc 2>/dev/null || echo 1)
"${PYBIN}/python" rugo/setup.py build_ext --parallel "$NPROC" bdist_wheel

# Repair with auditwheel, preserving the free-threaded ('t') ABI tag.
IS_FREE_THREADED=false
if [[ "$PYTHON_VERSION" =~ [0-9]+\.[0-9]+[Tt]$ ]]; then IS_FREE_THREADED=true; fi

for whl in dist/*.whl; do
  [ -f "$whl" ] || continue
  echo "Repairing wheel: $whl"
  auditwheel repair "$whl" -w dist/
  repaired=$(ls -t dist/*manylinux*.whl 2>/dev/null | head -n1)
  if [ "$IS_FREE_THREADED" = true ]; then
    if [[ "$(basename "$repaired")" != *-cp[0-9]*t-* ]]; then
      echo "ABI TAG ERROR: free-threaded build produced non-'t' wheel: $(basename "$repaired")"; exit 1
    fi
    echo "Verified free-threaded ABI tag: $(basename "$repaired")"
  else
    if [[ "$(basename "$repaired")" == *-cp[0-9]*t-* ]]; then
      echo "ABI TAG ERROR: GIL build produced free-threaded ('t') wheel: $(basename "$repaired")"; exit 1
    fi
  fi
done

echo "=== rugo manylinux wheels ==="
ls -lh dist/*manylinux*.whl || { echo "No manylinux wheels produced"; exit 1; }
