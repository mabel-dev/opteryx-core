#!/usr/bin/env bash
#
# Release gate: install each built manylinux wheel and IMPORT it.
#
# WHY THIS EXISTS — 0.9.56 shipped and took production down. Every query failed
# with:
#
#   ImportError: opteryx/compiled/nanobind/vectors...so:
#                undefined symbol: draken_cast_uint_to_string
#
# The cause was a `-Wl,--version-script` added to hide the C++ runtime symbols
# that leak out of our own translation units (the `import opteryx; import grpc`
# SIGABRT). manylinux's ld version-binds the dynamic symbol table differently
# from the Debian ld on a dev box, and the RTLD_GLOBAL bridge symbols stopped
# satisfying the sibling extensions' unversioned references.
#
# Every pre-release signal was green and every one of them was measured against
# the wrong artifact:
#   - a dev build on Debian gcc-12 imported fine (different libstdc++ ABI *and*
#     a different linker);
#   - `make q` passed 217/217 against that dev build;
#   - `nm -D --defined-only` showed the bridge symbols still EXPORTED in the
#     broken wheel — a symbol count does NOT catch this.
# The ONLY thing that catches it is importing the actual wheel that ships.
#
# So: no `continue-on-error` on this step. If it fails, the release must not go
# out. Each check runs in a FRESH interpreter because these failures are
# import-ORDER dependent (`grpc` before opteryx is safe; after it is not).
#
# rugo is gated too: its `rugo_native` carries 6 undefined `draken_*` references
# resolved via RTLD_GLOBAL, so it is exposed to exactly the same break, and
# `import rugo; import grpc` aborts the same way. Both wheels are built from
# these sources with the same LD_EXTRA.
#
# Usage (inside a manylinux container, repo mounted at /io):
#   dev/verify-wheel-imports.sh /io/io/dist opteryx
#   dev/verify-wheel-imports.sh /io/dist    rugo
set -euo pipefail

DIST_DIR="${1:-dist}"
MODE="${2:-opteryx}"

case "${MODE}" in
  opteryx)
    PKG="opteryx"
    # The RTLD_GLOBAL bridge check: pulls opteryx.expression ->
    # opteryx.compiled.nanobind.vectors, which resolves draken_* from the global
    # namespace. This is the exact import 0.9.56 broke.
    DEEP_IMPORT='import opteryx
from opteryx.connectors import OpteryxConnector'
    ;;
  rugo)
    PKG="rugo"
    # rugo_native resolves draken_vector_unwrap / draken_cast_* from draken via
    # RTLD_GLOBAL; parquet exercises it, and draken must import alongside.
    DEEP_IMPORT='import rugo
from rugo import parquet
import draken'
    ;;
  *)
    echo "ERROR: unknown mode '${MODE}' (expected 'opteryx' or 'rugo')" >&2
    exit 1
    ;;
esac

echo "=== wheel import gate: ${DIST_DIR} (mode: ${MODE}) ==="
ls -1 "${DIST_DIR}"/*.whl 2>/dev/null || {
  echo "ERROR: no wheels found in ${DIST_DIR}" >&2
  exit 1
}

# Resolve DIST_DIR before leaving the current directory, then run every
# interpreter check from a NEUTRAL cwd.
#
# ⛔ Never run these checks from the repo root. sys.path[0] is the cwd, so the
# SOURCE `rugo/` and `draken/` directories shadow the installed wheel and the
# gate tests the tree instead of the artifact. In the rugo workflow the repo is
# mounted at the container workdir (/io) and this produced a bogus failure:
#   ImportError: cannot import name 'draken_native' from partially initialized
#   module 'draken' ... (/io/draken/__init__.py)
# — source draken/ has no built draken_native.so. The opteryx workflow only
# escaped it because its checkout sits one level down (/io/io).
DIST_DIR="$(cd "${DIST_DIR}" && pwd)"
GATE_CWD="$(mktemp -d)"
trap 'rm -rf "${GATE_CWD}"' EXIT
cd "${GATE_CWD}"

FOUND=0

for PY in /opt/python/*/bin/python*; do
  [ -x "${PY}" ] || continue
  VER="$("${PY}" -c 'import sys; print(f"cp{sys.version_info[0]}{sys.version_info[1]}")' 2>/dev/null)" || continue
  WHEEL="$(ls "${DIST_DIR}"/*"${VER}"*manylinux*.whl 2>/dev/null | head -1 || true)"
  [ -n "${WHEEL}" ] || continue

  echo
  echo "--- ${VER}: ${WHEEL##*/} ---"
  FOUND=1

  "${PY}" -m pip install --quiet --force-reinstall --no-deps "${WHEEL}"

  # 1. Baseline import.
  "${PY}" -c "import ${PKG}; print('  [1/3] import ${PKG} ok:', getattr(${PKG}, '__version__', '?'))"

  # 2. RTLD_GLOBAL bridge symbols (draken_cast_uint_to_string et al).
  "${PY}" -c "${DEEP_IMPORT}
print('  [2/3] deep import ok (RTLD_GLOBAL bridge symbols resolve)')"

  # 3. C++ runtime interposition. grpc (reached via any google.cloud.* import)
  #    dlopens cygrpc; if our extensions export their private libstdc++, cygrpc
  #    binds part of its std::string calls into ours and aborts in free().
  #    Our package MUST be imported FIRST — that is the failing order.
  if "${PY}" -m pip install --quiet grpcio 2>/dev/null; then
    if "${PY}" -c "${DEEP_IMPORT}
import grpc" 2>/dev/null; then
      echo "  [3/3] ${PKG}-then-grpc ok (no C++ runtime interposition)"
    else
      # KNOWN, UNFIXED at the time of writing: 0.9.55 (and the 0.9.57 revert of
      # the version-script attempt) abort here. Hiding the leaked C++ runtime
      # symbols is what fixes it, and the only attempt so far broke the
      # RTLD_GLOBAL bridge instead. Until that lands, a release carrying this
      # abort can still be shipped — but ONLY by setting the variable below
      # deliberately, so it is a visible, reviewable choice in the workflow and
      # never a silent pass.
      echo "  [3/3] FAILED: ${PKG}-then-grpc aborts (C++ runtime interposition)" >&2
      if [ "${OPTERYX_ALLOW_KNOWN_GRPC_ABORT:-0}" = "1" ]; then
        echo "  ⚠ ACCEPTED as a KNOWN FAILURE via OPTERYX_ALLOW_KNOWN_GRPC_ABORT=1." >&2
        echo "  ⚠ This wheel WILL abort if anything imports ${PKG} before grpc" >&2
        echo "  ⚠ (i.e. before any google.cloud.* import). Remove this variable" >&2
        echo "  ⚠ as soon as the symbol-hiding fix lands." >&2
      else
        echo "  Set OPTERYX_ALLOW_KNOWN_GRPC_ABORT=1 to ship anyway (see above)." >&2
        exit 1
      fi
    fi
  else
    echo "  ERROR: could not install grpcio for ${VER}; interposition check could not run." >&2
    echo "  Refusing to report a pass for a check that did not execute." >&2
    exit 1
  fi
  # 4. CONSUMER SHAPE. opteryx_core and the standalone rugo wheel BOTH ship a
  #    draken/ package, to the same site-packages path — so whichever pip
  #    installs LAST wins. opteryx-catalog depends on rugo, so installing it
  #    after the wheel clobbers our draken with rugo's, which may be older and
  #    missing symbols our extensions need (rugo 0.4.22 lacks
  #    draken_cast_uint_to_string). That is an ImportError on EVERY query, and
  #    it reproduces on 0.9.55 as readily as 0.9.56 — it is a packaging
  #    collision, not a version regression.
  #
  #    Checks 1-3 CANNOT see this: a bare-wheel venv passes them in both the
  #    good and broken states. Only installing the real dependency set does.
  if [ "${MODE}" = "opteryx" ]; then
    if "${PY}" -m pip install --quiet opteryx-catalog 2>/dev/null; then
      if "${PY}" -c "${DEEP_IMPORT}" 2>/dev/null; then
        echo "  [4/4] consumer shape ok (draken survives opteryx-catalog/rugo)"
      else
        echo "  [4/4] FAILED: installing opteryx-catalog clobbered draken —" >&2
        echo "        rugo's bundled draken won and lacks symbols opteryx needs." >&2
        "${PY}" -c "${DEEP_IMPORT}" 2>&1 | tail -3 >&2
        exit 1
      fi
    else
      echo "  ERROR: could not install opteryx-catalog; consumer-shape check did not run." >&2
      echo "  Refusing to report a pass for a check that did not execute." >&2
      exit 1
    fi
  fi
done

if [ "${FOUND}" -eq 0 ]; then
  echo "ERROR: no wheel matched any interpreter in /opt/python — nothing was verified." >&2
  ls -1 "${DIST_DIR}" || true
  exit 1
fi

echo
echo "=== wheel import gate PASSED ==="
