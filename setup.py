"""
Simplified setup script for Opteryx - builds all Cython extensions and Rust module.
"""

import glob
import os
import re
import sys

from Cython.Build import cythonize
from setuptools import Extension, find_packages, setup
from setuptools_rust import RustExtension

# Shared build machinery + the single-source draken/rugo extension definitions,
# used identically by this wheel (opteryx_core) and the standalone `rugo` wheel
# (rugo/setup.py). See build_common.py — it is the one place those live, so the
# two builds cannot drift. The opteryx-only, side-effectful pieces (libcurl,
# consolidated-module generation, onnxruntime) stay in this file.
from build_common import (
    COMMON_SIMD_SOURCES,
    CPP_FLAGS,
    C_FLAGS,
    FREE_THREADED_BUILD,
    LD_EXTRA,
    WARNING_FLAGS,
    arch,
    build_ext,
    detect_architecture,
    draken_rugo_extensions,
    get_lz4_vendor_sources,
    get_parquet_vendor_sources,
    get_zstd_compress_sources,
    get_zstd_vendor_sources,
    include_dirs,
    is_mac,
    is_win,
    skene_extensions,
    write_draken_abi_modules,
)

LIBRARY = "opteryx"






# Build vendored libcurl as static library
def build_vendored_libcurl():
    """Build vendored libcurl as a static library for http_client extension."""
    import subprocess

    curl_src = os.path.join(os.path.dirname(__file__), "third_party", "curl")
    curl_build = os.path.join(os.path.dirname(__file__), "build", "curl")

    if not os.path.exists(curl_src):
        print("Warning: vendored curl source not found, skipping libcurl build")
        return None

    # Skip if already built
    libcurl_a = os.path.join(curl_build, "lib", ".libs", "libcurl.a")
    if os.path.exists(libcurl_a):
        print(f"Using cached libcurl: {libcurl_a}")
        return libcurl_a

    os.makedirs(curl_build, exist_ok=True)

    # Configure curl as static library with minimal dependencies
    print(f"Building vendored libcurl from {curl_src}...")

    # Try to find OpenSSL prefix via pkg-config
    # --with-openssl expects the install prefix (e.g. /usr), not the lib dir
    openssl_prefix = None
    import shutil

    if shutil.which("pkg-config"):
        result = subprocess.run(
            ["pkg-config", "--variable=prefix", "openssl"], capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            openssl_prefix = result.stdout.strip()
            print(f"  Found OpenSSL at: {openssl_prefix}")

    configure_cmd = [
        os.path.join(curl_src, "configure"),
        f"--prefix={curl_build}",
        "--disable-shared",
        "--enable-static",
        # REQUIRED, not cosmetic. libtool compiles a static-only build without
        # -fPIC, and every consumer of this archive is a shared object. On
        # x86_64 that survives because the toolchain defaults to PIE; on
        # aarch64 devtoolset-10 it does not, and the link dies with
        #   relocation R_AARCH64_ADR_PREL_PG_HI21 against `malloc@@GLIBC_2.17'
        #   ... recompile with -fPIC
        # taking http_client, _operators and pool_reader with it. Removing this
        # breaks the aarch64 wheels only — x86_64 will keep building green.
        "--with-pic",
        "--with-openssl",  # Enable SSL/TLS via OpenSSL for HTTPS support
        "--without-zlib",
        "--without-libpsl",
        "--without-libidn2",
        "--disable-ftp",
        "--disable-file",
        "--disable-ldap",
        "--disable-ldaps",
        "--disable-rtsp",
        "--disable-telnet",
        "--disable-tftp",
        "--disable-pop3",
        "--disable-imap",
        "--disable-smtp",
        "--disable-gopher",
        "--disable-dict",
        "--disable-debug",
    ]

    # Add OpenSSL path if found
    if openssl_prefix:
        configure_cmd.append(f"--with-openssl={openssl_prefix}")

    try:
        print(f"  Running: {' '.join(configure_cmd)}")
        result = subprocess.run(
            configure_cmd, cwd=curl_build, check=False, capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  Configure failed with code {result.returncode}")
            print(f"  STDOUT: {result.stdout[-500:]}")  # Last 500 chars
            print(f"  STDERR: {result.stderr[-500:]}")
            return None

        print("  Running: make")
        result = subprocess.run(
            ["make", "-j", str(os.cpu_count() or 1)],
            cwd=curl_build,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"  Make failed with code {result.returncode}")
            print(f"  STDERR: {result.stderr[-500:]}")
            return None

        print("  Running: make install")
        result = subprocess.run(
            ["make", "install"], cwd=curl_build, check=False, capture_output=True, text=True
        )
        if result.returncode != 0:
            print(f"  Make install failed with code {result.returncode}")
            print(f"  STDERR: {result.stderr[-500:]}")
            return None

        if os.path.exists(libcurl_a):
            print(f"Successfully built libcurl: {libcurl_a}")
            return libcurl_a
        else:
            print(f"Warning: libcurl.a not found at {libcurl_a}")
            # Check what files were created
            import glob

            lib_files = glob.glob(os.path.join(curl_build, "**", "*.a"), recursive=True)
            if lib_files:
                print(f"  Found .a files: {lib_files}")
            return None
    except Exception as e:
        print(f"Warning: Exception during libcurl build: {e}")
        import traceback

        traceback.print_exc()
        return None


def detect_system_libcurl():
    """Probe the system for a usable libcurl via pkg-config.

    Returns (include_dirs, link_args) tuple if found, else None.
    Used as the preferred link mode for local dev — vendored static is reserved
    for self-contained wheels (manylinux/CI), where pkg-config is unavailable
    or system curl is too old.
    """
    import shutil
    import subprocess

    if not shutil.which("pkg-config"):
        return None

    cflags = subprocess.run(["pkg-config", "--cflags", "libcurl"], capture_output=True, text=True)
    libs = subprocess.run(["pkg-config", "--libs", "libcurl"], capture_output=True, text=True)
    if cflags.returncode != 0 or libs.returncode != 0:
        return None

    include_dirs = [tok[2:] for tok in cflags.stdout.split() if tok.startswith("-I")]
    link_args = libs.stdout.split()
    if not link_args:
        return None
    return include_dirs, link_args


# Minimum OpenSSL we will link. 1.1.0 is where OpenSSL became internally
# thread-safe; below it the APPLICATION must install CRYPTO_set_locking_callback
# and CRYPTO_THREADID_set_callback or the library's own global state races.
_MIN_OPENSSL = (1, 1, 0)


def _pkg_config_openssl_version():
    """(major, minor, patch) from `pkg-config --modversion openssl`, or None.

    Deliberately a CONFIGURE-TIME probe rather than an `OPENSSL_VERSION_NUMBER`
    check in C. Two reasons, both learned the hard way:
      * http_client.cpp includes only <curl/curl.h> — it never sees an OpenSSL
        header, so `#if OPENSSL_VERSION_NUMBER < ...` there reads an UNDEFINED
        macro as 0 and silently compiles to nothing. A guard that always passes
        is worse than no guard.
      * LibreSSL pins OPENSSL_VERSION_NUMBER low while being perfectly thread
        safe, so the C macro rejects a good TLS stack. pkg-config reports
        LibreSSL's real version and does not.
    """
    import shutil
    import subprocess

    if not shutil.which("pkg-config"):
        return None
    result = subprocess.run(
        ["pkg-config", "--modversion", "openssl"], capture_output=True, text=True
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    # Versions look like "3.6.3", "1.1.1k", "1.0.2k-fips" — take the leading
    # numeric components and ignore any letter/suffix.
    parts = []
    for token in result.stdout.strip().split(".")[:3]:
        digits = ""
        for ch in token:
            if not ch.isdigit():
                break
            digits += ch
        if not digits:
            break
        parts.append(int(digits))
    if len(parts) < 2:
        return None
    while len(parts) < 3:
        parts.append(0)
    return tuple(parts)


def assert_openssl_thread_safe(*, fail_when_unknown):
    """Refuse to link an OpenSSL that is not internally thread-safe.

    Opteryx drives libcurl from many native engine threads at once, so a
    pre-1.1.0 OpenSSL needs locking callbacks that this project does not install.
    Shipping that combination produced a production SIGSEGV inside `lh_retrieve`
    (via ERR_clear_error → ERR_get_state) from concurrent TLS handshakes: the
    manylinux2014 wheel bundled CentOS 7's OpenSSL 1.0.2k. We refuse the build
    rather than carry an untestable compatibility shim.

    `fail_when_unknown` is True for the VENDORED path, where we are about to link
    `-lssl -lcrypto` from this very prefix and an unknown version is unacceptable.
    It is False for a system libcurl, which may not use OpenSSL at all (macOS
    ships SecureTransport/LibreSSL) — there an undeterminable version is not
    evidence of a problem, so only a KNOWN-BAD version fails.
    """
    version = _pkg_config_openssl_version()

    if version is None:
        if not fail_when_unknown:
            return
        raise RuntimeError(
            "Cannot determine the OpenSSL version via `pkg-config --modversion openssl`, "
            "and the vendored libcurl build is about to link -lssl -lcrypto against it.\n\n"
            f"Opteryx requires OpenSSL >= {'.'.join(map(str, _MIN_OPENSSL))}: it drives "
            "libcurl from many threads, and earlier OpenSSL is not internally thread-safe.\n\n"
            "Install OpenSSL development files and pkg-config:\n"
            "  - Debian/Ubuntu:  apt-get install libssl-dev pkg-config\n"
            "  - RHEL/Fedora:    dnf install openssl-devel pkgconf-pkg-config\n"
            "  - macOS:          brew install openssl@3 pkg-config"
        )

    if version < _MIN_OPENSSL:
        found = ".".join(map(str, version))
        want = ".".join(map(str, _MIN_OPENSSL))
        raise RuntimeError(
            f"OpenSSL {found} is too old to link: opteryx requires >= {want}.\n\n"
            "Opteryx drives libcurl concurrently from native engine threads. OpenSSL "
            "before 1.1.0 is not internally thread-safe — it requires the application to "
            "install CRYPTO locking callbacks, which this project deliberately does not "
            "do. Linking it produces intermittent SIGSEGV inside libcrypto during "
            "concurrent TLS handshakes, not a clean error.\n\n"
            "Install a supported OpenSSL (1.1.1 or 3.x) and make sure pkg-config finds it "
            "first, e.g. via PKG_CONFIG_PATH."
        )

    print(f"OpenSSL {'.'.join(map(str, version))} (>= {'.'.join(map(str, _MIN_OPENSSL))}) OK")


def resolve_libcurl():
    """Return (include_dirs, link_args) for libcurl, preferring system over vendored.

    Resolution order:
      1. OPTERYX_VENDOR_CURL=1 → force vendored static (CI/wheel builds).
      2. System libcurl via pkg-config → use it (fast, reliable for local dev).
      3. Vendored static build → fallback.
    Hard-fails if none succeed; the http_client extension is mandatory.

    Both paths are gated on a thread-safe OpenSSL — see assert_openssl_thread_safe.
    """
    force_vendor = os.environ.get("OPTERYX_VENDOR_CURL", "0").lower() in ("1", "true", "yes")

    if not force_vendor:
        assert_openssl_thread_safe(fail_when_unknown=False)
        sys_curl = detect_system_libcurl()
        if sys_curl is not None:
            sys_inc, sys_libs = sys_curl
            print(f"Using system libcurl (pkg-config): {' '.join(sys_libs)}")
            return sys_inc, sys_libs

    # Fall back to vendored static build. This path links -lssl -lcrypto from the
    # pkg-config prefix that build_vendored_libcurl() passes to --with-openssl, so
    # the gate is exact here and an undeterminable version is itself a failure.
    assert_openssl_thread_safe(fail_when_unknown=True)
    libcurl_a = build_vendored_libcurl()
    if libcurl_a and os.path.exists(libcurl_a):
        return ["third_party/curl/include"], [libcurl_a, "-lssl", "-lcrypto"]

    raise RuntimeError(
        "Failed to resolve libcurl. The http_client extension is REQUIRED.\n\n"
        "Either install system libcurl + pkg-config:\n"
        "  - macOS:           brew install curl pkg-config\n"
        "  - Ubuntu/Debian:   apt-get install libcurl4-openssl-dev pkg-config\n"
        "  - RHEL/Fedora:     yum install libcurl-devel pkgconfig\n\n"
        "Or install OpenSSL headers so the vendored static build can run:\n"
        "  - macOS:           brew install openssl\n"
        "  - Ubuntu/Debian:   apt-get install libssl-dev\n"
        "  - RHEL/Fedora:     yum install openssl-devel\n\n"
        "CI/wheel builds set OPTERYX_VENDOR_CURL=1 to force the vendored path."
    )


# Skip extension building for clean command
if "clean" in [arg.lower() for arg in sys.argv[1:] if arg and not arg.startswith("-")]:
    print("Skipping native extension build for clean command")
    sys.exit(0)



# Read version and metadata
with open(f"{LIBRARY}/__version__.py", "r") as v:
    content = v.read()
    for match in re.finditer(r'^(__\w+__)\s*=\s*["\']?([^"\']+)["\']?$', content, re.MULTILINE):
        var_name, var_value = match.groups()
        globals()[var_name] = var_value

with open("README.md", "r", encoding="UTF8") as f:
    long_description = f.read()


# Resolve libcurl - REQUIRED for http_client extension
# Skip for sdist (source distribution packaging) and clean - no compilation needed
# Skip when DRAKEN_BUILD=1 — draken-only builds don't need libcurl.
#
# Resolved BEFORE the extensions list (it used to sit after it) because extensions
# INSIDE the list now need the curl flags too — opteryx.operators._operators builds
# the HTTP-enabled copy of io_pipeline.hpp. Everything below is unchanged; only the
# position moved.
_DRAKEN_BUILD = bool(os.environ.get("DRAKEN_BUILD"))
_build_commands = {"build", "build_ext", "install", "bdist_wheel", "bdist", "develop"}
_skip_build = not any(
    arg.lower() in _build_commands for arg in sys.argv[1:] if arg and not arg.startswith("-")
)
_curl_include_dirs: list[str] = []
_curl_link_args: list[str] = []
if not _skip_build and not _DRAKEN_BUILD:
    _curl_include_dirs, _curl_link_args = resolve_libcurl()


# Define all extensions
extensions = [
    *draken_rugo_extensions(
        parquet_created_by="opteryx-rugo version %s (build %s)"
        % (__version__, __build__)
    ),
    # skene file-format extension (skene depends on draken alone; disjoint from rugo).
    *skene_extensions(),
    # Third-party libraries.
    #
    # The mabel codec C libraries live at the repo root (third_party/mabel/base*):
    # they are opteryx-free vendored code, and draken's kernels — which ship in the
    # standalone rugo wheel — compile them via build_common's draken_rugo_extensions.
    # Only the Cython wrappers below (opteryx's Python-visible base16/base64/base85
    # modules) stay under opteryx/. Include dirs for the moved headers come from
    # build_common's include_dirs.
    Extension(
        "opteryx.third_party.mabel.base64",
        sources=[
            "opteryx/third_party/mabel/base64/base64.pyx",
            "third_party/mabel/base64/_base64.c",
            "third_party/mabel/base64/_base64_dispatch.c",
            "third_party/mabel/base64/_base64_neon.c",
            "third_party/mabel/base64/_base64_avx2.c",
            "third_party/mabel/base64/_base64_rvv.c",
        ],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS + ["-std=c99", "-DBASE64_IMPLEMENTATION"],
    ),
    Extension(
        "opteryx.third_party.mabel.base16",
        sources=[
            "opteryx/third_party/mabel/base16/base16.pyx",
            # Unity build — _base16.c #includes the dispatch + per-arch SIMD sources.
            "third_party/mabel/base16/_base16.c",
        ],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS + ["-std=c99"],
    ),
    Extension(
        "opteryx.third_party.mabel.base85",
        sources=[
            "opteryx/third_party/mabel/base85/base85.pyx",
            "third_party/mabel/base85/_base85.c",
        ],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS + ["-std=c99"],
    ),
    Extension(
        "opteryx.third_party.cyan4973.xxhash",
        sources=[
            "opteryx/third_party/cyan4973/xxhash.pyx",
            "third_party/cyan4973/xxhash.c",
            "src/cpp/xxhash_build_info.c",
        ],
        include_dirs=include_dirs,
        define_macros=[
            ("XXH_NO_XXH128", "1"),
            # Opteryx-specific optimizations for analytics workloads
            ("XXH_INLINE_ALL", "1"),  # Force inlining for better optimization
            ("XXH_ACCEPT_NULL_INPUT_POINTER", "0"),  # We never pass NULL
            ("XXH_FORCE_ALIGN_CHECK", "0"),  # Inputs are properly aligned
        ],
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.third_party.yyjson.cyyjson",
        sources=[
            "opteryx/third_party/yyjson/cyyjson.pyx",
            "third_party/yyjson/src/yyjson.c",
        ],
        include_dirs=include_dirs + ["third_party/yyjson/src"],
        language="c",
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        # Opteryx-owned rewrite; do not revendor from the original Python package.
        name="opteryx.third_party.mbleven",
        sources=["opteryx/third_party/mbleven.pyx"],
        extra_compile_args=C_FLAGS,
    ),
    # High-performance distogram for cost-based optimization
    Extension(
        name="opteryx.third_party.maki_nage.distogram",
        sources=[
            "opteryx/third_party/maki_nage/distogram.pyx",
            "opteryx/third_party/maki_nage/_distogram_core.cpp",
            "opteryx/third_party/maki_nage/_distogram_avx2.cpp",
            "opteryx/third_party/maki_nage/_distogram_neon.cpp",
            "opteryx/third_party/maki_nage/_distogram_rvv.cpp",
        ],
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS,
        language="c++",
    ),
    # Core compiled components
    Extension(
        "opteryx.compiled.functions.strings",
        sources=[
            "opteryx/compiled/functions/strings.pyx",
            "src/cpp/simd_search.cpp",
            "src/cpp/simd_string_ops.cpp",
            "draken/simd/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # Lightweight C++ extension exposing SIMD string ops directly
    Extension(
        "opteryx.compiled.simd_strings",
        sources=[
            "src/cpp/simd_strings_extension.cpp",
            "src/cpp/simd_search.cpp",
            "src/cpp/simd_string_ops.cpp",
            "draken/simd/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    # SIMD capability probe — exposes cpu_architecture() to Python
    Extension(
        "opteryx.compiled.simd_probe",
        sources=[
            "opteryx/compiled/simd_probe.pyx",
            "draken/simd/simd_env.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    Extension(
        name="opteryx.compiled.functions.timestamp",
        sources=["opteryx/compiled/functions/timestamp.pyx"],
        extra_compile_args=C_FLAGS,
    ),
    # Buffering aggregates' memory ceilings, for `SHOW VARIABLES` to report.
    # Header-only and dependency-free ON PURPOSE: opteryx/variables.py needs these
    # values and sits below the engine in the import graph, so it cannot reach
    # them through opteryx.operators._operators without a circular import.
    Extension(
        "opteryx.compiled.agg_budgets",
        sources=["opteryx/compiled/agg_budgets.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    # Platform extension - exposes OS information without psutil dependency
    Extension(
        "opteryx.compiled.platform",
        sources=[
            "opteryx/compiled/platform.pyx",
            "src/cpp/platform.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    Extension(
        "opteryx.compiled.structures.carchar_set",
        sources=[
            "opteryx/compiled/structures/carchar_set.pyx",
            "draken/simd/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        depends=[
            "third_party/mabel/carchar/carchar_set.hpp",
            "third_party/mabel/carchar/carchar_common.hpp",
            "third_party/mabel/carchar/carchar_simd.hpp",
        ],
    ),
    Extension(
        "opteryx.compiled.structures.parvi_set",
        sources=[
            "opteryx/compiled/structures/parvi_set.pyx",
            "draken/simd/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        depends=[
            "third_party/mabel/parvi/parvi.hpp",
            "third_party/mabel/carchar/carchar_index.hpp",
            "third_party/mabel/carchar/carchar_common.hpp",
            "third_party/mabel/carchar/carchar_simd.hpp",
        ],
    ),
    Extension(
        "opteryx.compiled.structures.node",
        sources=["opteryx/compiled/structures/node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.compiled.structures.perfect_hash_set",
        sources=["opteryx/compiled/structures/perfect_hash_set.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        depends=[
            "third_party/mabel/perfect_hash/perfect_hash_set.hpp",
        ],
    ),
    Extension(
        "opteryx.compiled.structures.perfect_hash_map",
        sources=["opteryx/compiled/structures/perfect_hash_map.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        depends=[
            "third_party/mabel/perfect_hash/perfect_hash_map.hpp",
        ],
    ),
    # evaluator/* leaf modules are textually included by evaluator/_impl.pyx
    # and built into a single .so at opteryx/expression/evaluator/_impl.so.
    # The sibling __init__.py imports _impl, re-exports the public API, and
    # registers legacy submodule aliases.
    #
    # We compile a `_impl` submodule (not `__init__`) because Cython 3.x emits
    # an internal `PyImport_ImportModule("<pkg>.__init__")` call when typed
    # memoryviews are involved, and Python can't resolve that synthetic name
    # when the extension is named with `.__init__`.
    #
    # Built as C++ because evaluation.pyx cimports CompiledExpression from the
    # opteryx/compiled/expression/ C++ arena header.
    # opteryx.expression.evaluator._impl has been merged into _operators.
    # All evaluator leaf .pyx files are now textually included by _operators.pyx
    # so operators can call bytecode VM functions directly at C level.
    *[
        Extension(
            _name,
            sources=[_src],
            include_dirs=include_dirs,
            extra_compile_args=C_FLAGS,
        )
        for _name, _src in (
            ("opteryx.expression.__init__", "opteryx/expression/__init__.pyx"),
            ("opteryx.expression.functions.__init__", "opteryx/expression/functions/__init__.pyx"),
            (
                "opteryx.expression.functions.implementations.__init__",
                "opteryx/expression/functions/implementations/__init__.pyx",
            ),
            (
                "opteryx.expression.functions.registrar.__init__",
                "opteryx/expression/functions/registrar/__init__.pyx",
            ),
        )
    ],
    # yyjson.c used to be compiled in here for special_ops.pyx (`@?`), which
    # cimported it at C level — on Linux (RTLD_LOCAL) cyyjson.so's symbols are not
    # visible to other extensions. `@?` has a native kernel now and special_ops.pyx
    # is deleted, so nothing under operations/ touches yyjson and the source is no
    # longer linked in.
    Extension(
        "opteryx.expression.operations.__init__",
        sources=[
            "opteryx/expression/operations/__init__.pyx",
        ],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    # functions/catalog.pyx is textually included by functions/__init__.pyx;
    # registrar/* and implementations/* are similarly consolidated into their
    # package __init__ files. No per-leaf Extensions needed for any of them.
    # Compiled (C++) representation of an expression tree; lowered from Node
    # at bind time and walked by the evaluator. See src/cpp/expression/.
    Extension(
        "opteryx.compiled.expression.compiled_expression",
        sources=[
            "opteryx/compiled/expression/compiled_expression.pyx",
            "src/cpp/expression/compiled_expression.cpp",
        ],
        include_dirs=include_dirs + ["src/cpp"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.compiled.structures.column_deserializer",
        sources=[
            "opteryx/compiled/structures/column_deserializer.pyx",
            "src/cpp/ipc_deserialize.cpp",
            "src/cpp/memory_pool.cpp",
        ],
        # column_deserializer.pyx does `cdef extern from "ipc_deserialize.hpp"`;
        # add src/cpp to the include path so Cython can resolve the header.
        include_dirs=include_dirs + ["src/cpp"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # MemoryViewStream: high-performance memoryview-backed stream (Cython)
    Extension(
        "opteryx.compiled.structures.memory_view_stream",
        sources=["opteryx/compiled/structures/memory_view_stream.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
        language="c",
    ),
    Extension(
        "opteryx.compiled.structures.memory_pool",
        sources=[
            "opteryx/compiled/structures/memory_pool.pyx",
            "src/cpp/memory_pool.cpp",
        ],
        include_dirs=include_dirs + ["src/cpp"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "opteryx.compiled.structures.lru_k",
        sources=["opteryx/compiled/structures/lru_k.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # Parquet footer cache using LRU eviction
    Extension(
        "opteryx.compiled.structures.footer_cache",
        sources=["opteryx/compiled/structures/footer_cache.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # Lazy per-file column statistics (wraps vector[AggColumnStat])
    Extension(
        "opteryx.compiled.structures.column_stats",
        sources=["opteryx/compiled/structures/column_stats.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # C-backed integer buffer used across joins and other kernels
    Extension(
        "opteryx.compiled.structures.buffers",
        sources=[
            "opteryx/compiled/structures/buffers.pyx",
            "src/cpp/intbuffer.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # Helpers for relation statistics
    Extension(
        "opteryx.compiled.structures.relation_statistics",
        sources=[
            "opteryx/compiled/structures/relation_statistics.pyx",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # Expression evaluator — consolidated .so for all evaluator leaf modules.
    # Leaf .pyx files are textually included by _impl.pyx. yyjson.c used to be
    # compiled in for json_ops.pyx's `@?` row loop; `@?` has a native kernel now
    # and that loop is deleted, so no leaf here cimports yyjson.
    Extension(
        "opteryx.expression.evaluator._impl",
        sources=[
            "opteryx/expression/evaluator/_impl.pyx",
            "opteryx/expression/evaluator/bytecode_worker.cpp",
        ],
        include_dirs=include_dirs
        + [
            "opteryx/expression/evaluator",  # bytecode_worker.h, bitmap_worker_pool.h
            "third_party/yyjson/src",
        ],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    # All operator plan nodes — single consolidated .so.
    # bs_pool_submit_native / bs_pool_wait_native (src/cpp/bs_pool_bridge_c.h) are
    # implemented in thread_pool.so and resolved at import time (thread_pool is
    # loaded RTLD_GLOBAL in opteryx/compiled/__init__.py before this extension is
    # imported) — same cross-.so bridge pattern as draken_vector_unwrap above.
    Extension(
        "opteryx.operators._operators",
        sources=[
            "opteryx/operators/_operators.pyx",
            "src/cpp/hllpp.cpp",
            "third_party/tdigest-c/src/tdigest_cpp.cpp",
            # Native (zero-Python) engine's pool-path decimal decoder
            # (src/cpp/engine/native_decimal_pool_decode.hpp) calls straight
            # into opteryx::MemoryPool / deserialize_fixed_column — same
            # CPP_FLAGS as every other extension compiling these two .cpp
            # files (opteryx.compiled.structures.memory_pool,
            # opteryx.compiled.structures.column_deserializer), so the
            # per-.so copies stay layout-identical (unlike the BS::thread_pool
            # cross-.so ABI mismatch this codebase hit previously, which was
            # caused by differing -std=/feature-macro flags, not by multiple
            # compiled copies per se).
            "src/cpp/ipc_deserialize.cpp",
            "src/cpp/memory_pool.cpp",
            # NativeSkeneScanSource (src/cpp/engine/native_skene_scan_source.hpp)
            # decodes .skene files on worker threads, so this extension compiles
            # skene's reader in. Its own copy, like draken/core/vector_alloc.cpp
            # elsewhere: skene's reader is stateless pure functions over a
            # caller-supplied buffer — no static registry, no cross-TU singleton —
            # so a second compiled copy cannot produce the kernel_registry-style
            # split-state hazard. skene_native (the libskene wheel's extension)
            # keeps its own copy; neither extension links the other.
            "skene/src/checksum.cpp",
            "skene/src/probe.cpp",
            "skene/src/reader.cpp",
            "skene/src/reader_v1.cpp",
            "skene/src/encoding.cpp",
            "skene/src/statistics.cpp",
            "skene/src/value_order.cpp",
            "skene/src/bloom.cpp",
            "skene/src/file_io.cpp",
            "skene/src/writer.cpp",
            # skene's kLz4 section codec. Compiled in rather than left undefined
            # for the loader to satisfy from pool_reader.so: lz4 is stateless C
            # with no cross-TU state, so a second copy is free of the split-state
            # hazard, and an undefined symbol here would only surface when a
            # scan first met an LZ4 section. Note it stays .c while the vendored
            # zstd sources in this tree are .cpp — setuptools compiles it as C,
            # and lz4.h's extern "C" makes that transparent to skene's C++.
            "third_party/lz4/lz4.c",
            # NativeParquetScanSource submits work to a ParquetIOPipeline that
            # pool_reader.so constructed. io_pipeline.hpp is header-only, so this
            # extension gets its OWN inline copy of submit_row_group/decode_row_group
            # — and RUGO_ENABLE_HTTP must therefore MATCH pool_reader's, for two
            # independent reasons (see define_macros below).
            "src/cpp/http_client.cpp",
        ]
        # skene's kZstd section codec, both halves. Same argument as lz4.c above,
        # and it is NOT optional: skene/src/encoding.cpp calls ZSTD_compress /
        # ZSTD_compressBound / ZSTD_decompress / ZSTD_isError / ZSTD_getErrorName
        # unguarded. Leaving them undefined made this .so depend on some OTHER
        # extension being dlopen'd RTLD_GLOBAL first, and pool_reader.so — the only
        # candidate loaded on the import path — carries the DECOMPRESS set only
        # (get_parquet_vendor_sources). The compress symbols therefore resolved
        # nowhere and Linux failed the whole `import opteryx.operators` with
        # "undefined symbol: ZSTD_compressBound"; macOS hid it because
        # -undefined dynamic_lookup defers the binding. skene_native and
        # rugo_native already compile both halves in for exactly this reason.
        + get_zstd_vendor_sources()
        + get_zstd_compress_sources(),
        include_dirs=include_dirs
        + [
            "opteryx/operators/aggregate",
            "skene/include",   # skene/reader.h etc (NativeSkeneScanSource)
            "skene/src",       # skene's internal headers (reader_v1.h, encoding.h, ...)
            "third_party/zstd",          # skene's per-section codecs
            "third_party/zstd/common",
            "third_party/zstd/decompress",
            "third_party/zstd/compress",
            "third_party/lz4",           # lz4.h
        ]
        + _curl_include_dirs,
        # RUGO_ENABLE_HTTP must match opteryx.connectors.parquet_io.pool_reader.
        # This is the "differing feature macro" ABI hazard already called out in the
        # sources comment above, and it bit for real:
        #
        #   1. io_pipeline.hpp declares DATA MEMBERS (http_tuning_, http_tuning_set_)
        #      inside `#ifdef RUGO_ENABLE_HTTP`, so a mismatch changes the offset of
        #      every member declared after them — one class, two layouts.
        #   2. Without it, this .so's copy of decode_row_group() compiles the remote
        #      branch out and calls reject_remote_path() instead, which throws from
        #      OUTSIDE the try block — the work item dies, no result is ever queued,
        #      and the Source reports "pipeline drained with result(s) missing".
        #
        # A remote scan reaching NativeParquetScanSource hit (2) and silently returned
        # ZERO ROWS. Keep these two extensions' macro sets in lockstep.
        #
        # HAVE_ZSTD / ZSTD_STATIC_LINKING_ONLY match skene_native and rugo_native —
        # the vendored zstd TUs added to sources above are built the same way in
        # every extension that carries them.
        define_macros=[
            ("RUGO_ENABLE_HTTP", "1"),
            ("HAVE_ZSTD", "1"),
            ("ZSTD_STATIC_LINKING_ONLY", "1"),
        ],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA
        + _curl_link_args
        + (["-undefined", "dynamic_lookup"] if is_mac() else ["-Wl,--allow-shlib-undefined"]),
        depends=[
            "third_party/mabel/parvi/parvi.hpp",
            "third_party/mabel/carchar/carchar_index.hpp",
            "third_party/mabel/carchar/carchar_common.hpp",
            "third_party/mabel/carchar/carchar_simd.hpp",
        ],
    ),
    Extension(
        "opteryx.compiled.structures.shuffle_partition",
        sources=[
            "opteryx/compiled/structures/shuffle_partition.pyx",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # E.21b: morsel_ops.distinct — full implementation (Morsel.c_hash + _resolve_columns_to_indices now live).
    Extension(
        "opteryx.compiled.morsel_ops.distinct",
        sources=["opteryx/compiled/morsel_ops/distinct.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # NOTE: morsel_ops.sort moved to draken.morsels.sort (a Draken core primitive
    # built by build_common.draken_rugo_extensions, shipped in both wheels).
    Extension(
        "opteryx.compiled.morsel_ops.null_filter",
        sources=["opteryx/compiled/morsel_ops/null_filter.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # Thread pool (BS::thread_pool via BSThreadPoolBridge). thread_pool_bridge.cpp
    # is the ONE compiled home of bs_pool_bridge_c.h's cross-.so entry points —
    # see that header for why they must live only here.
    #
    # extra_compile_args MUST match CPP_FLAGS (-std=c++20), not a hand-rolled
    # -std=c++17 — this is the EXACT mismatch the "-operators" extension's comment
    # above warns about ("BS::thread_pool cross-.so ABI mismatch... caused by
    # differing -std=/feature-macro flags"). BS::thread_pool.hpp branches on
    # __cplusplus/__cpp_lib_move_only_function (e.g. which move_only_function
    # implementation it uses), so a -std=c++17-compiled PriorityPool has a
    # DIFFERENT memory layout than the -std=c++20-compiled one every other
    # extension that touches it (pool_reader.so, _operators.so, both on
    # CPP_FLAGS) expects — confirmed 2026-07-07: sharing a shared_ptr<PriorityPool>
    # from this extension into pool_reader/_operators segfaulted inside
    # std::priority_queue::emplace on the very first query, isolated to exactly
    # this mismatch. Gap #3 Phase 2b (docs/DUCKDB_GAP3_DECODE_BUDGET_PLAN.md).
    Extension(
        name="opteryx.compiled.thread_pool",
        sources=["opteryx/compiled/thread_pool.pyx", "opteryx/compiled/thread_pool_bridge.cpp"],
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS,
        language="c++",
    ),
    # MorselQueue (moodycamel MPMC + LightweightSemaphore; carries shared_ptr[CxxMorsel])
    #
    # extra_compile_args MUST be CPP_FLAGS, for the same reason spelled out on the
    # thread_pool extension above: this module carries shared_ptr<CxxMorsel> across
    # the .so boundary, and a hand-rolled -std=c++17 against the tree's -std=c++20
    # is exactly the feature-macro mismatch that produced the PriorityPool layout
    # divergence. It also missed the arch flags entirely (baseline SSE2 on x86)
    # despite sitting on the terminal output edge of every query.
    Extension(
        name="opteryx.compiled.morsel_queue",
        sources=["opteryx/compiled/morsel_queue.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS,
        language="c++",
    ),
    # HTTP Client (libcurl-based HTTP with connection pooling and Range request support)
]

if not _skip_build and not _DRAKEN_BUILD:
    # HTTP client extension - MANDATORY (only add if not cleaning)
    extensions.append(
        Extension(
            name="opteryx.compiled.http_client",
            sources=[
                "opteryx/compiled/http_client.pyx",
                "src/cpp/http_client.cpp",
            ],
            include_dirs=include_dirs + ["src/cpp"] + _curl_include_dirs,
            # CPP_FLAGS, not a hand-rolled -std=c++17 — see morsel_queue above.
            extra_compile_args=CPP_FLAGS,
            extra_link_args=_curl_link_args + ([] if is_win() else ["-lm"]),
            language="c++",
        )
    )


# Auto-generate consolidated modules
def generate_consolidated_module(module_dir, output_file):
    output_abs = os.path.abspath(output_file)
    pyx_files = sorted(
        [
            f
            for f in glob.glob(os.path.join(module_dir, "*.pyx"))
            if os.path.abspath(f) != output_abs
        ]
    )

    if pyx_files and os.path.exists(output_file):
        output_mtime_ns = os.stat(output_file).st_mtime_ns
        latest_source_mtime_ns = max(os.stat(pyx_file).st_mtime_ns for pyx_file in pyx_files)
        if latest_source_mtime_ns <= output_mtime_ns:
            print(f"Skipping {output_file}; consolidated module is up to date")
            return

    with open(output_file, "w", encoding="UTF8") as f:
        f.write("# Auto-generated consolidated module\n# DO NOT EDIT - generated by setup.py\n\n")
        for pyx_file in pyx_files:
            include_path = os.path.relpath(pyx_file, os.path.dirname(output_file))
            include_path = include_path.replace(os.sep, "/")
            f.write(f'include "{include_path}"\n')

    print(f"Generated {output_file} with {len(pyx_files)} includes")


# Generate vector_ops kernels
generate_consolidated_module(
    "opteryx/compiled/vector_ops", "opteryx/compiled/vector_ops/vector_ops.pyx"
)


# Add consolidated modules with their dependencies
# Link args for vector_ops (use -lcrypto on non-macOS and -pthread where appropriate)
# Use vendored digests to avoid runtime libcrypto dependency on target systems
# Vendored implementations: third_party/crypto/* (MD5, SHA1, SHA256)
vector_ops_link_args = []

if not is_win():
    vector_ops_link_args.append("-pthread")
if is_mac():
    vector_ops_link_args.extend(["-undefined", "dynamic_lookup"])

extensions.extend(
    [
        Extension(
            "opteryx.compiled.vector_ops.vector_ops",
            sources=(
                ["opteryx/compiled/vector_ops/vector_ops.pyx"]
                + sorted(
                    glob.glob("third_party/re2/re2/*.cc")
                    + [
                        "third_party/re2/util/strutil.cc",
                        "third_party/re2/util/rune.cc",
                        "draken/simd/simd_env.cpp",
                        "src/cpp/simd_search.cpp",
                        # src/cpp/simd_datepart.cpp is deliberately NOT here: its
                        # 8 exported simd_datepart_* entry points have no caller
                        # anywhere in the tree, so it was 518 lines of dead weight
                        # compiled into the shipped .so. The SOURCE is retained on
                        # purpose — its part/unit-specialized CAL_LOOP structure
                        # (compile-time divisors → multiply-shift) is the donor for
                        # the live draken_date_part rework, after which it goes.
                        "src/cpp/simd_string_ops.cpp",
                        "draken/simd/cpu_features.cpp",
                        "third_party/crypto/md5.cpp",
                        "third_party/crypto/sha1.cpp",
                        "third_party/crypto/sha2.cpp",
                        "third_party/crypto/sha512.cpp",
                    ]
                )
            ),
            include_dirs=include_dirs,
            language="c++",
            extra_compile_args=CPP_FLAGS,
            extra_link_args=vector_ops_link_args,
            define_macros=[("VENDORED_DIGESTS", "1")],
        ),
    ]
)
# Require vendored nanobind headers for building the nanobind-backed extension.
if not (
    os.path.exists("third_party/nanobind/nanobind.h")
    or os.path.exists("third_party/nanobind/nanobind/nanobind.h")
):
    raise SystemExit(
        "Vendored nanobind headers not found in third_party/nanobind.\n"
        "Please run `python tools/vendor_nanobind.py --tag <tag>` or add the headers to the repo."
    )

# E.2 — C′ pattern pilot: 6 bitwise ops as pure nanobind C++.
# draken_vector_unwrap / draken_vector_own_raw are implemented in draken_native.so
# and resolved at import time (draken/__init__.py loads draken_native with
# RTLD_GLOBAL before any consumer extension is imported).
_bitwise_bridge_link_args = (
    ["-undefined", "dynamic_lookup"] if is_mac() else ["-Wl,--allow-shlib-undefined"]
)
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_length",
        sources=[
            "src/cpp/vector_length_native.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA,
        language="c++",
    )
)
# ── Consolidated C′ vector-op module ──────────────────────────────────────
# Each vector_*.cpp below exposes `void register_<name>(nb::module_&)` instead of
# its own NB_MODULE; _vectors_module.cpp owns the single NB_MODULE(vectors, …) and
# calls them all, so the 21 kernels link into ONE shared object rather than 21.
# This removes the per-extension duplication of vector_alloc.cpp / nb_combined.cpp
# that bloated the wheel. New vector-op file → add it here AND register it in
# _vectors_module.cpp. (vector_length / vector_search / carchar / usearch / minilm
# stay separate — independent native libraries, not C′ kernels.)
_vectors_op_cpp = [
    "opteryx/compiled/nanobind/vector_accessors.cpp",
    "opteryx/compiled/nanobind/vector_array_reduce.cpp",
    "opteryx/compiled/nanobind/vector_bitwise.cpp",
    "opteryx/compiled/nanobind/vector_bool_ops.cpp",
    "opteryx/compiled/nanobind/vector_casts.cpp",
    "opteryx/compiled/nanobind/vector_codec.cpp",
    "opteryx/compiled/nanobind/vector_hash_codec.cpp",
    "opteryx/compiled/nanobind/vector_json.cpp",
    "opteryx/compiled/nanobind/vector_math.cpp",
    "opteryx/compiled/nanobind/vector_misc.cpp",
    "opteryx/compiled/nanobind/vector_selection_concat.cpp",
    "opteryx/compiled/nanobind/vector_sketch_reduce.cpp",
    "opteryx/compiled/nanobind/vector_special.cpp",
    "opteryx/compiled/nanobind/vector_split_native.cpp",
    "opteryx/compiled/nanobind/vector_string_case.cpp",
    "opteryx/compiled/nanobind/vector_string_misc.cpp",
    "opteryx/compiled/nanobind/vector_string_misc2.cpp",
    "opteryx/compiled/nanobind/vector_string_misc3.cpp",
    "opteryx/compiled/nanobind/vector_string_search.cpp",
    "opteryx/compiled/nanobind/vector_string_slice.cpp",
    "opteryx/compiled/nanobind/vector_temporal_arith.cpp",
    "opteryx/compiled/nanobind/vector_temporal_convert.cpp",
]
# Vendored sources pulled in by individual kernels — compiled ONCE for the module.
_vectors_extra_sources = [
    # vector_codec — mabel base64 / base85 (base64 is NOT a unity build: list all)
    "third_party/mabel/base64/_base64.c",
    "third_party/mabel/base64/_base64_dispatch.c",
    "third_party/mabel/base64/_base64_neon.c",
    "third_party/mabel/base64/_base64_avx2.c",
    "third_party/mabel/base64/_base64_rvv.c",
    "third_party/mabel/base85/_base85.c",
    # vector_hash_codec — vendored crypto digests + mabel base16 (unity build:
    # _base16.c #includes the dispatch + per-arch SIMD sources, which self-guard so
    # all variants compile on every platform — b16tobin_len/bintob16 come in via
    # _base16_dispatch.c). Only _base16.c is listed; listing the per-arch files
    # again would double-compile and duplicate symbols.
    "third_party/crypto/md5.cpp",
    "third_party/crypto/sha1.cpp",
    "third_party/crypto/sha2.cpp",
    "third_party/crypto/sha512.cpp",
    "third_party/mabel/base16/_base16.c",
    # vector_json — yyjson (compiled as C11 by build_extension's .c handling)
    "third_party/yyjson/src/yyjson.c",
    # vector_string_case — SIMD string ops (+ cpu_features dep)
    "src/cpp/simd_string_ops.cpp",
    "draken/simd/cpu_features.cpp",
]
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vectors",
        sources=(
            ["opteryx/compiled/nanobind/_vectors_module.cpp"]
            + _vectors_op_cpp
            + _vectors_extra_sources
            + [
                "draken/core/vector_alloc.cpp",
                "third_party/nanobind/src/nb_combined.cpp",
            ]
            + sorted(  # vector_string_misc2 — re2
                glob.glob("third_party/re2/re2/*.cc")
                + [
                    "third_party/re2/util/strutil.cc",
                    "third_party/re2/util/rune.cc",
                ]
            )
        ),
        include_dirs=include_dirs
        + [
            # mabel base16/base64/base85 include dirs now come from build_common's
            # include_dirs (the C libraries moved to the repo root).
            "third_party/yyjson/src",
            "third_party/usearch/fp16/include",
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

extensions.append(
    Extension(
        "opteryx.compiled.io.disk_reader",
        sources=[
            "src/cpp/disk_reader_native.cpp",
            "src/cpp/disk_io.cpp",
            "src/cpp/directories.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA,
        language="c++",
    )
)

extensions.append(
    Extension(
        "opteryx.types.vectors.vector_math",
        sources=["opteryx/types/vectors/vector_math.pyx"],
        include_dirs=include_dirs + ["third_party/usearch/fp16/include"],
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
        language="c++",
    )
)

extensions.append(
    Extension(
        "opteryx.compiled.io.process_ring",
        sources=[
            "opteryx/compiled/io/process_ring.pyx",
        ],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,  # Pure C, no C++ needed
        extra_link_args=LD_EXTRA,
        language="c",
    )
)

# PCG-backed random string helper
extensions.append(
    Extension(
        "opteryx.compiled.functions.random_helper",
        sources=["opteryx/compiled/functions/random_helper.pyx"],
        include_dirs=include_dirs + ["opteryx/third_party/pcg"],
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
        language="c++",
    )
)


extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_search",
        sources=[
            "src/cpp/vector_search_native.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA,
        language="c++",
    )
)

extensions.append(
    Extension(
        "opteryx.compiled.nanobind.usearch_native",
        sources=[
            "src/cpp/usearch_native.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/usearch/include",
            "third_party/usearch/fp16/include",
            "third_party/usearch/simsimd/include",
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS
        + [
            "-fno-strict-aliasing",
            "-DNB_COMPACT_ASSERTIONS",
            "-DUSEARCH_USE_SIMSIMD=1",
        ],
        extra_link_args=LD_EXTRA,
        language="c++",
    )
)


def _select_onnxruntime_sdk():
    """Locate the ONNX Runtime SDK the optional MiniLM extension links against.

    The SDK is NOT vendored (CLAUDE.md §4 — zero installed dependencies). Whoever wants
    the MiniLM EMBED capability supplies an extracted ONNX Runtime SDK out-of-band and
    points ``OPTERYX_ONNXRUNTIME_HOME`` at it — a directory holding ``include/`` and
    ``lib/``. Returns ``(root, rpath)``; the rpath is the absolute ``lib/`` so the loaded
    extension can ``dlopen`` the shared library at runtime from where it was built.
    """
    home = os.environ.get("OPTERYX_ONNXRUNTIME_HOME", "").strip()
    if not home:
        return None, None
    root = os.path.abspath(os.path.expanduser(home))
    rpath = os.path.join(root, "lib")
    return root, rpath


def _find_onnxruntime_library_path(lib_dir: str) -> str | None:
    """Return a full path to the ONNX Runtime shared library if available."""

    candidates = [
        "libonnxruntime.dylib",
        "libonnxruntime.1.22.0.dylib",
        "libonnxruntime.so",
        "libonnxruntime.so.1",
        "libonnxruntime.so.1.22.0",
    ]
    for candidate in candidates:
        path = os.path.join(lib_dir, candidate)
        if os.path.exists(path):
            return path
    return None


BUILD_EMBEDDINGS = os.environ.get("OPTERYX_BUILD_EMBEDDINGS", "0").lower() in ("1", "true", "yes")

if BUILD_EMBEDDINGS:
    _ort_root, _ort_rpath = _select_onnxruntime_sdk()
    _ort_include = os.path.join(_ort_root, "include") if _ort_root else None
    _ort_lib = os.path.join(_ort_root, "lib") if _ort_root else None
    # Fail loud, not silent: OPTERYX_BUILD_EMBEDDINGS=1 is an explicit request to build the
    # extension. If the out-of-band ONNX Runtime SDK is not where OPTERYX_ONNXRUNTIME_HOME
    # says, do NOT quietly skip the extension (which would surface later as a baffling
    # ImportError) — refuse the build with an actionable message.
    if not (_ort_include and _ort_lib and os.path.exists(_ort_include) and os.path.exists(_ort_lib)):
        raise SystemExit(
            "OPTERYX_BUILD_EMBEDDINGS=1 but the ONNX Runtime SDK was not found. Set "
            "OPTERYX_ONNXRUNTIME_HOME to a locally-obtained, extracted ONNX Runtime SDK "
            "directory containing include/ and lib/ (the SDK is not vendored — see "
            f"CLAUDE.md §4). Looked under: {_ort_root!r}."
        )
    ort_lib_path = _find_onnxruntime_library_path(_ort_lib)
    extra_link = []
    if ort_lib_path:
        # Use direct library path so the linker finds the versioned shared lib.
        extra_link.append(ort_lib_path)
    else:
        # Fallback to search by linker name.
        extra_link.append("-lonnxruntime")

    extensions.append(
        Extension(
            "opteryx.compiled.nanobind.minilm_native",
            sources=[
                "src/cpp/minilm_native.cpp",
                "third_party/nanobind/src/nb_combined.cpp",
            ],
            include_dirs=include_dirs
            + [
                _ort_include,
                "third_party/nanobind",
                "third_party/nanobind/src",
                "third_party/nanobind/ext/robin_map/include",
                # draken/core/fp16.h -> <fp16/fp16.h>: the EMBED capability kernel
                # packs its fp32 rows to fp16 to build a VECTOR_FP16 result.
                "third_party/usearch/fp16/include",
            ],
            extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
            extra_link_args=LD_EXTRA
            + [
                f"-L{_ort_lib}",
            ]
            + extra_link
            + [
                f"-Wl,-rpath,{_ort_rpath}",
            ],
            language="c++",
        )
    )

# C++ Parquet IO pipeline with lock-free queues
extensions.append(
    Extension(
        "opteryx.connectors.parquet_io.pool_reader",
        sources=(
            [
                "opteryx/connectors/parquet_io/pool_reader.pyx",
                # MemoryPool implementation — pool_reader calls
                # opteryx::MemoryPool::reserve_for_write (via pool_sink_adapter.hpp).
                # Must be compiled into this extension: on Linux extensions load
                # RTLD_LOCAL, so the symbol cannot be borrowed from memory_pool.so
                # (macOS only resolves it by flat-namespace dynamic_lookup).
                "src/cpp/memory_pool.cpp",
                # Rugo parquet sources for DecodeColumnFromChunk and infrastructure
                "rugo/src/parquet/decode_column.cpp",
                "rugo/src/parquet/decode.cpp",
                "rugo/src/parquet/compression.cpp",
                # miniz raw-DEFLATE inflate for the parquet GZIP codec.
                "third_party/miniz/miniz_tinfl.cpp",
                "rugo/src/parquet/metadata.cpp",
                "rugo/src/parquet/bloom_filter.cpp",
                "rugo/src/parquet/decode_encodings.cpp",
                "rugo/src/parquet/decode_page.cpp",
                "draken/simd/cpu_features.cpp",
                "src/cpp/http_client.cpp",
            ]
            + get_parquet_vendor_sources()
            + get_lz4_vendor_sources()  # lz4.c: LZ4_RAW block decode (parquet codec 7)
        ),
        include_dirs=(
            include_dirs
            + [
                "src/cpp",
                "rugo/src/parquet",
                "third_party/snappy",
                "third_party/zstd",
                "third_party/zstd/common",
                "third_party/zstd/decompress",
                "third_party/lz4",              # lz4.h
                "third_party/miniz",            # miniz_tinfl.h / miniz.h
                "third_party/bshoshany",
                "third_party/moodycamel",
            ]
            + _curl_include_dirs
        ),
        define_macros=[("HAVE_SNAPPY", "1"), ("HAVE_ZSTD", "1"), ("ZSTD_STATIC_LINKING_ONLY", "1"), ("HAVE_CONFIG_H", "1"), ("RUGO_ENABLE_HTTP", "1")],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=_curl_link_args + ([] if is_win() else ["-lm"]),
        depends=[
            "rugo/src/parquet/io_pipeline.hpp",
            "rugo/src/parquet/ipc_serialize.hpp",
            "rugo/src/parquet/decode.hpp",
            "rugo/src/parquet/metadata.hpp",
            "src/cpp/http_client.hpp",
            "src/cpp/pool_sink_adapter.hpp",
            "src/cpp/memory_pool.hpp",
            "draken/core/alloc.h",
        ],
    )
)

def discover_packages():
    """Discover packages, including those whose __init__ is a compiled .pyx.

    setuptools.find_packages only treats a directory as a package when it
    contains a literal __init__.py. This project compiles many __init__.pyx
    into __init__.so, so those directories are invisible to find_packages —
    which means find_packages cannot descend into them and any *pure-Python*
    file living in (or under) such a directory is silently omitted from the
    wheel, even though the compiled .so ships via ext_modules. That is how
    opteryx/expression/evaluator/__init__.py went missing.

    We start from find_packages and additionally add any directory under the
    owned roots that is a package by virtue of an __init__.pyx, so pure-Python
    siblings (e.g. evaluator/__init__.py, functions/implementations/*.py) ship.
    """
    base = set(
        find_packages(
            include=[LIBRARY, f"{LIBRARY}.*", "draken", "draken.*", "rugo", "rugo.*",
                     "skene", "skene.*"],
            exclude=["draken.tests", "draken.tests.*", "rugo.tests", "rugo.tests.*",
                     "skene.tests", "skene.tests.*"],
        )
    )
    for root in (LIBRARY, "draken", "rugo", "skene"):
        for dirpath, _dirnames, filenames in os.walk(root):
            parts = dirpath.split(os.sep)
            if "tests" in parts:
                continue
            if "__init__.pyx" in filenames or "__init__.py" in filenames:
                base.add(".".join(parts))
    return sorted(base)


# Stamp the draken ABI surface BEFORE setup() collects packages, so the generated
# modules ship in the wheel. All THREE consumers are named because this wheel
# bundles rugo and skene as well as opteryx (see discover_packages above) — a
# bundled package without its generated module would not import at all. Each
# package's __init__ calls the generated check; see the "draken ABI stamp"
# section of build_common.py for what it defends against.
print(f"draken ABI stamp: {write_draken_abi_modules('opteryx', 'rugo', 'skene')}")


# Setup configuration
setup(
    name=LIBRARY,
    version=__version__,
    description="Python SQL Query Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=discover_packages(),
    python_requires=">=3.11",
    url="https://github.com/mabel-dev/opteryx/",
    ext_modules=cythonize(
        # DRAKEN_BUILD=1 builds everything except extensions that still
        # reference old-draken typed-vector cimports (Gap 2):
        #   - opteryx.operators._operators (whole operators bundle)
        (
            [
                e
                for e in extensions
                if e.name
                not in {
                    "opteryx.operators._operators",
                    "opteryx.compiled.structures.column_deserializer",
                }
            ]
            if os.environ.get("DRAKEN_BUILD")
            else extensions
        ),
        compiler_directives={
            "language_level": "3",
            "linetrace": "a" in __version__ or "b" in __version__,
            # Declare every Cython module free-threading-safe so importing it does
            # not force the GIL back on under a free-threaded (PEP 703) CPython.
            # This is a DECLARATION only: it asserts the module's C code is safe to
            # run without the GIL — it does not make module-level globals/caches
            # thread-safe. Gated on the build interpreter actually being
            # free-threaded so GIL builds are unaffected.
            "freethreading_compatible": FREE_THREADED_BUILD,
        },
    ),
    rust_extensions=[]
    if _DRAKEN_BUILD
    else [RustExtension("opteryx.compute", "Cargo.toml", debug=False)],
    package_data={
        "": ["*.pyx", "*.pxd", "*.h"],
        # Standalone mimalloc preload lib built by build_common (see
        # draken.preload_library_path); ships in the wheel, linked into nothing.
        "draken": ["libmimalloc.so", "libmimalloc.dylib"],
    },
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
