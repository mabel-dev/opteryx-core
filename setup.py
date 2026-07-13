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
    include_dirs,
    is_linux,
    is_mac,
    is_win,
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


def resolve_libcurl():
    """Return (include_dirs, link_args) for libcurl, preferring system over vendored.

    Resolution order:
      1. OPTERYX_VENDOR_CURL=1 → force vendored static (CI/wheel builds).
      2. System libcurl via pkg-config → use it (fast, reliable for local dev).
      3. Vendored static build → fallback.
    Hard-fails if none succeed; the http_client extension is mandatory.
    """
    force_vendor = os.environ.get("OPTERYX_VENDOR_CURL", "0").lower() in ("1", "true", "yes")

    if not force_vendor:
        sys_curl = detect_system_libcurl()
        if sys_curl is not None:
            sys_inc, sys_libs = sys_curl
            print(f"Using system libcurl (pkg-config): {' '.join(sys_libs)}")
            return sys_inc, sys_libs

    # Fall back to vendored static build
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


# Define all extensions
extensions = [
    *draken_rugo_extensions(
        parquet_created_by="opteryx-rugo version %s (build %s)"
        % (__version__, __build__)
    ),
    # Third-party libraries
    Extension(
        "opteryx.third_party.mabel.base64",
        sources=[
            "opteryx/third_party/mabel/base64/base64.pyx",
            "opteryx/third_party/mabel/base64/_base64.c",
            "opteryx/third_party/mabel/base64/_base64_dispatch.c",
            "opteryx/third_party/mabel/base64/_base64_neon.c",
            "opteryx/third_party/mabel/base64/_base64_avx2.c",
            "opteryx/third_party/mabel/base64/_base64_rvv.c",
        ],
        include_dirs=include_dirs + ["opteryx/third_party/mabel"],
        extra_compile_args=C_FLAGS + ["-std=c99", "-DBASE64_IMPLEMENTATION"],
    ),
    Extension(
        "opteryx.third_party.mabel.base16",
        sources=[
            "opteryx/third_party/mabel/base16/base16.pyx",
            # Unity build — _base16.c #includes the dispatch + per-arch SIMD sources.
            "opteryx/third_party/mabel/base16/_base16.c",
        ],
        include_dirs=include_dirs + ["opteryx/third_party/mabel"],
        extra_compile_args=C_FLAGS + ["-std=c99"],
    ),
    Extension(
        "opteryx.third_party.mabel.base85",
        sources=[
            "opteryx/third_party/mabel/base85/base85.pyx",
            "opteryx/third_party/mabel/base85/_base85.c",
        ],
        include_dirs=include_dirs + ["opteryx/third_party/mabel"],
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
            "src/cpp/cpu_features.cpp",
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
            "src/cpp/cpu_features.cpp",
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
            "src/cpp/simd_env.cpp",
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
            "src/cpp/cpu_features.cpp",
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
        "opteryx.compiled.structures.carchar_index",
        sources=[
            "opteryx/compiled/structures/carchar_index.pyx",
            "src/cpp/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        depends=[
            "third_party/mabel/carchar/carchar_join_index.hpp",
            "third_party/mabel/carchar/carchar_index.hpp",
            "third_party/mabel/carchar/carchar_common.hpp",
            "third_party/mabel/carchar/carchar_simd.hpp",
        ],
    ),
    Extension(
        "opteryx.compiled.structures.parvi_index",
        sources=[
            "opteryx/compiled/structures/parvi_index.pyx",
            "src/cpp/cpu_features.cpp",
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
        "opteryx.compiled.structures.parvi_set",
        sources=[
            "opteryx/compiled/structures/parvi_set.pyx",
            "src/cpp/cpu_features.cpp",
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
    # operations/__init__ includes special_ops.pyx which cimports yyjson at C level.
    # On Linux (RTLD_LOCAL), cyyjson.so symbols are not visible to other extensions,
    # so yyjson.c must be compiled directly into this extension.
    Extension(
        "opteryx.expression.operations.__init__",
        sources=[
            "opteryx/expression/operations/__init__.pyx",
            "third_party/yyjson/src/yyjson.c",
        ],
        include_dirs=include_dirs + ["third_party/yyjson/src"],
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
        "opteryx.compiled.structures.bloom_filter",
        sources=["opteryx/compiled/structures/bloom_filter.pyx"],
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
    # Leaf .pyx files are textually included by _impl.pyx. json_ops.pyx cimports
    # yyjson at C level; on Linux (RTLD_LOCAL) cyyjson.so's symbols are not
    # visible to this extension, so yyjson.c must be compiled directly in (same
    # as operations/__init__ above).
    Extension(
        "opteryx.expression.evaluator._impl",
        sources=[
            "opteryx/expression/evaluator/_impl.pyx",
            "opteryx/expression/evaluator/bytecode_worker.cpp",
            "third_party/yyjson/src/yyjson.c",
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
        ],
        include_dirs=include_dirs
        + [
            "opteryx/operators/aggregate",
        ],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA
        + (["-undefined", "dynamic_lookup"] if is_mac() else ["-Wl,--allow-shlib-undefined"]),
        depends=[
            "third_party/mabel/parvi/parvi.hpp",
            "third_party/mabel/carchar/carchar_index.hpp",
            "third_party/mabel/carchar/carchar_common.hpp",
            "third_party/mabel/carchar/carchar_simd.hpp",
            "src/cpp/operators/loop_join_kernels.hpp",
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
    Extension(
        "opteryx.compiled.morsel_ops.sort",
        sources=[
            "opteryx/compiled/morsel_ops/sort.pyx",
            "src/cpp/simd_remap.cpp",
            "src/cpp/simd_env.cpp",
            "src/cpp/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
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
    Extension(
        name="opteryx.compiled.morsel_queue",
        sources=["opteryx/compiled/morsel_queue.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=["-O3", "-std=c++17"] + WARNING_FLAGS,
        language="c++",
    ),
    # HTTP Client (libcurl-based HTTP with connection pooling and Range request support)
]

# Resolve libcurl - REQUIRED for http_client extension
# Skip for sdist (source distribution packaging) and clean - no compilation needed
# Skip when DRAKEN_BUILD=1 — draken-only builds don't need libcurl.
_DRAKEN_BUILD = bool(os.environ.get("DRAKEN_BUILD"))
_build_commands = {"build", "build_ext", "install", "bdist_wheel", "bdist", "develop"}
_skip_build = not any(
    arg.lower() in _build_commands for arg in sys.argv[1:] if arg and not arg.startswith("-")
)
_curl_include_dirs: list[str] = []
_curl_link_args: list[str] = []
if not _skip_build and not _DRAKEN_BUILD:
    _curl_include_dirs, _curl_link_args = resolve_libcurl()

    # HTTP client extension - MANDATORY (only add if not cleaning)
    extensions.append(
        Extension(
            name="opteryx.compiled.http_client",
            sources=[
                "opteryx/compiled/http_client.pyx",
                "src/cpp/http_client.cpp",
            ],
            include_dirs=include_dirs + ["src/cpp"] + _curl_include_dirs,
            extra_compile_args=["-O3", "-std=c++17"] + WARNING_FLAGS,
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
                        "src/cpp/simd_env.cpp",
                        "src/cpp/simd_search.cpp",
                        "src/cpp/simd_datepart.cpp",
                        "src/cpp/simd_string_ops.cpp",
                        "src/cpp/cpu_features.cpp",
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
    "opteryx/third_party/mabel/base64/_base64.c",
    "opteryx/third_party/mabel/base64/_base64_dispatch.c",
    "opteryx/third_party/mabel/base64/_base64_neon.c",
    "opteryx/third_party/mabel/base64/_base64_avx2.c",
    "opteryx/third_party/mabel/base64/_base64_rvv.c",
    "opteryx/third_party/mabel/base85/_base85.c",
    # vector_hash_codec — vendored crypto digests + mabel base16 (unity build:
    # _base16.c #includes the dispatch + per-arch SIMD sources, which self-guard so
    # all variants compile on every platform — b16tobin_len/bintob16 come in via
    # _base16_dispatch.c). Only _base16.c is listed; listing the per-arch files
    # again would double-compile and duplicate symbols.
    "third_party/crypto/md5.cpp",
    "third_party/crypto/sha1.cpp",
    "third_party/crypto/sha2.cpp",
    "third_party/crypto/sha512.cpp",
    "opteryx/third_party/mabel/base16/_base16.c",
    # vector_json — yyjson (compiled as C11 by build_extension's .c handling)
    "third_party/yyjson/src/yyjson.c",
    # vector_string_case — SIMD string ops (+ cpu_features dep)
    "src/cpp/simd_string_ops.cpp",
    "src/cpp/cpu_features.cpp",
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
            "opteryx/third_party/mabel/base64",
            "opteryx/third_party/mabel/base85",
            "opteryx/third_party/mabel/base16",
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
        "opteryx.compiled.nanobind.carchar_native",
        sources=[
            "src/cpp/carchar_native.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/mabel/carchar",
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
    if is_mac() and arch == "aarch64":
        root = "third_party/onnxruntime/onnxruntime-osx-arm64-1.22.0"
        rpath = "@loader_path/../../../third_party/onnxruntime/onnxruntime-osx-arm64-1.22.0/lib"
        return root, rpath
    if is_linux() and arch == "x86_64":
        root = "third_party/onnxruntime/onnxruntime-linux-x64-1.22.0"
        rpath = r"$ORIGIN/../../../third_party/onnxruntime/onnxruntime-linux-x64-1.22.0/lib"
        return root, rpath
    return None, None


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
    if _ort_include and _ort_lib and os.path.exists(_ort_include) and os.path.exists(_ort_lib):
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
                "src/cpp/cpu_features.cpp",
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
            include=[LIBRARY, f"{LIBRARY}.*", "draken", "draken.*", "rugo", "rugo.*"],
            exclude=["draken.tests", "draken.tests.*", "rugo.tests", "rugo.tests.*"],
        )
    )
    for root in (LIBRARY, "draken", "rugo"):
        for dirpath, _dirnames, filenames in os.walk(root):
            parts = dirpath.split(os.sep)
            if "tests" in parts:
                continue
            if "__init__.pyx" in filenames or "__init__.py" in filenames:
                base.add(".".join(parts))
    return sorted(base)


# Setup configuration
setup(
    name=LIBRARY,
    version=__version__,
    description="Python SQL Query Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=discover_packages(),
    python_requires=">=3.13",
    url="https://github.com/mabel-dev/opteryx/",
    ext_modules=cythonize(
        # DRAKEN_BUILD=1 builds everything except extensions that still
        # reference old-draken typed-vector cimports (Gap 2):
        #   - opteryx.operators._operators (whole operators bundle)
        #   - opteryx.compiled.morsel_ops.sort (cimports StringVector)
        (
            [
                e
                for e in extensions
                if e.name
                not in {
                    "opteryx.operators._operators",
                    "opteryx.compiled.morsel_ops.sort",
                    "opteryx.compiled.structures.column_deserializer",
                    "opteryx.compiled.structures.bloom_filter",
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
