"""
Simplified setup script for Opteryx - builds all Cython extensions and Rust module.
"""

import glob
import os
import platform
import re
import sys
import threading

from Cython.Build import cythonize
from setuptools import Extension, find_packages, setup
from setuptools.command.build_ext import build_ext as build_ext_orig
from setuptools_rust import RustExtension

LIBRARY = "opteryx"


# Thread-local storage so each parallel ThreadPool worker in build_extensions
# sees its own per-extension build_temp without clobbering other threads.
_build_temp_local = threading.local()


class build_ext(build_ext_orig):
    """build_ext subclass that isolates each extension's object files into a
    unique subdirectory of build_temp.

    build_temp is backed by threading.local so parallel workers never see
    each other's value between the set and the first read inside
    build_ext_orig.build_extension() -> compiler.compile(output_dir=...).

    All source-file subdirectories are pre-created before compilation starts
    so that macOS clang's atomic-rename strategy (.tmp -> .o) never encounters
    a missing target directory.
    """

    @property
    def build_temp(self):
        # Per-thread override takes priority; fall back to the base value that
        # finalize_options / the setuptools machinery wrote on the main thread.
        return getattr(_build_temp_local, "value", None) or self.__dict__.get(
            "_build_temp_base", ""
        )

    @build_temp.setter
    def build_temp(self, value):
        # Keep both the thread-local and a shared base so the main-thread /
        # non-parallel code paths continue to work correctly.
        _build_temp_local.value = value
        self.__dict__["_build_temp_base"] = value

    def build_extensions(self):
        if self.compiler and ".S" not in self.compiler.src_extensions:
            self.compiler.src_extensions.append(".S")

        # Pre-compile yyjson.c as C code before C++ extensions need it
        import shutil
        import subprocess

        # Resolve C compiler: respect CC environment variable, otherwise prefer clang/gcc/cc from PATH.
        # Fall back to "cc" as a last resort so CI that uses e.g. "cc" or has CC set will work.
        compiler = (
            os.environ.get("CC")
            or shutil.which("clang")
            or shutil.which("gcc")
            or shutil.which("cc")
            or "cc"
        )

        os.makedirs("build/temp", exist_ok=True)
        yyjson_obj = "build/temp.yyjson.o"
        yyjson_src = "third_party/yyjson/src/yyjson.c"
        if not os.path.exists(yyjson_obj) or os.path.getmtime(yyjson_src) > os.path.getmtime(
            yyjson_obj
        ):
            print(f"Pre-compiling {yyjson_src} to {yyjson_obj} using compiler: {compiler}")
            result = subprocess.run(
                [
                    compiler,
                    "-O3",
                    "-std=c11",
                    "-Wno-unused-function",
                    "-Ithird_party/yyjson/src",
                    "-c",
                    yyjson_src,
                    "-o",
                    yyjson_obj,
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                print(f"Error compiling yyjson with {compiler}: {result.stderr}")
            else:
                print(f"Successfully compiled yyjson to {yyjson_obj}")

        # libcurl is already built at module initialization time
        super().build_extensions()

    def build_extension(self, ext):
        # Derive a stable per-extension subdirectory from the shared base so
        # that object files from different extensions never collide.
        orig_base = self.__dict__.get("_build_temp_base", "") or ""
        safe_name = ext.name.replace(".", "_")
        per_ext_build_temp = os.path.join(orig_base, safe_name)
        os.makedirs(per_ext_build_temp, exist_ok=True)

        # Pre-create every source-mirrored subdirectory so macOS clang's
        # atomic rename (.tmp -> .o) never encounters a missing directory.
        for src in ext.sources:
            subdir = os.path.join(per_ext_build_temp, os.path.dirname(src))
            if subdir:
                os.makedirs(subdir, exist_ok=True)

        # Point this thread's build_temp at the per-extension directory.
        # Other threads each maintain their own value and are unaffected.
        prev = getattr(_build_temp_local, "value", None)
        _build_temp_local.value = per_ext_build_temp
        try:
            if is_linux() and getattr(ext, "language", "") == "c++":
                ext.extra_link_args = list(getattr(ext, "extra_link_args", [])) + LD_EXTRA
            super().build_extension(ext)
        finally:
            _build_temp_local.value = prev


# Platform detection
def is_mac():
    return platform.system() == "Darwin"


def is_win():
    return platform.system() == "Windows"


def is_linux():
    return platform.system() == "Linux"


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


# Skip extension building for clean command
if "clean" in [arg.lower() for arg in sys.argv[1:] if arg and not arg.startswith("-")]:
    print("Skipping native extension build for clean command")
    sys.exit(0)


# Architecture detection for SIMD
def detect_architecture():
    machine = platform.machine().lower()
    # Respect environment ARCHFLAGS when cross-compiling or building universal
    # wheels (e.g., ``-arch x86_64 -arch arm64``). This makes the build
    # deterministic wrt which arch-specific sources to include.
    archflags = os.environ.get("ARCHFLAGS", "").lower()
    has_x86 = "x86_64" in archflags
    has_arm = "arm64" in archflags or "aarch64" in archflags
    if has_x86 and not has_arm:
        return "x86_64"
    if has_arm and not has_x86:
        return "aarch64"
    if has_x86 and has_arm:
        # Universal build - don't infer a single architecture. Leave
        # autodetection using the host platform below, which avoids
        # including arch-specific assembly for both targets in a single
        # compile invocation.
        pass
    # Distinguish between 32-bit ARM (arm/armv7) and 64-bit ARM (aarch64/arm64)
    if "aarch64" in machine or "arm64" in machine:
        return "aarch64"
    if "arm" in machine:
        return "arm"
    if "x86" in machine or "amd64" in machine:
        return "x86_64"
    return machine


arch = detect_architecture()
CPP_FLAGS = ["-O3", "-std=c++20"]
C_FLAGS = ["-O3", "-std=c11"]  # C11 required for xxhash _Alignas support

# Optional build-time optimizations (LTO / PGO) are gated by environment
# variables so CI/release automation can enable them without changing
# developer's local build defaults.
OPTERYX_ENABLE_LTO = os.environ.get("OPTERYX_ENABLE_LTO", "0").lower() in ("1", "true", "yes")
OPTERYX_ENABLE_PGO = os.environ.get("OPTERYX_ENABLE_PGO", "0").lower() in ("1", "true", "yes")
OPTERYX_PGO_PHASE = os.environ.get("OPTERYX_PGO_PHASE", "generate").lower()  # 'generate' or 'use'

if is_win():
    CPP_FLAGS = ["/O2", "/std:c++20"]
    C_FLAGS = ["/O2"]  # MSVC supports C11 by default in modern versions
    # MSVC LTO (link-time code generation)
    if OPTERYX_ENABLE_LTO:
        CPP_FLAGS.append("/GL")
        # linker flag /LTCG will be added via extra_link_args when needed
elif is_linux():
    CPP_FLAGS.append("-fvisibility=default")
    C_FLAGS.append("-fvisibility=default")

# Enable LTO for non-Windows when requested
if OPTERYX_ENABLE_LTO and not is_win():
    CPP_FLAGS.append("-flto")
    C_FLAGS.append("-flto")
    # ensure linker uses LTO as well
    LD_EXTRA = list(LD_EXTRA) if "LD_EXTRA" in globals() else []
    LD_EXTRA.append("-flto")

# PGO support (opt-in). The CI/release pipeline may run a profile-generate
# build followed by exercising the binary and then a profile-use rebuild.
if OPTERYX_ENABLE_PGO and not is_win():
    if OPTERYX_PGO_PHASE == "generate":
        CPP_FLAGS.append("-fprofile-generate")
        C_FLAGS.append("-fprofile-generate")
    elif OPTERYX_PGO_PHASE == "use":
        CPP_FLAGS.append("-fprofile-use")
        CPP_FLAGS.append("-fprofile-correction")
        C_FLAGS.append("-fprofile-use")
        C_FLAGS.append("-fprofile-correction")


# On Linux builds (manylinux) prefer static linking of libstdc++/libgcc to avoid
# runtime dependency on host-provided newer libstdc++ which can require
# GLIBCXX/GLIBC versions not available on older manylinux targets.
# macOS/Clang does not support -static-libgcc
LD_EXTRA = ["-static-libstdc++"] if is_mac() else ["-static-libstdc++", "-static-libgcc"]

# MSVC LTO linker flag when requested
if is_win() and OPTERYX_ENABLE_LTO:
    # '/LTCG' enables link-time code generation on MSVC
    LD_EXTRA.append("/LTCG")

# SIMD-specific flags (deterministic baseline to avoid host-specific AVX512/etc.)
if arch == "x86_64":
    CPP_FLAGS.extend(["-msse4.2", "-mavx2", "-march=haswell"])
    C_FLAGS.extend(["-msse4.2", "-mavx2", "-march=haswell"])
elif arch == "arm" and not is_mac():
    # 32-bit ARM needs explicit NEON; AArch64 already guarantees it.
    CPP_FLAGS.append("-mfpu=neon")

# Common warning suppressions
WARNING_FLAGS = [
    "-Wno-unused-function",
    "-Wno-unreachable-code-fallthrough",
    "-Wno-sign-compare",
    "-Wno-unused-command-line-argument",
]
CPP_FLAGS.extend(WARNING_FLAGS)
C_FLAGS.extend(WARNING_FLAGS)

# Include directories
include_dirs = [
    "src/cpp",
    "src/c",
    "third_party/mabel/draken",
    "third_party/mabel/carchar",
    "third_party/mabel/parvi",
    "third_party/fastfloat",
    "third_party/fastfloat/fast_float",
    "third_party/mabel/rugo/parquet",
    "third_party/yyjson/src",
    "third_party/re2",
    "third_party/cyan4973",
    "third_party/tdigest-c/src",
    "third_party/ulfjack/ryu",
    "third_party/nanobind",
    "third_party/crypto",
    "third_party/bshoshany",
    "third_party/moodycamel",
]

# Common SIMD / environment C++ sources used by multiple extensions
COMMON_SIMD_SOURCES = [
    "src/cpp/simd_env.cpp",
    "src/cpp/cpu_features.cpp",
    "src/cpp/simd_search.cpp",
]

# Read version and metadata
with open(f"{LIBRARY}/__version__.py", "r") as v:
    content = v.read()
    for match in re.finditer(r'^(__\w+__)\s*=\s*["\']?([^"\']+)["\']?$', content, re.MULTILINE):
        var_name, var_value = match.groups()
        globals()[var_name] = var_value

with open("README.md", "r", encoding="UTF8") as f:
    long_description = f.read()

    # Helper for draken extensions


def make_draken_extension(module_path, source_file, language="c++", depends=None):
    if depends is None:
        depends = ["third_party/mabel/draken/core/buffers.h"]

    sources = [f"third_party/mabel/draken/{source_file}"]
    # Include SIMD implementations for all draken vector modules so
    # simd_mix_hash, simd_popcount, and related functions are available at link time.
    for s in ("src/cpp/simd_hash.cpp", "src/cpp/simd_bitops.cpp"):
        if s not in sources:
            sources.append(s)

    # Common SIMD/environment sources - CPU features and SIMDs
    for s in ("src/cpp/simd_env.cpp", "src/cpp/cpu_features.cpp", "src/cpp/simd_search.cpp"):
        if s not in sources:
            sources.append(s)

    return Extension(
        name=f"opteryx.compiled.draken.{module_path}",
        sources=sources,
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS if language == "c++" else C_FLAGS,
        extra_link_args=LD_EXTRA if language == "c++" else [],
        language=language,
        depends=depends,
    )


def get_zstd_vendor_sources():
    """Return the vendored zstd sources so other extensions can link to the same files."""
    RUGO_PARQUET = "third_party/mabel/rugo/parquet"
    sources = [
        f"{RUGO_PARQUET}/vendor/zstd/common/entropy_common.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/fse_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/zstd_common.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/xxhash.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/common/error_private.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_decompress_block.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/huf_decompress.cpp",
        f"{RUGO_PARQUET}/vendor/zstd/decompress/zstd_ddict.cpp",
    ]
    machine = detect_architecture()
    if machine in ("x86_64", "amd64"):
        sources.append(f"{RUGO_PARQUET}/vendor/zstd/decompress/huf_decompress_amd64.S")
    return sources


def get_lz4_vendor_sources():
    """Return vendored lz4 block-codec sources."""
    RUGO_PARQUET = "third_party/mabel/rugo/parquet"
    return [f"{RUGO_PARQUET}/vendor/lz4/lz4.c"]


def get_parquet_vendor_sources():
    """Return vendored zstd/snappy source files to build into parquet extension.

    We only compile the decompression bits (zstd) and minimal snappy sources we
    need for decompression. The vendor code is included inside the project, so
    building them into the extension avoids linking to system libraries and
    avoids runtime missing symbol errors.
    """
    vendor_sources = []
    RUGO_PARQUET = "third_party/mabel/rugo/parquet"

    # Snappy sources (minimal subset for decompress)
    snappy_sources = [
        f"{RUGO_PARQUET}/vendor/snappy/snappy.cc",
        f"{RUGO_PARQUET}/vendor/snappy/snappy-sinksource.cc",
        f"{RUGO_PARQUET}/vendor/snappy/snappy-stubs-internal.cc",
    ]
    vendor_sources.extend(snappy_sources)

    # Zstd decompression sources
    vendor_sources.extend(get_zstd_vendor_sources())
    return vendor_sources


# Link args for parquet extension - ensure libcrypto is linked on Linux so
# the runtime 'ldd' check in CI can verify its presence. Don't add -lcrypto
# on macOS where the system library naming differs.
parquet_link_args = []
if not is_mac():
    # Ensure libcrypto is added to the DT_NEEDED entries of the shared
    # object even if no symbols are referenced (CI asserts its presence).
    parquet_link_args.extend(["-Wl,--no-as-needed", "-lcrypto", "-Wl,--as-needed"])

# Define all extensions
extensions = [
    # Third-party libraries
    Extension(
        "opteryx.third_party.mabel.base64",
        sources=[
            "opteryx/third_party/mabel/base64/base64.pyx",
            "opteryx/third_party/mabel/base64/_base64.c",
            "opteryx/third_party/mabel/base64/_base64_dispatch.c",
            "opteryx/third_party/mabel/base64/_base64_neon.c",
            "opteryx/third_party/mabel/base64/_base64_avx2.c",
            "opteryx/third_party/mabel/base64/_base64_avx512.c",
        ],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS + ["-std=c99", "-DBASE64_IMPLEMENTATION"],
    ),
    Extension(
        "opteryx.third_party.mabel.base16",
        sources=[
            "opteryx/third_party/mabel/base16/base16.pyx",
            "opteryx/third_party/mabel/base16/_base16.c",
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
        "opteryx.third_party.fastfloat.fast_float",
        sources=["opteryx/third_party/fastfloat/fast_float.pyx"],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
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
        "opteryx.third_party.facebook.zstd",
        sources=["opteryx/third_party/facebook/zstd.pyx"] + get_zstd_vendor_sources(),
        include_dirs=include_dirs
        + [
            "third_party/mabel/rugo/parquet/vendor/zstd",
            "third_party/mabel/rugo/parquet/vendor/zstd/common",
            "third_party/mabel/rugo/parquet/vendor/zstd/decompress",
        ],
        define_macros=[("ZSTD_STATIC_LINKING_ONLY", "1")],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    Extension(
        "opteryx.third_party.lz4.lz4",
        sources=["opteryx/third_party/lz4/lz4.pyx"] + get_lz4_vendor_sources(),
        include_dirs=include_dirs + ["third_party/mabel/rugo/parquet/vendor/lz4"],
        extra_compile_args=C_FLAGS,
        language="c",
    ),
    Extension(
        "opteryx.third_party.ulfjack.ryu",
        sources=["opteryx/third_party/ulfjack/ryu.pyx", "third_party/ulfjack/ryu/d2fixed.c"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        name="opteryx.third_party.fuzzy",
        sources=["opteryx/third_party/fuzzy/soundex.pyx"],
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        name="opteryx.third_party.mbleven",
        sources=["opteryx/third_party/mbleven.pyx"],
        extra_compile_args=C_FLAGS,
    ),
    # High-performance distogram for cost-based optimization
    Extension(
        name="opteryx.third_party.maki_nage.distogram",
        sources=["opteryx/third_party/maki_nage/distogram.pyx"],
        extra_compile_args=C_FLAGS,
    ),
    # File format readers
    Extension(
        "opteryx.compiled.rugo.parquet",
        sources=(
            [
                "third_party/mabel/rugo/parquet/parquet_reader.pyx",
                "third_party/mabel/rugo/parquet/metadata.cpp",
                "third_party/mabel/rugo/parquet/decode_encodings.cpp",
                "third_party/mabel/rugo/parquet/decode_page.cpp",
                "third_party/mabel/rugo/parquet/decode_column.cpp",
                "third_party/mabel/rugo/parquet/decode.cpp",
                "third_party/mabel/rugo/parquet/page_value_decoder.cpp",
                "third_party/mabel/rugo/parquet/compression.cpp",
                "third_party/mabel/rugo/parquet/bloom_filter.cpp",
                "src/cpp/cpu_features.cpp",
            ]
            + get_parquet_vendor_sources()
        ),
        include_dirs=(
            include_dirs
            + [
                "third_party/mabel/rugo/parquet/vendor/snappy",
                "third_party/mabel/rugo/parquet/vendor/zstd",
                "third_party/mabel/rugo/parquet/vendor/zstd/common",
                "third_party/mabel/rugo/parquet/vendor/zstd/decompress",
            ]
        ),
        define_macros=[("HAVE_SNAPPY", "1"), ("HAVE_ZSTD", "1"), ("ZSTD_STATIC_LINKING_ONLY", "1")],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=parquet_link_args + LD_EXTRA,
    ),
    Extension(
        "opteryx.compiled.rugo.jsonl",
        sources=[
            "third_party/mabel/rugo/jsonl/jsonl_reader.pyx",
            "third_party/mabel/rugo/jsonl/decode.cpp",
            "third_party/mabel/rugo/jsonl/yyjson_wrapper.cpp",
            "src/cpp/simd_env.cpp",
            "src/cpp/cpu_features.cpp",
            "src/cpp/simd_search.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_objects=["build/temp.yyjson.o"] if os.path.exists("build/temp.yyjson.o") else [],
    ),
    Extension(
        "opteryx.compiled.rugo._jsonl",
        sources=[
            "third_party/mabel/rugo/_jsonl/_jsonl_reader.pyx",
            "third_party/mabel/rugo/_jsonl/core/structural_scan.cpp",
            "third_party/mabel/rugo/_jsonl/core/interpreter.cpp",
            "third_party/mabel/rugo/_jsonl/core/value_parser.cpp",
            "third_party/mabel/rugo/_jsonl/core/field_span.cpp",
            "third_party/mabel/rugo/_jsonl/core/jsonl_reader.cpp",
            "third_party/mabel/rugo/_jsonl/core/column_builder.cpp",
            "src/cpp/simd_env.cpp",
            "src/cpp/cpu_features.cpp",
            "src/cpp/simd_search.cpp",
        ],
        include_dirs=include_dirs + ["third_party/mabel/rugo/_jsonl/core"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    # Draken core components
    make_draken_extension("interop.arrow", "interop/arrow.pyx"),
    make_draken_extension("interop.vector_sequence", "interop/vector_sequence.pyx"),
    make_draken_extension("vectors.vector", "vectors/vector.pyx"),
    make_draken_extension("vectors.bool_vector", "vectors/bool_vector.pyx"),
    make_draken_extension("vectors.float64_vector", "vectors/float64_vector.pyx"),
    make_draken_extension("vectors.array_vector", "vectors/array_vector.pyx"),
    make_draken_extension("vectors.vector_vector", "vectors/vector_vector.pyx"),
    make_draken_extension("vectors.time_vector", "vectors/time_vector.pyx"),
    make_draken_extension("vectors.interval_vector", "vectors/interval_vector.pyx"),
    make_draken_extension("vectors.scalar_constructors", "vectors/scalar_constructors.pyx"),
    Extension(
        "opteryx.compiled.draken.vectors.arithmetic_kernels",
        sources=["opteryx/compiled/draken/vectors/arithmetic_kernels.pyx"],
        include_dirs=include_dirs,
        language="c",
        extra_compile_args=C_FLAGS,
    ),
    make_draken_extension("vectors.int64_vector", "vectors/int64_vector.pyx", language="c++"),
    make_draken_extension("vectors.integer_vector", "vectors/integer_vector.pyx", language="c++"),
    Extension(
        "opteryx.compiled.draken.vectors.string_vector",
        sources=[
            "third_party/mabel/draken/vectors/string_vector.pyx",
            "src/cpp/simd_hash.cpp",
            "src/cpp/simd_bitops.cpp",
            "src/cpp/simd_env.cpp",
            "src/cpp/simd_search.cpp",
            "src/cpp/simd_string_ops.cpp",
            "src/cpp/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        define_macros=[("XXH_INLINE_ALL", "1")],
        extra_compile_args=CPP_FLAGS,
        language="c++",
    ),
    make_draken_extension("vectors.date32_vector", "vectors/date32_vector.pyx"),
    make_draken_extension("vectors._decimal_vector", "vectors/_decimal_vector.pyx"),
    make_draken_extension("vectors.timestamp_vector", "vectors/timestamp_vector.pyx"),
    make_draken_extension("morsels.morsel", "morsels/morsel.pyx"),
    make_draken_extension("storage.morsel_io", "storage/morsel_io.pyx"),
    # Pre-generated C module for morsels.align (Cython-generated C source)
    Extension(
        "opteryx.compiled.draken.morsels.align",
        sources=["third_party/mabel/draken/morsels/align.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
        language="c",
    ),
    # Hash API shim used by a few draken helpers (Cython wrapper)
    Extension(
        "opteryx.compiled.draken.vectors._hash_api",
        sources=[
            "opteryx/compiled/draken/vectors/_hash_api.pyx",
            "src/cpp/simd_hash.cpp",
            "src/cpp/simd_bitops.cpp",
            "src/cpp/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
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
        "opteryx.compiled.structures.bloom_filter",
        sources=["opteryx/compiled/structures/bloom_filter.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
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
        sources=["opteryx/compiled/structures/memory_pool.pyx"],
        include_dirs=include_dirs,
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
    # Grouped aggregate (hashed) — new ground-up rewrite, single .so
    Extension(
        "opteryx.operators.grouped_aggregate_hashed._grouped_agg",
        sources=[
            "opteryx/operators/grouped_aggregate_hashed/_grouped_agg.pyx",
            "src/cpp/hllpp.cpp",
            "third_party/tdigest-c/src/tdigest_cpp.cpp",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
        depends=[
            "third_party/mabel/parvi/parvi.hpp",
            "third_party/mabel/carchar/carchar_index.hpp",
            "third_party/mabel/carchar/carchar_common.hpp",
            "third_party/mabel/carchar/carchar_simd.hpp",
        ],
    ),
    # Ungrouped (global) aggregate engine — single .so
    Extension(
        "opteryx.operators.aggregate.ungrouped_agg",
        sources=["opteryx/operators/aggregate/ungrouped_agg.pyx"],
        include_dirs=include_dirs + ["opteryx/operators/aggregate"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
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
    Extension(
        "opteryx.compiled.morsel_ops.distinct",
        sources=[
            "opteryx/compiled/morsel_ops/distinct.pyx",
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
    # Thread pool (BS::thread_pool via BSThreadPoolBridge)
    Extension(
        name="opteryx.compiled.thread_pool",
        sources=["opteryx/compiled/thread_pool.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=["-O3", "-std=c++17"] + WARNING_FLAGS,
        language="c++",
    ),
    # Lock-free SPSC queue (moodycamel::ReaderWriterQueue for Python objects)
    Extension(
        name="opteryx.compiled.pyobject_queue",
        sources=["opteryx/compiled/pyobject_queue.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=["-O3", "-std=c++17"] + WARNING_FLAGS,
        language="c++",
    ),
    # HTTP Client (libcurl-based HTTP with connection pooling and Range request support)
    # Operators - physical execution plan nodes
    Extension(
        "opteryx.operators.cross_join_node",
        sources=["opteryx/operators/cross_join_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.distinct_node",
        sources=["opteryx/operators/distinct_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.aggregate.aggregate_node",
        sources=["opteryx/operators/aggregate/aggregate_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.draken_inner_join_node",
        sources=["opteryx/operators/draken_inner_join_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.exit_node",
        sources=["opteryx/operators/exit_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.explain_node",
        sources=["opteryx/operators/explain_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.filter_join_node",
        sources=["opteryx/operators/filter_join_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
        language="c++",
    ),
    Extension(
        "opteryx.operators.filter_node",
        sources=["opteryx/operators/filter_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.function_dataset_node",
        sources=["opteryx/operators/function_dataset_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.heap_sort_node",
        sources=["opteryx/operators/heap_sort_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.limit_node",
        sources=["opteryx/operators/limit_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.nested_loop_join_node",
        sources=["opteryx/operators/nested_loop_join_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.non_equi_join_node",
        sources=["opteryx/operators/non_equi_join_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.null_reader_node",
        sources=["opteryx/operators/null_reader_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.outer_join_node",
        sources=["opteryx/operators/outer_join_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
        language="c++",
    ),
    Extension(
        "opteryx.operators.parquet_read_node",
        sources=["opteryx/operators/parquet_read_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.projection_node",
        sources=["opteryx/operators/projection_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.read_node",
        sources=["opteryx/operators/read_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.set_variable_node",
        sources=["opteryx/operators/set_variable_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.show_columns_node",
        sources=["opteryx/operators/show_columns_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.show_create_node",
        sources=["opteryx/operators/show_create_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.show_value_node",
        sources=["opteryx/operators/show_value_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.shuffle_node",
        sources=["opteryx/operators/shuffle_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.sort_node",
        sources=["opteryx/operators/sort_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.table_management_node",
        sources=["opteryx/operators/table_management_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.union_node",
        sources=["opteryx/operators/union_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.unnest_join_node",
        sources=["opteryx/operators/unnest_join_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
    Extension(
        "opteryx.operators.view_management_node",
        sources=["opteryx/operators/view_management_node.pyx"],
        include_dirs=include_dirs,
        extra_compile_args=C_FLAGS,
    ),
]

# Build libcurl first - REQUIRED for http_client extension
# Skip for sdist (source distribution packaging) and clean - no compilation needed
_build_commands = {"build", "build_ext", "install", "bdist_wheel", "bdist", "develop"}
_skip_build = not any(
    arg.lower() in _build_commands for arg in sys.argv[1:] if arg and not arg.startswith("-")
)
_libcurl_path = None
if not _skip_build:
    _libcurl_path = build_vendored_libcurl()

    if not _libcurl_path or not os.path.exists(_libcurl_path):
        raise RuntimeError(
            f"Failed to build vendored libcurl. HTTP client extension is REQUIRED.\n\n"
            "Ensure OpenSSL development headers are installed:\n"
            "  - macOS: brew install openssl\n"
            "  - Ubuntu/Debian: apt-get install libssl-dev\n"
            "  - RHEL/CentOS/Fedora: yum install openssl-devel\n\n"
            "Then rebuild with: python setup.py build_ext --inplace"
        )

    # HTTP client extension - MANDATORY (only add if not cleaning)
    extensions.append(
        Extension(
            name="opteryx.compiled.http_client",
            sources=[
                "opteryx/compiled/http_client.pyx",
                "src/cpp/http_client.cpp",
            ],
            include_dirs=include_dirs + ["src/cpp", "third_party/curl/include"],
            extra_compile_args=["-O3", "-std=c++17"] + WARNING_FLAGS,
            extra_link_args=[
                _libcurl_path,
                "-lssl",  # OpenSSL SSL library
                "-lcrypto",  # OpenSSL crypto library
            ]
            + ([] if is_win() else ["-lm"]),  # Link math library on non-Windows
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


# Generate vector_ops, joins, and aggregation kernels
generate_consolidated_module(
    "opteryx/compiled/vector_ops", "opteryx/compiled/vector_ops/vector_ops.pyx"
)
generate_consolidated_module("opteryx/compiled/joins", "opteryx/compiled/joins/joins.pyx")


# Add consolidated modules with their dependencies
# Link args for vector_ops (use -lcrypto on non-macOS and -pthread where appropriate)
# Use vendored digests to avoid runtime libcrypto dependency on target systems
# Vendored implementations: third_party/crypto/* (MD5, SHA1, SHA256)
vector_ops_link_args = []

if not is_win():
    vector_ops_link_args.append("-pthread")

extensions.extend(
    [
        Extension(
            "opteryx.compiled.vector_ops.function_definitions",
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
        Extension(
            "opteryx.compiled.joins.join_definitions",
            sources=[
                "opteryx/compiled/joins/joins.pyx",
                "src/cpp/intbuffer.cpp",
                "src/cpp/cpu_features.cpp",
            ],
            include_dirs=include_dirs,
            language="c++",
            extra_compile_args=CPP_FLAGS,
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
        "opteryx.compiled.io.csv_rows",
        sources=[
            "opteryx/compiled/io/csv_rows.pyx",
        ],
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
        language="c++",
    )
)

extensions.append(
    Extension(
        "opteryx.compiled.io.json_rows",
        sources=[
            "opteryx/compiled/io/json_rows.pyx",
        ],
        include_dirs=include_dirs,
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

# Compiled utils: PCG-backed random string helper
extensions.append(
    Extension(
        "opteryx.compiled.utils.random_helper",
        sources=["opteryx/compiled/utils/random_helper.pyx"],
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
        rpath = "@loader_path/../../third_party/onnxruntime/onnxruntime-osx-arm64-1.22.0/lib"
        return root, rpath
    if is_linux() and arch == "x86_64":
        root = "third_party/onnxruntime/onnxruntime-linux-x64-1.22.0"
        rpath = r"$ORIGIN/../../third_party/onnxruntime/onnxruntime-linux-x64-1.22.0/lib"
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
        sources=[
            "opteryx/connectors/parquet_io/pool_reader.pyx",
        ],
        include_dirs=include_dirs,
        language="c++",
        extra_compile_args=CPP_FLAGS,
        depends=[
            "third_party/mabel/rugo/parquet/io_pipeline.hpp",
            "third_party/mabel/rugo/parquet/decode.hpp",
            "third_party/mabel/rugo/parquet/metadata.hpp",
        ],
    )
)

# Setup configuration
setup(
    name=LIBRARY,
    version=__version__,
    description="Python SQL Query Engine",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(include=[LIBRARY, f"{LIBRARY}.*", "opteryx_core", "opteryx_core.*"]),
    python_requires=">=3.13",
    url="https://github.com/mabel-dev/opteryx/",
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": "3",
            "linetrace": "a" in __version__ or "b" in __version__,
        },
    ),
    rust_extensions=[RustExtension("opteryx.compute", "Cargo.toml", debug=False)],  # Add Rust here
    package_data={"": ["*.pyx", "*.pxd", "*.h"]},
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
