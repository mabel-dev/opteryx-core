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

        # mimalloc removed: draken now uses the system allocator (see
        # draken/core/alloc.h). A bundled mimalloc — whether statically linked
        # per-module (cross-module free UB) or as a shared dylib (heap
        # corruption when a foreign native lib such as pandas/pyarrow loads into
        # the process) — proved unsafe to coexist with the rest of the Python
        # process. The system allocator is a single process-wide instance shared
        # by every extension and every foreign library, so neither failure mode
        # exists. mimalloc may return as an opt-in, measured prod build flag.

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

        # Mixed C/C++ extension fix: pre-compile .c sources with C_FLAGS so they
        # never receive -std=c++20, which clang rejects for C compilation units.
        # Only triggered when a C++ extension (language="c++") contains .c sources.
        c_sources = [s for s in ext.sources if s.lower().endswith(".c")]
        if c_sources and getattr(ext, "language", "") == "c++":
            # Build C-compatible compile args: start from C_FLAGS, then append
            # any extension-specific args that are not C++-standard flags.
            c_extra = [
                a
                for a in (ext.extra_compile_args or [])
                if not a.startswith("-std=") and not a.startswith("/std:")
            ]
            c_compile_args = list(C_FLAGS) + c_extra
            include_dirs = list(ext.include_dirs or []) + list(self.include_dirs or [])
            macros = list(ext.define_macros or [])
            # compile() returns the list of object file paths it created.
            c_objs = self.compiler.compile(
                c_sources,
                output_dir=per_ext_build_temp,
                macros=macros,
                include_dirs=include_dirs,
                extra_postargs=c_compile_args,
            )
            # Fold the pre-compiled objects into extra_objects so the linker sees
            # them, and remove the .c sources so they are not compiled again.
            ext.extra_objects = list(ext.extra_objects or []) + list(c_objs)
            ext.sources = [s for s in ext.sources if not s.lower().endswith(".c")]

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
    if "riscv64" in machine or "riscv" in machine:
        return "riscv64"
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
INCLUDE_DEBUG_SYMBOLS_IN_COMPILED_CODE = (
    os.environ.get("INCLUDE_DEBUG_SYMBOLS_IN_COMPILED_CODE", "NO").upper() == "YES"
)

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

# Free-threaded CPython (PEP 703, e.g. 3.14t). When the build interpreter has
# the GIL disabled, nanobind must be compiled with NB_FREE_THREADED so its
# internal registries use atomic refcounts and per-object locks, and so each
# NB_MODULE declares itself free-threading-safe (otherwise importing it forces
# the GIL back on). The macro must be applied consistently to nb_combined.cpp
# AND every translation unit that includes nanobind headers, so it lives in the
# shared CPP_FLAGS. On a regular (GIL) build Py_GIL_DISABLED is unset and we add
# nothing — the standard build path is untouched.
import sysconfig as _sysconfig

FREE_THREADED_BUILD = bool(_sysconfig.get_config_var("Py_GIL_DISABLED"))
if FREE_THREADED_BUILD:
    CPP_FLAGS.append("-DNB_FREE_THREADED")

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

# Enable LTO for non-Windows when requested (must be after LD_EXTRA initialization)
if OPTERYX_ENABLE_LTO and not is_win():
    CPP_FLAGS.append("-flto")
    C_FLAGS.append("-flto")
    # ensure linker uses LTO as well
    LD_EXTRA.append("-flto")

# AddressSanitizer (opt-in, dev only). Build the C/C++ extensions with ASAN to
# diagnose heap memory-safety bugs (overflow/use-after-free). draken_malloc uses
# system malloc, so ASAN instruments draken buffers. Run with the ASAN runtime
# preloaded (macOS: DYLD_INSERT_LIBRARIES=<clang asan dylib>) and
# ASAN_OPTIONS=detect_leaks=0. Never enabled for wheels.
OPTERYX_ENABLE_ASAN = os.environ.get("OPTERYX_ENABLE_ASAN", "0").lower() in ("1", "true", "yes")
if OPTERYX_ENABLE_ASAN and not is_win():
    _asan_flags = ["-fsanitize=address", "-fno-omit-frame-pointer", "-g"]
    CPP_FLAGS.extend(_asan_flags)
    C_FLAGS.extend(_asan_flags)
    LD_EXTRA.append("-fsanitize=address")

# MSVC LTO linker flag when requested
if is_win() and OPTERYX_ENABLE_LTO:
    # '/LTCG' enables link-time code generation on MSVC
    LD_EXTRA.append("/LTCG")

if not INCLUDE_DEBUG_SYMBOLS_IN_COMPILED_CODE and not is_win():
    CPP_FLAGS.append("-s")
    C_FLAGS.append("-s")

# SIMD-specific flags (deterministic baseline to avoid host-specific AVX512/etc.)
if arch == "x86_64":
    CPP_FLAGS.extend(["-msse4.2", "-mavx2", "-march=haswell"])
    C_FLAGS.extend(["-msse4.2", "-mavx2", "-march=haswell"])
elif arch == "arm" and not is_mac():
    # 32-bit ARM needs explicit NEON; AArch64 already guarantees it.
    CPP_FLAGS.append("-mfpu=neon")
elif arch == "riscv64":
    # rv64gcv: G (IMAFD+Zicsr+Zifencei) + C (compressed) + V (vector).
    # Enables __riscv_vector and the RVV intrinsic headers.
    CPP_FLAGS.extend(["-march=rv64gcv"])
    C_FLAGS.extend(["-march=rv64gcv"])

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
    ".",  # repo root for Cython cimport (draken.core.buffers etc.)
    "src/cpp",
    "src/c",
    "draken",  # new draken C++-first headers (quote-include "core/buffers.h")
    "draken/core",  # draken C++ headers, quote-include form (e.g. #include "buffers.h")
    "third_party/mabel/carchar",
    "third_party/mabel/parvi",
    "third_party/mabel/perfect_hash",
    "third_party/fastfloat",
    "third_party/fastfloat/fast_float",
    "rugo/src/parquet",
    "third_party/yyjson/src",
    "third_party/re2",
    "third_party/cyan4973",
    "third_party/tdigest-c/src",
    "third_party/ulfjack/ryu",
    "third_party/nanobind",
    "third_party/crypto",
    "third_party/bshoshany",
    "third_party/moodycamel",
    "third_party/boost_math",  # E.3: vendored boost::math headers (round via 2^52 trick)
    "third_party/utf8h",  # E.26: sheredom/utf8.h single-header UTF-8 library
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
        depends = ["draken/core/buffers.h", "draken/core/vector_alloc.h"]

    sources = [f"draken/{source_file}"]
    # Include SIMD implementations for all draken vector modules so
    # simd_mix_hash, simd_popcount, and related functions are available at link time.
    for s in ("src/cpp/simd_hash.cpp", "src/cpp/simd_bitops.cpp"):
        if s not in sources:
            sources.append(s)

    # Unified DrakenVector constructors (one copy per extension; globals are
    # extension-local — owned-vs-shared discrimination lives in the Cython
    # typed wrapper, never in cross-extension pointer comparison).
    if "draken/core/vector_alloc.cpp" not in sources:
        sources.append("draken/core/vector_alloc.cpp")

    # Common SIMD/environment sources - CPU features and SIMDs
    for s in ("src/cpp/simd_env.cpp", "src/cpp/cpu_features.cpp", "src/cpp/simd_search.cpp"):
        if s not in sources:
            sources.append(s)

    # draken uses the system allocator (draken/core/alloc.h); mimalloc is not
    # linked — see the note in build_extensions for why it was removed.
    return Extension(
        name=f"draken.{module_path}",
        sources=sources,
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS if language == "c++" else C_FLAGS,
        extra_link_args=LD_EXTRA if language == "c++" else [],
        language=language,
        depends=depends,
    )


def get_zstd_vendor_sources():
    """Return the vendored zstd sources so other extensions can link to the same files."""
    RUGO_PARQUET = "rugo/src/parquet"
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


def get_text_writer_cast_sources():
    """draken batch cast-to-string kernels (int/bool/date/timestamp) used by the
    rugo CSV/JSONL writers, plus their deps (ryu for the float caster symbol)."""
    return [
        "draken/ops/kernels/cast_numeric.cpp",
        "draken/ops/kernels/cast_temporal.cpp",
        "draken/ops/kernels/result_helpers.cpp",
        "draken/core/vector_alloc.cpp",
        "third_party/ulfjack/ryu/d2fixed.c",
        "third_party/ulfjack/ryu/d2s.c",
    ]


def get_zstd_compress_sources():
    """Return the vendored zstd COMPRESSION sources (single-threaded; no zstdmt,
    so no pool/threading deps). Compiled as C++ — byte-identical to upstream
    zstd 1.5.5 lib/compress/*.c, renamed .cpp like the decompress set."""
    RUGO_PARQUET = "rugo/src/parquet"
    names = [
        "zstd_compress",
        "zstd_compress_literals",
        "zstd_compress_sequences",
        "zstd_compress_superblock",
        "fse_compress",
        "huf_compress",
        "hist",
        "zstd_double_fast",
        "zstd_fast",
        "zstd_lazy",
        "zstd_ldm",
        "zstd_opt",
    ]
    return [f"{RUGO_PARQUET}/vendor/zstd/compress/{n}.cpp" for n in names]


def get_lz4_vendor_sources():
    """Return vendored lz4 block-codec sources."""
    RUGO_PARQUET = "rugo/src/parquet"
    return [f"{RUGO_PARQUET}/vendor/lz4/lz4.c"]


def get_parquet_vendor_sources():
    """Return vendored zstd/snappy source files to build into parquet extension.

    We only compile the decompression bits (zstd) and minimal snappy sources we
    need for decompression. The vendor code is included inside the project, so
    building them into the extension avoids linking to system libraries and
    avoids runtime missing symbol errors.
    """
    vendor_sources = []
    RUGO_PARQUET = "rugo/src/parquet"

    # Snappy sources (minimal subset for decompress). Vendored in third_party.
    snappy_sources = [
        "third_party/snappy/snappy.cc",
        "third_party/snappy/snappy-sinksource.cc",
        "third_party/snappy/snappy-stubs-internal.cc",
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

# E.24 — Cython shim layer: real Cython extensions at each draken vector/morsel
# import path, providing __pyx_vtable__ so cimport consumers can load them.
# Each shim links draken_native.so via RTLD_GLOBAL (loaded in draken/__init__.py)
# and uses -undefined dynamic_lookup / --allow-shlib-undefined to resolve
# draken_vector_unwrap / draken_vector_own_raw at runtime.
_shim_bridge_link_args = (
    ["-undefined", "dynamic_lookup"] if is_mac() else ["-Wl,--allow-shlib-undefined"]
)

_shim_extensions = [
    make_draken_extension("vectors.vector", "vectors/_vector_shim.pyx"),
    make_draken_extension("vectors.bool_vector", "vectors/_bool_vector_shim.pyx"),
    make_draken_extension("morsels.morsel", "morsels/_morsel_shim.pyx"),
]
# Append shim bridge link args to each shim extension
for _ext in _shim_extensions:
    _ext.extra_link_args = list(_ext.extra_link_args) + _shim_bridge_link_args

# Define all extensions
extensions = [
    # Draken ABI guard (Milestone A.1). Compiling this forces the frozen
    # buffers.h static_asserts (sizeof==40, per-field offsets, DrakenType tag
    # pins) to run on the dev platform — silent ABI drift becomes a build break.
    Extension(
        "draken.core._abi_guard",
        sources=["draken/core/_abi_guard.cpp"],
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
        language="c++",
        depends=["draken/core/buffers.h", "draken/core/string_slot.h"],
    ),
    # Draken nanobind binding (Milestone B.1): Vector handle + Morsel + int64 ingestion.
    # Single module; nanobind + vector_alloc globals (owned buffers use the system
    # allocator via draken/core/alloc.h — mimalloc removed, see build_extensions).
    Extension(
        "draken.draken_native",
        sources=[
            "draken/draken_native.cpp",
            "draken/core/vector_alloc.cpp",
            "draken/core/bitmap_ops.cpp",  # E.21: bitmap operations for bytecode VM
            "draken/core/frame_arena.cpp",  # per-frame allocator for native eval engine
            "draken/ops/compare_dv.cpp",  # arena-backed compare entry point
            "draken/ops/arithmetic_dv.cpp",  # arena-backed arithmetic entry point
            # Phase 9a: C kernel ABI implementations
            "draken/ops/kernels/error_handling.cpp",
            "draken/ops/kernels/result_helpers.cpp",  # Phase 9c: string VecResult builder
            "draken/ops/kernels/kernel_registry.cpp",
            "draken/ops/kernels/cast_numeric.cpp",
            "draken/ops/kernels/cast_string.cpp",
            "draken/ops/kernels/cast_temporal.cpp",
            "draken/ops/kernels/cast_dispatch.cpp",
            "draken/ops/kernels/extraction.cpp",
            "draken/ops/kernels/binary_op_arithmetic.cpp",
            "draken/ops/kernels/binary_op_other.cpp",
            "draken/ops/kernels/binary_op_temporal.cpp",
            "draken/ops/kernels/binop_dispatch.cpp",  # P9.1: unified draken_binop (canonical binop kernel)
            # Function kernels deferred to Phase 9f; they require nanobind wrappers not yet ported to extern "C"
            # Milestone C.1: hash op depends on simd_hash_i64 / simd_mix_hash.
            "src/cpp/simd_hash.cpp",
            "src/cpp/simd_env.cpp",
            "src/cpp/cpu_features.cpp",
            "third_party/ulfjack/ryu/d2fixed.c",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "draken/core",  # quote-include "buffers.h" from within draken/
            "src/cpp",  # simd_hash.h, simd_dispatch.h, cpu_features.h
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
            "third_party/usearch/fp16/include",  # fp16 IEEE half-precision conversion (D.11)
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA,
        language="c++",
        depends=[
            "draken/core/buffers.h",
            "draken/core/alloc.h",
            "draken/core/vector_alloc.h",
            "draken/ops/hash.h",
            "src/cpp/simd_hash.h",
            # Phase 9a: kernel ABI headers
            "draken/ops/kernels/c_kernel_abi.h",
            "draken/ops/kernels/error_handling.h",
            "draken/ops/kernels/result_helpers.h",
            "draken/ops/kernels/kernel_registry.h",
            "draken/ops/kernels/kernel_context.h",
            "draken/ops/kernels/cast_kernels.h",
            "draken/ops/kernels/binary_op_kernels.h",
            "draken/ops/kernels/extraction_kernels.h",
            "draken/ops/kernels/function_kernels.h",
        ],
    ),
    # Phase 9a: C kernel registry lookup wrapper (Cython interface for bytecode builder/executor)
    Extension(
        "draken.ops.kernels._kernel_registry",
        sources=[
            "draken/ops/kernels/_kernel_registry.pyx",
            "draken/ops/kernels/error_handling.cpp",
            "draken/ops/kernels/result_helpers.cpp",  # Phase 9c: string VecResult builder
            "draken/ops/kernels/kernel_registry.cpp",
            "draken/ops/kernels/cast_numeric.cpp",
            "draken/ops/kernels/cast_string.cpp",
            "draken/ops/kernels/cast_temporal.cpp",
            "draken/ops/kernels/cast_dispatch.cpp",
            "draken/ops/kernels/extraction.cpp",
            "draken/ops/kernels/binary_op_arithmetic.cpp",
            "draken/ops/kernels/binary_op_other.cpp",
            "draken/ops/kernels/binary_op_temporal.cpp",
            "draken/ops/kernels/binop_dispatch.cpp",  # P9.1: unified draken_binop (canonical binop kernel)
            # Function kernels deferred to Phase 9f
            "src/cpp/simd_hash.cpp",
            "src/cpp/simd_env.cpp",
            "src/cpp/cpu_features.cpp",
        ],
        include_dirs=include_dirs,
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
        language="c++",
        depends=[
            "draken/ops/kernels/c_kernel_abi.h",
            "draken/ops/kernels/error_handling.h",
            "draken/ops/kernels/result_helpers.h",
            "draken/ops/kernels/kernel_registry.h",
            "draken/ops/kernels/kernel_context.h",
            "draken/ops/kernels/cast_kernels.h",
            "draken/ops/kernels/binary_op_kernels.h",
            "draken/ops/kernels/extraction_kernels.h",
            "draken/ops/kernels/function_kernels.h",
        ],
    ),
    # E.24 Cython shims — real compiled extensions providing __pyx_vtable__
    *_shim_extensions,
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
            "opteryx/third_party/mabel/base16/_base16.c",
        ],
        include_dirs=include_dirs,
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
    # File format readers
    Extension(
        "rugo.parquet_reader",
        sources=(
            [
                "rugo/src/parquet/parquet_reader.pyx",
                "rugo/src/parquet/metadata.cpp",
                "rugo/src/parquet/decode_encodings.cpp",
                "rugo/src/parquet/decode_page.cpp",
                "rugo/src/parquet/decode_column.cpp",
                "rugo/src/parquet/decode.cpp",
                "rugo/src/parquet/page_value_decoder.cpp",
                "rugo/src/parquet/compression.cpp",
                "rugo/src/parquet/bloom_filter.cpp",
                "src/cpp/cpu_features.cpp",
                "draken/core/vector_alloc.cpp",
            ]
            + get_parquet_vendor_sources()
        ),
        include_dirs=(
            include_dirs
            + [
                "third_party/snappy",
                "rugo/src/parquet/vendor/zstd",
                "rugo/src/parquet/vendor/zstd/common",
                "rugo/src/parquet/vendor/zstd/decompress",
            ]
        ),
        define_macros=[("HAVE_SNAPPY", "1"), ("HAVE_ZSTD", "1"), ("ZSTD_STATIC_LINKING_ONLY", "1"), ("HAVE_CONFIG_H", "1")],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=parquet_link_args + LD_EXTRA,
    ),
    # Parquet writer — header-only C++ core (_parquet_writer.hpp /
    # _thrift_writer.hpp); reads draken vectors and emits PyArrow-readable
    # parquet. No vendored sources: it only reads (str_data/str_length are
    # static-inline headers) and constructs no vectors.
    Extension(
        "rugo.parquet_writer",
        sources=(
            ["rugo/src/parquet/parquet_writer.pyx"]
            + get_zstd_vendor_sources()  # common + decompress (shared common syms)
            + get_zstd_compress_sources()
        ),
        include_dirs=(
            include_dirs
            + [
                "rugo/src/parquet/vendor/zstd",
                "rugo/src/parquet/vendor/zstd/common",
                "rugo/src/parquet/vendor/zstd/decompress",
                "rugo/src/parquet/vendor/zstd/compress",
            ]
        ),
        define_macros=[("HAVE_ZSTD", "1"), ("ZSTD_STATIC_LINKING_ONLY", "1")],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    # JSONL writer — Morsel -> JSONL bytes. C++ formatting (_value_format.hpp /
    # _text_render.hpp); int/bool/date/timestamp use draken's batch cast-to-
    # string kernels, float uses std::to_chars.
    Extension(
        "rugo.jsonl._jsonl_writer",
        sources=["rugo/src/jsonl/_jsonl_writer.pyx"] + get_text_writer_cast_sources(),
        include_dirs=include_dirs + ["rugo/src"],
        depends=["rugo/src/_value_format.hpp", "rugo/src/_text_render.hpp"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    # CSV writer — Morsel -> CSV bytes (RFC 4180). Same C++ formatting core.
    Extension(
        "rugo.csv._csv_writer",
        sources=["rugo/src/csv/_csv_writer.pyx"] + get_text_writer_cast_sources(),
        include_dirs=include_dirs + ["rugo/src"],
        depends=["rugo/src/_value_format.hpp", "rugo/src/_text_render.hpp"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    Extension(
        "rugo.jsonl._jsonl_reader",
        sources=[
            "rugo/src/jsonl/_jsonl_reader.pyx",
            "rugo/src/jsonl/core/structural_scan.cpp",
            "rugo/src/jsonl/core/interpreter.cpp",
            "rugo/src/jsonl/core/value_parser.cpp",
            "rugo/src/jsonl/core/field_span.cpp",
            "rugo/src/jsonl/core/jsonl_reader.cpp",
            "rugo/src/jsonl/core/column_builder.cpp",
            "src/cpp/simd_env.cpp",
            "src/cpp/cpu_features.cpp",
            "src/cpp/simd_search.cpp",
            "draken/core/vector_alloc.cpp",
        ],
        # Headers in `depends` so editing one forces the extension to recompile.
        # (Without this, header-only changes leave stale .o files behind.)
        depends=[
            "rugo/src/jsonl/core/markers.hpp",
            "rugo/src/jsonl/core/parse_context.hpp",
            "rugo/src/jsonl/core/structural_scan.hpp",
            "rugo/src/jsonl/core/interpreter.hpp",
            "rugo/src/jsonl/core/value_parser.hpp",
            "rugo/src/jsonl/core/field_span.hpp",
            "rugo/src/jsonl/core/jsonl_reader.hpp",
            "rugo/src/jsonl/core/column_builder.hpp",
            "rugo/src/jsonl/core/fast_parsers.hpp",
            "draken/core/draken_bridge.h",
            "draken/core/string_slot.h",
            "draken/core/alloc.h",
            "draken/core/buffers.h",
        ],
        include_dirs=include_dirs + ["rugo/src/jsonl/core"],
        language="c++",
        extra_compile_args=CPP_FLAGS,
    ),
    Extension(
        "rugo.csv._csv_reader",
        sources=[
            "rugo/src/csv/_csv_reader.pyx",
            "rugo/src/csv/core/csv_scan.cpp",
            "rugo/src/csv/core/csv_row_map.cpp",
            "rugo/src/csv/core/csv_column_builder.cpp",
            "draken/core/vector_alloc.cpp",
        ],
        depends=[
            "rugo/src/csv/core/csv_parse_context.hpp",
            "rugo/src/csv/core/csv_scan.hpp",
            "rugo/src/csv/core/csv_row_map.hpp",
            "rugo/src/csv/core/csv_column_builder.hpp",
            "rugo/src/jsonl/core/fast_parsers.hpp",
            "draken/core/draken_bridge.h",
            "draken/core/string_slot.h",
            "draken/core/alloc.h",
            "draken/core/buffers.h",
        ],
        include_dirs=include_dirs
        + [
            "rugo/src/csv/core",
            "rugo/src/jsonl/core",  # fast_parsers.hpp
        ],
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
            (
                "opteryx.expression.operations.__init__",
                "opteryx/expression/operations/__init__.pyx",
            ),
        )
    ],
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
    # Leaf .pyx files are textually included by _impl.pyx.
    Extension(
        "opteryx.expression.evaluator._impl",
        sources=[
            "opteryx/expression/evaluator/_impl.pyx",
            "opteryx/expression/evaluator/bytecode_worker.cpp",
        ],
        include_dirs=include_dirs
        + [
            "opteryx/expression/evaluator",  # bytecode_worker.h, bitmap_worker_pool.h
        ],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
    ),
    # All operator plan nodes — single consolidated .so
    Extension(
        "opteryx.operators._operators",
        sources=[
            "opteryx/operators/_operators.pyx",
            "src/cpp/hllpp.cpp",
            "third_party/tdigest-c/src/tdigest_cpp.cpp",
        ],
        include_dirs=include_dirs
        + [
            "opteryx/operators/aggregate",
        ],
        language="c++",
        extra_compile_args=CPP_FLAGS,
        extra_link_args=LD_EXTRA,
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

    for ext in extensions:
        if ext.name == "rugo.parquet_reader":
            ext.sources = list(ext.sources) + ["src/cpp/http_client.cpp"]
            ext.include_dirs = list(ext.include_dirs) + _curl_include_dirs
            ext.extra_link_args = list(ext.extra_link_args) + _curl_link_args
            break

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

# E.2 — C′ pattern pilot: 6 bitwise ops as pure nanobind C++.
# draken_vector_unwrap / draken_vector_own_raw are implemented in draken_native.so
# and resolved at import time (draken/__init__.py loads draken_native with
# RTLD_GLOBAL before any consumer extension is imported).
_bitwise_bridge_link_args = (
    ["-undefined", "dynamic_lookup"] if is_mac() else ["-Wl,--allow-shlib-undefined"]
)
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_bitwise",
        sources=[
            "opteryx/compiled/nanobind/vector_bitwise.cpp",
            "draken/core/vector_alloc.cpp",  # draken_identity_sel, draken_zero_sel
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.3 — C′ pattern: abs / sign / sqrt / round as pure nanobind C++.
# Uses the same RTLD_GLOBAL bridge pattern as E.2 (vector_bitwise).
# boost::math vendored in third_party/boost_math/ but NOT used for round
# (boost::math::round is half-away-from-zero; round uses 2^52 trick instead).
# boost stays vendored for future log/exp/trig/special phases.
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_math",
        sources=[
            "opteryx/compiled/nanobind/vector_math.cpp",
            "draken/core/vector_alloc.cpp",  # draken_identity_sel, draken_zero_sel
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.19 — C′ pattern: subscript + math remainders as pure nanobind C++.
#
# vector_special: 2 functions (map_access_string, map_access_array).
#   Replaces: vector_subscript.pyx (deleted; vector_get_element was dead code).
#
# (ceil/floor/trunc/power/random/random_normal extended vector_math.cpp — same E.19.)

extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_special",
        sources=[
            "opteryx/compiled/nanobind/vector_special.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.4 — C′ pattern: base64/85 codec + bool utility ops as pure nanobind C++.
# Uses the same RTLD_GLOBAL bridge pattern as E.2/E.3 (vector_bitwise/vector_math).
#
# vector_codec: 4 functions (b64_encode/decode, b85_encode/decode).
#   Links vendored mabel C sources directly.  No new draken op layer (mabel is
#   already vendored C++; adding a draken wrapper op would be vestigial).
#   Output is always DENSE; dict-preserving output deferred (needs Part A bridge).
#
# vector_bool_ops: 4 functions (from_int8_mask, from_inverted_bitmap, all_true,
#   and_chain).  Kernel logic lives in draken/ops/bool_logical.h (already built
#   in D.5).  No new draken-side work needed.
#
# Replaces: bool_vector_ops.pyx, vector_base64.pyx, vector_base85.pyx (deleted).

extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_codec",
        sources=[
            "opteryx/compiled/nanobind/vector_codec.cpp",
            "draken/core/vector_alloc.cpp",
            # Mabel vendored C sources (base64 + base85).
            "opteryx/third_party/mabel/base64/_base64.c",
            "opteryx/third_party/mabel/base64/_base64_dispatch.c",
            "opteryx/third_party/mabel/base64/_base64_neon.c",
            "opteryx/third_party/mabel/base64/_base64_avx2.c",
            "opteryx/third_party/mabel/base64/_base64_rvv.c",
            "opteryx/third_party/mabel/base85/_base85.c",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "opteryx/third_party/mabel/base64",  # _base64.h
            "opteryx/third_party/mabel/base85",  # _base85.h
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
        "opteryx.compiled.nanobind.vector_bool_ops",
        sources=[
            "opteryx/compiled/nanobind/vector_bool_ops.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.5 — C′ pattern: array element-reduction ops (ANY/ALL over array columns).
# Uses draken_array_child_unwrap (new bridge function) + array_reductions.h.
# Replaces: vector_anyop_{eq,neq,gt,gte,lt,lte}.pyx + vector_allop_{eq,neq}.pyx (8 files deleted).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_array_reduce",
        sources=[
            "opteryx/compiled/nanobind/vector_array_reduce.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.6 — C′ batch: string length/emptiness + array element count (4 consumer functions).
# Replaces: vector_string_length.pyx, vector_string_emptiness.pyx, vector_length.pyx (3 deleted).
# vector_get_element.pyx split into vector_json_extract.pyx + vector_map_access.pyx (Cython).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_accessors",
        sources=[
            "opteryx/compiled/nanobind/vector_accessors.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.8 — C′ pattern: hex encode/decode + MD5/SHA digest as pure nanobind C++.
# First consumers to use draken_vector_own_string (Phase-6 bridge) directly.
# Replaces: vector_hex.pyx, vector_md5.pyx, vector_sha.pyx (deleted from vector_ops/).
#
# Links vendored crypto (MD5, SHA-1, SHA-256, SHA-512) and mabel base16 C sources.
# Output is always DENSE (identity selection); all hash outputs are long-form (>12 chars).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_hash_codec",
        sources=[
            "opteryx/compiled/nanobind/vector_hash_codec.cpp",
            "draken/core/vector_alloc.cpp",
            # Vendored crypto digest implementations.
            "third_party/crypto/md5.cpp",
            "third_party/crypto/sha1.cpp",
            "third_party/crypto/sha2.cpp",
            "third_party/crypto/sha512.cpp",
            # Mabel vendored C source (base16 hex encode/decode).
            # _base16.c is a unity build — it #includes _dispatch/_neon/_avx2 internally.
            # Do NOT add the sub-files separately; they would produce duplicate symbols.
            "opteryx/third_party/mabel/base16/_base16.c",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "opteryx/third_party/mabel/base16",  # _base16.h
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.9 Phase 8 C′: cast cluster — int↔string + string→int + int→timestamp.
#
# Replaces: vector_cast_int64_to_string.pyx, vector_cast_uint64_to_string.pyx,
#           vector_cast_string_to_int.pyx, vector_cast_int64_to_timestamp.pyx.
# Uses the same RTLD_GLOBAL bridge pattern as E.8 (vector_hash_codec).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_casts",
        sources=[
            "opteryx/compiled/nanobind/vector_casts.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.10 C′: bytewise string-search + array membership consumers.
#
# Replaces: vector_starts_ends.pyx, vector_contains.pyx,
#           vector_contains_all.pyx, vector_contains_any.pyx.
# Contains: vector_{starts_with,ci_starts_with,ends_with,ci_ends_with,
#                    contains,contains_any,contains_all}.
# Uses the same RTLD_GLOBAL bridge pattern as E.9 (vector_casts).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_string_search",
        sources=[
            "opteryx/compiled/nanobind/vector_string_search.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.11 C′: coalesce + iif + concat — multi-arg selection + bytewise string concat.
#
# Replaces: vector_coalesce.pyx, vector_iif.pyx, vector_concat.pyx
#           (vector_concat_array / vector_concat_ws_array had no callers; deleted).
# Contains: vector_coalesce, vector_iif, vector_concat.
# Uses the same RTLD_GLOBAL bridge pattern as E.10 (vector_string_search).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_selection_concat",
        sources=[
            "opteryx/compiled/nanobind/vector_selection_concat.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.12 C′: temporal conversion cluster — DATE32↔TIMESTAMP64, unix seconds, floor.
#
# Replaces: vector_date32_to_timestamp.pyx, vector_timestamp_to_date32.pyx,
#           vector_unixtime.pyx, vector_floor_temporal.pyx.
# Contains: vector_date32_to_timestamp, vector_timestamp_to_date32,
#           vector_unixtime, vector_floor_temporal.
# Uses the same RTLD_GLOBAL bridge pattern as E.11 (vector_selection_concat).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_temporal_convert",
        sources=[
            "opteryx/compiled/nanobind/vector_temporal_convert.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.13 C′: temporal arithmetic cluster — date_part, date_diff, date_trunc, date_format.
#
# Replaces: vector_date_part.pyx, vector_date_diff.pyx,
#           vector_date_trunc.pyx, vector_date_format.pyx.
# Contains: vector_date_part, vector_date_diff, vector_date_trunc, vector_date_format.
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_temporal_arith",
        sources=[
            "opteryx/compiled/nanobind/vector_temporal_arith.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.15 C′: levenshtein + position + random_strings — bytewise + ASCII output.
#
# Replaces: vector_levenshtein.pyx, vector_position.pyx, vector_random_string.pyx (deleted).
# Contains: vector_levenshtein, vector_position, vector_random_strings.
# Uses the same RTLD_GLOBAL bridge pattern as E.14 (vector_misc).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_string_misc",
        sources=[
            "opteryx/compiled/nanobind/vector_string_misc.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.16 C′: replace + cosine_similarity + cosine_distance — mixed string/fp16 ops.
#
# Part A: draken/ops/vector_cosine.h (fp16 cosine kernel; widened to float64).
# Replaces: vector_replace.pyx, vector_cosine.pyx (deleted).
# Contains: vector_replace, vector_cosine_similarity, vector_cosine_distance.
# vector_split split out to Phase 15b (requires draken_vector_own_array bridge).
# Uses the same RTLD_GLOBAL bridge pattern as E.15 (vector_string_misc).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_string_misc2",
        sources=(
            [
                "opteryx/compiled/nanobind/vector_string_misc2.cpp",
                "draken/core/vector_alloc.cpp",
                "third_party/nanobind/src/nb_combined.cpp",
            ]
            + sorted(
                glob.glob("third_party/re2/re2/*.cc")
                + [
                    "third_party/re2/util/strutil.cc",
                    "third_party/re2/util/rune.cc",
                ]
            )
        ),
        include_dirs=include_dirs
        + [
            "third_party/usearch/fp16/include",  # fp16_ieee_to_fp32_value (D.11)
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.17 C′: Soundex — dead-helper cleanup + ASCII phonetic encoding.
#
# Replaces: vector_soundex.pyx, vector_encode_utf8.pyx (deleted; encode_utf8 was
#   identity-only with no callers).  Dead helpers deleted: _helper_const,
#   _helper_select, _helper_string, _helper_trim, _helper_vector_conversion,
#   _string_vec_iter, case_helpers (all confirmed no-cimport consumers).
# Contains: vector_soundex.
# Uses the same RTLD_GLOBAL bridge pattern as E.16 (vector_string_misc2).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_string_misc3",
        sources=[
            "opteryx/compiled/nanobind/vector_string_misc3.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.16b C′: vector_split — bytewise single-byte string split → DRAKEN_ARRAY[VARCHAR].
#
# Requires draken_vector_own_array bridge (added in draken_native.cpp for E.16b).
# Replaces: vector_split.pyx (deleted).
# Contains: vector_split.
# Uses the same RTLD_GLOBAL bridge pattern as E.15/E.16.
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_split_native",
        sources=[
            "opteryx/compiled/nanobind/vector_split_native.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.18 C′: vector_json_extract + vector_map_access — JSON field extraction via yyjson.
#
# Replaces: vector_json_extract.pyx, vector_map_access.pyx (deleted).
#           Non-JSON subscript functions moved to vector_subscript.pyx.
# Contains: vector_json_extract (full JSONPath), vector_map_access (top-level key).
# Uses the same RTLD_GLOBAL bridge pattern as E.17 (vector_string_misc3).
# Links yyjson.o (pre-compiled C library) for JSON parsing + serialisation.
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_json",
        sources=[
            "opteryx/compiled/nanobind/vector_json.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/yyjson/src/yyjson.c",  # compiled as C11; no pre-built .o dep
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/yyjson/src",
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.14 C′: log / in_list / ip_in_cidr as pure nanobind C++.
#
# Replaces: vector_log.pyx, vector_in_list.pyx, vector_ip_in_cidr.pyx (deleted).
# Contains: vector_log, vector_in_list, vector_ip_in_cidr.
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_misc",
        sources=[
            "opteryx/compiled/nanobind/vector_misc.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.26 C′: UTF-8 cluster pilot — vector_lowercase with per-type dispatch.
#
# VARCHAR: ASCII-only fold via simd_to_lower (non-ASCII bytes unchanged).
# NVARCHAR: Unicode codepoint fold via utf8.h utf8lwr (length-preserving).
# VARBINARY: raises ValueError (case ops on opaque bytes unsupported).
# Replaces (partially): vector_lowercase.pyx — old .pyx kept until all five
# cluster files are ported; see cleanup ticket.
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_string_case",
        sources=[
            "opteryx/compiled/nanobind/vector_string_case.cpp",
            "src/cpp/simd_string_ops.cpp",
            "src/cpp/cpu_features.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
            "third_party/nanobind",
            "third_party/nanobind/src",
            "third_party/nanobind/ext/robin_map/include",
        ],
        extra_compile_args=CPP_FLAGS + ["-fno-strict-aliasing", "-DNB_COMPACT_ASSERTIONS"],
        extra_link_args=LD_EXTRA + _bitwise_bridge_link_args,
        language="c++",
    )
)

# E.26 C′: string slice / substring operations — slice_left, slice_right, substring.
# Replaces: vector_string_slice.pyx (deleted).
extensions.append(
    Extension(
        "opteryx.compiled.nanobind.vector_string_slice",
        sources=[
            "opteryx/compiled/nanobind/vector_string_slice.cpp",
            "draken/core/vector_alloc.cpp",
            "third_party/nanobind/src/nb_combined.cpp",
        ],
        include_dirs=include_dirs
        + [
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
                "rugo/src/parquet/metadata.cpp",
                "rugo/src/parquet/bloom_filter.cpp",
                "rugo/src/parquet/page_value_decoder.cpp",
                "rugo/src/parquet/decode_encodings.cpp",
                "rugo/src/parquet/decode_page.cpp",
                "src/cpp/cpu_features.cpp",
                "src/cpp/http_client.cpp",
            ]
            + get_parquet_vendor_sources()
        ),
        include_dirs=(
            include_dirs
            + [
                "src/cpp",
                "rugo/src/parquet",
                "third_party/snappy",
                "rugo/src/parquet/vendor/zstd",
                "rugo/src/parquet/vendor/zstd/common",
                "rugo/src/parquet/vendor/zstd/decompress",
                "third_party/bshoshany",
                "third_party/moodycamel",
            ]
            + _curl_include_dirs
        ),
        define_macros=[("HAVE_SNAPPY", "1"), ("HAVE_ZSTD", "1"), ("ZSTD_STATIC_LINKING_ONLY", "1"), ("HAVE_CONFIG_H", "1")],
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
    },
    cmdclass={"build_ext": build_ext},
    zip_safe=False,
)
