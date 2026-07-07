"""
Shared build machinery for the two wheels built from this single source tree:

  * ``opteryx_core`` — the full SQL engine (root ``setup.py``), which bundles
    draken + rugo intrinsically.
  * ``rugo`` — the standalone PyArrow/NumPy-free file engine (``rugo/setup.py``),
    which ships draken + rugo only.

Both wheels compile the SAME draken/rugo sources; only the *packaging* differs.
This module is the single source of truth for everything they share — the
``build_ext`` subclass, compiler/linker flags, include dirs, the draken vendor
helpers, and the draken + rugo extension definitions themselves
(``draken_rugo_extensions``) — so the two builds can never drift apart.

IMPORTANT: this module must stay import-safe. No subprocess calls, no file
generation, no ``setup()`` — only pure definitions driven by env/platform.
The side-effectful, opteryx-only pieces (libcurl resolution, consolidated
nanobind module generation, onnxruntime) live in the root ``setup.py``.
"""

import os
import platform
import sysconfig as _sysconfig
import threading

from setuptools import Extension
from setuptools.command.build_ext import build_ext as build_ext_orig


# Thread-local storage so each parallel ThreadPool worker in build_extensions
# sees its own per-extension build_temp without clobbering other threads.
_build_temp_local = threading.local()


# Platform detection
def is_mac():
    return platform.system() == "Darwin"


def is_win():
    return platform.system() == "Windows"


def is_linux():
    return platform.system() == "Linux"


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

        # Standalone mimalloc (vendored, third_party/mimalloc = v3.3.2), built as a
        # SEPARATE shared library shipped as package data in draken/ — linked into
        # NOTHING. draken itself still uses the system allocator (draken/core/alloc.h);
        # this .so is inert until a deployment activates it via LD_PRELOAD.
        #
        # This is NOT the bundling that got mimalloc removed before. That failed two
        # ways: statically linked per-module (cross-module free UB) and as a load-time
        # dependency of our extensions (heap corruption when a foreign lib like
        # pyarrow had already allocated via glibc). This ships an independent .so that
        # nothing links or dlopens. Under LD_PRELOAD, ld.so makes it the single
        # process-wide allocator from exec — before the interpreter, before any
        # foreign lib — so neither failure mode can occur (validated: full ClickBench
        # battery completes vs glibc OOM). Fixes glibc per-thread-arena fragmentation
        # OOM under the multi-threaded native engine. Path: draken.preload_library_path().
        _mi_ext = "dylib" if platform.system() == "Darwin" else "so"
        _mi_out = os.path.join("draken", "libmimalloc.%s" % _mi_ext)
        _mi_src = "third_party/mimalloc/src/static.c"
        _mi_shared = ["-dynamiclib"] if platform.system() == "Darwin" else ["-shared", "-pthread"]
        if not os.path.exists(_mi_out) or os.path.getmtime(_mi_src) > os.path.getmtime(_mi_out):
            print("Building standalone mimalloc -> %s using compiler: %s" % (_mi_out, compiler))
            _mi_res = subprocess.run(
                [
                    compiler,
                    "-O2",
                    "-DNDEBUG",
                    "-DMI_MALLOC_OVERRIDE",  # export malloc/free so LD_PRELOAD interposes
                    "-fPIC",
                    *_mi_shared,
                    "-Ithird_party/mimalloc/include",
                    _mi_src,
                    "-o",
                    _mi_out,
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            # Fail loud: a wheel that silently omits the preload lib would leave
            # deployments pointing LD_PRELOAD at a missing file (§1 fail fast).
            if _mi_res.returncode != 0:
                raise RuntimeError("Failed to build standalone mimalloc: %s" % _mi_res.stderr)
            print("Successfully built %s" % _mi_out)

        # libcurl is already built at module initialization time
        super().build_extensions()

    def build_extension(self, ext):
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


def draken_rugo_extensions(parquet_created_by):
    """The draken + rugo Extension objects, shared verbatim by both wheels.

    ``parquet_created_by`` is the string baked into the parquet footer's
    ``created_by`` field — it differs per wheel (the opteryx_core build passes
    its version, the rugo wheel passes its own), so it is the one parameter.

    Returns un-cythonized Extension objects; the caller runs ``cythonize``.
    """
    shim_extensions = [
        make_draken_extension("vectors.vector", "vectors/_vector_shim.pyx"),
        make_draken_extension("vectors.bool_vector", "vectors/_bool_vector_shim.pyx"),
        make_draken_extension("morsels.morsel", "morsels/_morsel_shim.pyx"),
    ]
    # Append shim bridge link args to each shim extension
    for _ext in shim_extensions:
        _ext.extra_link_args = list(_ext.extra_link_args) + _shim_bridge_link_args

    return [
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
                "draken/ops/kernels/function_kernels.cpp",  # Phase 9a-fn: scalar function kernels (C ABI)
                # Milestone C.1: hash op depends on simd_hash_i64 / simd_mix_hash.
                "src/cpp/simd_hash.cpp",
                "src/cpp/simd_env.cpp",
                "src/cpp/cpu_features.cpp",
                "third_party/ulfjack/ryu/d2fixed.c",
                "third_party/ulfjack/ryu/d2s.c",
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
                "draken/ops/kernels/function_kernels.cpp",  # Phase 9a-fn: scalar function kernels (C ABI)
                "src/cpp/simd_hash.cpp",
                "src/cpp/simd_env.cpp",
                "src/cpp/cpu_features.cpp",
                "third_party/ulfjack/ryu/d2fixed.c",
                "third_party/ulfjack/ryu/d2s.c",
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
            ],
        ),
        # E.24 Cython shims — real compiled extensions providing __pyx_vtable__
        *shim_extensions,
        # Single consolidated rugo extension — all six readers/writers in one .so.
        # Eliminates cross-.so symbol lookup for draken bridge functions
        # (draken_vector_own_raw, draken_vector_own_string, etc.).
        Extension(
            "rugo.rugo_native",
            sources=(
                [
                    "rugo/src/rugo_native.pyx",
                    # parquet reader C++ sources
                    "rugo/src/parquet/metadata.cpp",
                    "rugo/src/parquet/decode_encodings.cpp",
                    "rugo/src/parquet/decode_page.cpp",
                    "rugo/src/parquet/decode_column.cpp",
                    "rugo/src/parquet/decode.cpp",
                    "rugo/src/parquet/compression.cpp",
                    "rugo/src/parquet/bloom_filter.cpp",
                    "src/cpp/cpu_features.cpp",
                    "src/cpp/disk_io.cpp",
                    "draken/core/vector_alloc.cpp",
                    # jsonl reader C++ sources
                    "rugo/src/jsonl/core/structural_scan.cpp",
                    "rugo/src/jsonl/core/interpreter.cpp",
                    "rugo/src/jsonl/core/value_parser.cpp",
                    "rugo/src/jsonl/core/field_span.cpp",
                    "rugo/src/jsonl/core/jsonl_reader.cpp",
                    "rugo/src/jsonl/core/column_builder.cpp",
                    "src/cpp/simd_env.cpp",
                    "src/cpp/simd_search.cpp",
                    # csv reader C++ sources
                    "rugo/src/csv/core/csv_scan.cpp",
                    "rugo/src/csv/core/csv_row_map.cpp",
                    "rugo/src/csv/core/csv_column_builder.cpp",
                ]
                + get_parquet_vendor_sources()
                + get_zstd_compress_sources()
                + [s for s in get_text_writer_cast_sources()
                   if s not in {
                       "draken/core/vector_alloc.cpp",
                       "src/cpp/cpu_features.cpp",
                       "src/cpp/simd_env.cpp",
                       "src/cpp/simd_search.cpp",
                   }]
            ),
            include_dirs=(
                include_dirs
                + [
                    "rugo/src",
                    "rugo/src/parquet",
                    "rugo/src/jsonl/core",
                    "rugo/src/csv/core",
                    "third_party/snappy",
                    "rugo/src/parquet/vendor/zstd",
                    "rugo/src/parquet/vendor/zstd/common",
                    "rugo/src/parquet/vendor/zstd/decompress",
                    "rugo/src/parquet/vendor/zstd/compress",
                ]
            ),
            depends=[
                "rugo/src/_value_format.hpp",
                "rugo/src/_text_render.hpp",
                "rugo/src/jsonl/core/markers.hpp",
                "rugo/src/jsonl/core/parse_context.hpp",
                "rugo/src/jsonl/core/structural_scan.hpp",
                "rugo/src/jsonl/core/interpreter.hpp",
                "rugo/src/jsonl/core/value_parser.hpp",
                "rugo/src/jsonl/core/field_span.hpp",
                "rugo/src/jsonl/core/jsonl_reader.hpp",
                "rugo/src/jsonl/core/column_builder.hpp",
                "rugo/src/jsonl/core/fast_parsers.hpp",
                "rugo/src/csv/core/csv_parse_context.hpp",
                "rugo/src/csv/core/csv_scan.hpp",
                "rugo/src/csv/core/csv_row_map.hpp",
                "rugo/src/csv/core/csv_column_builder.hpp",
                "draken/core/draken_bridge.h",
                "draken/core/string_slot.h",
                "draken/core/alloc.h",
                "draken/core/buffers.h",
            ],
            define_macros=[
                ("HAVE_SNAPPY", "1"),
                ("HAVE_ZSTD", "1"),
                ("ZSTD_STATIC_LINKING_ONLY", "1"),
                ("HAVE_CONFIG_H", "1"),
                ("RUGO_PARQUET_CREATED_BY", '"%s"' % parquet_created_by),
            ],
            language="c++",
            extra_compile_args=CPP_FLAGS,
            extra_link_args=parquet_link_args + LD_EXTRA,
        ),
    ]
