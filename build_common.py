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
#
# `--exclude-libs,ALL` is load-bearing, not tidiness. It marks every symbol pulled
# in from a static ARCHIVE (libstdc++.a, libgcc.a, libgcc_eh.a) as hidden in this
# extension's dynamic symbol table. Without it, `-fvisibility=default` (set above)
# EXPORTS this extension's private copy of the C++ runtime — `__cxa_throw`,
# `__gxx_personality_v0`, the `_Unwind_*` family — and draken/__init__.py loads
# draken_native under RTLD_GLOBAL, publishing that private copy into the
# process-global symbol table. Extensions linked against the SHARED libstdc++
# (opteryx.connectors.parquet_io.pool_reader takes no LD_EXTRA, so it does) then
# bind their throw path to draken's private runtime while their handler search
# stays in the shared one. The two disagree, no handler is found, and
# std::terminate() aborts the process — turning what should be a catchable
# `except +` RuntimeError into a hard crash with no traceback. Observed in
# production as a Cloud Run 503 with a faulthandler dump whose C stack shows
# __cxa_throw resolved inside draken_native.so under a throw raised in
# pool_reader.so.
#
# Symbols from this extension's OWN objects — PyInit_*, and the bridge symbols
# draken_vector_unwrap / draken_vector_own_raw that the Cython shims resolve via
# RTLD_GLOBAL — come from .o files, not archives, so they are unaffected and stay
# exported. macOS is immune (two-level namespaces bind each image to its own
# runtime) and its linker does not accept the flag.
LD_EXTRA = (
    ["-static-libstdc++"]
    if is_mac()
    else ["-static-libstdc++", "-static-libgcc", "-Wl,--exclude-libs,ALL"]
)

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

# ThreadSanitizer (opt-in, dev only). Build the C/C++ extensions with TSAN to
# diagnose DATA RACES on the concurrent native paths (executor fan-out, the
# io_pipeline decode pool, the shared MemoryPool, grouped-agg combine) — races
# ASAN cannot see. TSAN reports the racing accesses on ANY interleaving, whether
# or not that run crashes, so it does not need the crash to reproduce. Run with
# the TSAN runtime preloaded (macOS: DYLD_INSERT_LIBRARIES=<clang tsan dylib>).
# CPython 3.14t itself is NOT tsan-instrumented, so use a suppressions file to
# quiet Python-internal (refcount/dict) false positives and focus on opteryx/
# rugo/draken/src frames. Mutually exclusive with ASAN. Never enabled for wheels.
OPTERYX_ENABLE_TSAN = os.environ.get("OPTERYX_ENABLE_TSAN", "0").lower() in ("1", "true", "yes")
if OPTERYX_ENABLE_TSAN and not OPTERYX_ENABLE_ASAN and not is_win():
    _tsan_flags = ["-fsanitize=thread", "-fno-omit-frame-pointer", "-g"]
    CPP_FLAGS.extend(_tsan_flags)
    C_FLAGS.extend(_tsan_flags)
    LD_EXTRA.append("-fsanitize=thread")

# MSVC LTO linker flag when requested
if is_win() and OPTERYX_ENABLE_LTO:
    # '/LTCG' enables link-time code generation on MSVC
    LD_EXTRA.append("/LTCG")

if (not INCLUDE_DEBUG_SYMBOLS_IN_COMPILED_CODE and not is_win()
        and not OPTERYX_ENABLE_ASAN and not OPTERYX_ENABLE_TSAN):
    # Strip at LINK time. extra_compile_args never reaches the linker, so a `-s`
    # on CPP_FLAGS/C_FLAGS is inert — the manylinux wheel shipped ~85% DWARF
    # (`-g` arrives for free via CPython's sysconfig CFLAGS). Stripping here
    # removes the debug sections and symbol table. gnu ld (manylinux, the wheel
    # target) honours -s; ld64 (macOS dev) ignores -s but strips with -Wl,-x.
    # Sanitizer builds are excluded above so race/leak reports stay readable.
    LD_EXTRA.append("-s" if not is_mac() else "-Wl,-x")

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
    "third_party/mabel",  # base64.pxd's quote-include form ("base64/_base64.h")
    "third_party/mabel/base16",  # vendored mabel codecs: the C libraries live at the
    "third_party/mabel/base64",  # repo root (opteryx-free, so draken kernels and the
    "third_party/mabel/base85",  # standalone rugo wheel can use them); opteryx's own
                                 # Cython wrappers stay under opteryx/third_party/mabel.
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
    "third_party/pcg",  # vendored PCG PRNG — RANDOM/NORMAL native kernels
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

    # Unified DrakenVector constructors (one copy per extension; globals are
    # extension-local — owned-vs-shared discrimination lives in the Cython
    # typed wrapper, never in cross-extension pointer comparison).
    if "draken/core/vector_alloc.cpp" not in sources:
        sources.append("draken/core/vector_alloc.cpp")

    # simd_bitops is still compiled in, unlike the rest of the shared SIMD layer:
    # it cannot be moved into draken_native because its `simd_popcount` collides
    # with draken/core/bitmap_ops.cpp's (see the note in draken_native's source
    # list). Removing it here without resolving that collision leaves these modules
    # with no definition at all.
    if "src/cpp/simd_bitops.cpp" not in sources:
        sources.append("src/cpp/simd_bitops.cpp")

    # The rest of the shared SIMD layer — simd_hash, simd_env, cpu_features,
    # simd_search — is NOT compiled in. draken_native is its single compiled home
    # (see its source list), and draken/__init__.py loads draken_native under
    # RTLD_GLOBAL before any of these modules can be imported, so they resolve at
    # runtime exactly as the bridge symbols do. Each of these four extensions used to
    # carry its own copy of all five.
    #
    # These modules link with -undefined dynamic_lookup / --allow-shlib-undefined,
    # so an unresolvable symbol is a crash at first call, NOT a link error. After
    # changing this list, verify every project-prefixed undefined symbol
    # (_simd_*, _draken_*, _opteryx_*, _kernel_*, _rugo_*, _avx_*, _neon_*) in each
    # rebuilt .so is exported by draken_native. A green build proves nothing here.

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
    """Return the vendored zstd sources so other extensions can link to the same files.

    Canonical single copy lives under ``third_party/zstd`` (shared by both wheels)."""
    ZSTD = "third_party/zstd"
    sources = [
        f"{ZSTD}/common/entropy_common.cpp",
        f"{ZSTD}/common/fse_decompress.cpp",
        f"{ZSTD}/common/zstd_common.cpp",
        f"{ZSTD}/common/xxhash.cpp",
        f"{ZSTD}/common/error_private.cpp",
        f"{ZSTD}/decompress/zstd_decompress.cpp",
        f"{ZSTD}/decompress/zstd_decompress_block.cpp",
        f"{ZSTD}/decompress/huf_decompress.cpp",
        f"{ZSTD}/decompress/zstd_ddict.cpp",
    ]
    machine = detect_architecture()
    if machine in ("x86_64", "amd64"):
        sources.append(f"{ZSTD}/decompress/huf_decompress_amd64.S")
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
    zstd 1.5.5 lib/compress/*.c, renamed .cpp like the decompress set.

    Canonical single copy lives under ``third_party/zstd`` (shared by both wheels)."""
    ZSTD = "third_party/zstd"
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
    return [f"{ZSTD}/compress/{n}.cpp" for n in names]


def get_lz4_vendor_sources():
    """Return vendored lz4 block-codec sources (canonical copy: third_party/lz4)."""
    return ["third_party/lz4/lz4.c"]


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


# The shared parquet extension links NOTHING beyond LD_EXTRA.
#
# It is built without RUGO_ENABLE_HTTP (only opteryx_core's own parquet/
# http_client extensions define it), so the remote-read path — the only thing
# that ever wanted OpenSSL — is compiled out here. This object references zero
# libcrypto symbols in either wheel.
#
# It previously force-linked `-Wl,--no-as-needed -lcrypto` purely so a CI `ldd`
# check could assert the library's presence. That made auditwheel vendor a 2.6MB
# libcrypto into the standalone rugo wheel and gave it a hard OpenSSL runtime
# dependency — for a library whose whole pitch is being small and dependency
# free. opteryx_core's genuine crypto need is unaffected: it comes from
# resolve_libcurl() (static libcurl + -lssl -lcrypto) on the extensions that
# actually call it.
parquet_link_args = []

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
        make_draken_extension(
            "vectors.vector",
            "vectors/_vector_shim.pyx",
            # Vector._to_json() includes the shared native JSON renderer.
            depends=[
                "draken/core/buffers.h",
                "draken/core/vector_alloc.h",
                "draken/interop/value_format.hpp",
                # value_format.hpp's render descriptor carries LogicalKind.
                "draken/logical_type.h",
            ],
        ),
        make_draken_extension("vectors.bool_vector", "vectors/_bool_vector_shim.pyx"),
        make_draken_extension("morsels.morsel", "morsels/_morsel_shim.pyx"),
        # The ONE sort implementation (vergesort prepass -> comparison-sort
        # fallback over the AoS short-circuit comparator, or plain SortKeyCmp for
        # 5+ key columns) lives in draken/morsels/sort.hpp — pure C++, no
        # opteryx/Python dependency. This extension is a thin Cython marshaling
        # shim over it (Morsel <-> shared_ptr<CxxMorsel>, nothing sort-related).
        # Both wheels build it: opteryx's SortSink/TopNSink/WindowSink call the
        # same header through src/cpp/engine/native_sort.hpp's re-export shim,
        # and the standalone rugo wheel calls this module directly (no opteryx
        # dependency). SQL ORDER BY semantics stay in opteryx's planner; only the
        # sort primitive lives here.
        make_draken_extension(
            "morsels.sort",
            "morsels/sort.pyx",
            depends=[
                "draken/core/buffers.h",
                "draken/core/vector_alloc.h",
                "draken/core/string_slot.h",
                "draken/core/vergesort.h",
                "draken/morsels/cxx_morsel.h",
                "draken/morsels/sort.hpp",
            ],
        ),
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
                # docs/EXECUTION_TRACING_DESIGN.md: the ONE compiled home of the shared
                # execution-tracer state (draken/core/trace.hpp) — rugo (pool_reader.so)
                # and the opteryx engine (_operators.so) are separate .so's and must not
                # each get their own copy (the BS::thread_pool trap; see
                # draken/core/trace_bridge_c.h). Belongs in THIS extension so it is
                # loaded RTLD_GLOBAL by draken/__init__.py alongside draken_vector_unwrap
                # et al.
                "draken/core/trace_bridge.cpp",
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
                "draken/ops/kernels/string_trim.cpp",  # Phase 9a-fn: TRIM/LTRIM/RTRIM (C ABI)
                "draken/ops/kernels/string_reverse_initcap.cpp",  # Phase 9a-fn: REVERSE/INITCAP (C ABI)
                "draken/ops/kernels/string_pad.cpp",  # Phase 9a-fn: LPAD/RPAD (C ABI)
                "draken/ops/kernels/string_replace_soundex.cpp",  # Phase 9a-fn: REPLACE/SOUNDEX (C ABI)
                "draken/ops/kernels/string_humanize.cpp",  # Phase 9a-fn: HUMANIZE (C ABI)
                "draken/ops/kernels/function_hash_encoding.cpp",  # Phase 9a-fn: MD5/SHA* (C ABI)
                "draken/ops/kernels/function_codec.cpp",  # Phase 9a-fn: HEX/BASE64/BASE85 ENCODE/DECODE (C ABI)
                "draken/ops/kernels/function_array_json.cpp",  # Phase 9a-fn: JSONB_OBJECT_KEYS (C ABI)
                "draken/ops/kernels/function_temporal.cpp",  # Phase 9a-fn: FROM_UNIXTIME (C ABI)
                "draken/ops/kernels/function_numeric.cpp",  # Phase 9a-fn: POWER/LOG/TRUNC (C ABI)
                "draken/ops/kernels/function_string_extra.cpp",  # Phase 9a-fn: OCTET_LENGTH/POSITION/LEVENSHTEIN/TO_ASCII (C ABI)
                "draken/ops/kernels/function_null_conditional.cpp",  # Phase 9a-fn: COALESCE/IFNULL/IFNOTNULL/IIF (C ABI)
                "draken/ops/kernels/function_vector_distance.cpp",  # Phase 9a-fn: EMBED/COSINE_SIMILARITY/COSINE_DISTANCE (C ABI)
                "draken/ops/kernels/function_rlike.cpp",  # RLIKE/NOT RLIKE over a plan-time-compiled DFA blob (C ABI, no RE2)
                "draken/ops/kernels/function_like_any.cpp",  # LIKE ANY/ILIKE ANY over a plan-time-compiled matcher blob (C ABI, no RE2)
                # Vendored digest cores backing function_hash_encoding.cpp. Headers are
                # already on include_dirs ("third_party/crypto"); the impls must be
                # listed here or the kernels fail to link.
                "third_party/crypto/md5.cpp",
                "third_party/crypto/sha1.cpp",
                "third_party/crypto/sha2.cpp",
                "third_party/crypto/sha512.cpp",
                # Vendored mabel base16 (digest->hex via bintob16_lower, and
                # function_codec.cpp's HEX_ENCODE/DECODE; unity build, _base16.c
                # #includes the dispatch + per-arch SIMD sources — only _base16.c
                # goes on the source list, see third_party/mabel/base16/_base16.c).
                "third_party/mabel/base16/_base16.c",
                # Vendored mabel base64 backing function_codec.cpp's BASE64_ENCODE/
                # DECODE (NOT a unity build: list every per-arch source, matching
                # setup.py's opteryx.third_party.mabel.base64 extension).
                "third_party/mabel/base64/_base64.c",
                "third_party/mabel/base64/_base64_dispatch.c",
                "third_party/mabel/base64/_base64_neon.c",
                "third_party/mabel/base64/_base64_avx2.c",
                "third_party/mabel/base64/_base64_rvv.c",
                # Vendored mabel base85 (scalar-only by design) backing
                # function_codec.cpp's BASE85_ENCODE/DECODE.
                "third_party/mabel/base85/_base85.c",
                # Milestone C.1: hash op depends on simd_hash_i64 / simd_mix_hash.
                "src/cpp/simd_hash.cpp",
                "src/cpp/simd_env.cpp",
                "src/cpp/cpu_features.cpp",
                # draken_native is the single compiled home of the shared SIMD layer
                # for the draken unit (simd_hash / simd_env / cpu_features above).
                # The vector/morsel/sort modules built by make_draken_extension
                # resolve those at runtime through the RTLD_GLOBAL load in
                # draken/__init__.py instead of each compiling its own copy. Do not
                # remove one without putting it back into every module that
                # make_draken_extension builds: those link with -undefined
                # dynamic_lookup / --allow-shlib-undefined, where a missing symbol is
                # a crash at first call, not a link error.
                #
                # src/cpp/simd_search.cpp is deliberately NOT here. Nothing in draken
                # references simd_search_substring — it was carried by all four
                # make_draken_extension modules as dead weight, and adding it here
                # merely moved the waste (the linker dead-stripped it; draken_native
                # exported no simd_search symbol). Other units (opteryx strings /
                # vector_ops, rugo) use it and compile it themselves.
                #
                # src/cpp/simd_bitops.cpp CANNOT join this list: it defines
                # `size_t simd_popcount(const uint8_t*, size_t)` with external
                # linkage, and so does draken/core/bitmap_ops.cpp (line 69) which is
                # already compiled in above. Adding it is an ld "duplicate symbols"
                # failure. The two are DIFFERENT implementations — bitmap_ops walks
                # byte-at-a-time with __builtin_popcount, simd_bitops does 8 bytes at
                # a time with __builtin_popcountll — so today which one a module
                # executes depends on which .cpp its source list happens to include,
                # and on Linux (RTLD_GLOBAL + -fvisibility=default) on which .so
                # loaded first. Same answer, materially different speed. Resolving
                # which implementation is canonical is a prerequisite to moving this
                # file, and is an architecture decision, not a build one.
                "third_party/ulfjack/ryu/d2fixed.c",
                "third_party/ulfjack/ryu/d2s.c",
                # extraction.cpp's `->`/`->>` kernels parse with yyjson.
                "third_party/yyjson/src/yyjson.c",
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
                "draken/core/trace.hpp",
                "draken/core/trace_bridge_c.h",
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
        #
        # Sources are the .pyx ONLY. Every kernel this module talks to already lives in
        # draken_native.so, which draken/__init__.py loads under RTLD_GLOBAL before any
        # consumer can reach this module (importing it runs the draken package init
        # first). So the small extern "C" surface it actually uses — kernel_registry_
        # lookup / _register and the kernel_alloc_*_ctx family, all declared in
        # kernel_registry.h — is resolved at runtime from draken_native, exactly as the
        # vector/morsel shims above resolve draken_vector_unwrap.
        #
        # This used to recompile ~45 of draken_native's own sources: the entire kernel
        # set plus its vendored digest/codec/ryu/yyjson backing. That put a SECOND copy
        # of kernel_registry.cpp's `static std::map` registry in the process. It worked
        # only by an unstated invariant — the single writer (register_kernel) and the
        # single reader (lookup_kernel) both live in this .pyx, so a register/lookup
        # pair always hit the same copy: bound locally on macOS, interposed onto
        # draken_native's copy by RTLD_GLOBAL on Linux. Nothing enforced that. A C-side
        # kernel_registry_lookup from draken_native would, on macOS, have read a map the
        # Python registrations never reached. One copy, and the invariant is moot.
        Extension(
            "draken.ops.kernels._kernel_registry",
            sources=["draken/ops/kernels/_kernel_registry.pyx"],
            include_dirs=include_dirs,
            extra_compile_args=CPP_FLAGS,
            extra_link_args=LD_EXTRA + _shim_bridge_link_args,
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
                "draken/ops/json_extract.h",
                "draken/ops/json_path.h",
                "draken/ops/string_result.h",
                "draken/ops/string_subscript.h",
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
                    # miniz raw-DEFLATE inflate for the parquet GZIP codec.
                    # tinfl_decompress_mem_to_mem is self-contained in this TU
                    # (no malloc, no other miniz object needed).
                    "third_party/miniz/miniz_tinfl.cpp",
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
                    # column_builder.cpp's parse_array_column parses array elements with yyjson.
                    "third_party/yyjson/src/yyjson.c",
                    "src/cpp/simd_env.cpp",
                    "src/cpp/simd_search.cpp",
                    # csv reader C++ sources
                    "rugo/src/csv/core/csv_scan.cpp",
                    "rugo/src/csv/core/csv_row_map.cpp",
                    "rugo/src/csv/core/csv_column_builder.cpp",
                ]
                + get_parquet_vendor_sources()
                + get_lz4_vendor_sources()  # lz4.c: LZ4_RAW block decode (parquet codec 7)
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
                    "third_party/zstd",
                    "third_party/zstd/common",
                    "third_party/zstd/decompress",
                    "third_party/zstd/compress",
                    "third_party/lz4",              # lz4.h
                    "third_party/miniz",            # miniz_tinfl.h / miniz.h
                    # parquet_writer.pxi's VECTOR_FP16 branch -> core/fp16.h -> <fp16/fp16.h>.
                    # draken_native and _kernel_registry already carry this; every extension
                    # that touches core/fp16.h needs the same include.
                    "third_party/usearch/fp16/include",
                ]
            ),
            depends=[
                "draken/interop/value_format.hpp",
                "draken/logical_type.h",   # LogicalKind, via value_format.hpp
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
