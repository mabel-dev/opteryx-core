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
``write_draken_abi_modules`` is *defined* here so all three wheels stamp
identically, but it writes nothing until a ``setup.py`` calls it.
"""

import hashlib
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
                # Append only what is not already present. Most extensions set
                # extra_link_args=LD_EXTRA at definition time, so a blind append
                # passed every flag twice. That was merely redundant for
                # -static-libstdc++/--exclude-libs, but ld rejects a REPEATED
                # --version-script outright ("anonymous version tag cannot be
                # combined with other version tags"), which would break the
                # C++-runtime hiding below. Dedupe, preserving order.
                _existing = list(getattr(ext, "extra_link_args", []))
                ext.extra_link_args = _existing + [a for a in LD_EXTRA if a not in _existing]
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


# Linux: the C++ runtime is linked DYNAMICALLY — one libstdc++ per process.
#
# ⛔ Do NOT bring back -static-libstdc++/-static-libgcc here. Static linking
# gave all 63 extensions a PRIVATE copy of the C++ runtime, and that is a bug
# class, not a hardening measure. Two production-grade crashes came out of it:
#
#  1. The every-GCS-query SIGSEGV (regressed in 0.9.54, diagnosed 2026-08-06).
#     manylinux wheels use the pre-C++11 COW string ABI; every .so carried its
#     own hidden _S_empty_rep_storage singleton while the weak _ZNSs* member
#     functions interposed across .so. pool_reader's footer parse
#     (ReadParquetMetadataFromBuffer) ran std::string code bound into
#     draken_native.so, the singleton address check in _M_dispose/_M_mutate
#     mismatched, and the "immortal" empty rep was operator-deleted: SIGSEGV
#     in free/_ZdlPv on EVERY GCS query (local parquet takes a different
#     footer path, which is why dev boxes never saw it). No per-.so fix works:
#     a version script localizing _ZNSs* stops the interposition but leaves N
#     singleton copies, and any std::string created in one .so and destroyed
#     in another (DecodedColumn.type / error_message, TestBloomFilterBytes,
#     ...) still frees static storage — verified on the x86 repro box, where
#     the localized build crashed local parquet as well. One shared runtime is
#     the only complete fix.
#  2. `import opteryx; import grpc` (any google.cloud.* import) aborted in
#     free(): cygrpc's dlopen bound half its std::string calls into our
#     exported private runtime and half into the system one.
#
# The original reason for static linking — old hosts missing newer GLIBCXX
# versions — is handled by the release pipeline: dev/build-wheels.sh runs
# `auditwheel repair`, which grafts the ONE devtoolset libstdc++ the build
# actually used into the wheel (opteryx_core.libs/) and patches rpaths, so the
# wheel is self-contained without any extension owning a private runtime.
#
# `--exclude-libs,ALL` stays: it hides symbols pulled from remaining static
# ARCHIVES (libgcc_eh.a etc. — and the whole C++ runtime again if someone
# ignores the warning above). hide_cxx_runtime.map stays as defense in depth:
# with dynamic libstdc++ our .so IMPORT the runtime instead of defining it, so
# the map's `local:` patterns match nothing — but if a static runtime ever
# sneaks back in, the map stops its export. It is `local:`-only, so PyInit_*
# and the RTLD_GLOBAL bridge symbols (draken_vector_unwrap /
# draken_vector_own_raw / draken_cast_*) keep their default exported binding —
# validated in the shipped 0.9.56 manylinux wheel, whose bridge imports
# resolve. (An earlier revert of the map blamed it for a bridge ImportError;
# that was actually the standalone rugo wheel overwriting opteryx_core's
# draken/ — see the release gate's consumer-shape check.)
_HIDE_CXX_RUNTIME_MAP = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "hide_cxx_runtime.map"
)
LD_EXTRA = (
    ["-static-libstdc++"]
    if is_mac()
    else [
        "-Wl,--exclude-libs,ALL",
        f"-Wl,--version-script={_HIDE_CXX_RUNTIME_MAP}",
    ]
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
    # -march=haswell sets the ISA floor (AVX2/BMI1+2/POPCNT/FMA) and implies
    # -msse4.2 -mavx2, so those are redundant. It ALSO implies -mtune=haswell,
    # which schedules for a 2013 microarchitecture; production runs on Zen/
    # Golden Cove. -mtune=generic keeps the same instruction set (no new ISA
    # requirement, so no SIGILL risk) while scheduling for modern parts.
    CPP_FLAGS.extend(["-march=haswell", "-mtune=generic"])
    C_FLAGS.extend(["-march=haswell", "-mtune=generic"])
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
    "draken/simd",  # shared SIMD layer (simd_hash/simd_env/cpu_features/simd_dispatch)
                    # — draken-owned so draken (and skene) never reach into src/cpp
    "src/c",
    "draken",  # new draken C++-first headers (quote-include "core/buffers.h")
    "draken/core",  # draken C++ headers, quote-include form (e.g. #include "buffers.h")
    "third_party/mabel",  # base64.pxd's quote-include form ("base64/_base64.h")
    "third_party/mabel/base16",  # vendored mabel codecs: the C libraries live at the
    "third_party/mabel/base64",  # repo root (opteryx-free, so draken kernels and the
    "third_party/mabel/base85",  # standalone rugo wheel can use them); opteryx's own
                                 # Cython wrappers stay under opteryx/third_party/mabel.
    "third_party/mabel/carchar",
    "third_party/mabel/medius",
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
    "draken/simd/simd_env.cpp",
    "draken/simd/cpu_features.cpp",
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

    # src/cpp/simd_bitops.cpp is GONE (deleted 2026-08-06). It used to be compiled
    # in solely to supply `simd_popcount`, which collided with
    # draken/core/bitmap_ops.cpp's. That collision is resolved: bitmap_ops.cpp's is
    # canonical — it lives in draken (which must not depend on opteryx's src/cpp),
    # every caller already declares it through draken's headers, and its ≤7-byte
    # tail uses __builtin_popcount (a POPCNT/CNT instruction) rather than
    # simd_bitops' 256-entry lookup table. The two main loops were identical
    # (8-byte words + __builtin_popcountll). These shims now resolve simd_popcount
    # from draken_native.so, loaded RTLD_GLOBAL by draken/__init__.py, the same way
    # they resolve every other draken_native symbol. simd_bitops' other exports
    # (simd_and/or/xor/not_mask, simd_select_bytes) had zero callers.

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
    rugo CSV/JSONL writers, plus their deps (ryu for the float caster symbol).
    f2s.c is the FLOAT32-precision shortest-round-trip formatter — d2s.c alone
    is not a substitute: promoting a float to double before calling d2s_buffered_n
    finds the shortest string for the WIDENED double, not the original float, and
    the two can disagree (draken/interop/value_format.hpp fmt_float)."""
    return [
        "draken/ops/kernels/cast_numeric.cpp",
        "draken/ops/kernels/cast_temporal.cpp",
        "draken/ops/kernels/result_helpers.cpp",
        "draken/core/vector_alloc.cpp",
        "third_party/ulfjack/ryu/d2fixed.c",
        "third_party/ulfjack/ryu/d2s.c",
        "third_party/ulfjack/ryu/f2s.c",
    ]


def get_zstd_compress_sources():
    """Return the vendored zstd COMPRESSION sources (single-threaded; no zstdmt,
    so no pool/threading deps). Compiled as C++ — byte-identical to upstream
    zstd 1.5.7 lib/compress/*.c, renamed .cpp like the decompress set.

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
        # New in 1.5.7: zstd_compress.c includes zstd_preSplit.h and calls into
        # it unconditionally, so it is not optional for a compress-capable build.
        "zstd_preSplit",
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
                "draken/simd/simd_hash.cpp",
                "draken/simd/simd_env.cpp",
                "draken/simd/cpu_features.cpp",
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
                # src/cpp/simd_bitops.cpp used to duplicate `simd_popcount` here and
                # is now deleted. draken/core/bitmap_ops.cpp's is canonical: draken
                # must not depend on opteryx's src/cpp, every caller already declares
                # it through draken's headers (core/bitmap_ops.h, vectors/vector.pxd,
                # vectors/bool_vector.pxd), and it is word-wide
                # (__builtin_popcountll) with a __builtin_popcount tail — strictly
                # better than the 256-entry LUT tail simd_bitops used. No duplicate
                # symbol remains, and which implementation runs no longer depends on
                # .so load order.
                "third_party/ulfjack/ryu/d2fixed.c",
                "third_party/ulfjack/ryu/d2s.c",
                "third_party/ulfjack/ryu/f2s.c",
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
                "draken/simd/simd_hash.h",
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
                    "draken/simd/cpu_features.cpp",
                    "src/cpp/disk_io.cpp",
                    "draken/core/vector_alloc.cpp",
                    # jsonl reader C++ sources
                    "rugo/src/jsonl/core/structural_scan.cpp",
                    "rugo/src/jsonl/core/interpreter.cpp",
                    "rugo/src/jsonl/core/value_parser.cpp",
                    "rugo/src/jsonl/core/field_span.cpp",
                    "rugo/src/jsonl/core/jsonl_reader.cpp",
                    "rugo/src/jsonl/core/column_builder.cpp",
                    "draken/simd/simd_env.cpp",
                    "src/cpp/simd_search.cpp",
                    # csv reader C++ sources
                    "rugo/src/csv/core/csv_scan.cpp",
                    "rugo/src/csv/core/csv_row_map.cpp",
                    "rugo/src/csv/core/csv_column_builder.cpp",
                    # explicit_schema's canonical type-name vocabulary, shared by
                    # the JSONL and CSV readers (rugo/src/declared_type.hpp).
                    "rugo/src/declared_type.cpp",
                ]
                + get_parquet_vendor_sources()
                + get_lz4_vendor_sources()  # lz4.c: LZ4_RAW block decode (parquet codec 7)
                + get_zstd_compress_sources()
                + [s for s in get_text_writer_cast_sources()
                   if s not in {
                       "draken/core/vector_alloc.cpp",
                       "draken/simd/cpu_features.cpp",
                       "draken/simd/simd_env.cpp",
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
                "rugo/src/jsonl/core/json_array_walker.hpp",
                "rugo/src/csv/core/csv_parse_context.hpp",
                "rugo/src/csv/core/csv_scan.hpp",
                "rugo/src/csv/core/csv_row_map.hpp",
                "rugo/src/csv/core/csv_column_builder.hpp",
                "draken/core/draken_bridge.h",
                "draken/core/string_slot.h",
                "draken/core/alloc.h",
                "draken/core/buffers.h",
                "draken/core/ipv4.h",
                "draken/core/iso_datetime.h",
                "draken/core/decimal_text.h",
                "rugo/src/declared_type.hpp",
                "rugo/src/declared_parse.hpp",
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


def skene_extensions():
    """The skene file-format extension (single-source, like draken/rugo above).

    Deliberately NOT part of rugo.rugo_native: skene and rugo are parallel and
    disjoint — neither imports the other (docs/SKENE_FILE_FORMAT_DESIGN.md).
    skene depends on draken alone; morsels cross the Python boundary through
    draken.morsels.morsel's cxx_to_morsel / morsel_to_cxx (capsule import, no
    link-time draken symbols).

    zstd and lz4 are both compiled in (same vendored copies as rugo — duplicate
    TUs across the two .so are benign: both codecs are stateless C, and each
    extension is self-contained exactly like skene's own libskene.a).
    """
    return [
        Extension(
            "skene.skene_native",
            sources=(
                [
                    "skene/src/skene_native.pyx",
                    "skene/src/checksum.cpp",
                    "skene/src/probe.cpp",
                    "skene/src/writer.cpp",
                    "skene/src/reader.cpp",
                    "skene/src/reader_v1.cpp",
                    "skene/src/reader_v2.cpp",
                    "skene/src/migrate.cpp",
                    "skene/src/value_order.cpp",
                    "skene/src/statistics.cpp",
                    "skene/src/encoding.cpp",
                    "skene/src/bloom.cpp",
                    "skene/src/file_io.cpp",
                    # One vector_alloc copy per extension — deliberate, matches
                    # make_draken_extension (globals are extension-local; owners
                    # carry their deleters so cross-extension frees are safe).
                    "draken/core/vector_alloc.cpp",
                ]
                + get_zstd_vendor_sources()
                + get_zstd_compress_sources()
                # lz4.c stays .c while the vendored zstd sources are .cpp;
                # setuptools picks the compiler per extension by suffix, so this
                # one TU is built as C and the rest as C++. lz4.h wraps its
                # declarations in extern "C", so skene's C++ callers link
                # against it unchanged (Encoding::kLz4, skene/src/encoding.cpp).
                + get_lz4_vendor_sources()
            ),
            include_dirs=(
                include_dirs
                + [
                    "skene/include",
                    "skene/src",
                    "third_party/zstd",
                    "third_party/zstd/common",
                    "third_party/zstd/decompress",
                    "third_party/zstd/compress",
                    "third_party/lz4",              # lz4.h
                ]
            ),
            depends=[
                "skene/include/skene/format.h",
                "skene/include/skene/reader.h",
                "skene/include/skene/writer.h",
                "skene/include/skene/probe.h",
                "skene/include/skene/status.h",
                "skene/include/skene/file_io.h",
                "skene/include/skene/checksum.h",
                "skene/src/reader_v1.h",
                "skene/src/encoding.h",
                "skene/src/statistics.h",
                "skene/src/value_order.h",
                "skene/src/bloom.h",
                "draken/core/buffers.h",
                "draken/core/vector_alloc.h",
                "draken/core/vector_owner.h",
                "draken/core/string_slot.h",
                "draken/logical_type.h",
                "draken/morsels/cxx_morsel.h",
            ],
            define_macros=[
                ("HAVE_ZSTD", "1"),
                ("ZSTD_STATIC_LINKING_ONLY", "1"),
            ],
            language="c++",
            extra_compile_args=CPP_FLAGS,
            extra_link_args=LD_EXTRA,
        ),
    ]


# ---------------------------------------------------------------------------
# draken ABI stamp
#
# opteryx_core, rugo and libskene are three distributions that each bundle the
# SAME `draken/` package, to the SAME site-packages path. pip cannot see that:
# whichever installs LAST silently overwrites the others' copy. When the winner
# comes from a different tree state, the consumer's extensions are left bound to
# a draken they were never compiled against. That is how 0.9.56 shipped an
# `undefined symbol: draken_cast_uint_to_string` on every query while every
# pre-release signal was green (see dev/verify-wheel-imports.sh), and it is the
# leading suspect for a later SIGSEGV inside free().
#
# The stamp gives draken an identity, so that collision surfaces as an
# immediate, named ImportError naming both sides — instead of an undefined
# symbol at first query, or a fault much later. It does NOT prevent the overlay:
# nothing at this layer can. Honest and diagnosable, not fixed.
#
# WHAT IT MEASURES: a content hash of every draken header — the struct layouts,
# type enums and kernel declarations consumers compile against — plus the kernel
# registry's name table. The registry counts because opteryx resolves kernels by
# NAME at runtime (see opteryx/expression/casts.pyx), so a missing registry
# entry is an ABI break even when no header moved.
#
# It deliberately answers the WEAKER question "were these built from the same
# ABI surface?" rather than "are these two technically compatible?". A
# comment-only edit to a header changes the stamp. That bias is the point: the
# three wheels are ruled to release in lockstep from one tree state, so
# "different tree state" and "must not be mixed" are the same statement here.
# Over-sensitivity costs a rebuild; under-sensitivity costs a production outage.
# ---------------------------------------------------------------------------

_REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

# Not a header, but its contents ARE the ABI: consumers look kernels up by name
# in this table at runtime.
DRAKEN_ABI_EXTRA_SOURCES = ("draken/ops/kernels/kernel_registry.cpp",)

# If any of these is absent we are not looking at a draken tree, and a hash over
# whatever remains would be a confident-looking answer to the wrong question.
DRAKEN_ABI_ANCHORS = (
    "draken/core/buffers.h",
    "draken/core/draken_bridge.h",
    "draken/logical_type.h",
)


def _draken_abi_files():
    """Every file whose content defines the draken ABI, as sorted repo-relative paths."""
    found = []
    for dirpath, dirnames, filenames in os.walk(os.path.join(_REPO_ROOT, "draken")):
        dirnames[:] = [d for d in dirnames if d != "tests" and not d.startswith(".")]
        for name in filenames:
            if name.endswith((".h", ".hpp")):
                rel = os.path.relpath(os.path.join(dirpath, name), _REPO_ROOT)
                found.append(rel.replace(os.sep, "/"))
    found.extend(DRAKEN_ABI_EXTRA_SOURCES)
    return sorted(set(found))


def draken_abi_stamp():
    """Content hash of the draken ABI surface — 16 hex chars.

    Deterministic across machines: sorted repo-relative paths and raw bytes, so
    the same tree state always produces the same stamp.
    """
    files = _draken_abi_files()
    for anchor in DRAKEN_ABI_ANCHORS + DRAKEN_ABI_EXTRA_SOURCES:
        if anchor not in files:
            raise RuntimeError(
                f"draken ABI surface is incomplete: {anchor} was not found under "
                f"{_REPO_ROOT}. Refusing to stamp a partial tree — the stamp would "
                "look valid and mean nothing."
            )
    digest = hashlib.sha256()
    for rel in files:
        with open(os.path.join(_REPO_ROOT, rel), "rb") as handle:
            payload = handle.read()
        digest.update(rel.encode("utf-8"))
        digest.update(b"\0")
        digest.update(payload)
        digest.update(b"\0")
    return digest.hexdigest()[:16]


_DRAKEN_STAMP_MODULE = '''\
# GENERATED FILE — do not edit, do not commit.
#
# Written by build_common.write_draken_abi_modules() on every build. Identifies
# the draken ABI surface this copy of the draken package was built from, so a
# consumer can tell whether the draken it is importing is the one it was
# compiled against. See the "draken ABI stamp" section of build_common.py.

DRAKEN_ABI_STAMP = "@@STAMP@@"
'''


_CONSUMER_ABI_MODULE = '''\
# GENERATED FILE — do not edit, do not commit.
#
# Written by build_common.write_draken_abi_modules() on every build. Holds the
# draken ABI stamp `@@PKG@@` was compiled against, and the check that refuses to
# run against a different one. All three distributions that bundle draken
# (opteryx_core, rugo, libskene) get this same module from one template, so the
# check cannot drift between them.
#
# There is deliberately NO environment variable to skip this check: a draken
# that does not match is not a degraded configuration, it is a broken one.

REQUIRED_DRAKEN_ABI_STAMP = "@@STAMP@@"


def check_draken_abi() -> None:
    """Raise ImportError unless the installed draken is the one we were built against.

    Called at the top of @@PKG@@/__init__.py, before any extension that resolves
    draken symbols is imported.
    """
    import importlib.util

    draken_spec = importlib.util.find_spec("draken")
    if draken_spec is None:
        raise ImportError(
            "`@@PKG@@` requires the `draken` package, which is not installed. "
            "It normally ships inside this wheel; a missing draken means the "
            "installation is incomplete."
        )
    location = draken_spec.origin or "unknown location"

    stamp_spec = importlib.util.find_spec("draken._abi_stamp")
    if stamp_spec is None:
        raise ImportError(
            "The installed `draken` carries no ABI stamp, so it cannot be matched "
            "against the draken `@@PKG@@` was built with "
            f"({REQUIRED_DRAKEN_ABI_STAMP}).\\n"
            f"  installed draken: {location}\\n"
            "An unstamped draken predates this check, which means it comes from an "
            "older release of opteryx_core, rugo or libskene that overwrote this "
            "one. Reinstall all the draken-bundling distributions you use from the "
            "same release."
        )

    from draken._abi_stamp import DRAKEN_ABI_STAMP

    if DRAKEN_ABI_STAMP != REQUIRED_DRAKEN_ABI_STAMP:
        raise ImportError(
            "draken ABI mismatch — the installed `draken` is not the one "
            "`@@PKG@@` was built against.\\n"
            f"  `@@PKG@@` was built against draken ABI  {REQUIRED_DRAKEN_ABI_STAMP}\\n"
            f"  the installed draken carries      ABI  {DRAKEN_ABI_STAMP}\\n"
            f"  installed draken: {location}\\n"
            "opteryx_core, rugo and libskene each bundle their own copy of "
            "`draken` at this same path, so whichever was installed LAST wins — "
            "and the winner here is not the one `@@PKG@@` was compiled against. "
            "Continuing would fail on an undefined draken_* symbol, or crash "
            "later inside native code.\\n"
            "Fix: install the draken-bundling distributions you use from the same "
            "release (they are tagged and published together), or install only "
            "the one you need."
        )
'''


def write_draken_abi_modules(*consumer_packages):
    """Generate the draken stamp module and each consumer's matching check.

    Writes ``draken/_abi_stamp.py`` (what this draken IS) and, for every package
    named, ``<package>/_draken_abi.py`` (what that package REQUIRES). All come
    from the same computed stamp, so an in-tree build always agrees with itself
    and the check only ever fires across tree states — which is exactly the
    install-overlay case it exists to catch.

    EVERY draken-consuming package the wheel ships must be named, not just the
    one the distribution is named after: the opteryx_core wheel also ships
    ``rugo`` and ``skene``, and a bundled package without its generated module
    would fail to import at all. The generated module is keyed on the PACKAGE,
    so the copy of ``rugo/_draken_abi.py`` inside opteryx_core is byte-identical
    to the one in the standalone rugo wheel.

    Called explicitly by each setup.py; never at import time (see module
    docstring). Returns the stamp so the caller can log it.
    """
    stamp = draken_abi_stamp()

    with open(
        os.path.join(_REPO_ROOT, "draken", "_abi_stamp.py"), "w", encoding="utf-8"
    ) as handle:
        handle.write(_DRAKEN_STAMP_MODULE.replace("@@STAMP@@", stamp))

    for package in consumer_packages:
        content = _CONSUMER_ABI_MODULE.replace("@@STAMP@@", stamp).replace("@@PKG@@", package)
        with open(
            os.path.join(_REPO_ROOT, package, "_draken_abi.py"), "w", encoding="utf-8"
        ) as handle:
            handle.write(content)

    return stamp
