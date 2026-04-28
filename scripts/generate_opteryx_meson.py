#!/usr/bin/env python3
"""Generate opteryx/meson.build from actual source file structure.

For each .pyx file in opteryx/compiled/:
  - If a companion .cpp exists, parse its embedded Cython distutils metadata
    to discover which extra src/cpp/*.cpp files are needed.
  - If no companion, build with .pyx only (pure Cython, calls Draken/Rugo types).

Skip opteryx/compiled/draken/ - those are built by the draken package.
"""

import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
OPTERYX_DIR = REPO_ROOT / "opteryx"
COMPILED_DIR = OPTERYX_DIR / "compiled"

# Extensions to skip (built elsewhere or require special handling)
SKIP_DIRS = {"draken"}

# Extensions that need libcurl (handled separately with link_args)
CURL_EXTENSIONS = {"http_client"}


def find_included_fragments(compiled_dir: Path) -> set[Path]:
    """Find .pyx files that are include-fragments of a consolidation module.

    Scan every .pyx file for ``include "..."`` directives.  The included
    paths are relative to the including file's directory.  We mark them as
    fragments so they are not compiled as standalone extension modules.
    """
    fragments: set[Path] = set()
    for pyx in compiled_dir.rglob("*.pyx"):
        text = pyx.read_text(errors="replace")
        for match in re.finditer(r'^include\s+"([^"]+)"', text, re.MULTILINE):
            included = (pyx.parent / match.group(1)).resolve()
            fragments.add(included)
    return fragments


def parse_cython_metadata(cpp_path: Path) -> dict:
    """Extract the embedded distutils JSON from a Cython-generated .cpp file."""
    try:
        content = cpp_path.read_text(errors="replace")
        m = re.search(
            r"BEGIN: Cython Metadata(.+?)END: Cython Metadata", content, re.DOTALL
        )
        if not m:
            return {}
        raw = m.group(1).strip()
        lines = [line.lstrip(" *").rstrip() for line in raw.split("\n")]
        return json.loads("\n".join(lines))
    except Exception:
        return {}


def pyx_to_module_name(pyx_path: Path, meta: dict | None = None) -> str:
    """Convert compiled/structures/bloom_filter.pyx → opteryx_compiled_structures_bloom_filter.

    If the pre-generated companion has an embedded module_name (e.g.
    "opteryx.compiled.vector_ops.function_definitions"), use that instead of the
    pyx filename — the .cpp was compiled with that name and exports PyInit_<last_part>.
    """
    if meta:
        module_name = meta.get("module_name", "")
        if module_name.startswith("opteryx.compiled."):
            # "opteryx.compiled.vector_ops.function_definitions"
            # → "opteryx_compiled_vector_ops_function_definitions"
            return module_name.replace(".", "_")

    rel = pyx_path.relative_to(COMPILED_DIR)
    parts = list(rel.with_suffix("").parts)
    return "opteryx_compiled_" + "_".join(parts)


def normalise_src_path(src: str) -> str:
    """Convert a distutils source path (from repo root) to meson-relative path.

    Meson paths in opteryx/meson.build are relative to the opteryx/ subdirectory.
    - src/cpp/...   →  ../src/cpp/...
    - opteryx/compiled/...  →  compiled/...  (skip – Meson builds from .pyx)
    """
    if src.startswith("opteryx/compiled/"):
        return None  # Skip: Meson generates this from .pyx
    if src.startswith("src/"):
        return "../" + src
    if src.startswith("third_party/"):
        return "../" + src
    return src


def _resolve_cimport_pxds(pyx_or_pxd: Path, seen: set | None = None) -> list[Path]:
    """Resolve cimport/include statements to .pxd paths reachable from the repo root.

    Returns a flat list of .pxd files (de-duped) transitively reachable from the
    given .pyx/.pxd file.  Only resolves paths that map to files actually present
    under REPO_ROOT; silently skips unresolved imports (stdlib, draken, etc.).
    """
    if seen is None:
        seen = set()
    if pyx_or_pxd in seen:
        return []
    seen.add(pyx_or_pxd)

    if not pyx_or_pxd.exists():
        return []
    text = pyx_or_pxd.read_text(errors="replace")

    result: list[Path] = []

    # Follow cimport lines: "from opteryx.compiled.X.Y cimport ..." → opteryx/compiled/X/Y.pxd
    #                        "from draken.X.Y cimport ..."          → draken/X/Y.pxd
    for m in re.finditer(r'^from\s+([\w.]+)\s+cimport', text, re.MULTILINE):
        module = m.group(1)
        rel = module.replace(".", "/") + ".pxd"
        candidate = (REPO_ROOT / rel).resolve()
        if candidate.exists() and candidate not in seen:
            result.append(candidate)
            result.extend(_resolve_cimport_pxds(candidate, seen))

    # Follow Cython include directives: include "foo.pyx" (relative to current file)
    for m in re.finditer(r'^include\s+"([^"]+)"', text, re.MULTILINE):
        child = (pyx_or_pxd.parent / m.group(1)).resolve()
        if child.exists() and child not in seen:
            result.extend(_resolve_cimport_pxds(child, seen))

    return result


def build_extension_entry(pyx_path: Path) -> str:
    """Build a python.extension_module() meson call for one .pyx file.

    Strategy: use the pre-generated Cython C++ (.cpp) as the primary source when
    it exists. Meson's Cython→C++ pipeline mis-routes --cplus output through the C
    compiler; building from the pre-generated .cpp bypasses Cython entirely and
    lets the C++ compiler handle it directly.

    The Meson module name is derived from the embedded Cython metadata's module_name
    (e.g. "opteryx.compiled.vector_ops.function_definitions") when available. This
    ensures the .so exports the correct PyInit_<name> function matching the Python
    package path, even if the .pyx file was renamed after the .cpp was generated.
    """
    stem = pyx_path.stem

    # Check for pre-generated Cython C++ companion
    cpp_companion = pyx_path.with_suffix(".cpp")
    c_companion = pyx_path.with_suffix(".c")
    extra_sources = []
    meta_include_dirs = []
    is_cpp = False

    define_macros: list[tuple[str, str]] = []
    meta: dict = {}

    if cpp_companion.exists():
        meta = parse_cython_metadata(cpp_companion)
        distutils = meta.get("distutils", {})
        raw_sources = distutils.get("sources", [])
        meta_include_dirs = distutils.get("include_dirs", [])
        define_macros = distutils.get("define_macros", [])
        is_cpp = True  # .cpp companion ⇒ always C++
        for src in raw_sources:
            norm = normalise_src_path(src)
            if norm is not None:
                extra_sources.append(norm)
        # Use pre-generated C++ file as primary source (bypass Cython)
        primary_src = "compiled/" + str(cpp_companion.relative_to(COMPILED_DIR))
    elif c_companion.exists():
        meta = parse_cython_metadata(c_companion)
        distutils = meta.get("distutils", {})
        raw_sources = distutils.get("sources", [])
        meta_include_dirs = distutils.get("include_dirs", [])
        define_macros = distutils.get("define_macros", [])
        for src in raw_sources:
            norm = normalise_src_path(src)
            if norm is not None:
                extra_sources.append(norm)
        primary_src = "compiled/" + str(c_companion.relative_to(COMPILED_DIR))
    else:
        # PYX_ONLY: no pre-generated companion, invoke Cython
        primary_src = "compiled/" + str(pyx_path.relative_to(COMPILED_DIR))

        # Parse # distutils: sources from .pyx and any include-d fragments
        def _collect_distutils_sources(path: Path, _seen: set | None = None) -> list[str]:
            if _seen is None:
                _seen = set()
            if path in _seen or not path.exists():
                return []
            _seen.add(path)
            text = path.read_text(errors="replace")
            srcs: list[str] = []
            for m in re.finditer(r'^#\s*distutils:\s*sources\s*=\s*(.+)$', text, re.MULTILINE):
                raw = m.group(1).strip()
                # Accept space-separated or comma-separated lists (optionally quoted)
                for tok in re.split(r'[,\s]+', raw):
                    tok = tok.strip().strip('"\'')
                    if tok:
                        srcs.append(tok)
            # Recurse into included files
            for m in re.finditer(r'^include\s+"([^"]+)"', text, re.MULTILINE):
                child = (path.parent / m.group(1)).resolve()
                srcs.extend(_collect_distutils_sources(child, _seen))
            return srcs

        for raw_src in _collect_distutils_sources(pyx_path):
            norm = normalise_src_path(raw_src)
            if norm is not None and norm not in extra_sources:
                extra_sources.append(norm)

        cpp_markers = [
            "cppclass", "libcpp.", "from libcpp", "except +",
            "language = c++", "language=c++",
            # C++ standard library headers included via cdef extern from *
            "#include <algorithm>", "#include <ios>", "#include <string>",
            "#include <vector>", "#include <map>", "#include <unordered_map>",
            "#include <memory>", "#include <functional>", "#include <utility>",
            "#include <stdexcept>", "#include <cstdint>", "#include <typeinfo>",
        ]

        def _text_has_cpp(path: Path) -> bool:
            if not path.exists():
                return False
            t = path.read_text(errors="replace")
            return any(m in t for m in cpp_markers)

        # Check the .pyx itself (including transitively included files) AND all
        # cimported .pxd files — if any of them use C++ constructs, we must
        # compile in C++ mode (the generated C file will include C++ headers).
        all_deps = [pyx_path] + _resolve_cimport_pxds(pyx_path)
        if any(_text_has_cpp(p) for p in all_deps):
            is_cpp = True

    # Derive module name: prefer metadata's module_name (ensures PyInit_ matches Python path)
    module_name = pyx_to_module_name(pyx_path, meta if meta else None)

    # Build the meson call
    lines = [f"python.extension_module('{module_name}',"]
    lines.append(f"  '{primary_src}',")

    if extra_sources:
        lines.append("  sources: [")
        for s in extra_sources:
            lines.append(f"    '{s}',")
        lines.append("  ],")

    # C++ mode via override_options (works with current Meson; cython_args --cplus is unreliable)
    if is_cpp:
        lines.append("  override_options: ['cython_language=cpp'],")

    # Use inc_mabel for extensions that have actual #include directives for
    # carchar/parvi headers (not just mentions in the embedded metadata comment).
    # Draken headers are in the base inc (opteryx always depends on draken).
    def has_mabel_include(path: Path) -> bool:
        """Return True if path references carchar/parvi headers (direct or cimport chain)."""
        if not path.exists():
            return False
        text = path.read_text(errors="replace")
        # Strip metadata block before scanning to avoid false positives
        text = re.sub(r'/\*.*?Cython Metadata.*?\*/', '', text, flags=re.DOTALL)
        if re.search(r'#include\s+[<"][^>"]*(?:carchar|parvi)', text):
            return True
        # Also match Cython extern-from declarations (in .pxd files) referencing carchar/parvi
        if re.search(r'cdef\s+extern\s+from\s+["\'][^"\']*(?:carchar|parvi)', text):
            return True
        return False

    need_mabel = has_mabel_include(OPTERYX_DIR / primary_src)
    if not need_mabel:
        for src_rel in extra_sources:
            # extra_sources are relative to opteryx/ dir (e.g. ../src/cpp/foo.cpp)
            src_abs = (OPTERYX_DIR / src_rel).resolve()
            if has_mabel_include(src_abs):
                need_mabel = True
                break
    if not need_mabel:
        # Check cimported .pxd files — if any extern from carchar/parvi, we need inc_mabel
        for dep_pxd in _resolve_cimport_pxds(pyx_path):
            if has_mabel_include(dep_pxd):
                need_mabel = True
                break
    if need_mabel:
        lines.append("  include_directories: [inc, inc_mabel],")
    else:
        lines.append("  include_directories: inc,")

    # Emit cpp_args / c_args for define_macros from Cython metadata
    if define_macros:
        flag_args = []
        for macro_name, macro_val in define_macros:
            if macro_val in ("1", "", None, True, 1):
                flag_args.append(f"-D{macro_name}=1")
            else:
                flag_args.append(f"-D{macro_name}={macro_val}")
        lang = "cpp" if is_cpp else "c"
        flags_str = ", ".join(f"'{f}'" for f in flag_args)
        lines.append(f"  {lang}_args: [{flags_str}],")

    # libcurl link_args for http_client
    if stem in CURL_EXTENSIONS:
        lines.append("  link_args: ['-lcurl'],")

    lines.append(")")
    return "\n".join(lines)


def collect_extensions() -> list[tuple[str, str]]:
    """Walk compiled/ and return (category_comment, meson_entry) pairs."""
    entries = []

    # Detect include-fragments: files included by a consolidation module should
    # NOT be compiled as standalone extension modules.
    fragments = find_included_fragments(COMPILED_DIR)

    # Group by immediate subdirectory for section headers
    sections: dict[str, list[Path]] = {}
    for pyx in sorted(COMPILED_DIR.rglob("*.pyx")):
        parts = pyx.relative_to(COMPILED_DIR).parts
        # Skip draken subdirectory (built by draken package)
        if parts[0] in SKIP_DIRS:
            continue
        # Skip include-fragments (compiled via their consolidation parent)
        if pyx.resolve() in fragments:
            continue
        section = parts[0] if len(parts) > 1 else "_root"
        sections.setdefault(section, []).append(pyx)

    SECTION_TITLES = {
        "_root": "ROOT-LEVEL EXTENSIONS",
        "functions": "FUNCTION EXTENSIONS",
        "io": "I/O EXTENSIONS",
        "joins": "JOIN EXTENSIONS",
        "morsel_ops": "MORSEL OPERATION EXTENSIONS",
        "structures": "DATA STRUCTURE EXTENSIONS",
        "utils": "UTILITY EXTENSIONS",
        "vector_ops": "VECTOR OPERATION EXTENSIONS",
    }

    for section, pyxs in sorted(sections.items()):
        title = SECTION_TITLES.get(section, section.upper() + " EXTENSIONS")
        entries.append(
            (
                f"\n# {'=' * 76}\n"
                f"# {title}\n"
                f"# {'=' * 76}\n",
                None,
            )
        )
        for pyx in pyxs:
            entries.append(("", build_extension_entry(pyx)))

    return entries


def generate_meson_build() -> str:
    header = """\
# Opteryx package build configuration
# AUTO-GENERATED by scripts/generate_opteryx_meson.py
# Do not edit by hand — re-run the generator to update.

# Get Python module (re-import for local scope in subdir context)
py = import('python')

# Note: project(), compiler flags, architecture detection, and optimisations
# are all handled in the root meson.build.

# ----------------------------------------------------------------------------
# Include directories
# ----------------------------------------------------------------------------
inc = include_directories(
  'compiled',
  '../src/cpp',
  '../third_party/fastfloat',
  '../third_party/fastfloat/fast_float',
  '../third_party/yyjson/src',
  '../third_party/re2',
  '../third_party/cyan4973',
  '../third_party/tdigest-c/src',
  '../third_party/ulfjack/ryu',
  '../third_party/nanobind',
  '../third_party/bshoshany',
  '../third_party/moodycamel',
  '../third_party/crypto',
  '../draken/src',              # C headers (core/buffers.h, interop/*.h)
  '..',                         # draken .pxd resolution (draken/core/, draken/vectors/, …)
  'third_party/mabel',
  'third_party/pcg',
)

# Mabel sub-library headers (Carchar, Parvi — carchar/parvi-dependent extensions only)
inc_mabel = include_directories(
  '../third_party/mabel/carchar',
  '../third_party/mabel/parvi',
)

"""
    entries = collect_extensions()
    body_parts = []
    for comment, entry in entries:
        if comment:
            body_parts.append(comment)
        if entry:
            body_parts.append(entry + "\n")

    footer = """
# ============================================================================
# Python package installation
# ============================================================================
# Note: Python source file installation is handled by mesonpy build backend

# ============================================================================
# THIRD-PARTY EXTENSION INCLUDES
# ============================================================================

# Zstd vendor include dirs (for third_party/facebook/zstd and pool_reader)
inc_zstd_vendor = include_directories(
  '../third_party/zstd',
  '../third_party/zstd/common',
  '../third_party/zstd/decompress',
)

# LZ4 vendor include dirs
inc_lz4_vendor = include_directories(
  '../third_party/lz4',
)

# Pool reader extra includes
inc_pool_reader = include_directories(
  '../rugo/src/parquet',
  '../third_party/snappy',
  '../third_party/zstd',
  '../third_party/zstd/common',
  '../third_party/zstd/decompress',
)

# ============================================================================
# THIRD-PARTY EXTENSIONS
# ============================================================================

# xxhash (cyan4973) — used for fast hashing
python.extension_module('xxhash',
  'third_party/cyan4973/xxhash.pyx',
  sources: [
    '../third_party/cyan4973/xxhash.c',
    '../src/cpp/xxhash_build_info.c',
    '../src/cpp/cpu_features.cpp',
  ],
  include_directories: inc,
  c_args: ['-DXXH_NO_XXH128=1'],
)

# zstd (facebook) — decompression wrapper
python.extension_module('zstd',
  'third_party/facebook/zstd.pyx',
  sources: [
    '../third_party/zstd/common/entropy_common.cpp',
    '../third_party/zstd/common/fse_decompress.cpp',
    '../third_party/zstd/common/zstd_common.cpp',
    '../third_party/zstd/common/xxhash.cpp',
    '../third_party/zstd/common/error_private.cpp',
    '../third_party/zstd/decompress/zstd_decompress.cpp',
    '../third_party/zstd/decompress/zstd_decompress_block.cpp',
    '../third_party/zstd/decompress/huf_decompress.cpp',
    '../third_party/zstd/decompress/zstd_ddict.cpp',
  ],
  include_directories: [inc, inc_zstd_vendor],
  c_args: ['-DZSTD_STATIC_LINKING_ONLY=1'],
)

# fast_float (fastfloat) — fast string-to-float parsing
python.extension_module('fast_float',
  'third_party/fastfloat/fast_float.pyx',
  override_options: ['cython_language=cpp'],
  include_directories: inc,
)

# fuzzy (soundex) — phonetic hashing
python.extension_module('fuzzy',
  'third_party/fuzzy/soundex.pyx',
  include_directories: inc,
)

# lz4 — LZ4 decompression wrapper
python.extension_module('lz4',
  'third_party/lz4/lz4.pyx',
  sources: [
    '../third_party/lz4/lz4.c',
  ],
  include_directories: [inc, inc_lz4_vendor],
)

# base16 (mabel) — base16 encoding/decoding
python.extension_module('base16',
  'third_party/mabel/base16/base16.pyx',
  sources: [
    'third_party/mabel/base16/_base16.c',
  ],
  include_directories: [inc, include_directories('third_party/mabel/base16')],
)

# base64 (mabel) — base64 encoding/decoding
python.extension_module('base64',
  'third_party/mabel/base64/base64.pyx',
  sources: [
    'third_party/mabel/base64/_base64.c',
    'third_party/mabel/base64/_base64_dispatch.c',
    'third_party/mabel/base64/_base64_neon.c',
    'third_party/mabel/base64/_base64_avx2.c',
    'third_party/mabel/base64/_base64_avx512.c',
  ],
  include_directories: [inc, include_directories('third_party/mabel/base64')],
)

# distogram (maki_nage) — statistical histograms for cost-based optimization
python.extension_module('distogram',
  'third_party/maki_nage/distogram.pyx',
  override_options: ['cython_language=cpp'],
  include_directories: inc,
)

# mbleven — edit-distance computation
python.extension_module('mbleven',
  'third_party/mbleven.pyx',
  include_directories: inc,
)

# ryu (ulfjack) — fast double-to-string conversion
python.extension_module('ryu',
  'third_party/ulfjack/ryu.pyx',
  sources: [
    '../third_party/ulfjack/ryu/d2fixed.c',
  ],
  include_directories: inc,
)

# cyyjson (yyjson) — fast JSON parser wrapper
python.extension_module('cyyjson',
  'third_party/yyjson/cyyjson.pyx',
  sources: [
    '../third_party/yyjson/src/yyjson.c',
  ],
  include_directories: inc,
)

# ============================================================================
# OPERATOR EXTENSIONS
# ============================================================================

python.extension_module('_operators',
  'operators/_operators.pyx',
  override_options: ['cython_language=cpp'],
  sources: [
    '../src/cpp/hllpp.cpp',
    '../third_party/tdigest-c/src/tdigest_cpp.cpp',
  ],
  include_directories: [inc, inc_mabel, include_directories('operators/aggregate')],
)

# ============================================================================
# NANOBIND EXTENSIONS (pure C++ with nanobind Python bindings)
# ============================================================================

inc_nanobind = include_directories(
  '../third_party/nanobind',
  '../third_party/nanobind/src',
  '../third_party/nanobind/ext/robin_map/include',
)

nb_cpp_args = ['-fno-strict-aliasing', '-DNB_COMPACT_ASSERTIONS']

# disk_reader — memory-mapped file I/O and directory listing
python.extension_module('disk_reader',
  '../src/cpp/disk_reader_native.cpp',
  sources: [
    '../src/cpp/disk_io.cpp',
    '../src/cpp/directories.cpp',
    '../third_party/nanobind/src/nb_combined.cpp',
  ],
  include_directories: [inc, inc_nanobind],
  cpp_args: nb_cpp_args,
)

# ============================================================================
# CONNECTOR EXTENSIONS
# ============================================================================

python.extension_module('pool_reader',
  'connectors/parquet_io/pool_reader.pyx',
  override_options: ['cython_language=cpp'],
  sources: [
    '../rugo/src/parquet/decode_column.cpp',
    '../rugo/src/parquet/decode.cpp',
    '../rugo/src/parquet/compression.cpp',
    '../rugo/src/parquet/metadata.cpp',
    '../rugo/src/parquet/bloom_filter.cpp',
    '../rugo/src/parquet/page_value_decoder.cpp',
    '../rugo/src/parquet/decode_encodings.cpp',
    '../rugo/src/parquet/decode_page.cpp',
    '../src/cpp/cpu_features.cpp',
    '../src/cpp/http_client.cpp',
    '../third_party/snappy/snappy.cc',
    '../third_party/snappy/snappy-sinksource.cc',
    '../third_party/snappy/snappy-stubs-internal.cc',
    '../third_party/zstd/common/entropy_common.cpp',
    '../third_party/zstd/common/fse_decompress.cpp',
    '../third_party/zstd/common/zstd_common.cpp',
    '../third_party/zstd/common/xxhash.cpp',
    '../third_party/zstd/common/error_private.cpp',
    '../third_party/zstd/decompress/zstd_decompress.cpp',
    '../third_party/zstd/decompress/zstd_decompress_block.cpp',
    '../third_party/zstd/decompress/huf_decompress.cpp',
    '../third_party/zstd/decompress/zstd_ddict.cpp',
  ],
  include_directories: [inc, inc_mabel, inc_pool_reader],
  cpp_args: ['-DHAVE_SNAPPY=1', '-DHAVE_ZSTD=1', '-DZSTD_STATIC_LINKING_ONLY=1'],
  link_args: ['-lcurl'],
)
"""
    return header + "\n".join(body_parts) + footer


if __name__ == "__main__":
    out_path = OPTERYX_DIR / "meson.build"
    content = generate_meson_build()
    out_path.write_text(content)
    print(f"Generated {out_path} ({len(content)} bytes)")

    # Count extensions
    count = content.count("python.extension_module(")
    print(f"Total extension modules: {count}")
