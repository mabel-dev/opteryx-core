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
        pyx_text = pyx_path.read_text(errors="replace")
        cpp_markers = ["cppclass", "libcpp.", "from libcpp", "except +", "new "]
        if any(marker in pyx_text for marker in cpp_markers):
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

    # C++ mode: only needed for PYX_ONLY files still invoking Cython
    # (.cpp/.c primary sources bypass Cython entirely — no cython_args needed)
    if is_cpp and not (cpp_companion.exists() or c_companion.exists()):
        lines.append("  cython_args: ['--cplus'],")

    # Use inc_mabel for extensions that have actual #include directives for
    # carchar/parvi headers (not just mentions in the embedded metadata comment).
    # Draken headers are in the base inc (opteryx always depends on draken).
    def has_mabel_include(path: Path) -> bool:
        """Return True if path has a real #include "carchar..." or #include "parvi..." line."""
        if not path.exists():
            return False
        text = path.read_text(errors="replace")
        # Strip metadata block before scanning to avoid false positives
        text = re.sub(r'/\*.*?Cython Metadata.*?\*/', '', text, flags=re.DOTALL)
        return bool(re.search(r'#include\s+[<"][^>"]*(?:carchar|parvi)', text))

    need_mabel = has_mabel_include(OPTERYX_DIR / primary_src)
    if not need_mabel:
        for src_rel in extra_sources:
            # extra_sources are relative to opteryx/ dir (e.g. ../src/cpp/foo.cpp)
            src_abs = (OPTERYX_DIR / src_rel).resolve()
            if has_mabel_include(src_abs):
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
  '../draken/src',
  '../draken/src/core',
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
