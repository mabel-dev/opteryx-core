# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False

"""
vector_ops/vector_dfa_replace.pyx

Vector-level DFA regex replacement.

Provides a single entry point:

    vector_dfa_replace(data: StringVector, pattern: bytes, replacement: bytes)
        -> StringVector

This mirrors the calling convention of vector_regex_replace so callers in the
vector_ops layer do not need to know which engine is used.  Internally:

  1. The pattern + normalised replacement are compiled to a DFA op-sequence by
     the Python-level RegexToDFACompiler (result cached with lru_cache so
     compilation cost is paid once per unique (pattern, replacement) pair
     across all morsels in a query).

  2. If the pattern is compilable, the DFA executor in
     opteryx.compiled.functions.regex_procedures is called — pure C, no Python
     objects in the per-row hot path.

  3. If the pattern is not compilable (DFA compiler returns fallback=True), the
     call is forwarded to vector_regex_replace (RE2 engine) so callers never
     need to implement the fallback themselves.

The lru_cache sits at the Python module level so it survives across calls from
text.py and any other consumer.  Cache entries are keyed on (pattern, replacement)
bytes pairs; 64 entries covers all realistic query workloads.
"""

import functools

from opteryx.compiled.draken.vectors.string_vector cimport StringVector


# ---------------------------------------------------------------------------
# Module-level compilation cache
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=64)
def _get_dfa_ops(pattern: bytes, replacement: bytes):
    """
    Compile (pattern, replacement) to a DFA op-sequence.

    Returns (ops, num_ops, fallback_to_re2) — the same tuple produced by
    CompiledProcedure.to_cython_args().  Result is cached so the compiler
    is only invoked once per unique (pattern, replacement) pair.

    Replacement normalisation (SQL r'\\1' → r'\1') is applied here so the
    cache key is the raw bytes from the caller; normalisation is transparent.
    """
    from opteryx.expression.functions.implementations.text import _normalise_replacement
    from opteryx.utils.regex_compiler import RegexToDFACompiler

    norm = _normalise_replacement(replacement)
    proc = RegexToDFACompiler().compile(pattern, norm)
    return proc.to_cython_args()


# ---------------------------------------------------------------------------
# Public vector entry point
# ---------------------------------------------------------------------------

cpdef StringVector vector_dfa_replace(
    StringVector data,
    bytes        pattern,
    bytes        replacement,
):
    """
    DFA-based regex replacement over a StringVector.

    Parameters
    ----------
    data:
        Input StringVector.
    pattern:
        Regex pattern bytes (e.g. b'^https?://(?:www\\.)?([^/]+)/.*$').
    replacement:
        Replacement bytes (e.g. b'\\\\1').  SQL double-backslash forms are
        normalised automatically.

    Returns
    -------
    StringVector
        Per-row results.  Rows with no match or null input are null.
        Falls back to RE2 transparently for patterns that cannot be
        compiled to a DFA op-sequence.
    """
    from opteryx.compiled.functions.regex_procedures import execute_regex_procedure
    from opteryx.compiled.vector_ops.vector_regex_replace import vector_regex_replace

    ops, num_ops, fallback = _get_dfa_ops(pattern, replacement)

    if fallback:
        return vector_regex_replace(data, pattern, replacement)

    return execute_regex_procedure(data, ops, num_ops, False)
