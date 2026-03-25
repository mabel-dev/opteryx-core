"""
Python shim for the native RE2-based regex -> DFA op compiler.

This module provides `RegexToDFACompiler` which delegates compilation to the
native binding (a nanobind/C++ translator compiled into
`opteryx.compiled.functions.regex_compiler_native`). Results are cached
(at the Python level) to avoid repeated native compilation for the same
(pattern, replacement) pair.

Return shape expected from the native binding `compile_regex(pattern, replacement)`:
    - on success: (ops_list, num_ops, False)
      where `ops_list` is an iterable of 5-tuples:
        (op_type:int, pattern:str|None, pattern_len:int, capture_id:int, target_char:int|None)
    - on fallback/parse error: (None, 0, True)

This shim converts the native return into `CompiledProcedure` / `Operation`
dataclasses used across the Python executor path.
"""

from functools import lru_cache
from typing import Optional, Tuple

# Normalisation helper lives with other text implementations.
from opteryx.expression.functions.implementations.text import _normalise_replacement

# Import the datatypes used by the rest of the Python codepath.
from opteryx.expression.functions.regex_compiler import (
    CompiledProcedure,
    Operation,
    OperationType,
)

# Try to import the native binding that will be built into the compiled functions
# package. If it is not available, the compiler will always fall back to RE2.
try:
    from opteryx.compiled.functions import regex_compiler_native as _native  # type: ignore

    _NATIVE_AVAILABLE = True
except Exception:
    _native = None  # type: ignore
    _NATIVE_AVAILABLE = False


class RegexToDFACompiler:
    """
    Wrapper that delegates regex->ops compilation to the native translator.

    Usage:
        compiler = RegexToDFACompiler()
        proc = compiler.compile(pattern_bytes, replacement_bytes)

    The compile call returns a `CompiledProcedure`. If the native translator
    is unavailable or the pattern is unsupported, the returned procedure will
    have `fallback_to_re2=True`.
    """

    def __init__(self, cache_size: int = 256):
        # We wrap the instance method with lru_cache at runtime so we can
        # memoize compiled results keyed by (pattern_str, replacement_str).
        # The bound method becomes a callable of one argument: the key tuple.
        self._cache_size = cache_size
        # create a cached wrapper around the instance method
        self._cached_compile = lru_cache(maxsize=self._cache_size)(self._compile_uncached)

    def compile(self, pattern: bytes, replacement: bytes) -> "CompiledProcedure":
        """
        Compile (pattern, replacement) to a `CompiledProcedure`.

        Pattern and replacement are expected to be bytes (the query engine
        frequently passes raw bytes). We normalise the replacement first
        (handles SQL escaped backreferences) and then call the native compiler.
        """
        # Normalise replacement to canonical single-backslash form (b'\1')
        norm_repl = _normalise_replacement(replacement)

        # Convert bytes -> str for the native binding (RE2 expects UTF-8).
        pat_str = (
            pattern.decode("utf-8") if isinstance(pattern, (bytes, bytearray)) else str(pattern)
        )
        repl_str = (
            norm_repl.decode("utf-8")
            if isinstance(norm_repl, (bytes, bytearray))
            else str(norm_repl)
        )

        # Use the cached native compile call keyed by the two strings.
        return self._cached_compile((pat_str, repl_str))

    def _compile_uncached(self, key: Tuple[str, str]) -> "CompiledProcedure":
        """
        Actual compile implementation invoked by the cached wrapper.

        Returns a CompiledProcedure. Any error or unavailability results in
        a fallback_to_re2=True proc.
        """
        pat_str, repl_str = key

        if not _NATIVE_AVAILABLE:
            return CompiledProcedure(fallback_to_re2=True)

        try:
            # Call native binding
            result = _native.compile_regex(pat_str, repl_str)
        except Exception:
            # Any error in native call -> fallback
            return CompiledProcedure(fallback_to_re2=True)

        # Expect a 3-tuple from native: (ops_list | None, num_ops, fallback_flag)
        try:
            ops_list, num_ops, fallback = result
        except Exception:
            return CompiledProcedure(fallback_to_re2=True)

        if fallback or ops_list is None:
            return CompiledProcedure(fallback_to_re2=True)

        # Convert native op tuples into Operation dataclasses
        operations = []
        try:
            for entry in ops_list:
                # Expect entry = (op_type, pattern_or_None, pattern_len, capture_id, target_char_or_None)
                op_type, pattern_val, pattern_len, capture_id, target_char = entry

                patt_bytes = None
                if pattern_val is not None:
                    # Native returns a Python str for patterns; encode to bytes for the existing executor
                    patt_bytes = pattern_val.encode("utf-8")

                targ = None if target_char is None else int(target_char)

                operations.append(
                    Operation(int(op_type), patt_bytes, int(pattern_len), int(capture_id), targ)
                )
        except Exception:
            # Any malformed result from native -> fallback to be safe
            return CompiledProcedure(fallback_to_re2=True)

        return CompiledProcedure(
            operations=operations, num_operations=len(operations), compiled=True
        )
