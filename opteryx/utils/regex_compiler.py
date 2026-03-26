"""
Shared types and compiler entry-point for the regex → DFA-ops pipeline.

OperationType, Operation, and CompiledProcedure are defined here because
regex_compiler_native (the C++ / nanobind extension shim) imports them from
this module.  Keeping them here avoids a circular dependency.

RegexToDFACompiler delegates immediately to the native RE2-backed compiler
(opteryx.compiled.functions.regex_compiler_native).  If the native extension
is not available it returns fallback_to_re2=True so the caller uses RE2
directly — there is no sre_parse fallback.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import List, Optional, Tuple


class OperationType(IntEnum):
    OP_MATCH_LITERAL = 0
    OP_MATCH_OPTIONAL_LITERAL = 1
    OP_FIND_CHAR = 2
    OP_EXTRACT_UNTIL_CHAR = 3
    OP_EXTRACT_WHILE_NOT = 4
    OP_START_CAPTURE = 5
    OP_END_CAPTURE = 6
    OP_DISCARD_REST = 7
    OP_RETURN_CAPTURE = 8


@dataclass
class Operation:
    op_type: int
    pattern: Optional[bytes] = None
    pattern_len: int = 0
    capture_id: int = -1
    target_char: Optional[int] = None


@dataclass
class CompiledProcedure:
    operations: Optional[List[Operation]] = None
    num_operations: int = 0
    compiled: bool = False
    fallback_to_re2: bool = False

    def to_cython_args(self) -> Tuple:
        if self.fallback_to_re2:
            return (None, 0, True)
        # If operations is None or empty, treat as fallback to RE2
        if not self.operations:
            return (None, 0, True)
        op_tuples = [
            (op.op_type, op.pattern, op.pattern_len, op.capture_id, op.target_char)
            for op in self.operations
        ]
        return (op_tuples, len(op_tuples), False)


# ---------------------------------------------------------------------------
# Compiler — delegates to the native RE2-backed extension
# ---------------------------------------------------------------------------

try:
    # The native extension is named _regex_compiler_native (underscore prefix)
    # so it does not shadow this package's regex_compiler_native.py shim.
    from opteryx.compiled.functions._regex_compiler_native import compile_regex as _compile_regex

    class RegexToDFACompiler:  # type: ignore[no-redef]
        def compile(self, pattern: bytes, replacement: bytes) -> CompiledProcedure:
            try:
                pat = (
                    pattern.decode("utf-8") if isinstance(pattern, (bytes, bytearray)) else pattern
                )
                rep = (
                    replacement.decode("utf-8")
                    if isinstance(replacement, (bytes, bytearray))
                    else replacement
                )
                ops_list, num_ops, fallback = _compile_regex(pat, rep)
                if fallback or ops_list is None:
                    return CompiledProcedure(fallback_to_re2=True)
                operations = [
                    Operation(
                        int(ot),
                        pb.encode("utf-8") if pb is not None else None,
                        int(pl),
                        int(ci),
                        None if tc is None else int(tc),
                    )
                    for ot, pb, pl, ci, tc in ops_list
                ]
                return CompiledProcedure(
                    operations=operations, num_operations=len(operations), compiled=True
                )
            except Exception:
                return CompiledProcedure(fallback_to_re2=True)

except Exception:
    # Native extension not built — provide a stub that always falls back to RE2.
    class RegexToDFACompiler:  # type: ignore[no-redef]
        def compile(self, pattern: bytes, replacement: bytes) -> CompiledProcedure:
            return CompiledProcedure(fallback_to_re2=True)
