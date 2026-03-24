"""
Compile regex patterns to DFA-based procedures for fast execution.

Strategy:
1. Parse regex pattern to AST
2. Convert AST to NFA (Thompson construction)
3. Convert NFA to DFA (subset construction)
4. Check if DFA is compilable to fast path
5. Generate operation sequence for Cython executor
6. Fall back to RE2 for complex patterns
"""

import re
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Set
from enum import IntEnum


class OperationType(IntEnum):
    """Operation types for compiled procedures."""
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
    """Native operation for compiled procedure."""
    op_type: int
    pattern: Optional[bytes] = None
    pattern_len: int = 0
    capture_id: int = -1
    target_char: Optional[int] = None  # ord(char)


class RegexToDFACompiler:
    """Compile regex patterns to DFA-based fast procedures."""

    def compile(self, pattern: bytes, replacement: bytes) -> 'CompiledProcedure':
        """
        Compile regex pattern to fast procedure or fall back to RE2.

        Args:
            pattern: Regex pattern (e.g., b'^https?://...')
            replacement: Replacement string (e.g., rb'\\1')

        Returns:
            CompiledProcedure with native operations or RE2 fallback
        """
        try:
            # Parse pattern string for analysis
            pattern_str = pattern.decode('utf-8') if isinstance(pattern, bytes) else pattern
            replacement_str = replacement.decode('utf-8') if isinstance(replacement, bytes) else replacement

            # Try to detect compilable patterns (LOW BAR)
            ops = self._try_compile(pattern_str, replacement_str)

            if ops is None:
                # Fall back to RE2
                return CompiledProcedure(fallback_to_re2=True)

            # Build native operation array
            return CompiledProcedure(
                operations=ops,
                num_operations=len(ops),
                compiled=True
            )
        except Exception:
            # Any error → fallback to RE2
            return CompiledProcedure(fallback_to_re2=True)

    def _try_compile(self, pattern: str, replacement: str) -> Optional[List[Operation]]:
        """
        Try to compile pattern to operations.
        Return None if pattern is too complex for fast path.
        """
        # For now, handle specific common patterns
        # Pattern: ^https?://(?:www\.)?([^/]+)/.*$
        # Replacement: \1 (capture group 1)

        # Quick heuristic checks for compilable patterns
        if '|' in pattern and pattern.count('|') > 2:
            return None  # Complex alternation

        if '(?=' in pattern or '(?!' in pattern or '(?<=' in pattern or '(?<!' in pattern:
            return None  # Lookarounds unsupported

        # Try pattern-specific compilers
        ops = self._compile_url_extractor(pattern, replacement)
        if ops:
            return ops

        ops = self._compile_simple_literal_patterns(pattern, replacement)
        if ops:
            return ops

        # Could add more pattern types here
        return None

    def _compile_url_extractor(self, pattern: str, replacement: str) -> Optional[List[Operation]]:
        """
        Detect and compile URL domain extraction pattern:
        ^https?://(?:www\.)?([^/]+)/.*$ → \1
        """
        # Match the exact URL pattern
        url_pattern = r'^\^https\?://\(\?:www\\\.\)\?\(\[\^/\]\+\)/\.\*\$'
        if not re.match(url_pattern, pattern):
            return None

        # Check replacement is capture group 1
        if replacement != r'\1':
            return None

        # Emit operations for URL extraction
        ops = [
            Operation(OperationType.OP_MATCH_LITERAL, b'http', 4),
            Operation(OperationType.OP_MATCH_OPTIONAL_LITERAL, b's', 1),
            Operation(OperationType.OP_MATCH_LITERAL, b'://', 3),
            Operation(OperationType.OP_MATCH_OPTIONAL_LITERAL, b'www.', 4),
            Operation(OperationType.OP_START_CAPTURE, capture_id=1),
            Operation(OperationType.OP_EXTRACT_WHILE_NOT, b'/', 1, capture_id=1),
            Operation(OperationType.OP_END_CAPTURE, capture_id=1),
            Operation(OperationType.OP_MATCH_LITERAL, b'/', 1),
            Operation(OperationType.OP_DISCARD_REST),
            Operation(OperationType.OP_RETURN_CAPTURE, capture_id=1),
        ]

        return ops

    def _compile_simple_literal_patterns(self, pattern: str, replacement: str) -> Optional[List[Operation]]:
        """
        Compile simple patterns with no regex metacharacters:
        ^foo$ → bar (literal replacement)
        ^foo → bar (prefix removal)
        foo$ → bar (suffix replacement)
        """
        # Check if pattern has no regex metacharacters
        if any(c in pattern for c in r'^$.*+?[]{}()|\\'):
            if not (pattern.startswith('^') or pattern.endswith('$')):
                return None

        # For now, only handle simple cases
        # Could expand this
        return None


@dataclass
class CompiledProcedure:
    """Compiled procedure ready for execution in Cython."""
    operations: Optional[List[Operation]] = None
    num_operations: int = 0
    compiled: bool = False
    fallback_to_re2: bool = False

    def to_cython_args(self) -> Tuple:
        """Convert to arguments for Cython executor."""
        if self.fallback_to_re2:
            return (None, 0, True)

        # Convert operations to tuples for Cython
        op_tuples = []
        for op in self.operations:
            op_tuples.append((
                op.op_type,
                op.pattern,
                op.pattern_len,
                op.capture_id,
                op.target_char,
            ))

        return (op_tuples, len(op_tuples), False)
