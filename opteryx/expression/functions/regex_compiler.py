"""
Compile anchored regex patterns to DFA-based procedures for fast execution.

The compiler walks the sre_parse AST produced by Python's internal regex
parser to translate patterns into sequences of native ops that the Cython
executor in compiled/functions/regex_procedures.pyx can run without RE2
overhead.

Supported subset (replacement must be r'\\1'):
  ^  …  $          Fully anchored at both ends (required).
  literal chars    → OP_MATCH_LITERAL
  x?               → OP_MATCH_OPTIONAL_LITERAL  (single byte)
  (?:lit*)?        → OP_MATCH_OPTIONAL_LITERAL  (multi-byte literal group)
  ([^c]+)          → OP_EXTRACT_WHILE_NOT(stop_char=c)   — capture group 1
  (.+)             → OP_EXTRACT_WHILE_NOT(stop_char=\\0) — capture to end
  .*               → OP_DISCARD_REST  (tail, before $)

Anything outside this subset (alternation, lookaheads, back-references,
non-trivial character classes, multiple capture groups, non-\\1 replacement)
causes the compiler to return fallback_to_re2=True and RE2 handles the query.
"""

import sre_constants
import sre_parse
from dataclasses import dataclass
from enum import IntEnum
from typing import List, Optional, Tuple


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
    """Single operation in a compiled procedure."""

    op_type: int
    pattern: Optional[bytes] = None
    pattern_len: int = 0
    capture_id: int = -1
    target_char: Optional[int] = None  # ord(char), or None → null-byte sentinel


class RegexToDFACompiler:
    """Compile regex patterns to DFA-based fast procedures."""

    def compile(self, pattern: bytes, replacement: bytes) -> "CompiledProcedure":
        """
        Attempt to compile (pattern, replacement) to a native op-sequence.

        Returns a CompiledProcedure with compiled=True on success, or
        fallback_to_re2=True when the pattern cannot be handled natively.
        Any unexpected exception is caught and treated as a fallback.
        """
        try:
            pattern_str = pattern.decode("utf-8") if isinstance(pattern, bytes) else pattern
            replacement_str = (
                replacement.decode("utf-8") if isinstance(replacement, bytes) else replacement
            )
            ops = self._try_compile(pattern_str, replacement_str)
            if ops is None:
                return CompiledProcedure(fallback_to_re2=True)
            return CompiledProcedure(operations=ops, num_operations=len(ops), compiled=True)
        except Exception:
            return CompiledProcedure(fallback_to_re2=True)

    # ------------------------------------------------------------------
    # Internal compiler
    # ------------------------------------------------------------------

    def _try_compile(self, pattern: str, replacement: str) -> Optional[List[Operation]]:
        # Cheap pre-flight: reject constructs sre_parse accepts but we never
        # support, so we avoid paying parse overhead for them.
        if "(?=" in pattern or "(?!" in pattern or "(?<=" in pattern or "(?<!" in pattern:
            return None  # lookaheads / lookbehinds

        return self._try_compile_generic(pattern, replacement)

    def _try_compile_generic(self, pattern: str, replacement: str) -> Optional[List[Operation]]:
        """
        Generic AST-walking compiler.

        Parses `pattern` with sre_parse, then walks the node list emitting
        ops for every construct in the supported subset.  Returns None for
        any unsupported construct, which causes the caller to fall back to RE2.
        """
        # Only \\1 replacement is supported: the executor returns capture group 1.
        if replacement != r"\1":
            return None

        try:
            parsed = sre_parse.parse(pattern)
        except Exception:
            return None

        nodes = list(parsed)

        # Pattern must be fully anchored: AT_BEGINNING at front, AT_END at back.
        if (
            len(nodes) < 2
            or nodes[0] != (sre_constants.AT, sre_constants.AT_BEGINNING)
            or nodes[-1] != (sre_constants.AT, sre_constants.AT_END)
        ):
            return None

        nodes = nodes[1:-1]  # strip anchors; they are enforced structurally

        ops: List[Operation] = []
        capture_emitted = False
        i = 0

        while i < len(nodes):
            opcode, av = nodes[i]

            # ----------------------------------------------------------
            # Run of consecutive LITERAL nodes → single OP_MATCH_LITERAL
            # ----------------------------------------------------------
            if opcode == sre_constants.LITERAL:
                run = bytearray()
                while i < len(nodes) and nodes[i][0] == sre_constants.LITERAL:
                    run.append(nodes[i][1])
                    i += 1
                ops.append(Operation(OperationType.OP_MATCH_LITERAL, bytes(run), len(run)))
                continue  # i already advanced inside the inner loop

            # ----------------------------------------------------------
            # MAX_REPEAT — handles x?, .*, and rejects everything else
            # ----------------------------------------------------------
            elif opcode == sre_constants.MAX_REPEAT:
                min_r, max_r, sub = av
                sub = list(sub)

                if min_r == 0 and max_r == 1:
                    # Optional construct: must reduce to a literal byte string.
                    lit = self._nodes_to_literal_bytes(sub)
                    if lit is None:
                        return None
                    ops.append(Operation(OperationType.OP_MATCH_OPTIONAL_LITERAL, lit, len(lit)))

                elif min_r == 0 and max_r == sre_constants.MAXREPEAT:
                    # Tail discard .*  — must be exactly [(ANY, None)]
                    if sub == [(sre_constants.ANY, None)]:
                        ops.append(Operation(OperationType.OP_DISCARD_REST))
                    else:
                        return None  # e.g. [a-z]* — unsupported

                else:
                    # + or {n,m} outside a capture group — not supported here
                    return None

            # ----------------------------------------------------------
            # SUBPATTERN — capturing (group ≥ 1) or non-capturing (None)
            # ----------------------------------------------------------
            elif opcode == sre_constants.SUBPATTERN:
                # Python 3.6+ always produces a 4-tuple:
                # (group_id, add_flags, del_flags, sub_pattern)
                group_id, _add_flags, _del_flags, sub = av
                sub = list(sub)

                if group_id is None:
                    # Non-capturing group (?:…): inline its literal content.
                    lit = self._nodes_to_literal_bytes(sub)
                    if lit is None:
                        return None
                    ops.append(Operation(OperationType.OP_MATCH_LITERAL, lit, len(lit)))
                else:
                    # Capturing group.  We only support group 1 and only once.
                    if group_id != 1 or capture_emitted:
                        return None
                    op = self._compile_capture_content(sub)
                    if op is None:
                        return None
                    ops.append(op)
                    capture_emitted = True

            else:
                # Any other opcode (BRANCH, IN, GROUPREF, AT mid-pattern, …)
                return None

            i += 1

        if not capture_emitted:
            return None

        ops.append(Operation(OperationType.OP_RETURN_CAPTURE, capture_id=1))
        return ops

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _nodes_to_literal_bytes(self, nodes: list) -> Optional[bytes]:
        """
        Return the literal byte string represented by `nodes`, or None.

        Handles:
          - A run of LITERAL nodes                     → bytes of their char values
          - A single non-capturing SUBPATTERN wrapping
            a literal run (e.g. (?:www\\.))            → recurse into its content

        Everything else returns None, causing the caller to reject the pattern.
        """
        if not nodes:
            return None

        # Single non-capturing group — unwrap and recurse.
        if len(nodes) == 1 and nodes[0][0] == sre_constants.SUBPATTERN:
            group_id, _add, _del, sub = nodes[0][1]
            if group_id is not None:
                return None  # capturing group is not a literal
            return self._nodes_to_literal_bytes(list(sub))

        # Every node must be a plain LITERAL.
        if not all(n[0] == sre_constants.LITERAL for n in nodes):
            return None

        return bytes(n[1] for n in nodes)

    def _compile_capture_content(self, nodes: list) -> Optional[Operation]:
        """
        Compile the body of capture group 1 to a single extraction op.

        Supported bodies:
          [^c]+   →  OP_EXTRACT_WHILE_NOT(target_char=c,    capture_id=1)
          .+      →  OP_EXTRACT_WHILE_NOT(target_char=None, capture_id=1)
                     (None becomes the null-byte sentinel in Cython; avx_search
                      returns -1 for normal UTF-8 text so the full remainder is
                      captured.  OP_RETURN_CAPTURE enforces non-empty via
                      cap.start < cap.end.)

        Everything else returns None.
        """
        # Capture body must be exactly one MAX_REPEAT node.
        if len(nodes) != 1 or nodes[0][0] != sre_constants.MAX_REPEAT:
            return None

        min_r, max_r, sub = nodes[0][1]
        sub = list(sub)

        # Must be one-or-more (+).
        if min_r != 1 or max_r != sre_constants.MAXREPEAT:
            return None

        # .+  →  capture everything to end of string
        if sub == [(sre_constants.ANY, None)]:
            return Operation(
                OperationType.OP_EXTRACT_WHILE_NOT,
                capture_id=1,
                target_char=None,
            )

        # [^c]+  →  capture until the single excluded character c
        # Some Python versions/AST shapes represent this as an IN class:
        #   (IN, [(NEGATE, None), (LITERAL, c)])
        if len(sub) == 1 and sub[0][0] == sre_constants.IN:
            cls = list(sub[0][1])
            # Character class must be exactly [NEGATE, LITERAL(c)].
            if (
                len(cls) == 2
                and cls[0] == (sre_constants.NEGATE, None)
                and cls[1][0] == sre_constants.LITERAL
            ):
                return Operation(
                    OperationType.OP_EXTRACT_WHILE_NOT,
                    capture_id=1,
                    target_char=cls[1][1],
                )

        # Some Python versions may emit a NOT_LITERAL node directly for [^c],
        # e.g. (NOT_LITERAL, ord(c)). Accept that shape as equivalent.
        if len(sub) == 1 and sub[0][0] == getattr(sre_constants, "NOT_LITERAL", None):
            # sub[0][1] should be the ord() of the excluded char.
            target = sub[0][1]
            return Operation(
                OperationType.OP_EXTRACT_WHILE_NOT,
                capture_id=1,
                target_char=target,
            )

        return None


# ------------------------------------------------------------------
# Compiled result
# ------------------------------------------------------------------


@dataclass
class CompiledProcedure:
    """Compiled procedure ready for execution in Cython."""

    operations: Optional[List[Operation]] = None
    num_operations: int = 0
    compiled: bool = False
    fallback_to_re2: bool = False

    def to_cython_args(self) -> Tuple:
        """
        Serialise to the 3-tuple expected by execute_regex_procedure:
            (op_tuples | None, num_ops, fallback_to_re2)

        Each op tuple is:
            (op_type, pattern_bytes | None, pattern_len, capture_id, target_char | None)
        """
        if self.fallback_to_re2:
            return (None, 0, True)

        op_tuples = [
            (op.op_type, op.pattern, op.pattern_len, op.capture_id, op.target_char)
            for op in self.operations
        ]
        return (op_tuples, len(op_tuples), False)
