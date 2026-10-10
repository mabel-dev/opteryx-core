"""
Bind-time compilation of RLIKE / NOT RLIKE patterns.

The draken RLIKE kernel accepts only a pre-compiled blob as its pattern; there is
no runtime regex interpreter to fall back to. Compiling the pattern is therefore a
LOWERING, not an optimization, and it lives in the binder, which every query
passes through. It used to live in PredicateRewriteStrategy, and turning that
strategy off (`disable_predicate_rewrite`) handed the raw regex text to the scan's
predicate kernel, so every RLIKE failed with a kernel error.
"""

from opteryx.exceptions import NotSupportedError
from opteryx.expression import NodeType
from opteryx.types import logical_type as _lt


def fresh_literal(literal, *, plan_context):
    """A NEW literal node with `literal`'s fields and its OWN ConstantColumn.

    `build_literal_node(value, identity_of=X)` retypes `X.schema_column` in place,
    and a literal's ConstantColumn can be shared between nodes. Handing it this copy
    instead keeps the original untouched while the rebuilt literal keeps its display
    name.
    """
    name = literal.schema_column.name if literal.schema_column is not None else None
    return literal.replace(schema_column=plan_context.columns.constant(name))


def compile_rlike_pattern(predicate, *, plan_context) -> None:
    """RLike/NotRLike with a literal pattern: compile the pattern into a byte
    DFA at bind time (RE2's parser only — see vector_dfa_compile.pyx's module
    docstring) and make the compiled blob the node's pattern operand.

    The node being bound is updated IN PLACE, never replaced: the plan step that
    owns it (a projection list, a filter) keeps its own reference, and a
    replacement node would leave that reference pointing at an unbound node —
    `SELECT name RLIKE 'u'` then fails with "an output column the engine could not
    resolve". Only the operand is swapped; the original pattern literal is never
    overwritten, so anything else holding it still sees the regex text.

    A non-literal pattern, or a literal pattern outside the compiler's
    supported scope (non-ASCII content, case-fold, nested anchors, or a
    state-count blowup), raises NotSupportedError here rather than reaching
    vector_rlike at runtime with a pattern it cannot interpret.
    """
    if predicate.value not in ("RLike", "NotRLike"):
        raise AssertionError(f"compile_rlike_pattern called on {predicate.value}")
    if predicate.right.node_type != NodeType.LITERAL:
        raise NotSupportedError(
            "**RLIKE**/REGEXP_LIKE requires a constant pattern — "
            f"got a non-literal expression for {predicate.value}."
        )

    # Already compiled: the pattern operand IS the blob. Compiling it again would
    # read the blob's bytes as a regex. A node is bound more than once when a
    # plan is re-bound (views, CTE bodies).
    if predicate.right.rlike_compiled:
        return

    pattern_value = predicate.right.value
    if isinstance(pattern_value, str):
        pattern_value = pattern_value.encode("utf8")
    elif not isinstance(pattern_value, bytes):
        raise NotSupportedError(
            f"**RLIKE**/REGEXP_LIKE pattern must be a string constant, got {type(pattern_value)!r}."
        )

    from opteryx.compiled import vector_ops as compiled_vector_ops

    # Prefer the SIMD op-program (blob version 2) when the pattern decomposes to
    # ASCII literals joined by `.*`/`.+` with optional `^`/`$` anchors — it beats
    # the transition-table DFA (blob version 1) on short and long columns alike.
    # The blob's version byte tells compiled_expression which kernel to dispatch
    # (draken_like_program vs draken_rlike). Non-decomposable patterns fall
    # through to the DFA, which stays the correct, fully-general path.
    compiled_blob = compiled_vector_ops.compile_rlike_program(pattern_value)
    if compiled_blob is None:
        compiled_blob = compiled_vector_ops.compile_rlike_dfa(pattern_value)
    if compiled_blob is None:
        raise NotSupportedError(
            "**RLIKE**/REGEXP_LIKE pattern is outside the supported regex dialect "
            "(no lookaround/backreferences/case-fold, ASCII pattern content only, "
            "anchors only at the outermost start/end, and the compiled automaton "
            f"must stay within the state-count cap): {pattern_value!r}"
        )

    from opteryx.planner import build_literal_node

    blob = build_literal_node(
        compiled_blob,
        identity_of=fresh_literal(predicate.right, plan_context=plan_context),
        suggested_type=_lt.VARBINARY,
        plan_context=plan_context,
    )
    blob.rlike_compiled = True
    predicate.right = blob
