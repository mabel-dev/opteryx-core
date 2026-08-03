"""Vector (nearest-neighbour) top-k fusion.

`OperatorFusionStrategy` fuses ORDER BY + LIMIT into a HeapSort, and flags it
`vector_topk_candidate` when the sort key is a nearest-neighbour distance over a
NUMERIC_VECTOR column. The flag is what keeps `TopNScanPushdownStrategy` off the
node, so which queries earn it is a real behavioural decision.

Fixture note: the vector source must be a NUMERIC_VECTOR, and an array/tuple
literal binds as ARRAY — so these use `CAST(embedding AS VECTOR(2))`, which is
the constructible form `get_vector_source_identifier` accepts. A bare `VECTOR`
with no dimension is refused (an ARRAY column's row lengths vary, so a width
cannot be inferred).
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest  # noqa: E402

import opteryx  # noqa: E402
from opteryx.exceptions import IncompatibleTypesError  # noqa: E402

# Three 2-d vectors and the query vector (1.0, 0.0):
#   match      (1,0) -> cosine distance 0.0
#   diagonal   (1,1) -> 0.293
#   orthogonal (0,1) -> 1.0
_VECTORS = """
    FROM (
        VALUES
            ('match', (1.0, 0.0)),
            ('diagonal', (1.0, 1.0)),
            ('orthogonal', (0.0, 1.0))
    ) AS vectors(label, embedding)
"""

_STRING_ARRAYS = """
    FROM (
        VALUES
            ('match', ('cape', 'canaveral')),
            ('diagonal', ('baikonur', 'kazakhstan')),
            ('orthogonal', ('vandenberg', 'california'))
    ) AS vectors(label, embedding)
"""


def _run(query: str):
    """Execute `query`; return (labels, session). Caller closes the session."""
    session = opteryx.session()
    try:
        labels = []
        for morsel in session.execute_to_morsels(query):
            if morsel.num_rows:
                labels += morsel.column(b"label").to_pylist()
        return labels, session
    except Exception:
        session.close()
        raise


def _fused(session) -> bool:
    return session.telemetry.get("optimization_fuse_operators_vector_heap_sort") is not None


def test_vector_topk_query_renders_vector_heap_sort():
    """ASC on COSINE_DISTANCE is nearest-neighbour — it fuses, and answers correctly."""
    labels, session = _run(
        f"""
        SELECT label {_VECTORS}
        ORDER BY COSINE_DISTANCE(CAST(embedding AS VECTOR(2)), CAST([1.0, 0.0] AS VECTOR(2)))
        LIMIT 2
        """
    )
    try:
        assert _fused(session)
        assert labels == ["match", "diagonal"]  # the two NEAREST, in order
    finally:
        session.close()


def test_cosine_similarity_desc_renders_vector_heap_sort():
    """DESC on COSINE_SIMILARITY is the same nearest-neighbour question, so it fuses too."""
    labels, session = _run(
        f"""
        SELECT label {_VECTORS}
        ORDER BY COSINE_SIMILARITY(CAST(embedding AS VECTOR(2)), CAST([1.0, 0.0] AS VECTOR(2))) DESC
        LIMIT 2
        """
    )
    try:
        assert _fused(session)
        assert labels == ["match", "diagonal"]
    finally:
        session.close()


def test_non_nearest_neighbor_vector_sort_does_not_render_vector_heap_sort():
    """DESC on COSINE_DISTANCE asks for the FARTHEST rows — not a nearest-neighbour
    search, so it must not be flagged as vector top-k."""
    labels, session = _run(
        f"""
        SELECT label {_VECTORS}
        ORDER BY COSINE_DISTANCE(CAST(embedding AS VECTOR(2)), CAST([1.0, 0.0] AS VECTOR(2))) DESC
        LIMIT 2
        """
    )
    try:
        assert not _fused(session)
        assert labels == ["orthogonal", "diagonal"]  # the two FARTHEST
    finally:
        session.close()


def test_string_array_sort_is_rejected():
    """A string array is not a vector. It never reaches the fusion — the binder
    rejects COSINE_DISTANCE over an ARRAY before planning gets that far."""
    with pytest.raises(IncompatibleTypesError):
        _run(
            f"""
            SELECT label {_STRING_ARRAYS}
            ORDER BY COSINE_DISTANCE(embedding, ('cape', 'canaveral'))
            LIMIT 2
            """
        )


def test_embed_literal_vector_sort_renders_vector_heap_sort():
    """EMBED(<literal>) is accepted as the query vector — `node_is_vector_query_expression`
    admits a constant EMBED call alongside a vector column and a vector literal.

    Asserted through EXPLAIN rather than execution: the fusion is a planner decision
    and EXPLAIN exercises the full plan+optimize path, but EXECUTING this query hits
    an unrelated gap — EMBED in an ORDER BY sort key raises NotSupportedError from the
    native engine ("a computed expression outside the c-native kernel set"). Reported
    separately; when that gap closes this should become an executing test like the rest.

    No embedding provider is registered: EMBED is a native kernel (a static hashed
    projection), not a pluggable Python provider.
    """
    session = opteryx.session()
    try:
        plan_text = ""
        for morsel in session.execute_to_morsels(
            f"""
            EXPLAIN SELECT label {_VECTORS}
            ORDER BY COSINE_DISTANCE(CAST(embedding AS VECTOR(2)), EMBED('match'))
            LIMIT 2
            """
        ):
            if morsel.num_rows:
                plan_text += "\n".join(
                    v.decode("utf-8") if isinstance(v, (bytes, bytearray)) else str(v)
                    for v in morsel.column(b"tree").to_pylist()
                )
        assert "fuse operators vector heap sort" in plan_text, plan_text
        assert _fused(session)
    finally:
        session.close()


def _cosine_node():
    """A bound COSINE_DISTANCE(CAST(col AS VECTOR(2)), <vector literal>) sort key."""
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.ast_rewriter import do_ast_rewriter
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.planner.binder import do_bind_phase
    from opteryx.models import QueryTelemetry
    from opteryx.third_party import sqloxide
    from opteryx.planner.logical_planner import LogicalPlanStepType

    sql = f"""
        SELECT label {_VECTORS}
        ORDER BY COSINE_DISTANCE(CAST(embedding AS VECTOR(2)), CAST([1.0, 0.0] AS VECTOR(2)))
        LIMIT 2
    """
    tokens = sqloxide.parse_sql(do_sql_rewrite(sql), "opteryx")
    plan, _, _ = do_logical_planning_phase(do_ast_rewriter(tokens, {})[0])
    bound = do_bind_phase(plan, QueryTelemetry("test_vector_topk_mutual_exclusion"))
    for _, node in bound.nodes(True):
        if node.node_type == LogicalPlanStepType.Order:
            return node.order_by[0][0]
    raise AssertionError("no Order node in the bound plan")


def test_vector_topk_sort_key_is_never_a_topn_pushdown_candidate():
    """The two strategies are mutually exclusive BY CONSTRUCTION, which is why
    TopNScanPushdownStrategy needs no vector special-case.

    A guard for the vector case used to sit in the pushdown, claiming the flagged
    node "has its own fused path". There is no such path, and the guard could never
    change the outcome: the pushdown admits only a plain column reference, and a
    vector top-k key is a COSINE_* call. This pins the property it stood in for, so
    removing the guard cannot silently start mattering.
    """
    from opteryx.expression import NodeType
    from opteryx.planner.optimizer.strategies.operator_fusion import OperatorFusionStrategy

    sort_key = _cosine_node()
    assert OperatorFusionStrategy._is_vector_topk_candidate([(sort_key, True)])

    # TopNScanPushdownStrategy stamps a scan only when the sort key is IDENTIFIER
    # ("the sort key must be a plain column reference"). A vector candidate never is.
    assert sort_key.node_type != NodeType.IDENTIFIER


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
