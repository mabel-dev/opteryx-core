"""Expression evaluation engines for Draken and Arrow vectors."""

from .comparisons import draken_compare

# Main evaluation API
from .evaluation import evaluate_and_append_draken
from .evaluation import evaluate_draken

# Function execution
from .function_execution import apply_bounded_function


def _verify_node_type_constants():
    """Fail-fast check: the compile-time DEF constants in evaluation.pyx must
    match the runtime NodeType enum. If this assertion fires, update the DEFs
    at the top of evaluation.pyx and rebuild.
    """
    from opteryx.expression import NodeType

    expected = {
        "UNKNOWN": 0,
        "AND": 17, "OR": 18, "XOR": 19, "NOT": 20, "DNF": 21, "CNF": 22,
        "CASE": 32, "WILDCARD": 33, "COMPARISON_OPERATOR": 34,
        "BINARY_OPERATOR": 35, "UNARY_OPERATOR": 36, "FUNCTION": 37,
        "IDENTIFIER": 38, "SUBQUERY": 39, "NESTED": 40, "AGGREGATOR": 41,
        "LITERAL": 42, "EXPRESSION_LIST": 43, "EVALUATED": 44, "CAST": 45,
        "EXTRACTION_OPERATOR": 46, "BETWEEN": 47,
    }
    for name, value in expected.items():
        actual = int(getattr(NodeType, name))
        if actual != value:
            raise AssertionError(
                f"NodeType.{name} = {actual}, but evaluation.pyx DEF expects {value}. "
                f"Update the DEF constants at the top of "
                f"opteryx/expression/evaluator/evaluation.pyx and rebuild."
            )


__all__ = [
    "evaluate_draken",
    "evaluate_and_append_draken",
    "draken_compare",
    "apply_bounded_function",
]
