"""
Test parallelization of selection and projection operations
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../.."))


def test_filter_node_is_stateless():
    """Test that FilterNode is registered as stateless"""
    from opteryx.operators.catalog import get_registry
    from opteryx.operators.filter import FilterNode

    assert get_registry().get(FilterNode).is_stateless is True, "FilterNode should be stateless"


def test_projection_node_is_stateless():
    """Test that ProjectionNode is registered as stateless"""
    from opteryx.operators.catalog import get_registry
    from opteryx.operators.projection import ProjectionNode

    assert (
        get_registry().get(ProjectionNode).is_stateless is True
    ), "ProjectionNode should be stateless"


if __name__ == "__main__":  # pragma: no cover
    from tests.tools import run_tests

    run_tests()
