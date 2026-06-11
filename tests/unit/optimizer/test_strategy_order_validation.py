# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""WP-2: declared optimizer rule-ordering contract.

`_validate_strategy_order` turns the comment-enforced ordering dependencies
between optimizer strategies into an executable, construction-time assertion.
These tests prove the real pipeline satisfies its contract and that each class
of violation (misorder, unknown token) fails loudly.
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.exceptions import InvalidInternalStateError
from opteryx.models import QueryTelemetry
from opteryx.planner.optimizer import OptimizerVisitor
from opteryx.planner.optimizer import _validate_strategy_order


class _Stub:
    """Minimal stand-in for an OptimizationStrategy: only ordering attrs matter."""

    def __init__(self, provides=(), requires=()):
        self.provides = provides
        self.requires = requires


def test_real_pipeline_satisfies_ordering_contract():
    # Constructing the visitor runs _validate_strategy_order; must not raise.
    visitor = OptimizerVisitor(QueryTelemetry())
    assert len(visitor.strategies) > 0
    # And the validator is happy when called directly on the live list.
    _validate_strategy_order(visitor.strategies)


def test_valid_order_passes():
    strategies = [
        _Stub(provides=("a",)),
        _Stub(requires=("a",), provides=("b",)),
        _Stub(requires=("a", "b")),
    ]
    _validate_strategy_order(strategies)  # should not raise


def test_misordered_requirement_raises_naming_both():
    # Requirer placed BEFORE its provider.
    provider = type("ProviderStrategy", (_Stub,), {})
    requirer = type("RequirerStrategy", (_Stub,), {})
    strategies = [
        requirer(requires=("cap",)),
        provider(provides=("cap",)),
    ]
    with pytest.raises(InvalidInternalStateError) as exc:
        _validate_strategy_order(strategies)
    message = str(exc.value)
    assert "RequirerStrategy" in message
    assert "ProviderStrategy" in message
    assert "cap" in message


def test_unknown_required_token_raises():
    # Nothing provides 'ghost' — typo / missing strategy.
    strategies = [
        _Stub(provides=("a",)),
        _Stub(requires=("ghost",)),
    ]
    with pytest.raises(InvalidInternalStateError) as exc:
        _validate_strategy_order(strategies)
    assert "ghost" in str(exc.value)


def test_same_position_is_too_late():
    # A strategy cannot satisfy its own requirement; provider must be strictly earlier.
    selfref = type("SelfRefStrategy", (_Stub,), {})
    strategies = [selfref(provides=("x",), requires=("x",))]
    with pytest.raises(InvalidInternalStateError):
        _validate_strategy_order(strategies)


def test_empty_list_passes():
    _validate_strategy_order([])  # no strategies, no constraints


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
