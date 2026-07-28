# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""Per-strategy A/B kill-switches.

`_STRATEGY_DISABLE_FLAGS` (opteryx/planner/optimizer/__init__.py) maps every
strategy class name in the pipeline to an `opteryx.config.features` boolean,
checked centrally in `OptimizerVisitor.optimize()` before a strategy's own
`should_i_run`. All flags default False (every strategy enabled) — this is
for A/B testing a strategy against the rest of the pipeline, not a permanent
behaviour switch.

The completeness test below is the guard against drift: it would have caught
`CrossJoinChainReorderStrategy` being imported but never added to
`OptimizerVisitor.strategies` (found 2026-07-28, reported separately — not
fixed here, out of scope for this change) had it been mapped to a flag.
"""

import os
import sys
import uuid

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import pytest

from opteryx.models import QueryTelemetry
from opteryx.planner.optimizer import OptimizerVisitor
from opteryx.planner.optimizer import _STRATEGY_DISABLE_FLAGS


def test_every_pipeline_strategy_has_a_flag():
    visitor = OptimizerVisitor(QueryTelemetry(str(uuid.uuid4())))
    names_in_pipeline = {type(s).__name__ for s in visitor.strategies}
    mapped = set(_STRATEGY_DISABLE_FLAGS.keys())
    assert names_in_pipeline == mapped, (
        names_in_pipeline - mapped,  # in the pipeline, missing a flag
        mapped - names_in_pipeline,  # mapped, but not actually in the pipeline
    )


def test_every_flag_exists_on_features_and_defaults_off():
    from opteryx import config

    for flag_name in _STRATEGY_DISABLE_FLAGS.values():
        assert hasattr(config.features, flag_name), flag_name
        assert getattr(config.features, flag_name) is False, flag_name


def test_disabling_a_strategy_skips_it():
    # PredicateCompactionStrategy: simple, easy to observe via its own telemetry.
    from opteryx import config
    from opteryx.planner import query_planner
    from opteryx.models import ExecutionContext

    sql = "SELECT * FROM $planets WHERE id > 5 AND id < 10 AND id > 6"

    def _run():
        qid = str(uuid.uuid4())
        telemetry = QueryTelemetry(qid)
        query_planner(
            operation=sql,
            parameters=None,
            visibility_filters=None,
            execution_context=ExecutionContext(),
            query_id=qid,
            telemetry=telemetry,
        )
        return telemetry

    on_telemetry = _run()
    assert on_telemetry.optimization_predicate_compaction > 0

    original = config.features.disable_predicate_compaction
    try:
        config.features.disable_predicate_compaction = True
        off_telemetry = _run()
        assert off_telemetry.optimization_predicate_compaction == 0
    finally:
        config.features.disable_predicate_compaction = original


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
