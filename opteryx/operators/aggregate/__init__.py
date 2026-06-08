"""
Ungrouped (global) aggregate operator.

Splits across multiple included files:
  ungrouped_agg.pyx         — base class UngroupedAggregate + helper functions
  ungrouped_agg_count.pyx   — CountStarAggregate, CountAggregate
  ungrouped_agg_sum.pyx     — SumInt64Aggregate, SumFloat64Aggregate
  ungrouped_agg_min_max.pyx — Min/MaxInt64Aggregate, Min/MaxFloat64Aggregate,
                               MinBytesAggregate, MaxBytesAggregate
  ungrouped_agg_any_value.pyx — AnyValueAggregate
  ungrouped_agg_count_distinct.pyx — CountDistinctAggregate
  ungrouped_agg_engine.pyx  — UngroupedAggregateEngine (drives the above)
  aggregate_node.pyx        — UngroupedAggregateNode (plan node wrapper)

All compiled into opteryx.operators._operators via the umbrella _operators.pyx.
"""


def __getattr__(name):
    from opteryx.operators import _operators
    attr = getattr(_operators, name, None)
    if attr is not None:
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
