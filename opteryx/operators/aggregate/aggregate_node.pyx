# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Global (ungrouped) aggregation node.

Execution is 100% native (see opteryx/managers/execution/compiler.py's
UngroupedAggregateNode branch, which reads `.aggregates` off this class,
hoists any computed operand via _project_agg_operands, parses the aggregate
list with _parse_aggregates and compiles the result into the engine's
set_agg_sink). This class is plan-time config only; the old Cython
UngroupedAggregateEngine, its per-type accumulator classes and the typed
result/literal spec machinery were deleted when the push path went dead.
"""

# BasePlanNode in scope via textual include from _operators.pyx (umbrella unit).


cdef class UngroupedAggregateNode(BasePlanNode):
    cdef public list aggregates

    def __init__(self, properties=None, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.aggregates = list(parameters.get("aggregates", []))

    @property
    def config(self):  # pragma: no cover
        from opteryx.expression import format_expression

        return f"AGGREGATE ({', '.join(format_expression(col) for col in self.aggregates)})"

    @property
    def name(self):  # pragma: no cover
        return "Ungrouped Aggregate"
