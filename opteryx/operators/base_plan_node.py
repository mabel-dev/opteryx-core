# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import time
from collections import defaultdict

from opteryx import EMPTY, EOS
from opteryx.compiled.draken import Morsel
from opteryx.expression import NodeType, get_all_nodes_of_type
from opteryx.tracing.event_recorder import record_event as _trace_record
from opteryx.types import OrsoTypes
from opteryx.utils import random_string

_DATA_FORMAT = "arrow,draken"
END = object()


class BasePlanNode:
    # Class-level defaults — overridden per-instance from catalog in __init__.
    is_join: bool = False
    is_scan: bool = False
    is_not_explained: bool = False
    is_stateless: bool = False

    def __init__(self, *, properties, **parameters):
        """
        This is the base class for nodes in the execution plan.

        The initializer accepts a QueryTelemetry node which is populated by different nodes
        differently to record what happened during the query execution.
        """
        from opteryx.models import QueryProperties, QueryTelemetry
        from opteryx.operators.catalog import get_registry

        self.properties: QueryProperties = properties
        self.telemetry: QueryTelemetry = QueryTelemetry(properties.query_id)
        self.parameters = parameters
        self.execution_time = 0
        self.identity = random_string()
        self.calls = 0
        self.records_in = 0
        self.bytes_in = 0
        self.records_out = 0
        self.bytes_out = 0
        self.columns = parameters.get("columns", [])

        self._time_stat_key = f"time_{self.name.lower().replace(' ', '_')}"
        self._empty_morsel_cache = None
        self._morsel_index = 0

        self.readings = defaultdict(int)

        # Populate runtime flags from the catalog (single source of truth).
        _meta = get_registry().get(self.__class__)
        if _meta is not None:
            self.is_scan = _meta.is_scan
            self.is_join = _meta.is_join
            self.is_stateless = _meta.is_stateless
            self.is_not_explained = _meta.is_not_explained

    @property
    def config(self) -> str:
        return ""

    @property
    def name(self):  # pragma: no cover
        """
        Friendly Name of this node
        """
        return "no name"

    @property
    def node_type(self) -> str:
        return self.name

    def to_mermaid(self, nid):
        """
        Generic method to convert a node to a mermaid entry
        """
        mermaid = f'NODE_{nid}["**{self.node_type.upper()}**<br />'
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '"]'

    def __str__(self) -> str:
        return f"{self.name} {self.sensors()}"

    def execute(self, morsel) -> None:  # pragma: no cover
        raise NotImplementedError()

    def __call__(self, morsel):
        # Cache frequently accessed attributes
        telemetry = self.telemetry
        time_stat_key = self._time_stat_key
        is_scan = self.is_scan

        # Process input metrics
        input_rows = 0
        input_bytes = 0
        if hasattr(morsel, "num_rows"):
            input_rows = morsel.num_rows
            input_bytes = morsel.nbytes
            self.records_in += input_rows
            self.bytes_in += input_bytes
            self.calls += 1

        # Get operator category for tracing (may be None for unregistered operators)
        operator_category = getattr(self.__class__, "category", None)
        category_value = operator_category.value if operator_category else None

        # Set up execution
        generator = self.execute(morsel)
        empty_morsel = None
        at_least_one = False

        while True:
            try:
                start_time = time.monotonic_ns()
                result = next(generator, END)
                execution_time = time.monotonic_ns() - start_time

                self.execution_time += execution_time
                telemetry.increase(time_stat_key, execution_time)

                if result == EMPTY:
                    # Node absorbed a morsel but produced nothing.
                    # Record trace event for this empty morsel.
                    _trace_record(
                        "operator_execute",
                        operator_name=self.name,
                        operator_id=self.identity,
                        operator_category=category_value,
                        morsel_index=self._morsel_index,
                        rows_in=input_rows,
                        rows_out=0,
                        bytes_in=input_bytes,
                        bytes_out=0,
                        duration_ns=execution_time,
                        produced_output=False,
                    )
                    self._morsel_index += 1
                    continue

                if result == END:
                    # End of generator stream. Record final marker.
                    _trace_record(
                        "operator_execute",
                        operator_name=self.name,
                        operator_id=self.identity,
                        operator_category=category_value,
                        morsel_index=-1,  # -1 signals end-of-stream
                        rows_in=0,
                        rows_out=0,
                        bytes_in=0,
                        bytes_out=0,
                        duration_ns=0,
                        produced_output=False,
                    )
                    if not at_least_one and empty_morsel is not None:
                        yield empty_morsel
                    break

                if is_scan:
                    self.calls += 1

                # Optimized attribute checking for table-like results
                output_rows = 0
                output_bytes = 0
                try:
                    output_rows = result.num_rows
                    output_bytes = result.nbytes
                    self.records_out += output_rows
                    self.bytes_out += output_bytes

                    if empty_morsel is None:
                        empty_morsel = result.slice(0, 0)

                    # Emit trace event for this morsel
                    produced_output = output_rows > 0
                    _trace_record(
                        "operator_execute",
                        operator_name=self.name,
                        operator_id=self.identity,
                        operator_category=category_value,
                        morsel_index=self._morsel_index,
                        rows_in=input_rows,
                        rows_out=output_rows,
                        bytes_in=input_bytes,
                        bytes_out=output_bytes,
                        duration_ns=execution_time,
                        produced_output=produced_output,
                    )

                    if output_rows > 0:
                        at_least_one = True
                        yield result
                        self._morsel_index += 1
                        continue
                    else:
                        telemetry.dead_ended_empty_morsels += 1
                except AttributeError:
                    # Not a table-like object — emit trace with unknown bytes
                    _trace_record(
                        "operator_execute",
                        operator_name=self.name,
                        operator_id=self.identity,
                        operator_category=category_value,
                        morsel_index=self._morsel_index,
                        rows_in=input_rows,
                        rows_out=0,
                        bytes_in=input_bytes,
                        bytes_out=0,
                        duration_ns=execution_time,
                        produced_output=True,
                    )

                at_least_one = True
                yield result
                self._morsel_index += 1

            except Exception as err:
                # Record error event before re-raising
                _trace_record(
                    "operator_execute",
                    operator_name=self.name,
                    operator_id=self.identity,
                    operator_category=category_value,
                    morsel_index=self._morsel_index,
                    error=err.__class__.__name__,
                    error_message=str(err),
                    rows_in=input_rows,
                    rows_out=0,
                    bytes_in=input_bytes,
                    bytes_out=0,
                    duration_ns=0,
                    produced_output=False,
                )
                raise err

    def sensors(self):
        base = {
            "calls": self.calls,
            "execution_time": self.execution_time,
            "records_in": self.records_in,
            "records_out": self.records_out,
            "bytes_in": self.bytes_in,
            "bytes_out": self.bytes_out,
        }
        base.update(self.readings)
        return base


class JoinNode(BasePlanNode):
    is_join = True

    def __init__(self, *, properties, **parameters):
        super().__init__(properties=properties, **parameters)

        self.left_readers = parameters.get("left_readers")
        self.right_readers = parameters.get("right_readers")
        self.left_relation_names = parameters.get("left_relation_names") or []
        self.right_relation_names = parameters.get("right_relation_names") or []
        self.on = parameters.get("on")
        self._join_key_cast_plan = None

    @staticmethod
    def _join_numeric_target_type(left_type, right_type):
        """
        Return the promoted OrsoType for implicit numeric join-key coercion.
        INTEGER + DOUBLE → DOUBLE (SQL92: 1 == 1.0 is TRUE)
        """
        from opteryx.types import find_compatible_type

        numeric_types = (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL)
        if left_type not in numeric_types or right_type not in numeric_types:
            return None
        if left_type == right_type:
            return None
        return find_compatible_type([left_type, right_type])

    def _build_join_key_cast_plan(self):
        if self._join_key_cast_plan is not None:
            return

        self._join_key_cast_plan = []
        if not self.on:
            return

        comparisons = get_all_nodes_of_type(self.on, (NodeType.COMPARISON_OPERATOR,))
        seen = set()

        for comparison in comparisons:
            if comparison.value != "Eq":
                continue

            left = comparison.left
            right = comparison.right
            if not left or not right:
                continue
            if left.node_type != NodeType.IDENTIFIER or right.node_type != NodeType.IDENTIFIER:
                continue
            if not left.schema_column or not right.schema_column:
                continue

            left_rel = left.source
            right_rel = right.source
            left_identity = left.schema_column.identity
            right_identity = right.schema_column.identity
            left_type = left.schema_column.type
            right_type = right.schema_column.type

            if left_rel in self.left_relation_names and right_rel in self.right_relation_names:
                left_column, right_column = left_identity, right_identity
            elif left_rel in self.right_relation_names and right_rel in self.left_relation_names:
                left_column, right_column = right_identity, left_identity
                left_type, right_type = right_type, left_type
            else:
                continue

            target_type = self._join_numeric_target_type(left_type, right_type)
            if target_type is None:
                continue

            signature = (left_column, right_column, target_type)
            if signature in seen:
                continue
            seen.add(signature)
            self._join_key_cast_plan.append(
                {
                    "left_column": left_column,
                    "right_column": right_column,
                    "target_type": target_type,
                }
            )

    def _apply_join_key_casts(self, morsel, *, is_left: bool):
        """
        Apply implicit join-key type coercions for numeric equality joins.
        Operates on Draken Morsels; returns a (possibly new) Morsel.
        SQL92: 1 == 1.0 is TRUE — INTEGER and DOUBLE keys are promoted to DOUBLE.
        """
        if morsel is None or morsel is EOS:
            return morsel

        self._build_join_key_cast_plan()
        if not self._join_key_cast_plan:
            return morsel

        from opteryx.compiled.draken.morsels.morsel import Morsel as _Morsel
        from opteryx.expression.casts import cast_to_double, cast_to_int

        names = list(morsel.column_names)
        vectors = [morsel.column(n) for n in names]

        changed = False
        for cast_rule in self._join_key_cast_plan:
            column_name = cast_rule["left_column"] if is_left else cast_rule["right_column"]
            if column_name not in names:
                continue

            idx = names.index(column_name)
            target_type = cast_rule["target_type"]

            if target_type == OrsoTypes.DOUBLE:
                vectors[idx] = cast_to_double(vectors[idx])
                changed = True
            elif target_type == OrsoTypes.INTEGER:
                vectors[idx] = cast_to_int(vectors[idx])
                changed = True

            if changed:
                self.readings["feature_implicit_join_key_cast"] += 1

        if not changed:
            return morsel

        return _Morsel.from_vectors(names, vectors)

    def to_mermaid(self, nid):
        """
        Generic method to convert a node to a mermaid entry
        """
        mermaid = f'NODE_{nid}["**JOIN ({self.join_type.upper()})**<br />'
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '"]'
