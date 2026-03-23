# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.


import time
from collections import defaultdict
from typing import Optional
from typing import Union

import pyarrow
import pyarrow.compute as pc
from opteryx.draken import Morsel
from opteryx.expression import NodeType
from opteryx.expression import get_all_nodes_of_type
from opteryx.tracing.event_recorder import record_event as _trace_record
from orso.tools import random_string
from orso.types import OrsoTypes
from pyarrow import Table

from opteryx import EMPTY
from opteryx import EOS

_DATA_FORMAT = "arrow,draken"
END = object()


class BasePlanNode:
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
        from opteryx.models import QueryProperties
        from opteryx.models import QueryTelemetry

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

        self.readings = defaultdict(int)

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

    def execute(self, morsel: pyarrow.Table) -> Optional[pyarrow.Table]:  # pragma: no cover
        raise NotImplementedError()

    def ensure_arrow_table(self, morsel: Union[Table, Morsel]) -> Table:
        """Ensure the provided morsel is a PyArrow table when needed."""
        if morsel is EOS:
            return EOS
        if isinstance(morsel, Morsel):
            self.readings["morsel_to_table_conversion"] += 1
            return morsel.to_arrow()
        return morsel

    def ensure_draken_morsel(self, table: Union[Table, Morsel]):
        """Ensure the provided morsel is a Draken morsel when needed.

        Returns either a single Morsel or a generator of Morsels.
        """
        if table is EOS:
            return EOS
        if isinstance(table, Table):
            self.readings["table_to_morsel_conversion"] += 1
            # Use iter_from_arrow to avoid expensive combine_chunks
            # Yields morsels aligned with Arrow chunk boundaries
            return Morsel.iter_from_arrow(table)
        return table

    def __call__(self, morsel: pyarrow.Table, join_leg: str) -> Optional[pyarrow.Table]:
        # Cache frequently accessed attributes
        telemetry = self.telemetry
        time_stat_key = self._time_stat_key
        is_scan = self.is_scan

        # Process input metrics
        num_rows = 0
        if hasattr(morsel, "num_rows"):
            num_rows = morsel.num_rows
            nbytes = morsel.nbytes
            self.records_in += num_rows
            self.bytes_in += nbytes
            self.calls += 1

        # Set up execution
        generator = self.execute(morsel, join_leg=join_leg)
        empty_morsel = None
        at_least_one = False
        _call_total_ns = 0

        while True:
            try:
                start_time = time.monotonic_ns()
                result = next(generator, END)
                execution_time = time.monotonic_ns() - start_time
                _call_total_ns += execution_time

                self.execution_time += execution_time
                telemetry.increase(time_stat_key, execution_time)

                if result == EMPTY:
                    # Node absorbed a morsel but produced nothing — record for
                    # telemetry and dead-end here; nothing goes downstream.
                    _trace_record(
                        "operator_execute",
                        operator_name=self.name,
                        operator_id=self.identity,
                        duration_ns=execution_time,
                        rows_in=num_rows,
                        rows_out=0,
                        produced_rows=False,
                    )
                    continue

                if result == END:
                    if not at_least_one and empty_morsel is not None:
                        yield empty_morsel
                    break

                if is_scan:
                    self.calls += 1

                # Optimized attribute checking
                try:
                    result_num_rows = result.num_rows
                    result_nbytes = result.nbytes
                    self.records_out += result_num_rows
                    self.bytes_out += result_nbytes

                    if empty_morsel is None:
                        empty_morsel = result.slice(0, 0)

                    if result_num_rows > 0:
                        at_least_one = True
                        _trace_record(
                            "operator_execute",
                            operator_name=self.name,
                            operator_id=self.identity,
                            duration_ns=execution_time,
                            rows_in=num_rows,
                            rows_out=result_num_rows,
                            produced_rows=True,
                        )
                        yield result
                        continue
                    else:
                        telemetry.dead_ended_empty_morsels += 1
                except AttributeError:
                    # Not a table-like object
                    pass

                at_least_one = True
                yield result

            except Exception as err:
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
    def _join_numeric_target_arrow_type(left_type, right_type):
        """
        Return a target Arrow type for implicit numeric join-key coercion.
        """
        numeric_types = (OrsoTypes.INTEGER, OrsoTypes.DOUBLE, OrsoTypes.DECIMAL)
        if left_type not in numeric_types or right_type not in numeric_types:
            return None
        if left_type in (OrsoTypes.DOUBLE, OrsoTypes.DECIMAL) or right_type in (
            OrsoTypes.DOUBLE,
            OrsoTypes.DECIMAL,
        ):
            return pyarrow.float64()
        return pyarrow.int64()

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

            target_arrow_type = self._join_numeric_target_arrow_type(left_type, right_type)
            if target_arrow_type is None:
                continue

            signature = (left_column, right_column, str(target_arrow_type))
            if signature in seen:
                continue
            seen.add(signature)
            self._join_key_cast_plan.append(
                {
                    "left_column": left_column,
                    "right_column": right_column,
                    "target_type": target_arrow_type,
                }
            )

    def _apply_join_key_casts(self, table: Table, *, is_left: bool) -> Table:
        """
        Apply implicit join-key type coercions for numeric equality joins.
        """
        if table is None or table is EOS or table.num_rows == 0:
            return table

        self._build_join_key_cast_plan()
        if not self._join_key_cast_plan:
            return table

        for cast_rule in self._join_key_cast_plan:
            column_name = cast_rule["left_column"] if is_left else cast_rule["right_column"]
            if column_name not in table.column_names:
                continue

            current = table.column(column_name)
            target_type = cast_rule["target_type"]
            if current.type == target_type:
                continue

            casted = pc.cast(current, target_type, safe=False)
            field_index = table.schema.get_field_index(column_name)
            table = table.set_column(field_index, pyarrow.field(column_name, target_type), casted)
            self.readings["feature_implicit_join_key_cast"] += 1

        return table

    def to_mermaid(self, nid):
        """
        Generic method to convert a node to a mermaid entry
        """
        mermaid = f'NODE_{nid}["**JOIN ({self.join_type.upper()})**<br />'
        mermaid += f"({self.execution_time / 1_000_000:,.2f}ms)"
        return mermaid + '"]'
