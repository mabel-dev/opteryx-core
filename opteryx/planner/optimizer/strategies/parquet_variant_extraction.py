# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Optimization Rule - Parquet VARIANT Extraction

Type: Heuristic
Goal: Read `data -> 'commit' ->> 'collection'` out of a Parquet VARIANT column -
      READ_PARQUET's, or a Parquet table's - at SCAN time, from the columns the file
      shredded it into, instead of rendering every row's whole value to JSON text and
      parsing it again

Order: where JsonlNestedExtractionStrategy runs, for the same reasons (after the
       expression rewrites; before PredicatePushdown and ProjectionPushdown).

Rewrite:
    SELECT data -> 'commit' ->> 'collection' FROM READ_PARQUET('f.parquet')
    ->
    SELECT "data->'commit'->>'collection'" FROM READ_PARQUET('f.parquet')
        -- a column rugo reads from the file's shredding; `data` is no longer read

The rewrite itself is JsonlNestedExtractionStrategy's (ScanExtractionStrategy); the
contract is docs/parquet_variant_design.md section 4.3. What differs:

* CHAINS are taken: any number of `->` steps, the last of which may be `->>`.
  `(a ->> 'b') ->> 'c'` is not a chain - it parses the STRING `a ->> 'b'` yields,
  a different operation - and is left alone. Each step is one object key, by
  the JSONL strategy's rule (`_key_step`).
* The scan answers the path per file, from how that file holds the column: its
  shredded columns where the path is shredded, the binary value where it is not,
  and - a file that holds the column as a string of JSON text, how a table's
  VARIANT column was written before the Parquet VARIANT type - one parse of each
  row, shared by every path over the column. A file with no such column reads
  NULL, as any column a file lacks does; one holding it as anything else is
  refused by the scan's footer gate, naming the file.

Scope: a column bound VARIANT on READ_PARQUET (the footer declares it) or on a table
whose data files are Parquet (the catalog declares it).

Known cost, accepted by ruling 2026-11-19 (design doc section 4.3, P5): a pushed
path is produced for every row the scan emits, which on a JSON-text file is a parse
of every row the scan's own prefilter keeps - where the unpushed extraction parses
only the rows the whole filter keeps.
"""

from draken.draken_native import DrakenType

from opteryx import config
from opteryx.compiled.structures.expressions import is_expression
from opteryx.expression import NodeType
from opteryx.models.dataset_format import PARQUET
from opteryx.models.dataset_format import manifest_format
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import PlanStep

from .jsonl_nested_extraction import ScanExtractionStrategy
from .jsonl_nested_extraction import _key_step
from .jsonl_nested_extraction import _reads_source

# Fields that only RECORD what other fields reference: an aggregate's `columns` lists every
# identifier inside its aggregates (binder/aggregate.py), and a scan's lists what it emits.
_BOOKKEEPING = {LogicalPlanStepType.Aggregate, LogicalPlanStepType.AggregateAndGroup}


def _reads_parquet(node: PlanStep) -> bool:
    """READ_PARQUET, or a table scan whose data files are Parquet."""
    if node.node_type == LogicalPlanStepType.FunctionDataset:
        return node.function == "READ_PARQUET"
    return (
        node.node_type == LogicalPlanStepType.Scan
        and node.manifest is not None
        and manifest_format(node.manifest, dataset=node.relation or "") == PARQUET
    )


class ParquetVariantExtractionStrategy(ScanExtractionStrategy):
    """Push `->` ... `->` / `->>` chains over Parquet VARIANT columns into the scan."""

    counter = "optimization_parquet_variant_extraction"

    def _sources(self, plan: LogicalPlan) -> dict:
        sources = {}
        for nid, node in plan.nodes(True):
            if not _reads_parquet(node):
                continue
            for column in node.schema.columns:
                column_type = column.column_type
                # A name holding `->` could not be told from a path by the scan.
                if (
                    column_type is not None
                    and column_type.physical == DrakenType.VARIANT
                    and "->" not in column.name
                ):
                    sources[column.identity] = (nid, column.name)
        return sources

    def _steps(self, expression, sources):
        steps = []
        node = expression
        while True:
            step = _key_step(node)
            if step is None or (node is not expression and step[0] != "->"):
                return None
            steps.append(step)
            node = node.left
            if node is None or node.node_type != NodeType.EXTRACTION_OPERATOR:
                break
        if not _reads_source(node, sources):
            return None
        steps.reverse()
        return node, steps

    def _record(self, node: PlanStep, specs: dict) -> None:
        physical = dict(node.parquet_physical_by_identity or {})
        physical.update(specs)
        node.parquet_physical_by_identity = physical

    def complete(self, plan: LogicalPlan, context) -> LogicalPlan:
        plan = super().complete(plan, context)
        if not config.features.disable_variant_typed_path:
            self._record_typed_paths(plan)
        return plan

    def _record_typed_paths(self, plan: LogicalPlan) -> None:
        """docs/parquet_variant_path_read_design.md §5: a pushed `->>` path read ONLY under
        `CAST(<path> AS INT64)` becomes a candidate the compiler may read straight from the
        shredded integer leaf, where the files' footers prove no row can need the cast. Recorded
        on the scan step as {path identity: (the CAST's schema column, `<path>::INT64`)}; a path
        read any other way, or under CASTs of more than one identity, is left alone."""
        text_paths = {}
        for nid, node in plan.nodes(True):
            if _reads_parquet(node):
                for identity, spec in (node.parquet_physical_by_identity or {}).items():
                    if "->>" in spec:
                        text_paths[identity] = (nid, spec)
        if not text_paths:
            return
        casts = {}
        bare = set()

        def _walk(expression):
            sc = expression.schema_column
            operand = expression.left if expression.node_type == NodeType.CAST else None
            if (
                operand is not None
                and expression.value in ("INT64", "INTEGER")
                and expression.format is None
                and sc is not None
                and operand.node_type == NodeType.IDENTIFIER
                and operand.schema_column is not None
                and operand.schema_column.identity in text_paths
            ):
                casts.setdefault(operand.schema_column.identity, {})[sc.identity] = sc
                return
            if expression.node_type == NodeType.IDENTIFIER and sc is not None and sc.identity in text_paths:
                bare.add(sc.identity)
            for child in expression.children():
                if is_expression(child):
                    _walk(child)

        for _, node in plan.nodes(True):
            skip_columns = _reads_parquet(node) or node.node_type in _BOOKKEEPING
            for key, value in node.field_values().items():
                if key == "columns" and skip_columns:
                    continue
                for item in value if type(value) in (list, tuple) else (value,):
                    if is_expression(item):
                        _walk(item)

        for identity, by_cast in casts.items():
            if identity in bare or len(by_cast) != 1:
                continue
            cast_column = next(iter(by_cast.values()))
            nid, spec = text_paths[identity]
            node = plan[nid]
            typed = dict(node.parquet_typed_paths or {})
            typed[identity] = (cast_column, spec + "::INT64")
            node.parquet_typed_paths = typed
            plan[nid] = node

    def should_i_run(self, plan: LogicalPlan) -> bool:
        scans = (LogicalPlanStepType.FunctionDataset, LogicalPlanStepType.Scan)
        return any(_reads_parquet(node) for _, node in plan.steps_of(scans))
