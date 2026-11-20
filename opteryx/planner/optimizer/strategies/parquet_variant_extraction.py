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

from opteryx.expression import NodeType
from opteryx.models.dataset_format import PARQUET
from opteryx.models.dataset_format import manifest_format
from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType
from opteryx.planner.logical_planner import PlanStep

from .jsonl_nested_extraction import ScanExtractionStrategy
from .jsonl_nested_extraction import _key_step
from .jsonl_nested_extraction import _reads_source


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

    def should_i_run(self, plan: LogicalPlan) -> bool:
        scans = (LogicalPlanStepType.FunctionDataset, LogicalPlanStepType.Scan)
        return any(_reads_parquet(node) for _, node in plan.steps_of(scans))
