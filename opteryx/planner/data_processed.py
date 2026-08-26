# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The `data_processed` meter — the quantity DATA_PROCESSED_BYTES bills on.

COMMERCIAL DEFINITION (product owner, 2026-08-24): the customer pays for the
LOGICAL UNCOMPRESSED BYTES ENTERING THE SYSTEM. A zstd-compressed column bills
its uncompressed bytes; a dictionary-encoded column bills the equivalent DENSE
bytes. Encoding is the engine's business, not the customer's.

That definition rules out both numbers this meter used to carry:

  * `bytes_fetched` (rugo's IO pipeline, `io_pipeline.hpp`) is COMPRESSED bytes
    measured at transfer. It is the right number for diagnosing IO and the
    wrong one for an invoice — it differs from the definition by the whole
    compression ratio.
  * the parquet footer's `total_uncompressed_size` is the decompressed size of
    the ENCODED pages. It fixes the compression half and leaves the dictionary
    half broken: a dict-encoded column reports its dictionary page plus its
    bit-packed indices, which is nowhere near its dense size. It is not even a
    bound — a PLAIN-encoded string column carries a 4-byte length per value and
    reports ABOVE dense.

A column with neither a fixed width nor ANALYZE'd string stats still falls back
to that footer figure (see ColumnStatistics.total_bytes), so ANALYZE is what
makes a variable-width column's billing figure correct rather than approximate.

So the meter is computed here instead, at PLAN TIME, from the per-column dense
widths the estimator already derives (`ColumnStatistics.total_bytes`, whose
source order is dense-first precisely so this can read it).

WHY PLAN TIME. jobs.opteryx enforces usage limits at submit time and must
charge the same number this bills, or enforcement and invoicing diverge (see
jobs.opteryx docs/design/billing-enforcement.md). A runtime meter cannot be
quoted before the query runs, so the two would be different quantities by
construction. Emitting the plan-time figure makes them ONE number. It also
means a query that is abandoned mid-stream still bills — the work was
committed when the plan was accepted.

The cost of that choice, stated plainly: run-time reductions the plan cannot
see are still billed. Skene row-group skipping is decided by the Source at run
time, and dynamic filters narrow nothing at plan time. The customer is charged
for those rows. Plan-time file/manifest pruning IS reflected, because the
pruned Manifest is what the scan carries by then.

WHAT IS COUNTED. Per Scan, the pre-filter relation: `scan_base_statistics`
(rows after manifest pruning, before any predicate) summed over the columns the
query REFERENCES — the projection plus any column a pushed predicate reads.
Nothing else in the plan contributes: bytes materialised by a join, an
aggregate or a sort are work the engine does, not data entering the system.

NOT COUNTED, and deliberately:
  * EXPLAIN — plans but never reads. Bills zero.
  * FunctionDataset (VALUES, FAKE, GENERATE_SERIES, UNNEST) — generated
    in-process, nothing enters from storage. Bills zero. This is a change:
    these operators used to add their materialised in-memory size to the same
    counter, mixing a second quantity into it.
  * `$no_table` — the one-row stand-in the planner substitutes for a statement
    with no FROM clause (`SELECT 1`) and for a statistics-only answer
    (`SELECT COUNT(*)` served from the manifest). It is a planner artifact, not
    a relation the user named, and nothing is read to produce it.
  * a column with no size signal at all (variable-width, no ANALYZE, no
    manifest size). It contributes nothing rather than a fabricated width; the
    scan's other columns still count.

Virtual datasets that ARE relations the user named ($planets, $satellites,
$missions) DO count. They are datasets with rows and columns that the query
reads, and the fact that the bytes come from process memory rather than a
bucket is an implementation detail of where we keep them.
"""

from typing import Optional

from opteryx.planner.logical_planner import LogicalPlan
from opteryx.planner.logical_planner import LogicalPlanStepType

__all__ = ["iter_scan_nodes", "measure_data_processed", "plan_relations"]


# The planner's one-row stand-in for a statement with no FROM clause, and for a
# statistics-only answer. Not a relation, and nothing is read to produce it.
_NO_TABLE = "$no_table"


def _scan_bytes(node, base_stats_cache: Optional[dict]) -> int:
    """Dense logical bytes read by one Scan node."""
    from opteryx.planner.optimizer.statistics_refresh import scan_base_statistics

    if node.relation == _NO_TABLE:
        return 0

    stats = scan_base_statistics(node, base_stats_cache)
    if stats is None:
        return 0
    # Sum only the columns with a known size. A column with no signal
    # contributes nothing — see the module docstring; it is not an excuse to
    # abandon the whole scan, and it is not a licence to invent a width.
    return sum(
        column.total_bytes
        for column in stats.columns.values()
        if column.total_bytes is not None
    )


def iter_scan_nodes(plan: LogicalPlan, shared_ctes: Optional[dict] = None):
    """Every Scan node this plan reads, once each.

    The one traversal both the meter and `plan_relations` run on. They MUST see
    the same scans: a bill that counts a CTE body and a relation list that does
    not (or the reverse) describes two different queries, and whichever
    consumer joins them later has no way to tell which is wrong. Sharing the
    walk makes that class of disagreement unrepresentable rather than merely
    unlikely.

    `shared_ctes` is the plan's materialized-CTE bodies. Their scans are NOT in
    the main graph — the body executes once off to the side — so they must be
    walked explicitly or a CTE over a large table bills nothing. Yielded once
    each, which is also how often they run, however many refs point at them.

    Yields nothing for an EXPLAIN. It plans the query and then describes it:
    nothing is read, nothing is billed, and nothing is recorded as having been
    touched. Checked on the whole forest rather than the exit point alone —
    Explain sits ABOVE Exit, so an exit-point test would miss it.
    """
    from opteryx.planner.relation_resolver import iter_plan_forest

    for _, node in plan.nodes(True):
        if node.node_type == LogicalPlanStepType.Explain:
            return

    seen_plans: set = set()

    def _walk(one_plan):
        # iter_plan_forest covers plans embedded in node properties (expression
        # subqueries hold whole plans off to the side) and dedupes by object
        # identity — one expression object is routinely reachable from several
        # nodes, and counting its plan twice would double-bill it.
        for sub_plan in iter_plan_forest(one_plan):
            if id(sub_plan) in seen_plans:
                continue
            seen_plans.add(id(sub_plan))
            for _, node in sub_plan.nodes(True):
                if node.node_type == LogicalPlanStepType.Scan:
                    yield node

    yield from _walk(plan)
    for body in (shared_ctes or {}).values():
        if isinstance(body, LogicalPlan):
            yield from _walk(body)


def _scan_relation(node) -> Optional[str]:
    """The name a Scan node read, as a consumer outside the engine can use it.

    For a catalog-backed scan that is `workspace.collection.dataset` — assembled
    from the connector rather than from `node.relation`, because what the user
    typed may be unqualified (the workspace comes from the session, not the
    statement) and an unqualified name cannot be joined back to a catalog
    entry. The connector holds the resolved workspace and the catalog-relative
    identifier, which are exactly the two halves of the identifier
    `catalog.load_dataset()` takes.

    Every other connector yields `node.relation` as bound: lower-cased, and
    whatever the user named. Their `dataset` attribute is a filesystem path by
    then, not an identifier, so it is deliberately not used.

    `$no_table` yields None. It is the planner's one-row stand-in for a
    statement with no FROM clause, not a relation anyone named — the same
    reason the meter bills it zero.
    """
    relation = getattr(node, "relation", None)
    if not relation or relation == _NO_TABLE:
        return None

    connector = getattr(node, "connector", None)
    workspace = getattr(connector, "workspace", None)
    dataset = getattr(connector, "dataset", None)
    if workspace and dataset:
        return f"{workspace}.{dataset}"
    return str(relation)


def plan_relations(plan: LogicalPlan, shared_ctes: Optional[dict] = None) -> list:
    """The relations this plan reads, sorted, deduplicated.

    Recorded so that consumers of the billing event can attribute a query to
    the things it touched without re-binding the SQL. That question has exactly
    one correct answer and the engine already knows it here; recovering it
    downstream would mean a second implementation of name resolution that can
    disagree with this one.

    Sorted because the list is compared and grouped downstream, and a set's
    iteration order would make two identical queries produce two different
    payloads. Deduplicated because a self-join reads one relation, twice — the
    count of scans is a plan property, not a usage fact.
    """
    return sorted(
        {
            name
            for name in (
                _scan_relation(node) for node in iter_scan_nodes(plan, shared_ctes)
            )
            if name
        }
    )


def measure_data_processed(
    plan: LogicalPlan,
    base_stats_cache: Optional[dict] = None,
    shared_ctes: Optional[dict] = None,
) -> int:
    """Dense logical bytes this plan will read — the DATA_PROCESSED_BYTES meter.

    Call on the FINAL optimized logical plan: manifest pruning, projection
    pushdown and predicate pushdown all change the answer, and all of them run
    inside the optimizer.

    `base_stats_cache` is the query's scan-statistics memo (threaded from
    `plan_query`); passing it makes this walk effectively free when the
    optimizer has already costed the same scans.
    """
    return int(
        sum(
            _scan_bytes(node, base_stats_cache)
            for node in iter_scan_nodes(plan, shared_ctes)
        )
    )
