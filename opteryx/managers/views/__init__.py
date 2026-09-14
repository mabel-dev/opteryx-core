# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
View lookup for the Relation Resolver.

A view is SQL text in the catalog. Planning it is NOT special: it goes through the same
logical planner as any other query, and the plan it produces is spliced into the calling
query by the resolver, which then rewrites, binds and optimizes the whole thing as one
plan. This module's only job is catalog lookup and turning view SQL into a plan.

A view carries its OWN CTEs. They travel with the plan (see `_view_as_plan`) and become
the scope the resolver uses for the view body — a view never sees the caller's CTEs.
"""

from typing import Dict
from typing import Optional
from typing import Tuple

from opteryx.connectors import view_store_connector
from opteryx.connectors.capabilities.eidetic import ViewDefinition
from opteryx.exceptions import DatasetNotFoundError
from opteryx.utils import lru_cache_with_expiry


def _view_plan_from_definition(definition) -> Optional[Tuple[object, Dict[str, object]]]:
    """Build (plan, ctes) for a view definition, or None."""
    if definition is None:
        return None
    view_plan, view_ctes = _view_as_plan(definition.statement)
    # Copy the cached plan so downstream mutation doesn't corrupt the cache. The CTE
    # plans are copied by the resolver at splice time (copy_sub_plan), so they are not
    # copied here.
    view_plan = view_plan.copy()
    return _bind_row_count_estimate(view_plan, definition.last_row_count), view_ctes


def resolve_relation(relation: str, telemetry, catalog_cache=None):
    """Catalog resolution step: resolve a relation in a single catalog round
    trip, returning one of:

      ('view', (view_logical_plan, view_ctes))  — expand it in place
      ('dataset', dataset_object)               — hand to table_engine via prefetched_table=
      (None, None)                              — unknown here; bind it normally

    Connectors exposing ``get_relation`` resolve the dataset and view in one
    ``get_all``; others fall back to a view-only probe so behaviour is
    unchanged. Non-eidetic connectors (e.g. local filesystem) never look up
    views, so they return (None, None) and bind on the normal path.

    The connector asked is the workspace's VIEW STORE, which for an
    externally-bound workspace is the opteryx catalog entry rather than the
    connector serving its data - a view is catalog text and CREATE VIEW wrote
    it there, so this is where it is read back from. That is a catalog round
    trip a workspace whose data connector is not eidetic did not pay before -
    accepted (architect, 2026-09-14) so that a view means the same thing in
    every workspace, rather than existing only where the catalog also serves
    data. The early return above keeps disk and virtual relations free.

    A DATASET answer from that lookup is honoured exactly as before. It used
    to be dropped whenever the store was not the same OBJECT as
    `connector_factory`'s answer, on the assumption that the two objects
    differing meant the data lived outside the catalog. They also differ when
    both resolvers are installed and resolve to the SAME catalog - two cache
    entries, one catalog - which is the ordinary production shape, so every
    relation in it reported as not found.

    `catalog_cache` is an OPT-IN, caller-owned `CatalogCache`. It caches the round
    trip above and nothing else: what goes in it is the raw `(kind, object)` the
    connector answered with, BEFORE a view is turned into a plan. Caching the plan
    instead would hand the same plan object to every caller and the resolver splices
    (and so mutates) what it is given.

    Only the check path passes one. The dataset document is the version pointer and
    the catalog re-reads it every call for that reason, so an entry held for a minute
    is a plan built against a possibly superseded snapshot - see `opteryx.CatalogCache`
    for why that is fine for a check and wrong for anything that reads rows.
    """
    import time as _cat_time

    _cat0 = _cat_time.monotonic_ns()
    try:
        store = view_store_connector(relation, telemetry)
        if not store.eidetic:
            # Nowhere holds a view for this name (local filesystem, a virtual
            # dataset): bind it on the normal path, no round trip.
            return None, None

        resolver = getattr(store, "get_relation", None)
        if resolver is None:
            definition = _get_view_definition(relation, telemetry, store)
            return ("view", _view_plan_from_definition(definition)) if definition else (None, None)
        cached = None if catalog_cache is None else catalog_cache.get(relation)
        if cached is None:
            cached = resolver(relation)
            if catalog_cache is not None:
                catalog_cache.put(relation, cached)
        kind, obj = cached
        if kind == "view":
            return "view", _view_plan_from_definition(obj)
        if kind == "dataset":
            return "dataset", obj
        return None, None
    finally:
        # The catalog lookup is a cloud round trip (Firestore), distinct from the GCS
        # manifest/footer fetch timed as time_binding_metadata. Kept separate so the two
        # cloud costs are visible independently.
        if telemetry is not None:
            telemetry.time_binding_catalog += _cat_time.monotonic_ns() - _cat0


def _get_view_definition(view_name: str, telemetry, store=None) -> Optional[ViewDefinition]:
    """Return the view definition for a view, or None if the name is not a view.

    `store` is the view store the caller already resolved; resolved here when a
    caller has none. It is NOT `connector_factory`'s answer for an externally
    bound workspace - see `view_store_connector`.

    Only "this is not a view" is swallowed. A catalog that is unreachable, or a view
    whose definition is corrupt, raises — degrading those into None reports the relation
    as a missing dataset, which sends the user hunting for the wrong problem.
    """
    connector = store if store is not None else view_store_connector(view_name, telemetry)
    if not connector.eidetic:
        return None
    try:
        return connector.get_view(view_name)
    except DatasetNotFoundError:
        return None


@lru_cache_with_expiry(maxsize=128, ttl=300)
def _view_as_plan(view_sql: str) -> tuple:
    """Return (logical_plan, ctes) for a view's SQL.

    The plan is NOT rewritten here. The resolver splices it into the calling query and
    the Plan Rewriter then runs once over the whole expanded plan — so a subquery in a
    view body is eliminated by the same pass that handles the main query.

    The view's own CTEs are returned alongside the plan: they are the scope the resolver
    resolves the view body against.
    """
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.planner.sql_rewriter import do_sql_rewrite
    from opteryx.third_party import sqloxide

    # The same rewriter the query planner runs. This used only to strip comments and
    # collapse whitespace, which meant a view body could not use any of the syntax the
    # rewriter exists to translate; it now goes through the one path.
    clean_sql = do_sql_rewrite(view_sql)
    try:
        parsed_statements = sqloxide.parse_sql(clean_sql, _dialect="opteryx")
    except ValueError as parser_error:
        from opteryx.planner.parse_error import raise_parse_error

        raise_parse_error(clean_sql, parser_error)
    logical_plan, _, view_ctes = do_logical_planning_phase(parsed_statements[0])

    # views don't have an exit node
    plan_head = logical_plan.get_exit_points()[0]
    logical_plan.remove_node(plan_head, True)

    return logical_plan, view_ctes


def _bind_row_count_estimate(logical_plan: dict, row_count: Optional[int]) -> dict:
    """Bind a row count estimate to the logical plan's root node."""
    if row_count is None:
        return logical_plan

    root_nid = logical_plan.get_exit_points()[0]
    root_node = logical_plan[root_nid]
    root_node.estimated_row_count = row_count
    logical_plan[root_nid] = root_node
    return logical_plan
