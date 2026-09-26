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

import copy
from typing import Dict
from typing import Optional
from typing import Tuple

from opteryx.connectors import view_store_connector
from opteryx.connectors.capabilities.eidetic import ViewDefinition
from opteryx.exceptions import DatasetNotFoundError
from opteryx.utils import lru_cache_with_expiry


def _view_plan_from_definition(definition, *, plan_context) -> Optional[Tuple[object, Dict[str, object]]]:
    """Build (plan, ctes) for a view definition, or None."""
    if definition is None:
        return None
    view_plan, view_ctes = _view_as_plan(definition.statement, plan_context=plan_context)
    return _bind_row_count_estimate(view_plan, definition.last_row_count), view_ctes


def resolve_relation(relation: str, telemetry, catalog_cache=None, memo=None, *, plan_context):
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

    A DATASET answer from that lookup is ALWAYS honoured: the catalog is
    authoritative (architect, 2026-09-17). It is the record of every relation a
    workspace exposes, whether the rows live in GCS or behind somebody else's
    socket, and for an externally-bound workspace its entries are refreshed from
    that source.

    Two earlier gates stood here and both are gone. The first compared the store
    to `connector_factory`'s answer by OBJECT identity, which reported every
    relation missing in an ordinary deployment - two resolvers over one catalog
    answer with two connector objects. The second compared their catalog
    FACTORIES, which fixed that but dropped every externally-bound relation,
    because a PostgreSQL connector holds no catalog to compare: each one was
    then re-described from the SOURCE at bind time, two round trips per scan to
    learn what the catalog already held, and with a worse row count than the
    refresh had measured.

    What the second gate was really protecting was an entry the refresh has
    never described: no columns, so a projection failed as "column not found"
    and a bare COUNT(*) was answered from the empty stub manifest as ZERO - a
    wrong answer reported as success. That is now handled where the record is
    actually read rather than by dropping every external answer: a connector
    handed a record it cannot build a schema from asks the server, and one
    handed a record that is described but INCOMPLETE refuses and names the
    refresh. See `PostgresTable._schema_from_catalog_record`.

    `catalog_cache` is an OPT-IN, caller-owned `CatalogCache`. It caches the round
    trip above and nothing else: what goes in it is the raw `(kind, object)` the
    connector answered with, BEFORE a view is turned into a plan. Caching the plan
    instead would hand the same plan object to every caller and the resolver splices
    (and so mutates) what it is given.

    Only the check path passes one. The dataset document is the version pointer and
    the catalog re-reads it every call for that reason, so an entry held for a minute
    is a plan built against a possibly superseded snapshot - see `opteryx.CatalogCache`
    for why that is fine for a check and wrong for anything that reads rows.

    `memo` is the resolver's per-STATEMENT store of the same raw tuples, filled ahead
    of time by `prefetch_relations` so that the relations named in one plan cost one
    round trip between them rather than one each. It is not a cache: it lives for one
    resolution and is not stale by construction, which is why the planner passes one
    where it will not pass a `CatalogCache`. A name resolved through it still gets its
    own `_finish` here.
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
            return ("view", _view_plan_from_definition(definition, plan_context=plan_context)) if definition else (None, None)

        raw = None if memo is None else memo.get(relation)
        if raw is None:
            raw = None if catalog_cache is None else catalog_cache.get(relation)
        if raw is None:
            raw = resolver(relation)
            if catalog_cache is not None:
                catalog_cache.put(relation, raw)
            if memo is not None:
                memo[relation] = raw
        return _finish(store, relation, raw, telemetry, plan_context=plan_context)
    finally:
        # The catalog lookup is a cloud round trip (Firestore), distinct from the GCS
        # manifest/footer fetch timed as time_binding_metadata. Kept separate so the two
        # cloud costs are visible independently.
        if telemetry is not None:
            telemetry.time_binding_catalog += _cat_time.monotonic_ns() - _cat0


def _finish(store, relation: str, raw, telemetry, *, plan_context):
    """Turn a connector's raw ``(kind, obj)`` answer into the resolver's answer.

    Run PER REFERENCE, never once per name: a view becomes a fresh plan copy here
    (the resolver splices, and so mutates, what it is given), and the dataset gate
    below is evaluated against the relation actually being resolved. Only the round
    trip that produced `raw` is shared - see `prefetch_relations`.
    """
    kind, obj = raw
    if kind == "view":
        return "view", _view_plan_from_definition(obj, plan_context=plan_context)
    if kind == "dataset":
        # THE CATALOG IS AUTHORITATIVE (architect, 2026-09-17) - see
        # `resolve_relation` for what this replaced and what now guards the case
        # the old gate was really protecting against.
        return "dataset", obj
    return None, None


def prefetch_relations(relations, telemetry, memo, catalog_cache=None) -> None:
    """Look up several relations in as few catalog round trips as the stores allow,
    filling `memo` with the raw ``(kind, object)`` each one answered with.

    This is the ONLY thing that is shared between references: `resolve_relation`
    still runs per reference and still builds a relation its own view plan, so
    two references to one view cannot end up spliced from one plan object.

    What goes in `memo` is the same raw tuple `CatalogCache` holds, and for the same
    reason - the dataset object is safe to share (the connector reads it and keeps its
    own mutable state), a built view plan is not.

    Relations are grouped by the catalog that would be asked for them, because that is
    the thing a round trip is made to; a store that offers no plural `get_relations` is
    simply left to `resolve_relation`'s per-name path, which is exactly today's cost.
    Nothing here raises: a name that cannot be looked up in a batch is left out of the
    memo and pays its own round trip, where the error surfaces against that one name.
    """
    import time as _cat_time

    by_store: Dict[int, Tuple[object, list]] = {}
    for relation in relations:
        if relation in memo:
            continue
        if catalog_cache is not None:
            cached = catalog_cache.get(relation)
            if cached is not None:
                memo[relation] = cached
                continue
        store = view_store_connector(relation, telemetry)
        if not store.eidetic or getattr(store, "get_relations", None) is None:
            continue
        entry = by_store.setdefault(id(store), (store, []))
        if relation not in entry[1]:
            entry[1].append(relation)

    _cat0 = _cat_time.monotonic_ns()
    try:
        for store, names in by_store.values():
            if len(names) < 2:
                # One name is one round trip either way; leave it to resolve_relation
                # so a failure is raised from the same place it always was.
                continue
            for relation, raw in store.get_relations(names).items():
                memo[relation] = raw
                if catalog_cache is not None:
                    catalog_cache.put(relation, raw)
    finally:
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


def _view_as_plan(view_sql: str, *, plan_context) -> tuple:
    """Return (logical_plan, ctes) for a view's SQL, planned into THIS query's context.

    Only the parse is cached (`_parse_view`); planning runs per query because every
    column a plan carries is minted in the query's own column table (architect,
    2026-09-26) - a plan cached across queries would carry another query's columns.

    The plan is NOT rewritten here. The resolver splices it into the calling query and
    the Plan Rewriter then runs once over the whole expanded plan — so a subquery in a
    view body is eliminated by the same pass that handles the main query.

    The view's own CTEs are returned alongside the plan: they are the scope the resolver
    resolves the view body against.
    """
    from opteryx.planner.logical_planner import do_logical_planning_phase

    logical_plan, _, view_ctes = do_logical_planning_phase(
        copy.deepcopy(_parse_view(view_sql)), plan_context=plan_context
    )

    # views don't have an exit node
    plan_head = logical_plan.get_exit_points()[0]
    logical_plan.remove_node(plan_head, True)

    return logical_plan, view_ctes


@lru_cache_with_expiry(maxsize=128, ttl=300)
def _parse_view(view_sql: str) -> dict:
    """Rewrite and parse a view's SQL. Cached: callers must copy before planning."""
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
    return parsed_statements[0]


def _bind_row_count_estimate(logical_plan: dict, row_count: Optional[int]) -> dict:
    """Bind a row count estimate to the logical plan's root node."""
    if row_count is None:
        return logical_plan

    root_nid = logical_plan.get_exit_points()[0]
    root_node = logical_plan[root_nid]
    root_node.estimated_row_count = row_count
    logical_plan[root_nid] = root_node
    return logical_plan
