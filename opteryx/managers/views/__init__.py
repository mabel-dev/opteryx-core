# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Optional

from opteryx.connectors import connector_factory
from opteryx.connectors.capabilities.eidetic import ViewDefinition
from opteryx.utils import lru_cache_with_expiry


def _view_plan_from_definition(definition):
    """Build a view's logical plan from its definition (or None)."""
    if definition is None:
        return None
    view_plan = _view_as_plan(definition.statement)
    # Copy the cached plan so mutations during binding don't affect the cache
    view_plan = view_plan.copy()
    return _bind_row_count_estimate(view_plan, definition.last_row_count)


def get_view_plan(view_name: str, telemetry) -> dict:
    """Return the logical plan for a view, if it exists."""
    return _view_plan_from_definition(_get_view_definition(view_name, telemetry))


def resolve_relation(relation: str, telemetry):
    """Catalog resolution step: resolve a relation in a single catalog round
    trip, returning one of:

      ('view', view_logical_plan)   — expand it in place
      ('dataset', dataset_object)   — hand to table_engine via prefetched_table=
      (None, None)                  — unknown here; bind it normally

    Connectors exposing ``get_relation`` resolve the dataset and view in one
    ``get_all``; others fall back to a view-only probe so behaviour is
    unchanged. Non-eidetic connectors (e.g. local filesystem) never look up
    views, so they return (None, None) and bind on the normal path.
    """
    connector = connector_factory(relation, telemetry)
    if not connector.eidetic:
        return None, None
    resolver = getattr(connector, "get_relation", None)
    if resolver is None:
        definition = _get_view_definition(relation, telemetry)
        return ("view", _view_plan_from_definition(definition)) if definition else (None, None)
    kind, obj = resolver(relation)
    if kind == "view":
        return "view", _view_plan_from_definition(obj)
    if kind == "dataset":
        return "dataset", obj
    return None, None


def _get_view_definition(view_name: str, telemetry) -> Optional[ViewDefinition]:
    """Return the view definition for a view, if it exists."""

    connector = connector_factory(view_name, telemetry)
    if not connector.eidetic:
        return None
    try:
        view_definition = connector.get_view(view_name)
        if view_definition is None:
            return None
        return view_definition
    except Exception as exc:
        # Missing views or catalog errors are non-fatal for planning
        return None


@lru_cache_with_expiry(maxsize=128, ttl=300)
def _view_as_plan(view_sql: str) -> dict:
    """Return the logical plan for a view."""
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.third_party import sqloxide
    from opteryx.utils.sql import clean_statement, remove_comments

    clean_sql = clean_statement(remove_comments(view_sql))
    parsed_statements = sqloxide.parse_sql(clean_sql, _dialect="opteryx")
    logical_plan, _, _ = do_logical_planning_phase(parsed_statements[0])

    # views don't have an exit node
    plan_head = logical_plan.get_exit_points()[0]
    logical_plan.remove_node(plan_head, True)

    return logical_plan


def _bind_row_count_estimate(logical_plan: dict, row_count: Optional[int]) -> dict:
    """Bind a row count estimate to the logical plan's root node."""
    if row_count is None:
        return logical_plan

    root_nid = logical_plan.get_exit_points()[0]
    root_node = logical_plan[root_nid]
    root_node.estimated_row_count = row_count
    logical_plan[root_nid] = root_node
    return logical_plan
