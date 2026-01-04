# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Optional

from orso.tools import lru_cache_with_expiry

from opteryx.connectors import connector_factory
from opteryx.connectors.capabilities.eidetic import ViewDefinition


def get_view_plan(view_name: str, telemetry) -> dict:
    """Return the logical plan for a view, if it exists."""
    # DEBUG: print(f"Fetching view plan for {view_name}")
    definition = _get_view_definition(view_name, telemetry)
    if definition is None:
        return None
    view_sql = definition.statement
    view_plan = _view_as_plan(view_sql)
    statistics_bound = _bind_row_count_estimate(view_plan, definition.last_row_count)
    return statistics_bound


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


@lru_cache_with_expiry(max_size=128, valid_for_seconds=300)
def _view_as_plan(view_sql: str) -> dict:
    """Return the logical plan for a view."""
    from opteryx.planner.logical_planner import do_logical_planning_phase
    from opteryx.third_party import sqloxide
    from opteryx.utils.sql import clean_statement
    from opteryx.utils.sql import remove_comments

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
