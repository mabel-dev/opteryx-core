# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from fnmatch import fnmatch
from typing import Iterable

from opteryx.exceptions import PermissionsError
from opteryx.models import ExecutionContext

ACTION_MAP = {
    "READ": {"reader", "writer", "owner"},
    "DELETE": {"writer", "owner"},
    "WRITE": {"writer", "owner"},
    "UPDATE": {"writer", "owner"},
}


def can_perform_action(
    execution_context: ExecutionContext, table: str, action: str = "READ"
) -> bool:
    """Check if any of the given roles can perform the action on the table.

    Args:
        execution_context (ExecutionContext): The execution context containing access policies.
        table (str): The table to check.
        action (str): The action to check. Defaults to "READ".

    Returns:
        bool: True if any role can perform the action on the table, False otherwise.
    """
    if table.count(".") == 0:
        return action == "READ"  # Local table, allow reading, nothing else
    if table.startswith("public."):
        return action == "READ"  # Public schema, allow reading, nothing else

    username = execution_context.user
    if table.startswith(f"personal.{username}."):
        return True  # Personal schema, allow all actions

    policies: Iterable[dict] = execution_context.access_policies
    action_map = ACTION_MAP.get(action, set())

    try:
        for policy in policies:
            pattern = policy.get("pattern", "")
            role = policy.get("role", "reader")
            if role in action_map and fnmatch(table, pattern):
                return True
        return False

    except Exception as exc:
        # On any error, deny access
        from orso.logging import get_logger

        get_logger().error(
            f"Permission check failed for policies {policies} on table {table} with action {action}: {exc}"
        )
        raise PermissionsError(f"Permission denied for action {action} on table {table}.")
