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
    # Creating a brand-new relation risks nothing existing; a writer may do it.
    "CREATE": {"writer", "owner"},
    # Dropping a relation destroys it and its history; a writer may change a
    # relation's contents but only an owner may remove the relation itself.
    # CREATE OR REPLACE on an existing relation reuses this tier: it has the
    # same blast radius as DROP (the old relation's data/history is gone).
    "DROP": {"owner"},
    # ALTER changes a relation's physical layout (e.g. CLUSTER BY) rather than
    # its contents — same tier as DROP: a writer may change what's in a
    # relation, but only an owner may change what the relation fundamentally is.
    "ALTER": {"owner"},
    # SHOW MANIFEST FOR exposes file paths and layout (bucket/partition
    # structure), not just data — stricter than a normal READ.
    "MANIFEST": {"owner"},
}


def can_perform_workspace_action(
    execution_context: ExecutionContext, workspace: str, action: str = "ALTER"
) -> bool:
    """Check whether the session may perform a workspace-level action.

    This is deliberately not `can_perform_action`: that function reads a name
    with no dots as a local table and short-circuits to READ-only, and a policy
    pattern granting ownership *inside* a workspace (e.g. "ws.*") does not
    fnmatch the bare workspace name anyway. Ownership of the workspace itself is
    required and is not implied by owning anything within it - a policy must
    match the workspace name directly (e.g. "ws", or "*").

    Args:
        execution_context (ExecutionContext): The execution context containing access policies.
        workspace (str): The workspace name.
        action (str): The action to check. Defaults to "ALTER".

    Returns:
        bool: True if any role can perform the action on the workspace, False otherwise.
    """
    policies: Iterable[dict] = execution_context.access_policies
    action_map = ACTION_MAP.get(action, set())

    try:
        for policy in policies:
            pattern = policy.get("pattern", "")
            role = policy.get("role", "reader")
            if role in action_map and fnmatch(workspace, pattern):
                return True
        return False

    except Exception as exc:
        # On any error, deny access
        from opteryx.logging import get_logger

        get_logger().error(
            f"Permission check failed for policies {policies} on workspace {workspace} with action {action}: {exc}"
        )
        raise PermissionsError(f"Permission denied for action {action} on workspace {workspace}.")


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
        from opteryx.logging import get_logger

        get_logger().error(
            f"Permission check failed for policies {policies} on table {table} with action {action}: {exc}"
        )
        raise PermissionsError(f"Permission denied for action {action} on table {table}.")
