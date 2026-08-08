# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from fnmatch import fnmatch
from typing import Iterable
from typing import List

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


def implicit_policies(username: str) -> List[dict]:
    """The grants every session holds without a policy being issued for them.

    These are hard-coded in the ENGINE, not handed over by the policy service,
    so they never appear in `execution_context.access_policies`. This is the
    SINGLE declaration of them: `can_perform_action` enforces this list and
    `SHOW GRANTS` ($grants) reports it, so the two cannot drift into disagreeing
    about what a caller holds.

    Returned in the policy dict shape the issued policies use, and in the order
    they are evaluated. Every pattern is `<namespace>.*` — see
    `can_perform_action` for why the `*` is matched as a literal prefix rather
    than a glob.

    An anonymous session (no username) holds no personal namespace: there is no
    `personal.<nobody>` for it to own.
    """
    policies = []
    if username:
        policies.append({"pattern": f"personal.{username}.*", "role": "owner"})
    policies.append({"pattern": "public.*", "role": "reader"})
    return policies


def can_perform_workspace_action(
    execution_context: ExecutionContext, workspace: str, action: str = "ALTER"
) -> bool:
    """Check whether the session may perform a workspace-level action.

    This is deliberately not `can_perform_action`: that function reads a name
    with no dots as a local table and short-circuits to READ-only, so a bare
    workspace name can never clear it.

    A policy grants a workspace-level action when it covers the workspace in
    full. "ws.*" is how ownership of the whole workspace is issued, so it
    qualifies; so does a pattern matching the bare name ("ws", "*"). A policy
    scoped to part of a workspace does not - "ws.coll.*" reduces to "ws.coll",
    which is not the workspace, so it grants nothing at this level.

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
            if role not in action_map:
                continue
            # A trailing ".*" spans everything under the name it qualifies; drop
            # it to ask what that name is. Anything else must match as written.
            covered = pattern[:-2] if pattern.endswith(".*") else pattern
            if fnmatch(workspace, covered):
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

    action_map = ACTION_MAP.get(action, set())

    # The implicit grants CAP what they cover: a name inside `public.` or inside
    # the caller's own `personal.` namespace is answered here and does NOT fall
    # through to the issued policies. That short-circuit is what makes `public.`
    # read-only for everyone regardless of what a policy says about it.
    #
    # The trailing `*` is stripped and the remainder matched as a literal
    # prefix, not with fnmatch: fnmatch normalizes case per-platform (so the
    # same policy would decide differently on macOS and Linux) and would treat
    # glob metacharacters in a username as live, widening the namespace a
    # caller owns.
    for policy in implicit_policies(execution_context.user):
        if table.startswith(policy["pattern"][:-1]):
            return policy["role"] in action_map

    policies: Iterable[dict] = execution_context.access_policies

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
