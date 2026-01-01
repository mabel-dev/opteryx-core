# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Iterable

from opteryx import config
from opteryx.exceptions import PermissionsError

USE_PERMISSIONS_SERVICE = config.USE_PERMISSIONS_SERVICE


def can_read_table(roles: Iterable[str], table: str, action: str = "READ") -> bool:
    """Check if any of the given roles can perform the action on the table.

    Args:
        roles (Iterable[str]): The roles to check.
        table (str): The table to check.
        action (str): The action to check. Defaults to "READ".

    Returns:
        bool: True if any role can perform the action on the table, False otherwise.
    """
    if not USE_PERMISSIONS_SERVICE:
        return True  # No permissions service configured, allow all
    if table.count(".") == 0:
        return True  # Local table, allow all

    from opteryx.managers.permissions.check_permission import Action
    from opteryx.managers.permissions.check_permission import check_permission

    try:
        response = check_permission(
            roles=list(roles),
            action=Action[action.upper()],
            resources=[table],
        )
        return response.decision == "allow"
    except Exception as exc:
        # On any error, deny access
        from orso.logging import get_logger

        get_logger().error(
            f"Permission check failed for roles {roles} on table {table} with action {action}: {exc}"
        )
        raise PermissionsError(f"Permission check failed for table {table} with action {action}")
