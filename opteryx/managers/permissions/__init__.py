# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""The permissions capability - what a session may do, and who decides it.

The engine's intrinsic answer is that everything is allowed. Opteryx on its
own has no workspaces to own, no identities to grant to, and no policy service
to have issued anything: a CLI or embedded session reads what it can reach.
Access control is a property of a DEPLOYMENT, not of a query engine, so the
engine ships the permissive answer and nothing else.

A deployment that does have those things registers a permissions capability
over the intrinsic one:

    import opteryx
    import opteryx_access

    opteryx.register_permissions_capability(opteryx_access.capability())

Registration is the ONLY sanctioned way to change what a permission check
means. There is no capability sniffing on the check path and no fallback:
whatever is registered when a query binds is what decides it.

A capability answers three questions:

    can_perform_action(execution_context, resource, action) -> bool
    can_perform_workspace_action(execution_context, workspace, action) -> bool
    grants(identity, policies) -> list[dict]

The first two are the gates the binder and `information_schema` call. The
third backs `SHOW GRANTS` ($grants), so that what is reported and what is
enforced come from one place and cannot drift into disagreeing.

Returning `False` is how a capability denies; the callers turn that into the
refusal their layer reports. A capability may raise instead when it cannot
decide at all - a malformed policy, an unreachable policy store - and that
error is deliberately not caught here: an access check that failed to run is
not the same as one that ran and said no, and must never be flattened into it.
"""

from typing import Any
from typing import Dict
from typing import List

from opteryx.exceptions import InvalidConfigurationError

__all__ = (
    "active_permissions_capability",
    "can_perform_action",
    "can_perform_workspace_action",
    "register_permissions_capability",
)

# The members a capability must provide. Checked once, at registration, so a
# capability missing one is reported when it is installed rather than when the
# first query happens to reach that gate.
_REQUIRED_MEMBERS = ("can_perform_action", "can_perform_workspace_action", "grants")


class PermitAll:
    """The intrinsic capability: every action on every resource is allowed.

    Not a placeholder for a "real" implementation - this is the correct answer
    for an engine with no deployment around it. It is also the reason there is
    no None state to test for: a capability is always installed.
    """

    name = "permit-all"

    def can_perform_action(self, execution_context, resource: str, action: str) -> bool:
        return True

    def can_perform_workspace_action(self, execution_context, workspace: str, action: str) -> bool:
        return True

    def grants(self, identity: str, policies: List[dict]) -> List[Dict[str, Any]]:
        """One row saying so.

        The session's issued policies are deliberately NOT reported here. With
        no capability registered they decide nothing, and listing them would
        describe a restriction that is not being applied - the one thing
        `SHOW GRANTS` exists to be trusted about.
        """
        return [{"pattern": "*", "role": "*", "actions": "*"}]


_CORE = PermitAll()

_active = _CORE

# Set the first time a check is answered. A capability registered after that
# would mean two queries in one process were decided by different rules, with
# nothing in either result saying which - see `register_permissions_capability`.
_consulted = False


def register_permissions_capability(capability) -> None:
    """Install `capability` as the one that decides permission checks.

    Must be called before the first check is answered - typically at start-up,
    alongside `set_default_connector`. Registering after a check has already
    been decided raises, rather than silently changing the rules underneath a
    process that has already let something through under the old ones.

    Raises:
        InvalidConfigurationError: if `capability` is missing a required
            member, or if a check has already been answered.
    """
    global _active

    missing = [member for member in _REQUIRED_MEMBERS if getattr(capability, member, None) is None]
    if missing:
        raise InvalidConfigurationError(
            config_item="permissions_capability",
            provided_value=type(capability).__name__,
            valid_value_description=(
                f"an object providing {', '.join(_REQUIRED_MEMBERS)} - this one is "
                f"missing {', '.join(missing)}."
            ),
        )

    if _consulted:
        # Queries already let through under the old rules cannot be recalled, and
        # nothing in their results says which rules decided them. Refuse rather
        # than let one process answer the same question two ways.
        raise InvalidConfigurationError(
            config_item="permissions_capability",
            provided_value=type(capability).__name__,
            valid_value_description=(
                "a capability registered before any permission check is answered - this "
                f"process has already decided one under {type(_active).__name__}. "
                "Register the capability during startup, before running queries."
            ),
        )

    _active = capability


def active_permissions_capability():
    """The capability currently deciding checks. Never None - the core is always there."""
    return _active


def _capability():
    """The active capability, marking it as having decided something."""
    global _consulted
    _consulted = True
    return _active


def can_perform_action(execution_context, table: str, action: str = "READ") -> bool:
    """Whether this session may perform `action` on `table`."""
    return _capability().can_perform_action(execution_context, table, action)


def can_perform_workspace_action(execution_context, workspace: str, action: str = "ALTER") -> bool:
    """Whether this session may perform `action` on `workspace` as a whole.

    Deliberately separate from `can_perform_action`: a bare workspace name is
    not a relation, and a grant over part of a workspace does not carry
    authority over the workspace itself.
    """
    return _capability().can_perform_workspace_action(execution_context, workspace, action)
