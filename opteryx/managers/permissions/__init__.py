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

A capability answers five questions:

    can_perform_action(execution_context, resource, action) -> bool
    can_perform_workspace_action(execution_context, workspace, action) -> bool
    can_principal_perform_action(principal, resource, action) -> bool
    can_principal_own_materialized_view(principal) -> bool
    grants(identity, policies) -> list[dict]

The first two are the gates the binder and `information_schema` call, and both
ask about the session doing the asking.

The third asks about somebody else - a principal named in a statement, who has
no session here and whose policies this process was never issued. It exists
because a materialized view refreshes as a pinned identity rather than as its
author, so `ALTER MATERIALIZED VIEW ... OWNER TO` has to establish that the
incoming owner can read what the view reads before pinning it there. Resolving
that principal's policies and interpreting them both belong to the capability:
the engine is never handed policies it was not issued, and there is no second
implementation of what a policy means to drift away from this one.

The fourth asks whether a principal may be PINNED as a materialized view's
owner at all, which is a different question from what they may read. A
deployment has identities that are not accounts - the platform's own
automation - and they are uncosted: nothing bills them, because nothing sells
them. Pinning a view's refresh to one is a way to have work done for free, and
no reading check catches it, because those identities can read plenty. Which
names those are is the deployment's to know, not the engine's, so the engine
asks rather than holding a list.

The fifth backs `SHOW GRANTS` ($grants), so that what is reported and what is
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
    "can_principal_own_materialized_view",
    "can_principal_perform_action",
    "register_permissions_capability",
)

# The members a capability must provide. Checked once, at registration, so a
# capability missing one is reported when it is installed rather than when the
# first query happens to reach that gate.
_REQUIRED_MEMBERS = (
    "can_perform_action",
    "can_perform_workspace_action",
    "can_principal_perform_action",
    "can_principal_own_materialized_view",
    "grants",
)


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

    def can_principal_perform_action(self, principal: str, resource: str, action: str) -> bool:
        return True

    def can_principal_own_materialized_view(self, principal: str) -> bool:
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


def can_principal_perform_action(principal: str, table: str, action: str = "READ") -> bool:
    """Whether `principal` may perform `action` on `table`.

    Asked about somebody who is not the caller: there is no execution context
    because that principal has no session here, and the calling session's
    policies say nothing about what they hold. The capability resolves their
    policies itself.

    A caller's own authority is never a substitute for this answer. Statements
    that name a principal to act as - rather than acting as their author - have
    to establish what that principal can do, or authority becomes something a
    caller can hand out by naming somebody who has it.
    """
    return _capability().can_principal_perform_action(principal, table, action)


def can_principal_own_materialized_view(principal: str) -> bool:
    """Whether `principal` may be pinned as the identity a materialized view
    refreshes as.

    Not a question about a resource, and not answerable from one: a principal
    who can read every source of a view may still be one this deployment
    refuses to pin work on. The platform's own automation is the case that
    matters - those identities are not accounts, nothing bills them, and a view
    pinned to one refreshes forever at nobody's expense.

    Which names those are belongs to the deployment. The engine holds no list
    and recognises no name - it asks, and refuses when the answer is no.
    """
    return _capability().can_principal_own_materialized_view(principal)
