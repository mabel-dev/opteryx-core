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

A capability answers nine questions:

    can_perform_action(execution_context, resource, action) -> bool
    can_perform_workspace_action(execution_context, workspace, action) -> bool
    can_principal_perform_action(principal, resource, action) -> bool
    can_principal_own_materialized_view(principal) -> bool
    grants(identity, policies) -> list[dict]
    apply_grant(execution_context, pattern, role, principal) -> policy id
    apply_revoke(execution_context, pattern, role, principal) -> policy id
    grants_on(execution_context, pattern) -> list[dict]
    effective_grants_on(execution_context, pattern) -> list[dict]

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

The last four are the grant-administration surface behind the engine's
`GRANT`, `REVOKE`, `SHOW GRANTS ON`, and `SHOW EFFECTIVE GRANTS ON`
statements. The last two are the two questions an object can be asked: what
is stored AT it, and who can reach it at all (that, plus everything above it
that covers it). The engine speaks in patterns by the time these are
called - the planner maps `WORKSPACE w` to `w.*`, `COLLECTION w.c` to
`w.c.*`, `DATASET w.c.d` to `w.c.d` - and the
capability owns every rule (owner authority, the no-self-service rule,
1:1 resolution, conflict refusal, audit). The engine stores no policy and
interprets nothing.

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
    "apply_grant",
    "apply_revoke",
    "can_perform_action",
    "can_perform_workspace_action",
    "can_principal_own_materialized_view",
    "can_principal_perform_action",
    "effective_grants_on",
    "grants_on",
    "register_permissions_capability",
)

# The members a capability must provide. Checked once, at registration, so a
# capability missing one is reported when it is installed rather than when the
# first query happens to reach that gate.
#
# Adding a member here is a BREAKING CHANGE for any registered capability: one
# written against the previous list fails at registration, loudly, at start-up.
# That is the intended behaviour and the reason the check exists - a capability
# that is missing a member the engine will call must not be installed and then
# fail at the statement. `effective_grants_on` was added with
# `SHOW EFFECTIVE GRANTS ON`; a deployment must upgrade its capability in step.
_REQUIRED_MEMBERS = (
    "can_perform_action",
    "can_perform_workspace_action",
    "can_principal_perform_action",
    "can_principal_own_materialized_view",
    "grants",
    "apply_grant",
    "apply_revoke",
    "grants_on",
    "effective_grants_on",
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
        return [{"pattern": "*", "level": "", "role": "*", "actions": "*"}]

    def _refuse_administration(self, statement: str):
        # A GRANT that "succeeded" here would be fake green: there is no policy
        # store, so nothing was granted and nothing could ever be enforced.
        # Loud refusal, never a no-op.
        raise InvalidConfigurationError(
            config_item="permissions_capability",
            provided_value=self.name,
            valid_value_description=(
                f"a capability that can administer grants. {statement} needs a policy "
                "service to act on; this engine has none registered, so there is nothing "
                "to grant against and nothing that would enforce the result. Deployments "
                "register one at startup with register_permissions_capability()."
            ),
        )

    def apply_grant(self, execution_context, pattern: str, role: str, principal: str):
        self._refuse_administration("GRANT")

    def apply_revoke(self, execution_context, pattern: str, role: str, principal: str):
        self._refuse_administration("REVOKE")

    def grants_on(self, execution_context, pattern: str):
        self._refuse_administration("SHOW GRANTS ON")

    def effective_grants_on(self, execution_context, pattern: str):
        self._refuse_administration("SHOW EFFECTIVE GRANTS ON")


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


def apply_grant(execution_context, pattern: str, role: str, principal: str):
    """Add ONE policy: `role` on `pattern` to `principal`. Returns its id.

    Behind `GRANT <role> ON <object> TO USER <principal>`, with the object
    already mapped to its pattern by the planner. The capability owns every
    rule - owner authority covering the pattern, the no-self-service rule,
    conflict refusal, the audit record. There is no upgrade path: changing an
    existing grant is REVOKE then GRANT, by the caller.
    """
    return _capability().apply_grant(execution_context, pattern, role, principal)


def apply_revoke(execution_context, pattern: str, role: str, principal: str):
    """Delete ONE policy: the exact (`principal`, `pattern`, `role`) match.

    Behind `REVOKE <role> ON <object> FROM USER <principal>`. Strictly 1:1 -
    access held through a policy at a different level is reported by the
    capability (naming that policy and its level), never narrowed and never
    silently left in place. Returns the revoked policy's id.
    """
    return _capability().apply_revoke(execution_context, pattern, role, principal)


def grants_on(execution_context, pattern: str):
    """The rows behind `SHOW GRANTS ON <object>`: one row per stored policy
    AT the object, `(user, pattern, level, role)`.

    Gated by the capability on the same authority a mutation needs: who may
    see the grants on an object is who may change them.
    """
    return _capability().grants_on(execution_context, pattern)


def effective_grants_on(execution_context, pattern: str):
    """The rows behind `SHOW EFFECTIVE GRANTS ON <object>`: one row per stored
    policy that COVERS the object, in the same four columns.

    The other question an object can be asked. `grants_on` reports what is
    stored at it - 1:1 with what a GRANT or REVOKE there would act on - and so
    reports nothing for a dataset reachable only through the workspace owner's
    `w.*`. This reports that owner, and the `pattern` and `level` columns say
    which policy grants the access, which is what has to change to remove it.

    One row per covering policy, not per user: a user may reach an object
    through more than one, and collapsing them would hide the one an
    administrator has to act on. Gated identically to `grants_on`.

    Whether a policy covers the object is the capability's to decide, with the
    same matcher that decides real queries - the engine holds no second
    implementation of coverage to drift from it.
    """
    return _capability().effective_grants_on(execution_context, pattern)
