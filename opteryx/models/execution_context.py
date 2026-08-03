# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import datetime
from dataclasses import dataclass, field
from typing import Iterable, List

from opteryx.variables import SystemVariables, SystemVariablesContainer, VariableOwner


@dataclass
class ExecutionContext:
    """
    Manages the context for query execution.

    Previously named ConnectionContext, renamed to reflect that this is about
    query execution, not connection state.

    Attributes:
        connection_id: int
            Unique identifier for the execution context.
        connected_at: datetime.datetime
            Timestamp indicating when the context was established.
        user: str, optional
            User identity for the execution, defaults to None.
        schema: str, optional
            Schema to be used in the execution, defaults to None.
        memberships: Iterable[str], optional
            Groups/roles the user belongs to.
        entitlements: Iterable[str], optional
            Platform-level capabilities granted to the caller (e.g. `data_admin`).
            Distinct from `access_policies`, which are per-dataset pattern/role
            grants: an entitlement is a property of the CALLER, not of a table.
            Carried so the engine can report what the caller holds; it does NOT
            by itself grant anything here — the submitting service still derives
            row/visibility filters from these before handing over the query.
            Defaults to empty: absent entitlements must never be assumed.
        variables: dict
            System variables available during execution.
        access_policies: Optional[List[dict]]
            Policies defining access to datasets
        billing_account: str, optional
            Account usage from this execution is billed to. Distinct from `user`:
            many users can bill to one account.
    """

    query_id: str = None
    connected_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC), init=False
    )
    user: str = None
    schema: str = None
    memberships: Iterable[str] = None
    entitlements: Iterable[str] = None
    variables: SystemVariablesContainer = field(init=False)
    access_policies: List[dict] = field(default_factory=list)
    billing_account: str = None

    def __post_init__(self):
        """
        Initializes additional attributes after the object has been created.
        """
        # The initializer is a function rather than an empty constructor so we init here
        object.__setattr__(self, "variables", SystemVariables.snapshot(VariableOwner.USER))
        # Stamp this session's identity onto the INTERNAL identity variables. Only the
        # VALUE is replaced — type/owner/visibility are preserved from the registration
        # in opteryx/variables.py, which is the single declaration of what each variable
        # IS. Re-stating that metadata here is how `external_user` came to be
        # UNRESTRICTED in the table while still being hidden in practice: two
        # declarations, silently disagreeing. They are all INTERNAL-owned, so a USER
        # session cannot `SET` any of them (INTERNAL outranks USER) — in particular a
        # caller cannot grant themselves entitlements.
        for name, value in (
            ("user_memberships", list(self.memberships or [])),
            ("external_user", self.user or ""),
            ("user_entitlements", list(self.entitlements or [])),
            ("billing_account", self.billing_account or ""),
            # Mirrored onto a variable so `SHOW GRANTS` ($grants) can read it:
            # virtual datasets are handed `variables`, never the context itself.
            # This is a mirror of `access_policies`, not a second source of truth
            # — can_perform_action still reads the field, so a session cannot
            # widen its own grants by reaching the variable.
            ("access_policies", list(self.access_policies or [])),
        ):
            var_type, _old_value, owner, visibility = self.variables._variables[name]
            self.variables._variables[name] = (var_type, value, owner, visibility)
