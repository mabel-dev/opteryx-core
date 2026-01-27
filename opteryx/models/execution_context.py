# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import datetime
from dataclasses import dataclass
from dataclasses import field
from typing import Iterable
from typing import List

import pyarrow
from orso.types import OrsoTypes

from opteryx.shared.variables import SystemVariables
from opteryx.shared.variables import SystemVariablesContainer
from opteryx.shared.variables import VariableOwner
from opteryx.shared.variables import Visibility


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
        variables: dict
            System variables available during execution.
        access_policies: Optional[List[dict]]
            Policies defining access to datasets
    """

    query_id: str = None
    connected_at: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now(datetime.UTC), init=False
    )
    user: str = None
    schema: str = None
    memberships: Iterable[str] = None
    variables: SystemVariablesContainer = field(init=False)
    access_policies: List[dict] = field(default_factory=list)

    def __post_init__(self):
        """
        Initializes additional attributes after the object has been created.
        """
        # The initializer is a function rather than an empty constructor so we init here
        object.__setattr__(self, "variables", SystemVariables.snapshot(VariableOwner.USER))
        self.variables._variables["user_memberships"] = (
            OrsoTypes.ARRAY,
            pyarrow.array(self.memberships),
            VariableOwner.SERVER,
            Visibility.UNRESTRICTED,
        )
