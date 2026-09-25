# cython: language_level=3
# cython: nonecheck=False
# cython: cdivision=True
# cython: initializedcheck=False
# cython: infer_types=True
# cython: wraparound=False
# cython: boundscheck=False
# cython: optimize.use_switch=True
# cython: optimize.unpack_method_calls=True

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0

"""
View Management Node

Handles CREATE/ALTER/DROP VIEW operations at execution time.
"""

from typing import Optional

from opteryx.connectors import TableType
from opteryx.constants import QueryStatus
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import md_code
from opteryx.models import NonTabularResult
from opteryx.models import QueryProperties
from opteryx.models import object_message

# BasePlanNode/JoinNode in scope via _operators.pyx include.


class ViewManagementNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, step, str action):
        """`step` is the statement's typed plan step, read directly by each
        action. `action`: 'create_view', 'alter_view', 'drop_view' or 'comment'."""
        BasePlanNode.__init__(self, properties, step, step.columns, step.pre_update_columns)
        self.action: str = action

    @property
    def name(self):  # pragma: no cover - simple string
        return "View Management"

    @property
    def config(self):  # pragma: no cover - simple string
        if self.action == "drop_view":
            return f"drop {', '.join(self.step.view_names or [])}"
        elif self.action == "comment":
            return f"comment on {self.step.object_name}"
        return f"{self.action} {self.step.view_name}"

    @property
    def _author(self):
        """The session user this DDL is attributed to, or None when unauthenticated.

        None is passed through rather than substituted, so a store that requires
        attribution rejects the write instead of recording an invented identity.
        """
        from opteryx.variables import resolve

        return resolve("external_user", self.properties.variables, None) or None

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        # Perform the action and return a NonTabularResult object

        if self.action in ("create_view", "alter_view"):
            if not self.step.connector:
                # The binder resolves the VIEW STORE and nothing else may: a
                # lazy connector_factory() here would re-derive the workspace's
                # DATA binding and write the definition to a source we do not
                # own - silently, and only for the workspaces where it matters.
                raise InvalidInternalStateError(
                    f"{self.action} reached execution without a bound view store"
                )

            if self.action == "create_view" and self.step.if_not_exists:
                existing_type, _ = self.step.connector.locate_object(self.step.view_name)
                if existing_type == TableType.View:
                    return NonTabularResult(
                        record_count=0,
                        status=QueryStatus.SQL_SUCCESS,
                        message=object_message(
                            "view", "", self.step.view_name, "already exists, nothing created"
                        ),
                    )

            # The session user, not a fixed literal - attributing every view to
            # "opteryx" made the stored owner useless for telling authors apart.
            # ALTER VIEW replaces by definition; CREATE only under OR REPLACE.
            update_if_exists = self.action == "alter_view" or bool(self.step.or_replace)
            self.step.connector.create_view(
                self.step.view_name,
                self.step.view_sql,
                update_if_exists=update_if_exists,
                owner=self._author,
                schema=self.step.view_schema,
            )

            # `update_if_exists` is what the store DID, so it is what the
            # receipt says: OR REPLACE over an existing view did not create one,
            # and ALTER VIEW never does.
            verb = "replaced" if update_if_exists else "created"
            return NonTabularResult(
                record_count=1,
                status=QueryStatus.SQL_SUCCESS,
                message=object_message(verb, "view", self.step.view_name),
            )

        elif self.action == "drop_view":
            if not self.step.view_names:
                raise ValueError("No view names supplied for DROP VIEW")

            dropped = 0
            for vn in self.step.view_names:
                # Bound by the binder, from the VIEW STORE - see create_view.
                if not self.step.connectors or vn not in self.step.connectors:
                    raise InvalidInternalStateError(
                        f"drop_view reached execution without a bound view store for {vn}"
                    )
                connector = self.step.connectors[vn]

                if connector.locate_object(vn)[0] != TableType.View:
                    if self.step.if_exists:
                        continue
                    raise DatasetNotFoundError(connector=connector, dataset=vn)

                connector.drop_view(vn, author=self._author)
                dropped += 1

            return NonTabularResult(
                record_count=dropped,
                status=QueryStatus.SQL_SUCCESS,
                message=f"dropped {dropped:,} view(s): {', '.join(md_code(v) for v in self.step.view_names)}",
            )

        elif self.action == "comment":
            # COMMENT ON VIEW/TABLE/EXTENSION
            if not self.step.object_name:
                raise ValueError("No object name supplied for COMMENT")

            if not self.step.connector:
                # visit_comment resolves this, choosing the view store or the
                # data binding by what the name holds; re-deriving one of them
                # here would comment on the wrong object's behalf.
                raise InvalidInternalStateError(
                    f"comment reached execution without a bound connector for {self.step.object_name}"
                )

            # Try to locate the object to verify it exists (unless IF EXISTS is specified)
            object_type, _ = self.step.connector.locate_object(self.step.object_name)
            if object_type not in (TableType.View, TableType.Table):
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.object_name)

            # Declared on the Writable mixin, and visit_comment has already
            # rejected a non-Writable connector.
            # The session user, not a fixed literal - every other DDL path here
            # attributes to _author, and recording "system" made the stored
            # describer useless for telling authors apart.
            self.step.connector.set_comment(self.step.object_name, self.step.comment, describer=self._author)

            return NonTabularResult(
                record_count=1,
                status=QueryStatus.SQL_SUCCESS,
                message=object_message("commented on", "", self.step.object_name),
            )

        else:
            raise NotImplementedError(f"Unsupported view action: {self.action}")
