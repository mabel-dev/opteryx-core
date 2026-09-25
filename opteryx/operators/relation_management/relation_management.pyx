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
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Relation Management Node

Handles CREATE / DROP / TRUNCATE TABLE operations at execution time.
Operates on relations managed by Writable connectors.
"""

from typing import Optional

from opteryx.constants import QueryStatus
from opteryx.exceptions import DatasetNotFoundError
from opteryx.exceptions import InvalidInternalStateError
from opteryx.exceptions import md_code
from opteryx.exceptions import md_column
from opteryx.models import NonTabularResult
from opteryx.models import object_message
from opteryx.models import QueryProperties

# BasePlanNode/JoinNode in scope via _operators.pyx include.


def _trigger_holder_exists(connector, holder: str) -> bool:
    """Whether `holder` names something a trigger can hang off.

    A commit trigger lives under a dataset; a schedule or signal trigger lives
    under the task it fires. ALTER and DROP name the holder without saying
    which, so either kind answers. `is_task` is on the Writable capability
    with a False default, so a store with no tasks answers rather than raises.
    """
    return connector.relation_exists(holder) or connector.is_task(holder)


def _names(values) -> str:
    """A comma-separated list of object names, each in a code span."""
    return ", ".join(md_code(value) for value in values or ())


def _absent(name: str) -> str:
    """The IF EXISTS no-op: the statement ran, the target was not there."""
    return f"{md_code(name)} does not exist, nothing changed"


def _trigger_event(node) -> str:
    """The event half of a CREATE TRIGGER, as EXPLAIN shows it: `<table>`,
    `schedule '<cron>' [at time zone '<zone>'] [over <table>]` or
    `signal [over <table>]`."""
    if node.event_kind == "schedule":
        event = f"schedule '{node.schedule}'"
        if node.time_zone:
            event += f" at time zone '{node.time_zone}'"
    elif node.event_kind == "signal":
        event = "signal"
    else:
        return node.table_name
    if node.window_source:
        event += f" over {node.window_source}"
    return event


class RelationManagementNode(BasePlanNode):
    def __init__(self, properties: QueryProperties, step, str action):
        """`step` is the statement's typed plan step; every field an action reads
        is read off it, so an action can only read what its statement declares.
        `action` names which statement this is (the physical planner's dispatch)."""
        BasePlanNode.__init__(self, properties, step, step.columns, step.pre_update_columns)
        self.action = action

    @property
    def name(self):
        return "Relation Management"

    @property
    def config(self):
        if self.action == "drop_relation":
            return f"drop {', '.join(self.step.relation_names or [])}"
        if self.action == "create_relation" and self.step.relationships:
            return (
                f"create {self.step.relation_name} with "
                f"{len(self.step.relationships)} declared relationship(s)"
            )
        if self.action == "create_collection":
            return f"create collection {self.step.collection_name}"
        if self.action == "clone_collection":
            return f"clone collection {self.step.source_collection} into {self.step.collection_name}"
        if self.action == "clone_relation":
            return f"clone {self.step.source_relation} into {self.step.relation_name}"
        if self.action == "resync_relation":
            return f"resync {self.step.relation_name}" + (" (force)" if self.step.force else "")
        if self.action == "detach_relation":
            return f"detach {self.step.relation_name}"
        if self.action == "drop_collection":
            return f"drop collection {', '.join(self.step.collection_names or [])}"
        if self.action == "cluster_by":
            return f"cluster {self.step.relation_name} by ({', '.join(self.step.cluster_columns or [])})"
        if self.action == "rename_relation":
            return f"rename {self.step.relation_name} to {self.step.new_relation_name}"
        if self.action == "add_column":
            return f"add column {self.step.column_name} to {self.step.relation_name}"
        if self.action == "drop_column":
            return f"drop column {self.step.column_name} from {self.step.relation_name}"
        if self.action == "rename_column":
            return f"rename column {self.step.column_name} to {self.step.new_column_name} on {self.step.relation_name}"
        if self.action == "alter_column_type":
            return f"alter column {self.step.column_name} on {self.step.relation_name} to {self.step.new_column_type}"
        if self.action == "add_relationship":
            return (
                f"add constraint {self.step.constraint_name} on {self.step.relation_name} "
                f"({self.step.column_name}) references {self.step.references_relation_name} "
                f"({self.step.references_column_name}) not enforced"
            )
        if self.action == "drop_relationship":
            return f"drop constraint {self.step.constraint_name} on {self.step.relation_name}"
        if self.action == "alter_workspace":
            return f"alter workspace {self.step.workspace_name} set {self.step.property_name} = {self.step.property_value}"
        if self.action == "alter_workspace_secure":
            if self.step.secure_destinations is None:
                return f"alter workspace {self.step.workspace_name} drop secure {self.step.secure_object}"
            return f"alter workspace {self.step.workspace_name} set secure {self.step.secure_object} to {', '.join(self.step.secure_destinations)}"
        if self.action == "drop_workspace":
            return f"drop workspace {self.step.workspace_name}"
        if self.action == "create_tag":
            return f"create tag {self.step.tag_name} on {self.step.relation_name} as of {self.step.version_spec}"
        if self.action == "drop_tag":
            return f"drop tag {self.step.tag_name} on {self.step.relation_name}"
        if self.action == "rollback_relation":
            return f"rollback {self.step.relation_name} to version {self.step.version_spec}"
        if self.action == "drop_trigger":
            return f"drop trigger {self.step.trigger_name} on {self.step.table_name}"
        if self.action == "create_task":
            on = f" on {self.step.on_table}" if self.step.on_table else ""
            return f"create task {self.step.task_name}{on}"
        if self.action == "create_trigger":
            return f"create trigger {self.step.trigger_name} on {_trigger_event(self.step)} execute {self.step.task_name}"
        if self.action == "alter_trigger_suspended":
            return f"alter trigger {self.step.trigger_name} on {self.step.table_name} {'suspend' if self.step.suspended else 'resume'}"
        if self.action == "alter_trigger_minimum_interval":
            return f"alter trigger {self.step.trigger_name} on {self.step.table_name} set minimum interval to {self.step.minimum_interval_seconds} seconds"
        if self.action == "drop_task":
            return f"drop task {self.step.task_name}"
        if self.action == "alter_task":
            return f"alter task {self.step.task_name} (redefine statement)"
        if self.action == "listen":
            return f"listen to {self.step.task_name} for {self.step.outcome.lower()}"
        if self.action == "unlisten":
            return f"unlisten {self.step.task_name}"
        if self.action == "alter_trigger_owner":
            return f"alter trigger {self.step.trigger_name} on {self.step.table_name} owner to {'CURRENT_USER' if self.step.owner_is_current_user else self.step.new_owner}"
        if self.action == "alter_materialized_view_suspended":
            return f"alter materialized view {self.step.relation_name} {'suspend' if self.step.suspended else 'resume'}"
        if self.action == "alter_materialized_view_owner":
            return f"alter materialized view {self.step.relation_name} owner to {'CURRENT_USER' if self.step.owner_is_current_user else self.step.new_owner}"
        if self.action == "grant_access":
            return f"grant {self.step.role} on {self.step.object_kind} {self.step.object_name} to user {self.step.principal}"
        if self.action == "revoke_access":
            return f"revoke {self.step.role} on {self.step.object_kind} {self.step.object_name} from user {self.step.principal}"
        if self.action == "call_procedure":
            # The ARGUMENT VALUES are not rendered. They are whatever the caller wrote
            # - a message body, a recipient - and this string reaches EXPLAIN output
            # and the query log, so the name is shown and the payload is not.
            return f"call {self.step.procedure_name} ({len(self.step.arguments or [])} argument(s))"
        return f"{self.action} {self.step.relation_name}"

    @property
    def _author(self):
        """The session user this DDL is attributed to, or None when unauthenticated.

        None is passed through rather than substituted, so a store that requires
        attribution rejects the write instead of recording an invented identity.
        """
        from opteryx.variables import resolve

        return resolve("external_user", self.properties.variables, None) or None

    @property
    def _subscriber(self):
        """The session user a subscription belongs to.

        A subscription must belong to a person, so an unauthenticated session
        cannot hold one - refused here rather than recorded against a null
        identity, which would be a subscription nobody could ever see or
        remove.
        """
        user = self.step.execution_context.user if self.step.execution_context else None
        if not user:
            raise PermissionError(
                "**LISTEN** and **UNLISTEN** need an authenticated user: a "
                "subscription belongs to a person, and this session has none."
            )
        return user

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        """Run the action, then say what it did.

        The receipt is built HERE rather than at each of the forty-odd return
        statements below, so there is one place holding the wording and no way
        for a new action to be added with a count and no sentence - `_receipt`
        refuses an action it does not know.
        """
        result = self._apply(morsel, **kwargs)
        result.message = self._receipt(result.record_count)
        return result

    def _receipt(self, count: int) -> str:
        """What this statement did, for the person who ran it.

        `count` is the same number the result carries, and its meaning is the
        action's: the number of relations dropped, the number of files a DETACH
        materialised, or - for the many actions that act on exactly one object -
        1 for "done" and 0 for "IF [NOT] EXISTS matched, nothing happened". The
        zero cases are spelled out rather than left to a bare "0", because the
        reader's question there is whether their statement did nothing or ran at
        all.

        Every action in `_apply` has an entry. An action without one raises: a
        receipt channel that silently says nothing for a new statement type is
        the kind of fake-green this codebase does not keep (CLAUDE.md 1/9).
        """
        action = self.action

        if action == "create_relation":
            if not count:
                return object_message("table", "", self.step.relation_name, "already exists, nothing created")
            return object_message("created", "table", self.step.relation_name)

        if action == "drop_relation":
            kind = "materialized view(s)" if self.step.is_materialized_view else "table(s)"
            if not count:
                return f"no {kind} dropped"
            return f"dropped {count:,} {kind}: {_names(self.step.relation_names)}"

        if action == "create_collection":
            return object_message("created", "collection", self.step.collection_name)

        if action == "clone_collection":
            return (
                f"cloned collection {md_code(self.step.source_collection)} to "
                f"{md_code(self.step.collection_name)} ({count:,} relation(s))"
            )

        if action == "clone_relation":
            return f"cloned {md_code(self.step.source_relation)} to {md_code(self.step.relation_name)}"

        if action == "resync_relation":
            return f"resynced {md_code(self.step.relation_name)} with its upstream"

        if action == "detach_relation":
            return (
                f"detached {md_code(self.step.relation_name)} "
                f"({count:,} borrowed file(s) materialized)"
            )

        if action == "drop_collection":
            if not count:
                return "no collection(s) dropped"
            return f"dropped {count:,} collection(s): {_names(self.step.collection_names)}"

        if action == "truncate_relation":
            return object_message("truncated", "", self.step.relation_name)

        if action == "cluster_by":
            if not count:
                return _absent(self.step.relation_name)
            return f"set cluster by on {md_code(self.step.relation_name)}"

        if action == "rename_relation":
            if not count:
                return _absent(self.step.relation_name)
            return f"renamed {md_code(self.step.relation_name)} to {md_code(self.step.new_relation_name)}"

        if action == "add_column":
            if not count:
                return _absent(self.step.relation_name)
            return f"added column {md_column(self.step.column_name)} to {md_code(self.step.relation_name)}"

        if action == "drop_column":
            if not count:
                return _absent(self.step.relation_name)
            return f"dropped column {md_column(self.step.column_name)} from {md_code(self.step.relation_name)}"

        if action == "rename_column":
            if not count:
                return _absent(self.step.relation_name)
            return (
                f"renamed column {md_column(self.step.column_name)} to "
                f"{md_column(self.step.new_column_name)} in {md_code(self.step.relation_name)}"
            )

        if action == "alter_column_type":
            if not count:
                return _absent(self.step.relation_name)
            return (
                f"changed the type of column {md_column(self.step.column_name)} in "
                f"{md_code(self.step.relation_name)}"
            )

        if action == "add_relationship":
            if not count:
                return _absent(self.step.relation_name)
            return (
                f"added constraint {md_code(self.step.constraint_name)} on "
                f"{md_code(self.step.relation_name)}"
            )

        if action == "drop_relationship":
            if not count:
                return (
                    f"constraint {md_code(self.step.constraint_name)} not found on "
                    f"{md_code(self.step.relation_name)}, nothing dropped"
                )
            return (
                f"dropped constraint {md_code(self.step.constraint_name)} on "
                f"{md_code(self.step.relation_name)}"
            )

        if action == "create_task":
            if not count:
                return object_message("task", "", self.step.task_name, "already exists, nothing created")
            if self.step.on_table:
                return (
                    f"created task {md_code(self.step.task_name)}, fired by commits to "
                    f"{md_code(self.step.on_table)}"
                )
            return object_message("created", "task", self.step.task_name)

        if action == "create_trigger":
            if not count:
                return object_message(
                    "trigger", "", self.step.trigger_name, "already exists, nothing created"
                )
            return (
                f"created trigger {md_code(self.step.trigger_name)} on "
                f"{md_code(_trigger_event(self.step))}"
            )

        if action == "alter_trigger_suspended":
            state = "suspended" if self.step.suspended else "resumed"
            return f"{state} trigger {md_code(self.step.trigger_name)} on {md_code(self.step.table_name)}"

        if action == "alter_trigger_minimum_interval":
            return (
                f"set the minimum interval of trigger {md_code(self.step.trigger_name)} on "
                f"{md_code(self.step.table_name)} to {self.step.minimum_interval_seconds:,} second(s)"
            )

        if action == "alter_trigger_owner":
            return (
                f"trigger {md_code(self.step.trigger_name)} on {md_code(self.step.table_name)} now "
                f"runs as {md_code(self.step.resolved_owner)}"
            )

        if action == "drop_task":
            return object_message("dropped", "task", self.step.task_name)

        if action == "alter_task":
            return f"redefined the statement of task {md_code(self.step.task_name)}"

        if action == "listen":
            return f"listening to task {md_code(self.step.task_name)}"

        if action == "unlisten":
            return f"no longer listening to task {md_code(self.step.task_name)}"

        if action == "drop_trigger":
            return (
                f"dropped trigger {md_code(self.step.trigger_name)} on {md_code(self.step.table_name)}"
            )

        if action == "create_tag":
            return (
                f"created tag {md_code(self.step.tag_name)} on {md_code(self.step.relation_name)} "
                f"at {md_code(self.step.version_spec)}"
            )

        if action == "drop_tag":
            return f"dropped tag {md_code(self.step.tag_name)} on {md_code(self.step.relation_name)}"

        if action == "rollback_relation":
            return (
                f"rolled {md_code(self.step.relation_name)} back to "
                f"{md_code(self.step.version_spec)}"
            )

        if action == "alter_materialized_view_owner":
            return f"changed the owner of materialized view {md_code(self.step.relation_name)}"

        if action == "alter_materialized_view_suspended":
            state = "suspended" if self.step.suspended else "resumed"
            return f"{state} refreshes of materialized view {md_code(self.step.relation_name)}"

        if action == "alter_workspace":
            return (
                f"set {md_column(self.step.property_name)} on workspace "
                f"{md_code(self.step.workspace_name)}"
            )

        if action == "alter_workspace_secure":
            if self.step.secure_destinations is None:
                return (
                    f"cleared the secure sanction on {md_code(self.step.secure_object)} in "
                    f"workspace {md_code(self.step.workspace_name)}"
                )
            return (
                f"marked {md_code(self.step.secure_object)} secure to "
                f"{len(self.step.secure_destinations):,} destination(s): "
                f"{_names(self.step.secure_destinations)}"
            )

        if action == "drop_workspace":
            return object_message("dropped", "workspace", self.step.workspace_name)

        if action == "grant_access":
            return (
                f"granted {md_column(self.step.role)} on {md_code(self.step.pattern)} to "
                f"{md_code(self.step.principal)}"
            )

        if action == "revoke_access":
            return (
                f"revoked {md_column(self.step.role)} on {md_code(self.step.pattern)} from "
                f"{md_code(self.step.principal)}"
            )

        if action == "call_procedure":
            return object_message("called", "procedure", self.step.procedure_name)

        raise InvalidInternalStateError(f"no receipt wording for relation action: {action}")

    def _apply(self, morsel=None, **kwargs) -> NonTabularResult:
        if self.action == "create_relation":
            if self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_not_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise ValueError(f"relation already exists: {self.step.relation_name}")
            self.step.connector.create_relation(self.step.relation_name, self.step.schema, author=self._author)
            # CREATE TABLE ... CONSTRAINT. Written after the relation exists,
            # because the store is a subcollection on the dataset document and
            # there is nothing to hang one on before that. The binder has
            # already authorized every far end and checked both columns, so
            # what is left here is the write.
            for relationship in self.step.relationships or []:
                self.step.connector.declare_relationship(
                    relation_parts=relationship["relation_parts"],
                    column_name=relationship["column_name"],
                    references_relation_parts=relationship["references_relation_parts"],
                    references_column_name=relationship["references_column_name"],
                    constraint_name=relationship["constraint_name"],
                    cardinality=relationship["cardinality"],
                    author=self._author,
                )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_relation":
            dropped = 0
            for relation_name in self.step.relation_names:
                connector = self.step.connectors[relation_name]
                if not connector.relation_exists(relation_name):
                    if self.step.if_exists:
                        continue
                    raise DatasetNotFoundError(connector=connector, dataset=relation_name)
                # Type guard in both directions: a materialized view's backing
                # store is a dataset, so DROP TABLE would "work" on it - but
                # would strand its refresh triggers on every source table.
                target_is_mv = connector.is_materialized_view(relation_name)
                if self.step.is_materialized_view:
                    if not target_is_mv:
                        raise ValueError(
                            f"{relation_name} is not a materialized view; "
                            "use DROP TABLE or DROP VIEW"
                        )
                    connector.drop_materialized_view(
                        relation_name, if_exists=bool(self.step.if_exists), author=self._author
                    )
                else:
                    if target_is_mv:
                        raise ValueError(
                            f"{relation_name} is a materialized view; "
                            "use DROP MATERIALIZED VIEW"
                        )
                    connector.drop_relation(
                        relation_name, if_exists=bool(self.step.if_exists), author=self._author
                    )
                dropped += 1
            return NonTabularResult(record_count=dropped, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_collection":
            # No exists-check here, unlike create_relation: the connector settles
            # existence in a single atomic call, so there is no window between
            # checking and creating. The cost is that IF NOT EXISTS cannot report
            # 0-vs-1 for "already there" - the count is 1 for "the collection now
            # exists", not "a collection was created this instant".
            self.step.connector.create_collection(
                self.step.collection_name, if_not_exists=bool(self.step.if_not_exists), author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "clone_collection":
            # One statement, one refusal: the connector checks every target
            # name before it forks anything, because a half-cloned collection
            # is the state nobody can act on.
            cloned = self.step.connector.clone_collection(
                self.step.collection_name, self.step.source_collection, author=self._author
            )
            return NonTabularResult(record_count=cloned, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "clone_relation":
            # No bytes move: the new dataset's manifest names the source's
            # files. The record count is 1 - the dataset created - rather than
            # a row count, which would be the upstream's and would read as
            # though this statement had written those rows.
            self.step.connector.clone_relation(
                self.step.relation_name,
                self.step.source_relation,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "resync_relation":
            # The catalog settles whether this is a fork, whether it is behind,
            # and whether FORCE was needed - all three are facts about two
            # datasets' current snapshots, and none of them survives being
            # decided earlier.
            self.step.connector.resync_relation(
                self.step.relation_name, author=self._author, force=bool(self.step.force)
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "detach_relation":
            # The one fork statement that copies. The count is the number of
            # borrowed files materialised, which is what the caller is now
            # storing and being billed for.
            copied = self.step.connector.detach_relation(self.step.relation_name, author=self._author)
            return NonTabularResult(record_count=copied, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_collection":
            dropped = 0
            for collection_name in self.step.collection_names:
                connector = self.step.connectors[collection_name]
                if not connector.collection_exists(collection_name):
                    if self.step.if_exists:
                        continue
                    raise DatasetNotFoundError(connector=connector, dataset=collection_name)
                connector.drop_collection(
                    collection_name, if_exists=bool(self.step.if_exists), author=self._author
                )
                dropped += 1
            return NonTabularResult(record_count=dropped, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "truncate_relation":
            if not self.step.connector.relation_exists(self.step.relation_name):
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            self.step.connector.truncate_relation(self.step.relation_name, author=self._author)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "cluster_by":
            if not self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            # Declared on the Writable mixin, and visit_alter_relation has
            # already rejected a non-Writable connector.
            self.step.connector.set_cluster_by(
                self.step.relation_name, self.step.cluster_columns, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "add_relationship":
            if not self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            # The workspace's own store, not the relation's catalog entry -
            # see Writable.declare_relationship. Both ends are in this
            # workspace; the logical planner refused the statement otherwise.
            self.step.connector.declare_relationship(
                relation_parts=self.step.relation_parts,
                column_name=self.step.column_name,
                references_relation_parts=self.step.references_relation_parts,
                references_column_name=self.step.references_column_name,
                constraint_name=self.step.constraint_name,
                cardinality=self.step.cardinality,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_relationship":
            if not self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            removed = self.step.connector.drop_relationship(
                relation_parts=self.step.relation_parts,
                constraint_name=self.step.constraint_name,
                if_exists=bool(self.step.constraint_if_exists),
                author=self._author,
            )
            return NonTabularResult(
                record_count=1 if removed else 0, status=QueryStatus.SQL_SUCCESS
            )

        elif self.action == "rename_relation":
            if not self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            # A rename must never absorb an existing relation - that would
            # destroy the target's data and history with no DROP in the SQL.
            if self.step.connector.relation_exists(self.step.new_relation_name):
                raise ValueError(f"relation already exists: {self.step.new_relation_name}")
            self.step.connector.rename_relation(
                self.step.relation_name, self.step.new_relation_name, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "add_column":
            if not self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            self.step.connector.add_column(
                self.step.relation_name,
                self.step.column_name,
                self.step.column_type,
                nullable=True if self.step.nullable is None else self.step.nullable,
                default=self.step.default,
                if_not_exists=bool(self.step.if_not_exists),
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_column":
            if not self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            self.step.connector.drop_column(
                self.step.relation_name,
                self.step.column_name,
                if_exists=bool(self.step.column_if_exists),
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "rename_column":
            if not self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            self.step.connector.rename_column(
                self.step.relation_name, self.step.column_name, self.step.new_column_name, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_column_type":
            if not self.step.connector.relation_exists(self.step.relation_name):
                if self.step.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.relation_name)
            self.step.connector.alter_column_type(
                self.step.relation_name, self.step.column_name, self.step.new_column_type, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_task":
            if self.step.if_not_exists and self.step.connector.is_task(self.step.task_name):
                # The whole statement, including its ON <table> trigger arm, is
                # a no-op: a task that already exists keeps its existing trigger
                # too, matching CREATE VIEW IF NOT EXISTS.
                return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
            self.step.connector.create_task(
                self.step.task_name,
                self.step.statement,
                author=self._author,
                or_replace=bool(self.step.or_replace),
                writes=self.step.target_tables or [],
                reads=self.step.source_tables or [],
            )
            if self.step.on_table:
                # Derived, not authored: the statement declared the dependency,
                # so the trigger that implements it is this statement's to make -
                # the same bargain CREATE MATERIALIZED VIEW strikes. `or_replace`
                # is passed so re-running the statement repoints its own trigger
                # rather than colliding with it.
                if not self.step.connector.relation_exists(self.step.on_table):
                    raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.on_table)
                self.step.connector.create_trigger(
                    self.step.on_table,
                    f"task__{self.step.task_name.replace('.', '__')}",
                    self.step.task_name,
                    author=self._author,
                    or_replace=True,
                )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_trigger":
            if self.step.if_not_exists and any(
                t.get("name") == self.step.trigger_name
                for t in self.step.connector.list_triggers(self.step.table_name)
            ):
                return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
            if (self.step.event_kind or "commit") == "commit":
                if not self.step.connector.relation_exists(self.step.table_name):
                    raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.table_name)
                # The call a commit trigger has always made, with no event
                # keywords: a connector (or a catalog behind one) that predates
                # the other two events keeps working unchanged.
                self.step.connector.create_trigger(
                    self.step.table_name,
                    self.step.trigger_name,
                    self.step.task_name,
                    author=self._author,
                    or_replace=bool(self.step.or_replace),
                )
            else:
                # A clock or a signal has no source dataset: the holder is the
                # task, so it is the task whose existence is checked - a dataset
                # of that name would be the wrong kind of thing to hang it off.
                if not self.step.connector.is_task(self.step.table_name):
                    raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.table_name)
                self.step.connector.create_trigger(
                    self.step.table_name,
                    self.step.trigger_name,
                    self.step.task_name,
                    author=self._author,
                    or_replace=bool(self.step.or_replace),
                    event_kind=self.step.event_kind or "commit",
                    schedule=self.step.schedule,
                    time_zone=self.step.time_zone,
                    window_source=self.step.window_source,
                )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_trigger_suspended":
            if not _trigger_holder_exists(self.step.connector, self.step.table_name):
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.table_name)
            self.step.connector.set_trigger_suspended(
                self.step.table_name,
                self.step.trigger_name,
                self.step.suspended,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_trigger_minimum_interval":
            # Pre-parse reduced the value to whole seconds; the store records
            # it and the catalog enforces it at fire time.
            if not self.step.connector.relation_exists(self.step.table_name):
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.table_name)
            self.step.connector.set_trigger_minimum_interval(
                self.step.table_name,
                self.step.trigger_name,
                self.step.minimum_interval_seconds,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_trigger_owner":
            # The binder resolved CURRENT_USER to the session identity and proved
            # that principal can be billed; this records the transfer.
            if not _trigger_holder_exists(self.step.connector, self.step.table_name):
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.table_name)
            self.step.connector.set_trigger_owner(
                self.step.table_name, self.step.trigger_name, self.step.resolved_owner, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_task":
            self.step.connector.drop_task(
                self.step.task_name,
                if_exists=bool(self.step.if_exists),
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_task":
            # Defense in depth: the binder already checked. Redefines the
            # statement (and what it reads/writes) only - never the trigger,
            # never the schedule.
            if not self.step.connector.is_task(self.step.task_name):
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.task_name)
            self.step.connector.alter_task_statement(
                self.step.task_name,
                self.step.statement,
                author=self._author,
                writes=self.step.target_tables or [],
                reads=self.step.source_tables or [],
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "listen":
            # The subscriber is the session user and can be nobody else. Read
            # from the execution context rather than `_author`, which answers
            # "who is this write attributed to" - a different question that
            # happens to have the same answer, and one whose None means
            # "unattributed" rather than "nobody to subscribe".
            self.step.connector.add_listener(
                self.step.task_name,
                user=self._subscriber,
                outcome=self.step.outcome,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "unlisten":
            self.step.connector.drop_listener(self.step.task_name, user=self._subscriber)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_trigger":
            # The holder must exist regardless of IF EXISTS - that modifier
            # speaks about the trigger, not the table or task it hangs off.
            if not _trigger_holder_exists(self.step.connector, self.step.table_name):
                raise DatasetNotFoundError(connector=self.step.connector, dataset=self.step.table_name)
            self.step.connector.drop_trigger(
                self.step.table_name,
                self.step.trigger_name,
                author=self._author,
                missing_ok=bool(self.step.if_exists),
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_tag":
            self.step.connector.create_tag(
                self.step.relation_name,
                self.step.tag_name,
                self.step.version_spec,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_tag":
            self.step.connector.drop_tag(
                self.step.relation_name,
                self.step.tag_name,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "rollback_relation":
            self.step.connector.rollback_relation(
                self.step.relation_name,
                self.step.version_spec,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_materialized_view_owner":
            new_owner = self.step.new_owner
            if self.step.owner_is_current_user:
                # Resolved here rather than at plan time so it is the identity
                # that actually ran the statement, not one captured earlier.
                new_owner = self._author
                if not new_owner:
                    raise ValueError(
                        "OWNER TO CURRENT_USER needs an authenticated session; "
                        "this one has no user to assign the view to."
                    )
            self.step.connector.set_materialized_view_owner(
                self.step.relation_name, new_owner, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_materialized_view_suspended":
            self.step.connector.set_materialized_view_suspended(
                self.step.relation_name, self.step.suspended, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_workspace":
            # `maintenance` is a property in the statement and a GRANT in the
            # store. It is not written to the workspace record at all: what it
            # names is whether the platform's maintenance identity holds WRITE
            # here, so turning it on issues that grant and turning it off
            # revokes it. One piece of state, so the setting and the authority
            # cannot drift apart - there is no second place for them to disagree.
            #
            # WHICH identity is not decided here, exactly as `grant_access`
            # below does not decide who may grant: the capability holds the
            # name, and an engine with none registered refuses rather than
            # reporting a success that granted nothing.
            if self.step.property_name == "maintenance":
                from opteryx.managers.permissions import set_workspace_maintenance

                set_workspace_maintenance(
                    self.step.execution_context, self.step.workspace_name, self.step.property_value
                )
                return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

            self.step.connector.set_workspace_property(
                self.step.workspace_name,
                self.step.property_name,
                self.step.property_value,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_workspace_secure":
            # The SOURCE workspace's record. The binder required ownership of it,
            # and the connector writes only that workspace's own entry - so the
            # destination cannot sanction a copy into itself from here or anywhere.
            if self.step.secure_destinations is None:
                self.step.connector.clear_workspace_secure(
                    self.step.workspace_name, self.step.secure_object, author=self._author
                )
            else:
                self.step.connector.mark_workspace_secure(
                    self.step.workspace_name,
                    self.step.secure_object,
                    list(self.step.secure_destinations),
                    author=self._author,
                )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_workspace":
            self.step.connector.drop_workspace(self.step.workspace_name, author=self._author)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "grant_access":
            # Adds exactly ONE policy. Every rule — owner authority covering the
            # pattern, the no-self-service rule, conflict refusal, the audit
            # record — lives in the registered permissions capability; the engine
            # hands over and reports. There is no upgrade path: changing an
            # existing grant is REVOKE then GRANT, by the caller.
            from opteryx.managers.permissions import apply_grant

            apply_grant(self.step.execution_context, self.step.pattern, self.step.role, self.step.principal)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "revoke_access":
            # Deletes exactly ONE policy, resolved 1:1 by (principal, pattern,
            # role). Access held through a policy at a different level errors,
            # naming that policy — never narrowed, never a silent no-op.
            from opteryx.managers.permissions import apply_revoke

            apply_revoke(self.step.execution_context, self.step.pattern, self.step.role, self.step.principal)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "call_procedure":
            # Re-resolved by name rather than carried as a callable on the plan, so
            # nothing live is pinned into a plan that gets copied and explained.
            # `plan_call` already proved the name resolves and the arity matches; a
            # miss here means the registry changed underneath the statement, which is
            # an error and not a no-op.
            from opteryx.procedures import get_procedure

            procedure = get_procedure(self.step.procedure_name)
            if procedure is None:
                raise ValueError(f"procedure is no longer registered: {self.step.procedure_name}")

            # Who is calling. Built here rather than captured at registration because
            # the registry is process-global: one registration serves every session, so
            # a procedure that addresses the caller ("notify SELF") can only learn who
            # that is from the statement being executed. `_author` is the same
            # `external_user` resolution the DDL actions above attribute with, and it
            # passes None through rather than inventing an identity.
            from opteryx.procedures import ProcedureContext
            from opteryx.variables import resolve

            context = ProcedureContext(
                user=self._author,
                billing_account=resolve("billing_account", self.properties.variables, None)
                or None,
                query_id=self.properties.query_id,
            )

            # Runs EXACTLY ONCE, and the handler is the only judge of whether it
            # worked: there is no success value to inspect, so a failure raises and the
            # statement fails with it. Nothing is caught here - swallowing the
            # exception would report SQL_SUCCESS for a notification that never sent.
            procedure.handler(context, *(self.step.arguments or []))
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        else:
            raise NotImplementedError(f"Unsupported relation action: {self.action}")
