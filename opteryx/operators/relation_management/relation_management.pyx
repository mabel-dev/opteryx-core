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
from opteryx.models import NonTabularResult
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
    def __init__(self, properties: QueryProperties, **parameters):
        BasePlanNode.__init__(self, properties=properties, **parameters)
        self.action: str = parameters.get("action")

        # CREATE
        self.relation_name: Optional[str] = parameters.get("relation_name")
        self.schema = parameters.get("schema")
        self.if_not_exists: bool = parameters.get("if_not_exists", False)

        # DROP
        self.relation_names = parameters.get("relation_names")
        self.connectors = parameters.get("connectors")
        self.if_exists: bool = parameters.get("if_exists", False)
        # DROP MATERIALIZED VIEW arrives as a flagged drop_relation
        self.is_materialized_view: bool = parameters.get("is_materialized_view", False)

        # CREATE COLLECTION
        self.collection_name: Optional[str] = parameters.get("collection_name")

        # DROP COLLECTION
        self.collection_names = parameters.get("collection_names")

        # ALTER ... CLUSTER BY
        self.cluster_columns = parameters.get("cluster_columns")

        # ALTER ... RENAME TO
        self.new_relation_name: Optional[str] = parameters.get("new_relation_name")

        # ADD / DROP / RENAME COLUMN, ALTER COLUMN ... TYPE
        self.column_name: Optional[str] = parameters.get("column_name")
        self.column_type = parameters.get("column_type")
        self.column_nullable: bool = parameters.get("nullable", True)
        self.column_default = parameters.get("default")
        self.column_if_not_exists: bool = parameters.get("if_not_exists", False)
        self.column_if_exists: bool = parameters.get("column_if_exists", False)
        self.new_column_name: Optional[str] = parameters.get("new_column_name")
        self.new_column_type = parameters.get("new_column_type")

        # ADD / DROP CONSTRAINT (declared column relationships)
        self.constraint_name: Optional[str] = parameters.get("constraint_name")
        self.constraint_if_exists: bool = parameters.get("constraint_if_exists", False)
        # Split parts, never a dotted string - see opteryx.managers.relationships.
        self.relation_parts = parameters.get("relation_parts")
        self.references_relation_name: Optional[str] = parameters.get("references_relation_name")
        self.references_relation_parts = parameters.get("references_relation_parts")
        self.references_column_name: Optional[str] = parameters.get("references_column_name")
        self.cardinality: Optional[str] = parameters.get("cardinality")

        # CREATE / DROP TAG, ROLLBACK TO VERSION
        self.tag_name: Optional[str] = parameters.get("tag_name")
        # "current" | "previous" | a tag name | a snapshot id as text. Left as the
        # reader wrote it: resolving any of the first three is a catalog read,
        # and the connector is what holds the catalog.
        self.version_spec: Optional[str] = parameters.get("version_spec")

        # CREATE / ALTER / DROP TRIGGER. `table_name` is the HOLDER the trigger
        # hangs off: the dataset whose commits fire it, or - for a schedule or
        # signal trigger, which has no source dataset - the task it fires.
        self.trigger_name: Optional[str] = parameters.get("trigger_name")
        self.table_name: Optional[str] = parameters.get("table_name")
        # CREATE TRIGGER's event: "commit" | "schedule" | "signal", and the
        # clauses that describe a clock or a signal. All None for a commit
        # trigger, whose event is the commit itself.
        self.event_kind: str = parameters.get("event_kind") or "commit"
        self.schedule: Optional[str] = parameters.get("schedule")
        self.time_zone: Optional[str] = parameters.get("time_zone")
        self.window_source: Optional[str] = parameters.get("window_source")

        # CREATE TASK / DROP TASK. `statement` carries the task's SQL with its
        # `:name` placeholders intact - they are bound when it is EXECUTEd, not
        # when it is defined.
        self.task_name: Optional[str] = parameters.get("task_name")
        # CREATE TASK ... ON <table>: the dataset whose commits fire it. The
        # trigger is created alongside the task, so one statement leaves nothing
        # half-wired.
        self.on_table: Optional[str] = parameters.get("on_table")
        # ALTER TASK ... OWNER TO. `resolved_owner` is the binder's answer, with
        # CURRENT_USER already turned into the principal it names.
        self.resolved_owner: Optional[str] = parameters.get("resolved_owner")
        self.statement: Optional[str] = parameters.get("statement")
        self.or_replace: bool = parameters.get("or_replace", False)
        # What the task's statement WRITES, derived from its own AST by
        # `plan_create_task` and recorded so a pipeline can be followed THROUGH a
        # task rather than ending at it. A list because TRUNCATE names several;
        # empty for a task that reads and writes nothing back.
        self.target_tables: list = parameters.get("target_tables") or []

        # LISTEN TO / UNLISTEN. The subscriber is never carried: it is always
        # the session user, taken from the execution context below, because
        # there is no syntax that names a principal.
        self.outcome: Optional[str] = parameters.get("outcome")

        # ALTER MATERIALIZED VIEW ... OWNER TO
        self.new_owner: Optional[str] = parameters.get("new_owner")
        self.owner_is_current_user: bool = parameters.get("owner_is_current_user", False)
        self.suspended = parameters.get("suspended")
        self.minimum_interval_seconds = parameters.get("minimum_interval_seconds")

        # ALTER WORKSPACE ... SET
        self.workspace_name: Optional[str] = parameters.get("workspace_name")
        self.property_name: Optional[str] = parameters.get("property_name")
        self.property_value = parameters.get("property_value")
        # ALTER WORKSPACE ... SET SECURE <object> TO <ws>[, ...] | DROP SECURE <object>.
        # `secure_destinations` is None for DROP - the sanction is withdrawn.
        self.secure_object: Optional[str] = parameters.get("secure_object")
        self.secure_destinations = parameters.get("secure_destinations")

        # GRANT / REVOKE
        self.pattern: Optional[str] = parameters.get("pattern")
        self.role: Optional[str] = parameters.get("role")
        self.principal: Optional[str] = parameters.get("principal")
        self.object_kind: Optional[str] = parameters.get("object_kind")
        self.object_name: Optional[str] = parameters.get("object_name")
        # Stashed by the binder (visit_grant_access/visit_revoke_access): the
        # capability needs the acting identity at execution time, where there
        # is no BindingContext to read it from.
        self.execution_context = parameters.get("execution_context")

        # CALL <procedure>(...)
        self.procedure_name: Optional[str] = parameters.get("procedure_name")
        self.arguments: Optional[list] = parameters.get("arguments")

        # CREATE TABLE ... CONSTRAINT - the relationships the statement declared,
        # validated by the logical planner and authorized by the binder.
        self.relationships = parameters.get("relationships")

        # CREATE / TRUNCATE / ALTER
        self.connector = parameters.get("connector")

    @property
    def name(self):
        return "Relation Management"

    @property
    def config(self):
        if self.action == "drop_relation":
            return f"drop {', '.join(self.relation_names or [])}"
        if self.action == "create_relation" and self.relationships:
            return (
                f"create {self.relation_name} with "
                f"{len(self.relationships)} declared relationship(s)"
            )
        if self.action == "create_collection":
            return f"create collection {self.collection_name}"
        if self.action == "drop_collection":
            return f"drop collection {', '.join(self.collection_names or [])}"
        if self.action == "cluster_by":
            return f"cluster {self.relation_name} by ({', '.join(self.cluster_columns or [])})"
        if self.action == "rename_relation":
            return f"rename {self.relation_name} to {self.new_relation_name}"
        if self.action == "add_column":
            return f"add column {self.column_name} to {self.relation_name}"
        if self.action == "drop_column":
            return f"drop column {self.column_name} from {self.relation_name}"
        if self.action == "rename_column":
            return f"rename column {self.column_name} to {self.new_column_name} on {self.relation_name}"
        if self.action == "alter_column_type":
            return f"alter column {self.column_name} on {self.relation_name} to {self.new_column_type}"
        if self.action == "add_relationship":
            return (
                f"add constraint {self.constraint_name} on {self.relation_name} "
                f"({self.column_name}) references {self.references_relation_name} "
                f"({self.references_column_name}) not enforced"
            )
        if self.action == "drop_relationship":
            return f"drop constraint {self.constraint_name} on {self.relation_name}"
        if self.action == "alter_workspace":
            return f"alter workspace {self.workspace_name} set {self.property_name} = {self.property_value}"
        if self.action == "alter_workspace_secure":
            if self.secure_destinations is None:
                return f"alter workspace {self.workspace_name} drop secure {self.secure_object}"
            return f"alter workspace {self.workspace_name} set secure {self.secure_object} to {', '.join(self.secure_destinations)}"
        if self.action == "drop_workspace":
            return f"drop workspace {self.workspace_name}"
        if self.action == "create_tag":
            return f"create tag {self.tag_name} on {self.relation_name} as of {self.version_spec}"
        if self.action == "drop_tag":
            return f"drop tag {self.tag_name} on {self.relation_name}"
        if self.action == "rollback_relation":
            return f"rollback {self.relation_name} to version {self.version_spec}"
        if self.action == "drop_trigger":
            return f"drop trigger {self.trigger_name} on {self.table_name}"
        if self.action == "create_task":
            on = f" on {self.on_table}" if self.on_table else ""
            return f"create task {self.task_name}{on}"
        if self.action == "create_trigger":
            return f"create trigger {self.trigger_name} on {_trigger_event(self)} execute {self.task_name}"
        if self.action == "alter_trigger_suspended":
            return f"alter trigger {self.trigger_name} on {self.table_name} {'suspend' if self.suspended else 'resume'}"
        if self.action == "alter_trigger_minimum_interval":
            return f"alter trigger {self.trigger_name} on {self.table_name} set minimum interval to {self.minimum_interval_seconds} seconds"
        if self.action == "drop_task":
            return f"drop task {self.task_name}"
        if self.action == "listen":
            return f"listen to {self.task_name} for {self.outcome.lower()}"
        if self.action == "unlisten":
            return f"unlisten {self.task_name}"
        if self.action == "alter_trigger_owner":
            return f"alter trigger {self.trigger_name} on {self.table_name} owner to {'CURRENT_USER' if self.owner_is_current_user else self.new_owner}"
        if self.action == "alter_materialized_view_suspended":
            return f"alter materialized view {self.relation_name} {'suspend' if self.suspended else 'resume'}"
        if self.action == "alter_materialized_view_owner":
            return f"alter materialized view {self.relation_name} owner to {'CURRENT_USER' if self.owner_is_current_user else self.new_owner}"
        if self.action == "grant_access":
            return f"grant {self.role} on {self.object_kind} {self.object_name} to user {self.principal}"
        if self.action == "revoke_access":
            return f"revoke {self.role} on {self.object_kind} {self.object_name} from user {self.principal}"
        if self.action == "call_procedure":
            # The ARGUMENT VALUES are not rendered. They are whatever the caller wrote
            # - a message body, a recipient - and this string reaches EXPLAIN output
            # and the query log, so the name is shown and the payload is not.
            return f"call {self.procedure_name} ({len(self.arguments or [])} argument(s))"
        return f"{self.action} {self.relation_name}"

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
        user = self.execution_context.user if self.execution_context else None
        if not user:
            raise PermissionError(
                "**LISTEN** and **UNLISTEN** need an authenticated user: a "
                "subscription belongs to a person, and this session has none."
            )
        return user

    def __call__(self, morsel=None, **kwargs) -> NonTabularResult:
        if self.action == "create_relation":
            if self.connector.relation_exists(self.relation_name):
                if self.if_not_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise ValueError(f"relation already exists: {self.relation_name}")
            self.connector.create_relation(self.relation_name, self.schema, author=self._author)
            # CREATE TABLE ... CONSTRAINT. Written after the relation exists,
            # because the store is a subcollection on the dataset document and
            # there is nothing to hang one on before that. The binder has
            # already authorized every far end and checked both columns, so
            # what is left here is the write.
            for relationship in self.relationships or []:
                self.connector.declare_relationship(
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
            for relation_name in self.relation_names:
                connector = self.connectors[relation_name]
                if not connector.relation_exists(relation_name):
                    if self.if_exists:
                        continue
                    raise DatasetNotFoundError(connector=connector, dataset=relation_name)
                # Type guard in both directions: a materialized view's backing
                # store is a dataset, so DROP TABLE would "work" on it - but
                # would strand its refresh triggers on every source table.
                target_is_mv = connector.is_materialized_view(relation_name)
                if self.is_materialized_view:
                    if not target_is_mv:
                        raise ValueError(
                            f"{relation_name} is not a materialized view; "
                            "use DROP TABLE or DROP VIEW"
                        )
                    connector.drop_materialized_view(
                        relation_name, if_exists=self.if_exists, author=self._author
                    )
                else:
                    if target_is_mv:
                        raise ValueError(
                            f"{relation_name} is a materialized view; "
                            "use DROP MATERIALIZED VIEW"
                        )
                    connector.drop_relation(
                        relation_name, if_exists=self.if_exists, author=self._author
                    )
                dropped += 1
            return NonTabularResult(record_count=dropped, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_collection":
            # No exists-check here, unlike create_relation: the connector settles
            # existence in a single atomic call, so there is no window between
            # checking and creating. The cost is that IF NOT EXISTS cannot report
            # 0-vs-1 for "already there" - the count is 1 for "the collection now
            # exists", not "a collection was created this instant".
            self.connector.create_collection(
                self.collection_name, if_not_exists=self.if_not_exists, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_collection":
            dropped = 0
            for collection_name in self.collection_names:
                connector = self.connectors[collection_name]
                if not connector.collection_exists(collection_name):
                    if self.if_exists:
                        continue
                    raise DatasetNotFoundError(connector=connector, dataset=collection_name)
                connector.drop_collection(
                    collection_name, if_exists=self.if_exists, author=self._author
                )
                dropped += 1
            return NonTabularResult(record_count=dropped, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "truncate_relation":
            if not self.connector.relation_exists(self.relation_name):
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            self.connector.truncate_relation(self.relation_name, author=self._author)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "cluster_by":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            # Declared on the Writable mixin, and visit_alter_relation has
            # already rejected a non-Writable connector.
            self.connector.set_cluster_by(
                self.relation_name, self.cluster_columns, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "add_relationship":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            # The workspace's own store, not the relation's catalog entry -
            # see Writable.declare_relationship. Both ends are in this
            # workspace; the logical planner refused the statement otherwise.
            self.connector.declare_relationship(
                relation_parts=self.relation_parts,
                column_name=self.column_name,
                references_relation_parts=self.references_relation_parts,
                references_column_name=self.references_column_name,
                constraint_name=self.constraint_name,
                cardinality=self.cardinality,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_relationship":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            removed = self.connector.drop_relationship(
                relation_parts=self.relation_parts,
                constraint_name=self.constraint_name,
                if_exists=self.constraint_if_exists,
                author=self._author,
            )
            return NonTabularResult(
                record_count=1 if removed else 0, status=QueryStatus.SQL_SUCCESS
            )

        elif self.action == "rename_relation":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            # A rename must never absorb an existing relation - that would
            # destroy the target's data and history with no DROP in the SQL.
            if self.connector.relation_exists(self.new_relation_name):
                raise ValueError(f"relation already exists: {self.new_relation_name}")
            self.connector.rename_relation(
                self.relation_name, self.new_relation_name, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "add_column":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            self.connector.add_column(
                self.relation_name,
                self.column_name,
                self.column_type,
                nullable=self.column_nullable,
                default=self.column_default,
                if_not_exists=self.column_if_not_exists,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_column":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            self.connector.drop_column(
                self.relation_name,
                self.column_name,
                if_exists=self.column_if_exists,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "rename_column":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            self.connector.rename_column(
                self.relation_name, self.column_name, self.new_column_name, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_column_type":
            if not self.connector.relation_exists(self.relation_name):
                if self.if_exists:
                    return NonTabularResult(record_count=0, status=QueryStatus.SQL_SUCCESS)
                raise DatasetNotFoundError(connector=self.connector, dataset=self.relation_name)
            self.connector.alter_column_type(
                self.relation_name, self.column_name, self.new_column_type, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_task":
            self.connector.create_task(
                self.task_name,
                self.statement,
                author=self._author,
                or_replace=self.or_replace,
                writes=self.target_tables,
            )
            if self.on_table:
                # Derived, not authored: the statement declared the dependency,
                # so the trigger that implements it is this statement's to make -
                # the same bargain CREATE MATERIALIZED VIEW strikes. `or_replace`
                # is passed so re-running the statement repoints its own trigger
                # rather than colliding with it.
                if not self.connector.relation_exists(self.on_table):
                    raise DatasetNotFoundError(connector=self.connector, dataset=self.on_table)
                self.connector.create_trigger(
                    self.on_table,
                    f"task__{self.task_name.replace('.', '__')}",
                    self.task_name,
                    author=self._author,
                    or_replace=True,
                )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_trigger":
            if self.event_kind == "commit":
                if not self.connector.relation_exists(self.table_name):
                    raise DatasetNotFoundError(connector=self.connector, dataset=self.table_name)
                # The call a commit trigger has always made, with no event
                # keywords: a connector (or a catalog behind one) that predates
                # the other two events keeps working unchanged.
                self.connector.create_trigger(
                    self.table_name,
                    self.trigger_name,
                    self.task_name,
                    author=self._author,
                    or_replace=self.or_replace,
                )
            else:
                # A clock or a signal has no source dataset: the holder is the
                # task, so it is the task whose existence is checked - a dataset
                # of that name would be the wrong kind of thing to hang it off.
                if not self.connector.is_task(self.table_name):
                    raise DatasetNotFoundError(connector=self.connector, dataset=self.table_name)
                self.connector.create_trigger(
                    self.table_name,
                    self.trigger_name,
                    self.task_name,
                    author=self._author,
                    or_replace=self.or_replace,
                    event_kind=self.event_kind,
                    schedule=self.schedule,
                    time_zone=self.time_zone,
                    window_source=self.window_source,
                )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_trigger_suspended":
            if not _trigger_holder_exists(self.connector, self.table_name):
                raise DatasetNotFoundError(connector=self.connector, dataset=self.table_name)
            self.connector.set_trigger_suspended(
                self.table_name,
                self.trigger_name,
                self.suspended,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_trigger_minimum_interval":
            # Pre-parse reduced the value to whole seconds; the store records
            # it and the catalog enforces it at fire time.
            if not self.connector.relation_exists(self.table_name):
                raise DatasetNotFoundError(connector=self.connector, dataset=self.table_name)
            self.connector.set_trigger_minimum_interval(
                self.table_name,
                self.trigger_name,
                self.minimum_interval_seconds,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_trigger_owner":
            # The binder resolved CURRENT_USER to the session identity and proved
            # that principal can be billed; this records the transfer.
            if not _trigger_holder_exists(self.connector, self.table_name):
                raise DatasetNotFoundError(connector=self.connector, dataset=self.table_name)
            self.connector.set_trigger_owner(
                self.table_name, self.trigger_name, self.resolved_owner, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_task":
            self.connector.drop_task(
                self.task_name,
                if_exists=self.if_exists,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "listen":
            # The subscriber is the session user and can be nobody else. Read
            # from the execution context rather than `_author`, which answers
            # "who is this write attributed to" - a different question that
            # happens to have the same answer, and one whose None means
            # "unattributed" rather than "nobody to subscribe".
            self.connector.add_listener(
                self.task_name,
                user=self._subscriber,
                outcome=self.outcome,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "unlisten":
            self.connector.drop_listener(self.task_name, user=self._subscriber)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_trigger":
            # The holder must exist regardless of IF EXISTS - that modifier
            # speaks about the trigger, not the table or task it hangs off.
            if not _trigger_holder_exists(self.connector, self.table_name):
                raise DatasetNotFoundError(connector=self.connector, dataset=self.table_name)
            self.connector.drop_trigger(
                self.table_name,
                self.trigger_name,
                author=self._author,
                missing_ok=self.if_exists,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "create_tag":
            self.connector.create_tag(
                self.relation_name,
                self.tag_name,
                self.version_spec,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_tag":
            self.connector.drop_tag(
                self.relation_name,
                self.tag_name,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "rollback_relation":
            self.connector.rollback_relation(
                self.relation_name,
                self.version_spec,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_materialized_view_owner":
            new_owner = self.new_owner
            if self.owner_is_current_user:
                # Resolved here rather than at plan time so it is the identity
                # that actually ran the statement, not one captured earlier.
                new_owner = self._author
                if not new_owner:
                    raise ValueError(
                        "OWNER TO CURRENT_USER needs an authenticated session; "
                        "this one has no user to assign the view to."
                    )
            self.connector.set_materialized_view_owner(
                self.relation_name, new_owner, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_materialized_view_suspended":
            self.connector.set_materialized_view_suspended(
                self.relation_name, self.suspended, author=self._author
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_workspace":
            self.connector.set_workspace_property(
                self.workspace_name,
                self.property_name,
                self.property_value,
                author=self._author,
            )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "alter_workspace_secure":
            # The SOURCE workspace's record. The binder required ownership of it,
            # and the connector writes only that workspace's own entry - so the
            # destination cannot sanction a copy into itself from here or anywhere.
            if self.secure_destinations is None:
                self.connector.clear_workspace_secure(
                    self.workspace_name, self.secure_object, author=self._author
                )
            else:
                self.connector.mark_workspace_secure(
                    self.workspace_name,
                    self.secure_object,
                    list(self.secure_destinations),
                    author=self._author,
                )
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "drop_workspace":
            self.connector.drop_workspace(self.workspace_name, author=self._author)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "grant_access":
            # Adds exactly ONE policy. Every rule — owner authority covering the
            # pattern, the no-self-service rule, conflict refusal, the audit
            # record — lives in the registered permissions capability; the engine
            # hands over and reports. There is no upgrade path: changing an
            # existing grant is REVOKE then GRANT, by the caller.
            from opteryx.managers.permissions import apply_grant

            apply_grant(self.execution_context, self.pattern, self.role, self.principal)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "revoke_access":
            # Deletes exactly ONE policy, resolved 1:1 by (principal, pattern,
            # role). Access held through a policy at a different level errors,
            # naming that policy — never narrowed, never a silent no-op.
            from opteryx.managers.permissions import apply_revoke

            apply_revoke(self.execution_context, self.pattern, self.role, self.principal)
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        elif self.action == "call_procedure":
            # Re-resolved by name rather than carried as a callable on the plan, so
            # nothing live is pinned into a plan that gets copied and explained.
            # `plan_call` already proved the name resolves and the arity matches; a
            # miss here means the registry changed underneath the statement, which is
            # an error and not a no-op.
            from opteryx.procedures import get_procedure

            procedure = get_procedure(self.procedure_name)
            if procedure is None:
                raise ValueError(f"procedure is no longer registered: {self.procedure_name}")

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
            procedure.handler(context, *(self.arguments or []))
            return NonTabularResult(record_count=1, status=QueryStatus.SQL_SUCCESS)

        else:
            raise NotImplementedError(f"Unsupported relation action: {self.action}")
