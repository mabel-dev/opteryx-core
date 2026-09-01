# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

from typing import Tuple

from opteryx.exceptions import ColumnNotFoundError
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.utils import suggest_alternative


def visit_create_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the CREATE TABLE node to determine which connector should handle
    storing the table.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support CREATE TABLE"
        )

    # Same tier as CTAS's fresh-create branch in visit_insert - creating a
    # brand-new relation requires writer or owner, checked here rather than
    # left to the connector (some connectors, e.g. the catalog, auto-vivify
    # the workspace/collection on first write with no gate of their own).
    if not can_perform_action(context.execution_context, node.relation_name, action="CREATE"):
        raise PermissionError(
            f"User does not have permission to create table {node.relation_name}"
        )

    # A CONSTRAINT declared in the CREATE carries the same two obligations
    # ALTER TABLE ... ADD CONSTRAINT carries, and for the same reasons - see
    # visit_add_relationship. The far end is gated at READ so nobody declares
    # relationships into data they have never seen, and both columns are checked
    # because this is the only validation point the store has.
    #
    # The near end differs in one way: the table does not exist yet, so its
    # columns are the ones this statement declares rather than the ones a
    # connector reports.
    declared_columns = {column.name for column in node.schema.columns}
    for relationship in node.relationships or []:
        if relationship["column_name"] not in declared_columns:
            raise ColumnNotFoundError(
                column=relationship["column_name"],
                dataset=node.relation_name,
                suggestion=suggest_alternative(relationship["column_name"], sorted(declared_columns)),
            )

        references_relation_name = relationship["references_relation_name"]
        if not can_perform_action(
            context.execution_context, references_relation_name, action="READ"
        ):
            raise PermissionError(
                f"User does not have permission to read {references_relation_name}, so it "
                f"cannot be referenced by a constraint on {node.relation_name}"
            )

        references_connector = connector_factory(
            references_relation_name, telemetry=context.telemetry
        )
        _require_column(
            references_connector, references_relation_name, relationship["references_column_name"]
        )

    node.columns = []
    return node, context


def visit_drop_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the DROP TABLE node to determine which connectors should handle
    removing the tables.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connectors = {}
    for relation_name in node.relation_names:
        connector = connector_factory(relation_name, telemetry=context.telemetry)
        if not isinstance(connector, Writable):
            raise ReadOnlyConnectorError(
                f"connector for {relation_name} does not support DROP TABLE"
            )

        # Ensure this user can drop the table - DROP is owner-only, a writer cannot
        if not can_perform_action(context.execution_context, relation_name, action="DROP"):
            raise PermissionError(f"User does not have permission to drop table {relation_name}")

        node.connectors[relation_name] = connector

    node.columns = []
    return node, context


def visit_create_collection(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """
    Bind the CREATE COLLECTION node to determine which connector should handle
    creating the collection.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.collection_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.collection_name} does not support CREATE COLLECTION"
        )

    # Creating a collection risks nothing existing, so this is the fresh-create
    # tier a writer holds - NOT the owner tier DROP COLLECTION requires, which
    # exists because dropping destroys. An owner policy covering the workspace
    # (e.g. "workspace.*") matches a 2-part collection name via fnmatch.
    if not can_perform_action(context.execution_context, node.collection_name, action="CREATE"):
        raise PermissionError(
            f"User does not have permission to create collection {node.collection_name}"
        )

    node.columns = []
    return node, context


def visit_drop_collection(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the DROP COLLECTION node to determine which connectors should handle
    removing the collections.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connectors = {}
    for collection_name in node.collection_names:
        connector = connector_factory(collection_name, telemetry=context.telemetry)
        if not isinstance(connector, Writable):
            raise ReadOnlyConnectorError(
                f"connector for {collection_name} does not support DROP COLLECTION"
            )

        # Same tier as DROP TABLE/VIEW - a writer's per-relation grant does not
        # extend to removing the collection itself, only an owner may. An
        # owner policy pattern covering the whole workspace (e.g. "workspace.*")
        # already matches a 2-part collection name via fnmatch, so no new
        # permission mechanism is needed for "owner of the workspace."
        if not can_perform_action(context.execution_context, collection_name, action="DROP"):
            raise PermissionError(
                f"User does not have permission to drop collection {collection_name}"
            )

        node.connectors[collection_name] = connector

    node.columns = []
    return node, context


def visit_alter_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER TABLE ... CLUSTER BY node to determine which connector
    should handle persisting the new sort order.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    # Same tier as DROP - ALTER changes the relation's physical layout, not
    # just its contents, so a writer cannot do it, only an owner can.
    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    _reject_materialized_view_target(node, "**ALTER TABLE ... CLUSTER BY**")

    node.columns = []
    return node, context


def visit_rename_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER TABLE ... RENAME TO node to determine which connector should
    handle moving the relation.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    # A rename destroys the source name and creates the target one, so it is
    # gated at both ends: owner on the source (same tier as DROP - the old
    # relation stops existing under that name) and the fresh-create tier on the
    # target, so a writer cannot move a relation into a collection they have no
    # grant on. The workspace is guaranteed unchanged by the logical planner, so
    # both names resolve through the same connector.
    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to rename table {node.relation_name}"
        )
    if not can_perform_action(context.execution_context, node.new_relation_name, action="CREATE"):
        raise PermissionError(
            f"User does not have permission to rename table to {node.new_relation_name}"
        )

    _reject_materialized_view_target(node, "**ALTER TABLE ... RENAME TO**")

    node.columns = []
    return node, context


def visit_add_column(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER TABLE ... ADD COLUMN node to determine which connector
    should handle adding the column.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    # Same tier as CLUSTER BY/RENAME TO - a column ADD changes what the relation
    # fundamentally is, not just its contents, so a writer cannot do it, only an
    # owner can.
    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    _reject_materialized_view_target(node, "**ALTER TABLE ... ADD COLUMN**")

    node.columns = []
    return node, context


def visit_drop_column(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER TABLE ... DROP COLUMN node to determine which connector
    should handle removing the column.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    _reject_materialized_view_target(node, "**ALTER TABLE ... DROP COLUMN**")
    _guard_relationships_through_dropped_column(node, context)

    node.columns = []
    return node, context


def visit_rename_column(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER TABLE ... RENAME COLUMN node to determine which connector
    should handle renaming the column.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    _reject_materialized_view_target(node, "**ALTER TABLE ... RENAME COLUMN**")

    node.columns = []
    return node, context


def visit_add_relationship(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind `ALTER TABLE ... ADD CONSTRAINT ... FOREIGN KEY ... NOT ENFORCED`.

    This is the first statement in the engine that names two datasets, so it is
    the first that has to authorize two, and the two ends are not the same
    question.

    The near (altered) dataset is gated at ALTER - owner - which is the tier
    every statement that changes a dataset's metadata already uses, CLUSTER BY
    among them. Nothing new.

    The far (referenced) dataset is gated at READ, and that check has no
    precedent here: no other DDL statement reaches a second dataset at all. It
    is a correctness control, not a confidentiality one - you had to know the far
    dataset's name to type the statement, so refusing it conceals nothing. What
    it stops is somebody declaring relationships into data they have never seen,
    which produces confident garbage and puts the far dataset's name in front of
    everyone who can read the near one. Grants are per-dataset, so owning the
    near table says nothing about being able to read the far one even though the
    logical planner has already confined both to the same workspace.

    Both columns are checked to exist. A declaration naming a column that is not
    there is not a relationship, and this statement is the only validation point
    the store has - the store is not writable any other way.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    if not can_perform_action(
        context.execution_context, node.references_relation_name, action="READ"
    ):
        raise PermissionError(
            f"User does not have permission to read {node.references_relation_name}, so it "
            f"cannot be referenced by a constraint on {node.relation_name}"
        )

    _reject_materialized_view_target(node, "**ALTER TABLE ... ADD CONSTRAINT**")

    # IF EXISTS makes a missing TABLE a no-op, and a relation that is not there
    # has no columns to check. Without this the column check below would report
    # the very absence the statement asked to tolerate, and it would report it
    # as the connector's "relation does not exist", not as anything a reader
    # could act on. The far end is not checked either - there is no declaration
    # to validate when nothing is being declared - but both permission gates
    # above have already been answered, since what the caller may do does not
    # depend on what happens to exist.
    if node.if_exists and not node.connector.relation_exists(node.relation_name):
        node.columns = []
        return node, context

    _require_column(node.connector, node.relation_name, node.column_name)

    # Read-only is enough for the far end: nothing is written there.
    references_connector = connector_factory(
        node.references_relation_name, telemetry=context.telemetry
    )
    _require_column(
        references_connector, node.references_relation_name, node.references_column_name
    )

    node.columns = []
    return node, context


def visit_drop_relationship(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """Bind `ALTER TABLE ... DROP CONSTRAINT`.

    One end only: the constraint is named, not the dataset it referenced, and
    removing a declaration discloses nothing about the far side. Owner on the
    near dataset, the same tier that added it.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    _reject_materialized_view_target(node, "**ALTER TABLE ... DROP CONSTRAINT**")

    node.columns = []
    return node, context


def _require_column(connector, relation_name: str, column_name: str) -> None:
    """Refuse a relationship end that names a column the dataset does not have."""
    column_types = connector.relation_column_types(relation_name)
    if column_name not in column_types:
        raise ColumnNotFoundError(
            column=column_name,
            dataset=relation_name,
            suggestion=suggest_alternative(column_name, list(column_types)),
        )


def visit_alter_column_type(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER TABLE ... ALTER COLUMN ... TYPE node: resolve the connector,
    and reject an illegal type change before anything is written.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.managers.permissions import can_perform_action
    from opteryx.types import is_legal_widen

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER TABLE"
        )

    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    _reject_materialized_view_target(node, "**ALTER TABLE ... ALTER COLUMN ... TYPE**")

    current_types = node.connector.relation_column_types(node.relation_name)
    if node.column_name not in current_types:
        raise ColumnNotFoundError(
            column=node.column_name,
            dataset=node.relation_name,
            suggestion=suggest_alternative(node.column_name, list(current_types)),
        )
    node.current_column_type = current_types[node.column_name]

    if not is_legal_widen(node.current_column_type, node.new_column_type):
        raise UnsupportedSyntaxError(
            f"**ALTER TABLE ... ALTER COLUMN ... TYPE** cannot change '{node.column_name}' "
            f"from {node.current_column_type} to {node.new_column_type} - only a lossless "
            "widening within the same type family is supported (e.g. INT32 to INT64)."
        )

    node.columns = []
    return node, context


def visit_analyze(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ANALYZE TABLE / DROP STATISTICS node.

    Both statements share the Analyze node, dispatching on `action`, so they
    share this binder and its permission tier.

    Gated at ALTER (owner), the same tier as ALTER TABLE ... CLUSTER BY: neither
    changes the relation's rows, both rewrite the metadata the optimizer plans
    from, and DROP STATISTICS destroys it outright. Until this existed neither
    statement was authorized at all - the Analyze node had no visitor, and a
    node type with no visitor is silently passed through by
    BinderVisitor.visit_node.
    """
    from opteryx.connectors import connector_factory
    from opteryx.managers.permissions import can_perform_action

    verb = "analyze" if node.action == "analyze_table" else "drop statistics on"

    if not can_perform_action(context.execution_context, node.table_name, action="ALTER"):
        raise PermissionError(f"User does not have permission to {verb} table {node.table_name}")

    node.connector = connector_factory(node.table_name, telemetry=context.telemetry)

    node.columns = []
    return node, context


def visit_alter_materialized_view_owner(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """Bind ALTER MATERIALIZED VIEW ... OWNER TO ...

    Two identities decide a materialized view, and this statement moves one of
    them without touching the other, so it owes both an answer.

    Definition-time: whoever authors a view must be able to run the query it is
    defined by, or authoring becomes a way to read what you could not read
    directly. `visit_insert` enforces that against the caller on every
    registration.

    Execution-time: a TRIGGERED refresh runs as the pinned `runs-as` principal,
    judged on that principal's own grants and inheriting nothing from its
    author - a refresh that borrowed its author's authority would be a deputy
    acting beyond its own. That identity belongs to the trigger and is not
    consulted here: a `REFRESH MATERIALIZED VIEW` statement may be submitted by
    anyone and is assessed on the submitting identity's own merits, so nothing
    in this engine reads `runs-as`. It only writes it, and this is what writes
    it.

    So this statement asks three things. The incoming owner must be an identity
    this deployment is willing to pin work on at all - a platform identity is
    not an account and is billed to nobody, so a view pinned to one refreshes
    for free forever, which no reading check would ever catch. AND the caller
    must hold workspace owner,
    deliberately stricter than the view itself: a workspace owner can already
    grant themselves anything in the workspace and so escalates nothing by
    transferring, where a mere relation owner could borrow authority they do
    not have. AND the incoming owner must independently be able to read every
    source - without which a transfer is a way to aim somebody else's authority
    at data, or to leave a view whose triggered refresh can only ever fail.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import InvalidInternalStateError
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action
    from opteryx.managers.permissions import can_perform_workspace_action
    from opteryx.managers.permissions import can_principal_own_materialized_view
    from opteryx.managers.permissions import can_principal_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER MATERIALIZED VIEW"
        )

    workspace = node.relation_name.split(".", 1)[0]
    if not can_perform_workspace_action(context.execution_context, workspace, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to change the owner of {node.relation_name} "
            f"(owner of workspace {workspace} required)"
        )

    # CURRENT_USER names the executing session, which the session-scoped gate
    # answers exactly - and the principal it resolves to is not known here, only
    # when the statement executes.
    owner = node.new_owner
    if node.owner_is_current_user:
        owner = context.execution_context.user or "CURRENT_USER"

    # Asked of the RESOLVED owner, so `CURRENT_USER` is judged as the principal
    # it names rather than exempted for having been spelled differently. A rule
    # that turns on how an identity was written is not a rule.
    #
    # This is not a reading question and the source loop below cannot stand in
    # for it: the identities refused here can typically read a great deal. What
    # they cannot do is pay. They are the platform's own automation - identities
    # rather than accounts, with no billing account behind them - so a view
    # pinned to one refreshes on a schedule forever and lands on nobody's bill.
    # Users and service accounts are both costed (a service account cannot exist
    # without a billing account seat), which is why they are not refused here.
    if not can_principal_own_materialized_view(owner):
        raise PermissionError(
            f"{owner} cannot be made the owner of {node.relation_name}. It is a platform "
            "identity rather than an account, so work it performs is billed to nobody - "
            "and a materialized view's refreshes run as its owner. Transfer the view to a "
            "user or to a service account, both of which carry a billing account."
        )

    sources = node.connector.materialized_view_sources(node.relation_name)
    if not sources:
        # `visit_insert` refuses to register a view with no catalog sources, so a
        # registered view that reports none is a record that cannot be true - not
        # a view that genuinely reads nothing. Deliberately NOT PermissionError:
        # nothing was denied here, the check could not be run at all, and a gate
        # that answers "allowed" because it found nothing to look at is exactly
        # the hole this whole check exists to close.
        raise InvalidInternalStateError(
            f"materialized view {node.relation_name} has no source tables recorded, so "
            f"there is nothing to establish that {owner} can read what it reads. Its "
            "registration record is incomplete; re-register it with CREATE OR REPLACE "
            "MATERIALIZED VIEW before transferring it."
        )

    for source in sources:
        if node.owner_is_current_user:
            permitted = can_perform_action(context.execution_context, source, action="READ")
        else:
            permitted = can_principal_perform_action(node.new_owner, source, action="READ")
        if not permitted:
            raise PermissionError(
                f"{owner} does not have permission to read {source}, a source of "
                f"{node.relation_name} (read required). A triggered refresh runs as the "
                "view's owner and inherits nothing from whoever transferred it, so an "
                "owner that cannot read a source is a view that can never refresh itself."
            )

    node.columns = []
    return node, context


def visit_alter_materialized_view_suspended(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """Bind ALTER MATERIALIZED VIEW ... SUSPEND | RESUME.

    WRITE on the view, not the workspace-owner tier ALTER ... OWNER TO needs.
    Suspending borrows nobody's authority - it only stops the view refreshing, and
    anyone who may replace its contents may certainly stop them being replaced
    automatically.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support ALTER MATERIALIZED VIEW"
        )

    if not can_perform_action(context.execution_context, node.relation_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to suspend or resume {node.relation_name} "
            "(write required)"
        )

    node.columns = []
    return node, context


def visit_create_task(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind CREATE TASK.

    A task is stored SQL and nothing more - it carries NO identity. A person
    running it with EXECUTE runs it as themselves; an unattended run carries the
    TRIGGER's pinned owner. Both are gated again at execution, against whoever
    the run actually is, and nothing here replaces that.

    What this adds is an AUTHORING bound: a task may only do what its author
    could do at the moment they wrote it. Execution-time checks alone are not
    enough, because the principal a task runs as need not be the one who last
    edited it. A trigger pins its owner on creation and keeps it across edits,
    so with only a WRITE-on-the-name gate, anyone holding write on that name
    could rewrite the statement and have it fire under the trigger owner's
    authority - the editor supplying the instructions and a higher-privileged
    principal supplying the permissions. That is a confused deputy, and it is
    created by the EDIT, which is why the check belongs here.

    So the author must independently be able to READ every source and WRITE the
    target, checked on every registration rather than only the first - a
    `CREATE OR REPLACE` that repoints a task is exactly the case that matters.

    Deliberately NOT a durable guarantee: the author's grants may be revoked
    later while the task remains, and the task will still run as whoever the
    trigger names. That is accepted - this bounds what can be AUTHORED, not what
    remains true forever. Execution-time checks are what cover the rest.

    Also gated: the write this statement performs, registering an object under
    this name. `ON <table>` additionally lands a trigger, checked below at the
    same tier CREATE TRIGGER is.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action
    from opteryx.managers.permissions import can_principal_own_materialized_view

    node.connector = connector_factory(node.task_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.task_name} does not support CREATE TASK"
        )

    if not can_perform_action(context.execution_context, node.task_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to create task {node.task_name}"
        )

    # THE AUTHORING BOUND. `plan_create_task` reads these off the task's AST for
    # exactly this check. Sources need only READ and a target needs WRITE -
    # folding them together would demand read access on a table a write-only
    # grant covers, the same split `visit_insert` makes for a materialized view.
    #
    # `target_tables` is a LIST because TRUNCATE names several; every other write
    # form names one. It covers every statement that changes a relation's
    # contents - INSERT, CTAS, UPDATE, DELETE, MERGE, TRUNCATE - which it did not
    # always: while the derivation read only INSERT's target, the target of an
    # UPDATE or a MERGE fell through into `source_tables` and was checked at READ,
    # leaving this bound armed for one statement form out of six.
    #
    # Virtual and information_schema relations are skipped: they are not catalog
    # objects, carry no grants to check, and are governed by their own visibility
    # rules at execution. Skipped explicitly rather than silently - a task that
    # reads only these is bounded by nothing here, which is correct, because
    # there is no authority to borrow.
    for source in node.source_tables or []:
        if source.startswith("$") or "information_schema" in source.split("."):
            continue
        if not can_perform_action(context.execution_context, source, action="READ"):
            raise PermissionError(
                f"User does not have permission to read {source}, a source of task "
                f"{node.task_name} (read required). A task may only do what its "
                "author could do: an unattended run carries the trigger's owner, so "
                "authoring one that reads what you cannot would borrow their authority."
            )

    for target in node.target_tables or []:
        if not can_perform_action(context.execution_context, target, action="WRITE"):
            raise PermissionError(
                f"User does not have permission to write {target}, a target "
                f"of task {node.task_name} (write required). A task may only do what its "
                "author could do."
            )

    # `ON <table>` lands a trigger on that dataset, whose unattended runs will
    # carry THIS author's identity - so it takes both of CREATE TRIGGER's gates:
    # WRITE on the table, and an author who can be billed. One statement must
    # not do by implication what the explicit statement would refuse.
    if getattr(node, "on_table", None):
        if not can_perform_action(context.execution_context, node.on_table, action="WRITE"):
            raise PermissionError(
                f"User does not have permission to create a trigger on table "
                f"{node.on_table}, which would fire {node.task_name}"
            )
        author = context.execution_context.user
        if not can_principal_own_materialized_view(author):
            raise PermissionError(
                f"{author} cannot own the trigger this statement creates. It is a "
                "platform identity rather than an account, so work it performs is "
                "billed to nobody - and a trigger runs its task as its owner."
            )

    node.columns = []
    return node, context


def _trigger_work_relations(visitor, connector, trigger: dict, context) -> Tuple[list, list]:
    """`(sources, targets)` for the work a trigger fires.

    A trigger points at one of two things and the catalog records which: a TASK
    (`target-task`), whose stored statement is re-read and its relations taken
    off the AST exactly as `plan_create_task` takes them; or a materialized VIEW
    (`target-view`), whose sources the catalog already records and whose target
    is the view itself.

    The task's statement is parsed here rather than planned, for the reason
    `plan_create_task` gives: it may carry `:name` placeholders that only bind at
    EXECUTE, and planning would refuse them.
    """
    from opteryx.exceptions import InvalidInternalStateError
    from opteryx.third_party import sqloxide
    from opteryx.utils.query_parser import _extract_tables_from_ast
    from opteryx.utils.query_parser import extract_write_targets

    target_task = trigger.get("target-task")
    if target_task:
        task_sql = connector.task_definition(target_task)
        parsed = sqloxide.parse_sql(task_sql, _dialect="opteryx")
        inner = parsed[0]
        # The SAME derivation `plan_create_task` records as the task's `writes`,
        # called rather than restated - two spellings of "what does this write"
        # would drift, and the one that drifts low is a gate that stops checking.
        # Read from the statement and not from the recorded `writes`, because a
        # task registered before that field existed has none.
        targets = extract_write_targets(inner)
        sources = [
            relation
            for relation in _extract_tables_from_ast(inner)
            if relation not in targets
            and not relation.startswith("$")
            and "information_schema" not in relation.split(".")
        ]
        return sources, targets

    target_view = trigger.get("target-view")
    if target_view:
        return list(connector.materialized_view_sources(target_view) or []), [target_view]

    # A trigger record naming neither is one whose work cannot be established, so
    # there is nothing to check an incoming owner against. Deliberately NOT
    # treated as "allowed": a gate that passes because it found nothing to look
    # at is the hole the check exists to close.
    raise InvalidInternalStateError(
        "trigger record names neither a target task nor a target view, so there is "
        "nothing to establish what its owner would be running."
    )


def visit_alter_trigger_owner(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """Bind ALTER TRIGGER ... OWNER TO.

    The owner is the identity an UNATTENDED run carries. A person running
    `EXECUTE` runs the task as themselves and answers for it; a trigger fires
    with nobody present, so it must name whose authority it uses - and one task
    fired by two triggers can legitimately run as two different principals.

    Three gates, matching the materialized view's:

    - WRITE on the TABLE the trigger hangs off, matching creation: landing or
      changing a trigger is an update to that table.
    - The incoming owner must be able to PAY. Platform identities are refused,
      which is a billing question rather than a permissions one - they can read
      a great deal but carry no billing account, so work pinned to one runs on a
      schedule forever and lands on nobody's bill.
    - The incoming owner must independently be able to do what the task DOES -
      READ its sources and WRITE its target. Without this, a caller holding only
      write on the table can aim a task at a higher-privileged principal and have
      it run with that principal's authority, which is the escalation `CREATE
      TASK`'s authoring bound and `CREATE TRIGGER`'s pin-to-author close on the
      other two paths. It also refuses a transfer that would leave a trigger only
      able to fail, the same reason the view's owner-change checks its sources.

    Asked of the RESOLVED owner, so CURRENT_USER is judged as the principal it
    names rather than exempted for having been spelled differently.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action
    from opteryx.managers.permissions import can_principal_own_materialized_view
    from opteryx.managers.permissions import can_principal_perform_action

    node.connector = connector_factory(node.table_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.table_name} does not support ALTER TRIGGER"
        )

    if not can_perform_action(context.execution_context, node.table_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to alter a trigger on table {node.table_name}"
        )

    owner = context.execution_context.user if node.owner_is_current_user else node.new_owner
    if not can_principal_own_materialized_view(owner):
        raise PermissionError(
            f"{owner} cannot be made the owner of trigger {node.trigger_name}. It is a "
            "platform identity rather than an account, so work it performs is billed to "
            "nobody - and a trigger runs its task as its owner. Transfer it to a user or "
            "to a service account, both of which carry a billing account."
        )

    # What the trigger actually fires, and what that work touches. Read off the
    # trigger's own record rather than taken from the statement: the caller names
    # a trigger, not a task, and the binding between them is the catalog's.
    trigger = next(
        (t for t in node.connector.list_triggers(node.table_name) if t.get("name") == node.trigger_name),
        None,
    )
    if trigger is None:
        raise ColumnNotFoundError(
            f"trigger {node.trigger_name} was not found on {node.table_name}, so there is "
            f"nothing to establish that {owner} can perform the work it fires."
        )

    sources, targets = _trigger_work_relations(self, node.connector, trigger, context)

    # Asked of the incoming owner's OWN grants, inheriting nothing from the caller
    # - a deputy that borrowed the transferrer's authority would be the very thing
    # this gate exists to stop.
    for source in sources:
        if node.owner_is_current_user:
            permitted = can_perform_action(context.execution_context, source, action="READ")
        else:
            permitted = can_principal_perform_action(owner, source, action="READ")
        if not permitted:
            raise PermissionError(
                f"{owner} does not have permission to read {source}, which trigger "
                f"{node.trigger_name} reads when it fires (read required). An unattended "
                "run carries the trigger's owner and inherits nothing from whoever "
                "transferred it."
            )

    for target in targets:
        if node.owner_is_current_user:
            permitted = can_perform_action(context.execution_context, target, action="WRITE")
        else:
            permitted = can_principal_perform_action(owner, target, action="WRITE")
        if not permitted:
            raise PermissionError(
                f"{owner} does not have permission to write {target}, which trigger "
                f"{node.trigger_name} writes when it fires (write required)."
            )

    node.resolved_owner = owner

    node.columns = []
    return node, context


def visit_drop_task(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind DROP TASK.

    Gated at WRITE on the task itself, symmetric with creation. A task owns no
    storage, so this is not the DROP tier a table's would be - there is nothing
    to reclaim and nothing unrecoverable about it, since the statement can be
    registered again.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.task_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.task_name} does not support DROP TASK"
        )

    if not can_perform_action(context.execution_context, node.task_name, action="WRITE"):
        raise PermissionError(f"User does not have permission to drop task {node.task_name}")

    node.columns = []
    return node, context


def visit_create_trigger(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind CREATE TRIGGER.

    Two gates. WRITE on the TABLE the trigger hangs off - symmetric with
    `visit_drop_trigger`, landing a trigger is an update to that dataset. And
    the author must be a principal who can be BILLED, because the trigger's
    unattended runs execute as its owner and the owner pins to the author here.
    This is where authority is actually conferred - a task is just stored SQL,
    and a person running EXECUTE answers for themselves - so this is where the
    gate lives.

    What the task may do is NOT re-checked: its statement is gated by the binder
    at execution, against the owner, every time it fires. A creation-time copy
    of that check would go stale the moment the owner's grants changed.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action
    from opteryx.managers.permissions import can_principal_own_materialized_view

    node.connector = connector_factory(node.table_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.table_name} does not support CREATE TRIGGER"
        )

    if not can_perform_action(context.execution_context, node.table_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to create a trigger on table {node.table_name}"
        )

    author = context.execution_context.user
    if not can_principal_own_materialized_view(author):
        raise PermissionError(
            f"{author} cannot own trigger {node.trigger_name}. It is a platform "
            "identity rather than an account, so work it performs is billed to "
            "nobody - and a trigger runs its task as its owner."
        )

    node.columns = []
    return node, context


def visit_alter_trigger_suspended(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """Bind ALTER TRIGGER ... SUSPEND|RESUME. Same tier as creating one."""
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.table_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.table_name} does not support ALTER TRIGGER"
        )

    if not can_perform_action(context.execution_context, node.table_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to alter a trigger on table {node.table_name}"
        )

    node.columns = []
    return node, context


def visit_drop_trigger(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the DROP TRIGGER node to determine which connector should handle
    removing the trigger.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.table_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.table_name} does not support DROP TRIGGER"
        )

    # WRITE on the table the trigger hangs off — symmetric with creation:
    # landing a refresh trigger on a source table is gated at WRITE by
    # visit_insert's MV branch, so removing one is an update to that same
    # table, not to the trigger's target view.
    if not can_perform_action(context.execution_context, node.table_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to drop a trigger on table {node.table_name}"
        )

    node.columns = []
    return node, context


def _bind_snapshot_ddl(self, node: Node, context: BindingContext, statement: str):
    """Shared binding for CREATE TAG, DROP TAG and ROLLBACK TO VERSION.

    All three are ALTER TABLE statements about one relation, and all three are
    gated at the same tier as every other ALTER. A tag pins its snapshot's
    storage indefinitely and the pinned bytes are charged, so creating one
    commits the relation's owner to an open-ended cost, and dropping one is how
    data stops being kept. A rollback replaces what every reader of the relation
    sees. None of these is a writer's call.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support {statement}"
        )
    if not node.connector.supports_version_travel:
        # These statements all name snapshots. A store with no snapshots has
        # nothing to tag and nothing to roll back to, and saying so here beats a
        # connector failing later on a method it has no business having.
        raise UnsupportedSyntaxError(
            f"{statement} is not supported for {node.relation_name} - it requires a "
            "connector with snapshot-based time travel."
        )

    if not can_perform_action(context.execution_context, node.relation_name, action="ALTER"):
        raise PermissionError(
            f"User does not have permission to alter table {node.relation_name}"
        )

    node.columns = []
    return node, context


def visit_create_tag(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind ALTER TABLE ... CREATE TAG."""
    return _bind_snapshot_ddl(self, node, context, "**ALTER TABLE ... CREATE TAG**")


def visit_drop_tag(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind ALTER TABLE ... DROP TAG."""
    return _bind_snapshot_ddl(self, node, context, "**ALTER TABLE ... DROP TAG**")


def visit_rollback_relation(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """Bind ALTER TABLE ... ROLLBACK TO VERSION."""
    return _bind_snapshot_ddl(self, node, context, "**ALTER TABLE ... ROLLBACK TO VERSION**")


def visit_alter_workspace(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the ALTER WORKSPACE ... SET node to determine which connector should
    handle persisting the workspace property.
    """
    from opteryx.connectors import workspace_settings_connector
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_workspace_action

    # The SETTINGS connector, not the data one: these properties live in the
    # opteryx catalog entry whatever the workspace's data is bound to.
    node.connector = workspace_settings_connector(
        node.workspace_name, telemetry=context.telemetry
    )
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.workspace_name} does not support ALTER WORKSPACE"
        )

    # Owner of the whole workspace - a grant covering only part of it is not
    # enough, since these properties govern the workspace entire (see
    # can_perform_workspace_action for why this is not can_perform_action).
    if not can_perform_workspace_action(
        context.execution_context, node.workspace_name, action="ALTER"
    ):
        raise PermissionError(
            f"User does not have permission to alter workspace {node.workspace_name}"
        )

    node.columns = []
    return node, context


def visit_drop_workspace(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the DROP WORKSPACE node to determine which connector should handle
    the drop, same shape as visit_alter_workspace.
    """
    from opteryx.connectors import workspace_settings_connector
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_workspace_action

    # Settings connector, same reasoning as ALTER WORKSPACE. Dropping an
    # externally-bound workspace unlinks it - the catalog decides that, from
    # the binding it can see on the workspace's own `$properties`.
    node.connector = workspace_settings_connector(
        node.workspace_name, telemetry=context.telemetry
    )
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.workspace_name} does not support DROP WORKSPACE"
        )

    # Owner of the whole workspace, same tier as ALTER WORKSPACE - opteryx_access's
    # ACTION_ROLES already requires "owner" for DROP, same as it does for a
    # relation-level DROP TABLE/VIEW; this just applies it at workspace scope.
    if not can_perform_workspace_action(
        context.execution_context, node.workspace_name, action="DROP"
    ):
        raise PermissionError(
            f"User does not have permission to drop workspace {node.workspace_name}"
        )

    node.columns = []
    return node, context


def visit_truncate_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the TRUNCATE TABLE node to determine which connector should handle
    truncating the table.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support TRUNCATE TABLE"
        )

    # TRUNCATE is a bulk row delete - same tier as DELETE.
    if not can_perform_action(context.execution_context, node.relation_name, action="DELETE"):
        raise PermissionError(
            f"User does not have permission to truncate table {node.relation_name}"
        )

    _reject_materialized_view_target(node, "**TRUNCATE TABLE**")

    node.columns = []
    return node, context


def visit_optimize_relation(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the OPTIMIZE node to determine which connector should handle
    compacting the relation's data files.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.managers.permissions import can_perform_action

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support OPTIMIZE"
        )

    # OPTIMIZE rewrites files losslessly and declares no new structure - same
    # trust tier as INSERT/UPDATE, not the owner-only tier ALTER TABLE uses.
    if not can_perform_action(context.execution_context, node.relation_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to optimize table {node.relation_name}"
        )

    # NOT refused for a materialized view, unlike every other table modifier.
    # OPTIMIZE compacts the files a relation's rows already live in; it is
    # lossless and declares no new structure, so the view still holds exactly
    # what its SELECT produced. It is physical maintenance, not a write - the
    # thing `_reject_materialized_view_target` exists to stop is a statement
    # whose effect the next REFRESH would discard, and compaction has no such
    # effect to discard.

    node.columns = []
    return node, context


def _types_compatible(src, tgt) -> bool:
    """Permitted source→target type relationships for INSERT.

    Strict: exact match, NULL into anything, or INTEGER → DOUBLE widening.
    Unresolved literal types (None) are permitted at bind time —
    runtime catches real mismatches.
    """
    from opteryx.types.logical_type import LogicalCategory, ColumnType

    # Normalize ColumnType → LogicalCategory for comparison.
    src_lc = src.category if isinstance(src, ColumnType) else src
    tgt_lc = tgt.category if isinstance(tgt, ColumnType) else tgt

    if src_lc == tgt_lc:
        return True
    if src_lc == LogicalCategory.NULL:
        return True
    if src_lc is None:
        return True
    if src_lc == LogicalCategory.INTEGER and tgt_lc == LogicalCategory.FLOAT:
        return True
    return False


def _guard_relationships_through_dropped_column(node, context) -> None:
    """Refuse or warn when `DROP COLUMN` would leave a relationship pointing at
    nothing.

    WARN AT PLAN TIME, because telling someone before the damage beats
    recording it after. Until this existed a dropped column silently orphaned
    every relationship through it: the rows stayed `active`, the projection
    kept emitting them, and a BI client kept being handed a NavigationProperty
    onto a column that no longer existed.

    Two outcomes, and the split is by `origin`:

      ASSERTED  -> REFUSED. A person declared this, and the column it runs
                   through is the entire content of the declaration. Dropping
                   the column does not falsify the claim, it makes it
                   unstatable - so the right answer is not to record a broken
                   row, it is to make the person retract the claim first. This
                   is `RESTRICT` semantics, and it is the same shape as
                   `drop_dataset` refusing while a materialized view reads the
                   dataset. It does NOT contradict "nothing is enforced, ever"
                   (§6.1): that rule is about DML, a write whose VALUES break a
                   relationship, which still succeeds. This is DDL removing the
                   object a declaration names.

      INFERRED  -> WARNED. A proposal is a machine's guess that nobody has
                   answered. Blocking a schema change on one would let the
                   inference job veto DDL, which is a much worse failure than
                   losing a proposal - and the proposal is not lost, it is
                   marked broken after the drop.

    INBOUND REFERENCES NEITHER REFUSE NOR APPEAR IN THE MESSAGE, and that is a
    visibility rule, not an oversight. A relationship declared on another
    dataset that points at this column belongs to someone who may hold a grant
    this caller does not - naming it here would disclose the existence, name
    and shape of data they cannot read, which is exactly what §8.2 constructs
    the projection to prevent. Refusing on one would disclose it just as
    surely, by making the drop fail for a reason the caller cannot see, and it
    would also let any workspace member block another team's schema change by
    declaring a constraint at it. So inbound references are broken after the
    fact and their owners are told through their own catalog.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    lookup = getattr(node.connector, "relationships_through_column", None)
    if lookup is None:
        return

    try:
        through = lookup(node.relation_name, node.column_name)
    except Exception as err:  # noqa: BLE001
        # The check is advisory and the drop is the user's statement, so a
        # store that cannot answer must not block it. It must not pass
        # silently either - a guard that fails open without saying so is worse
        # than no guard, because the absence of a warning starts to mean
        # something it does not.
        context.telemetry.add_message(
            f"could not check which relationships run through {node.relation_name}."
            f"{node.column_name} ({type(err).__name__}); they may be left broken"
        )
        return

    outbound = [row for row in through if not row.get("inbound")]
    asserted = [row for row in outbound if row.get("origin") == "asserted"]
    if asserted:
        names = ", ".join(sorted(str(row.get("constraint_name")) for row in asserted))
        raise UnsupportedSyntaxError(
            f"{node.relation_name}.{node.column_name} is referenced by declared "
            f"relationship(s) {names}, which would be left pointing at nothing. "
            f"Remove them first with **ALTER TABLE {node.relation_name} DROP "
            "CONSTRAINT <name>**, then drop the column."
        )

    proposals = [row for row in outbound if row.get("origin") != "asserted"]
    if proposals:
        context.telemetry.add_message(
            f"dropping {node.relation_name}.{node.column_name} breaks "
            f"{len(proposals)} suggested relationship(s); they will be marked "
            "broken rather than removed"
        )


def _reject_materialized_view_target(node, statement: str) -> None:
    """Refuse a table modifier aimed at a materialized view.

    A materialized view is not a table. Its contents are not something anyone
    writes - they are derived, by definition, from its SELECT. A statement that
    wrote to one directly would either be silently discarded by the next
    refresh or, worse, survive and leave the view disagreeing with its own
    definition, which is the state the whole construct exists to prevent.

    So every table modifier is refused here and the error names the statement
    that does mean something: change the definition, refresh it, or drop it.
    `DROP TABLE` is refused in the same spirit, one layer down in
    relation_management, where the drop path already had the type guard.

    Exactly four statements may land on a view, and each is allowed for its own
    reason rather than by omission:

      CREATE MATERIALIZED VIEW   it defines the view (carries
                                 `is_materialized_view`, checked by its caller)
      REFRESH MATERIALIZED VIEW  it rebuilds the view from that definition
                                 (carries `is_refresh`, likewise)
      DROP MATERIALIZED VIEW     it removes the view (typed one layer down, in
                                 relation_management)
      OPTIMIZE                   it compacts files losslessly and changes no
                                 contents, so there is nothing a REFRESH could
                                 discard (see visit_optimize_relation)

    Everything else - DDL and mutation alike, including MERGE, UPDATE and
    DELETE - is refused. A statement added here that mutates a relation and is
    not on that list must call this.
    """
    from opteryx.exceptions import UnsupportedSyntaxError

    if not node.connector.is_materialized_view(node.relation_name):
        return
    raise UnsupportedSyntaxError(
        f"{node.relation_name} is a materialized view, not a table - {statement} "
        "cannot write to one. Its contents come from its defining **SELECT**: "
        "change them with **CREATE OR REPLACE MATERIALIZED VIEW**, rebuild them "
        "with **REFRESH MATERIALIZED VIEW**, or remove it with "
        "**DROP MATERIALIZED VIEW**."
    )


def _scanned_relations(visitor, context) -> list:
    """Every relation the statement's bound SELECT subtree reads, first-seen order.

    Taken off the plan rather than re-parsed out of the SQL: each Scan node
    already names the relation it reads, and by bind time views and CTEs have
    been resolved to what is actually scanned - so this sees through an
    indirection that text matching would miss.

    A subquery embedded in an expression (`WHERE x IN (SELECT ...)`, `EXISTS
    (...)`, a scalar subquery) carries its own LogicalPlan as the `value` of a
    NodeType.SUBQUERY node hanging off the owning node's properties - e.g. a
    Filter's `.condition`. It is not spliced into this graph until
    decorrelation runs in the optimizer, well after binding calls this, so a
    plain node walk misses any relation scanned only inside one. Each node's
    expression properties are searched for embedded subqueries too, recursing
    into their sub-plans, with the same walk relation_resolver's CTE/view
    expansion already relies on to find them.
    """
    from opteryx.planner.logical_planner import LogicalPlanStepType
    from opteryx.planner.relation_resolver import _expression_subqueries

    relations: list = []

    def _collect(graph) -> None:
        if graph is None:
            return
        for _, plan_node in graph.nodes(True):
            if plan_node.node_type == LogicalPlanStepType.Scan:
                relation = plan_node.relation
                if relation not in relations:
                    relations.append(relation)
            for subquery in _expression_subqueries(plan_node):
                _collect(subquery.value)

    _collect(getattr(visitor, "graph", None))
    return relations


def _enforce_egress(visitor, node, context) -> None:
    """Refuse a write that would copy a protected workspace's data elsewhere.

    Applied to **every** write that reads catalog relations - CTAS, CREATE OR
    REPLACE, CREATE MATERIALIZED VIEW, and plain INSERT ... SELECT - not only
    to CTAS. Covering CTAS alone would not be a control: the same copy is two
    statements away (`CREATE TABLE mine.x AS SELECT ... LIMIT 0`, then
    `INSERT INTO mine.x SELECT ...`), and a boundary with a two-statement
    bypass teaches people to route around it rather than respect it.

    A materialized view's refresh is covered by the same call, because a
    refresh is not a statement of its own: `trigger_firing._fire_refresh`
    submits `CREATE OR REPLACE TABLE <view> AS <sql>`, which arrives here as an
    ordinary CTAS. The catalog checks the same boundary again at fire time,
    before the refresh job is written - two independent checks on the same
    setting, which is deliberate for a protection that can be switched on long
    after the view was created.

    Called after the target's permission check in both paths, so a caller who
    may not write the target learns that first and cannot use this to probe
    another workspace's protection state.

    Non-catalog sources are dropped: `$planets`, `information_schema`, and
    anything behind a non-Writable connector belong to no workspace, so they
    cannot leave one.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import EgressRestrictedError

    sources = []
    for relation in _scanned_relations(visitor, context):
        if relation.startswith("$") or "information_schema" in relation.split("."):
            continue
        gateway = connector_factory(relation, telemetry=context.telemetry)
        if isinstance(gateway, Writable):
            sources.append(relation)

    if not sources:
        return

    refusals = node.connector.egress_verdict(node.relation_name, sources)
    if not refusals:
        return

    # One refusal reads as the catalog wrote it. Several are composed here
    # rather than reported one at a time: a join across three protected
    # workspaces would otherwise take three attempts to discover, and someone
    # asking for access needs to ask once.
    if len(refusals) == 1:
        raise EgressRestrictedError(refusals[0].message)

    workspaces = ", ".join(f"'{refusal.workspace}'" for refusal in refusals)
    remediations = " ".join(refusal.remediation for refusal in refusals)
    raise EgressRestrictedError(
        f"Cannot write {node.relation_name}: it would copy data out of workspaces "
        f"{workspaces}, which restrict egress. Clear them with: {remediations}"
    )


def visit_insert(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """
    Bind the INSERT node:
    - resolve target connector, must be Writable
    - read target schema from connector
    - resolve source columns (VALUES feeder or bound SELECT tail)
    - resolve target column order (schema order or explicit list)
    - validate column count and per-column type compatibility
    - record column_mapping for the InsertNode to permute morsels at write time
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import (
        ReadOnlyConnectorError,
        DatasetNotFoundError,
        UnsupportedSyntaxError,
        InvalidInternalStateError,
    )
    from opteryx.expression import NodeType
    from opteryx.managers.permissions import can_perform_action
    from opteryx.models import LogicalColumn
    from opteryx.types.schema import RelationSchema

    from opteryx.types.logical_type import LogicalCategory
    from opteryx.types.schema import SchemaColumn

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support INSERT"
        )

    create_target = getattr(node, "create_target", False)
    if_not_exists = getattr(node, "if_not_exists", False)
    or_replace = getattr(node, "or_replace", False)
    is_materialized_view = getattr(node, "is_materialized_view", False)

    # Bind-time capture, same reasoning as match_threshold: the InsertNode reads
    # this once from its own parameters rather than the native write path
    # touching session variables mid-execution.
    node.write_coalesce_rows = context.execution_context.variables["write_coalesce_rows"]

    if is_materialized_view:
        # The MV target: writer tier. Creating a view is authoring derived data
        # in a place you may already write - it does nothing you could not do by
        # hand with a CTAS, so it needs no more authority than that. Note this
        # makes CREATE OR REPLACE MATERIALIZED VIEW writer-tier where CREATE OR
        # REPLACE TABLE stays owner-tier; a view's contents are rebuildable from
        # its definition, so the blast radius genuinely is lower.
        if not can_perform_action(context.execution_context, node.relation_name, action="WRITE"):
            raise PermissionError(
                f"User does not have permission to create materialized view "
                f"{node.relation_name} (write required)"
            )

        # Source-table extraction: the bound SELECT subtree already knows every
        # relation it scans - collect them from the Scan nodes rather than
        # re-parsing the SQL. Every scanned relation must be catalog-resident:
        # a virtual dataset ($planets), information_schema, or a non-catalog
        # source could never fire the MV's refresh.
        source_tables = []
        for relation in _scanned_relations(self, context):
            gateway = connector_factory(relation, telemetry=context.telemetry)
            if (
                relation.startswith("$")
                or "information_schema" in relation.split(".")
                or not isinstance(gateway, Writable)
            ):
                raise UnsupportedSyntaxError(
                    f"Materialized view source '{relation}' is not a catalog table; "
                    "an MV can only read catalog tables - commits to them are what "
                    "fire its refresh."
                )
            source_tables.append(relation)

        if not source_tables:
            raise UnsupportedSyntaxError(
                "A materialized view needs at least one catalog table as a source - "
                "nothing could ever fire its refresh."
            )

        # Sources need only READ: if you can read a table you may derive from
        # it, provided you can write where the result lands. Requiring write
        # here would mean no view could ever be built over data you are only
        # permitted to read, which is most of what views are for.
        #
        # This check runs on EVERY registration, not just the first, and it runs
        # against whoever is executing - so an editor can never repoint a view
        # at sources they could not have read themselves. That is what keeps a
        # pinned `runs-as` owner from turning edits into a confused deputy.
        for source in source_tables:
            if not can_perform_action(context.execution_context, source, action="READ"):
                raise PermissionError(
                    f"User does not have permission to read materialized view "
                    f"source {source} (read required)"
                )

        node.source_tables = source_tables

    # The write is aimed at an existing materialized view. Allowed only for the
    # two statements that own one: CREATE (OR REPLACE) MATERIALIZED VIEW, and
    # REFRESH MATERIALIZED VIEW, which reaches the binder as a CTAS carrying
    # `is_refresh`. Everything else - plain CTAS, CREATE OR REPLACE TABLE,
    # INSERT - is refused, whether or not the target exists yet.
    if not is_materialized_view and not getattr(node, "is_refresh", False):
        _reject_materialized_view_target(
            node, "**CREATE TABLE ... AS SELECT**" if create_target else "**INSERT**"
        )

    if create_target:
        node.is_replace = False
        existing_column_names = None
        if node.connector.relation_exists(node.relation_name):
            if if_not_exists:
                node.is_noop = True
                node.columns = []
                node.target_schema = None
                node.column_mapping = None
                node.target_column_names = None
                return node, context
            if not or_replace:
                raise ValueError(
                    f"relation already exists: {node.relation_name} "
                    "(CTAS does not append to existing relations; use INSERT)"
                )
            # CREATE OR REPLACE on an existing relation has the same blast
            # radius as DROP (the old relation's data/history is gone) - reuse
            # that tier rather than inventing a new one.
            #
            # Materialized views are the exception, because their contents are
            # derived and rebuildable rather than authored. Both statements that
            # legitimately replace one land at a lower tier: CREATE OR REPLACE
            # MATERIALIZED VIEW was already authorized at WRITE by the
            # `is_materialized_view` branch above, and REFRESH MATERIALIZED VIEW
            # arrives here carrying `is_refresh` and takes the REFRESH tier.
            if getattr(node, "is_refresh", False):
                if not can_perform_action(
                    context.execution_context, node.relation_name, action="REFRESH"
                ):
                    raise PermissionError(
                        f"User does not have permission to refresh materialized view "
                        f"{node.relation_name}"
                    )
            elif not is_materialized_view and not can_perform_action(
                context.execution_context, node.relation_name, action="DROP"
            ):
                raise PermissionError(
                    f"User does not have permission to replace table {node.relation_name}"
                )
            node.is_replace = True
            existing_column_names = node.connector.relation_column_names(node.relation_name)
        else:
            if not can_perform_action(context.execution_context, node.relation_name, action="CREATE"):
                raise PermissionError(
                    f"User does not have permission to create table {node.relation_name}"
                )

        # After the target's permission check, before any schema work: the
        # source workspaces have to agree to the copy as well as the caller
        # being allowed to write the target.
        _enforce_egress(self, node, context)

        if getattr(self, "graph", None) is None or node.source_tail_id is None:
            raise InvalidInternalStateError(
                "visit_insert: CTAS requires graph and source_tail_id"
            )
        feeder = self.graph[node.source_tail_id]
        if not getattr(feeder, "columns", None):
            raise InvalidInternalStateError(
                "visit_insert: CTAS source feeder has no bound columns"
            )

        target_columns = []
        seen_names = {}
        for src_col in feeder.columns:
            sc = src_col.schema_column
            target_name = (
                src_col.alias
                or (sc.name if sc is not None else None)
                or src_col.source_column
            )
            if not target_name:
                raise InvalidInternalStateError(
                    "CTAS source column has no resolvable name"
                )
            # Disambiguate duplicates: SELECT 1, 1 → 1, 1_1
            if target_name in seen_names:
                seen_names[target_name] += 1
                target_name = f"{target_name}_{seen_names[target_name]}"
            else:
                seen_names[target_name] = 0
            if sc.category is None or sc.category == LogicalCategory.NULL:
                raise UnsupportedSyntaxError(
                    f"CTAS column '{target_name}' has unresolved type; "
                    "specify the **SELECT**'s column types explicitly"
                )
            from opteryx.types.schema import mint_column_identity
            flat = SchemaColumn(
                name=target_name,
                column_type=sc.column_type,
                nullable=getattr(sc, "nullable", True),
                identity=mint_column_identity(getattr(node, "relation_name", None), target_name),
            )
            target_columns.append(flat)

        if existing_column_names is not None and not getattr(
            node.connector, "supports_schema_evolution_on_replace", False
        ):
            # Schema-preserving REPLACE only for connectors that can't evolve
            # schema (e.g. the catalog connector has no public primitive to
            # write a new schema version outside create_dataset's internal
            # path) - a changed column set fails loud here rather than
            # silently committing data the declared schema can't describe.
            # Column-name comparison only, not full type fidelity - the
            # catalog's stored schema and Opteryx's RelationSchema are two
            # independent type representations with no bridge between them yet.
            new_names = [c.name for c in target_columns]
            if set(new_names) != set(existing_column_names) or len(new_names) != len(
                existing_column_names
            ):
                raise UnsupportedSyntaxError(
                    f"**CREATE OR REPLACE** {node.relation_name}: the **SELECT**'s columns "
                    f"({new_names}) differ from the existing relation's columns "
                    f"({existing_column_names}) - schema-changing REPLACE is not yet supported"
                )

        target_schema = RelationSchema(
            name=node.relation_name,
            columns=target_columns,
        )
        node.target_schema = target_schema
        node.column_mapping = list(range(len(target_columns)))
        node.target_column_names = [c.name for c in target_columns]
        node.columns = []
        return node, context

    if not node.connector.relation_exists(node.relation_name):
        raise DatasetNotFoundError(connector=node.connector, dataset=node.relation_name)

    # Appending rows to an existing relation - same tier as CREATE VIEW/COMMENT.
    if not can_perform_action(context.execution_context, node.relation_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to insert into {node.relation_name}"
        )

    # INSERT ... SELECT copies just as durably as CTAS does - see _enforce_egress
    # on why covering only the CREATE path would not be a boundary. A VALUES
    # insert scans nothing and drops straight through.
    _enforce_egress(self, node, context)

    # Read the existing relation's schema through the connector-agnostic table
    # engine, not _relation_dir/_read_dataset_json, which are
    # LocalStoreConnector-only private filesystem helpers. Catalog-backed
    # connectors (e.g. OpteryxConnector) have no such attributes, so calling
    # them here crashed unconditionally on every non-local deployment:
    # AttributeError: 'OpteryxConnector' object has no attribute '_relation_dir'.
    #
    # The DECLARED schema, not the current snapshot's - see
    # BaseTable.get_declared_schema. This used to read get_dataset_metadata(),
    # which resolves a snapshot before it reads anything and so refused every
    # relation with nothing committed to it: the FIRST insert into a
    # freshly-created table could never run, and no SQL-driven pipeline could
    # bootstrap its own tables. An INSERT never reads the target's data - it
    # discarded that manifest, having paid a full table.scan() of every data
    # file and its statistics to build it.
    table = node.connector.table_engine(node.relation_name, telemetry=context.telemetry)
    target_schema = table.get_declared_schema()  # RelationSchema with SchemaColumn list

    node.target_schema = target_schema
    node.columns = []  # binder convention; INSERT produces no output columns

    # ---- 1. Source column count and types ----
    values_node = node.values_feeder  # set for VALUES path; None for SELECT
    if values_node is not None:
        if not values_node.values:
            raise UnsupportedSyntaxError("**INSERT** **VALUES** requires at least one row")
        source_column_count = len(values_node.values[0])
        # Probe types from the first row (parser-resolved literal types).
        source_types = [values_node.values[0][i].type for i in range(source_column_count)]
        # Validate all rows have the same column count.
        for row in values_node.values:
            if len(row) != source_column_count:
                raise UnsupportedSyntaxError(
                    f"**INSERT** row has {len(row)} values, expected {source_column_count}"
                )
    else:
        if getattr(self, "graph", None) is None or node.source_tail_id is None:
            raise InvalidInternalStateError(
                "visit_insert: SELECT path requires graph and source_tail_id"
            )
        feeder = self.graph[node.source_tail_id]
        if not getattr(feeder, "columns", None):
            raise InvalidInternalStateError(
                "visit_insert: source feeder has no bound columns"
            )
        source_column_count = len(feeder.columns)
        source_types = [c.schema_column.category for c in feeder.columns]

    # ---- 2. Target column order (schema order, or explicit list order) ----
    explicit_columns = getattr(node, "explicit_columns", None)
    if explicit_columns is None:
        target_columns_in_order = list(target_schema.columns)
    else:
        schema_by_name = {c.name: c for c in target_schema.columns}
        target_columns_in_order = []
        for cname in explicit_columns:
            if cname not in schema_by_name:
                raise ColumnNotFoundError(
                    column=cname,
                    dataset=node.relation_name,
                    suggestion=suggest_alternative(cname, list(schema_by_name)),
                )
            target_columns_in_order.append(schema_by_name[cname])
        if len(target_columns_in_order) != len(target_schema.columns):
            raise UnsupportedSyntaxError(
                f"**INSERT** explicit column list must list all target columns "
                f"(target has {len(target_schema.columns)}, got {len(target_columns_in_order)}). "
                "Partial column inserts are not yet supported."
            )

    # ---- 3. Validate count and per-column types ----
    if source_column_count != len(target_columns_in_order):
        raise UnsupportedSyntaxError(
            f"**INSERT** row has {source_column_count} values, "
            f"expected {len(target_columns_in_order)} (target table: {node.relation_name})"
        )

    for src_idx, target_col in enumerate(target_columns_in_order):
        src_type = source_types[src_idx]
        if not _types_compatible(src_type, target_col.category):
            raise UnsupportedSyntaxError(
                f"**INSERT** type mismatch on column '{target_col.name}': "
                f"source {src_type} is not compatible with target {target_col.category}"
            )

    # ---- 4. Build column mapping (source idx → target schema idx) ----
    schema_index_by_name = {c.name: i for i, c in enumerate(target_schema.columns)}
    column_mapping = [
        schema_index_by_name[target_columns_in_order[src_idx].name]
        for src_idx in range(source_column_count)
    ]
    node.column_mapping = column_mapping
    node.target_column_names = [c.name for c in target_schema.columns]

    # ---- 5. VALUES feeder mutation: replace placeholder columns ----
    # The downstream FunctionDataset has been bound with placeholder column
    # names (`$col0`, ...). Replace those with LogicalColumns matching the
    # user-listed target order so the source pipeline carries meaningful
    # names; the InsertNode will permute to schema order at write time.
    if values_node is not None:
        target_relation_name = values_node.alias
        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=col.name,
                source=target_relation_name,
                schema_column=col,
            )
            for col in target_columns_in_order
        ]
        values_node.columns = columns
        schema = RelationSchema(
            name=target_relation_name,
            columns=[c.schema_column for c in columns],
        )
        context.schemas[target_relation_name] = schema

    return node, context


def visit_merge(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind the MERGE node.

    The join, the action chain and the blended columns were already desugared
    into an ordinary SELECT by `plan_merge` and bound like any other, so this
    binds only the SINK: who may write, what shape the rows must have, and
    whether the statement is inside the row cap.
    """
    from opteryx.connectors import connector_factory
    from opteryx.connectors.capabilities import Writable
    from opteryx.exceptions import InvalidInternalStateError
    from opteryx.exceptions import ReadOnlyConnectorError
    from opteryx.exceptions import UnsupportedSyntaxError
    from opteryx.managers.permissions import can_perform_action
    from opteryx.planner.logical_planner.merge_desugar import MERGE_ACTION_COLUMN
    from opteryx.planner.logical_planner.merge_desugar import MERGE_FILE_COLUMN
    from opteryx.planner.logical_planner.merge_desugar import MERGE_ORDINAL_COLUMN

    node.connector = connector_factory(node.relation_name, telemetry=context.telemetry)
    if not isinstance(node.connector, Writable):
        raise ReadOnlyConnectorError(
            f"connector for {node.relation_name} does not support MERGE"
        )

    # MERGE both deletes and appends, so it needs the same authority as any
    # other write to the relation - no more, and no less.
    if not can_perform_action(context.execution_context, node.relation_name, action="WRITE"):
        raise PermissionError(
            f"User does not have permission to merge into {node.relation_name}"
        )

    # `statement_name` is set by every builder that produces this node
    # (plan_merge, plan_update, plan_delete). None means a fourth builder
    # appeared without setting it, which would put the word "None" in a user's
    # error - fail on the invariant instead of papering over it.
    if not node.statement_name:
        raise InvalidInternalStateError(
            "visit_merge: the merge sink node does not name its statement"
        )
    # Refused before any manifest is read. Writing to a view would either be
    # discarded by the next REFRESH or survive and leave the view disagreeing
    # with its own definition - see _reject_materialized_view_target.
    _reject_materialized_view_target(node, f"**{node.statement_name}**")

    # Read the target's schema through the connector-agnostic table engine, as
    # visit_insert does — the gateway connector has no schema of its own.
    table = node.connector.table_engine(node.relation_name, telemetry=context.telemetry)
    if getattr(table, "get_dataset_metadata", None) is not None:
        target_schema, _target_manifest = table.get_dataset_metadata()
    else:
        target_schema = table.get_dataset_schema()
    node.target_schema = target_schema
    # The ordered data-file list the sink maps `$merge_file` through. It must be
    # the SAME list, in the SAME order, that the scan indexed against - both come
    # from this relation's manifest, read once here.
    node.file_paths = list(_target_manifest.get_file_paths()) if _target_manifest else []
    node.columns = []  # binder convention; MERGE produces no output columns

    # ---- output shape -----------------------------------------------------
    # `plan_merge` built the projection as the target's columns in schema order
    # followed by the three control columns, so the sink can split by position
    # rather than by name. Verify that here: a drift between the desugar and the
    # sink would write the right values into the wrong columns.
    feeder = self.graph[node.source_tail_id]
    if not getattr(feeder, "columns", None):
        raise InvalidInternalStateError("visit_merge: source feeder has no bound columns")

    # `column.alias` is the output name — the same field ExitNode derives its
    # `final_names` from. NOT `schema_column.name`, which on a computed column
    # is the rendered expression text rather than anything the desugar chose.
    produced = [c.alias for c in feeder.columns]
    expected = list(node.target_column_names) + [
        MERGE_ACTION_COLUMN,
        MERGE_FILE_COLUMN,
        MERGE_ORDINAL_COLUMN,
    ]
    if produced != expected:
        raise InvalidInternalStateError(
            "visit_merge: merge projection does not match the target's columns "
            f"(produced {produced}, expected {expected})"
        )

    target_by_name = {c.name: c for c in target_schema.columns}
    for index, column_name in enumerate(node.target_column_names):
        target_column = target_by_name.get(column_name)
        if target_column is None:  # pragma: no cover - names come from the schema
            raise InvalidInternalStateError(
                f"visit_merge: target has no column {column_name}"
            )
        produced_category = feeder.columns[index].schema_column.category
        if not _types_compatible(produced_category, target_column.category):
            raise UnsupportedSyntaxError(
                f"**MERGE INTO** type mismatch on column '{column_name}': "
                f"{produced_category} is not compatible with target "
                f"{target_column.category}"
            )

    return node, context


def _bind_grant_administration(node: Node, context: BindingContext, action: str) -> Node:
    """Shared binding for GRANT, REVOKE and SHOW GRANTS ON.

    The bind-time gate is the same question the capability's apply/list path
    asks again authoritatively at execution — asked here too so a pre-flight
    bind (the jobs API checks permissions before queueing) refuses a statement
    the caller could never run, instead of queueing it to fail later. Both
    reads go to the one registered capability, so they cannot disagree.

    No connector is bound: nothing here reads or writes data, and whether the
    named object exists is the policy service's caller-side concern (see
    opteryx-access grants.py — deliberately not a permissions question).

    The execution context is stashed on the node because the apply happens at
    EXECUTION time, where the operator has no BindingContext — the capability
    needs the acting identity to hold the authority check, the no-self-service
    rule, and the audit record to.
    """
    from opteryx.managers.permissions import can_perform_action
    from opteryx.managers.permissions import can_perform_workspace_action

    if node.object_kind == "workspace":
        # A bare workspace name is not a relation — `can_perform_action` reads
        # a dotless name as a local table and short-circuits, so the
        # workspace-level check is its own gate, exactly as ALTER WORKSPACE's.
        permitted = can_perform_workspace_action(
            context.execution_context, node.object_name, action=action
        )
    else:
        permitted = can_perform_action(
            context.execution_context, node.object_name, action=action
        )
    if not permitted:
        raise PermissionError(
            f"User does not have permission to administer grants on "
            f"{node.object_kind} {node.object_name} (owner required)"
        )

    node.execution_context = context.execution_context
    node.columns = []
    return node


def visit_grant_access(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind GRANT <role> ON <kind> <object> TO USER <user>. Owner-gated."""
    return _bind_grant_administration(node, context, "GRANT"), context


def visit_revoke_access(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind REVOKE <role> ON <kind> <object> FROM USER <user>. Owner-gated."""
    return _bind_grant_administration(node, context, "REVOKE"), context


def visit_show_grants_on(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    """Bind SHOW GRANTS ON <kind> <object>.

    Gated at GRANT — the same authority a mutation needs, deliberately: who
    may see the grants on an object is who may change them (architect ruling
    2026-08-27). A weaker read-tier gate would let a reader enumerate who else
    holds what.
    """
    return _bind_grant_administration(node, context, "GRANT"), context


def visit_show_effective_grants_on(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    """Bind SHOW EFFECTIVE GRANTS ON <kind> <object>.

    Identically gated to SHOW GRANTS ON, and not weakened because it reports
    MORE: it names every principal who can reach the object, which is strictly
    more of the thing the owner gate exists to protect.
    """
    return _bind_grant_administration(node, context, "GRANT"), context
