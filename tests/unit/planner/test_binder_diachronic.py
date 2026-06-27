import datetime

from opteryx.connectors.capabilities import Diachronic
from opteryx.models import Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.binder.common import BinderVisitor
from opteryx.types.logical_type import INT64
from opteryx.types.schema import RelationSchema, SchemaColumn, mint_column_identity


class FakeConnector(Diachronic):
    __mode__ = "FAKE"

    def get_dataset_schema(self):
        return RelationSchema(
            name="fake",
            columns=[
                SchemaColumn(
                    name="id",
                    column_type=INT64,
                    identity=mint_column_identity("fake", "id"),
                )
            ],
        )


def test_binder_sets_diachronic_dates():
    visitor = BinderVisitor()
    node = Node(node_type=None)
    node.relation = "fake"
    node.alias = "fake"
    node.start_date = datetime.datetime(2021, 1, 1)
    node.end_date = datetime.datetime(2021, 1, 2)
    node.connector = FakeConnector()

    from types import SimpleNamespace

    context = BindingContext(
        schemas={},
        query_id="query_id",
        connection=SimpleNamespace(memberships=["opteryx"]),
        relations={},
        telemetry=None,
    )

    # Monkeypatch the connector_factory so our fake connector is used
    import opteryx.connectors as connectors_module

    original_factory = connectors_module.connector_factory

    def fake_factory(_dataset, telemetry, **config):
        return FakeConnector(**config)

    connectors_module.connector_factory = fake_factory

    # Call visit_scan directly
    node, _ = visitor.visit_scan(node, context)
    # Restore the connector factory
    connectors_module.connector_factory = original_factory
    assert getattr(node, "connector", None) is not None
    # Ensure Diachronic support results in connector start/ end dates set from node
    assert node.connector.start_date == node.start_date
    assert node.connector.end_date == node.end_date
