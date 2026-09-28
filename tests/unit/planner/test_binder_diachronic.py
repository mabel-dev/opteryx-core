import datetime

from opteryx.connectors.base.base_connector import BaseConnector
from opteryx.connectors.base.base_connector import BaseTable
from opteryx.connectors.capabilities import Diachronic
from opteryx.compiled.structures.plan_steps import ScanStep
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.planner.plan_context import PlanContext
from opteryx.planner.binder.common import BinderVisitor
from opteryx.types.logical_type import INT64
from opteryx.types.schema import ColumnDescriptor, RelationDescriptor


class FakeTable(BaseTable, Diachronic):
    """The per-query reader the gateway hands the binder."""

    __mode__ = "FAKE"

    def __init__(self, **kwargs):
        BaseTable.__init__(self, **kwargs)
        Diachronic.__init__(self, **kwargs)

    def get_dataset_schema(self):
        return RelationDescriptor(
            name="fake",
            columns=[
                ColumnDescriptor(
                    name="id",
                    column_type=INT64,
                )
            ],
        )


class FakeGateway(BaseConnector):
    """What connector_factory returns: the binder reads capabilities off the
    gateway and gets its reader from table_engine()."""

    __mode__ = "FAKE"
    supports_diachronic = True

    def table_engine(self, name, **kwargs):
        return FakeTable(dataset=name, **kwargs)


def test_binder_sets_diachronic_dates():
    visitor = BinderVisitor()
    node = ScanStep()
    node.relation = "fake"
    node.alias = "fake"
    node.start_date = datetime.datetime(2021, 1, 1)
    node.end_date = datetime.datetime(2021, 1, 2)

    from types import SimpleNamespace

    context = BindingContext(
        schemas={},
        query_id="query_id",
        execution_context=SimpleNamespace(memberships=["opteryx"]),
        relations={},
        telemetry=None,
        plan_context=PlanContext(),
    )

    # Monkeypatch the connector_factory so our fake connector is used
    import opteryx.connectors as connectors_module

    original_factory = connectors_module.connector_factory

    def fake_factory(_dataset, telemetry, **config):
        return FakeGateway()

    connectors_module.connector_factory = fake_factory

    # Restore the connector factory even when visit_scan raises: a leaked fake
    # factory fails every later test in the process that binds a dataset.
    try:
        node, _ = visitor.visit_scan(node, context)
    finally:
        connectors_module.connector_factory = original_factory
    assert node.connector is not None
    # Ensure Diachronic support results in connector start/ end dates set from node
    assert node.connector.start_date == node.start_date
    assert node.connector.end_date == node.end_date
