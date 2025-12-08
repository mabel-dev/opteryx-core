
import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))
os.environ["OPTERYX_DEBUG"] = "1"

from tests import is_arm, is_mac, is_windows, skip_if
from tests import set_up_iceberg
import opteryx
from opteryx.connectors import IcebergConnector
from opteryx.compiled.structures.relation_statistics import to_int
from opteryx.models import QueryStatistics

print(f"Running Iceberg tests on Python {sys.version.split(' ')[0]}, Opteryx {opteryx.__version__}")

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_basic():

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    table = catalog.load_table("opteryx.tweets")
    table.scan().to_arrow()


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_schema():

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    table = catalog.load_table("opteryx.tweets")
    table.schema().as_arrow()

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_connector():

    catalog = set_up_iceberg()

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    table = opteryx.query("SELECT * FROM iceberg.opteryx.tweets WHERE followers = 10")
    assert table.shape[0] == 353

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_stats_tweets():

    from opteryx.connectors import IcebergConnector, connector_factory

    catalog = set_up_iceberg()

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    connector = connector_factory("iceberg.opteryx.tweets", QueryStatistics())
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 100000
    assert stats.lower_bounds[b"followers"] == 0
    assert stats.upper_bounds[b"followers"] == 8266250
    assert stats.lower_bounds[b"user_name"] == to_int("")
    assert stats.upper_bounds[b"user_name"] == to_int("🫖🔫")
    assert stats.lower_bounds[b"tweet_id"] == to_int(1346604539013705728)
    assert stats.upper_bounds[b"tweet_id"] == to_int(1346615999009755142)
    assert stats.lower_bounds[b"text"] == to_int("!! PLEASE STOP A")
    assert stats.upper_bounds[b"text"] == to_int("🪶Cultural approq")
    assert stats.lower_bounds[b"timestamp"] == to_int("2021-01-05T23:48")
    assert stats.upper_bounds[b"timestamp"] == to_int("2021-01-06T00:35")
    
@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_stats_missions():

    from opteryx.connectors import IcebergConnector, connector_factory

    catalog = set_up_iceberg()

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    connector = connector_factory("iceberg.opteryx.tweets", QueryStatistics())
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 100000
    assert stats.lower_bounds[b"followers"] == 0
    assert stats.upper_bounds[b"followers"] == 8266250
    assert stats.lower_bounds[b"user_name"] == to_int("")
    assert stats.upper_bounds[b"user_name"] == to_int("🫖🔫")
    assert stats.lower_bounds[b"tweet_id"] == to_int(1346604539013705728)
    assert stats.upper_bounds[b"tweet_id"] == to_int(1346615999009755142)
    assert stats.lower_bounds[b"text"] == to_int("!! PLEASE STOP A")
    assert stats.upper_bounds[b"text"] == to_int("🪶Cultural approq")
    assert stats.lower_bounds[b"timestamp"] == to_int("2021-01-05T23:48")
    assert stats.upper_bounds[b"timestamp"] == to_int("2021-01-06T00:35")

@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_get_stats_remote():

    from decimal import Decimal
    from pyiceberg.catalog import load_catalog
    from opteryx.connectors import IcebergConnector, connector_factory

    DATA_CATALOG_CONNECTION = os.environ.get("DATA_CATALOG_CONNECTION")
    DATA_CATALOG_STORAGE = os.environ.get("DATA_CATALOG_STORAGE")

    catalog = load_catalog(
        "opteryx",
        **{
            "uri": DATA_CATALOG_CONNECTION,
            "warehouse": DATA_CATALOG_STORAGE,
        }
    )

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    connector = connector_factory("iceberg.iceberg.planets", QueryStatistics())
    connector.get_dataset_schema()
    stats = connector.relation_statistics

    assert stats.record_count == 9
    assert stats.lower_bounds[b"id"] == 1, stats.lower_bounds[b"id"]
    assert stats.upper_bounds[b"id"] == 9, stats.upper_bounds[b"id"]
    assert stats.lower_bounds[b"name"] == to_int("Earth"), stats.lower_bounds[b"name"]
    assert stats.upper_bounds[b"name"] == to_int("Venus"), stats.upper_bounds[b"name"]
    assert stats.lower_bounds[b"mass"] == to_int(0.0146), stats.lower_bounds[b"mass"]
    assert stats.upper_bounds[b"mass"] == to_int(1898.0), stats.upper_bounds[b"mass"]
    assert stats.lower_bounds[b"diameter"] == 2370, stats.lower_bounds[b"diameter"]
    assert stats.upper_bounds[b"diameter"] == 142984, stats.upper_bounds[b"diameter"]
    assert stats.lower_bounds[b"gravity"] == to_int(Decimal("0.7")), stats.lower_bounds[b"gravity"]
    assert stats.upper_bounds[b"gravity"] == to_int(Decimal("23.1")), stats.upper_bounds[b"gravity"]
    assert stats.lower_bounds[b"surfacePressure"] == to_int(0.0), stats.lower_bounds[b"surfacePressure"]
    assert stats.upper_bounds[b"surfacePressure"] == to_int(92.0), stats.upper_bounds[b"surfacePressure"]


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_remote():

    from pyiceberg.catalog import load_catalog

    DATA_CATALOG_CONNECTION = os.environ.get("DATA_CATALOG_CONNECTION")
    DATA_CATALOG_STORAGE = os.environ.get("DATA_CATALOG_STORAGE")

    catalog = load_catalog(
        "opteryx",
        **{
            "uri": DATA_CATALOG_CONNECTION,
            "warehouse": DATA_CATALOG_STORAGE,
        }
    )

    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    table = opteryx.query("SELECT * FROM iceberg.iceberg.tweets WHERE followers = 10")
    assert table.shape[0] == 353


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_empty_table_read():
    """
    Tests that an Iceberg table created with no data behaves correctly:
    - Read without time travel returns an empty result with expected columns
    - Time-travel (FOR clause) raises DatasetReadError because there are no snapshots
    """

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa

    # Create/replace an empty table with a simple schema and no snapshots
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table_name = "opteryx.empty_test"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    table = catalog.create_table(table_name, schema=schema)

    # Non-timetravel read should return an empty table with a valid schema
    result = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.empty_test")
    assert result.shape == (0, len(schema))


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_empty_table_count_and_schema():
    """
    Verify that empty tables return a valid schema and COUNT(*) returns 0.
    """

    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa

    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table_name = "opteryx.empty_test"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    catalog.create_table(table_name, schema=schema)

    result = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.empty_test")
    assert result.shape == (0, len(schema))
    # Check the schema column names as expected
    assert result.column_names == ["id", "name"]

    count_result = opteryx.query_to_arrow("SELECT COUNT(*) FROM iceberg.opteryx.empty_test;")
    assert count_result.shape == (1, 1)
    # Ensure the COUNT(*) value is 0
    assert count_result.to_pandas().iloc[0, 0] == 0


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_empty_table_select_columns():
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table_name = "opteryx.empty_test"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    catalog.create_table(table_name, schema=schema)

    res = opteryx.query_to_arrow("SELECT id FROM iceberg.opteryx.empty_test")
    assert res.shape == (0, 1)
    assert res.column_names == ["id"]


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_single_snapshot_where_clause():
    catalog = set_up_iceberg()
    opteryx.register_store(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )

    import pyarrow as pa
    schema = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])
    table_name = "opteryx.single_snapshot"
    try:
        catalog.drop_table(table_name)
    except Exception:
        pass
    table = catalog.create_table(table_name, schema=schema)
    snapshot_data = pa.Table.from_arrays([pa.array([1, 2, 3]), pa.array(["a", "b", "c"])], schema=schema)
    
    table.append(snapshot_data)

    res = opteryx.query_to_arrow("SELECT * FROM iceberg.opteryx.single_snapshot WHERE id = 1")
    assert res.shape[0] == 1


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_two_snapshot_inspection_and_contents():
    catalog = set_up_iceberg()
    # Load the table and inspect snapshot history
    table = catalog.load_table("opteryx.two_snap_battery")
    snapshots = table.inspect.snapshots().sort_by("committed_at").to_pylist()
    assert len(snapshots) >= 2

    # Verify first snapshot rows
    first_snapshot_id = snapshots[0]["snapshot_id"]
    first_snapshot = table.snapshot_by_id(first_snapshot_id)
    assert first_snapshot is not None
    first_scan = table.scan(snapshot_id=first_snapshot_id).to_arrow()
    assert first_scan.num_rows == 2

    # Verify second snapshot rows
    second_snapshot_id = snapshots[-1]["snapshot_id"]
    second_snapshot = table.snapshot_by_id(second_snapshot_id)
    second_scan = table.scan(snapshot_id=second_snapshot_id).to_arrow()
    assert second_scan.num_rows == 3

def __test_firestore_gcs_connector_registration():
    """
    Test that the FirestoreGCSConnector can be registered without errors.
    This test does not require actual GCS or Firestore access.
    """
    FIRESTORE_DATABASE = os.environ.get("FIRESTORE_DATABASE")
    BUCKET_NAME = os.environ.get("GCS_BUCKET")
    GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

    from pyiceberg_firestore_gcs import FirestoreCatalog
    import opteryx
    
    opteryx.register_store(
        prefix="_default",
        connector=IcebergConnector,
        remove_prefix=False,
        catalog=FirestoreCatalog,
        firestore_project=GCP_PROJECT_ID,
        firestore_database=FIRESTORE_DATABASE,
        gcs_bucket=BUCKET_NAME,
    )

    res = opteryx.query("SELECT * FROM public.space.planets;")
    print(res)


if __name__ == "__main__":  # pragma: no cover
    from tests import run_tests

    #run_tests()
    __test_firestore_gcs_connector_registration()
