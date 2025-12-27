"""
Test Iceberg connector view functionality (Eidetic capability)
"""

import os
import sys

sys.path.insert(1, os.path.join(sys.path[0], "../../.."))

import opteryx
from opteryx.connectors import IcebergConnector
from tests import set_up_iceberg, is_arm, is_mac, is_windows, skip_if


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_create_and_get_view():
    """Test creating and retrieving a view"""
    
    catalog = set_up_iceberg()
    opteryx.register_workspace(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    
    # Create a view directly using the catalog
    view_sql = "SELECT name, mass FROM iceberg.opteryx.planets WHERE mass > 1"
    catalog.create_view(
        identifier=("opteryx", "heavy_planets"),
        sql=view_sql,
        author="test_user"
    )
    
    try:
        # Create a connector instance to test the Eidetic methods
        from opteryx.connectors import connector_factory
        from opteryx.models import QueryTelemetry
        
        connector = connector_factory("iceberg.opteryx.planets", QueryTelemetry())
        
        # Test get_view
        view_def = connector.get_view("heavy_planets")
        assert view_def.name == "heavy_planets"
        assert view_def.statement == view_sql
        assert view_def.owner == "test_user"
        
    finally:
        # Clean up - drop the view
        catalog.drop_view(("opteryx", "heavy_planets"))


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_list_views():
    """Test listing views"""
    
    catalog = set_up_iceberg()
    opteryx.register_workspace(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    
    # Create multiple views
    views_to_create = [
        ("test_view_1", "SELECT * FROM iceberg.opteryx.planets WHERE id < 5"),
        ("test_view_2", "SELECT name FROM iceberg.opteryx.planets"),
    ]
    
    for view_name, view_sql in views_to_create:
        catalog.create_view(
            identifier=("opteryx", view_name),
            sql=view_sql,
            author="test_user"
        )
    
    try:
        # Create a connector instance to test list_views
        from opteryx.connectors import connector_factory
        from opteryx.models import QueryTelemetry
        
        connector = connector_factory("iceberg.opteryx.planets", QueryTelemetry())
        
        # Test list_views
        views = connector.list_views()
        view_names = [v.name for v in views]
        
        assert "test_view_1" in view_names
        assert "test_view_2" in view_names
        
    finally:
        # Clean up - drop the views
        for view_name, _ in views_to_create:
            catalog.drop_view(("opteryx", view_name))


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_create_and_drop_view():
    """Test creating and dropping a view using connector methods"""
    
    catalog = set_up_iceberg()
    opteryx.register_workspace(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    
    from opteryx.connectors import connector_factory
    from opteryx.models import QueryTelemetry
    
    connector = connector_factory("iceberg.opteryx.planets", QueryTelemetry())
    
    # Test create_view
    view_sql = "SELECT name, gravity FROM iceberg.opteryx.planets WHERE gravity > 10"
    connector.create_view("test_gravity_view", view_sql, owner="test_user")
    
    # Verify it was created
    view_def = connector.get_view("test_gravity_view")
    assert view_def.name == "test_gravity_view"
    assert view_def.statement == view_sql
    
    # Test drop_view
    connector.drop_view("test_gravity_view")
    
    # Verify it was dropped by checking list_views
    views = connector.list_views()
    view_names = [v.name for v in views]
    assert "test_gravity_view" not in view_names


@skip_if(is_arm() or is_windows() or is_mac())
def test_iceberg_view_with_namespace():
    """Test view operations with explicit namespace in view name"""
    
    catalog = set_up_iceberg()
    opteryx.register_workspace(
        "iceberg",
        IcebergConnector,
        catalog=catalog,
        remove_prefix=True,
    )
    
    from opteryx.connectors import connector_factory
    from opteryx.models import QueryTelemetry
    
    connector = connector_factory("iceberg.opteryx.planets", QueryTelemetry())
    
    # Test with namespace.view_name format
    view_sql = "SELECT * FROM iceberg.opteryx.satellites WHERE planetId > 5"
    connector.create_view("opteryx.outer_satellites", view_sql, owner="test_user")
    
    try:
        # Get view with namespace
        view_def = connector.get_view("opteryx.outer_satellites")
        assert view_def.name == "outer_satellites"
        assert view_def.statement == view_sql
        
    finally:
        # Drop view with namespace
        connector.drop_view("opteryx.outer_satellites")


if __name__ == "__main__":  # pragma: no cover
    import time
    
    start_suite = time.monotonic_ns()
    
    passed = 0
    failed = 0
    
    tests = [
        ("test_iceberg_create_and_get_view", test_iceberg_create_and_get_view),
        ("test_iceberg_list_views", test_iceberg_list_views),
        ("test_iceberg_create_and_drop_view", test_iceberg_create_and_drop_view),
        ("test_iceberg_view_with_namespace", test_iceberg_view_with_namespace),
    ]
    
    print(f"RUNNING {len(tests)} ICEBERG VIEW TESTS")
    
    for index, (test_name, test_func) in enumerate(tests):
        print(f"\033[38;2;255;184;108m{(index + 1):04}\033[0m {test_name}", end="", flush=True)
        try:
            start = time.monotonic_ns()
            test_func()
            print(f"\033[38;2;26;185;67m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms\033[0m ✅")
            passed += 1
        except BaseException as err:
            print(f"\033[0;31m{str(int((time.monotonic_ns() - start)/1e6)).rjust(4)}ms ❌\033[0m")
            print(">", err)
            failed += 1
    
    print("- ✅ \033[0;32mdone\033[0m")
    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed) if (passed + failed) > 0 else 0}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )
