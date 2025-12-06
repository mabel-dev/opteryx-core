"""
Test Harness

This module provides a set of utility functions and decorators to assist with
conditional test execution, platform-specific checks, and test result reporting
when running pytest tests locally. 

It includes functionality to:

- Check the platform and Python implementation (e.g., ARM architecture, Windows, macOS, PyPy).
- Conditionally skip tests based on platform, Python version, or environment variables.
- Download files and determine character display widths.
- Truncate and format printable strings.
- Discover and run test functions from the calling module, capturing output and providing detailed
  pass/fail status reports.

The primary entry point is the `run_tests` function, which discovers and executes all functions in
the calling module whose names start with 'test_', capturing their output and reporting the results
in a formatted manner.

Functions:
    is_arm(): Check if the current platform is ARM architecture.
    is_windows(): Check if the current platform is Windows.
    is_mac(): Check if the current platform is macOS.
    is_pypy(): Check if the current Python implementation is PyPy.
    manual(): Check if manual testing is enabled via the MANUAL_TEST environment variable.
    is_version(version): Check if the current Python version matches the specified version.
    skip(func): Decorator to skip the execution of a test function and issue a warning.
    skip_if(is_true=True): Decorator to conditionally skip the execution of a test function based on a condition.
    download_file(url, path): Download a file from a given URL and save it to a specified path.
    character_width(symbol): Determine the display width of a character based on its Unicode East Asian Width property.
    trunc_printable(value, width, full_line=True): Truncate a string to fit within a specified width, accounting for character widths.
    run_tests(): Discover and run test functions defined in the calling module.

Usage:
    To use this module, define your test functions in the calling module with names starting with 'test_'.
    Then call `run_tests()` to execute them and display the results.

Example:
    # In your test module
    def test_example():
        assert True

    if __name__ == "__main__":
        run_tests()
"""
import contextlib
import datetime
import os
import platform
from functools import wraps
from typing import Optional
from orso.tools import lru_cache_with_expiry

def is_arm():  # pragma: no cover
    """
    Check if the current platform is ARM architecture.

    Returns:
        bool: True if the platform is ARM, False otherwise.
    """
    return platform.machine() in ("armv7l", "aarch64")


def is_windows():  # pragma: no cover
    """
    Check if the current platform is Windows.

    Returns:
        bool: True if the platform is Windows, False otherwise.
    """
    return platform.system().lower() == "windows"


def is_mac():  # pragma: no cover
    """
    Check if the current platform is macOS.

    Returns:
        bool: True if the platform is macOS, False otherwise.
    """
    return platform.system().lower() == "darwin"


def is_linux():  # pragma: no cover
    """
    Check if the current platform is Linux.

    Returns:
        bool: True if the platform is Linux, False otherwise.
    """
    return platform.system().lower() == "linux"


def is_pypy():  # pragma: no cover
    """
    Check if the current Python implementation is PyPy.

    Returns:
        bool: True if the Python implementation is PyPy, False otherwise.
    """
    return platform.python_implementation() == "PyPy"


def manual():  # pragma: no cover
    """
    Check if manual testing is enabled via the MANUAL_TEST environment variable.

    Returns:
        bool: True if MANUAL_TEST environment variable is set, False otherwise.
    """
    import os

    return os.environ.get("MANUAL_TEST") is not None


def is_version(version: str) -> bool:  # pragma: no cover
    """
    Check if the current Python version matches the specified version.

    Parameters:
        version (str): The version string to check against.

    Returns:
        bool: True if the current Python version matches, False otherwise.

    Raises:
        Exception: If the version string is empty.
    """
    import sys

    if len(version) == 0:
        raise Exception("is_version needs a version")
    if version[-1] != ".":
        version += "."
    print(sys.version)
    return (sys.version.split(" ")[0] + ".").startswith(version)


def skip(func):  # pragma: no cover
    """
    Decorator to skip the execution of a test function and issue a warning.

    Parameters:
        func (Callable): The test function to skip.

    Returns:
        Callable: The wrapped function that issues a warning.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        import warnings

        warnings.warn(f"Skipping {func.__name__}")

    return wrapper


def skip_if(is_true: bool = True, reason: str = ""):  # pragma: no cover
    """
    Decorator to conditionally skip the execution of a test function based on a condition.

    Parameters:
        is_true (bool): Condition to skip the function. Defaults to True.

    Returns:
        Callable: The decorator that conditionally skips the test function.

    Example:
        I want to skip this test on ARM machines:

            @skip_if(is_arm()):
            def test...

        I want to skip this test on Windows machines running Python 3.8

            @skip_if(is_windows() and is_version("3.8"))
            def test...
    """
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if is_true and not manual():
                import warnings

                msg = reason or "conditional execution"
                warnings.warn(f"Skipping {func.__name__} because of {msg}.")
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorate


def find_file(path: str) -> Optional[str]:
    import glob

    matches = glob.iglob(path)
    return next(matches, None)


def download_file(url: str, path: str):  # pragma: no cover
    """
    Download a file from a given URL and save it to a specified path.

    Parameters:
        url (str): The URL to download the file from.
        path (str): The path to save the downloaded file.

    Returns:
        None
    """
    import requests

    response = requests.get(url)
    with open(path, "wb") as f:
        f.write(response.content)
    print(f"Saved downloaded contents to {path}")


def character_width(symbol: str) -> int:  # pragma: no cover
    """
    Determine the display width of a character based on its Unicode East Asian Width property.

    Parameters:
        symbol (str): The character to measure.

    Returns:
        int: The width of the character (1 or 2).
    """
    import unicodedata

    return 2 if unicodedata.east_asian_width(symbol) in ("F", "N", "W") else 1


def trunc_printable(value: str, width: int, full_line: bool = True) -> str:  # pragma: no cover
    """
    Truncate a string to fit within a specified width, accounting for character widths.

    Parameters:
        value (str): The string to truncate.
        width (int): The maximum display width.
        full_line (bool): Whether to pad the string to the full width. Defaults to True.

    Returns:
        str: The truncated string.
    """
    if not isinstance(value, str):
        value = str(value)

    offset = 0
    emit = ""
    ignoring = False

    for char in value:
        if char == "\n":
            emit += "↵"
            offset += 1
            continue
        if char == "\r":
            continue
        emit += char
        if char == "\033":
            ignoring = True
        if not ignoring:
            offset += character_width(char)
        if ignoring and char == "m":
            ignoring = False
        if not ignoring and offset >= width:
            return emit + "\033[0m"
    line = emit + "\033[0m"
    if full_line:
        return line + " " * (width - offset)
    return line


def run_tests():  # pragma: no cover
    """
    Discover and run test functions defined in the calling module. Test functions should be named starting with 'test_'.

    This function captures the output of each test, reports pass/fail status, and provides detailed error information if a test fails.

    Returns:
        None
    """
    import contextlib
    import inspect
    import os
    import shutil
    import time
    import traceback
    from io import StringIO

    OS_SEP = os.sep

    manual_test = os.environ.get("MANUAL_TEST")
    os.environ["MANUAL_TEST"] = "1"

    display_width = shutil.get_terminal_size((100, 20))[0]

    # Get the calling module
    caller_module = inspect.getmodule(inspect.currentframe().f_back)
    test_methods = []
    for name, obj in inspect.getmembers(caller_module):
        if inspect.isfunction(obj) and name.startswith("test_"):
            test_methods.append(obj)

    print(f"\n\033[38;2;139;233;253m\033[3mRUNNING SET OF {len(test_methods)} TESTS\033[0m\n")
    start_suite = time.monotonic_ns()

    passed = 0
    failed = 0

    for index, method in enumerate(test_methods):
        start_time = time.monotonic_ns()
        test_name = f"\033[38;2;255;184;108m{(index + 1):04}\033[0m \033[38;2;189;147;249m{str(method.__name__)}\033[0m"
        print(test_name.ljust(display_width - 20), end="", flush=True)
        error = None
        output = ""
        try:
            stdout = StringIO()  # Create a StringIO object
            with contextlib.redirect_stdout(stdout):
                method()
            output = stdout.getvalue()
        except Exception as err:
            error = err
        finally:
            if error is None:
                passed += 1
                status = "\033[38;2;26;185;67m pass"
            else:
                failed += 1
                status = "\033[38;2;255;121;198m fail"
        time_taken = int((time.monotonic_ns() - start_time) / 1e6)
        print(f"\033[0;32m{str(time_taken).rjust(8)}ms {status}\033[0m")
        if error:
            traceback_details = traceback.extract_tb(error.__traceback__)
            file_name, line_number, function_name, code_line = traceback_details[-1]
            file_name = file_name.split(OS_SEP)[-1]
            print(
                f"  \033[38;2;255;121;198m{error.__class__.__name__}\033[0m"
                + f" {error}\n"
                + f"  \033[38;2;241;250;140m{file_name}\033[0m"
                + "\033[38;2;98;114;164m:\033[0m"
                + f"\033[38;2;26;185;67m{line_number}\033[0m"
                + f" \033[38;2;98;114;164m{code_line}\033[0m"
            )
        if output:
            print(
                "\033[38;2;98;114;164m"
                + "=" * display_width
                + "\033[0m"
                + output.strip()
                + "\n"
                + "\033[38;2;98;114;164m"
                + "=" * display_width
                + "\033[0m"
            )

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )



@lru_cache_with_expiry
def set_up_iceberg():
    """
    Set up a local Iceberg catalog for testing with NVD data.

    Parameters:
        parquet_files: List[str]
            List of paths to Parquet files partitioned by CVE date.
        base_path: str, optional
            Path to create the Iceberg warehouse, defaults to '/tmp/iceberg_nvd'.

    Returns:
        str: Path to the created Iceberg table.
    """
    import pyarrow
    import opteryx
    from pyiceberg.catalog.sql import SqlCatalog
    from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
    from opteryx.connectors.iceberg_connector import IcebergConnector

    worker_id = os.environ.get('PYTEST_XDIST_WORKER', 'gw0')
    ICEBERG_BASE_PATH: str = f"tmp/iceberg/{worker_id}"

    def cast_dataset(dataset):
        for i, column in enumerate(dataset.column_names):

            field = dataset.schema.field(column)

            if pyarrow.types.is_date64(field.type):
                dataset = dataset.set_column(
                    dataset.schema.get_field_index(column),
                    column,
                    pyarrow.compute.cast(dataset[column], pyarrow.timestamp('ms'))
                )

            if pyarrow.types.is_decimal(field.type):
                column_data = dataset.column(field.name)
                
                # Extract values as float, scale them up, and convert to integers
                values = column_data.to_numpy(zero_copy_only=False)
                #int_values = [int(float(v) * (10**scale)) if v is not None else None for v in values]
                float_values = [float(v)  if v is not None else None for v in values]
                # Create INT array (no need to convert back to decimal)
                float_array = pyarrow.array(float_values, type=pyarrow.float64())
                
                # Replace column directly with the int array
                dataset = dataset.set_column(i, field.name, float_array)


        return dataset

    existing = os.path.exists(ICEBERG_BASE_PATH)

    os.makedirs(ICEBERG_BASE_PATH, exist_ok=True)

    # Step 1: Create a local Iceberg catalog
    catalog = SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{ICEBERG_BASE_PATH}/pyiceberg_catalog.db",
            "warehouse": f"file://{ICEBERG_BASE_PATH}",
        },
    )

    needs_setup = True
    if existing:
        try:
            catalog.load_table("opteryx.planets")
            catalog.load_table("opteryx.tweets")
            needs_setup = False
        except NoSuchTableError:
            needs_setup = True

    if not needs_setup:
        # Ensure a pair of convenience test tables exist even if catalog is already
        # initialized by a prior run. These are used in battery tests.
        from pyiceberg.exceptions import NoSuchTableError as _NoSuchTableError
        import pyarrow as _pa

        def _ensure_table_empty(identifier):
            try:
                catalog.load_table(identifier)
            except _NoSuchTableError:
                schema = _pa.schema([_pa.field("id", _pa.int64()), _pa.field("name", _pa.string())])
                catalog.create_table(identifier, schema=schema)

        def _ensure_single_snapshot(identifier):
            try:
                catalog.load_table(identifier)
            except _NoSuchTableError:
                schema = _pa.schema([_pa.field("id", _pa.int64()), _pa.field("name", _pa.string())])
                table = catalog.create_table(identifier, schema=schema)
                data = _pa.Table.from_arrays([_pa.array([1, 2, 3]), _pa.array(["a", "b", "c"])], schema=schema)
                table.append(data)

        def _ensure_two_snapshots(identifier):
            try:
                catalog.load_table(identifier)
            except _NoSuchTableError:
                schema = _pa.schema([_pa.field("id", _pa.int64()), _pa.field("name", _pa.string())])
                table = catalog.create_table(identifier, schema=schema)
                data1 = _pa.Table.from_arrays([_pa.array([1, 2]), _pa.array(["a", "b"])], schema=schema)
                table.append(data1)
                data2 = _pa.Table.from_arrays([_pa.array([1, 2, 3]), _pa.array(["a", "b", "c"])], schema=schema)
                table.overwrite(data2)

        _ensure_table_empty("opteryx.empty_battery")
        _ensure_single_snapshot("opteryx.single_snap_battery")
        _ensure_two_snapshots("opteryx.two_snap_battery")
        return catalog

    with contextlib.suppress(NamespaceAlreadyExistsError):
        catalog.create_namespace("opteryx")

    _epoch_schema = pyarrow.schema([
        pyarrow.field("epoch", pyarrow.timestamp("ms", tz="UTC"))
    ])
    _epoch_table = pyarrow.Table.from_arrays(
        [pyarrow.array([datetime.datetime.now(datetime.timezone.utc)], type=_epoch_schema.field("epoch").type)],
        schema=_epoch_schema
    )
    if not catalog.table_exists("opteryx.epoch"):
        catalog.create_table("opteryx.epoch", schema=_epoch_schema).append(_epoch_table)

    data = opteryx.query_to_arrow("SELECT tweet_id, text, timestamp, user_id, user_verified, user_name, hash_tags, followers, following, tweets_by_user, is_quoting, is_reply_to, is_retweeting FROM testdata.flat.formats.parquet")
    table = catalog.create_table("opteryx.tweets", schema=data.schema)
    table.append(data.slice(0, 50000))
    table.append(data.slice(50000, 50000))

    opteryx.register_store(
        "iceberg", IcebergConnector, catalog=catalog, remove_prefix=True
    )

    modern_timestamp = datetime.datetime(2025, 11, 18, 21, 6, 21, 210000)

    for dataset in ('planets', 'satellites', 'missions', 'astronauts'):
        if dataset == 'planets':
            from opteryx.virtual_datasets import planet_data

            def snapshot_props(label, cutoff):
                props = {"temporal_version": label}
                if cutoff is not None:
                    props["end_date"] = cutoff.isoformat()
                else:
                    props["end_date"] = "current"
                return props

            def load_planet_snapshot(cutoff):
                table_data = planet_data.read() if cutoff is None else planet_data.read(end_date=cutoff)
                return cast_dataset(table_data)

            snapshots = [
                ("pre_uranus", datetime.datetime(1781, 4, 25)),
                ("pre_neptune", datetime.datetime(1846, 11, 12)),
                ("pre_pluto", datetime.datetime(1930, 3, 12)),
                ("modern", modern_timestamp),
            ]

            snapshot_label, cutoff = snapshots[0]
            snapshot_data = load_planet_snapshot(cutoff)
            table = catalog.create_table("opteryx.planets", schema=snapshot_data.schema)
            if cutoff is None:
                table.append(snapshot_data, snapshot_properties=snapshot_props(snapshot_label, cutoff))
            else:
                table.append(snapshot_data, snapshot_properties=snapshot_props(snapshot_label, cutoff))

            latest_snapshot = snapshot_data
            for snapshot_label, cutoff in snapshots[1:]:
                snapshot_data = load_planet_snapshot(cutoff if cutoff is not modern_timestamp else None)
                if cutoff is None or cutoff is modern_timestamp:
                    table.overwrite(snapshot_data, snapshot_properties=snapshot_props(snapshot_label, None))
                else:
                    table.overwrite(snapshot_data, snapshot_properties=snapshot_props(snapshot_label, cutoff))
                latest_snapshot = snapshot_data

            expected_rows = latest_snapshot.num_rows
            del latest_snapshot
            del snapshot_data

            iceberged = opteryx.query("SELECT * FROM iceberg.opteryx.planets")
            assert iceberged.rowcount == expected_rows
            del iceberged  # Free memory immediately
            continue

        data = opteryx.query_to_arrow(f"SELECT * FROM ${dataset}")
        data = cast_dataset(data)

        table = catalog.create_table(f"opteryx.{dataset}", schema=data.schema)
        table.append(data)

        # Verify row count without loading full result set into memory
        expected_rows = data.num_rows
        del data  # Free memory immediately
        
        iceberged = opteryx.query(f"SELECT * FROM iceberg.opteryx.{dataset}")
        assert iceberged.rowcount == expected_rows
        del iceberged  # Free memory immediately


    # Create additional tables that tests rely on (empty and single snapshot)
    from pyiceberg.exceptions import NoSuchTableError as _NoSuchTableError
    import pyarrow as _pa

    def _ensure_table_empty(identifier):
        try:
            catalog.load_table(identifier)
        except _NoSuchTableError:
            schema = _pa.schema([_pa.field("id", _pa.int64()), _pa.field("name", _pa.string())])
            catalog.create_table(identifier, schema=schema)

    def _ensure_single_snapshot(identifier):
        try:
            catalog.load_table(identifier)
        except _NoSuchTableError:
            schema = _pa.schema([_pa.field("id", _pa.int64()), _pa.field("name", _pa.string())])
            table = catalog.create_table(identifier, schema=schema)
            data = _pa.Table.from_arrays([_pa.array([1, 2, 3]), _pa.array(["a", "b", "c"])], schema=schema)
            table.append(data)

    def _ensure_two_snapshots(identifier):
        try:
            catalog.load_table(identifier)
        except _NoSuchTableError:
            schema = _pa.schema([_pa.field("id", _pa.int64()), _pa.field("name", _pa.string())])
            table = catalog.create_table(identifier, schema=schema)
            data1 = _pa.Table.from_arrays([_pa.array([1, 2]), _pa.array(["a", "b"])], schema=schema)
            table.append(data1)
            data2 = _pa.Table.from_arrays([_pa.array([1, 2, 3]), _pa.array(["a", "b", "c"])], schema=schema)
            table.overwrite(data2)

    _ensure_table_empty("opteryx.empty_battery")
    _ensure_single_snapshot("opteryx.single_snap_battery")
    _ensure_two_snapshots("opteryx.two_snap_battery")

    return catalog
