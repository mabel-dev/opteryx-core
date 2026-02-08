import pytest

from io import BytesIO

from opteryx.connectors.filesystem_connector import FileSystemTable
from opteryx.models.query_telemetry import QueryTelemetry


class FakeStreamWithFilter(BytesIO):
    def __init__(self, data: bytes, filters_applied: bool):
        super().__init__(data)
        # expose memoryview as existing callers expect
        self.memoryview = memoryview(data)
        self.filters_applied = filters_applied


def test_read_blob_omits_selection_when_filesystem_applied_filters():
    telemetry = QueryTelemetry("test_fs_sel")

    class FakeFS:
        def open_input_file(self, blob_name, columns=None, filters=None):
            return FakeStreamWithFilter(b"abc", filters_applied=True)

    # decoder that asserts selection is None
    def decoder(buf, projection=None, selection=None, just_schema=False):
        assert selection is None
        return (1, 1, len(buf), b"ignored")

    fs = FakeFS()
    table = FileSystemTable(dataset="some/path", filesystem=fs, storage_type="TEST", telemetry=telemetry)

    # should not raise assertion inside decoder
    table.read_blob(blob_name="some/path/file.parquet", decoder=decoder, projection=None, selection=[("a", "=", 1)])


def test_read_blob_passes_selection_when_filesystem_did_not_apply_filters():
    telemetry = QueryTelemetry("test_fs_sel2")

    class FakeFS:
        def open_input_file(self, blob_name, columns=None, filters=None):
            return FakeStreamWithFilter(b"abc", filters_applied=False)

    def decoder(buf, projection=None, selection=None, just_schema=False):
        assert selection is not None
        return (1, 1, len(buf), b"ignored")

    fs = FakeFS()
    table = FileSystemTable(dataset="some/path", filesystem=fs, storage_type="TEST", telemetry=telemetry)

    table.read_blob(blob_name="some/path/file.parquet", decoder=decoder, projection=None, selection=[("a", "=", 1)])
