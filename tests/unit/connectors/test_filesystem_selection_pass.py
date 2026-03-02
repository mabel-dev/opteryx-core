import sys
import types
from io import BytesIO

from opteryx.connectors.filesystem_connector import FileSystemTable
from opteryx.models.query_telemetry import QueryTelemetry


class FakeStreamWithFilter(BytesIO):
    def __init__(self, data: bytes, filters_applied: bool):
        super().__init__(data)
        # expose memoryview as existing callers expect
        self.memoryview = memoryview(data)
        self.filters_applied = filters_applied
        self.was_closed = False

    def close(self):
        self.was_closed = True
        super().close()


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


def test_read_blob_closes_stream_after_decode():
    telemetry = QueryTelemetry("test_fs_sel3")
    opened = {}

    class FakeFS:
        def open_input_file(self, blob_name, columns=None, filters=None):
            stream = FakeStreamWithFilter(b"abc", filters_applied=False)
            opened["stream"] = stream
            return stream

    def decoder(buf, projection=None, selection=None, just_schema=False):
        return (1, 1, len(buf), b"ignored")

    fs = FakeFS()
    table = FileSystemTable(dataset="some/path", filesystem=fs, storage_type="TEST", telemetry=telemetry)

    table.read_blob(blob_name="some/path/file.parquet", decoder=decoder, projection=None, selection=None)

    assert opened["stream"].was_closed is True


def test_get_dataset_metadata_closes_stream(monkeypatch):
    telemetry = QueryTelemetry("test_fs_sel4")
    opened = {}

    class FakeMetadataStream(FakeStreamWithFilter):
        def size(self):
            return len(self.memoryview)

    class FakeFS:
        def open_input_file(self, blob_name, columns=None, filters=None):
            stream = FakeMetadataStream(b"abc", filters_applied=False)
            opened["stream"] = stream
            return stream

    fake_parquet = types.SimpleNamespace(
        ParquetFile=lambda stream: types.SimpleNamespace(
            metadata=types.SimpleNamespace(num_rows=7)
        )
    )
    monkeypatch.setitem(sys.modules, "pyarrow.parquet", fake_parquet)

    fs = FakeFS()
    table = FileSystemTable(dataset="some/path", filesystem=fs, storage_type="TEST", telemetry=telemetry)
    table.get_dataset_schema = lambda: types.SimpleNamespace(columns=[])
    table.get_list_of_blob_names = lambda prefix: ["some/path/file.parquet"]

    _, manifest = table.get_dataset_metadata()

    assert manifest.get_file_count() == 1
    assert opened["stream"].was_closed is True
