"""ORC oracle: PyArrow (pyarrow.orc) is the independent reader."""

import io

import pyarrow as pa
import pyarrow.orc as orc

from . import arrow_codec

READER = {"pyarrow": pa.__version__}


def inspect(data: bytes) -> dict:
    f = orc.ORCFile(io.BytesIO(data))
    stripes = [{"index": i, "num_rows": f.read_stripe(i).num_rows} for i in range(f.nstripes)]
    table = f.read()
    return {
        "file": {
            "num_rows": f.nrows,
            "num_stripes": f.nstripes,
            "compression": f.compression,
            "compression_block_size": f.compression_size,
            "writer": f.writer,
            "writer_version": f.writer_version,
            "file_version": f.file_version,
            "software_version": f.software_version,
            "row_index_stride": f.row_index_stride,
            "content_length": f.content_length,
            "file_length": f.file_length,
            "file_footer_length": f.file_footer_length,
            "file_postscript_length": f.file_postscript_length,
            "stripe_statistics_count": f.nstripe_statistics,
            "stripe_statistics_length": f.stripe_statistics_length,
            "user_metadata": {k.decode("utf-8"): v.decode("utf-8") for k, v in f.metadata.items()},
        },
        "schema": {"fields": arrow_codec.describe_schema(table.schema)},
        "stripes": stripes,
        "columns": arrow_codec.encode_table_columns(table),
    }
