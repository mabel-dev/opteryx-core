"""
S3 filesystem implementation using Opteryx's optimized I/O.

This implements pyarrow.fs.FileSystem interface but uses Opteryx's
stream wrappers for high-performance S3 access.
"""

import io
import os
from typing import List
from typing import Union

from opteryx.exceptions import MissingDependencyError
from opteryx.exceptions import UnmetRequirementError


class S3File(io.BytesIO):
    """
    File-like wrapper for S3 objects.

    Reads the entire object into memory on open for maximum performance.
    """

    def __init__(self, path: str, minio_client):
        """Initialize S3 file by reading entire object."""
        from opteryx.utils import paths

        bucket, object_path, name, extension = paths.get_parts(path)
        stream = None
        try:
            stream = minio_client.get_object(
                bucket_name=bucket, object_name=object_path + "/" + name + extension
            )
            content = stream.read()
            # Initialize BytesIO with the content
            super().__init__(content)
        finally:
            if stream:
                stream.close()


class OpteryxS3FileSystem:
    """
    Custom S3 filesystem using MinIO client for optimal performance.

    Supports both AWS S3 and MinIO-compatible storage. Provides Arrow-compatible
    filesystem interface via duck typing.
    """

    def __init__(self, bucket=None, region=None, **kwargs):
        self.bucket = bucket
        self.region = region

        try:
            from minio import Minio  # type:ignore
        except ImportError as err:  # pragma: no cover
            raise MissingDependencyError(err.name) from err

        # fmt:off
        end_point = kwargs.get("S3_END_POINT", os.environ.get("MINIO_END_POINT"))
        access_key = kwargs.get("S3_ACCESS_KEY", os.environ.get("MINIO_ACCESS_KEY"))
        secret_key = kwargs.get("S3_SECRET_KEY", os.environ.get("MINIO_SECRET_KEY"))
        secure = kwargs.get("S3_SECURE", str(os.environ.get("MINIO_SECURE", "TRUE")).lower() == "true")
        # fmt:on

        if end_point is None:  # pragma: no cover
            raise UnmetRequirementError(
                "MinIo (S3) adapter requires MINIO_END_POINT, MINIO_ACCESS_KEY and MINIO_SECRET_KEY set in environment variables."
            )

        # Minio v7 uses keyword-only args for construction (endpoint=...).
        try:
            self.minio = Minio(
                end_point, access_key=access_key, secret_key=secret_key, secure=secure
            )
        except TypeError:
            # Fall back to positional args for older Minio versions.
            self.minio = Minio(end_point, access_key, secret_key, secure=secure)

    def get_file_info(self, paths: Union[str, List[str]]):
        """Get info about S3 objects."""
        from pyarrow.fs import FileInfo
        from pyarrow.fs import FileType

        # Handle both single path and list of paths
        single_path = isinstance(paths, str)
        if single_path:
            paths = [paths]

        infos = []
        for path in paths:
            from opteryx.utils import paths as path_utils

            bucket, object_path, name, extension = path_utils.get_parts(path)
            full_object_name = object_path + "/" + name + extension

            try:
                stat = self.minio.stat_object(bucket_name=bucket, object_name=full_object_name)
                info = FileInfo(path=path, type=FileType.File, size=stat.size)
            except:
                info = FileInfo(path=path, type=FileType.NotFound)
            infos.append(info)

        return infos[0] if single_path else infos

    def open_input_stream(self, path: str):
        """Open an S3 object for reading as a stream."""
        return S3File(path, self.minio)

    def open_input_file(self, path: str):
        """Open an S3 object for random access reading."""
        return S3File(path, self.minio)
