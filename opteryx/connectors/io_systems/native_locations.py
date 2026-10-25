# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Where a native whole-file Source reads each of a scan's files from
(docs/REMOTE_AVRO_ORC_DATA_FILES_DESIGN.md).

The native Avro and ORC Sources map a local file or fetch a remote one whole with one GET.
This resolves, once per scan and at compile time, the URL each remote file is fetched from
and the credential that authenticates the fetch - exactly one of a signed URL or an
`Authorization` header, never both and never neither, which is the rule the Parquet
scan path already holds (`pool_reader._native_auth_header`). Which one is the FILESYSTEM'S
declaration, not this module's: GCS sends a bearer header (signing is its opt-in), S3 always
signs. A new filesystem gets the right behaviour by declaring its own mechanism.
"""

from typing import List
from typing import Tuple

from opteryx.exceptions import NotSupportedError
from opteryx.exceptions import md_code

# The schemes a catalog table's data files can be read from natively.
REMOTE_SCHEMES = ("gs", "s3")


def remote_files(files: List[str]) -> List[str]:
    """The files that need a remote filesystem to be read: those on a scheme the Sources
    read natively. (Other schemes are refused by `native_locations`, naming them.)"""
    return [file for file in files if file.partition("://")[0] in REMOTE_SCHEMES and "://" in file]


def function_scan_urls(function: str, path: str, files: List[str], credentialed_filesystem) -> List[str]:
    """Where a READ_* FUNCTION scan reads each of `files` from: a list parallel to it holding "" for
    a local path (memory-mapped) or the URL to GET. THE RULE (docs/ANONYMOUS_REMOTE_READS_DESIGN.md
    §0): a READ_* function NEVER uses the platform's credentials. A user-supplied gs:// / s3:// path
    is rewritten to its public object URL by the anonymous filesystems and fetched with no
    credential at all; the store's own ACL decides. Only with `credentials =>` is a file fetched as
    a short-lived URL signed with the CUSTOMER's stored secret (the binder checked every file
    against its scope), through `credentialed_filesystem`. Raises NotSupportedError for anything
    else, before any byte is read."""
    if credentialed_filesystem is not None:
        return [credentialed_filesystem.rewrite_to_signed_url(file, expiry_seconds=900) for file in files]
    protocol = path.split("://")[0] if "://" in path else ""
    if protocol == "":
        return ["" for _ in files]
    if protocol in ("http", "https"):
        return list(files)
    if protocol == "gs":
        from opteryx.connectors.io_systems.anonymous_gcs_filesystem import anonymous_gcs_filesystem

        filesystem = anonymous_gcs_filesystem()
    elif protocol == "s3":
        from opteryx.connectors.io_systems.anonymous_s3_filesystem import anonymous_s3_filesystem

        filesystem = anonymous_s3_filesystem()
    else:
        raise NotSupportedError(
            f"{function}('{path}'): '{protocol}://' is not a supported scheme; "
            f"{function} reads local paths, http(s):// URLs and gs:// / s3:// objects."
        )
    if filesystem.native_auth_header() is not None:
        raise NotSupportedError(f"{function}('{path}'): an authenticated read is not supported.")
    return [filesystem.rewrite_to_signed_url(file) for file in files]


def native_locations(filesystem, files: List[str], dataset: str) -> Tuple[List[str], str]:
    """(urls, auth_header) for `files`, the dataset's data files.

    `urls` is parallel to `files`: "" for a local path (memory-mapped), else the URL to GET.
    `auth_header` is the one `Authorization` value every remote GET carries, "" when the
    URLs are signed. Raises NotSupportedError, naming the file, for a scheme the Sources do
    not read, for a dataset mixing local and remote files, and for a remote filesystem that
    declares neither a signing hook nor a header (an anonymous request would be refused by
    the store and must not be sent)."""
    schemes = [file.partition("://")[0] if "://" in file else "" for file in files]
    for file, scheme in zip(files, schemes):
        if scheme not in ("",) + REMOTE_SCHEMES:
            raise NotSupportedError(
                f"The data file {md_code(file)} of {dataset} uses the scheme '{scheme}://'; "
                f"Avro and ORC data files are read from local paths and {', '.join(s + '://' for s in REMOTE_SCHEMES)} only."
            )
    if all(scheme == "" for scheme in schemes):
        return ["" for _ in files], ""
    if any(scheme == "" for scheme in schemes):
        raise NotSupportedError(
            f"{dataset} mixes local and remote data files, which is not supported."
        )

    if filesystem.signs_urls:
        return [filesystem.rewrite_to_signed_url(file) for file in files], ""
    header = filesystem.native_auth_header()
    if not header:
        raise NotSupportedError(
            f"The filesystem for {dataset} declares neither signed URLs nor an authorization "
            "header, so its remote data files cannot be read natively."
        )
    return [filesystem.object_url(file) for file in files], header
