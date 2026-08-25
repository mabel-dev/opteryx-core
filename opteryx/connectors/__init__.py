# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""
Data Source Connectors with Resolution-First Lookup

This module provides connectors to various data sources, enabling Opteryx to query
data from files, databases, cloud storage, and other systems. Connectors are lazily
loaded to improve startup performance and reduce memory footprint.

Architecture:
Connectors abstract different data sources behind a common interface (BaseConnector),
allowing the query engine to work with any data source transparently. Each connector
is responsible for:
- Reading data and converting it to PyArrow format
- Providing schema information
- Supporting predicate pushdown when possible
- Handling authentication and connection management

Which connector serves a dataset is decided per lookup by a RESOLUTION CHAIN,
walked in `connector_factory` — first answer wins:

1. Static table — entries from `register_workspace(prefix, connector, **kwargs)`.
   For embedded use, tests, and deployments that want import-time wiring.
2. Installed resolver — `set_workspace_resolver(fn)`. `fn(workspace)` returns a
   `Resolution` (connector + arbitrary nested config + version) or None. This is
   how a deployment binds workspaces to catalogs from data (a registry) instead
   of code: the resolver runs on every lookup, so config changes go live without
   re-registration or redeploy. Resolver exceptions PROPAGATE — a resolver that
   fails must fail the query, never silently fall through to another slot, which
   would route a workspace's query at the wrong catalog.
3. Static default — `set_default_connector(connector, **kwargs)`.
4. Local disk — the terminal fallback, always available.

That chain answers ONE question: which connector serves a workspace's DATA.
Which catalog holds a workspace's SETTINGS and lifecycle is a SEPARATE
question with a separate resolver - `set_workspace_settings_resolver` - read
through `workspace_settings_connector`. The settings answer is always the
opteryx catalog entry, whatever the data is bound to, and never needs a
binding's stored credential. Workspace-scoped DDL (`ALTER WORKSPACE`, `DROP
WORKSPACE`) asks the second question; everything else asks the first. Do not
collapse them back into one call - see `set_workspace_settings_resolver` for
what that cost last time.

Connector instances are cached per resolved key (the workspace/prefix, or
"_default"/"_disk" for the shared fallbacks) and validated by a VERSION compare
on every lookup: the resolution's version (or, absent one, a fingerprint of the
resolved config) must match the version the cached instance was built with,
otherwise the instance is rebuilt and replaces the cache entry. A config change
therefore rotates the connector on the next lookup with no cross-process
signaling — and nothing hashes the config, so config values may be arbitrarily
nested.

System metadata (information_schema) is a reserved nested schema addressed
as `<workspace>.information_schema.<table>` and served by OpteryxConnector
itself (see opteryx/connectors/information_schema.py) - not a separate
top-level connector prefix.

Legacy Compatibility:
The following names are supported for backward compatibility and map to FileSystemConnector:
- DiskConnector: Local file system access
- GcpCloudStorageConnector: Google Cloud Storage

Lazy Loading:
Connectors are only imported when actually needed, which significantly improves
module import time. The lazy loading is transparent to users - all import patterns
work normally, but the actual connector classes are loaded on first access.

Usage Patterns:

1. Direct Import:
   from opteryx.connectors import FileSystemConnector

2. Registration (the connector is UNINSTANTIATED - a class or factory):
   opteryx.register_workspace("my_prefix", create_gcs_connector, bucket="my-bucket")

3. Query Usage - the prefix is the first segment of the relation name:
   opteryx.session().execute_to_morsels("SELECT * FROM my_prefix.my_dataset")

   A bucket URL is NOT a valid relation name: `FROM gs://bucket/file.parquet` is
   a syntax error. Register a prefix as above, or name the file directly with
   `read_parquet('...')`.

4. Dynamic resolution (a deployment-owned registry):
   from opteryx.connectors import Resolution, set_workspace_resolver

   def resolve(workspace: str) -> Resolution | None:
       binding = my_registry.read(workspace)          # e.g. one Firestore doc get
       if binding is None:
           return None                                # fall through to slots 3/4
       return Resolution(
           connector=OpteryxConnector,
           config={"catalog": SomeMetastore, **binding.config},  # nesting is fine
           version=binding.version,                   # bumped on registry writes
       )

   set_workspace_resolver(resolve)

Connector Development:
1. Inherit from BaseConnector
2. Implement required methods (read_dataset, get_dataset_schema)
3. Add optional optimizations (predicate pushdown, column pruning)
4. Register with appropriate prefixes
5. Add comprehensive tests

Example Custom Connector:
    class MyConnector(BaseConnector):
        def read_dataset(self, dataset, **kwargs):
            # Yield Draken Morsels (no pyarrow in the engine)
            yield morsel

        def get_dataset_schema(self, dataset):
            # Return a RelationSchema
            return RelationSchema(...)

Performance Considerations:
- Implement predicate pushdown to reduce data transfer
- Support column pruning for wide tables
- Use async operations for I/O bound connectors
- Cache schema information when appropriate
- Consider connection pooling for database connectors
"""

# Lazy imports - connectors are only loaded when actually needed
# This significantly improves module import time from ~500ms to ~130ms

import re

from enum import Enum

# load the base set of prefixes
# fmt:off


class TableType(str, Enum):
    """Enum representing the type of object in a data catalog"""
    Table = "Table"
    View = "View"



# Slot 1: static registrations (register_workspace). Shape per entry:
# {"connector": class_or_factory, "prefix": prefix, **kwargs}
_storage_prefixes = {}

# Cache of connector INSTANCES, keyed by resolved key (workspace/prefix string,
# or "_default"/"_disk"). Values are the instances themselves - tests iterate
# and clear this directly, so keep values as bare instances.
_connector_cache = {}

# The version each cached instance was built against, same keys as
# _connector_cache. Kept separate (not a (version, instance) tuple) so
# _connector_cache.values() stays an iterable of connector instances.
_connector_versions = {}

# Slot 3: default connector configuration ({"connector": cls, **kwargs})
_default_connector = None

# Slot 2: the installed resolver, or None
_workspace_resolver = None

# The SETTINGS resolver, or None. Deliberately separate from
# `_workspace_resolver`: see `set_workspace_settings_resolver`.
_workspace_settings_resolver = None
# fmt:on

# A workspace segment the resolver is worth consulting for. Protocol-style
# names (gs://bucket/path) and other non-identifiers skip the resolver and
# fall through to the default/disk slots, exactly as they always have.
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


__all__ = (
    # Core connectors
    "OpteryxConnector",
    "OpteryxTable",
    "FileSystemConnector",
    "MabelConnector",
    # Factory functions for filesystem connectors
    "create_local_connector",
    "create_gcs_connector",
    "create_local_mabel_connector",
    "create_gcs_mabel_connector",
    # Utilities
    "set_default_connector",
    "set_workspace_resolver",
    "set_workspace_settings_resolver",
    "workspace_settings_connector",
    "Resolution",
    "TableType",
    # Legacy names (backward compatibility) - map to factories
    "DiskConnector",
    "GcpCloudStorageConnector",
)


class Resolution:
    """One answer to "what backs this workspace?".

    `connector` is an uninstantiated class or factory (same contract as
    `register_workspace`). `config` is an ordinary dict, arbitrarily nested -
    nothing hashes it - passed to the connector as keyword arguments.
    `version` identifies the config revision the answer came from (e.g. a
    registry document's version field); when it changes, the cached connector
    instance is rebuilt. Leave it None to version by config fingerprint, which
    rebuilds whenever the resolved config's repr changes.
    """

    __slots__ = ("connector", "config", "version")

    def __init__(self, connector, config: dict | None = None, version=None):
        if not (isinstance(connector, type) or callable(connector)):
            raise ValueError(
                "Resolution.connector must be uninstantiated (a class or factory function)."
            )
        self.connector = connector
        self.config = dict(config or {})
        self.version = version


def register_workspace(prefix, connector, **kwargs):
    """Register a connector for a specific prefix."""
    # Accept both uninstantiated classes and factory functions
    if not (isinstance(connector, type) or callable(connector)):
        raise ValueError(
            "connectors registered with `register_workspace` must be uninstantiated (a class or factory function)."
        )

    # Store connector class/factory directly (not as a string)
    _storage_prefixes[prefix] = {
        "connector": connector,  # type: ignore
        "prefix": prefix,
        **kwargs,
    }


def set_default_connector(connector, **kwargs):
    """
    Set the default connector to use when no prefix matches.

    Args:
        connector: Connector class to use as default
        **kwargs: Configuration parameters for the connector

    Example:
        set_default_connector(OpteryxConnector,
                            catalog=FirestoreCatalog,
                            firestore_project="my-project",
                            ...)
    """
    global _default_connector

    if not isinstance(connector, type):
        raise ValueError("Default connector must be an uninstantiated class.")

    _default_connector = {
        "connector": connector,
        **kwargs,
    }


def set_workspace_resolver(resolver) -> None:
    """Install (or, with None, remove) the workspace resolver.

    `resolver(workspace)` is called on every lookup that no static
    registration answered, with the first dot-segment of the dataset name.
    It returns a `Resolution`, or None to fall through to the default/disk
    slots. Exceptions it raises propagate to the caller - by design, a
    resolver failure fails the query rather than mis-routing it.
    """
    global _workspace_resolver

    if resolver is not None and not callable(resolver):
        raise ValueError("workspace resolver must be callable (or None to remove it).")

    _workspace_resolver = resolver


def set_workspace_settings_resolver(resolver) -> None:
    """Install (or, with None, remove) the workspace SETTINGS resolver.

    There are two different questions about a workspace, and they must not
    share a resolver:

    - Which connector serves this workspace's DATA? Depends on the workspace's
      external binding, and answering it may require that binding's stored
      credential. That is `set_workspace_resolver`.
    - Which catalog holds this workspace's SETTINGS and lifecycle? Always the
      opteryx catalog entry, whatever the data is bound to, and answering it
      needs no credential ever. That is this resolver.

    They were once one call, which meant `ALTER WORKSPACE` decrypted an
    external catalog's credential to reach a property the opteryx catalog
    entry owns - so a workspace whose stored credential had gone bad could
    not be repaired or dropped through SQL at all, the repairing statement
    itself dying at bind time. Two resolvers make that conflation
    unspellable rather than merely fixed.

    `resolver(workspace)` returns a `Resolution` or None; None falls through
    the same way the data resolver's does. Exceptions propagate.
    """
    global _workspace_settings_resolver

    if resolver is not None and not callable(resolver):
        raise ValueError(
            "workspace settings resolver must be callable (or None to remove it)."
        )

    _workspace_settings_resolver = resolver


def workspace_settings_connector(workspace_name: str, telemetry):
    """The connector that owns `workspace_name`'s settings and lifecycle.

    For workspace-SCOPED statements only - `ALTER WORKSPACE`, `DROP
    WORKSPACE`. Relation-scoped DDL keeps going through `connector_factory`,
    and must: routing `CREATE TABLE` at a bound workspace's own metastore is
    what makes that metastore refuse the write, which is what enforces the
    rule that an externally-bound workspace never domiciles opteryx datasets.

    With no settings resolver installed this defers to `connector_factory`.
    That is not a fallback but the same answer arrived at cheaply: a
    deployment with no settings resolver has no external bindings either, so
    the data connector and the settings connector are the same object and no
    credential is in play. A deployment that installs a DATA resolver without
    a settings resolver is a different matter - it has bindings but no
    settings routing, which is exactly the bug this seam exists to prevent -
    and is refused here rather than silently reintroducing it.
    """
    if _workspace_settings_resolver is not None:
        resolution = _workspace_settings_resolver(workspace_name)
        if resolution is not None:
            if not isinstance(resolution, Resolution):
                raise ValueError(
                    "workspace settings resolver must return a Resolution or None, "
                    f"got {type(resolution)}"
                )
            # Namespaced away from connector_factory's cache: the same
            # workspace name legitimately resolves to a DIFFERENT connector
            # for settings than for data, and one key for both would hand
            # whichever asked second the other's answer.
            cache_key = f"$settings:{workspace_name}"
            version = resolution.version
            entry = dict(resolution.config)
            if version is None:
                version = _fingerprint(entry)
            cached = _connector_cache.get(cache_key)
            if cached is not None and _connector_versions.get(cache_key) == version:
                return cached
            build_entry = {key: value for key, value in entry.items() if key != "connector"}
            instance = _build_connector(resolution.connector, build_entry, telemetry)
            instance._matched_prefix = workspace_name
            _connector_cache[cache_key] = instance
            _connector_versions[cache_key] = version
            return instance

    if _workspace_resolver is not None:
        raise ValueError(
            "a workspace DATA resolver is installed but no settings resolver is - "
            "workspace properties and lifecycle live in the opteryx catalog entry, "
            "not in whatever external catalog a workspace's data is bound to, so "
            "resolving them through the data binding would decrypt a credential "
            "that has no bearing on the answer. Install one with "
            "set_workspace_settings_resolver()."
        )

    return connector_factory(workspace_name, telemetry=telemetry)


def create_local_connector(**kwargs):
    """
    Create a FileSystemConnector for local storage.

    Args:
        **kwargs: Additional parameters passed to FileSystemConnector

    Returns:
        FileSystemConnector configured for local storage
    """
    from opteryx.connectors.filesystem_connector import FileSystemConnector
    from opteryx.connectors.io_systems import OpteryxLocalFileSystem

    filesystem = OpteryxLocalFileSystem()
    return FileSystemConnector(filesystem=filesystem, storage_type="LOCAL", **kwargs)


def create_gcs_connector(bucket=None, **kwargs):
    """
    Create a FileSystemConnector for Google Cloud Storage.

    Args:
        bucket: GCS bucket name (optional)
        **kwargs: Additional parameters passed to FileSystemConnector

    Returns:
        FileSystemConnector configured for GCS
    """
    from opteryx.connectors.filesystem_connector import FileSystemConnector
    from opteryx.connectors.io_systems import OpteryxGcsFileSystem

    filesystem = OpteryxGcsFileSystem(bucket=bucket, **kwargs)
    return FileSystemConnector(filesystem=filesystem, storage_type="GCS", **kwargs)


def known_prefix(prefix) -> bool:
    return prefix in _storage_prefixes


def _fingerprint(entry: dict) -> str:
    """A stable-within-process identity for a resolved config.

    Used as the version for answers that don't carry one (static
    registrations, the static default, resolver Resolutions with
    version=None): if the resolved config's repr changes - a re-registration
    with different kwargs, a config value read fresh each call - the cached
    instance no longer matches and is rebuilt. reprs of classes and modules
    are stable; reprs of instances are id-based, which is also correct here
    (a new instance means the caller intends a new configuration).
    """
    return repr(sorted((key, repr(value)) for key, value in entry.items()))


def _build_connector(connector, entry: dict, telemetry):
    """Instantiate `connector` with `entry` as configuration.

    Handles the same three connector shapes registration always has: a legacy
    string name, a class, or a factory callable.
    """
    if isinstance(connector, str):
        if connector == "DiskConnector":
            from opteryx.connectors.filesystem_connector import FileSystemConnector
            from opteryx.connectors.io_systems import OpteryxLocalFileSystem

            filesystem = OpteryxLocalFileSystem()
            return FileSystemConnector(
                filesystem=filesystem, storage_type="LOCAL", telemetry=telemetry, **entry
            )
        if connector == "GcpCloudStorageConnector":
            from opteryx.connectors.filesystem_connector import FileSystemConnector
            from opteryx.connectors.io_systems import OpteryxGcsFileSystem

            filesystem = OpteryxGcsFileSystem(**entry)
            return FileSystemConnector(
                filesystem=filesystem, storage_type="GCS", telemetry=telemetry, **entry
            )
        # Unknown string connector - try __getattr__
        connector_class = __getattr__(connector)
        return connector_class(telemetry=telemetry, **entry)
    if isinstance(connector, type) or callable(connector):
        # A class is instantiated; a factory function is called - same shape.
        return connector(telemetry=telemetry, **entry)
    raise ValueError(f"Invalid connector type: {type(connector)}")


def connector_factory(dataset, telemetry, **config):
    """
    Get or create a connector instance for the given dataset's workspace.

    Walks the resolution chain (static table -> installed resolver -> static
    default -> local disk; see the module docstring), then reuses the cached
    connector instance for the resolved key if its version still matches,
    rebuilding it otherwise. Connectors are long-lived gateways to a catalog,
    cached per workspace/prefix, not per dataset.

    Args:
        dataset: The dataset reference (e.g., "catalog.schema.table")
        telemetry: Query telemetry object
        **config: Additional configuration

    Returns:
        A connector instance for the dataset's workspace
    """

    # if it starts with a $, it's a special internal dataset
    if dataset[0] == "$":
        from opteryx.connectors.virtual_data_connector import VirtualDataConnector

        # Virtual data connector is a gateway - it doesn't need dataset/telemetry
        # Those are passed when creating the table reader via table_engine()
        return VirtualDataConnector()

    connector = None
    entry: dict = {}
    cache_key = None
    version = None
    matched_prefix = None

    # Slot 1: static registrations. Same match rule as ever - the prefix
    # itself, or the prefix followed by a dot.
    for prefix, storage_details in _storage_prefixes.items():
        if dataset == prefix or dataset.startswith(prefix + "."):
            if isinstance(storage_details, dict):
                entry = {**config, **storage_details}
                connector = entry.get("connector")
            else:
                # storage_details is a string (connector class name)
                connector = storage_details
                entry = {**config, "prefix": prefix}
            cache_key = prefix
            matched_prefix = prefix
            break

    # Slot 2: the installed resolver, consulted with the first dot-segment -
    # but only for identifier-shaped names. Protocol paths (gs://...) and
    # other non-identifiers keep falling through to the default/disk slots.
    if connector is None and _workspace_resolver is not None:
        workspace = dataset.split(".", 1)[0]
        if _IDENTIFIER.match(workspace):
            resolution = _workspace_resolver(workspace)
            if resolution is not None:
                if not isinstance(resolution, Resolution):
                    raise ValueError(
                        "workspace resolver must return a Resolution or None, "
                        f"got {type(resolution)}"
                    )
                connector = resolution.connector
                entry = {**config, **resolution.config}
                cache_key = workspace
                version = resolution.version
                matched_prefix = workspace

    # Slot 3: the static default. One shared gateway instance, as always -
    # connectors like OpteryxConnector key their catalogs per workspace
    # internally, so per-workspace duplicates here would only multiply
    # clients without adding isolation.
    if connector is None and _default_connector is not None:
        entry = {**config, **_default_connector}
        connector = entry.get("connector")
        cache_key = "_default"

    # Slot 4: local disk, the terminal fallback.
    if connector is None:
        from opteryx.connectors.filesystem_connector import FileSystemConnector
        from opteryx.connectors.io_systems import OpteryxLocalFileSystem

        cache_key = "_disk"
        if cache_key in _connector_cache:
            return _connector_cache[cache_key]
        filesystem = OpteryxLocalFileSystem()
        connector_instance = FileSystemConnector(
            filesystem=filesystem, storage_type="LOCAL", telemetry=telemetry, **config
        )
        connector_instance._matched_prefix = None
        _connector_cache[cache_key] = connector_instance
        return connector_instance

    # Version the answer: an explicit version from a Resolution wins; anything
    # else is fingerprinted so a config change rotates the instance.
    if version is None:
        version = _fingerprint(entry)

    cached = _connector_cache.get(cache_key)
    if cached is not None and _connector_versions.get(cache_key) == version:
        return cached

    # The connector itself is not configuration - it IS the thing being
    # configured - so it is not passed to the constructor. `prefix` is kept in
    # the entry for compatibility: factories and connectors have always
    # received (and mostly ignored) it.
    build_entry = {key: value for key, value in entry.items() if key != "connector"}
    connector_instance = _build_connector(connector, build_entry, telemetry)

    # Store the matched prefix so binder-side code can extract dataset names
    connector_instance._matched_prefix = matched_prefix

    # Cache the instance; replacing the entry is the eviction - rotated-config
    # instances never accumulate.
    _connector_cache[cache_key] = connector_instance
    _connector_versions[cache_key] = version

    return connector_instance


def __getattr__(connector_name: str):
    """Lazy load connector classes on first access."""
    if connector_name == "OpteryxConnector":
        from opteryx.connectors.opteryx_connector import OpteryxConnector

        return OpteryxConnector
    if connector_name == "FileSystemConnector":
        from opteryx.connectors.filesystem_connector import FileSystemConnector

        return FileSystemConnector
    if connector_name == "LocalStoreConnector":
        from opteryx.connectors.local_store_connector import LocalStoreConnector

        return LocalStoreConnector
    if connector_name == "MabelConnector":
        from opteryx.connectors.mabel_connector import MabelConnector

        return MabelConnector
    if connector_name == "create_local_mabel_connector":
        from opteryx.connectors.mabel_connector import create_local_mabel_connector

        return create_local_mabel_connector
    if connector_name == "create_gcs_mabel_connector":
        from opteryx.connectors.mabel_connector import create_gcs_mabel_connector

        return create_gcs_mabel_connector
    if connector_name == "GcpCloudStorageConnector":
        # Return FileSystemConnector with GCS filesystem
        return create_gcs_connector
    if connector_name == "DiskConnector":
        # Return FileSystemConnector with local filesystem
        return create_local_connector

    raise AttributeError(f"module {__name__!r} has no attribute {connector_name!r}")
