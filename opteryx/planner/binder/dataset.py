# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

import fnmatch
from typing import Tuple

from draken.draken_native import DrakenType

from opteryx.exceptions import (
    AmbiguousDatasetError,
    InvalidFunctionParameterError,
    UnsupportedSyntaxError,
)
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.logical_type import LogicalCategory, ColumnType, _NUMERIC_TYPES, _TEMPORAL_TYPES
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema, mint_column_identity
from opteryx.utils import random_string

# JSONL columns rugo's decoder can currently produce that READ_JSONL knows how to
# describe as a ColumnType. Anything else (ARRAY, nested object/VARIANT, ...) is
# out of scope for Stage 1 and fails loud rather than being silently mistyped.
_JSONL_SUPPORTED_TYPES = {
    DrakenType.INT64,
    DrakenType.FLOAT64,
    DrakenType.BOOL,
    DrakenType.VARCHAR,
    DrakenType.NULL,
}

# Bare-path dataset function glob support (READ_JSONL, READ_PARQUET): a path
# containing any of these characters is treated as a pattern rather than an
# exact path.
_GLOB_METACHARACTERS = frozenset("*?[")


def _is_glob_pattern(path: str) -> bool:
    return any(ch in path for ch in _GLOB_METACHARACTERS)


def _resolve_glob_files(path: str, filesystem) -> list:
    """Expand a bare dataset function's glob pattern into a sorted list of matched
    file paths (used by READ_JSONL and READ_PARQUET).

    Splits ``path`` at the first glob metacharacter into a metacharacter-free
    base directory (everything before the last '/' preceding that
    metacharacter) and lists files under it via the filesystem's
    ``list_files(base_dir, recursive=True)`` -- the same directory-listing
    mechanism Parquet's catalog/ad-hoc directory fan-out uses, but implemented
    independently here since a bare dataset function has no connector/manifest
    object of its own yet. Candidates are then filtered with ``fnmatch.fnmatch``
    against the full pattern.

    Returns matches sorted lexicographically by path -- this is the
    documented, deterministic multi-file order: the first file (post-sort) is
    the one bind-time schema resolution reads.

    Note: fnmatch's ``*`` matches ``/`` too (it is not path-aware like
    ``pathlib.Path.glob``), so a single ``*`` segment already spans
    directories under a recursive listing -- e.g. ``logs/*.jsonl`` can match
    files nested below ``logs/``, and ``logs/**/*.jsonl`` behaves the same as
    ``logs/*.jsonl``. This is a direct consequence of using ``fnmatch`` as
    directed rather than a hand-rolled path-aware matcher.
    """
    first_meta = min(path.index(ch) for ch in _GLOB_METACHARACTERS if ch in path)
    prefix = path[:first_meta]
    base_dir = prefix.rsplit("/", 1)[0] if "/" in prefix else ""
    candidates = filesystem.list_files(base_dir, recursive=True)
    matched = [p for p in candidates if fnmatch.fnmatch(p, path)]
    return sorted(matched)


def visit_function_dataset(
    self, node: Node, context: BindingContext
) -> Tuple[Node, BindingContext]:
    # We need to build the schema and add it to the schema collection.
    # Default: no connector, so predicate_pushdown's FunctionDataset sink treats
    # this node as not-predicate-pushable (`if not node.connector`). Overridden
    # below only for READ_JSONL and READ_PARQUET, which have a real backing
    # reader to push into.
    node.connector = None
    if node.function == "VALUES":
        relation_name = node.alias or f"$values-{random_string()}"
        types = {}
        element_types = {}
        if len(node.values) > 0:
            for i, column in enumerate(node.columns):
                if len(node.values[0]) >= i:
                    value = node.values[0][i]
                    types[column] = value.type  # ColumnType
                    # Phase 2: element is embedded in ARRAY/VECTOR ColumnType.
                    _val_cat = value.type.category if isinstance(value.type, ColumnType) else value.type
                    if _val_cat in (LogicalCategory.ARRAY, LogicalCategory.VECTOR):
                        _elem = value.type.element if isinstance(value.type, ColumnType) else None
                        if _elem is None:
                            schema_column = getattr(value, "schema_column", None)
                            if schema_column is not None and isinstance(getattr(schema_column, "column_type", None), ColumnType):
                                _elem = schema_column.column_type.element
                        element_types[column] = _elem
        def _build_value_column(column):
            ct = types.get(column)  # ColumnType or None
            ident = mint_column_identity(relation_name, column)
            if isinstance(ct, ColumnType):
                return SchemaColumn(name=column, column_type=ct, identity=ident)
            from opteryx.types import logical_type as _lt2
            return SchemaColumn(name=column, column_type=_lt2.NULL, identity=ident)
        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=column,
                source=relation_name,
                schema_column=_build_value_column(column),
            )
            for column in node.columns
        ]
        schema = RelationSchema(
            name=relation_name,
            columns=[c.schema_column for c in columns],
        )
        context.schemas[relation_name] = schema
        node.columns = columns
        node.schema = schema
    elif node.function == "UNNEST":
        # this is strictly SELECT * FROM UNNEST(literal) AS alias(column)
        relation_name = node.alias

        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=node.unnest_target,
                source=relation_name,
                schema_column=SchemaColumn(name=node.unnest_target, identity=mint_column_identity(relation_name, node.unnest_target)),
            )
        ]
        schema = RelationSchema(name=relation_name, columns=[c.schema_column for c in columns])
        context.schemas[relation_name] = schema
        # ensure origin is set so later passes (projection pushdown, etc.)
        for column in schema.columns:
            column.origin = [relation_name]
        node.columns = columns
        node.schema = schema
    elif node.function == "GENERATE_SERIES":
        element_type = None
        first_arg = node.args[0]
        if first_arg.node_type == NodeType.NESTED:
            first_arg = first_arg.centre
        # Phase 2: first_arg.type is ColumnType; compare via .category
        first_arg_cat = first_arg.type.category if isinstance(first_arg.type, ColumnType) else first_arg.type
        if first_arg_cat is not None and first_arg_cat in _NUMERIC_TYPES:
            arg_cts = {n.type for n in node.args}
            arg_cats = {t.category if isinstance(t, ColumnType) else t for t in arg_cts}
            if len(arg_cts) == 1:
                element_type = list(arg_cts)[0]  # ColumnType
            elif arg_cats == {LogicalCategory.INTEGER, LogicalCategory.FLOAT}:
                element_type = _lt.FLOAT64
            else:
                raise InvalidFunctionParameterError(
                    "GENERATE_SERIES for numbers takes 1 (stop), 2 (start, stop) or 3 (start, stop, interval) parameters."
                )
        if first_arg_cat is not None and first_arg_cat in _TEMPORAL_TYPES:
            element_type = _lt.TIMESTAMP()

        node.relation_name = node.alias
        _gs_schema_col = SchemaColumn(name=node.alias, column_type=element_type if isinstance(element_type, ColumnType) else None, identity=mint_column_identity(node.relation_name, node.alias))
        columns = [
            LogicalColumn(
                node_type=NodeType.IDENTIFIER,
                source_column=node.alias,
                source=node.relation_name,
                schema_column=_gs_schema_col,
            )
        ]
        schema = RelationSchema(
            name=node.relation_name,
            columns=[c.schema_column for c in columns],
        )
        context.schemas[node.relation_name] = schema
        # tag generated columns with their origin relation name so downstream
        # binder/optimizer logic can detect their source
        for column in schema.columns:
            column.origin = [node.relation_name]
        node.columns = columns
        node.schema = schema
    elif node.function == "READ_JSONL":
        from opteryx.connectors.io_systems import create_filesystem
        from opteryx.exceptions import DatasetNotFoundError
        from opteryx.exceptions import DatasetReadError
        from opteryx.exceptions import NotSupportedError
        from rugo.jsonl import read_jsonl as _rugo_read_jsonl

        path_arg = node.args[0] if node.args else None
        if path_arg is not None and path_arg.node_type == NodeType.NESTED:
            path_arg = path_arg.centre
        if path_arg is None or path_arg.node_type != NodeType.LITERAL or not isinstance(
            path_arg.value, str
        ):
            raise InvalidFunctionParameterError(
                "READ_JSONL requires a single string literal path, "
                "e.g. READ_JSONL('file.jsonl')."
            )
        path = path_arg.value

        # Validate READ_JSONL's named options (Stage 3). Only `ignore_errors`,
        # `infer_schema`, and `infer_sample_size` are wired through to rugo;
        # `explicit_schema` is a known, documented gap (rugo has no working
        # per-chunk explicit_schema override today -- see the module docstring
        # in opteryx/connectors/jsonl_io/__init__.py), so it fails loud with a
        # distinct error rather than being silently ignored or treated as a
        # typo. Any other key is an unrecognized option.
        named_args = node.named_args or {}

        def _literal_value(key):
            arg = named_args[key]
            if arg.node_type == NodeType.NESTED:
                arg = arg.centre
            return arg

        if "explicit_schema" in named_args:
            raise NotSupportedError(
                "READ_JSONL('explicit_schema=...') is not supported: rugo has no "
                "working per-chunk explicit_schema override today, so this option "
                "cannot be honored yet."
            )
        for key in named_args:
            if key not in ("ignore_errors", "infer_schema", "infer_sample_size"):
                raise InvalidFunctionParameterError(
                    f"READ_JSONL received an unrecognized option '{key}'."
                )

        if "ignore_errors" in named_args:
            arg = _literal_value("ignore_errors")
            if arg.node_type != NodeType.LITERAL or not isinstance(arg.value, bool):
                raise InvalidFunctionParameterError(
                    "READ_JSONL option 'ignore_errors' must be a boolean literal."
                )
            fail_on_error = not arg.value
        else:
            fail_on_error = True

        if "infer_schema" in named_args:
            arg = _literal_value("infer_schema")
            if arg.node_type != NodeType.LITERAL or not isinstance(arg.value, bool):
                raise InvalidFunctionParameterError(
                    "READ_JSONL option 'infer_schema' must be a boolean literal."
                )
            infer_schema = arg.value
        else:
            infer_schema = True

        if "infer_sample_size" in named_args:
            arg = _literal_value("infer_sample_size")
            if (
                arg.node_type != NodeType.LITERAL
                or isinstance(arg.value, bool)
                or not isinstance(arg.value, int)
            ):
                raise InvalidFunctionParameterError(
                    "READ_JSONL option 'infer_sample_size' must be an integer literal."
                )
            infer_sample_size = arg.value
        else:
            infer_sample_size = 5

        # Resolve the filesystem the same way Parquet scans do (protocol prefix
        # of the path -> opteryx.connectors.io_systems.create_filesystem). There
        # is no connector object yet for a bare file-path dataset function.
        protocol = path.split("://")[0] if "://" in path else ""
        is_glob = _is_glob_pattern(path)

        # "gcs://" is not a recognized scheme here -- reject it outright rather than
        # treating it as an alias for "gs://". (create_filesystem's own protocol_map
        # treats "gs"/"gcs" as equivalent for other, authorization-checked callers,
        # but the native Parquet scan gate (pool_reader.native_scan_supported) only
        # recognizes the literal "gs://" prefix as remote and mis-detects "gcs://"
        # as local, so admitting it here for a bare dataset function is a trap, not
        # a convenience.)
        if protocol == "gcs":
            raise InvalidFunctionParameterError(
                f"READ_JSONL('{path}'): 'gcs://' is not a supported scheme; use 'gs://'."
            )

        # SECURITY: unlike visit_scan (catalog-backed table scans, gated by
        # can_perform_action before any connector is opened), READ_JSONL is a bare
        # dataset function -- any SQL text that can reach the binder can name any
        # path. create_filesystem("gs") authenticates with this PROCESS's own
        # ambient service-account credentials (opteryx.connectors.io_systems.
        # gcs_filesystem.get_storage_credentials -> google.auth.default()), not
        # anything scoped to the requesting user, so it must never be used for a
        # user-supplied gs:// path here. Instead READ_JSONL always does a plain,
        # unauthenticated HTTPS GET (anonymous_gcs_filesystem) -- never a signed URL,
        # never the ambient bearer token. GCS's own object-level IAM decides the
        # outcome: a public object (e.g. gs://opteryx/...) is read; a private one
        # (e.g. gs://opteryx_data/...) 403s from GCS itself, not from any allow/deny
        # decision Opteryx makes. Bucket LISTING is a separate IAM permission from
        # object GET and is not assumed granted anonymously, so a glob pattern over
        # gs:// is rejected outright rather than silently escalating to an
        # authenticated listing call. This does not touch create_filesystem itself,
        # so catalog-backed GCS read paths (visit_scan) are unaffected; READ_PARQUET
        # below applies the identical restriction for the identical reason.
        if protocol == "gs":
            if is_glob:
                raise NotSupportedError(
                    f"READ_JSONL('{path}'): glob patterns are not supported for gs:// paths. "
                    "READ_JSONL never uses this process's platform GCS credentials, so it only "
                    "ever does a plain, unauthenticated GET of a single object -- there is no "
                    "anonymous bucket-listing call available to resolve a glob against."
                )
            from opteryx.connectors.io_systems.anonymous_gcs_filesystem import (
                anonymous_gcs_filesystem,
            )

            filesystem = anonymous_gcs_filesystem()
        else:
            filesystem = create_filesystem(protocol)

        # Stage 4: glob support. A non-glob path is just a matched-file list of
        # length 1 -- there is no separate single-file code path, so Stages
        # 1-3's behavior for an exact path cannot diverge from this one.
        if is_glob:
            jsonl_files = _resolve_glob_files(path, filesystem)
            if not jsonl_files:
                raise DatasetNotFoundError(connector="READ_JSONL", dataset=path)
        else:
            jsonl_files = [path]

        # Bind-time schema is resolved from the first matched file (matches
        # sorted lexicographically by path above); every other matched file's
        # decoded columns/types are validated against this same schema at
        # execution time (JsonlReadNode.read_morsels), the identical
        # fail-loud-on-mismatch policy already applied across chunks of a
        # single file.
        schema_source_path = jsonl_files[0]

        file_obj = filesystem.open_input_file(schema_source_path)
        try:
            # rugo.jsonl.read_metadata()'s type inference is a no-op today (the
            # 'schema' dict it returns is never populated in
            # rugo/src/jsonl/_jsonl_reader.pxi -> every column reports "object"),
            # so the real per-column DrakenType is read off an actual decode
            # instead. This pays the same full-parse cost read_metadata already
            # pays internally -- it is not a lightweight metadata-only path
            # either, so there is no cheaper alternative available today.
            # The resolved options are honored here too -- e.g. ignore_errors
            # must apply to this bind-time sample read, not just per-chunk
            # decode, or a malformed file would still fail loud at bind time
            # regardless of the option.
            with _rugo_read_jsonl(
                file_obj.memoryview,
                fail_on_error=fail_on_error,
                infer_schema=infer_schema,
                infer_sample_size=infer_sample_size,
            ) as reader:
                sample_morsel = next(iter(reader))
        except RuntimeError as err:
            raise DatasetReadError(f"Cannot read JSONL file '{schema_source_path}': {err}") from err
        finally:
            file_obj.close()

        physical_names = [
            name.decode("utf-8") if isinstance(name, bytes) else name
            for name in sample_morsel.column_names
        ]

        if node.columns:
            # READ_JSONL('...') AS alias(col1, col2, ...) -- renaming columns via
            # the alias's own column list -- is not supported: `node.columns`
            # holds pre-bind plain strings here, never replaced with bound
            # LogicalColumn objects the rest of the pipeline expects, so this
            # shape crashes later with an opaque AttributeError instead of
            # failing loud at the point of the actual problem. `AS alias` (a
            # plain relation rename, no column list) is unaffected.
            raise NotSupportedError(
                f"READ_JSONL('{path}') AS alias(...) is not supported -- only "
                "AS alias (renaming the relation, not its columns) is. Use a "
                "SELECT ... AS new_name wrapper to rename individual columns."
            )
        external_names = physical_names

        relation_name = node.alias or f"$read_jsonl-{random_string()}"

        schema_columns = []
        for physical_name, external_name in zip(physical_names, external_names):
            vector = sample_morsel.column(physical_name.encode("utf-8"))
            physical_type = vector.type
            if physical_type not in _JSONL_SUPPORTED_TYPES:
                raise NotSupportedError(
                    f"READ_JSONL column '{physical_name}' has inferred type "
                    f"{physical_type!r}, which Stage 1 does not support (only "
                    "INT64/FLOAT64/BOOL/VARCHAR/NULL are supported)."
                )
            schema_columns.append(
                SchemaColumn(
                    name=external_name,
                    column_type=ColumnType(physical=physical_type),
                    identity=mint_column_identity(relation_name, external_name),
                )
            )

        schema = RelationSchema(name=relation_name, columns=schema_columns)
        context.schemas[relation_name] = schema
        for column in schema.columns:
            column.origin = [relation_name]

        node.alias = relation_name
        # node.columns is deliberately left unset here (matching visit_scan's Scan
        # nodes, which never set it at bind time either): ProjectionPushdownStrategy
        # populates it for the first time from `node.schema.columns` filtered to
        # what's actually referenced above this node. Setting it here to the full
        # column list would make that same strategy's generic "collect this node's
        # own `.columns` as used identities" step (which runs before the
        # Scan/FunctionDataset pruning branch, for every node type) see every
        # column as already "used", defeating pruning for this node before it can
        # ever run.
        node.schema = schema
        # Carried for the physical operator (JsonlReadNode): the source path and
        # the file's own (pre-alias) column order/names, so decoded chunks can be
        # re-identified against the pruned `node.columns` the optimizer produces
        # (by identity, via jsonl_physical_by_identity below -- a positional zip
        # against this list would go stale once columns are pruned/reordered).
        node.dataset = path
        # Stage 4: the resolved, sorted, non-empty list of files this READ_JSONL
        # reads -- length 1 for a plain (non-glob) path, so JsonlReadNode has a
        # single fan-out loop rather than a separate single-file code path.
        node.jsonl_files = jsonl_files
        node.jsonl_physical_columns = physical_names
        # Resolved READ_JSONL options (Stage 3), carried for the physical
        # operator (JsonlReadNode) to pass through to every chunk's decode.
        node.jsonl_fail_on_error = fail_on_error
        node.jsonl_infer_schema = infer_schema
        node.jsonl_infer_sample_size = infer_sample_size
        # identity -> pre-alias physical (JSON key) name. Stable across projection
        # pushdown pruning `node.columns` (SchemaColumn objects/identities are
        # reused, never re-minted, by that pass) -- physical_planner uses this to
        # recover the correct physical names for whatever subset of `node.columns`
        # survives optimization, and to translate pushed predicates' IDENTIFIER
        # operands into rugo's physical-name predicate tuples.
        node.jsonl_physical_by_identity = {
            schema_column.identity: physical_name
            for physical_name, schema_column in zip(physical_names, schema_columns)
        }
        # Enables predicate pushdown (opteryx/planner/optimizer/strategies/
        # predicate_pushdown.py's _handle_predicates gates on `node.connector`
        # being truthy and PredicatePushable-capable) for a plain col-OP-literal
        # comparison a rugo predicate tuple can express; see JsonlPredicatePushable
        # for exactly what shapes that covers.
        from opteryx.connectors.jsonl_io import JsonlPredicatePushable

        node.connector = JsonlPredicatePushable()
    elif node.function == "READ_PARQUET":
        from opteryx.connectors._rugo_schema import rugo_to_relation_schema
        from opteryx.connectors.filesystem_connector import FileSystemTable
        from opteryx.connectors.io_systems import create_filesystem
        from opteryx.exceptions import DatasetNotFoundError
        from opteryx.exceptions import DatasetReadError
        from opteryx.exceptions import NotSupportedError
        from opteryx.models.file_entry import FileEntry
        from opteryx.models.manifest import Manifest
        from rugo.parquet import read_metadata_from_memoryview

        path_arg = node.args[0] if node.args else None
        if path_arg is not None and path_arg.node_type == NodeType.NESTED:
            path_arg = path_arg.centre
        if path_arg is None or path_arg.node_type != NodeType.LITERAL or not isinstance(
            path_arg.value, str
        ):
            raise InvalidFunctionParameterError(
                "READ_PARQUET requires a single string literal path, "
                "e.g. READ_PARQUET('file.parquet')."
            )
        path = path_arg.value

        # Unlike READ_JSONL, Parquet's schema is unambiguous (read straight off the
        # file's own footer, not inferred from sample rows), so there is nothing
        # analogous to Stage 3's ignore_errors/infer_schema/infer_sample_size to
        # configure -- any named option is a mistake, not a typo to silently ignore.
        if node.named_args:
            raise InvalidFunctionParameterError(
                f"READ_PARQUET does not take options; received {sorted(node.named_args)}."
            )

        protocol = path.split("://")[0] if "://" in path else ""
        is_glob = _is_glob_pattern(path)

        # "gcs://" is not a recognized scheme -- see the identical check/comment on
        # the READ_JSONL branch above (pool_reader's native scan gate only matches
        # the literal "gs://" prefix and mis-detects "gcs://" as a local path).
        if protocol == "gcs":
            raise InvalidFunctionParameterError(
                f"READ_PARQUET('{path}'): 'gcs://' is not a supported scheme; use 'gs://'."
            )

        # SECURITY: identical reasoning and mechanism to READ_JSONL's gs:// handling
        # above -- READ_PARQUET is equally a bare dataset function with no
        # can_perform_action authorization layer, so it must never use this
        # process's ambient/platform GCS credentials for a user-supplied path.
        if protocol == "gs":
            if is_glob:
                raise NotSupportedError(
                    f"READ_PARQUET('{path}'): glob patterns are not supported for gs:// "
                    "paths. READ_PARQUET never uses this process's platform GCS "
                    "credentials, so it only ever does a plain, unauthenticated GET of "
                    "a single object -- there is no anonymous bucket-listing call "
                    "available to resolve a glob against."
                )
            from opteryx.connectors.io_systems.anonymous_gcs_filesystem import (
                anonymous_gcs_filesystem,
            )

            filesystem = anonymous_gcs_filesystem()
            storage_type = "GCS"
        else:
            filesystem = create_filesystem(protocol)
            storage_type = {"http": "HTTP", "https": "HTTP"}.get(protocol, "LOCAL")

        # Bind-time file resolution: a non-glob path is just a matched-file list of
        # length 1, mirroring READ_JSONL's Stage 4 design so the two cases can never
        # diverge. A glob's candidates are filtered to '.parquet' (the same silent
        # non-parquet-file exclusion FileSystemConnector already applies to ordinary
        # directory-backed Parquet datasets -- not a stricter policy invented here).
        if is_glob:
            parquet_files = [
                f for f in _resolve_glob_files(path, filesystem) if f.lower().endswith(".parquet")
            ]
            if not parquet_files:
                raise DatasetNotFoundError(connector="READ_PARQUET", dataset=path)
        else:
            parquet_files = [path]

        # Bind-time schema comes from the first file's own footer (Parquet's schema
        # is embedded and unambiguous, unlike READ_JSONL's sample-row inference) --
        # a real rugo metadata read, same primitive FileSystemTable.read_blob(...,
        # just_schema=True) uses for catalog/ad-hoc Parquet scans.
        schema_source_path = parquet_files[0]
        stream = filesystem.open_input_stream(schema_source_path)
        try:
            rugo_metadata = read_metadata_from_memoryview(stream.memoryview)
        except RuntimeError as err:
            raise DatasetReadError(f"Cannot read Parquet file '{schema_source_path}': {err}") from err
        finally:
            stream.close()

        relation_name = node.alias or f"$read_parquet-{random_string()}"
        physical_schema = rugo_to_relation_schema(rugo_metadata, schema_name=relation_name)
        physical_names = [c.name for c in physical_schema.columns]

        if node.columns:
            # See the identical check/comment on the READ_JSONL branch above --
            # AS alias(col1, col2, ...) column renaming is not supported; AS
            # alias (relation rename only) is unaffected.
            raise NotSupportedError(
                f"READ_PARQUET('{path}') AS alias(...) is not supported -- only "
                "AS alias (renaming the relation, not its columns) is. Use a "
                "SELECT ... AS new_name wrapper to rename individual columns."
            )
        external_names = physical_names

        schema_columns = [
            SchemaColumn(
                name=external_name,
                column_type=physical_column.column_type,
                identity=mint_column_identity(relation_name, external_name),
            )
            for physical_column, external_name in zip(physical_schema.columns, external_names)
        ]
        schema = RelationSchema(name=relation_name, columns=schema_columns)
        context.schemas[relation_name] = schema
        for column in schema.columns:
            column.origin = [relation_name]

        # Manifest: file_path/file_format/file_size_in_bytes only. record_count and
        # column_stats are left at their defaults (0/None) -- they're pruning
        # accelerators for cost-based optimizer strategies (manifest pruning,
        # stats-only COUNT(*), LIMIT-driven file pruning), which are Scan-only today
        # and don't fire for a FunctionDataset-typed node regardless -- an accepted
        # v1 gap, not an oversight; predicate/projection/limit pushdown (the
        # correctness-affecting kind) already applies generically to FunctionDataset
        # nodes the same as Scan.
        file_infos = filesystem.get_file_info(parquet_files)
        file_entries = [
            FileEntry(
                file_path=f,
                file_format="PARQUET",
                record_count=0,
                file_size_in_bytes=getattr(info, "size", 0) or 0,
            )
            for f, info in zip(parquet_files, file_infos)
        ]
        manifest = Manifest(file_entries, schema)

        # node.connector is a real FileSystemTable (not just a predicate-pushdown
        # capability marker the way JsonlPredicatePushable is for READ_JSONL) --
        # this READ_PARQUET reuses the existing native ParquetReadNode wholesale
        # (physical_planner._create_function_dataset_node), which reads
        # connector.filesystem/.storage_type/.dataset directly, exactly as it does
        # for a catalog-backed or ad-hoc-registered Scan.
        table = FileSystemTable(
            dataset=path,
            filesystem=filesystem,
            storage_type=storage_type,
            telemetry=context.telemetry,
        )
        table.schema = schema

        node.alias = relation_name
        # node.columns deliberately left unset -- see the identical comment on the
        # READ_JSONL branch above; ProjectionPushdownStrategy populates it later.
        node.schema = schema
        node.manifest = manifest
        node.dataset = path
        node.connector = table
    else:
        raise UnsupportedSyntaxError(f"{node.function} cannot be used in place of a table.")
    return node, context


def visit_scan(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    import time as _bind_time

    from opteryx.connectors import connector_factory
    from opteryx.exceptions import DatabaseError
    from opteryx.managers.permissions import can_perform_action

    node.relation = node.relation.lower()

    # Internal-only relations back a dedicated SQL surface and must not be
    # addressable by name. `internal_relation` is set by the planner that owns the
    # surface (e.g. plan_show_variables) and is never set from user SQL, so this
    # rejects the typed form without blocking the sanctioned one. Checked before
    # any connector/catalog work so a rejected scan costs nothing.
    from opteryx.connectors.virtual_data_connector import INTERNAL_ONLY_DATASETS

    if node.relation in INTERNAL_ONLY_DATASETS and not node.internal_relation:
        raise UnsupportedSyntaxError(
            f"'{node.relation}' cannot be queried directly; use `SHOW VARIABLES`."
        )

    if node.alias in context.relations:
        raise AmbiguousDatasetError(dataset=node.alias)

    # External-IO instrumentation for the binder (a big, mostly-invisible chunk of
    # time_planning_binder for catalog datasets): connector/catalog resolution and
    # the dataset metadata fetch (schema + footer stats over GCS/Firestore) are the
    # per-query network cost paid before any data reads. Accumulated across scans;
    # time_ prefix → as_dict reports seconds.
    _bind_connector0 = _bind_time.monotonic_ns()

    # Get connector gateway (cached by prefix)
    gateway = connector_factory(node.relation, telemetry=context.telemetry)

    # Extract the dataset name (remove prefix if configured)
    dataset_name = node.relation

    # Create table-specific engine
    engine_kwargs = {}
    if "variables" in dir(gateway):
        engine_kwargs["variables"] = context.execution_context.variables
    if gateway.supports_diachronic:
        engine_kwargs["at_date"] = node.at_date
    if getattr(gateway, "requires_execution_context", False):
        engine_kwargs["execution_context"] = context.execution_context

    # Reuse the dataset resolved by the catalog resolution step, if present, so
    # table_engine doesn't re-read the catalog. Absent → normal binding path.
    resolved_dataset = getattr(node, "resolved_dataset", None)
    if resolved_dataset is not None:
        engine_kwargs["prefetched_table"] = resolved_dataset

    node.connector = gateway.table_engine(
        dataset_name, telemetry=context.telemetry, **engine_kwargs
    )
    if context.telemetry is not None:
        context.telemetry.time_binding_connector += (
            _bind_time.monotonic_ns() - _bind_connector0
        )

    # ensure this user can read the table. Relations that govern their own
    # per-row permissions (e.g. information_schema, which filters each row it
    # emits by the caller's READ access to the underlying table) opt out of
    # this relation-level gate rather than being blocked from the metadata
    # view entirely.
    if not getattr(node.connector, "self_governs_permissions", False):
        if not can_perform_action(context.execution_context, node.relation, action="READ"):
            raise PermissionError(f"User does not have permission to read {node.relation}")

    if "variables" in dir(node.connector):
        node.connector.variables = context.execution_context.variables
    if gateway.supports_diachronic:
        node.connector.start_date = node.start_date
        node.connector.end_date = node.end_date
    _bind_meta0 = _bind_time.monotonic_ns()
    try:
        # Get dataset schema and build manifest (if supported by connector)
        # For Opteryx catalog connectors, this creates a Manifest with file-level stats
        if getattr(node.connector, "get_dataset_metadata", None) is not None:
            node.schema, node.manifest = node.connector.get_dataset_metadata()
            # Propagate dataset commit timestamp from the connector to the
            # logical node so it becomes available to physical nodes
            # (and ultimately shown as `committed_at` in telemetry).
            dc = getattr(node.connector, "dataset_committed_at", None)
            if dc is not None:
                node.dataset_committed_at = dc
        else:
            # Fallback for connectors that don't have manifest support yet
            node.schema = node.connector.get_dataset_schema()
            node.manifest = None
        context.schemas[node.alias] = node.schema
        for column in node.schema.columns:
            column.origin = [node.alias]

        context.relations[node.alias] = node.connector.__mode__
    except DatabaseError as err:
        raise err
    except Exception as e:
        from opteryx.exceptions import DatasetReadError

        raise DatasetReadError(f"Cannot read information for dataset '{node.relation}': {e}") from e
    finally:
        if context.telemetry is not None:
            context.telemetry.time_binding_metadata += (
                _bind_time.monotonic_ns() - _bind_meta0
            )

    return node, context
