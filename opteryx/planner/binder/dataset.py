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
    md_cause,
    md_code,
    md_syntax,
)
from opteryx.expression import NodeType
from opteryx.models import LogicalColumn, Node
from opteryx.planner.binder.binding_context import BindingContext
from opteryx.types.logical_type import (
    LogicalCategory,
    ColumnType,
    _NUMERIC_TYPES,
    _TEMPORAL_TYPES,
    column_type_from_vector,
)
from opteryx.types import logical_type as _lt
from opteryx.types.schema import SchemaColumn, RelationSchema, mint_column_identity
from opteryx.utils import random_string

# JSONL columns rugo's decoder can currently produce that READ_JSONL knows how to
# describe as a ColumnType.
#
# ARRAY: rugo's parse_array_column (rugo/src/jsonl/core/column_builder.cpp) only
# produces a DRAKEN_ARRAY vector for a uniform, scalar-element JSON array (e.g.
# ["en"], [1, 2, 3]) -- non-uniform/nested arrays fall back to raw-text VARCHAR,
# which is already covered above. column_type_from_vector (opteryx/types/
# logical_type.py) is the sanctioned reconstructor for ARRAY<element> and is used
# below in place of the bare ColumnType(physical) construction; it degrades a
# parameterized or nested element to ARRAY<VARIANT> rather than guessing, which
# rugo's JSONL sampling never produces here in practice (its element is always a
# plain scalar), but the fallback still applies uniformly if that ever changes.
#
# VARIANT: a nested JSON object column (rugo's parse_objects=True default, see
# docs/json_variant_type_plan.md -- VARIANT is already a fully-wired, extraction-
# only physical type: unparameterized, so column_type_from_vector's plain
# `ColumnType(physical)` fallback is already correct for it with no new branch.
# Every place VARIANT is invalid (CAST, GROUP BY/DISTINCT/ORDER BY keys, ARRAY_AGG,
# comparison/arithmetic operators, [i] subscript) already raises a clear error
# (VariantKeyError / operator_map misses) regardless of which reader produced the
# column, so no JSONL-specific enforcement belongs here -- only `->`/`->>` (and
# CAST of the *extracted* NVARCHAR text) are how a VARIANT column is meant to be
# used, same as a VARIANT produced by any other path.
# The reader's capability declaration lives with the reader glue
# (opteryx/connectors/jsonl_io) — shared with the filesystem connector's
# JSONL-dataset schema inference so the two gates cannot drift.
from opteryx.connectors.jsonl_io import JSONL_SUPPORTED_TYPES as _JSONL_SUPPORTED_TYPES

# Bind-time JSONL schema inference decodes exactly the first chunk the SCAN will
# decode; taken from the scan's own splitter so the two cannot drift. See the call
# site in the READ_JSONL branch for why it must be this chunk and not a smaller one.
from opteryx.connectors.jsonl_io import iter_newline_chunks as _iter_newline_chunks

# CSV columns rugo's decoder can currently produce (rugo's sniff_csv_column_types
# only widens INT64 -> FLOAT64 -> VARCHAR -- no BOOL, unlike JSONL, since CSV has
# no native boolean literal syntax to sniff).
_CSV_SUPPORTED_TYPES = {
    DrakenType.INT64,
    DrakenType.FLOAT64,
    DrakenType.VARCHAR,
    DrakenType.NULL,
}

# Bare-path dataset function glob support (READ_JSONL, READ_PARQUET): a path
# containing any of these characters is treated as a pattern rather than an
# exact path.
_GLOB_METACHARACTERS = frozenset("*?[")

# The options each file-reading dataset function accepts, in the order they are
# offered as examples in error messages (so the first entry should be the one a
# user most often reaches for). These are the ONLY names accepted -- the reader
# parameters they translate to (rugo's `fail_on_error`, `has_header`, `delimiter`)
# are deliberately not aliases, so writing one is an unrecognized option and the
# "did you mean" hint below points at the SQL spelling.
_READ_JSONL_OPTIONS = ("ignore_errors", "infer_schema", "infer_sample_size")
_READ_CSV_OPTIONS = ("ignore_errors", "separator", "has_header_row", "infer_sample_size")


def _is_glob_pattern(path: str) -> bool:
    return any(ch in path for ch in _GLOB_METACHARACTERS)


def _option_name_of(node) -> str:
    """The option name an `option = value` positional argument was written with, or "".

    `READ_JSONL('f.jsonl', ignore_errors = true)` does NOT parse as a named
    argument -- sqlparser only produces `FunctionArg::Named` for the `=>` form, so
    the `=` form arrives as an ordinary *positional* expression: a COMPARISON_OPERATOR
    'Eq' node over an unqualified identifier and a value. Recognising that shape is
    the only way to tell the user their operator (not their option) was the mistake.
    A qualified left side (`t.ignore_errors`) is not an option spelling, so it
    returns "" and falls through to the generic positional-argument error.
    """
    if node.node_type != NodeType.COMPARISON_OPERATOR or node.value != "Eq":
        return ""
    left = node.left
    if left is None or left.node_type != NodeType.IDENTIFIER or left.source is not None:
        return ""
    return left.source_column


def _validate_reader_options(function: str, args: list, named_args: dict, options: tuple) -> None:
    """Reject anything after the path that is not a recognized `option => value`.

    Every file-reading dataset function (READ_JSONL/READ_CSV/READ_PARQUET) takes
    exactly one positional argument -- the path -- and its options by name. Before
    this check, only `named_args` was validated, so a mis-typed *operator*
    (`ignore_errors = true`) landed in `args` instead and was silently discarded:
    the query bound clean and read with the default. Options that quietly do
    nothing are worse than options that don't exist, so every surplus argument is
    an error here, and the message names which of the two mistakes was made --
    wrong operator, unrecognized name, or both -- with a `suggest_alternative`
    hint whenever the name is a near miss.
    """
    from opteryx.utils import suggest_alternative

    def _unrecognized(name: str, operator_also_wrong: bool) -> str:
        # A reader with no options at all can never have a "did you mean" or an
        # operator hint -- there is no spelling of this that would have worked.
        if not options:
            return f"{function} does not take options; received '{name}'."
        message = f"{function} received an unrecognized option '{name}'."
        suggestion = suggest_alternative(name, options)
        if suggestion:
            message += f" Did you mean '{suggestion}'?"
        else:
            message += f" Valid options are: {', '.join(sorted(options))}."
        if operator_also_wrong:
            message += (
                f" Options are also passed with '=>', not '=' "
                f"(e.g. {suggestion or options[0]} => ...)."
            )
        return message

    for extra in args[1:]:
        if extra.node_type == NodeType.NESTED:
            extra = extra.centre
        name = _option_name_of(extra)
        if name and name in options:
            raise InvalidFunctionParameterError(
                f"{function} option '{name}' must be passed with '=>', not '=' -- "
                f"write {function}(..., {name} => ...)."
            )
        if name:
            raise InvalidFunctionParameterError(_unrecognized(name, operator_also_wrong=True))
        raise InvalidFunctionParameterError(
            f"{function} takes a single positional argument (the path); options must be "
            f"named with '=>' (e.g. {function}(..., {options[0]} => ...))."
            if options
            else f"{function} takes a single positional argument (the path) and no options."
        )

    for name in named_args:
        if name not in options:
            raise InvalidFunctionParameterError(_unrecognized(name, operator_also_wrong=False))


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


# How each rejected argument shape is named back to the writer. Naming the shape
# they wrote is the whole value of the message -- "was given a subquery" tells them
# which of the two remedies below applies, "invalid argument" does not.
_UNNEST_ARGUMENT_SHAPES = {
    NodeType.SUBQUERY: "a subquery",
    NodeType.IDENTIFIER: "a column reference",
    NodeType.FUNCTION: "a function call",
    NodeType.LITERAL: "a single value",
}

_UNNEST_REMEDY = (
    f"Write the values out, {md_code('UNNEST((1, 2, 3)) AS x')}; to expand an array a query "
    f"produces, join to it instead: "
    f"{md_code('SELECT x FROM (SELECT ARRAY_AGG(c) AS a FROM t) AS s CROSS JOIN UNNEST(s.a) AS x')}."
)


def _validate_unnest_argument(node: Node) -> None:
    """`FROM UNNEST(...)` builds a relation out of a literal array -- nothing else.

    It is a SOURCE: there is no input stream here to resolve a column reference, a
    function call or a subquery against, so the argument has to carry its own
    values. `_unnest` (opteryx/operators/function_dataset/function_dataset.pyx)
    reads `args[0].value` and iterates it as an array, and every other shape
    reaches it as something it will iterate anyway -- a string splits into its
    characters, an int raises a bare TypeError, and a subquery hands it a
    LogicalPlan whose `Graph.__getitem__` returns None rather than raising, so
    the legacy `__getitem__` iteration protocol walks it forever and the query
    hangs allocating instead of failing.

    Refused here because the binder is where the shape is already known -- by
    read_morsels the plan is built and the only thing left to do is run it. This
    mirrors the CROSS JOIN UNNEST route, which already refuses a source it cannot
    resolve (opteryx/managers/execution/compiler.py::_compile_unnest).
    """
    if len(node.args) != 1:
        raise InvalidFunctionParameterError(
            f"{md_syntax('UNNEST')} in the {md_syntax('FROM')} clause takes exactly one array, "
            f"{len(node.args)} arguments were given. {_UNNEST_REMEDY}"
        )

    argument = node.args[0]
    # A parenthesised single value -- UNNEST((1)) -- is a one-row relation; _unnest
    # wraps `centre.value` itself rather than iterating it.
    if argument.node_type == NodeType.NESTED:
        return
    if argument.node_type == NodeType.LITERAL and isinstance(argument.value, (list, tuple)):
        return

    raise InvalidFunctionParameterError(
        f"{md_syntax('UNNEST')} in the {md_syntax('FROM')} clause builds a relation from a "
        f"literal array, and was given "
        f"{_UNNEST_ARGUMENT_SHAPES.get(argument.node_type, 'an expression')}. {_UNNEST_REMEDY}"
    )


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
        _validate_unnest_argument(node)
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
                    "GENERATE_SERIES for numbers takes 1 (stop), 2 (start, stop) or 3 (start, stop, interval) parameters. Write `GENERATE_SERIES(10)`, `GENERATE_SERIES(1, 10)` or `GENERATE_SERIES(1, 10, 2)`."
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

        # `explicit_schema` is checked against every spelling the user could have
        # written -- `=>` (named_args) and `=` (a positional Eq expression) -- so the
        # documented gap reports itself either way rather than the second form
        # reporting a wrong operator for an option that would fail regardless.
        written_options = set(named_args)
        for extra in node.args[1:]:
            written_options.add(
                _option_name_of(extra.centre if extra.node_type == NodeType.NESTED else extra)
            )
        if "explicit_schema" in written_options:
            raise NotSupportedError(
                "READ_JSONL('explicit_schema=...') is not supported: rugo has no "
                "working per-chunk explicit_schema override today, so this option "
                "cannot be honored yet."
            )
        _validate_reader_options("READ_JSONL", node.args, named_args, _READ_JSONL_OPTIONS)

        if "ignore_errors" in named_args:
            arg = _literal_value("ignore_errors")
            if arg.node_type != NodeType.LITERAL or not isinstance(arg.value, bool):
                raise InvalidFunctionParameterError(
                    "READ_JSONL option 'ignore_errors' must be a boolean literal. It has to be a literal value, not a column or an expression."
                )
            fail_on_error = not arg.value
        else:
            fail_on_error = True

        if "infer_schema" in named_args:
            arg = _literal_value("infer_schema")
            if arg.node_type != NodeType.LITERAL or not isinstance(arg.value, bool):
                raise InvalidFunctionParameterError(
                    "READ_JSONL option 'infer_schema' must be a boolean literal. It has to be a literal value, not a column or an expression."
                )
            infer_schema = arg.value
        else:
            infer_schema = True

        # `infer_sample_size` sets how many leading records are read to decide BOTH the
        # column set (the union of their keys, first-seen order) and each column's type.
        # NDJSON is not required to be homogeneous, so a key absent from record 0 but
        # present in record 3 is still a column at the default of 5; a key that first
        # appears only past the window is not visible at all. There is no free choice of
        # default here -- some number has to be picked, and 5 is it.
        if "infer_sample_size" in named_args:
            arg = _literal_value("infer_sample_size")
            if (
                arg.node_type != NodeType.LITERAL
                or isinstance(arg.value, bool)
                or not isinstance(arg.value, int)
            ):
                raise InvalidFunctionParameterError(
                    "READ_JSONL option 'infer_sample_size' must be an integer literal. It has to be a literal value, not a column or an expression."
                )
            # Rejected here rather than left to rugo's own guard so the error names the SQL
            # option the user actually wrote instead of the reader's parameter.
            if arg.value <= 0:
                raise InvalidFunctionParameterError(
                    "READ_JSONL option 'infer_sample_size' must be greater than 0; "
                    f"received {arg.value}."
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
                    f"READ_JSONL('{path}'): glob patterns are not supported for gs:// paths. Name the file exactly, or read the whole prefix without a wildcard."
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

        # Bind-time schema is resolved from the first matched file that actually
        # CONTAINS A RECORD (matches sorted lexicographically by path above);
        # every other matched file's decoded columns/types are validated against
        # this same schema at execution time (JsonlReadNode.read_morsels), the
        # identical fail-loud-on-mismatch policy already applied across chunks of
        # a single file.
        #
        # A record-less file -- zero bytes, or nothing but blank/whitespace lines
        # -- carries no schema to infer: rugo yields NO morsel at all for it, so
        # `next(iter(reader))` used to leak a bare StopIteration out of the
        # binder (surfacing as "RuntimeError: generator raised StopIteration",
        # PEP 479). Such a file is a legitimately empty relation, not a read
        # failure, so it is SKIPPED as a schema source rather than being allowed
        # to define one.
        #
        # Skipping matters beyond tidiness: with zero columns bound,
        # read_morsels' zero-column branch deliberately suppresses the per-file
        # schema-drift checks (nothing is projected, so nothing can disagree), so
        # binding zero columns off an empty FIRST file would make a glob silently
        # return column-less rows for every other matched file. Skipping keeps
        # the schema coming from a file that has one.
        sample_morsel = None
        schema_source_path = jsonl_files[0]

        for candidate_path in jsonl_files:
            file_obj = filesystem.open_input_file(candidate_path)
            try:
                # rugo.jsonl.read_metadata()'s type inference is a no-op today (the
                # 'schema' dict it returns is never populated in
                # rugo/src/jsonl/_jsonl_reader.pxi -> every column reports "object"),
                # so the real per-column DrakenType is read off an actual decode
                # instead -- there is no metadata-only path that reports types.
                #
                # That decode is bounded to the file's FIRST newline-aligned chunk
                # rather than the whole file. Decoding the whole file materialised a
                # vector for every row of every column purely to read the types back
                # off the result: ~450MB of peak RSS and half the wall time on a
                # 100MB file, and unbounded -- peak grew with file size, so a large
                # enough file could not be planned at all, however cheap its
                # execution.
                #
                # The bound is `iter_newline_chunks`' first chunk SPECIFICALLY, not
                # a smaller sample, and it is taken from that function rather than
                # recomputed here so the two cannot drift. rugo's inferred column
                # TYPES come from the whole buffer it is handed, not from the first
                # `infer_sample_size` records (that governs the column SET) -- a
                # column of ints that turns into floats halfway down a file decodes
                # as INT64 from a short prefix and FLOAT64 from the whole file. Since
                # read_morsels validates every chunk's decoded types against the
                # schema bound here, binding off anything OTHER than the exact bytes
                # of chunk 0 would make that check fail on chunk 0 itself for such a
                # file. Binding off chunk 0 makes them agree by construction.
                #
                # This does not make cross-chunk type drift safe -- a file whose
                # later chunks decode differently still fails loud there, exactly as
                # it did before this change; it just no longer fails on the first
                # chunk for a file that used to read fine.
                #
                # The resolved options are honored here too -- e.g. ignore_errors
                # must apply to this bind-time sample read, not just per-chunk
                # decode, or a malformed file would still fail loud at bind time
                # regardless of the option. A malformed record beyond chunk 0 is now
                # caught by that chunk's decode at execution time instead, under the
                # same fail_on_error policy and with the same error.
                schema_chunk = next(_iter_newline_chunks(file_obj.memoryview), None)
                if schema_chunk is None:
                    # Zero-byte file: no chunk at all, so no schema to infer. Handled
                    # as the record-less case below, the same as a file of blank lines.
                    candidate_morsel = None
                else:
                    with _rugo_read_jsonl(
                        schema_chunk,
                        fail_on_error=fail_on_error,
                        infer_schema=infer_schema,
                        infer_sample_size=infer_sample_size,
                    ) as reader:
                        candidate_morsel = next(iter(reader), None)
            except RuntimeError as err:
                raise DatasetReadError(f"The JSONL file {md_code(candidate_path)} could not be read. {md_cause(err)}") from err
            finally:
                file_obj.close()

            if candidate_morsel is not None:
                sample_morsel = candidate_morsel
                schema_source_path = candidate_path
                break

        # Every matched file is record-less: the relation is genuinely empty and
        # binds with ZERO columns -- `SELECT *` over it returns no rows, and
        # naming a column fails loud with ColumnNotFoundError. This is exactly
        # what READ_CSV already binds for the same input (rugo.csv returns a
        # zero-column morsel rather than yielding nothing), so the two readers
        # agree on what an empty file means.
        if sample_morsel is None:
            physical_names = []
        else:
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
                "**SELECT** ... AS new_name wrapper to rename individual columns."
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
                    "INT64/FLOAT64/BOOL/VARCHAR/NULL/ARRAY/VARIANT are supported)."
                )
            schema_columns.append(
                SchemaColumn(
                    name=external_name,
                    column_type=column_type_from_vector(vector),
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
        # configure -- any option is a mistake, not a typo to silently ignore. The
        # empty option tuple makes every name unrecognized, in either spelling.
        _validate_reader_options("READ_PARQUET", node.args, node.named_args or {}, ())

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
                    f"READ_PARQUET('{path}'): glob patterns are not supported for gs:// paths. Name the file exactly, or read the whole prefix without a wildcard."
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
            raise DatasetReadError(f"The Parquet file {md_code(schema_source_path)} could not be read. {md_cause(err)}") from err
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
                "**SELECT** ... AS new_name wrapper to rename individual columns."
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
    elif node.function == "READ_CSV":
        from opteryx.connectors.csv_io import CsvPredicatePushable
        from opteryx.connectors.csv_io import read_csv_file
        from opteryx.connectors.io_systems import create_filesystem
        from opteryx.exceptions import DatasetNotFoundError
        from opteryx.exceptions import DatasetReadError
        from opteryx.exceptions import NotSupportedError

        path_arg = node.args[0] if node.args else None
        if path_arg is not None and path_arg.node_type == NodeType.NESTED:
            path_arg = path_arg.centre
        if path_arg is None or path_arg.node_type != NodeType.LITERAL or not isinstance(
            path_arg.value, str
        ):
            raise InvalidFunctionParameterError(
                "READ_CSV requires a single string literal path, "
                "e.g. READ_CSV('file.csv')."
            )
        path = path_arg.value

        # Validate READ_CSV's named options. `separator`/`has_header_row` map
        # directly to rugo's own `delimiter`/`has_header` params; `ignore_errors`/
        # `infer_sample_size` map to rugo's `fail_on_error` (inverted, the same
        # translation READ_JSONL's `ignore_errors` gets above) / `infer_sample_size`.
        named_args = node.named_args or {}

        def _literal_value(key):
            arg = named_args[key]
            if arg.node_type == NodeType.NESTED:
                arg = arg.centre
            return arg

        _validate_reader_options("READ_CSV", node.args, named_args, _READ_CSV_OPTIONS)

        if "separator" in named_args:
            arg = _literal_value("separator")
            if (
                arg.node_type != NodeType.LITERAL
                or not isinstance(arg.value, str)
                or len(arg.value) != 1
            ):
                raise InvalidFunctionParameterError(
                    "READ_CSV option 'separator' must be a single-character string literal. It has to be a literal value, not a column or an expression."
                )
            separator = arg.value
        else:
            separator = ","

        if "has_header_row" in named_args:
            arg = _literal_value("has_header_row")
            if arg.node_type != NodeType.LITERAL or not isinstance(arg.value, bool):
                raise InvalidFunctionParameterError(
                    "READ_CSV option 'has_header_row' must be a boolean literal. It has to be a literal value, not a column or an expression."
                )
            has_header_row = arg.value
        else:
            has_header_row = True

        if "ignore_errors" in named_args:
            arg = _literal_value("ignore_errors")
            if arg.node_type != NodeType.LITERAL or not isinstance(arg.value, bool):
                raise InvalidFunctionParameterError(
                    "READ_CSV option 'ignore_errors' must be a boolean literal. It has to be a literal value, not a column or an expression."
                )
            fail_on_error = not arg.value
        else:
            fail_on_error = True

        if "infer_sample_size" in named_args:
            arg = _literal_value("infer_sample_size")
            if (
                arg.node_type != NodeType.LITERAL
                or isinstance(arg.value, bool)
                or not isinstance(arg.value, int)
                or arg.value <= 0
            ):
                raise InvalidFunctionParameterError(
                    "READ_CSV option 'infer_sample_size' must be a positive integer literal. It has to be a literal value, not a column or an expression."
                )
            infer_sample_size = arg.value
        else:
            infer_sample_size = 5

        # Resolve the filesystem the same way READ_JSONL/READ_PARQUET do (protocol
        # prefix of the path -> opteryx.connectors.io_systems.create_filesystem).
        protocol = path.split("://")[0] if "://" in path else ""
        is_glob = _is_glob_pattern(path)

        # "gcs://" is not a recognized scheme -- see the identical check/comment on
        # the READ_JSONL branch above.
        if protocol == "gcs":
            raise InvalidFunctionParameterError(
                f"READ_CSV('{path}'): 'gcs://' is not a supported scheme; use 'gs://'."
            )

        # SECURITY: identical reasoning and mechanism to READ_JSONL's gs:// handling
        # above -- READ_CSV is equally a bare dataset function with no
        # can_perform_action authorization layer, so it must never use this
        # process's ambient/platform GCS credentials for a user-supplied path.
        if protocol == "gs":
            if is_glob:
                raise NotSupportedError(
                    f"READ_CSV('{path}'): glob patterns are not supported for gs:// paths. Name the file exactly, or read the whole prefix without a wildcard."
                )
            from opteryx.connectors.io_systems.anonymous_gcs_filesystem import (
                anonymous_gcs_filesystem,
            )

            filesystem = anonymous_gcs_filesystem()
        else:
            filesystem = create_filesystem(protocol)

        # Stage 4-equivalent glob support, mirroring READ_JSONL: a non-glob path
        # is just a matched-file list of length 1, so there is no separate
        # single-file code path.
        if is_glob:
            csv_files = _resolve_glob_files(path, filesystem)
            if not csv_files:
                raise DatasetNotFoundError(connector="READ_CSV", dataset=path)
        else:
            csv_files = [path]

        # Bind-time schema: a real, full rugo.csv.read_csv() pass over the first
        # matched file that actually CONTAINS A RECORD. Unlike READ_JSONL's cheap
        # first-chunk peek, this reads the whole file -- rugo.csv has no chunked
        # entry point to sample from cheaply (see opteryx.connectors.csv_io's
        # module docstring), and unlike Parquet there is no embedded footer schema
        # to read instead. The same file is read again at execution time by
        # CsvReadNode; this double-read is an accepted v1 cost, not an oversight.
        #
        # rugo.csv reports a record-less file (zero bytes, whitespace only, or a
        # header with no data rows) as a morsel with ZERO COLUMNS -- it does not
        # even carry the header names through. Such a file has no schema to give,
        # so it is skipped as a schema source for the same reason READ_JSONL skips
        # its record-less files: binding zero columns off an empty FIRST file would
        # make a glob silently return column-less rows for every other matched
        # file, because the zero-column branch in CsvReadNode.read_morsels
        # deliberately suppresses the cross-file drift check.
        sample_morsel = None
        schema_source_path = csv_files[0]

        for candidate_path in csv_files:
            file_obj = filesystem.open_input_file(candidate_path)
            try:
                candidate_morsel = read_csv_file(
                    file_obj.memoryview,
                    delimiter=separator,
                    has_header=has_header_row,
                    fail_on_error=fail_on_error,
                    infer_sample_size=infer_sample_size,
                )
            except RuntimeError as err:
                raise DatasetReadError(f"The CSV file {md_code(candidate_path)} could not be read. {md_cause(err)}") from err
            finally:
                file_obj.close()

            if len(candidate_morsel.column_names) > 0:
                sample_morsel = candidate_morsel
                schema_source_path = candidate_path
                break

        # Every matched file is record-less -- a genuinely empty relation, bound
        # with zero columns. Matches READ_JSONL's identical case above.
        if sample_morsel is None:
            physical_names = []
        else:
            physical_names = [
                name.decode("utf-8") if isinstance(name, bytes) else name
                for name in sample_morsel.column_names
            ]

        if node.columns:
            # See the identical check/comment on the READ_JSONL branch above --
            # AS alias(col1, col2, ...) column renaming is not supported; AS
            # alias (relation rename only) is unaffected.
            raise NotSupportedError(
                f"READ_CSV('{path}') AS alias(...) is not supported -- only "
                "AS alias (renaming the relation, not its columns) is. Use a "
                "**SELECT** ... AS new_name wrapper to rename individual columns."
            )
        external_names = physical_names

        relation_name = node.alias or f"$read_csv-{random_string()}"

        schema_columns = []
        for physical_name, external_name in zip(physical_names, external_names):
            vector = sample_morsel.column(physical_name.encode("utf-8"))
            physical_type = vector.type
            if physical_type not in _CSV_SUPPORTED_TYPES:
                raise NotSupportedError(
                    f"READ_CSV column '{physical_name}' has inferred type "
                    f"{physical_type!r}, which is not supported (only "
                    "INT64/FLOAT64/VARCHAR/NULL are supported)."
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
        # node.columns deliberately left unset -- see the identical comment on the
        # READ_JSONL branch above; ProjectionPushdownStrategy populates it later.
        node.schema = schema
        # Carried for the physical operator (CsvReadNode): the source path(s) and
        # the file's own (pre-alias) column order/names, mirroring READ_JSONL's
        # jsonl_files/jsonl_physical_columns.
        node.dataset = path
        node.csv_files = csv_files
        node.csv_physical_columns = physical_names
        # Resolved READ_CSV options, carried for the physical operator to pass
        # through to every file's decode.
        node.csv_separator = separator
        node.csv_has_header_row = has_header_row
        node.csv_fail_on_error = fail_on_error
        node.csv_infer_sample_size = infer_sample_size
        # identity -> pre-alias physical (CSV header) name -- see the identical
        # jsonl_physical_by_identity comment on the READ_JSONL branch above.
        node.csv_physical_by_identity = {
            schema_column.identity: physical_name
            for physical_name, schema_column in zip(physical_names, schema_columns)
        }
        # Enables predicate pushdown for a plain col-OP-literal comparison a
        # rugo predicate tuple can express; see CsvPredicatePushable.
        node.connector = CsvPredicatePushable()
    else:
        raise UnsupportedSyntaxError(f"{node.function} cannot be used in place of a table. It returns a value, not a set of rows, so it belongs in the **SELECT** list rather than the **FROM** clause.")
    return node, context


def visit_scan(self, node: Node, context: BindingContext) -> Tuple[Node, BindingContext]:
    import time as _bind_time

    from opteryx.connectors import connector_factory
    from opteryx.exceptions import DatabaseError
    from opteryx.managers.permissions import can_perform_action

    # Captured before the case-fold below so connectors that opt in via
    # requires_original_case (e.g. MabelConnector's preserve_sql_case) can
    # recover exactly what the user typed - once .lower() runs, the original
    # casing is gone for good.
    original_relation = node.relation
    node.relation = node.relation.lower()

    # Internal-only relations back a dedicated SQL surface and must not be
    # addressable by name. `internal_relation` is set by the planner that owns the
    # surface (e.g. plan_show_variables) and is never set from user SQL, so this
    # rejects the typed form without blocking the sanctioned one. Checked before
    # any connector/catalog work so a rejected scan costs nothing.
    from opteryx.connectors.virtual_data_connector import INTERNAL_ONLY_SURFACES

    if node.relation in INTERNAL_ONLY_SURFACES and not node.internal_relation:
        # Name the surface that replaces the one they typed. A generic "use SHOW
        # VARIABLES" would send a `$user` caller to the wrong statement.
        surface = INTERNAL_ONLY_SURFACES[node.relation]
        raise UnsupportedSyntaxError(
            f"'{node.relation}' cannot be queried directly; use `{surface}`."
        )

    # Case-folded: a relation alias is an unquoted SQL identifier, so `FROM t P, t
    # p` collides exactly as `FROM t p, t p` does — the same fold `locate_identifier`
    # applies when resolving a reference (binder.py's `_candidates`). Without this,
    # two same-name-different-case aliases would both register and a later
    # case-insensitive reference to either would find both, raising
    # AmbiguousIdentifierError somewhere downstream instead of this clearer error here.
    if node.alias and node.alias.lower() in {r.lower() for r in context.relations}:
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
    if getattr(gateway, "requires_original_case", False):
        engine_kwargs["original_relation"] = original_relation

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
            # A view is expanded before it reaches here, so the relation being
            # refused can be one the caller never wrote. Name the view they did
            # write, or the refusal reads as being about a table they have never
            # heard of - see relation_resolver, which stamps `via_view`.
            via_view = getattr(node, "via_view", None)
            if via_view:
                raise PermissionError(
                    f"View {via_view} reads {node.relation}, which the user does not "
                    "have permission to read"
                )
            raise PermissionError(f"User does not have permission to read {node.relation}")

    # SHOW MANIFEST FOR exposes file paths/layout, not just data — stricter
    # than READ, and independent of self_governs_permissions (no connector
    # self-governs manifest access the way information_schema self-governs
    # per-row READ).
    if getattr(node, "for_manifest_only", False):
        if not can_perform_action(context.execution_context, node.relation, action="MANIFEST"):
            raise PermissionError(
                f"User does not have permission to view the manifest for {node.relation}"
            )

    if "variables" in dir(node.connector):
        node.connector.variables = context.execution_context.variables
    if gateway.supports_diachronic:
        node.connector.start_date = node.start_date
        node.connector.end_date = node.end_date
    _bind_meta0 = _bind_time.monotonic_ns()
    try:
        # Get dataset schema and build manifest (if supported by connector)
        # For Opteryx catalog connectors, this creates a Manifest with file-level stats
        #
        # A schema-only bind takes the first branch instead, when the connector has one:
        # the Manifest is the file list and per-column statistics for the WHOLE relation
        # and it is the larger of binding's two cloud reads. Nothing before the optimizer
        # reads it, so a caller that stops at the end of binding pays for it and throws
        # it away. `node.manifest` stays None and every later stage that needs one is
        # unreachable from here - see BindingContext.schema_only.
        if node.for_snapshots_only and not context.schema_only:
            # SHOW SNAPSHOTS FOR: the commit history is the result. Neither the
            # Manifest (binding's expensive half) nor the relation's own column
            # schema is read — this Scan contributes the history and nothing
            # else, so the schema it carries is the history's, which is also what
            # the ShowSnapshots node above it emits.
            #
            # Reading the dataset schema here would not just be wasted work, it
            # would be WRONG for a relation with nothing committed: resolving a
            # schema needs a snapshot to resolve it against, and a relation that
            # has never been written to has none. That is an empty history — the
            # accurate answer to this statement — not a failure to read one.
            #
            # A schema_only bind falls through to the branch below instead and
            # leaves `context.snapshots` empty; visit_show_snapshots then refuses
            # rather than reporting an empty history, which would read as "this
            # relation has never been written to".
            from opteryx.models.snapshot_history import snapshots_output_schema

            node.manifest = None
            get_snapshots = getattr(node.connector, "get_snapshots", None)
            # None (no commit log on this connector) is NOT an empty history, and
            # visit_show_snapshots tells the two apart. Storing the absence keeps
            # that distinction rather than flattening it to "no rows".
            context.snapshots[node.alias] = (
                None if get_snapshots is None else get_snapshots()
            )
            node.schema = snapshots_output_schema(node.alias)
        elif context.schema_only and getattr(node.connector, "get_dataset_schema", None) is not None:
            node.schema = node.connector.get_dataset_schema()
            node.manifest = None
        elif getattr(node.connector, "get_dataset_metadata", None) is not None:
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

        if context.schema_only:
            # What the relation HAS, recorded before anything narrows it.
            #
            # `node.schema` is the same object as `context.schemas[alias]`, and
            # binding an aggregate or a projection replaces its `columns` with just
            # the ones that survive (visit_aggregate_and_group, visit_project). So by
            # the time binding returns, a Scan's schema describes what the statement
            # USED, not what the relation offers - `SELECT COUNT(*) FROM t GROUP BY a`
            # leaves a one-column `t` behind.
            #
            # For the reader being offered completions that is the wrong set, and
            # wrong in the direction that hides the columns they have not typed yet.
            # The list is rebound rather than mutated, so holding this one keeps the
            # full width.
            node.unpruned_columns = list(node.schema.columns)

        context.manifests[node.alias] = node.manifest
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

    # A bound Scan reads its whole schema until something proves otherwise. Narrowing
    # that set is ProjectionPushdownStrategy's job and it overwrites this wholesale;
    # supplying the full width HERE is what makes that strategy an optimization rather
    # than a load-bearing planning stage. Without it, `Scan.columns` stayed None all the
    # way to the physical planner whenever pushdown didn't run (its kill-switch set, per
    # opteryx/config.py) and every query died — the pass that PRUNES columns was also
    # the only pass that RESOLVED them.
    #
    # Every other node type the pushdown pass writes `.columns` onto (Subquery, Union,
    # Join, the READ_* FunctionDatasets) already arrives here carrying a column list, so
    # Scan was the only hole.
    node.columns = [
        LogicalColumn(
            node_type=NodeType.IDENTIFIER,
            source_column=column.name,
            source=(column.origin[0] if column.origin else None),
            schema_column=column,
        )
        for column in node.schema.columns
    ]

    return node, context
