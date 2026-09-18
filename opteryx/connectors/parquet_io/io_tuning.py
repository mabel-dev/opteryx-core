"""Single resolution point for the SET-able per-scan Parquet IO knobs.

These are the `VariableOwner.USER` half of the reader variables — what
`variables.py` calls "per-request IO shaping". They resolve default -> env ->
SET through `variables.resolve`, so what `SHOW VARIABLES` advertises and what
the engine runs cannot drift.

This module exists because there are TWO scan paths that must agree:

* the native path (`open_native_scan_plan`, the production default), reached
  from `managers/execution/compiler.py`; and
* the trampoline operator (`operators/parquet_read/parquet_read.pyx`).

Until 2026-09-16 only the trampoline resolved them, so every one of these SETs
was inert on the production path. Duplicating the resolution in the compiler
would have been the obvious fix and the wrong one — two copies drift. Both
callers now resolve here.

SCOPE: every value here is resolved PER SCAN, from that scan's variables. The
tuning structures they feed are per-scan too — `set_http_tuning` /
`set_coalesce_tuning` are methods on the scan's own `ParquetIOPipeline`, and
`HttpTuning` is passed BY VALUE per request (see http_client.hpp) precisely so
one query's override cannot leak into the next. Nothing here is global state.
That is what would let a future per-dataset override (a table hint, or catalog
metadata) supply a different value per scan without any further plumbing.

The `VariableOwner.SERVER` knobs — `parquet_gcs_io_workers`,
`parquet_local_io_workers`, `max_execution_workers` and their caps — are NOT
here and must not be: they size shared thread pools, so they are engine
properties rather than per-scan shaping.
"""

from opteryx import config
from opteryx.variables import resolve as _resolve_var

__all__ = [
    "PER_SCAN_VARIABLES",
    "resolve_coalesce_tuning",
    "resolve_fetch_ahead",
    "resolve_fetch_ahead_gate",
    "resolve_http_tuning",
    "resolve_in_flight_limit",
    "resolve_memory_budget",
]

# The variables a SINGLE SCAN may carry its own value for — the vocabulary a
# table hint (`WITH(parquet_io_fetch_ahead=128)`) is allowed to name.
#
# Membership here is a claim about SCOPE, not about permission: it says the knob
# feeds a structure this scan owns, so two scans in one query can hold different
# values without interfering. Permission is still the variables table's job —
# every one of these is RESTRICTED, and the hint path runs the same
# `check_settable` gate as SET.
#
# `parquet_gcs_io_workers`, `parquet_local_io_workers`, `max_execution_workers`
# and the caps are deliberately ABSENT: they size shared thread pools, so a
# per-scan value is not expressible — the two legs of a join share the pools.
PER_SCAN_VARIABLES = frozenset({
    "disable_http2",
    "disable_http_multiplexing",
    "http_max_connections_per_host",
    "http_max_retries",
    "http_min_bandwidth_mbps",
    "http_pipewait",
    "http_request_timeout_floor_ms",
    "parquet_io_coalesce_max_bytes",
    "parquet_io_coalesce_waste_ratio",
    "parquet_io_fetch_ahead",
    "parquet_io_fetch_ahead_min_row_groups",
    "parquet_io_in_flight_limit",
    "parquet_io_memory_budget_bytes",
})


def _value(name, variables, default, overrides):
    """Resolve one knob: per-scan override first, else default -> env -> SET.

    `overrides` is this scan's validated hint settings (name -> value); the
    names are already checked against PER_SCAN_VARIABLES and through the
    variables permission gate by the time they reach here.
    """
    if overrides is not None and name in overrides:
        return overrides[name]
    return _resolve_var(name, variables, default)


def resolve_http_tuning(variables, overrides=None) -> tuple:
    """The 7-tuple `set_http_tuning` / `CppIOPipeline.__cinit__` expect.

    Bandwidth is stored and SET in Mbps (the human-facing unit) and converted to
    bytes/s here, matching `HttpTuning`'s C++ field.

    The two multiplexing flags are stored as `disable_*` — the state a caller
    normally does NOT want, per variables.py's naming convention — and inverted
    here into the positive sense `HttpTuning` uses.
    """
    min_bw_mbps = _value(
        "http_min_bandwidth_mbps", variables, config.HTTP_MIN_BANDWIDTH_MBPS, overrides
    )
    return (
        _value(
            "http_max_connections_per_host", variables, config.HTTP_MAX_CONNECTIONS_PER_HOST, overrides
        ),
        _value("http_max_retries", variables, config.HTTP_MAX_RETRIES, overrides),
        min_bw_mbps * 1.0e6 / 8.0,
        _value(
            "http_request_timeout_floor_ms", variables, config.HTTP_REQUEST_TIMEOUT_FLOOR_MS, overrides
        ),
        not _value(
            "disable_http_multiplexing", variables, config.DISABLE_HTTP_MULTIPLEXING, overrides
        ),
        _value("http_pipewait", variables, config.HTTP_PIPEWAIT, overrides),
        _value("disable_http2", variables, config.DISABLE_HTTP2, overrides),
    )


def resolve_coalesce_tuning(variables, overrides=None) -> tuple:
    """(waste_ratio, max_bytes) for remote range coalescing — see
    `ParquetIOPipeline::set_coalesce_tuning` for what each bound protects."""
    return (
        _value(
            "parquet_io_coalesce_waste_ratio", variables, config.PARQUET_IO_COALESCE_WASTE_RATIO, overrides
        ),
        _value(
            "parquet_io_coalesce_max_bytes", variables, config.PARQUET_IO_COALESCE_MAX_BYTES, overrides
        ),
    )


def resolve_in_flight_limit(variables, overrides=None) -> int:
    """ABSOLUTE cap on submitted-but-unconsumed row groups; 0 = auto
    (`max(workers, fetch_ahead) + 2`). Absolute rather than a delta so "many
    threads, shallow window" is expressible without a negative value, which
    silently failed to apply in production."""
    return int(
        _value(
            "parquet_io_in_flight_limit", variables, config.PARQUET_IO_IN_FLIGHT_LIMIT, overrides
        )
    )


def resolve_fetch_ahead(variables, overrides=None) -> int:
    """Remote fetch-ahead depth; 0 = off (the coupled path)."""
    return int(_value("parquet_io_fetch_ahead", variables, config.PARQUET_IO_FETCH_AHEAD, overrides))


def resolve_fetch_ahead_gate(variables, overrides=None) -> int:
    """Minimum REMOTE row groups (post-pruning) before the depth above is armed;
    0 = no minimum. A knob of its own, not derived from the depth or the worker
    count — tuning either through the other is what made the worker/window sweep
    unattributable."""
    return int(_value(
        "parquet_io_fetch_ahead_min_row_groups", variables,
        config.PARQUET_IO_FETCH_AHEAD_MIN_ROW_GROUPS, overrides))


# The auto budget's share of the container's (or host's) memory. Half: the scan
# is one consumer among the query's operators — a sort/aggregate downstream of a
# wide scan needs its own room, and the worker OOM this exists to prevent was a
# scan that ran ahead of exactly such a consumer.
_AUTO_MEMORY_BUDGET_FRACTION = 0.5


def resolve_memory_budget(variables, overrides=None) -> int:
    """Memory admission budget for the scan's IO pipeline, in bytes, as the
    pipeline should run it: a positive cap, or 0 for off.

    The knob's own vocabulary is 0 = auto, -1 = off. Auto derives from the
    cgroup limit when one is in effect (the container's real ceiling), else
    physical RAM (bare metal / dev), times _AUTO_MEMORY_BUDGET_FRACTION; when
    neither can be detected there is nothing honest to derive from, and the
    result is off (0) — which the pipeline's `memory_budget_bytes` read-back
    then shows, rather than a guessed number."""
    raw = int(_value(
        "parquet_io_memory_budget_bytes", variables, config.PARQUET_IO_MEMORY_BUDGET_BYTES, overrides))
    if raw > 0:
        return raw
    if raw < 0:
        return 0
    from opteryx.compiled.platform import cgroup_memory_limit_bytes
    from opteryx.compiled.platform import physical_memory_total_bytes

    ceiling = cgroup_memory_limit_bytes() or physical_memory_total_bytes() or 0
    if ceiling <= 0:
        return 0
    return int(ceiling * _AUTO_MEMORY_BUDGET_FRACTION)
