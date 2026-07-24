# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# See the License at http://www.apache.org/licenses/LICENSE-2.0
# Distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND.

"""System variables — the single discoverable surface for engine configuration.

Every tunable is registered here so `SHOW VARIABLES` can answer two questions:
what exists, and what is settable. A knob that is NOT in this table is invisible
to operators and users alike — `http_max_connections_per_host` (formerly the
env-only `OPTERYX_HTTP_MAX_HOST_CONNECTIONS`, sat unreachable in C++, and read
by `get_many()` only, never `get()`) is the example that proved the point.

Resolution is a three-layer chain, in increasing precedence:

    code default  ->  environment variable  ->  SET (session)

`opteryx.config` collapses the first two (`get("NAME", default)` is
default-or-env), and the defaults below are seeded FROM config, so registering a
variable here automatically inherits its env override. The third layer is `SET`,
available only for USER-owned variables.

Owner decides WHO may write the value — it is the write-side authority:
    USER     - the user can `SET` it for their session
    SERVER   - the environment variable sets it; NOT settable mid-query
    INTERNAL - the runtime decides it (detected, derived, or asserted by the
               submitting service); no one sets it by hand

Visibility decides WHO may READ it, and is an independent axis:
    UNRESTRICTED - listed for everyone
    RESTRICTED   - listed only for `platform_admin`, and additionally requires
                   that entitlement to write (see __setitem__)

For variables we're creating and naming, use sensible defaults and if it's a
feature flag, name the variable for the state the user probably doesn't want -
e.g. disable_optimizer (default to False)
"""

from enum import Enum
from typing import Any, Dict, Tuple, Type

from opteryx import config
from opteryx.__version__ import __version__
from opteryx.compiled.simd_probe import cpu_architecture
from opteryx.exceptions import PermissionsError, VariableNotFoundError
from opteryx.types.logical_type import BOOLEAN, FLOAT64, INT64, VARCHAR, ARRAY, VARIANT


class VariableOwner(int, Enum):
    # Manually assign numbers because USER < INTERNAL < SERVER
    SERVER = 30  # set on the server, fixed per instantiation
    INTERNAL = 20  # set by the system, can be updated by the system
    USER = 10  # set by the user, can be updated by the user


class Visibility(str, Enum):
    RESTRICTED = "restricted"  # only visible to platform administrators
    UNRESTRICTED = "unrestricted"  # visible to all users


# The entitlement that both REVEALS RESTRICTED variables in `SHOW VARIABLES` and is
# required to WRITE them. Held by the caller and asserted by the submitting service
# (see ExecutionContext.entitlements) — a user cannot `SET` themselves this, because
# `user_entitlements` is INTERNAL-owned.
#
# Visibility and VariableOwner are independent axes that BOTH gate writes: owner rank
# decides who is eligible at all, and RESTRICTED then additionally demands this
# entitlement regardless of rank. So a USER-owned RESTRICTED variable (e.g.
# `disable_optimizer`) is settable only by a platform admin, while a USER-owned
# UNRESTRICTED one (e.g. `trace`) is settable by anyone.
PLATFORM_ADMIN_ENTITLEMENT = "platform_admin"


VariableSchema = Tuple[Type, Any, VariableOwner, Visibility]

# fmt: off
SYSTEM_VARIABLES_DEFAULTS: Dict[str, VariableSchema] = {
    # ── INTERNAL — the runtime decides these; nobody sets them by hand ──────────
    # Session identity, asserted by the submitting service at session construction.
    # UNRESTRICTED: a caller seeing their own identity is not a disclosure — it is
    # already theirs, and `SELECT * FROM $user` reports it regardless.
    "external_user": (VARCHAR, "", VariableOwner.INTERNAL, Visibility.UNRESTRICTED),
    "user_memberships": (ARRAY(VARIANT), [[]], VariableOwner.INTERNAL, Visibility.UNRESTRICTED),
    # Platform capabilities held by the caller (e.g. `data_admin`). Defaults to EMPTY —
    # an unset entitlement list must never be read as "has everything".
    "user_entitlements": (ARRAY(VARIANT), [[]], VariableOwner.INTERNAL, Visibility.UNRESTRICTED),
    # Detected from the CPU at import; there is no env var and no SET for these.
    "architecture": (ARRAY(VARIANT), cpu_architecture(), VariableOwner.INTERNAL, Visibility.RESTRICTED),
    # UNRESTRICTED: already public via `SELECT VERSION()`, so hiding it from
    # SHOW VARIABLES would conceal nothing while making the two surfaces disagree.
    "version": (VARCHAR, __version__, VariableOwner.INTERNAL, Visibility.UNRESTRICTED),

    # ── USER — settable per session with `SET` ──────────────────────────────────
    # See docs/EXECUTION_TRACING_DESIGN.md. Read fresh per statement (query_session's
    # _execute_statements), so `SET trace TO true; SELECT ...` arms tracing for
    # that SELECT even in the same batch — unlike match_threshold (bind-time only),
    # this is read at statement-dispatch time since the native tracer's gate must
    # be armed before the driver submits, not partway through binding.
    #
    # UNRESTRICTED, deliberately: a user must be able to enable tracing on their OWN
    # query for us to debug/diagnose it — trace output describes execution shape
    # (row-group timings, operator spans), not their data or tenant, so this is not
    # a data-access grant. Do not move this to RESTRICTED without re-checking that
    # reasoning; RESTRICTED now additionally gates WRITE (see
    # SystemVariablesContainer.__setitem__), so doing so would require every
    # caller who needs a trace to hold `platform_admin`.
    "trace": (BOOLEAN, config.OPTERYX_TRACE, VariableOwner.USER, Visibility.UNRESTRICTED),
    "match_threshold": (FLOAT64, config.MATCH_THRESHOLD, VariableOwner.USER, Visibility.UNRESTRICTED),
    # Late-materialization tuning is per-QUERY: the right values depend on this
    # query's predicate selectivity, not on the deployment.
    "parquet_late_materialization_abandon_after": (
        INT64, config.PARQUET_LATE_MATERIALIZATION_ABANDON_AFTER,
        VariableOwner.SERVER, Visibility.RESTRICTED),
    "parquet_late_materialization_max_selectivity": (
        FLOAT64, config.PARQUET_LATE_MATERIALIZATION_MAX_SELECTIVITY,
        VariableOwner.SERVER, Visibility.RESTRICTED),

    # ── USER + RESTRICTED — deployment-shaped, but deliberately SETTABLE for tuning.
    # This is the entire reason this three-layer chain (default -> env -> SET) exists:
    # `parquet_gcs_io_workers` is the exact variable a real investigation swept from
    # 8 to 128 by redeploying six times to find the optimum for one query shape (the
    # answer differs by query — narrow vs wide projection). RESTRICTED so an ordinary
    # caller cannot perturb deployment-wide-looking knobs on their own query, but a
    # `platform_admin` caller CAN — this is the mechanism by which "what happens if
    # we set this to N" gets answered without a redeploy. Do not move these to plain
    # SERVER in a future tidy-up without re-reading this comment.
    "parquet_gcs_io_workers": (INT64, config.PARQUET_GCS_IO_WORKERS, VariableOwner.USER, Visibility.RESTRICTED),
    "parquet_local_io_workers": (INT64, config.PARQUET_LOCAL_IO_WORKERS, VariableOwner.USER, Visibility.RESTRICTED),
    "max_execution_workers": (INT64, config.MAX_EXECUTION_WORKERS, VariableOwner.USER, Visibility.RESTRICTED),
    # Same reasoning as the three above, for the HTTP client that services GCS
    # range reads (src/cpp/http_client.cpp / rugo/src/parquet/io_pipeline.hpp).
    # Named for what they DO, not for the env var they replace — the old env-only
    # names (OPTERYX_HTTP_MIN_BW_MBPS etc.) were too terse to be self-explanatory
    # on a `SHOW VARIABLES` a caller has never seen before.
    "http_max_connections_per_host": (
        INT64, config.HTTP_MAX_CONNECTIONS_PER_HOST, VariableOwner.USER, Visibility.RESTRICTED),
    "http_max_retries": (
        INT64, config.HTTP_MAX_RETRIES, VariableOwner.USER, Visibility.RESTRICTED),
    "http_min_bandwidth_mbps": (
        FLOAT64, config.HTTP_MIN_BANDWIDTH_MBPS, VariableOwner.USER, Visibility.RESTRICTED),
    "http_request_timeout_floor_ms": (
        INT64, config.HTTP_REQUEST_TIMEOUT_FLOOR_MS, VariableOwner.USER, Visibility.RESTRICTED),
    # HTTP/2 multiplexing. Named for the state a caller does NOT want (per the
    # convention at the top of this file), so the default-False state is the fast
    # one. `disable_http_multiplexing` exists to A/B the CURLOPT_PIPEWAIT fix
    # against the old connection-per-range behaviour WITHOUT a redeploy — the
    # same reason parquet_gcs_io_workers is settable. `disable_http2` is a
    # diagnostic, not a performance knob: it pins HTTP/1.1 so the contribution of
    # h2 can be measured (a low connection cap should then become catastrophic
    # rather than faster). See HttpTuning in src/cpp/http_client.hpp.
    "disable_http_multiplexing": (
        BOOLEAN, config.DISABLE_HTTP_MULTIPLEXING, VariableOwner.USER, Visibility.RESTRICTED),
    "disable_http2": (
        BOOLEAN, config.DISABLE_HTTP2, VariableOwner.USER, Visibility.RESTRICTED),
    # Splits submission depth from thread count — `parquet_gcs_io_workers` alone
    # moves both, so the sweep that found 16 optimal could not attribute the win
    # to concurrency vs pipelining depth. Also drives IO pool size.
    "parquet_io_in_flight_headroom": (
        INT64, config.PARQUET_IO_IN_FLIGHT_HEADROOM, VariableOwner.USER, Visibility.RESTRICTED),

    # ── SERVER (informational) — these DECLARE system behaviour to a client ─────
    # Not read by the engine, and that is not a reason to drop them: they are an
    # interface contract. A client asks "what character set will I get back?", "when
    # will you time me out?" — and the answer has to be published somewhere. Their
    # value is the ANSWER, so it must track real behaviour; a stale entry here is a
    # lie to every client that reads it.
    "character_set_client": (VARCHAR, "utf8", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "system_time_zone": (VARCHAR, "UTC", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "sql_mode": (VARCHAR, "opteryx", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "default_storage_engine": (VARCHAR, "rugo-parquet", VariableOwner.SERVER, Visibility.UNRESTRICTED),
    # Seconds. Mirrors jobs.opteryx's JOB_MAX_RUNTIME (timedelta(minutes=20)) — a job
    # still non-terminal past this is reported FAILED. Declared in ANOTHER REPO, so
    # nothing here can catch it drifting; re-check both if either moves.
    # NOTE: MySQL's variable of this name is MILLIseconds, so a MySQL-speaking client
    # reads 1200 as 1.2s rather than 20 minutes.
    "max_execution_time": (INT64, 1200, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    # Characters. Mirrors jobs.opteryx's submit guard, which rejects a longer
    # sql_text with HTTP 400 before the engine ever sees it. Same cross-repo
    # caveat as max_execution_time above.
    "max_sql_length": (INT64, 256_000, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    # INT32 max (2**30 - 1). NOT ENFORCED YET — this currently only DECLARES the
    # intended ceiling; nothing caps a result set at this value today. Wiring the
    # enforcement is deliberately separate work.
    "sql_select_limit": (INT64, 1073741824, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    # Days a JOB RECORD (the metadata: status, telemetry, error) survives. Mirrors
    # jobs.opteryx's JOB_TTL_DAYS, which sets each job's `purge_at`.
    "job_retention_days": (INT64, 14, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    # Days RESULTS stay downloadable — SHORTER than the record's lifetime above, so a
    # job can still be inspected after its data has gone. Mirrors the jobs service
    # withholding the results URL once a job started more than 7 days ago.
    "result_retention_days": (INT64, 7, VariableOwner.SERVER, Visibility.UNRESTRICTED),

    # ── SERVER — the environment variable sets these; not settable mid-query ────
    # There is no practical reason to disable the optimizer mid-query, and the read
    # site binds `config.DISABLE_OPTIMIZER` by value at import
    # (planner/optimizer/__init__.py), so a per-session value could not have taken
    # effect anyway — as a USER variable this was a `SET` that silently did nothing.
    # SERVER makes it honest: env-set at startup, discoverable, not settable.
    # RESTRICTED because it is documented **DANGEROUS** (most queries fail with it on).
    "disable_optimizer": (BOOLEAN, config.DISABLE_OPTIMIZER, VariableOwner.SERVER, Visibility.RESTRICTED),
    # Deployment shape: where the caches live, which project.
    "array_agg_max_values_per_group": (INT64, config.ARRAY_AGG_MAX_VALUES_PER_GROUP, VariableOwner.SERVER, Visibility.UNRESTRICTED),
    "max_consecutive_cache_failures": (INT64, config.MAX_CONSECUTIVE_CACHE_FAILURES, VariableOwner.SERVER, Visibility.RESTRICTED),
    "local_store_root": (VARCHAR, config.LOCAL_STORE_ROOT, VariableOwner.SERVER, Visibility.RESTRICTED),
    "manifest_cache_path": (VARCHAR, config.MANIFEST_CACHE_PATH, VariableOwner.SERVER, Visibility.RESTRICTED),
    "manifest_cache_bytes": (INT64, config.MANIFEST_CACHE_BYTES, VariableOwner.SERVER, Visibility.RESTRICTED),
    "manifest_remote_location": (VARCHAR, config.MANIFEST_REMOTE_LOCATION, VariableOwner.SERVER, Visibility.RESTRICTED),
    "footer_remote_location": (VARCHAR, config.FOOTER_REMOTE_LOCATION, VariableOwner.SERVER, Visibility.RESTRICTED),
    "kvstore_location": (VARCHAR, config.KVSTORE_LOCATION, VariableOwner.SERVER, Visibility.RESTRICTED),
    "kvstore_key_prefix": (VARCHAR, config.KVSTORE_KEY_PREFIX, VariableOwner.SERVER, Visibility.RESTRICTED),
    # Credentials-adjacent: names the cloud project this deployment reads from.
    "gcp_project_id": (VARCHAR, config.GCP_PROJECT_ID or "", VariableOwner.SERVER, Visibility.RESTRICTED),
    # Diagnostics — same class as `trace`, but process-wide rather than per-query,
    # so they are env-set rather than SET-able.
    "opteryx_debug": (BOOLEAN, config.OPTERYX_DEBUG, VariableOwner.SERVER, Visibility.RESTRICTED),
    "instrument_engine": (BOOLEAN, config.OPTERYX_INSTRUMENT_ENGINE, VariableOwner.SERVER, Visibility.RESTRICTED),
    "disable_gc_during_query": (BOOLEAN, config.OPTERYX_DISABLE_GC_DURING_QUERY, VariableOwner.SERVER, Visibility.RESTRICTED),
    "validate_optimizer_plans": (BOOLEAN, config.VALIDATE_OPTIMIZER_PLANS, VariableOwner.SERVER, Visibility.RESTRICTED),
}
# fmt: on


class SystemVariablesContainer:
    def __init__(self, owner: VariableOwner = VariableOwner.USER):
        self._variables = SYSTEM_VARIABLES_DEFAULTS.copy()
        self._owner = owner

    def __getitem__(self, key: str) -> Any:
        if key not in self._variables:
            raise VariableNotFoundError(key)
        return self._variables[key][1]

    def __setitem__(self, key: str, value: Any) -> None:
        if key[0] == "@":
            variable_type = value.type
            owner = VariableOwner.USER
            visibility = Visibility.UNRESTRICTED
        else:
            if key not in self._variables:
                from opteryx.utils import suggest_alternative

                suggestion = suggest_alternative(key, list(self._variables.keys()))

                raise VariableNotFoundError(variable=key, suggestion=suggestion)
            variable_type, _, owner, visibility = self._variables[key]
            if owner > self._owner:
                raise PermissionsError(f"User does not have permission to set variable `{key}`")
            # A RESTRICTED variable requires `platform_admin` to WRITE, independent of
            # (and in addition to) the owner-rank check above. This is deliberately
            # separate from Visibility's read-side gate in variables_data.py: a
            # RESTRICTED-but-USER-owned variable (e.g. `disable_optimizer`) would
            # otherwise be settable by any caller purely by owner rank, with no
            # entitlement check at all — visibility said "don't list it for
            # non-admins" but said nothing about writes. `trace` is UNRESTRICTED
            # specifically so it is NOT caught by this — see its comment above.
            if visibility == Visibility.RESTRICTED and not self._caller_is_platform_admin():
                raise PermissionsError(
                    f"Setting `{key}` requires the `{PLATFORM_ADMIN_ENTITLEMENT}` entitlement."
                )
            if variable_type != value.type:
                raise ValueError(f"Invalid type for `{key}`, {variable_type} expected.")

        self._variables[key] = (variable_type, value.value, owner, visibility)

    def _caller_is_platform_admin(self) -> bool:
        """Whether this container's caller holds `platform_admin`.

        Fails closed: a container with no `user_entitlements` entry (or a value
        that doesn't behave like an iterable of names) reads as "not an admin",
        never as "unknown, so allow it".
        """
        if "user_entitlements" not in self._variables:
            return False
        held = self._variables["user_entitlements"][1]
        if callable(getattr(held, "to_pylist", None)):
            held = held.to_pylist()
        if not isinstance(held, (list, tuple, set, frozenset)):
            return False
        return PLATFORM_ADMIN_ENTITLEMENT in held

    def details(self, key: str) -> VariableSchema:
        if key not in self._variables:
            raise VariableNotFoundError(key)
        return self._variables[key]

    def __contains__(self, key: str) -> bool:
        return key in self._variables

    def __iter__(self):
        return iter(self._variables)

    def __len__(self):
        return len(self._variables)

    def snapshot(self, owner: VariableOwner = VariableOwner.USER) -> "SystemVariablesContainer":
        return SystemVariablesContainer(owner)

    def as_column(self, key: str):
        """Return a variable as a CONSTANT column"""
        from opteryx.types.schema import ConstantColumn

        # system variables aren't stored with the @@
        variable = self._variables[key[2:]] if key.startswith("@@") else self._variables.get(key)
        if not variable:
            raise VariableNotFoundError(key)
        return ConstantColumn(name=key, column_type=variable[0], value=variable[1])


# load the base set
SystemVariables = SystemVariablesContainer(VariableOwner.INTERNAL)


def resolve(name: str, variables, default=None):
    """Resolve a tunable through the full chain: code default -> env -> SET.

    THE single resolution point. Call this from a read site instead of reading
    `config.X` directly, so that the value a `SHOW VARIABLES` row advertises and the
    value the engine actually uses cannot drift apart — if they can drift,
    `SHOW VARIABLES` lies.

    The first two layers are already collapsed by the time we get here: the defaults
    table is seeded from `opteryx.config`, whose `get()` is default-or-env. So the
    session container IS the resolved value, and this reads it.

    Parameters:
        name: the variable name as registered in SYSTEM_VARIABLES_DEFAULTS.
        variables: the session's SystemVariablesContainer (typically
            `self.properties.variables` inside an operator). A caller with no
            session (EXPLAIN-only paths, direct-construction tests) may pass
            None/{} and gets `default`.
        default: value when this session carries no such variable. Passing the
            `config.X` constant keeps a read site working for callers that have no
            session at all.
    """
    if not variables or name not in variables:
        return default
    return variables[name]
