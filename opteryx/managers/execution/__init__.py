import gc
from typing import Any, Generator

from opteryx import config
from opteryx.exceptions import InvalidInternalStateError

from .serial_engine import ResultType
from .serial_engine import execute as serial_execute
from .serial_engine import is_special_op

def _with_optional_gc_disabled(
    results: Generator[Any, None, None],
) -> Generator[Any, None, None]:
    """Wrap result iteration with optional GC disable/restore for diagnostics."""
    if not config.OPTERYX_DISABLE_GC_DURING_QUERY:
        yield from results
        return

    gc_was_enabled = gc.isenabled()
    if gc_was_enabled:
        gc.disable()

    try:
        yield from results
    finally:
        if gc_was_enabled:
            gc.enable()


def execute(plan, telemetry):
    # Check if this plan has a statistics-only result (no execution needed)
    stats_result = getattr(plan, "_statistics_only_result", None)
    if stats_result is not None:
        # Return a generator that yields just the result (no EOS)
        def statistics_only_generator():
            yield stats_result

        results, result_type = statistics_only_generator(), ResultType.TABULAR
        return _with_optional_gc_disabled(results), result_type

    # Validate query plan to ensure it's acyclic
    if not plan.is_acyclic():
        raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

    # Label the join legs to ensure left/right ordering
    plan.label_join_legs()

    # Triage by plan head. Non-pipeline special operations (EXPLAIN / SET / SHOW /
    # INSERT / DDL) run on serial_engine — they have no morsel pipeline to drive
    # and never parallelise. EVERY data pipeline (SELECT and friends) is compiled to
    # the native pipeline-graph engine (src/cpp/engine/) and runs 100% natively —
    # THE data executor, no second engine, no fallback. A plan the native engine has
    # no operator for raises NotSupportedError at compile time (hard-cutover posture;
    # see the engine_cutover_decisions memory).
    head_nodes = list(set(plan.get_exit_points()))
    if len(head_nodes) == 1 and is_special_op(plan[head_nodes[0]]):
        results, result_type = serial_execute(plan, telemetry=telemetry)
    else:
        from .compiler import execute_native

        results, result_type = execute_native(plan, telemetry=telemetry)

    if result_type == ResultType.TABULAR:
        return _with_optional_gc_disabled(results), result_type
    return results, result_type
