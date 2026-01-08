from opteryx.config import features
from opteryx.exceptions import InvalidInternalStateError
from opteryx.utils.free_threading import is_free_threading_available

from .parallel_engine import execute as parallel_execute
from .serial_engine import ResultType
from .serial_engine import execute as serial_execute

ENABLE_FREE_THREADING = features.enable_free_threading


def execute(plan, telemetry):
    # Check if this plan has a statistics-only result (no execution needed)
    stats_result = getattr(plan, "_statistics_only_result", None)
    if stats_result is not None:
        # Return a generator that yields just the result (no EOS)
        def statistics_only_generator():
            yield stats_result

        return statistics_only_generator(), ResultType.TABULAR

    # Validate query plan to ensure it's acyclic
    if not plan.is_acyclic():
        raise InvalidInternalStateError("Query plan is cyclic, cannot execute.")

    # Label the join legs to ensure left/right ordering
    plan.label_join_legs()

    # Use parallel engine if free-threading is available, otherwise use serial
    if ENABLE_FREE_THREADING and is_free_threading_available():
        # DEBUG: print("\033[38;2;255;184;108mUsing parallel execution engine.\033[0m")
        return parallel_execute(plan, telemetry=telemetry)
    else:
        return serial_execute(plan, telemetry=telemetry)
