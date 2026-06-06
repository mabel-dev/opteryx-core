from typing import List

from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
from opteryx.types.logical_type import LogicalCategory


def get_builtin_temporal_extra_functions() -> List[FunctionDefinition]:
    """Temporal functions with parameter-dependent return types.

    This module provides registrar entries for temporal functions whose return
    type depends on input parameters (e.g. EXTRACT/DATEPART). The actual kernel
    implementations live in the `implementations.temporal` module.
    """
    # Local import to avoid heavy top-level imports / circular deps.
    from opteryx.expression.functions.implementations import temporal as date_functions
    from opteryx.expression.functions.registrar import _datepart_return_type

    return [
        FunctionDefinition(
            name="EXTRACT",
            aliases=(),
            category="temporal",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Extract a part from a date/timestamp.",
            documentation="Extracts a named part (year, month, day, epoch, etc.) from a date or timestamp.",
            overloads=(
                FunctionOverload(
                    id="EXTRACT_2",
                    parameters=(
                        ParameterSpec(name="part", type_family="string", constant_only=True),
                        ParameterSpec(name="date", type_family="temporal"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_datepart_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=date_functions.date_part,
                        cost_us_per_million=0.87,
                    ),
                ),
                FunctionOverload(
                    id="EXTRACT_INT",
                    parameters=(
                        ParameterSpec(name="part", type_family="string", constant_only=True),
                        ParameterSpec(name="date", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_datepart_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=date_functions.date_part,
                        cost_us_per_million=0.97,
                    ),
                ),
            ),
        ),
    ]
