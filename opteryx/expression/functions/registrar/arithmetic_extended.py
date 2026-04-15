from pyarrow import compute

from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
from opteryx.expression.functions.implementations import arithmetic as number_functions
from opteryx.expression.functions.implementations import temporal as date_functions

# Helper from package-level registrar for concise entries
from opteryx.expression.functions.registrar import _make
from opteryx.types import OrsoTypes


def get_builtin_arithmetic_extended_functions() -> list[FunctionDefinition]:
    """Numeric functions not in the core arithmetic group."""
    # Parameter shortcuts
    _num = ParameterSpec(name="num", type_family="numeric")
    _date_value = ParameterSpec(name="value", type_family="date")
    _timestamp_value = ParameterSpec(name="value", type_family="timestamp")
    _temporal_unit = ParameterSpec(name="unit", type_family="string", constant_only=True)

    return [
        _make(
            "SIGN",
            getattr(compute, "sign"),
            OrsoTypes.INTEGER,
            (_num,),
            summary="Sign of number (-1, 0, 1).",
            cost=327.02,
        ),
        FunctionDefinition(
            name="TRUNC",
            aliases=("TRUNCATE",),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Truncate a numeric or temporal value.",
            documentation="Truncates numeric values toward zero or temporal values to the start of a unit.",
            overloads=(
                FunctionOverload(
                    id="TRUNC_numeric",
                    parameters=(
                        _num,
                        ParameterSpec(
                            name="scale", type_family="integer", variadic=True, optional=True
                        ),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        engine="arrow",
                        id="numeric",
                        callable_ref=number_functions.trunc,
                        cost_us_per_million=142.78,
                    ),
                ),
                FunctionOverload(
                    id="TRUNC_date",
                    parameters=(
                        _date_value,
                        _temporal_unit,
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.TIMESTAMP),
                    kernel=KernelSpec(
                        engine="draken",
                        id="date",
                        callable_ref=date_functions.trunc_date,
                        cost_us_per_million=0.92,
                    ),
                ),
                FunctionOverload(
                    id="TRUNC_timestamp",
                    parameters=(
                        _timestamp_value,
                        _temporal_unit,
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.TIMESTAMP),
                    kernel=KernelSpec(
                        engine="draken",
                        id="timestamp",
                        callable_ref=date_functions.trunc_timestamp,
                        cost_us_per_million=0.96,
                    ),
                ),
            ),
        ),
        _make(
            "POWER",
            number_functions.safe_power,
            OrsoTypes.DOUBLE,
            (_num, ParameterSpec(name="exp", type_family="numeric")),
            cost=0.86,
            summary="Raise base to exponent (SQL-92).",
        ),
        _make(
            "LOG",
            number_functions.log,
            OrsoTypes.DOUBLE,
            (_num, ParameterSpec(name="base", type_family="numeric")),
            summary="Logarithm with arbitrary base.",
            cost=1.04,
        ),
    ]
