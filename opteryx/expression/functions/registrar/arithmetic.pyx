"""Arithmetic registrar: combined core + extended arithmetic function definitions.

Provides all arithmetic and numeric functions (rounding, magnitude, exponentiation, etc.)
with Draken vector kernels.
"""

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
# LogicalCategory imported via __init__.pyx (textually included); canonical ColumnTypes also in scope.


def get_builtin_arithmetic_functions() -> list[FunctionDefinition]:
    """Arithmetic and numeric functions (core)."""
    return [
        FunctionDefinition(
            name="ROUND",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Round to nearest integer.",
            documentation="Rounds input number to nearest integer or specified decimal places.",
            overloads=(
                FunctionOverload(
                    id="ROUND_1",
                    parameters=(ParameterSpec(name="num", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.round1,
                        cost_us_per_million=862.39,
                    ),
                ),
                FunctionOverload(
                    id="ROUND_2",
                    parameters=(
                        ParameterSpec(name="num", type_family="numeric"),
                        ParameterSpec(name="precision", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.round2,
                        cost_us_per_million=2575.73,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="ABS",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Absolute value.",
            documentation="Returns absolute value of input number.",
            overloads=(
                FunctionOverload(
                    id="ABS_1",
                    parameters=(ParameterSpec(name="num", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.abs_value,
                        cost_us_per_million=562.56,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="CEILING",
            aliases=("CEIL",),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Round up to nearest integer.",
            documentation="Returns smallest integer greater than or equal to input.",
            overloads=(
                FunctionOverload(
                    id="CEILING_1",
                    parameters=(
                        ParameterSpec(name="num", type_family="numeric"),
                        ParameterSpec(
                            name="scale", type_family="integer", variadic=True, optional=True
                        ),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.ceiling,
                        cost_us_per_million=2525.34,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="FLOOR",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Round down to nearest integer.",
            documentation="Returns largest integer less than or equal to input.",
            overloads=(
                FunctionOverload(
                    id="FLOOR_1",
                    parameters=(
                        ParameterSpec(name="num", type_family="numeric"),
                        ParameterSpec(
                            name="scale", type_family="integer", variadic=True, optional=True
                        ),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.floor,
                        cost_us_per_million=2577.53,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="SQRT",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Square root.",
            documentation="Returns square root of input number.",
            overloads=(
                FunctionOverload(
                    id="SQRT_1",
                    parameters=(ParameterSpec(name="num", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.sqrt_value,
                        cost_us_per_million=549.22,
                    ),
                ),
            ),
        ),
    ]


def get_builtin_arithmetic_extended_functions() -> list[FunctionDefinition]:
    """Numeric functions not in the core arithmetic group (extended)."""
    # Parameter shortcuts
    _num = ParameterSpec(name="num", type_family="numeric")
    _date_value = ParameterSpec(name="value", type_family="date")
    _timestamp_value = ParameterSpec(name="value", type_family="timestamp")
    _temporal_unit = ParameterSpec(
        name="unit",
        type_family="string",
        constant_only=True,
        # The truncation boundaries draken_date_trunc implements. Narrower than
        # DATEDIFF's part set (millisecond/microsecond are differences, not
        # boundaries) — the two were previously both just "a unit string".
        domain=("year", "quarter", "month", "week", "day", "hour", "minute", "second"),
    )

    return [
        FunctionDefinition(
            name="SIGN",
            aliases=(),
            category="arithmetic",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Sign of number (-1, 0, 1).",
            documentation="Returns -1 for negative, 0 for zero, 1 for positive.",
            overloads=(
                FunctionOverload(
                    id="SIGN_1",
                    parameters=(_num,),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.sign_value,
                        cost_us_per_million=890.70,
                    ),
                ),
            ),
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="numeric",
                        callable_ref=number_functions.trunc,
                        cost_us_per_million=2560.56,
                    ),
                ),
                FunctionOverload(
                    id="TRUNC_date",
                    parameters=(
                        _date_value,
                        _temporal_unit,
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_TIMESTAMP()),
                    kernel=KernelSpec(
                        engine="draken",
                        id="date",
                        callable_ref=date_functions.trunc_date,
                        cost_us_per_million=1126.66,
                    ),
                ),
                FunctionOverload(
                    id="TRUNC_timestamp",
                    parameters=(
                        _timestamp_value,
                        _temporal_unit,
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_TIMESTAMP()),
                    kernel=KernelSpec(
                        engine="draken",
                        id="timestamp",
                        callable_ref=date_functions.trunc_timestamp,
                        cost_us_per_million=1035.01,
                    ),
                ),
            ),
        ),
        _make(
            "POWER",
            number_functions.safe_power,
            _CT_FLOAT64,
            (_num, ParameterSpec(name="exp", type_family="numeric")),
            cost=6247.94,
            summary="Raise base to exponent (SQL-92).",
        ),
        _make(
            "LOG",
            number_functions.log,
            _CT_FLOAT64,
            (_num, ParameterSpec(name="base", type_family="numeric")),
            summary="Logarithm with arbitrary base.",
            cost=4113.33,
        ),
    ]
