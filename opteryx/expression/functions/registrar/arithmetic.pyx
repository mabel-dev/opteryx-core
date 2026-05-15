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
from opteryx.types import OrsoTypes


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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.round1,
                        cost_us_per_million=2.0,
                    ),
                ),
                FunctionOverload(
                    id="ROUND_2",
                    parameters=(
                        ParameterSpec(name="num", type_family="numeric"),
                        ParameterSpec(name="precision", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.round2,
                        cost_us_per_million=2.0,
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
                        cost_us_per_million=1.5,
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.ceiling,
                        cost_us_per_million=1.8,
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.floor,
                        cost_us_per_million=1.8,
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.sqrt_value,
                        cost_us_per_million=3.2,
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
    _temporal_unit = ParameterSpec(name="unit", type_family="string", constant_only=True)

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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.INTEGER),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=number_functions.sign_value,
                        cost_us_per_million=1.2,
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="numeric",
                        callable_ref=number_functions.trunc,
                        cost_us_per_million=1.7,
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
