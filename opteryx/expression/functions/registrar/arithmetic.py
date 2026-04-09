from orso.types import OrsoTypes
from pyarrow import compute

from opteryx.expression.functions import FunctionDefinition
from opteryx.expression.functions import FunctionOverload
from opteryx.expression.functions import KernelSpec
from opteryx.expression.functions import LifecycleSpec
from opteryx.expression.functions import ParameterSpec
from opteryx.expression.functions import ReturnSpec
from opteryx.expression.functions.implementations import arithmetic as number_functions


def get_builtin_arithmetic_functions() -> list[FunctionDefinition]:
    """Arithmetic and numeric functions."""
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
                        engine="arrow",
                        id="default",
                        callable_ref=compute.abs,
                        cost_us_per_million=221.0,
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
                        engine="arrow",
                        id="default",
                        callable_ref=number_functions.ceiling,
                        cost_us_per_million=138.0,
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
                        engine="arrow",
                        id="default",
                        callable_ref=number_functions.floor,
                        cost_us_per_million=135.0,
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
                        engine="arrow",
                        id="default",
                        callable_ref=compute.sqrt,
                        cost_us_per_million=242.0,
                    ),
                ),
            ),
        ),
    ]
