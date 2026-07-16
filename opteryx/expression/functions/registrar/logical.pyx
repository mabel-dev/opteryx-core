from typing import List

from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
from opteryx.expression.functions.implementations.logical import null_if as _lf_null_if



def get_builtin_logical_functions() -> List[FunctionDefinition]:
    """
    Logical and control-flow function registrar entries.

    Provides:
      - COALESCE (variadic, resolver-based return)
      - IFNULL / IFNOTNULL
      - NULLIF
      - IIF (vectorized conditional)
      - _PASSTHRU (utility, for tests/compat)
    """

    # Small adapter object bundling kernels implemented elsewhere
    class other_functions:
        null_if = staticmethod(_lf_null_if)

    # COALESCE/IFNULL/IFNOTNULL/IIF are c-native: the bytecode builder resolves
    # draken_{name} from the kernel registry and sets BC_INSTR_C_NATIVE, and every
    # VM arm gates on that flag before reading callable_ref. There is no Python
    # fallback (and never a silent one — an unsupported operand type fails loud in
    # the kernel), so callable_ref is None rather than a dead nanobind binding.
    _variadic_any = (
        ParameterSpec(name="arg0", type_family="any"),
        ParameterSpec(name="args", type_family="any", variadic=True, optional=True),
    )

    return [
        FunctionDefinition(
            name="COALESCE",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return first non-null argument.",
            documentation="Returns the first non-null value from the list of arguments.",
            overloads=(
                FunctionOverload(
                    id="COALESCE_variadic",
                    parameters=_variadic_any,
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_coalesce
                        cost_us_per_million=3386.42,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IFNULL",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return value if not null, else default.",
            documentation="Returns first argument if not null, otherwise returns second argument.",
            overloads=(
                FunctionOverload(
                    id="IFNULL_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="default", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_ifnull
                        cost_us_per_million=3737.57,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IFNOTNULL",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return second argument if first is not null.",
            documentation="Returns second argument if first argument is not null, otherwise null.",
            overloads=(
                FunctionOverload(
                    id="IFNOTNULL_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="result", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_ifnotnull
                        cost_us_per_million=3782.80,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="NULLIF",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return null if equal, else first value.",
            documentation="Returns null if arguments are equal, otherwise returns first argument.",
            overloads=(
                FunctionOverload(
                    id="NULLIF_1",
                    parameters=(
                        ParameterSpec(name="value", type_family="any"),
                        ParameterSpec(name="compare", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=other_functions.null_if,
                        cost_us_per_million=0.72,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="IIF",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Inline if: return second or third arg based on condition.",
            documentation="Returns second argument if condition is true, otherwise third argument.",
            overloads=(
                FunctionOverload(
                    id="IIF_1",
                    parameters=(
                        ParameterSpec(name="condition", type_family="boolean"),
                        ParameterSpec(name="true_value", type_family="any"),
                        ParameterSpec(name="false_value", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_iif_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_iif
                        cost_us_per_million=3689.18,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="_PASSTHRU",
            aliases=(),
            category="logical",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return input unchanged.",
            documentation="Returns the input value unchanged. Used for testing and compatibility.",
            overloads=(
                FunctionOverload(
                    id="_PASSTHRU_1",
                    parameters=(ParameterSpec(name="value", type_family="any"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=lambda x: x,
                        cost_us_per_million=-0.017,
                    ),
                ),
            ),
        ),
    ]


__all__ = ["get_builtin_logical_functions"]
