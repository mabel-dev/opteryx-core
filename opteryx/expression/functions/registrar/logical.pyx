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
    """

    # Small adapter object bundling kernels implemented elsewhere
    class other_functions:
        null_if = staticmethod(_lf_null_if)

    # IFNULL/IFNOTNULL share COALESCE's branch-type resolver+validation
    # (_coalesce_return_type, defined in the package __init__ this file is
    # `include`d into) but need their own name in the error it raises.
    def _ifnull_return_type(arg_nodes):
        return _coalesce_return_type(arg_nodes, func_name="IFNULL")

    def _ifnotnull_return_type(arg_nodes):
        return _coalesce_return_type(arg_nodes, func_name="IFNOTNULL")

    # COALESCE/IFNULL/IFNOTNULL/IIF are c-native: the bytecode builder resolves
    # draken_{name} from the kernel registry and sets BC_INSTR_C_NATIVE, and every
    # VM arm gates on that flag before reading callable_ref. There is no Python
    # fallback (and never a silent one — an unsupported operand type fails loud in
    # the kernel), so callable_ref is None rather than a dead nanobind binding.
    # The null-conditional family's branches are typed `any`, which overstates
    # them twice over. `_check_blend_compatible` (registrar/__init__.pyx) mirrors
    # nc_dispatch: the branches must be ALL BOOLEAN, ALL string, or a blendable
    # fixed-width numeric/temporal mix — so they must agree with each other
    # (`homogeneous=True` on every overload below), and DECIMAL is not in any of
    # those families at all (`excludes`). types.json puts DECIMAL squarely in the
    # numeric family, so "any" reads as a promise that
    # `IFNULL(decimal_col, decimal_col)` works; it does not.
    def _blend(name: str) -> ParameterSpec:
        return ParameterSpec(
            name=name,
            type_family="any",
            excludes=("DECIMAL",),
            documentation=(
                "A branch value. All branches must share one blendable family — all "
                "BOOLEAN, all string, or a numeric/temporal scalar mix. DECIMAL is not "
                "blendable; CAST it to DOUBLE first."
            ),
        )

    _variadic_any = (
        _blend("arg0"),
        ParameterSpec(
            name="args",
            type_family="any",
            variadic=True,
            optional=True,
            excludes=("DECIMAL",),
            documentation="Further branch values, of the same blendable family as `arg0`.",
        ),
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
                    homogeneous=True,
                    return_spec=ReturnSpec(mode="resolver", resolver=_coalesce_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_coalesce
                        cost_us_per_million=3144.69,
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
                    parameters=(_blend("value"), _blend("default")),
                    homogeneous=True,
                    return_spec=ReturnSpec(mode="resolver", resolver=_ifnull_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_ifnull
                        cost_us_per_million=3288.64,
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
                    parameters=(_blend("value"), _blend("result")),
                    homogeneous=True,
                    return_spec=ReturnSpec(mode="resolver", resolver=_ifnotnull_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_ifnotnull
                        cost_us_per_million=2822.13,
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
                    # NULLIF lowers to IIF(a = b, NULL, a) at plan-build time, so
                    # it inherits IIF's blend rule on top of needing `a` and `b`
                    # comparable. A DECIMAL operand fails at execution (err_op=15).
                    parameters=(_blend("value"), _blend("compare")),
                    homogeneous=True,
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
                        _blend("true_value"),
                        _blend("false_value"),
                    ),
                    homogeneous=True,
                    return_spec=ReturnSpec(mode="resolver", resolver=_iif_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,   # c-native: draken_iif
                        cost_us_per_million=3589.92,
                    ),
                ),
            ),
        ),
    ]


__all__ = ["get_builtin_logical_functions"]
