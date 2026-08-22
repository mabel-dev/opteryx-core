from typing import List

from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)


def get_builtin_temporal_extra_functions() -> List[FunctionDefinition]:
    """Temporal functions with parameter-dependent return types.

    The actual kernel implementations live in the `implementations.temporal`
    module.

    EXTRACT's return type used to be computed by a resolver over the `part`
    literal, for parts that produced a `double` (`julian`) or a `date` (`date`).
    Neither part exists: the `part` domain below is CLOSED, and every part in it
    returns an INT64, so the resolver only hid a fixed type behind a function
    call. It is declared fixed here instead.
    """
    # Local import to avoid heavy top-level imports / circular deps.
    from opteryx.expression.functions.implementations import temporal as date_functions

    return [
        FunctionDefinition(
            name="EXTRACT",
            aliases=(),
            category="temporal",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Extract a part from a date/timestamp.",
            documentation=(
                "Extracts a named part from a date or timestamp. The supported parts are "
                "year, quarter, month, day, hour, minute, second and epoch - the list is "
                "closed, and a part outside it is refused."
            ),
            overloads=(
                FunctionOverload(
                    id="EXTRACT_2",
                    parameters=(
                        ParameterSpec(
                            name="part",
                            type_family="string",
                            constant_only=True,
                            # The parts draken_date_part actually implements,
                            # plus `epoch`. This IS the closed set: `week`,
                            # `dow`, `doy`, `julian` and `date` are NOT among
                            # them; each is refused as "outside the c-native
                            # kernel set".
                            #
                            # `epoch` has no draken_date_part part id and never
                            # reaches that kernel — EXTRACT(EPOCH FROM x) is
                            # normalised to UNIXTIME(x) while the logical plan is
                            # built, which is the same value and is native. It is
                            # in the domain because it is accepted SQL, not
                            # because the kernel answers it.
                            domain=(
                                "year", "quarter", "month", "day",
                                "hour", "minute", "second", "epoch",
                            ),
                            documentation=(
                                "The part to extract. Sub-day parts (hour, minute, second) "
                                "require a TIMESTAMP operand — over a DATE the kernel refuses "
                                "them ('sub-day part of a DATE'), so a DATE operand accepts "
                                "only year, quarter, month, day and epoch. `epoch` is Unix "
                                "epoch SECONDS as an INTEGER, identical to TO_UNIXTIME."
                            ),
                        ),
                        ParameterSpec(name="date", type_family="temporal"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=date_functions.date_part,
                        cost_us_per_million=4761.27,
                    ),
                ),
                FunctionOverload(
                    id="EXTRACT_INT",
                    parameters=(
                        ParameterSpec(name="part", type_family="string", constant_only=True),
                        ParameterSpec(name="date", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
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
