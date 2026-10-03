from opteryx.expression.functions import ParameterSpec

# Use package-level helper to construct concise FunctionDefinition entries.
# LogicalCategory imported via __init__.pyx (textually included); canonical ColumnTypes also in scope.


def get_builtin_temporal_functions() -> list:
    """Full temporal function set (registrar entries)."""

    # Parameter shortcuts
    _part = ParameterSpec(
        name="part",
        type_family="string",
        constant_only=True,
        # DATEDIFF's unit set — verified against the engine, not copied from the
        # prose. It is WIDER than TRUNC's and TIME_BUCKET's (millisecond and
        # microsecond are differences, not truncation boundaries), which is
        # exactly why the domain belongs on the parameter and not in a shared
        # "date parts" paragraph.
        # Singular and plural, as _DIFF_PARTS in compiled_expression.pyx reads them.
        domain=(
            "year", "years", "quarter", "quarters", "month", "months",
            "week", "weeks", "day", "days", "hour", "hours",
            "minute", "minutes", "second", "seconds",
            "millisecond", "milliseconds", "microsecond", "microseconds",
        ),
    )
    # `datetime`, not `temporal`: every kernel below takes DATE or TIMESTAMP
    # only, and `temporal` would bind TIME/INTERVAL straight into a raw
    # kernel TypeError.
    _not_calendar = ("TIME", "INTERVAL")
    _date = ParameterSpec(name="date", type_family="datetime", excludes=_not_calendar)

    # Implementations are provided by the temporal implementation module.
    from opteryx.expression.functions.implementations import temporal as date_functions

    return [
        _make(
            "TIME_BUCKET",
            date_functions.date_floor,
            _CT_TIMESTAMP(),
            (
                ParameterSpec(
                    name="magnitude",
                    type_family="numeric",
                    # A bucket WIDTH: a whole number of `units`, at least one.
                    # `numeric` alone made -125533.0000 a legal argument, and the
                    # engine then answered with a raw
                    # `TypeError: Failed to extract integer scalar from constant
                    # vector` for the DECIMAL and a bare "outside the c-native
                    # kernel set" refusal for the negative. A FLOAT magnitude IS
                    # accepted (2.0 buckets by 2), so the exclusion is DECIMAL
                    # specifically, not "non-integer".
                    minimum=1,
                    excludes=("DECIMAL",),
                ),
                ParameterSpec(
                    name="units",
                    type_family="string",
                    constant_only=True,
                    # Singular and plural, as _BUCKET_UNITS in
                    # compiled_expression.pyx reads them.
                    domain=(
                        "year", "years", "quarter", "quarters", "month", "months",
                        "week", "weeks", "day", "days", "hour", "hours",
                        "minute", "minutes", "second", "seconds",
                    ),
                ),
                _date,
            ),
            summary="Bucket date into fixed-width intervals.",
            cost=1.08,
        ),
        _make(
            "DATEDIFF",
            date_functions.date_diff,
            _CT_INT64,
            (_part, _date, ParameterSpec(name="end", type_family="datetime", excludes=_not_calendar)),
            aliases=("DATE_DIFF",),
            cost=2280.32,
            summary="Difference between two dates in the specified unit.",
        ),
        _make(
            "TIMEDIFF",
            date_functions.time_diff,
            _CT_INT64,
            (
                ParameterSpec(name="time1", type_family="datetime", excludes=_not_calendar),
                ParameterSpec(name="time2", type_family="datetime", excludes=_not_calendar),
            ),
            aliases=("TIME_DIFF",),
            cost=2331.70,
            summary="Difference between two times.",
        ),
        _make(
            "FORMAT_TIMESTAMP",
            date_functions.date_format,
            _CT_VARCHAR,
            (ParameterSpec(name="pattern", type_family="string", constant_only=True), _date),
            aliases=("FORMAT_DATE",),
            cost=33455.82,
            summary="Format date/timestamp as string (BigQuery FORMAT_TIMESTAMP/FORMAT_DATE convention: pattern first).",
        ),
        _make(
            "FROM_UNIXTIME",
            date_functions.from_unixtimestamp,
            _CT_TIMESTAMP(),
            (
                ParameterSpec(
                    name="ts",
                    type_family="numeric",
                    # Epoch SECONDS. The kernel's own limit is far wider (~year
                    # 294247, where the microsecond tick stops fitting int64),
                    # but a TIMESTAMP outside year 1..9999 cannot be materialised
                    # at all — it surfaced as a raw
                    # `ValueError: year must be in 1..9999`. These are the exact
                    # inclusive endpoints of that window.
                    minimum=-62135596800,
                    maximum=253402300799,
                ),
            ),
            cost=685.61,
            summary="Convert Unix timestamp to TIMESTAMP.",
        ),
        FunctionDefinition(
            name="UNIXTIME",
            aliases=("TO_UNIXTIME",),
            category="misc",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Convert TIMESTAMP to Unix epoch seconds.",
            documentation=(
                "Converts a DATE or TIMESTAMP to whole Unix epoch seconds as an INTEGER; "
                "sub-second detail is discarded. With no argument, gives the epoch "
                "seconds at which the query's connection was opened - the same value as "
                "UNIXTIME(NOW())."
            ),
            overloads=(
                FunctionOverload(
                    id="UNIXTIME_default",
                    parameters=(_date,),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=date_functions.unixtime,
                        cost_us_per_million=517.15,
                    ),
                ),
                # Never executed: the binder folds a zero-argument call to a
                # constant (constant.pyx, fixed_value_function) before overload
                # resolution. Declared so the catalog, and so the docs, know it.
                FunctionOverload(
                    id="UNIXTIME_0",
                    parameters=(),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="zero_arg",
                        callable_ref=lambda *a: None,
                        cost_us_per_million=1.0,
                    ),
                ),
            ),
        ),
    ]
