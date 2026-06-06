from opteryx.expression.functions import ParameterSpec

# Use package-level helper to construct concise FunctionDefinition entries.
from opteryx.types.logical_type import LogicalCategory


def get_builtin_temporal_functions() -> list:
    """Full temporal function set (registrar entries)."""

    # Parameter shortcuts
    _part = ParameterSpec(name="part", type_family="string", constant_only=True)
    _date = ParameterSpec(name="date", type_family="temporal")

    # Implementations are provided by the temporal implementation module.
    from opteryx.expression.functions.implementations import temporal as date_functions

    return [
        _make(
            "TIME_BUCKET",
            date_functions.date_floor,
            LogicalCategory.TIMESTAMP,
            (
                ParameterSpec(name="magnitude", type_family="numeric"),
                ParameterSpec(name="units", type_family="string", constant_only=True),
                _date,
            ),
            summary="Bucket date into fixed-width intervals.",
            cost=1.08,
        ),
        _make(
            "DATEDIFF",
            date_functions.date_diff,
            LogicalCategory.INTEGER,
            (_part, _date, ParameterSpec(name="end", type_family="temporal")),
            aliases=("DATE_DIFF",),
            cost=0.88,
            summary="Difference between two dates in the specified unit.",
        ),
        _make(
            "TIMEDIFF",
            date_functions.time_diff,
            LogicalCategory.INTEGER,
            (
                ParameterSpec(name="time1", type_family="temporal"),
                ParameterSpec(name="time2", type_family="temporal"),
            ),
            aliases=("TIME_DIFF",),
            cost=0.68,
            summary="Difference between two times.",
        ),
        _make(
            "DATE_FORMAT",
            date_functions.date_format,
            LogicalCategory.VARCHAR,
            (_date, ParameterSpec(name="pattern", type_family="string", constant_only=True)),
            cost=0.85,
            summary="Format date/timestamp as string.",
        ),
        _make(
            "FROM_UNIXTIME",
            date_functions.from_unixtimestamp,
            LogicalCategory.TIMESTAMP,
            (ParameterSpec(name="ts", type_family="numeric"),),
            cost=3.17,
            summary="Convert Unix timestamp to TIMESTAMP.",
        ),
        _make(
            "UNIXTIME",
            date_functions.unixtime,
            LogicalCategory.INTEGER,
            (_date,),
            aliases=("TO_UNIXTIME",),
            cost=8.84,
            summary="Convert TIMESTAMP to Unix epoch seconds.",
        ),
    ]
