def build_specialized_kernel(
    list group_by_columns,
    list agg_function_codes,
    list agg_columns,
):
    if len(group_by_columns) != 1 or len(agg_function_codes) != 1:
        return None

    function_code = agg_function_codes[0]
    key_column = group_by_columns[0]

    if function_code == AGG_COUNT_STAR:
        return Int64CountStarKernel(key_column)

    if function_code == AGG_AVG and agg_columns[0] is not None:
        return Int64AvgFloat64Kernel(key_column, agg_columns[0])

    if function_code == AGG_COUNT_DISTINCT and agg_columns[0] is not None:
        return Int64CountDistinctInt64Kernel(key_column, agg_columns[0])

    return None
