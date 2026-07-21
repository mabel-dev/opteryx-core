"""Helpers for exporting IDE-style function signatures from the live catalog."""

from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any

from opteryx.expression.functions import FunctionDefinition
from opteryx.expression.functions import FunctionOverload
from opteryx.expression.functions import ParameterSpec
from opteryx.expression.functions import get_catalog
from opteryx.types.logical_type import serialize_column_type

_TYPE_LABELS = {
    "any": "any",
    "array": "array",
    "boolean": "boolean",
    "integer": "integer",
    "numeric": "number",
    "numeric_vector": "vector",
    "string": "varchar",
    "temporal": "temporal",
}

_DOCUMENTATION_CATEGORIES = OrderedDict(
    [
        (
            "Conversion Functions",
            (
                "CAST",
                "TRY_CAST",
                "BOOLEAN",
                "VARBINARY",
                "INTEGER",
                "FLOAT",
                "VARCHAR",
                "TIMESTAMP",
                "HUMANIZE",
            ),
        ),
        (
            "Date & Time Functions",
            (
                "CURRENT_DATE",
                "CURRENT_TIME",
                "CURRENT_TIMESTAMP",
                "NOW",
                "TODAY",
                "DATE",
                "EXTRACT",
                "YEAR",
                "MONTH",
                "DAY",
                "HOUR",
                "MINUTE",
                "SECOND",
                "WEEK",
                "QUARTER",
                "FORMAT_TIMESTAMP",
                "FORMAT_DATE",
                "TRUNC",
                "DATEDIFF",
                "TIMEDIFF",
                "TIME_BUCKET",
                "UNIXTIME",
                "FROM_UNIXTIME",
                "DATE_DIFF",
                "TIME_DIFF",
                "TO_UNIXTIME",
            ),
        ),
        (
            "Numeric Functions",
            (
                "ABS",
                "CEIL",
                "CEILING",
                "FLOOR",
                "ROUND",
                "TRUNC",
                "SIGN",
                "SQRT",
                "POWER",
                "LOG",
                "E",
                "PI",
                "PHI",
            ),
        ),
        (
            "String Functions",
            (
                "LENGTH",
                "UPPER",
                "LOWER",
                "TITLE",
                "TITLECASE",
                "INITCAP",
                "TRIM",
                "LTRIM",
                "RTRIM",
                "CONCAT",
                "CONCAT_WS",
                "SUBSTRING",
                "LEFT",
                "RIGHT",
                "REVERSE",
                "REPLACE",
                "REGEXP_REPLACE",
                "SPLIT",
                "POSITION",
                "LPAD",
                "RPAD",
                "ASCII",
                "CHAR",
                "CASE",
                "GET_STRING",
                "SOUNDEX",
                "LEVENSHTEIN",
                "MATCH",
            ),
        ),
        (
            "Array Functions",
            (
                "ARRAY",
                "ARRAY_CONTAINS",
                "ARRAY_CONTAINS_ANY",
                "ARRAY_CONTAINS_ALL",
                "TRY_ARRAY",
                "GREATEST",
                "LEAST",
                "SORT",
                "UNNEST",
            ),
        ),
        (
            "Struct/JSON Functions",
            ("JSONB_OBJECT_KEYS",),
        ),
        (
            "Aggregate Functions",
            (
                "COUNT",
                "SUM",
                "AVG",
                "MIN",
                "MAX",
                "ARRAY_AGG",
                "ANY_VALUE",
                "APPROX_COUNT_DISTINCT",
                "APPROX_PERCENTILE",
            ),
        ),
        (
            "Hash & Encoding Functions",
            (
                "HASH",
                "MD5",
                "SHA1",
                "SHA224",
                "SHA256",
                "SHA384",
                "SHA512",
                "BASE64_ENCODE",
                "BASE64_DECODE",
                "BASE85_ENCODE",
                "BASE85_DECODE",
                "HEX_ENCODE",
                "HEX_DECODE",
            ),
        ),
        (
            "Vector / Embedding Functions",
            (
                "COSINE_SIMILARITY",
                "COSINE_DISTANCE",
                "EMBED",
            ),
        ),
        (
            "Utility Functions",
            (
                "COALESCE",
                "IFNULL",
                "IFNOTNULL",
                "NULLIF",
                "IIF",
                "RAND",
                "RANDOM",
                "NORMAL",
                "RANDOM_STRING",
                "GENERATE_SERIES",
                "CONNECTION_ID",
                "DATABASE",
                "USER",
                "VERSION",
                "GREATEST",
                "LEAST",
                "GET_STRING",
                "PASSTHRU",
            ),
        ),
    ]
)

_FALLBACK_CATEGORY_LABELS = {
    "arithmetic": "Numeric Functions",
    "array": "Array Functions",
    "constant": "Utility Functions",
    "hash_encoding": "Hash & Encoding Functions",
    "logical": "Utility Functions",
    "temporal": "Date & Time Functions",
    "text": "String Functions",
    "utility": "Utility Functions",
}

_HIDDEN_FUNCTIONS = {
    "ARRAY",
    "CASE",
    "GET_STRING",
    "PASSTHRU",
    "TRY_ARRAY",
}

_PUBLIC_SYNTAX_EXPORTS = {
    "_MATCH_AGAINST": "MATCH",
}

_COMMON_PARAMETER_DOCUMENTATION = {
    "a": "First input value.",
    "arg0": "First input value.",
    "args": "Additional input values.",
    "arr": "Input array or vector value.",
    "b": "Second input value.",
    "blob": "Binary or text value to encode, decode, or transform.",
    "chars": "Characters to remove from the input string.",
    "compare": "Value to compare against the primary input.",
    "condition": "Boolean expression used to choose which result to return.",
    "date": "Date, time, or timestamp value to evaluate.",
    "default": "Fallback value returned when the primary value is null.",
    "delimiter": "Separator used to split the input string.",
    "end": "Ending date, time, or timestamp value.",
    "exp": "Exponent to raise the base value by.",
    "expr": "Value to place into the constructed result.",
    "false_value": "Value returned when `condition` evaluates to false.",
    "fill": "Padding text used when the input is shorter than the target width.",
    "haystack": "String to search within.",
    "item": "Single value to compare against the array.",
    "items": "Collection of values to compare against the array.",
    "json": "JSON object or document value to inspect.",
    "key": "Field name or key to extract.",
    "limit": "Maximum number of items or splits to return.",
    "magnitude": "Bucket size or interval width for the calculation.",
    "more": "Additional input values.",
    "n": "Integer control value used by the function.",
    "needle": "Substring or value to search for.",
    "num": "Numeric input value.",
    "part": "Named date or time part, such as `year`, `month`, or `day`.",
    "pattern": "Pattern string used to format, search, or match values.",
    "precision": "Number of decimal places to keep when rounding.",
    "replacement": "Replacement text used for matched content.",
    "result": "Value returned when the condition or null check succeeds.",
    "scale": "Scale that controls which digit position is affected.",
    "search": "Text or pattern to replace in the input.",
    "sep": "Separator inserted between concatenated values.",
    "start": "Starting position for the operation.",
    "str": "Input string value.",
    "str1": "First input string value.",
    "struct": "Structured value or object to read from.",
    "text": "Input text value.",
    "time1": "First date, time, or timestamp value.",
    "time2": "Second date, time, or timestamp value.",
    "true_value": "Value returned when `condition` evaluates to true.",
    "ts": "Unix timestamp expressed in seconds.",
    "type_name": "Name of the target type to use for the result.",
    "units": "Named unit for the calculation, such as `minute` or `day`.",
    "val": "Input value.",
    "value": "Primary input value.",
    "vec": "Second vector or text value to compare against.",
    "width": "Target width for the output.",
}

_PARAMETER_DOCUMENTATION_OVERRIDES = {
    "ARRAY": {
        "expr": "Value or expression to place into the new array.",
        "type_name": "Element type for the array, such as `INTEGER` or `VARCHAR`.",
    },
    "TRY_ARRAY": {
        "expr": "Value or expression to place into the new array.",
        "type_name": "Element type for the array, such as `INTEGER` or `VARCHAR`.",
    },
    "CEILING": {
        "num": "Numeric value to round upward.",
        "scale": "Decimal scale to apply before taking the ceiling. Negative values round to tens, hundreds, and larger positions.",
    },
    "CONCAT_WS": {
        "sep": "Separator inserted between each concatenated string.",
        "str1": "First string value to concatenate.",
        "more": "Additional string values to concatenate after `str1`.",
    },
    "COSINE_DISTANCE": {
        "arr": "First vector or text input.",
        "vec": "Second vector or text input.",
    },
    "COSINE_SIMILARITY": {
        "arr": "First vector or text input.",
        "vec": "Second vector or text input.",
    },
    "EXTRACT": {
        "part": "Date or time part to extract, such as `year`, `month`, `day`, or `epoch`.",
    },
    "FORMAT_TIMESTAMP": {
        "pattern": "Format string used to render the temporal value as text.",
    },
    "DATEDIFF": {
        "part": "Unit to measure the difference in, such as `day`, `month`, or `year`.",
    },
    "EMBED": {
        "text": "Input text to convert into an embedding vector.",
    },
    "FLOOR": {
        "num": "Numeric value to round downward.",
        "scale": "Decimal scale to apply before taking the floor. Negative values round to tens, hundreds, and larger positions.",
    },
    "NORMAL": {
        "n": "Number of random values to generate.",
    },
    "POSITION": {
        "needle": "Substring to search for.",
        "haystack": "String to search within.",
    },
    "RANDOM": {
        "n": "Number of random values to generate.",
    },
    "RANDOM_STRING": {
        "n": "Number of random bytes to generate for each row.",
    },
    "REGEXP_REPLACE": {
        "pattern": "Regular expression pattern to match in the input string.",
    },
    "ROUND": {
        "num": "Numeric value to round.",
        "precision": "Number of decimal places to keep. Negative values round to tens, hundreds, and larger positions.",
    },
    "SUBSTRING": {
        "str": "Input string to extract a substring from.",
        "start": "One-based starting position of the substring.",
    },
    "TIME_BUCKET": {
        "magnitude": "Bucket width for each interval.",
        "units": "Unit for the bucket width, such as `minute`, `hour`, or `day`.",
    },
    "TRUNC": {
        "num": "Numeric value to truncate.",
        "scale": "Decimal scale to keep before truncating toward zero.",
        "value": "Date, time, or timestamp value to truncate.",
        "unit": "Granularity to truncate to, such as `day`, `month`, or `year`.",
    },
}

_RETURN_OVERRIDES = {
    "ARRAY": (
        "array<type_name>",
        "Returns a typed array whose element type is taken from `type_name`.",
    ),
    "CASE": (
        "compatible input type",
        "Returns the selected branch value using the first compatible result type from the CASE expression.",
    ),
    "COALESCE": (
        "compatible input type",
        "Returns the first non-null argument using a type compatible with the supplied values.",
    ),
    "EXTRACT": (
        "integer | double | date",
        "Returns `double` for parts such as `epoch` and `julian`, `date` for `date`, and `integer` for most other parts.",
    ),
    "EMBED": (
        "vector",
        "Returns an embedding vector.",
    ),
    "GREATEST": (
        "element type of `arr`",
        "Returns a single element from `arr`, preserving the array's element type.",
    ),
    "IFNOTNULL": (
        "compatible input type",
        "Returns the result value using a type compatible with the supplied arguments when the first argument is not null.",
    ),
    "IFNULL": (
        "compatible input type",
        "Returns either the primary value or the fallback value using a type compatible with both arguments.",
    ),
    "LEAST": (
        "element type of `arr`",
        "Returns a single element from `arr`, preserving the array's element type.",
    ),
    "PASSTHRU": (
        "same as `value`",
        "Returns the input unchanged, preserving the original type of `value`.",
    ),
    "SORT": (
        "same as `arr`",
        "Returns a sorted array while preserving the input array type.",
    ),
    "SPLIT": (
        "array<element type of `string`>",
        "Returns an array whose element type is the string type of `string` — the parts are substrings of the input, so the element type is fixed and known.",
    ),
    "TRY_ARRAY": (
        "array<type_name>",
        "Returns a typed array whose element type is taken from `type_name`, or null when conversion fails.",
    ),
}

_FUNCTION_NOTES = {
    "ARRAY": "The `type_name` argument must be a constant expression naming the target element type.",
    "CEILING": "When `scale` is provided, positive values affect digits to the right of the decimal point and negative values affect tens, hundreds, and larger positions.",
    "CURRENT_DATE": "Canonical SQL-92 form is `CURRENT_DATE`. Opteryx also accepts `CURRENT_DATE()`.",
    "CURRENT_TIME": "Canonical SQL-92 form is `CURRENT_TIME`. Opteryx also accepts `CURRENT_TIME()`.",
    "CURRENT_TIMESTAMP": "Canonical SQL-92 form is `CURRENT_TIMESTAMP`. Opteryx also accepts `CURRENT_TIMESTAMP()`.",
    "EXTRACT": "Canonical SQL-92 form is `EXTRACT(part FROM date)`. Return type depends on `part`: `epoch` and `julian` produce `double`, `date` produces `date`, and most other parts produce `integer`.",
    "EMBED": "This function depends on the configured embedding provider and returns a numeric `vector`.",
    "FLOOR": "When `scale` is provided, positive values affect digits to the right of the decimal point and negative values affect tens, hundreds, and larger positions.",
    "_MATCH_AGAINST": "Canonical form is `MATCH(str) AGAINST(pattern)`. Opteryx normalizes this syntax to an internal helper.",
    "NORMAL": "This function is volatile. The integer argument controls how many values are generated, not a seed.",
    "POSITION": "Canonical SQL-92 form is `POSITION(needle IN haystack)`. Opteryx also accepts `POSITION(needle, haystack)`.",
    "ROUND": "Uses PyArrow's default half-to-even rule to break ties when a value falls exactly between two candidates.",
    "RANDOM": "This function is volatile. The integer argument controls how many values are generated, not a seed.",
    "RANDOM_STRING": "This function is volatile. It returns `n` random bytes as `VARBINARY` for each row; the integer argument is the byte length, not a seed.",
    "SUBSTRING": "Canonical SQL-92 form is `SUBSTRING(str FROM start FOR length)`. Opteryx also accepts `SUBSTRING(str[, start[, length]])`.",
    "TRIM": "Canonical SQL-92 form is `TRIM([BOTH|LEADING|TRAILING] [chars] FROM str)`. Opteryx also accepts `TRIM(str[, chars])` as well as `LTRIM` and `RTRIM`.",
    "TRY_ARRAY": "The `type_name` argument must be a constant expression naming the target element type.",
}

_RELATED_HINTS = {
    "ARRAY": ("TRY_ARRAY", "SORT", "GREATEST", "LEAST"),
    "ARRAY_CONTAINS": ("ARRAY_CONTAINS_ANY", "ARRAY_CONTAINS_ALL", "SORT"),
    "ARRAY_CONTAINS_ALL": ("ARRAY_CONTAINS", "ARRAY_CONTAINS_ANY", "SORT"),
    "ARRAY_CONTAINS_ANY": ("ARRAY_CONTAINS", "ARRAY_CONTAINS_ALL", "SORT"),
    "ASCII": ("CHAR", "LEFT", "RIGHT"),
    "BASE64_DECODE": ("BASE64_ENCODE", "BASE85_DECODE", "HEX_DECODE"),
    "BASE64_ENCODE": ("BASE64_DECODE", "BASE85_ENCODE", "HEX_ENCODE"),
    "BASE85_DECODE": ("BASE85_ENCODE", "BASE64_DECODE", "HEX_DECODE"),
    "BASE85_ENCODE": ("BASE85_DECODE", "BASE64_ENCODE", "HEX_ENCODE"),
    "CASE": ("COALESCE", "IFNULL", "IIF"),
    "CEILING": ("ROUND", "FLOOR", "TRUNC"),
    "CHAR": ("ASCII", "UPPER", "LOWER"),
    "COALESCE": ("IFNULL", "IFNOTNULL", "CASE"),
    "CONCAT": ("CONCAT_WS", "LEFT", "RIGHT"),
    "CONCAT_WS": ("CONCAT", "TRIM", "SUBSTRING"),
    "COSINE_DISTANCE": ("COSINE_SIMILARITY", "EMBED"),
    "COSINE_SIMILARITY": ("COSINE_DISTANCE", "EMBED"),
    "DATEDIFF": ("EXTRACT", "TRUNC", "TIME_BUCKET"),
    "EXTRACT": ("TRUNC", "DATEDIFF", "TIME_BUCKET"),
    "EMBED": ("COSINE_SIMILARITY", "COSINE_DISTANCE"),
    "FLOOR": ("ROUND", "CEILING", "TRUNC"),
    "GREATEST": ("LEAST", "SORT", "ARRAY"),
    "IFNOTNULL": ("IFNULL", "COALESCE", "NULLIF"),
    "IFNULL": ("COALESCE", "IFNOTNULL", "NULLIF"),
    "LEAST": ("GREATEST", "SORT", "ARRAY"),
    "LOWER": ("UPPER", "TITLE", "INITCAP"),
    "LTRIM": ("TRIM", "RTRIM", "REPLACE"),
    "NULLIF": ("IFNULL", "IFNOTNULL", "COALESCE"),
    "POSITION": ("SUBSTRING", "REPLACE", "REGEXP_REPLACE"),
    "REPLACE": ("REGEXP_REPLACE", "SUBSTRING", "POSITION"),
    "ROUND": ("CEILING", "FLOOR", "TRUNC"),
    "RTRIM": ("TRIM", "LTRIM", "REPLACE"),
    "SORT": ("ARRAY", "GREATEST", "LEAST"),
    "SUBSTRING": ("LEFT", "RIGHT", "POSITION"),
    "TIME_BUCKET": ("TRUNC", "EXTRACT", "DATEDIFF"),
    "TITLE": ("INITCAP", "UPPER", "LOWER"),
    "TRIM": ("LTRIM", "RTRIM", "REPLACE"),
    "TRUNC": ("ROUND", "CEILING", "FLOOR"),
    "UPPER": ("LOWER", "TITLE", "INITCAP"),
}


def _parameter_type_label(parameter: ParameterSpec) -> str:
    return _TYPE_LABELS.get(parameter.type_family, parameter.type_family)


def _parameter_export_type_label(
    function: FunctionDefinition, overload: FunctionOverload, parameter: ParameterSpec
) -> str:
    if function.name == "TRUNC" and overload.id in ("TRUNC_date", "TRUNC_timestamp") and parameter.name == "value":
        return "temporal"
    return _parameter_type_label(parameter)


def _normalise_sentence(text: str) -> str:
    text = text.strip()
    if not text:
        return ""
    if text[-1] not in ".!?":
        return f"{text}."
    return text


_DRAKEN_TO_SQL: dict[str, str] = {
    "int8": "INTEGER", "int16": "INTEGER", "int32": "INTEGER", "int64": "INTEGER",
    "float32": "FLOAT", "float64": "FLOAT",
    "bool": "BOOLEAN",
    "varchar": "VARCHAR", "nvarchar": "NVARCHAR", "varbinary": "VARBINARY",
    "date32": "DATE", "timestamp64": "TIMESTAMP", "time32": "TIME", "time64": "TIME",
    "decimal": "DECIMAL", "decimal128": "DECIMAL",
    "boolean": "BOOLEAN",
}


def _type_label(column_type) -> str:
    """User-facing SQL type name for a ColumnType, or 'unknown'."""
    if column_type is None:
        return "unknown"
    raw = serialize_column_type(column_type).lower()
    return _DRAKEN_TO_SQL.get(raw, raw.upper() if raw != "unknown" else raw)


def _documentation_category(
    function: FunctionDefinition, display_name: str, overload: FunctionOverload
) -> str:
    if function.name == "TRUNC":
        if overload.id in ("TRUNC_date", "TRUNC_timestamp"):
            return "Date & Time Functions"
        return "Numeric Functions"

    for category_name, function_names in _DOCUMENTATION_CATEGORIES.items():
        if display_name in function_names or function.name in function_names:
            return category_name
    return _FALLBACK_CATEGORY_LABELS.get(
        function.category, function.category.replace("_", " ").title()
    )


def _parameter_signature_label(parameter: ParameterSpec) -> str:
    label = parameter.name
    if parameter.variadic:
        label = f"{label}..."
    if parameter.optional:
        label = f"[{label}]"
    return label


def _function_behavior_sentence(function: FunctionDefinition) -> str:
    if function.volatility == "volatile":
        return "Repeated calls can produce different results even when the arguments are the same."
    if function.volatility == "stable":
        return "Its value is derived from the current execution context and can change between statements."
    if function.deterministic:
        return "For the same inputs, it produces the same output."
    return ""


def _parameter_base_documentation(function_name: str, parameter: ParameterSpec) -> str:
    documentation = _PARAMETER_DOCUMENTATION_OVERRIDES.get(function_name, {}).get(parameter.name)
    if documentation:
        return documentation

    if parameter.documentation:
        return parameter.documentation

    documentation = _COMMON_PARAMETER_DOCUMENTATION.get(parameter.name)
    if documentation:
        return documentation

    type_label = _parameter_type_label(parameter)
    if type_label == "varchar":
        return "String input value."
    if type_label == "number":
        return "Numeric input value."
    if type_label == "integer":
        return "Integer input value."
    if type_label == "boolean":
        return "Boolean input value."
    if type_label == "array":
        return "Array input value."
    if type_label == "vector":
        return "Numeric vector input value."
    if type_label == "temporal":
        return "Date, time, or timestamp input value."
    if type_label == "blob":
        return "Binary input value."
    return f"Input value of type `{type_label}`."


def _parameter_documentation(function_name: str, parameter: ParameterSpec) -> str:
    documentation = _normalise_sentence(_parameter_base_documentation(function_name, parameter))

    if parameter.constant_only:
        documentation = f"{documentation} Must be a constant expression."
    if parameter.variadic:
        if parameter.optional:
            documentation = f"{documentation} Optional. Can be repeated."
        else:
            documentation = f"{documentation} Can be repeated."
    elif parameter.optional:
        documentation = f"{documentation} Optional."

    return documentation


def _lifecycle_export(function: FunctionDefinition) -> dict[str, Any]:
    lifecycle = function.lifecycle
    return {
        "status": lifecycle.status,
        "introduced": lifecycle.introduced,
        "deprecated_in": lifecycle.deprecated_in,
        "remove_after": lifecycle.remove_after,
        "replacement": lifecycle.replacement,
    }


def _arity_export(overload: FunctionOverload) -> dict[str, Any]:
    parameters = overload.parameters
    minimum = sum(
        1 for parameter in parameters if not parameter.optional and not parameter.variadic
    )
    variadic = any(parameter.variadic for parameter in parameters)
    maximum: int | None = None if variadic else len(parameters)
    return {
        "minimum": minimum,
        "maximum": maximum,
        "variadic": variadic,
    }


def _signature_label(name: str, overload: FunctionOverload) -> str:
    if name in ("CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP"):
        return name

    if name == "EXTRACT":
        return "EXTRACT(part FROM date)"

    if name == "MATCH":
        return "MATCH(str) AGAINST(pattern)"

    if name == "POSITION":
        return "POSITION(needle IN haystack)"

    if name == "SUBSTRING":
        if len(overload.parameters) == 2:
            return "SUBSTRING(str FROM start)"
        if len(overload.parameters) == 3:
            return "SUBSTRING(str FROM start FOR length)"

    if name == "TRIM":
        return "TRIM([BOTH|LEADING|TRAILING] [chars] FROM str)"

    if not overload.parameters:
        return f"{name}()"

    labels = [_parameter_signature_label(parameter) for parameter in overload.parameters]
    return f"{name}({', '.join(labels)})"


def _return_metadata(function: FunctionDefinition, overload: FunctionOverload) -> tuple[str, str]:
    override = _RETURN_OVERRIDES.get(function.name)
    if override is not None:
        return override

    return_spec = overload.return_spec
    if return_spec.mode == "fixed":
        type_label = _type_label(return_spec.fixed_type)
        if type_label == "boolean":
            return (
                type_label,
                "Returns `true` or `false` based on whether the function's condition is satisfied.",
            )
        return type_label, f"Returns the computed result as `{type_label}`."

    if return_spec.mode == "same_as_arg":
        index = return_spec.arg_index or 0
        parameter = overload.parameters[index] if index < len(overload.parameters) else None
        if parameter is None:
            return "same as input", "Returns a value with the same type as the selected input."
        type_label = f"same as `{parameter.name}`"
        return type_label, f"Returns a value with the same type as `{parameter.name}`."

    if function.name in {"COALESCE", "IFNULL", "IFNOTNULL", "_CASE"}:
        return _RETURN_OVERRIDES[function.name]

    return "dynamic", "Returns a value whose type depends on the supplied arguments."


def _function_documentation(
    display_name: str,
    function: FunctionDefinition,
    category_label: str,
    overload: FunctionOverload,
) -> str:
    del category_label  # category is exposed separately in the exported structure

    if function.name == "TRUNC":
        if overload.id in ("TRUNC_date", "TRUNC_timestamp"):
            return "Truncates a temporal value to the start of the specified unit."
        return "Truncates a numeric value toward zero at the requested scale."

    base = function.documentation or function.summary or display_name
    base = _normalise_sentence(base)

    replacements = (
        (
            "Returns input string with all characters in uppercase.",
            "Transforms the input string to uppercase.",
        ),
        (
            "Returns input string with all characters in lowercase.",
            "Transforms the input string to lowercase.",
        ),
        (
            "Returns the number of characters in the input string.",
            "Calculates the number of characters in the input string.",
        ),
        (
            "Returns concatenation of all input strings.",
            "Concatenates all input strings.",
        ),
        (
            "Returns substring starting at position with optional length.",
            "Extracts a substring starting at the given position, with an optional length.",
        ),
        (
            "Returns absolute value of input number.",
            "Calculates the absolute value of the input number.",
        ),
        (
            "Returns smallest integer greater than or equal to input.",
            "Calculates the smallest integer greater than or equal to the input.",
        ),
        (
            "Returns largest integer less than or equal to input.",
            "Calculates the largest integer less than or equal to the input.",
        ),
        (
            "Returns square root of input number.",
            "Calculates the square root of the input number.",
        ),
        (
            "Returns the first non-null value from the list of arguments.",
            "Selects the first non-null value from the list of arguments.",
        ),
        (
            "Returns first argument if not null, otherwise returns second argument.",
            "Selects the first argument when it is not null; otherwise uses the second argument.",
        ),
        (
            "Returns second argument if first argument is not null, otherwise null.",
            "Selects the second argument when the first argument is not null; otherwise yields null.",
        ),
        (
            "Returns null if arguments are equal, otherwise returns first argument.",
            "Compares the two arguments and yields null when they are equal; otherwise preserves the first argument.",
        ),
        (
            "Returns second argument if condition is true, otherwise third argument.",
            "Selects between the second and third arguments based on the condition.",
        ),
        (
            "Returns the input value unchanged. Used for testing and compatibility.",
            "Preserves the input value unchanged. Used for testing and compatibility.",
        ),
        (
            "Returns a value based on conditional expressions.",
            "Selects a value based on conditional expressions.",
        ),
        (
            "Returns the maximum element from an array column.",
            "Determines the maximum element in an array column.",
        ),
        (
            "Returns the minimum element from an array column.",
            "Determines the minimum element in an array column.",
        ),
        (
            "Returns a sorted version of an array column.",
            "Sorts an array column.",
        ),
    )

    for source, target in replacements:
        if base == source:
            return target

    if base.startswith("Returns "):
        return f"Computes {base[len('Returns ') :].lower()}"
    if base.startswith("Return "):
        return f"Computes {base[len('Return ') :].lower()}"

    return base


def _related_functions(
    function: FunctionDefinition,
    display_name: str,
    category_label: str,
    alias_names: set[str],
    overload: FunctionOverload,
) -> list[str]:
    related = []
    seen = {display_name, *function.aliases, *alias_names, *_HIDDEN_FUNCTIONS}

    def _add(name: str) -> None:
        if name in seen:
            return
        seen.add(name)
        related.append(name)

    if function.name == "TRUNC":
        if overload.id in ("TRUNC_date", "TRUNC_timestamp"):
            hint_names = ("EXTRACT", "TIME_BUCKET", "DATEDIFF")
        else:
            hint_names = ("ROUND", "CEILING", "FLOOR")
    else:
        hint_names = _RELATED_HINTS.get(function.name, ())

    for name in hint_names:
        _add(name)

    for candidate in _DOCUMENTATION_CATEGORIES.get(category_label, ()):
        _add(candidate)
        if len(related) >= 5:
            break

    return related[:5]


def _export_overload(
    display_name: str,
    function: FunctionDefinition,
    overload: FunctionOverload,
    alias_names: set[str],
) -> dict[str, Any]:
    return_type, return_documentation = _return_metadata(function, overload)
    category_label = _documentation_category(function, display_name, overload)

    exported = {
        "id": overload.id,
        "label": _signature_label(display_name, overload),
        "category": category_label,
        "documentation": _function_documentation(display_name, function, category_label, overload),
        "related_functions": _related_functions(
            function=function,
            display_name=display_name,
            category_label=category_label,
            alias_names=alias_names,
            overload=overload,
        ),
        "return_type": return_type,
        "returns": {
            "type": return_type,
            "documentation": return_documentation,
        },
        "arity": _arity_export(overload),
        "execution": {
            "kernel_id": overload.kernel.id,
            "engine": overload.kernel.engine,
            "cost_us_per_million": overload.kernel.cost_us_per_million,
        },
        "parameters": [
            {
                "label": parameter.name,
                "type": _parameter_export_type_label(function, overload, parameter),
                "documentation": _parameter_documentation(function.name, parameter),
                "optional": parameter.optional,
                "variadic": parameter.variadic,
                "constant_only": parameter.constant_only,
                "null_handling": parameter.null_handling,
            }
            for parameter in overload.parameters
        ],
    }

    notes = _FUNCTION_NOTES.get(function.name)
    if function.name == "TRUNC":
        if overload.id in ("TRUNC_date", "TRUNC_timestamp"):
            notes = "Truncates to the start of the specified unit. The `unit` argument must be a constant expression."
        else:
            notes = "Truncation is performed toward zero rather than toward negative infinity."
    if notes:
        exported["notes"] = notes

    return exported


def export_function_signatures(
    include_aliases: bool = True, include_internal: bool = False
) -> OrderedDict[str, dict[str, Any]]:
    catalog = get_catalog()
    exported: dict[str, dict[str, Any]] = {}

    functions = sorted(catalog.list_functions(), key=lambda item: item.name)
    alias_names = {
        alias
        for function in functions
        for alias in function.aliases
        if include_internal or not alias.startswith("_")
    }

    for function in functions:
        public_name = _PUBLIC_SYNTAX_EXPORTS.get(function.name, function.name)

        if (
            not include_internal
            and function.name.startswith("_")
            and function.name not in _PUBLIC_SYNTAX_EXPORTS
        ):
            continue
        if public_name in _HIDDEN_FUNCTIONS:
            continue
        aliases = []
        if include_aliases:
            aliases = [
                alias
                for alias in function.aliases
                if (include_internal or not alias.startswith("_"))
                and alias not in _HIDDEN_FUNCTIONS
            ]

        exported[public_name] = {
            "catalog_name": function.name,
            "aliases": aliases,
            "summary": _normalise_sentence(
                function.summary or function.documentation or public_name
            ),
            "volatility": function.volatility,
            "deterministic": function.deterministic,
            "foldable": function.foldable,
            "pushdown_safe": function.pushdown_safe,
            "lifecycle": _lifecycle_export(function),
            "overloads": [
                _export_overload(
                    display_name=public_name,
                    function=function,
                    overload=overload,
                    alias_names=alias_names,
                )
                for overload in function.overloads
            ],
        }

    ordered = OrderedDict()
    for name in sorted(exported):
        ordered[name] = exported[name]
    return ordered


def write_function_signatures(path: str | Path) -> None:
    output_path = Path(path)
    output_path.write_text(
        json.dumps(export_function_signatures(), indent=4) + "\n",
        encoding="utf8",
    )
