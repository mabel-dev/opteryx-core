"""Kernel implementations for scalar functions.

Kernels are organized by semantic domain:
- type_conversion: CAST variants, BOOLEAN, INTEGER, DOUBLE, DECIMAL, VARCHAR, DATE, BLOB, TRY_*
- text: LENGTH, UPPER, LOWER, LEFT, RIGHT, REVERSE, SOUNDEX, TITLE, INITCAP, CONCAT, CONCAT_WS, SUBSTRING, POSITION, TRIM, LTRIM, RTRIM, LPAD, RPAD, LEVENSHTEIN, SPLIT, MATCH_AGAINST, REPLACE, REGEXP_REPLACE
- arithmetic: ROUND, FLOOR, CEIL, ABS, SIGN, SQRT, TRUNC, POWER, LN, LOG10, LOG2, LOG
- temporal: DATE_TRUNC, TIME_BUCKET, DATEDIFF, TIMEDIFF, DATEPART, DATE_FORMAT, YEAR, MONTH, DAY, WEEK, HOUR, MINUTE, SECOND, QUARTER, FROM_UNIXTIME, UNIXTIME
- logical: COALESCE, IFNULL, IFNOTNULL, IIF, NULLIF, CASE, SEARCH
- hash_encoding: HASH, MD5, SHA1, SHA224, SHA256, SHA384, SHA512, BASE64_*, BASE85_*, HEX_*
- utility: ARRAY_CONTAINS, ARRAY_CONTAINS_ANY, ARRAY_CONTAINS_ALL, GET, GET_STRING, SORT, GREATEST, LEAST, JSONB_OBJECT_KEYS, HUMANIZE, COSINE_SIMILARITY, RANDOM, RAND, NORMAL, RANDOM_STRING

Note: Binary operators (Plus, Minus, Multiply, etc.) are handled separately via managers/expression/binary_operators.py.
Aggregate functions are handled separately via the operators subsystem.
"""

# Re-export all kernels for public API
