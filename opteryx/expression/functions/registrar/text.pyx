"""
Text registrar: combined core text + extended text function definitions.

This module provides two public getters:
- get_builtin_text_functions()
- get_builtin_text_extended_functions()

They return lists of FunctionDefinition objects wired to the implementation
callables in opteryx.expression.functions.implementations.text and to a small
set of compiled vector ops when available.
"""

from typing import List

from opteryx.compiled import vector_ops as compiled_vector_ops
from opteryx.compiled.nanobind.vectors import (
    vector_lowercase,
    vector_uppercase,
    vector_initcap,
    vector_reverse,
)
from opteryx.compiled.nanobind.vectors import (
    vector_string_slice_left,
    vector_string_slice_right,
    vector_string_substring,
)
from opteryx.compiled.nanobind.vectors import (
    vector_md5,
    vector_sha1,
    vector_sha256,
    vector_sha512,
)
from opteryx.compiled.nanobind.vectors import vector_soundex
from opteryx.compiled.nanobind.vectors import (
    vector_levenshtein,
)
from opteryx.compiled.nanobind.vectors import (
    vector_replace,
)
from opteryx.compiled.nanobind.vectors import (
    vector_ci_ends_with,
    vector_ci_starts_with,
    vector_ends_with,
    vector_starts_with,
)
from opteryx.compiled.nanobind.vectors import (
    vector_length,
    vector_octet_length,
    vector_string_length,
)
import draken.draken_native as _draken_native_text_reg
from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
# LogicalCategory imported via __init__.pyx (textually included); canonical ColumnTypes also in scope.


def get_builtin_text_functions() -> List[FunctionDefinition]:
    """
    Core text functions (UPPER, LOWER, LENGTH, CONCAT, SUBSTRING, ...).

    Uses the implementations in opteryx.expression.functions.implementations.text.
    """
    from opteryx.expression.functions.implementations import text as string_functions

    # Parameter shortcuts
    _str = ParameterSpec(name="str", type_family="string")
    _string = ParameterSpec(name="string", type_family="string")

    return [
        _make(
            "UPPER",
            vector_uppercase,
            _CT_VARCHAR,
            (_str,),
            aliases=("UCASE",),
            category="text",
            engine="draken",
            summary="Convert string to uppercase.",
            cost=6236.70,
        ),
        _make(
            "LOWER",
            vector_lowercase,
            _CT_VARCHAR,
            (_str,),
            aliases=("LCASE",),
            category="text",
            engine="draken",
            summary="Convert string to lowercase.",
            cost=6473.41,
        ),
        FunctionDefinition(
            name="LENGTH",
            aliases=("CHAR_LENGTH", "CHARACTER_LENGTH"),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return length of string or number of elements in an array.",
            documentation=None,
            overloads=(
                FunctionOverload(
                    id="LENGTH_string",
                    parameters=(_string,),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=vector_string_length,
                        cost_us_per_million=264.95,
                    ),
                ),
                FunctionOverload(
                    id="LENGTH_array",
                    parameters=(ParameterSpec(name="arr", type_family="array"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=vector_length,
                        cost_us_per_million=296.23,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="OCTET_LENGTH",
            aliases=("BYTE_LENGTH",),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Return the number of bytes in a string, regardless of string type.",
            documentation=None,
            overloads=(
                FunctionOverload(
                    id="OCTET_LENGTH_string",
                    parameters=(_string,),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=vector_octet_length,
                        cost_us_per_million=266.96,
                    ),
                ),
            ),
        ),
        _make(
            "INITCAP",
            vector_initcap,
            _CT_VARCHAR,
            (_string,),
            aliases=("TITLE",),
            category="text",
            engine="draken",
            summary="Capitalize first letter of each word.",
            cost=13437.32,
        ),
        _make(
            "REVERSE",
            vector_reverse,
            _CT_VARCHAR,
            (_string,),
            category="text",
            engine="draken",
            summary="Reverse a string.",
            cost=5284.14,
        ),
        _make(
            "SOUNDEX",
            vector_soundex,
            _CT_VARCHAR,
            (_string,),
            category="text",
            engine="draken",
            summary="Return Soundex code of string.",
            cost=21737.77,
        ),
        FunctionDefinition(
            name="CONCAT",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Concatenate strings.",
            documentation="Concatenates multiple string arguments into a single string.",
            # ONE OVERLOAD PER STRING TYPE. CONCAT concatenates a single string
            # type; VARCHAR, NVARCHAR and VARBINARY each get their own overload so
            # the catalog states two things the old single `any`-typed overload
            # could not. First, HOMOGENEITY: mixed operands match no overload and
            # are refused by resolution, rather than binding and then failing
            # further down. Second, the RETURN TYPE: it is the operand type, not a
            # hardcoded VARCHAR — `CONCAT(b'a', b'b')` is VARBINARY, the same
            # answer `b'a' || b'b'` gives, and the two spellings agreeing is the
            # whole point since one desugars into the other.
            #
            # The old overload typed every parameter `any`, which coerced
            # non-strings to VARCHAR — `CONCAT(id, name)` worked. That is
            # deliberately gone (architect, 2026-08-09): the cast is now the
            # caller's to write, `CONCAT(CAST(id AS VARCHAR), name)`, matching `||`,
            # which never coerced. See
            # RATIFIED/string-concatenation-requires-homogeneous-string-types.
            overloads=(
                FunctionOverload(
                    id="CONCAT_varchar",
                    parameters=(
                        ParameterSpec(name="str1", type_family="varchar"),
                        ParameterSpec(name="str2", type_family="varchar"),
                        ParameterSpec(name="strs", type_family="varchar", variadic=True, optional=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        # No draken_concat kernel exists — the optimizer (predicate_rewriter.py)
                        # always rewrites CONCAT(a, b, ...) to a || b || ... (StringConcat) chains
                        # before execution, so this call is never dispatched under normal
                        # operation. Unreachable via callable_ref too: the native engine has no
                        # per-morsel Python fallback for projections (compiler.py `_unsupported`),
                        # so a stray CONCAT that reaches the executor (e.g. DISABLE_OPTIMIZER=1)
                        # is refused at plan time rather than run through this. Confirmed dead
                        # 2026-07-17 in every mode: with the optimizer enabled it never reaches
                        # kernel dispatch; with it disabled it is refused before reaching here.
                        callable_ref=None,
                        cost_us_per_million=523.0,
                    ),
                ),
                FunctionOverload(
                    id="CONCAT_nvarchar",
                    parameters=(
                        ParameterSpec(name="str1", type_family="nvarchar"),
                        ParameterSpec(name="str2", type_family="nvarchar"),
                        ParameterSpec(name="strs", type_family="nvarchar", variadic=True, optional=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_NVARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,  # rewritten to `||`; see CONCAT_varchar
                        cost_us_per_million=523.0,
                    ),
                ),
                FunctionOverload(
                    id="CONCAT_varbinary",
                    parameters=(
                        ParameterSpec(name="str1", type_family="varbinary"),
                        ParameterSpec(name="str2", type_family="varbinary"),
                        ParameterSpec(name="strs", type_family="varbinary", variadic=True, optional=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARBINARY),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,  # rewritten to `||`; see CONCAT_varchar
                        cost_us_per_million=523.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="SUBSTRING",
            aliases=("SUBSTR",),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Extract substring.",
            documentation="Extracts a substring starting at the specified position with optional length.",
            overloads=(
                FunctionOverload(
                    id="SUBSTRING_2",
                    parameters=(
                        _string,
                        ParameterSpec(name="from_pos", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=lambda s, f: vector_string_substring(
                            s, f, _draken_native_text_reg.vector_null_from_length(len(s))
                        ),
                        cost_us_per_million=378.0,
                    ),
                ),
                FunctionOverload(
                    id="SUBSTRING_3",
                    parameters=(
                        _string,
                        ParameterSpec(name="from_pos", type_family="integer"),
                        ParameterSpec(name="count", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=vector_string_substring,
                        cost_us_per_million=9921.62,
                    ),
                ),
            ),
        ),
        _make(
            "LEFT",
            vector_string_slice_left,
            _CT_VARCHAR,
            (
                _string,
                ParameterSpec(name="length", type_family="integer"),
            ),
            engine="draken",
            summary="Extract leftmost characters.",
            cost=9174.75,
        ),
        _make(
            "RIGHT",
            vector_string_slice_right,
            _CT_VARCHAR,
            (
                _string,
                ParameterSpec(name="length", type_family="integer"),
            ),
            engine="draken",
            summary="Extract rightmost characters.",
            cost=8650.90,
        ),
        _make(
            "_STARTS_WITH",
            vector_starts_with,
            _CT_BOOLEAN,
            (
                ParameterSpec(name="haystack", type_family="string"),
                ParameterSpec(name="needle", type_family="string", constant_only=True),
            ),
            engine="draken",
            summary="Internal prefix match (case-sensitive).",
            cost=1955.83,
        ),
        _make(
            "_CI_STARTS_WITH",
            vector_ci_starts_with,
            _CT_BOOLEAN,
            (
                ParameterSpec(name="haystack", type_family="string"),
                ParameterSpec(name="needle", type_family="string", constant_only=True),
            ),
            engine="draken",
            summary="Internal prefix match (case-insensitive).",
            cost=872.24,
        ),
        _make(
            "_ENDS_WITH",
            vector_ends_with,
            _CT_BOOLEAN,
            (
                ParameterSpec(name="haystack", type_family="string"),
                ParameterSpec(name="needle", type_family="string", constant_only=True),
            ),
            engine="draken",
            summary="Internal suffix match (case-sensitive).",
            cost=2082.09,
        ),
        _make(
            "_CI_ENDS_WITH",
            vector_ci_ends_with,
            _CT_BOOLEAN,
            (
                ParameterSpec(name="haystack", type_family="string"),
                ParameterSpec(name="needle", type_family="string", constant_only=True),
            ),
            engine="draken",
            summary="Internal suffix match (case-insensitive).",
            cost=869.18,
        ),
    ]


def get_builtin_text_extended_functions() -> List[FunctionDefinition]:
    """
    Extended text functions (CONCAT_WS, POSITION, TRIM/LTRIM/RTRIM, REPLACE, REGEXP_REPLACE, etc.)
    Combined with the core group in one module for maintainability.
    """
    from opteryx.compiled import vector_ops as compiled_vector_ops
    from opteryx.compiled.nanobind.vectors import (
        vector_ltrim,
        vector_rtrim,
        vector_trim,
    )
    from opteryx.expression.functions.implementations import text as string_functions

    def _split_return_type(arg_nodes):
        """SPLIT(str, delim[, limit]): ARRAY<element> where element is the input's
        OWN string type.

        The parts are substrings of the input, so the element type is fixed and
        knowable — it is whatever string family went in. The draken_split kernel tags
        the child with `str->type` for exactly this reason; these two must agree.
        VARIANT would be a lie that also strands the result (VARIANT has no
        gather/compare path, so it could not survive an ORDER BY or join).
        """
        sc = getattr(arg_nodes[0], "schema_column", None) if arg_nodes else None
        elem = getattr(sc, "column_type", None) if sc is not None else None
        if elem is None:
            # Unbound/unknown operand — the kernel still emits the input's type, but
            # the binder cannot name it here. VARCHAR is the only string type a
            # SPLIT operand can be by default.
            elem = _CT_VARCHAR
        return (_CT_ARRAY(elem), elem)

    # Parameter shortcuts
    _string = ParameterSpec(name="string", type_family="string")
    # REGEXP_REPLACE is implemented ONLY as whole-match capture extraction: the
    # optimizer rewrites REGEXP_REPLACE(s, pat, '\1') to _DFA_EXTRACT when `pat`
    # compiles to an anchored DFA program, and everything else reaches
    # implementations.text.regex_replace, which raises — RE2 was removed from the
    # execution path and there is no general replacement kernel. The catalog
    # declared the unrestricted three-argument form, so it claimed a capability
    # the engine does not have. Both restrictions are now on the parameters.
    _pattern = ParameterSpec(
        name="pattern",
        type_family="string",
        constant_only=True,
        value_format="dfa-regex",
        documentation=(
            "Must compile to a DFA program — anchored, consuming the whole input. "
            "A pattern outside that subset is refused; there is no runtime regex matcher."
        ),
    )
    _replacement = ParameterSpec(
        name="replacement",
        type_family="string",
        constant_only=True,
        domain=("\\1",),
        documentation=(
            "Only the whole-match capture reference `'\\1'` is supported. An arbitrary "
            "replacement template is refused."
        ),
    )
    _compiled_program = ParameterSpec(
        name="compiled_program", type_family="binary", constant_only=True
    )
    _search = ParameterSpec(name="search", type_family="string")

    def _trim_return_type(arg_nodes):
        """TRIM/LTRIM/RTRIM always return VARCHAR."""
        return _CT_VARCHAR

    # SQL-92's `TRIM([LEADING|TRAILING|BOTH] <chars> FROM <str>)` — the dialect
    # parses it and logical_planner_builders.trim_string maps the three directions
    # onto TRIM/LTRIM/RTRIM with the characters as a second argument, so this
    # parameter is what makes that spelling reachable. It is also the call form,
    # `TRIM(s, 'ab')`, which is how Postgres and DuckDB spell the same thing.
    #
    # A SET OF CHARACTERS, not a substring: `TRIM(BOTH 'ab' FROM 'baXab')` is 'X'
    # (architect ruling, 2026-08-10). Empty set strips nothing.
    #
    # `constant_only` is load-bearing, not decoration. draken_trim is
    # SHAPE-PRESERVING — it computes the trimmed range ONCE per physical unique
    # value and carries the input's selection and validity onto the result — and
    # that is sound only while the trimmed range is a function of the value's bytes
    # alone. A per-ROW character set would make it a function of the row too.
    # `constant_only` is enforced at BIND (compiled_expression.pyx), which is where
    # a caller gets a message naming the argument; draken_trim refuses a non-constant
    # set as well, because it is a C ABI kernel with callers that never pass through
    # the binder.
    _trim_characters = ParameterSpec(
        name="characters",
        type_family="string",
        optional=True,
        constant_only=True,
        documentation=(
            "The SET of characters to strip, not a substring to match: "
            "`TRIM(BOTH 'ab' FROM 'baXab')` is `X`. Must be constant. Omitted, "
            "ASCII whitespace is stripped. Over an NVARCHAR operand the set is "
            "matched by codepoint, so a multibyte character can never be split."
        ),
    )

    vector_dfa_extract = getattr(compiled_vector_ops, "vector_dfa_extract")

    return [
        FunctionDefinition(
            name="CONCAT_WS",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Concatenate strings with separator.",
            documentation="Concatenates strings with specified separator, skipping nulls.",
            # One overload per string type, as CONCAT — and the SEPARATOR is bound
            # by the same rule, because it is concatenated into the result like any
            # other operand. `CONCAT_WS('-', b'a', b'b')` is refused: a VARCHAR
            # separator against VARBINARY values is exactly the mix the ruling
            # forbids, and it would desugar into a mixed `||` chain.
            overloads=(
                FunctionOverload(
                    id="CONCAT_WS_varchar",
                    parameters=(
                        ParameterSpec(name="separator", type_family="varchar"),
                        ParameterSpec(name="str1", type_family="varchar"),
                        ParameterSpec(name="strs", type_family="varchar", variadic=True, optional=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        # No draken_concat_ws kernel exists — every arity is rewritten by the
                        # optimizer (predicate_rewriter.py) to a StringConcat chain before
                        # execution; see CONCAT's kernel comment above for why callable_ref is
                        # unreachable in every mode, including DISABLE_OPTIMIZER=1.
                        callable_ref=None,
                        cost_us_per_million=587.0,
                    ),
                ),
                FunctionOverload(
                    id="CONCAT_WS_nvarchar",
                    parameters=(
                        ParameterSpec(name="separator", type_family="nvarchar"),
                        ParameterSpec(name="str1", type_family="nvarchar"),
                        ParameterSpec(name="strs", type_family="nvarchar", variadic=True, optional=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_NVARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,  # rewritten to `||`; see CONCAT_WS_varchar
                        cost_us_per_million=587.0,
                    ),
                ),
                FunctionOverload(
                    id="CONCAT_WS_varbinary",
                    parameters=(
                        ParameterSpec(name="separator", type_family="varbinary"),
                        ParameterSpec(name="str1", type_family="varbinary"),
                        ParameterSpec(name="strs", type_family="varbinary", variadic=True, optional=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARBINARY),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=None,  # rewritten to `||`; see CONCAT_WS_varchar
                        cost_us_per_million=587.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="SPLIT",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Split string into array.",
            documentation="Splits a string into an array using the specified delimiter.",
            overloads=(
                FunctionOverload(
                    id="SPLIT_2",
                    parameters=(
                        _string,
                        ParameterSpec(name="delimiter", type_family="string"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_split_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.split,
                        cost_us_per_million=531.0,
                    ),
                ),
                FunctionOverload(
                    id="SPLIT_3",
                    parameters=(
                        _string,
                        ParameterSpec(name="delimiter", type_family="string"),
                        ParameterSpec(name="limit", type_family="integer"),
                    ),
                    return_spec=ReturnSpec(mode="resolver", resolver=_split_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.split,
                        cost_us_per_million=589.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="POSITION",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Find position of substring.",
            documentation="Returns the starting position (1-based) of substring in string, or 0 if not found.",
            overloads=(
                FunctionOverload(
                    id="POSITION_2",
                    parameters=(
                        ParameterSpec(name="sub", type_family="string"),
                        _string,
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.position,
                        cost_us_per_million=3317.72,
                    ),
                ),
            ),
        ),
        _make(
            "REPLACE",
            vector_replace,
            _CT_VARCHAR,
            (
                _string,
                _search,
                ParameterSpec(name="replace_val", type_family="string"),
            ),
            engine="draken",
            summary="Replace all occurrences.",
            cost=9169.95,
        ),
        FunctionDefinition(
            name="REGEXP_REPLACE",
            aliases=("REGEX_REPLACE",),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Replace using regex pattern.",
            documentation="Replaces all matches of a regular expression pattern with a replacement string.",
            overloads=(
                FunctionOverload(
                    id="REGEXP_REPLACE_3",
                    parameters=(
                        _string,
                        _pattern,
                        _replacement,
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.regex_replace,
                        cost_us_per_million=26767.88,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="_DFA_EXTRACT",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Extract capture group using compiled DFA procedure.",
            documentation="Internal Draken-native capture-group extraction for REGEXP_REPLACE(s, pat, '\\1') calls the optimizer compiled to a DFA program.",
            overloads=(
                FunctionOverload(
                    id="_DFA_EXTRACT_2",
                    parameters=(
                        _string,
                        _compiled_program,
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=vector_dfa_extract,
                        cost_us_per_million=112.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="TRIM",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Remove leading and trailing whitespace.",
            documentation="Removes leading and trailing whitespace from string.",
            overloads=(
                FunctionOverload(
                    id="TRIM_1",
                    parameters=(_string, _trim_characters),
                    return_spec=ReturnSpec(mode="resolver", resolver=_trim_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=vector_trim,
                        cost_us_per_million=3529.82,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="LTRIM",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Remove leading whitespace.",
            documentation="Removes leading whitespace from string.",
            overloads=(
                FunctionOverload(
                    id="LTRIM_1",
                    parameters=(_string, _trim_characters),
                    return_spec=ReturnSpec(mode="resolver", resolver=_trim_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=vector_ltrim,
                        cost_us_per_million=3361.79,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="RTRIM",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Remove trailing whitespace.",
            documentation="Removes trailing whitespace from string.",
            overloads=(
                FunctionOverload(
                    id="RTRIM_1",
                    parameters=(_string, _trim_characters),
                    return_spec=ReturnSpec(mode="resolver", resolver=_trim_return_type),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=vector_rtrim,
                        cost_us_per_million=3178.18,
                    ),
                ),
            ),
        ),
        _make(
            "LEVENSHTEIN",
            vector_levenshtein,
            _CT_INT64,
            (
                _string,
                ParameterSpec(name="str2", type_family="string"),
            ),
            engine="draken",
            category="text",
            summary="Compute Levenshtein distance.",
            cost=2363.32,
        ),
        FunctionDefinition(
            name="LPAD",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Left-pad string to width.",
            documentation="Pads string on the left with fill character to reach specified width.",
            overloads=(
                FunctionOverload(
                    id="LPAD_3",
                    parameters=(
                        _string,
                        # `width` and `fill` are CONSTANTS. left_pad/right_pad
                        # (implementations/text.pyx) read `width[0]` and
                        # `fill[0]` — the FIRST row's value — and apply it to
                        # every row, so a column-valued width or fill is a
                        # silent wrong answer, not an error. The catalog typed
                        # them as an ordinary `integer` and `varchar`, which is
                        # what let `RPAD('eta', 8, s_null)` be written at all.
                        ParameterSpec(name="width", type_family="integer", constant_only=True),
                        ParameterSpec(name="fill", type_family="string", constant_only=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.left_pad,
                        cost_us_per_million=318.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="RPAD",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Right-pad string to width.",
            documentation="Pads string on the right with fill character to reach specified width.",
            overloads=(
                FunctionOverload(
                    id="RPAD_3",
                    parameters=(
                        _string,
                        # `width` and `fill` are CONSTANTS. left_pad/right_pad
                        # (implementations/text.pyx) read `width[0]` and
                        # `fill[0]` — the FIRST row's value — and apply it to
                        # every row, so a column-valued width or fill is a
                        # silent wrong answer, not an error. The catalog typed
                        # them as an ordinary `integer` and `varchar`, which is
                        # what let `RPAD('eta', 8, s_null)` be written at all.
                        ParameterSpec(name="width", type_family="integer", constant_only=True),
                        ParameterSpec(name="fill", type_family="string", constant_only=True),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.right_pad,
                        cost_us_per_million=321.0,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="TO_CHAR",
            aliases=("CHR",),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Convert codepoint to character.",
            documentation="Converts an integer codepoint to its corresponding character.",
            overloads=(
                FunctionOverload(
                    id="TO_CHAR_1",
                    parameters=(
                        ParameterSpec(
                            name="num",
                            type_family="integer",
                            # A Unicode CODEPOINT, not an arbitrary integer. Typed
                            # as a plain `integer`, TO_CHAR(-303083) satisfied the
                            # signature and the kernel answered with a raw
                            # `ValueError: draken_to_char: codepoint -303083 is not
                            # a Unicode scalar value`.
                            minimum=0,
                            maximum=1114111,
                            documentation=(
                                "A Unicode codepoint in 0..1114111 (U+10FFFF). The surrogate "
                                "range 55296..57343 (U+D800..U+DFFF) is excluded as well — "
                                "those are not Unicode scalar values."
                            ),
                        ),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARCHAR),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.to_char,
                        cost_us_per_million=8.2,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="TO_ASCII",
            aliases=("ASCII", "ORD"),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="Convert character to codepoint.",
            documentation="Converts the first character of a string to its integer codepoint.",
            overloads=(
                FunctionOverload(
                    id="TO_ASCII_1",
                    parameters=(_string,),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_INT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.to_ascii,
                        cost_us_per_million=6.8,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="_MATCH_AGAINST",
            aliases=(),
            category="text",
            volatility="stable",
            deterministic=False,
            lifecycle=LifecycleSpec(status="active"),
            summary="Text matching by embedding cosine similarity.",
            documentation=(
                "True when COSINE_SIMILARITY(column, query) >= the `match_threshold` "
                "session variable (default 0.5). Matching is only as semantic as the "
                "active EMBED capability: the built-in embedder is a lexical hashed "
                "projection, so by default MATCH behaves as a case-insensitive exact "
                "match rather than a meaning-based one. Install a semantic embedding "
                "capability, and/or tune `match_threshold`, to change that. Empty or "
                "stopword-only text embeds to a zero vector, giving an undefined (NaN) "
                "similarity, which never matches."
            ),
            overloads=(
                FunctionOverload(
                    id="_MATCH_AGAINST_2",
                    parameters=(
                        _string,
                        ParameterSpec(name="query", type_family="string"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_BOOLEAN),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        callable_ref=string_functions.match_against,
                        cost_us_per_million=1_500_000.0,
                    ),
                ),
            ),
        ),
    ]


__all__ = [
    "get_builtin_text_functions",
    "get_builtin_text_extended_functions",
]
