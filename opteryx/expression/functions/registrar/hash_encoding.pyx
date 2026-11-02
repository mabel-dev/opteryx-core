from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
# LogicalCategory imported via __init__.pyx (textually included); canonical ColumnTypes also in scope.


def get_builtin_hash_encoding_functions() -> list[FunctionDefinition]:
    """Hash, encoding, and random-generation functions."""

    # Parameter short-hands.
    #
    # These are "string", not "any", because that is what the kernels accept —
    # every one of them rejects a non-string with `draken_<fn>: string operand
    # required`. Declaring "any" did not make them accept more; it only moved
    # the refusal from plan time into the kernel, where it surfaced as a raw
    # ValueError naming an internal function (or, over a real column, as
    # `ExprMultiProjectOperator: error code 1`). The "string" family covers
    # VARCHAR, NVARCHAR and VARBINARY, which is the full set they do accept.
    _any = ParameterSpec(name="val", type_family="string")
    _n = ParameterSpec(name="n", type_family="integer")
    _b = ParameterSpec(name="blob", type_family="string")

    # The decoders are strict about their input encoding: anything that is not
    # well formed in that encoding raises. `string` alone said nothing about it,
    # so every VARCHAR was a legal argument by the signature and most of them
    # fail. `value_format` states the real constraint.
    def _encoded(fmt: str) -> ParameterSpec:
        return ParameterSpec(
            name="blob",
            type_family="string",
            value_format=fmt,
            documentation=f"Must be well-formed {fmt} text; other input is rejected at execution.",
        )

    # Return types follow what the kernels ACTUALLY produce, which is also what the
    # value semantically IS:
    #   digests + *_ENCODE -> hex/base64/base85 TEXT      -> VARCHAR
    #   *_DECODE           -> raw decoded BYTES           -> VARBINARY
    # These previously all declared VARBINARY while the kernels emitted VARCHAR, so
    # the same function returned bytes for a literal argument (constant folding
    # trusts the declared type) but str for a column argument (the kernel's real
    # data flows) — e.g. MD5('Earth') -> b'5cdd...' vs MD5(name) -> '5cdd...'.
    # HASH is the exception: it emits VARBINARY and always did, so it stays.
    functions: list[FunctionDefinition] = [
        _make(
            "HASH", _CT_VARBINARY, (_any,), cost=27801.26, summary="Generic hash."
        ),
        _make(
            "MD5",
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=71082.66,
            summary="MD5 hash.",
        ),
        _make(
            "SHA1",
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=34097.11,
            summary="SHA-1 hash.",
        ),
        _make(
            "SHA224",
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=39687.55,
            summary="SHA-224 hash.",
        ),
        _make(
            "SHA256",
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=32107.07,
            summary="SHA-256 hash.",
        ),
        _make(
            "SHA384",
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=91700.50,
            summary="SHA-384 hash.",
        ),
        _make(
            "SHA512",
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=92702.58,
            summary="SHA-512 hash.",
        ),
        # RANDOM and NORMAL need explicit FunctionDefinition to include both arities
        FunctionDefinition(
            name="RANDOM",
            aliases=("RAND",),
            category="hash_encoding",
            volatility="volatile",
            deterministic=False,
            lifecycle=LifecycleSpec(status="active"),
            summary="Generate random numbers.",
            documentation="Returns uniform random float(s) in [0, 1).",
            overloads=(
                FunctionOverload(
                    id="RANDOM_default",
                    parameters=(_n,),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        cost_us_per_million=2742.84,
                    ),
                ),
                FunctionOverload(
                    id="RANDOM_0",
                    parameters=(),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="zero_arg",
                        cost_us_per_million=2737.36,
                    ),
                ),
            ),
        ),
        FunctionDefinition(
            name="NORMAL",
            aliases=(),
            category="hash_encoding",
            volatility="volatile",
            deterministic=False,
            lifecycle=LifecycleSpec(status="active"),
            summary="Generate normally-distributed random numbers.",
            documentation="Returns normally-distributed random float(s).",
            overloads=(
                FunctionOverload(
                    id="NORMAL_default",
                    parameters=(_n,),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        cost_us_per_million=5355.11,
                    ),
                ),
                FunctionOverload(
                    id="NORMAL_0",
                    parameters=(),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="zero_arg",
                        cost_us_per_million=5348.12,
                    ),
                ),
            ),
        ),
        # Other convenience encoders/decoders
        #
        # RANDOM_STRING(n) -> VARBINARY: n random BYTES per row (architect ruling
        # 2026-07-17). C-native only — the draken_random_string kernel
        # (function_string_extra.cpp) is the sole implementation. It is VOLATILE, so
        # it is never constant-folded — constant_folding.py excludes it.
        # The old `number_functions.random_strings` binding is removed: its
        # (row_count, width) VARCHAR callable never matched this 1-param VARBINARY
        # signature, and the native engine has no per-morsel Python fallback anyway.
        FunctionDefinition(
            name="RANDOM_STRING",
            aliases=(),
            category="text",
            volatility="volatile",
            deterministic=False,
            lifecycle=LifecycleSpec(status="active"),
            summary="Generate random bytes.",
            documentation="Returns n random bytes as VARBINARY, one value per row.",
            overloads=(
                FunctionOverload(
                    id="RANDOM_STRING_default",
                    parameters=(_n,),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_VARBINARY),
                    kernel=KernelSpec(
                        engine="draken",
                        id="default",
                        cost_us_per_million=9189.69,
                    ),
                ),
            ),
        ),
        _make(
            "BASE64_ENCODE",
            _CT_VARCHAR,
            (_b,),
            engine="draken",
            summary="Base64 encode.",
            cost=5229.89,
        ),
        _make(
            "BASE64_DECODE",
            _CT_VARBINARY,
            (_encoded("base64"),),
            engine="draken",
            summary="Base64 decode.",
            cost=2.60,
        ),
        _make(
            "BASE85_ENCODE",
            _CT_VARCHAR,
            (_b,),
            engine="draken",
            summary="Base85 encode.",
            cost=7321.06,
        ),
        _make(
            "BASE85_DECODE",
            _CT_VARBINARY,
            (_encoded("base85"),),
            engine="draken",
            summary="Base85 decode.",
            cost=9180.68,
        ),
        _make(
            "HEX_ENCODE",
            _CT_VARCHAR,
            (_b,),
            summary="Hex encode.",
            cost=6652.75,
        ),
        _make(
            "HEX_DECODE", _CT_VARBINARY, (_encoded("hex"),), summary="Hex decode.", cost=3.87
        ),
    ]

    return functions
