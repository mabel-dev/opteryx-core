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

    # Local imports to avoid top-level import cycles and to keep costs lazy.
    from opteryx.compiled.nanobind.vectors import (
        vector_base64_decode,
        vector_base64_encode,
        vector_base85_decode,
        vector_base85_encode,
    )
    from opteryx.compiled.nanobind.vectors import (
        vector_hex_decode,
        vector_hex_encode,
        vector_md5,
        vector_sha1,
        vector_sha224,
        vector_sha256,
        vector_sha384,
        vector_sha512,
    )
    from draken.interop.vector_sequence import vector_from_sequence
    import draken.draken_native as _draken_native_he
    from draken.vectors.vector import Vector as _Vector
    from opteryx.expression.functions.implementations import arithmetic as number_functions
    from opteryx.third_party.cyan4973.xxhash import hash_bytes

    def _hash_kernel(array):
        result = [
            hex(hash_bytes(str(x).encode()))[2:].encode() if x is not None else None
            for x in array
        ]
        return _Vector(vector_from_sequence(result, dtype=_draken_native_he.DrakenType.VARBINARY))

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
            "HASH", _hash_kernel, _CT_VARBINARY, (_any,), cost=437424.69, summary="Generic hash."
        ),
        _make(
            "MD5",
            vector_md5,
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=70856.38,
            summary="MD5 hash.",
        ),
        _make(
            "SHA1",
            vector_sha1,
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=31113.37,
            summary="SHA-1 hash.",
        ),
        _make(
            "SHA224",
            vector_sha224,
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=38132.87,
            summary="SHA-224 hash.",
        ),
        _make(
            "SHA256",
            vector_sha256,
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=32147.72,
            summary="SHA-256 hash.",
        ),
        _make(
            "SHA384",
            vector_sha384,
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=89807.05,
            summary="SHA-384 hash.",
        ),
        _make(
            "SHA512",
            vector_sha512,
            _CT_VARCHAR,
            (_any,),
            engine="draken",
            cost=91033.24,
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
                        callable_ref=number_functions.random_number,
                        cost_us_per_million=85541.62,
                    ),
                ),
                FunctionOverload(
                    id="RANDOM_0",
                    parameters=(),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="zero_arg",
                        callable_ref=number_functions.random_number,
                        cost_us_per_million=2824.05,
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
                        callable_ref=number_functions.random_normal,
                        cost_us_per_million=86571.94,
                    ),
                ),
                FunctionOverload(
                    id="NORMAL_0",
                    parameters=(),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=_CT_FLOAT64),
                    kernel=KernelSpec(
                        engine="draken",
                        id="zero_arg",
                        callable_ref=number_functions.random_normal,
                        cost_us_per_million=7397.25,
                    ),
                ),
            ),
        ),
        # Other convenience encoders/decoders
        #
        # RANDOM_STRING(n) -> VARBINARY: n random BYTES per row (architect ruling
        # 2026-07-17). C-native only — the draken_random_string kernel
        # (function_string_extra.cpp) is the sole implementation, so callable_ref
        # is None (the coalesce/iif precedent: a c-native function declares "no
        # Python"). It is VOLATILE, so it is never constant-folded (the one path a
        # callable_ref would otherwise serve) — constant_folding.py excludes it.
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
                        callable_ref=None,
                        cost_us_per_million=372872.83,
                    ),
                ),
            ),
        ),
        _make(
            "BASE64_ENCODE",
            vector_base64_encode,
            _CT_VARCHAR,
            (_b,),
            engine="draken",
            summary="Base64 encode.",
            cost=6998.99,
        ),
        _make(
            "BASE64_DECODE",
            vector_base64_decode,
            _CT_VARBINARY,
            (_encoded("base64"),),
            engine="draken",
            summary="Base64 decode.",
            cost=2.60,
        ),
        _make(
            "BASE85_ENCODE",
            vector_base85_encode,
            _CT_VARCHAR,
            (_b,),
            engine="draken",
            summary="Base85 encode.",
            cost=8774.93,
        ),
        _make(
            "BASE85_DECODE",
            vector_base85_decode,
            _CT_VARBINARY,
            (_encoded("base85"),),
            engine="draken",
            summary="Base85 decode.",
            cost=9180.68,
        ),
        _make(
            "HEX_ENCODE",
            vector_hex_encode,
            _CT_VARCHAR,
            (_b,),
            summary="Hex encode.",
            cost=9353.05,
        ),
        _make(
            "HEX_DECODE", vector_hex_decode, _CT_VARBINARY, (_encoded("hex"),), summary="Hex decode.", cost=3.87
        ),
    ]

    return functions
