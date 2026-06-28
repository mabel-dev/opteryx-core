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

    # Parameter short-hands
    _any = ParameterSpec(name="val", type_family="any")
    _n = ParameterSpec(name="n", type_family="integer")
    _b = ParameterSpec(name="blob", type_family="any")

    functions: list[FunctionDefinition] = [
        _make(
            "HASH", _hash_kernel, _CT_VARBINARY, (_any,), cost=437424.69, summary="Generic hash."
        ),
        _make(
            "MD5",
            vector_md5,
            _CT_VARBINARY,
            (_any,),
            engine="draken",
            cost=70856.38,
            summary="MD5 hash.",
        ),
        _make(
            "SHA1",
            vector_sha1,
            _CT_VARBINARY,
            (_any,),
            engine="draken",
            cost=31113.37,
            summary="SHA-1 hash.",
        ),
        _make(
            "SHA224",
            vector_sha224,
            _CT_VARBINARY,
            (_any,),
            engine="draken",
            cost=38132.87,
            summary="SHA-224 hash.",
        ),
        _make(
            "SHA256",
            vector_sha256,
            _CT_VARBINARY,
            (_any,),
            engine="draken",
            cost=32147.72,
            summary="SHA-256 hash.",
        ),
        _make(
            "SHA384",
            vector_sha384,
            _CT_VARBINARY,
            (_any,),
            engine="draken",
            cost=89807.05,
            summary="SHA-384 hash.",
        ),
        _make(
            "SHA512",
            vector_sha512,
            _CT_VARBINARY,
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
                        null_policy="compress",
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
                        null_policy="compress",
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
                        null_policy="compress",
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
                        null_policy="compress",
                        cost_us_per_million=7397.25,
                    ),
                ),
            ),
        ),
        # Other convenience encoders/decoders
        _make(
            "RANDOM_STRING",
            number_functions.random_strings,
            _CT_VARBINARY,
            (_n,),
            volatility="volatile",
            summary="Generate random strings.",
            cost=372872.83,
        ),
        _make(
            "BASE64_ENCODE",
            vector_base64_encode,
            _CT_VARBINARY,
            (_b,),
            engine="draken",
            summary="Base64 encode.",
            cost=6998.99,
        ),
        _make(
            "BASE64_DECODE",
            vector_base64_decode,
            _CT_VARBINARY,
            (_b,),
            engine="draken",
            summary="Base64 decode.",
            cost=2.60,
        ),
        _make(
            "BASE85_ENCODE",
            vector_base85_encode,
            _CT_VARBINARY,
            (_b,),
            engine="draken",
            summary="Base85 encode.",
            cost=8774.93,
        ),
        _make(
            "BASE85_DECODE",
            vector_base85_decode,
            _CT_VARBINARY,
            (_b,),
            engine="draken",
            summary="Base85 decode.",
            cost=9180.68,
        ),
        _make(
            "HEX_ENCODE",
            vector_hex_encode,
            _CT_VARBINARY,
            (_b,),
            summary="Hex encode.",
            cost=9353.05,
        ),
        _make(
            "HEX_DECODE", vector_hex_decode, _CT_VARBINARY, (_b,), summary="Hex decode.", cost=3.87
        ),
    ]

    return functions
