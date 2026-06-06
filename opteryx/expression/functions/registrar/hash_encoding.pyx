from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ReturnSpec,
)
from opteryx.types.logical_type import LogicalCategory


def get_builtin_hash_encoding_functions() -> list[FunctionDefinition]:
    """Hash, encoding, and random-generation functions."""

    # Local imports to avoid top-level import cycles and to keep costs lazy.
    from opteryx.compiled.nanobind.vector_codec import (
        vector_base64_decode,
        vector_base64_encode,
        vector_base85_decode,
        vector_base85_encode,
    )
    from opteryx.compiled.nanobind.vector_hash_codec import (
        vector_hex_decode,
        vector_hex_encode,
        vector_md5,
        vector_sha1,
        vector_sha256,
        vector_sha512,
    )
    from opteryx.expression.functions.implementations import arithmetic as number_functions
    from opteryx.expression.functions.implementations import text as string_functions
    from opteryx.expression.functions.registrar import _iterate_single_parameter as _isingle
    from opteryx.third_party.cyan4973.xxhash import hash_bytes

    # Small wrapper kernels for single-argument stringification/encoding paths.
    _hash_kernel = _isingle(lambda x: hex(hash_bytes(str(x).encode()))[2:])
    _sha224_kernel = _isingle(string_functions.get_sha224)
    _sha384_kernel = _isingle(string_functions.get_sha384)

    # Parameter short-hands
    _any = ParameterSpec(name="val", type_family="any")
    _n = ParameterSpec(name="n", type_family="integer")
    _b = ParameterSpec(name="blob", type_family="any")

    functions: list[FunctionDefinition] = [
        _make(
            "HASH", _hash_kernel, LogicalCategory.BLOB, (_any,), cost=437424.69, summary="Generic hash."
        ),
        _make(
            "MD5",
            vector_md5,
            LogicalCategory.BLOB,
            (_any,),
            engine="draken",
            cost=8.44,
            summary="MD5 hash.",
        ),
        _make(
            "SHA1",
            vector_sha1,
            LogicalCategory.BLOB,
            (_any,),
            engine="draken",
            cost=5.10,
            summary="SHA-1 hash.",
        ),
        _make(
            "SHA224",
            _sha224_kernel,
            LogicalCategory.BLOB,
            (_any,),
            cost=634394.82,
            summary="SHA-224 hash.",
        ),
        _make(
            "SHA256",
            vector_sha256,
            LogicalCategory.BLOB,
            (_any,),
            engine="draken",
            cost=7.56,
            summary="SHA-256 hash.",
        ),
        _make(
            "SHA384",
            _sha384_kernel,
            LogicalCategory.BLOB,
            (_any,),
            cost=714225.82,
            summary="SHA-384 hash.",
        ),
        _make(
            "SHA512",
            vector_sha512,
            LogicalCategory.BLOB,
            (_any,),
            engine="draken",
            cost=7.47,
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=LogicalCategory.DOUBLE),
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=LogicalCategory.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="zero_arg",
                        callable_ref=number_functions.random_number,
                        null_policy="compress",
                        cost_us_per_million=85541.62,
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=LogicalCategory.DOUBLE),
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
                    return_spec=ReturnSpec(mode="fixed", fixed_type=LogicalCategory.DOUBLE),
                    kernel=KernelSpec(
                        engine="draken",
                        id="zero_arg",
                        callable_ref=number_functions.random_normal,
                        null_policy="compress",
                        cost_us_per_million=86571.94,
                    ),
                ),
            ),
        ),
        # Other convenience encoders/decoders
        _make(
            "RANDOM_STRING",
            number_functions.random_strings,
            LogicalCategory.BLOB,
            (_n,),
            volatility="volatile",
            summary="Generate random strings.",
            cost=372872.83,
        ),
        _make(
            "BASE64_ENCODE",
            vector_base64_encode,
            LogicalCategory.BLOB,
            (_b,),
            engine="draken",
            summary="Base64 encode.",
            cost=3.40,
        ),
        _make(
            "BASE64_DECODE",
            vector_base64_decode,
            LogicalCategory.BLOB,
            (_b,),
            engine="draken",
            summary="Base64 decode.",
            cost=2.60,
        ),
        _make(
            "BASE85_ENCODE",
            vector_base85_encode,
            LogicalCategory.BLOB,
            (_b,),
            engine="draken",
            summary="Base85 encode.",
            cost=3.60,
        ),
        _make(
            "BASE85_DECODE",
            vector_base85_decode,
            LogicalCategory.BLOB,
            (_b,),
            engine="draken",
            summary="Base85 decode.",
            cost=2.85,
        ),
        _make(
            "HEX_ENCODE",
            vector_hex_encode,
            LogicalCategory.BLOB,
            (_b,),
            summary="Hex encode.",
            cost=539725.99,
        ),
        _make(
            "HEX_DECODE", vector_hex_decode, LogicalCategory.BLOB, (_b,), summary="Hex decode.", cost=3.87
        ),
    ]

    return functions
