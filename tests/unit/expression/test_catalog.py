"""Tests for the function catalog."""

from types import SimpleNamespace

import pytest
from orso.types import OrsoTypes

from opteryx.expression.functions import (
    BindingContext,
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
    ResolvedFunction,
    ReturnSpec,
    get_catalog,
)


class TestFunctionCatalog:
    """Test FunctionCatalog basic operations."""

    def test_catalog_singleton(self):
        """get_catalog() returns a singleton."""
        cat1 = get_catalog()
        cat2 = get_catalog()
        assert cat1 is cat2

    def test_register_function(self):
        """Can register a function definition."""
        catalog = get_catalog()

        func_def = FunctionDefinition(
            name="TEST_FUNC",
            aliases=(),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            documentation="Test function",
            summary="A test function",
            overloads=(
                FunctionOverload(
                    id="TEST_FUNC_1",
                    parameters=(ParameterSpec(name="x", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=int),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=lambda x: x,
                        cost_us_per_million=1.0,
                    ),
                ),
            ),
        )

        catalog.register(func_def)
        assert catalog.get_definition("TEST_FUNC") is func_def

    def test_register_with_aliases(self):
        """Aliases are registered and resolvable."""
        catalog = get_catalog()

        func_def = FunctionDefinition(
            name="CANONICAL",
            aliases=("ALIAS1", "ALIAS2"),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            documentation="Test",
            summary="Test",
            overloads=(
                FunctionOverload(
                    id="CANONICAL_1",
                    parameters=(),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=int),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=lambda: 42,
                        cost_us_per_million=0.0,
                    ),
                ),
            ),
        )

        catalog.register(func_def)

        # All names should resolve to the same function
        assert catalog.get_definition("CANONICAL") is func_def
        assert catalog.get_definition("ALIAS1") is func_def
        assert catalog.get_definition("ALIAS2") is func_def

    def test_get_kernel_default(self):
        """get_kernel with no kernel_id returns default kernel."""
        catalog = get_catalog()

        def my_kernel(x):
            return x * 2

        func_def = FunctionDefinition(
            name="DOUBLE_TEST",
            aliases=(),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            documentation="Test",
            summary="Test",
            overloads=(
                FunctionOverload(
                    id="DOUBLE_TEST_1",
                    parameters=(ParameterSpec(name="x", type_family="numeric"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=int),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=my_kernel,
                        cost_us_per_million=2.0,
                    ),
                ),
            ),
        )

        catalog.register(func_def)
        kernel = catalog.get_kernel("DOUBLE_TEST")
        assert kernel is my_kernel
        assert kernel(5) == 10

    def test_get_cost(self):
        """get_cost returns kernel cost."""
        catalog = get_catalog()

        func_def = FunctionDefinition(
            name="EXPENSIVE",
            aliases=(),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            documentation="Test",
            summary="Test",
            overloads=(
                FunctionOverload(
                    id="EXPENSIVE_1",
                    parameters=(),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=int),
                    kernel=KernelSpec(
                        id="default",
                        callable_ref=lambda: 0,
                        cost_us_per_million=999.99,
                    ),
                ),
            ),
        )

        catalog.register(func_def)
        cost = catalog.get_cost("EXPENSIVE")
        assert cost == 999.99

    def test_list_functions(self):
        """list_functions returns all registered functions."""
        catalog = get_catalog()

        func_def = FunctionDefinition(
            name="LISTED",
            aliases=(),
            category="metrics",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            documentation="Test",
            summary="Test",
            overloads=(),
        )

        catalog.register(func_def)
        functions = catalog.list_functions(category="metrics")
        assert any(f.name == "LISTED" for f in functions)

    def test_register_duplicate_raises(self):
        """Registering duplicate function name raises error."""
        catalog = get_catalog()

        func_def = FunctionDefinition(
            name="UNIQUE",
            aliases=(),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            documentation="Test",
            summary="Test",
            overloads=(),
        )

        catalog.register(func_def)

        with pytest.raises(ValueError, match="already registered"):
            catalog.register(func_def)

    def test_builtin_functions_loaded(self):
        """Builtin functions are loaded on catalog initialization."""
        catalog = get_catalog()

        # Check a sample of builtin functions from each category
        # Text functions
        assert catalog.get_definition("UPPER") is not None
        assert catalog.get_definition("LOWER") is not None
        assert catalog.get_definition("LENGTH") is not None
        assert catalog.get_definition("CONCAT") is not None

        # Arithmetic functions
        assert catalog.get_definition("ROUND") is not None
        assert catalog.get_definition("ABS") is not None
        assert catalog.get_definition("CEILING") is not None
        assert catalog.get_definition("FLOOR") is not None

        # Logical functions
        assert catalog.get_definition("COALESCE") is not None
        assert catalog.get_definition("IFNULL") is not None
        assert catalog.get_definition("NULLIF") is not None
        
        # Note: Type conversion functions (INTEGER, VARCHAR, DOUBLE, etc.) are 
        # intentionally NOT in the catalog - they are handled via CAST(x AS type)
        # and x::type syntax as specialized operations, not functions

    def test_builtin_function_aliases(self):
        """Builtin functions with aliases are resolvable."""
        catalog = get_catalog()

        # TITLE and TITLECASE are aliases for INITCAP
        initcap_def = catalog.get_definition("INITCAP")
        title_def = catalog.get_definition("TITLE")
        titlecase_def = catalog.get_definition("TITLECASE")
        assert initcap_def is title_def
        assert title_def is titlecase_def

    def test_builtin_function_kernels(self):
        """Builtin functions have accessible kernels."""
        catalog = get_catalog()

        # Get a kernel for UPPER
        upper_kernel = catalog.get_kernel("UPPER")
        assert upper_kernel is not None

        # Get cost for LOWER
        lower_cost = catalog.get_cost("LOWER")
        assert lower_cost == 5.0


class TestResolve:
    """Tests for FunctionCatalog.resolve() overload resolution."""

    def _make_catalog(self) -> "FunctionCatalog":
        """Fresh isolated catalog (not the singleton) for test isolation."""
        from opteryx.expression.functions.catalog import FunctionCatalog
        cat = FunctionCatalog.__new__(FunctionCatalog)
        cat._functions = {}
        cat._aliases = {}
        return cat

    def _make_context(self):
        return BindingContext(schema={}, bound_args={})

    @staticmethod
    def _make_node(type_, *, element_type=None):
        schema_column = SimpleNamespace(type=type_, element_type=element_type)
        return SimpleNamespace(type=type_, element_type=element_type, schema_column=schema_column)

    def test_resolve_returns_none_for_unknown_function(self):
        """resolve() returns None for unregistered function name."""
        catalog = self._make_catalog()
        result = catalog.resolve("NONEXISTENT", [], self._make_context())
        assert result is None

    def test_resolve_basic(self):
        """resolve() returns ResolvedFunction for a registered function."""
        from opteryx.expression.functions import (
            FunctionDefinition, FunctionOverload, KernelSpec,
            LifecycleSpec, ParameterSpec, ReturnSpec, ResolvedFunction,
        )

        catalog = self._make_catalog()
        catalog.register(FunctionDefinition(
            name="MY_UPPER",
            aliases=(),
            category="text",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="",
            documentation="",
            overloads=(
                FunctionOverload(
                    id="MY_UPPER_1",
                    parameters=(ParameterSpec(name="s", type_family="string"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.VARCHAR),
                    kernel=KernelSpec(id="default", callable_ref=str.upper, cost_us_per_million=1.0),
                ),
            ),
        ))

        node = object()  # no .type attribute
        result = catalog.resolve("MY_UPPER", [node], self._make_context())

        assert isinstance(result, ResolvedFunction)
        assert result.function_definition.name == "MY_UPPER"
        assert result.inferred_return_type == OrsoTypes.VARCHAR
        assert result.selected_overload.id == "MY_UPPER_1"

    def test_resolve_via_alias(self):
        """resolve() resolves aliases to the canonical function."""
        from opteryx.expression.functions import (
            FunctionDefinition, FunctionOverload, KernelSpec,
            LifecycleSpec, ParameterSpec, ReturnSpec,
        )

        catalog = self._make_catalog()
        catalog.register(FunctionDefinition(
            name="CANONICAL_FN",
            aliases=("ALIAS_FN",),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="",
            documentation="",
            overloads=(
                FunctionOverload(
                    id="CANONICAL_FN_1",
                    parameters=(ParameterSpec(name="x", type_family="any"),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.INTEGER),
                    kernel=KernelSpec(id="default", callable_ref=lambda x: x, cost_us_per_million=1.0),
                ),
            ),
        ))

        result = catalog.resolve("ALIAS_FN", [object()], self._make_context())
        assert result is not None
        assert result.function_definition.name == "CANONICAL_FN"

    def test_resolve_arity_mismatch_raises(self):
        """resolve() raises TypeError when no overload matches the argument count."""
        from opteryx.expression.functions import (
            FunctionDefinition, FunctionOverload, KernelSpec,
            LifecycleSpec, ParameterSpec, ReturnSpec,
        )

        catalog = self._make_catalog()
        catalog.register(FunctionDefinition(
            name="FIXED_ARITY",
            aliases=(),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="",
            documentation="",
            overloads=(
                FunctionOverload(
                    id="FIXED_ARITY_2",
                    parameters=(
                        ParameterSpec(name="a", type_family="any"),
                        ParameterSpec(name="b", type_family="any"),
                    ),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.INTEGER),
                    kernel=KernelSpec(id="default", callable_ref=lambda a, b: a, cost_us_per_million=1.0),
                ),
            ),
        ))

        with pytest.raises(TypeError, match="does not accept"):
            catalog.resolve("FIXED_ARITY", [object()], self._make_context())  # 1 arg, needs 2

    def test_resolve_variadic(self):
        """resolve() matches variadic overloads with any number of args."""
        from opteryx.expression.functions import (
            FunctionDefinition, FunctionOverload, KernelSpec,
            LifecycleSpec, ParameterSpec, ReturnSpec,
        )

        catalog = self._make_catalog()
        catalog.register(FunctionDefinition(
            name="VARIADIC_FN",
            aliases=(),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="",
            documentation="",
            overloads=(
                FunctionOverload(
                    id="VARIADIC_FN_1",
                    parameters=(ParameterSpec(name="args", type_family="any", variadic=True),),
                    return_spec=ReturnSpec(mode="fixed", fixed_type=OrsoTypes.VARCHAR),
                    kernel=KernelSpec(id="default", callable_ref=lambda *a: a, cost_us_per_million=1.0),
                ),
            ),
        ))

        # Should match 0, 1, 3, 10 args
        ctx = self._make_context()
        for n in (1, 3, 10):
            result = catalog.resolve("VARIADIC_FN", [object()] * n, ctx)
            assert result is not None, f"Expected match for {n} args"

    def test_resolve_return_type_same_as_arg(self):
        """resolve() uses first arg type for same_as_arg return spec."""
        from opteryx.expression.functions import (
            FunctionDefinition, FunctionOverload, KernelSpec,
            LifecycleSpec, ParameterSpec, ReturnSpec,
        )

        catalog = self._make_catalog()
        catalog.register(FunctionDefinition(
            name="PASSTHRU_FN",
            aliases=(),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="",
            documentation="",
            overloads=(
                FunctionOverload(
                    id="PASSTHRU_FN_1",
                    parameters=(ParameterSpec(name="x", type_family="any"),),
                    return_spec=ReturnSpec(mode="same_as_arg", arg_index=0),
                    kernel=KernelSpec(id="default", callable_ref=lambda x: x, cost_us_per_million=0.0),
                ),
            ),
        ))

        class NodeWithType:
            type = OrsoTypes.DOUBLE

        result = catalog.resolve("PASSTHRU_FN", [NodeWithType()], self._make_context())
        assert result.inferred_return_type == OrsoTypes.DOUBLE

    def test_resolve_return_type_resolver(self):
        """resolve() calls resolver function to infer return type."""
        from opteryx.expression.functions import (
            FunctionDefinition, FunctionOverload, KernelSpec,
            LifecycleSpec, ParameterSpec, ReturnSpec,
        )

        sentinel_type = OrsoTypes.TIMESTAMP

        def my_resolver(arg_nodes):
            return sentinel_type

        catalog = self._make_catalog()
        catalog.register(FunctionDefinition(
            name="RESOLVER_FN",
            aliases=(),
            category="test",
            volatility="immutable",
            deterministic=True,
            lifecycle=LifecycleSpec(status="active"),
            summary="",
            documentation="",
            overloads=(
                FunctionOverload(
                    id="RESOLVER_FN_1",
                    parameters=(ParameterSpec(name="x", type_family="any"),),
                    return_spec=ReturnSpec(mode="resolver", resolver=my_resolver),
                    kernel=KernelSpec(id="default", callable_ref=lambda x: x, cost_us_per_million=0.0),
                ),
            ),
        ))

        result = catalog.resolve("RESOLVER_FN", [object()], self._make_context())
        assert result.inferred_return_type == sentinel_type

    def test_resolve_catalog_registered_functions(self):
        """Catalog-registered functions are resolvable by name and arity."""
        from opteryx.expression.functions import get_catalog
        catalog = get_catalog()

        # EXTRACT is hand-crafted with a 2-arg overload; test with 2 args.
        for name, argc in (
            ("TRIM", 1),
            ("LEVENSHTEIN", 2),
            ("SHA256", 1),
            ("EXTRACT", 2),
        ):
            assert catalog.get_definition(name) is not None, f"{name} should be in catalog"
            result = catalog.resolve(name, [object()] * argc, BindingContext(schema={}, bound_args={}))
            assert result is not None, f"resolve('{name}') should return a match"

        assert catalog.get_definition("TRUNC") is not None

        assert catalog.get_definition("DATEPART") is None
        assert catalog.get_definition("DATE_PART") is None
        assert catalog.get_definition("DATE_TRUNC") is None
        assert catalog.get_definition("DATETRUNC") is None

    def test_resolve_cosine_similarity_prefers_numeric_vector_overload(self):
        catalog = get_catalog()

        result = catalog.resolve(
            "COSINE_SIMILARITY",
            [
                self._make_node(OrsoTypes.ARRAY, element_type=OrsoTypes.DOUBLE),
                self._make_node(OrsoTypes.ARRAY, element_type=OrsoTypes.DOUBLE),
            ],
            self._make_context(),
        )

        assert result is not None
        assert result.selected_overload.id == "COSINE_SIMILARITY_VECTOR"

    def test_resolve_cosine_similarity_prefers_text_overload(self):
        catalog = get_catalog()

        result = catalog.resolve(
            "COSINE_SIMILARITY",
            [
                self._make_node(OrsoTypes.VARCHAR),
                self._make_node(OrsoTypes.VARCHAR),
            ],
            self._make_context(),
        )

        assert result is not None
        assert result.selected_overload.id == "COSINE_SIMILARITY_TEXT"

    def test_resolve_cosine_distance_prefers_numeric_vector_overload(self):
        catalog = get_catalog()

        result = catalog.resolve(
            "COSINE_DISTANCE",
            [
                self._make_node(OrsoTypes.ARRAY, element_type=OrsoTypes.DOUBLE),
                self._make_node(OrsoTypes.ARRAY, element_type=OrsoTypes.DOUBLE),
            ],
            self._make_context(),
        )

        assert result is not None
        assert result.selected_overload.id == "COSINE_DISTANCE_VECTOR"

    def test_resolve_trunc_selects_numeric_and_temporal_overloads(self):
        from opteryx.expression.functions import get_catalog

        catalog = get_catalog()

        numeric = catalog.resolve(
            "TRUNC",
            [self._make_node(OrsoTypes.DOUBLE), self._make_node(OrsoTypes.INTEGER)],
            self._make_context(),
        )
        assert numeric is not None
        assert numeric.selected_overload.id == "TRUNC_numeric"
        assert numeric.inferred_return_type == OrsoTypes.DOUBLE

        temporal = catalog.resolve(
            "TRUNC",
            [self._make_node(OrsoTypes.TIMESTAMP), self._make_node(OrsoTypes.VARCHAR)],
            self._make_context(),
        )
        assert temporal is not None
        assert temporal.selected_overload.id == "TRUNC_temporal"
        assert temporal.inferred_return_type == OrsoTypes.TIMESTAMP
