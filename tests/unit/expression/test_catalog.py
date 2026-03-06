"""Tests for the function catalog."""

import pytest

from opteryx.expression.functions import (
    FunctionDefinition,
    FunctionOverload,
    KernelSpec,
    LifecycleSpec,
    ParameterSpec,
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
        assert catalog.get_definition("CEIL") is not None
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

        # CEILING is an alias for CEIL
        ceil_def = catalog.get_definition("CEIL")
        ceiling_def = catalog.get_definition("CEILING")
        assert ceil_def is ceiling_def

    def test_builtin_function_kernels(self):
        """Builtin functions have accessible kernels."""
        catalog = get_catalog()

        # Get a kernel for UPPER
        upper_kernel = catalog.get_kernel("UPPER")
        assert upper_kernel is not None

        # Get cost for LOWER
        lower_cost = catalog.get_cost("LOWER")
        assert lower_cost == 5.0
