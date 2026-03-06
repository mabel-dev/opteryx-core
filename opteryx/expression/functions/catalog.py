"""Function catalog: centralized registry of function definitions, overloads, kernels, and metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import Literal
from typing import Optional
from typing import Tuple

# Type stubs for now; will be imported from actual modules
OrsoTypes = Any  # opteryx/datatypes/OrsoTypes
Node = Any  # AST node type


@dataclass(frozen=True)
class ParameterSpec:
    """Specification for a single function parameter."""

    name: str
    type_family: str  # exact, numeric, temporal, array<any>, any, etc.
    optional: bool = False
    variadic: bool = False
    constant_only: bool = False
    null_handling: Literal["strict", "passthrough", "unknown"] = "strict"
    documentation: str = ""


@dataclass(frozen=True)
class ResolvedArg:
    """Result of resolving one argument node during binding."""

    node: Node
    inferred_type: OrsoTypes
    coercion_cost: float = 0.0


@dataclass(frozen=True)
class BindingContext:
    """Runtime environment for type resolution and overload matching."""

    schema: Dict[str, OrsoTypes]  # available column types
    bound_args: Dict[int, ResolvedArg]  # previously bound arguments


@dataclass(frozen=True)
class ReturnSpec:
    """Specification for a function's return type."""

    mode: Literal["fixed", "same_as_arg", "resolver"]
    fixed_type: Optional[OrsoTypes] = None
    arg_index: Optional[int] = None
    # resolver receives: (parameters, resolved_args, context) and returns OrsoTypes
    resolver: Optional[
        Callable[[Tuple[ParameterSpec, ...], Dict[int, ResolvedArg], BindingContext], OrsoTypes]
    ] = None


@dataclass(frozen=True)
class KernelSpec:
    """Specification for a function kernel implementation."""

    id: str  # kernel identifier, e.g., "integer_integer" or "polymorphic"
    callable_ref: Callable
    null_policy: Literal["strict", "passthrough", "custom"] = "strict"
    cost_us_per_million: float = 0.0  # measured cost per million rows


@dataclass(frozen=True)
class LifecycleSpec:
    """Lifecycle management for a function (deprecation, removal, etc.)."""

    status: Literal["active", "deprecated", "experimental", "removed"]
    introduced: Optional[str] = None  # e.g., "v0.1"
    deprecated_in: Optional[str] = None
    remove_after: Optional[str] = None
    replacement: Optional[str] = None  # recommended replacement function name


@dataclass(frozen=True)
class FunctionOverload:
    """A single overload (callable form) of a function."""

    id: str
    parameters: Tuple[ParameterSpec, ...]
    return_spec: ReturnSpec
    kernel: KernelSpec


@dataclass(frozen=True)
class FunctionDefinition:
    """Definition of a single logical function with all overloads and metadata."""

    name: str
    aliases: Tuple[str, ...]  # e.g., ("CEILING", "CEIL")
    category: str  # "string", "numeric", "temporal", "aggregate", etc.
    volatility: Literal["immutable", "stable", "volatile"]
    deterministic: bool  # if false, cannot be constant-folded
    lifecycle: LifecycleSpec
    documentation: str  # long-form description, examples, notes
    summary: str  # one-line summary for signature help
    overloads: Tuple[FunctionOverload, ...]
    pushdown_safe: bool = False  # safe for remote connector pushdown
    foldable: bool = False  # enables constant folding (requires immutable + deterministic)


@dataclass(frozen=True)
class ResolvedFunction:
    """Output of overload resolution; used by binder and executor."""

    function_definition: FunctionDefinition
    selected_overload: FunctionOverload
    resolved_args: Dict[int, ResolvedArg]  # per-argument resolution with inferred types
    inferred_return_type: OrsoTypes


class FunctionCatalog:
    """Centralized registry of function definitions and resolution logic."""

    def __init__(self) -> None:
        """Initialize catalog and load builtin functions."""
        self._functions: Dict[str, FunctionDefinition] = {}
        self._aliases: Dict[str, str] = {}  # alias -> canonical name

        # Load builtin functions
        self._load_builtin_functions()

    def _load_builtin_functions(self) -> None:
        """Load all builtin function definitions into the catalog."""
        from opteryx.expression.functions.builtin_functions import get_builtin_functions

        for func_def in get_builtin_functions():
            self.register(func_def)

    def register(self, func_def: FunctionDefinition) -> None:
        """Register a function definition and its aliases.

        Args:
            func_def: FunctionDefinition to register.

        Raises:
            ValueError: If function already registered or alias conflicts.
        """
        if func_def.name in self._functions:
            raise ValueError(f"Function '{func_def.name}' already registered.")

        self._functions[func_def.name] = func_def

        # Register aliases
        for alias in func_def.aliases:
            if alias in self._aliases or alias in self._functions:
                raise ValueError(f"Alias '{alias}' conflicts with existing function or alias.")
            self._aliases[alias] = func_def.name

    def resolve(
        self,
        name: str,
        arg_nodes: list[Node],
        context: BindingContext,
    ) -> Optional[ResolvedFunction]:
        """Resolve a function call to a specific overload with inferred return type.

        Args:
            name: Function name (can be alias; will be resolved to canonical).
            arg_nodes: List of argument AST nodes.
            context: Binding context with schema and other metadata.

        Returns:
            ResolvedFunction with selected overload and inferred return type, or None if no match.
        """
        # TODO: Implement overload resolution algorithm
        # 1. Resolve alias to canonical name
        # 2. Find all overloads with matching arity
        # 3. Score overloads by type compatibility
        # 4. Select best match (or error if ambiguous)
        # 5. Infer return type
        # 6. Return ResolvedFunction
        raise NotImplementedError("resolve() not yet implemented")

    def get_definition(self, name: str) -> Optional[FunctionDefinition]:
        """Get function definition by name (or alias).

        Args:
            name: Function name or alias.

        Returns:
            FunctionDefinition or None if not found.
        """
        canonical = self._aliases.get(name, name)
        return self._functions.get(canonical)

    def get_kernel(self, func_name: str, kernel_id: Optional[str] = None) -> Optional[Callable]:
        """Get kernel callable by function name and optional kernel ID.

        Args:
            func_name: Function name.
            kernel_id: Specific kernel ID (e.g., "integer_integer"). If None, use default.

        Returns:
            Callable kernel or None if not found.
        """
        func_def = self.get_definition(func_name)
        if not func_def:
            return None

        if kernel_id is None:
            # Use first overload's kernel as default
            if func_def.overloads:
                return func_def.overloads[0].kernel.callable_ref
            return None

        # Find overload with matching kernel ID
        for overload in func_def.overloads:
            if overload.kernel.id == kernel_id:
                return overload.kernel.callable_ref

        return None

    def get_default_kernel(self, func_name: str) -> Optional[Callable]:
        """Get default kernel for a function (convenience wrapper).

        Args:
            func_name: Function name.

        Returns:
            Callable kernel or None if not found.
        """
        return self.get_kernel(func_name, kernel_id=None)

    def get_cost(self, func_name: str, kernel_id: Optional[str] = None) -> Optional[float]:
        """Get cost estimate for a function kernel.

        Args:
            func_name: Function name.
            kernel_id: Specific kernel ID. If None, use default.

        Returns:
            Cost in microseconds per million rows, or None if not found.
        """
        func_def = self.get_definition(func_name)
        if not func_def:
            return None

        if kernel_id is None:
            # Use first overload's kernel cost as default
            if func_def.overloads:
                return func_def.overloads[0].kernel.cost_us_per_million
            return None

        # Find overload with matching kernel ID
        for overload in func_def.overloads:
            if overload.kernel.id == kernel_id:
                return overload.kernel.cost_us_per_million

        return None

    def list_functions(
        self,
        include_deprecated: bool = False,
        category: Optional[str] = None,
    ) -> list[FunctionDefinition]:
        """List all registered functions.

        Args:
            include_deprecated: If False, exclude functions with status="deprecated".
            category: If specified, only return functions matching this category.

        Returns:
            List of FunctionDefinition objects.
        """
        result = []
        for func_def in self._functions.values():
            if not include_deprecated and func_def.lifecycle.status == "deprecated":
                continue
            if category is not None and func_def.category != category:
                continue
            result.append(func_def)
        return result


# Global catalog singleton
_CATALOG: Optional[FunctionCatalog] = None


def get_catalog() -> FunctionCatalog:
    """Get or create the global function catalog.

    Returns:
        The global FunctionCatalog instance.
    """
    global _CATALOG
    if _CATALOG is None:
        _CATALOG = FunctionCatalog()
    return _CATALOG
