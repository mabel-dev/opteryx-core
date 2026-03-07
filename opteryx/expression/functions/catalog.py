"""Function catalog: centralized registry of function definitions, overloads, kernels, and metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import Literal
from typing import Optional
from typing import Tuple

from orso.types import OrsoTypes

Node = Any  # AST node type (duck-typed; no import to avoid circular deps)


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
    # resolver receives the bound arg nodes and returns OrsoTypes.
    # May return (OrsoTypes, OrsoTypes) tuple for typed arrays: (array_type, element_type).
    resolver: Optional[Callable[[list], Any]] = None


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
    inferred_element_type: Optional[OrsoTypes] = None  # set for ARRAY<X> return types


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
        from opteryx.expression.functions.native_function_registrar import get_builtin_functions

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
        context: Optional[BindingContext] = None,
    ) -> Optional[ResolvedFunction]:
        """Resolve a function call to a specific overload with inferred return type.

        Resolution algorithm:
        1. Resolve alias to canonical name.
        2. Filter overloads by arity (accounting for optional/variadic params).
        3. Score remaining overloads by type family compatibility.
        4. Select overload with lowest total score; error if ambiguous.
        5. Infer return type from ReturnSpec.
        6. Return ResolvedFunction.

        Args:
            name: Function name (can be alias; will be resolved to canonical).
            arg_nodes: List of argument AST nodes.
            context: Binding context with schema and other metadata.

        Returns:
            ResolvedFunction with selected overload and inferred return type, or None if not found.

        Raises:
            TypeError: If function is found but no overload matches the call arity.
            TypeError: If multiple overloads tie and cannot be auto-ranked.
        """
        # 1. Resolve alias to canonical name
        canonical = self._aliases.get(name, name)
        func_def = self._functions.get(canonical)
        if func_def is None:
            return None

        argc = len(arg_nodes)

        # 2. Filter overloads by arity
        candidates: list[FunctionOverload] = []
        for overload in func_def.overloads:
            params = overload.parameters
            if not params:
                if argc == 0:
                    candidates.append(overload)
                continue

            required = sum(1 for p in params if not p.optional and not p.variadic)
            has_variadic = any(p.variadic for p in params)
            max_params = float("inf") if has_variadic else len(params)

            if required <= argc <= max_params:
                candidates.append(overload)

        if not candidates:
            raise TypeError(
                f"Function '{canonical}' does not accept {argc} argument(s). "
                f"Available overload(s): {[o.id for o in func_def.overloads]}"
            )

        # 3+4. Score by type family and select best match.
        # Scoring: exact type match = 0, family match = 1, "any" param = 2, no match = inf.
        # For Phase 1 nodes without type info, all args score as "any" (score 2 per param).
        # TODO Phase 2+: use resolved arg types from context for precise scoring.
        _INF = float("inf")

        def _score_overload(overload: FunctionOverload) -> float:
            params = overload.parameters
            if not params:
                return 0.0
            total = 0.0
            param_iter = iter(params)
            current_param = next(param_iter, None)
            for node in arg_nodes:
                if current_param is None:
                    return _INF  # too many args for non-variadic
                node_type = getattr(node, "type", None)
                if node_type is None or current_param.type_family == "any":
                    total += 2.0
                elif node_type == current_param.type_family:
                    total += 0.0  # exact match
                else:
                    total += 1.0  # family/coercion match
                if not current_param.variadic:
                    current_param = next(param_iter, None)
            return total

        scored = sorted(candidates, key=_score_overload)
        best_score = _score_overload(scored[0])
        tied = [o for o in scored if abs(_score_overload(o) - best_score) < 1e-9]

        if len(tied) > 1:
            options = "\n  ".join(
                f"- ({', '.join(p.type_family for p in o.parameters)}) "
                f"-> {o.return_spec.fixed_type}"
                for o in tied
            )
            raise TypeError(
                f"Ambiguous function call: {canonical}({', '.join(str(getattr(n, 'type', '?')) for n in arg_nodes)}) matches:\n  {options}\n"
                "Please use explicit CAST to disambiguate."
            )

        selected = scored[0]

        # 5. Infer return type from ReturnSpec.
        # For each arg node, prefer schema_column.type (set by binder after recursive bind)
        # over node.type (set at parse time; may be None for identifiers until bound).
        def _node_type(node) -> Optional[OrsoTypes]:
            sc = getattr(node, "schema_column", None)
            if sc is not None:
                return sc.type
            return getattr(node, "type", None)

        return_spec = selected.return_spec
        resolved_args = {
            i: ResolvedArg(node=node, inferred_type=_node_type(node))
            for i, node in enumerate(arg_nodes)
        }

        inferred_element_type: Optional[OrsoTypes] = None
        if return_spec.mode == "fixed":
            inferred_type = return_spec.fixed_type
        elif return_spec.mode == "same_as_arg":
            idx = return_spec.arg_index or 0
            inferred_type = resolved_args[idx].inferred_type if idx in resolved_args else None
        elif return_spec.mode == "resolver" and return_spec.resolver is not None:
            raw = return_spec.resolver(arg_nodes)
            if isinstance(raw, tuple):
                inferred_type, inferred_element_type = raw
            else:
                inferred_type = raw
        else:
            inferred_type = None

        # 6. Return ResolvedFunction
        return ResolvedFunction(
            function_definition=func_def,
            selected_overload=selected,
            resolved_args=resolved_args,
            inferred_element_type=inferred_element_type,
            inferred_return_type=inferred_type,
        )

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

    @classmethod
    def from_legacy_dict(
        cls,
        functions_dict: Dict[str, tuple],
        *,
        skip_existing: bool = True,
    ) -> "FunctionCatalog":
        """Create a FunctionCatalog pre-populated from the legacy FUNCTIONS dict.

        The legacy dict format is:
            { "FUNC_NAME": (callable, return_type_str, cost_float), ... }

        Generated entries use conservative defaults:
        - Single variadic overload accepting any argument types.
        - All return types mapped from string to OrsoTypes; VARIANT maps to NULL.
        - Lifecycle status "active".
        - No metadata (summary, documentation, etc.) — add manually as needed.

        Args:
            functions_dict: The legacy FUNCTIONS dict from opteryx.functions.
            skip_existing: If True, skip functions already registered. If False, raise on conflict.

        Returns:
            A new FunctionCatalog instance loaded with all legacy functions.
        """
        from orso.types import OrsoTypes

        _TYPE_MAP: Dict[str, OrsoTypes] = {
            "VARCHAR": OrsoTypes.VARCHAR,
            "INTEGER": OrsoTypes.INTEGER,
            "DOUBLE": OrsoTypes.DOUBLE,
            "BOOLEAN": OrsoTypes.BOOLEAN,
            "DATE": OrsoTypes.DATE,
            "TIMESTAMP": OrsoTypes.TIMESTAMP,
            "TIME": OrsoTypes.TIME,
            "BLOB": OrsoTypes.BLOB,
            "ARRAY": OrsoTypes.ARRAY,
            "ARRAY<VARCHAR>": OrsoTypes.ARRAY,
            "DECIMAL": OrsoTypes.DECIMAL,
            "INTERVAL": OrsoTypes.INTERVAL,
            "JSONB": OrsoTypes.JSONB,
            "STRUCT": OrsoTypes.STRUCT,
            "VARIANT": OrsoTypes.NULL,  # dynamic — type resolved at runtime
        }

        catalog = cls.__new__(cls)
        catalog._functions = {}
        catalog._aliases = {}
        # Load builtins first (already-registered managed functions take precedence)
        catalog._load_builtin_functions()

        for name, entry in functions_dict.items():
            if not isinstance(entry, tuple) or len(entry) != 3:
                continue  # skip malformed entries

            callable_ref, return_type_str, cost = entry
            orso_type = _TYPE_MAP.get(return_type_str, OrsoTypes.NULL)

            if name in catalog._functions:
                if skip_existing:
                    continue
                raise ValueError(f"Function '{name}' already registered.")

            func_def = FunctionDefinition(
                name=name,
                aliases=(),
                category="legacy",
                volatility="immutable",
                deterministic=True,
                lifecycle=LifecycleSpec(status="active"),
                summary="",
                documentation="",
                overloads=(
                    FunctionOverload(
                        id=f"{name}_variadic",
                        parameters=(
                            ParameterSpec(
                                name="args", type_family="any", variadic=True, optional=True
                            ),
                        ),
                        return_spec=ReturnSpec(mode="fixed", fixed_type=orso_type),
                        kernel=KernelSpec(
                            id="default",
                            callable_ref=callable_ref,
                            cost_us_per_million=float(cost),
                        ),
                    ),
                ),
            )
            catalog._functions[name] = func_def

        return catalog

    def load_legacy_dict(self, functions_dict: Dict[str, tuple]) -> None:
        """Register functions from legacy FUNCTIONS dict into this catalog instance.

        Skips any functions already registered (by name). Use this to backfill
        remaining legacy functions into an existing catalog.

        Args:
            functions_dict: The legacy FUNCTIONS dict from opteryx.functions.
        """
        from orso.types import OrsoTypes

        _TYPE_MAP: Dict[str, OrsoTypes] = {
            "VARCHAR": OrsoTypes.VARCHAR,
            "INTEGER": OrsoTypes.INTEGER,
            "DOUBLE": OrsoTypes.DOUBLE,
            "BOOLEAN": OrsoTypes.BOOLEAN,
            "DATE": OrsoTypes.DATE,
            "TIMESTAMP": OrsoTypes.TIMESTAMP,
            "TIME": OrsoTypes.TIME,
            "BLOB": OrsoTypes.BLOB,
            "ARRAY": OrsoTypes.ARRAY,
            "ARRAY<VARCHAR>": OrsoTypes.ARRAY,
            "DECIMAL": OrsoTypes.DECIMAL,
            "INTERVAL": OrsoTypes.INTERVAL,
            "JSONB": OrsoTypes.JSONB,
            "STRUCT": OrsoTypes.STRUCT,
            "VARIANT": OrsoTypes.NULL,
        }

        for name, entry in functions_dict.items():
            if not isinstance(entry, tuple) or len(entry) != 3:
                continue
            if name in self._functions:
                continue  # already registered with full metadata

            callable_ref, return_type_str, cost = entry
            orso_type = _TYPE_MAP.get(return_type_str, OrsoTypes.NULL)

            func_def = FunctionDefinition(
                name=name,
                aliases=(),
                category="legacy",
                volatility="immutable",
                deterministic=True,
                lifecycle=LifecycleSpec(status="active"),
                summary="",
                documentation="",
                overloads=(
                    FunctionOverload(
                        id=f"{name}_variadic",
                        parameters=(
                            ParameterSpec(
                                name="args", type_family="any", variadic=True, optional=True
                            ),
                        ),
                        return_spec=ReturnSpec(mode="fixed", fixed_type=orso_type),
                        kernel=KernelSpec(
                            id="default",
                            callable_ref=callable_ref,
                            cost_us_per_million=float(cost),
                        ),
                    ),
                ),
            )
            self._functions[name] = func_def


# Global catalog singleton
_CATALOG: Optional[FunctionCatalog] = None


def get_catalog() -> FunctionCatalog:
    """Get or create the global function catalog.

    On first call, initialises the catalog with builtins from native_function_registrar,
    then backfills all remaining functions from the legacy FUNCTIONS dict so all 105
    scalar functions are resolvable before Phase 2 (binder adoption) is complete.

    Returns:
        The global FunctionCatalog instance.
    """
    global _CATALOG
    if _CATALOG is None:
        _CATALOG = FunctionCatalog()
        # Backfill any functions not yet manually registered in the registrar
        from opteryx.functions import FUNCTIONS as _LEGACY_FUNCTIONS

        _CATALOG.load_legacy_dict(_LEGACY_FUNCTIONS)
    return _CATALOG
