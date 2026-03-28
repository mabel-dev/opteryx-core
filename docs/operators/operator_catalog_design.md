# Operator Catalog and Dispatcher Design (Revised)

## 1. Overview

This document describes the operator catalog system that provides centralized registration, metadata management, and execution path tracking for all Opteryx relational operators. It has been revised to clarify the distinction between **catalog‑level operator class metadata** and **concrete physical operator instances** built by the planner.

- **Catalog (abstract classes)** – stores static metadata about each operator type (e.g., category, parallelism hints, telemetry flags). Registration occurs once per operator class.
- **Physical Plan Instances (concrete)** – the planner instantiates operators with query‑specific parameters (e.g., filter expressions, join conditions). These instances hold runtime state and are executed by the dispatcher/executor.

The catalog does **not** store instance parameters or per‑query state; it serves only as a central source of class‑level information.

## 2. Purpose

- **Centralize operator metadata** – All operator classes register themselves once, providing a single source of truth for their properties.
- **Track execution paths** – Support visualization and debugging by mapping operator relationships (derived from the logical plan, not stored in the catalog).
- **Extract telemetry** – Provide two levels of telemetry (verbose and user) with configurable sinks.
- **Generate visualizations** – Create Mermaid diagrams of execution plans from logical plans.
- **Prepare for parallelism** – Store queue depths and parallel execution hints for future use by the dispatcher.

## 3. Core Files

### 3.1 `opteryx/operators/catalog.py`
- `OperatorMetadata` dataclass – stores static class‑level properties.
- `OperatorRegistry` class – central registry with thread‑safe registration and retrieval.
- `OperatorCategory` enum – classification for visualization (SCAN, JOIN, FILTER, AGGREGATE, IO, etc.).
- Global registry instance accessible via `get_registry()` (thread‑safe).

### 3.2 `opteryx/operators/base_plan_node.py`
Updated base class that:
- Registers its subclasses **once per class** using `__init_subclass__`.
- Stores class‑level metadata (category, data container, parallelism hints, telemetry flags).
- Assigns a unique instance ID (UUID) when a concrete operator is instantiated (without re‑registering the class).
- Provides a hook to associate the instance ID with the registry for tracing (optional).

### 3.3 `opteryx/operators/telemetry.py`
- `TelemetryEvent` dataclass – represents a single telemetry point.
- `TelemetryBuffer` – collects events with level‑based filtering and configurable flush policies.
- `TelemetrySink` – abstract base class; concrete implementations for logging, Prometheus, etc.
- Global sink registry and per‑query configuration.

### 3.4 `opteryx/operators/visualization.py`
- Functions to generate Mermaid diagrams from a logical plan (not from stored maps).
- Styling based on operator category.
- Support for filtering by category or operator type.

### 3.5 `opteryx/operators/__init__.py`
Re‑exports catalog classes and functions.

## 4. Operator Metadata Schema

Every operator **class** registers the following properties:

| Property | Type | Description |
|----------|------|-------------|
| `friendly_name` | str | Human‑readable operator name (defaults to class name). |
| `category` | OperatorCategory | **Explicit classification** (SCAN, JOIN, etc.). |
| `data_container` | str | "arrow", "draken", or "both" – may be extended later. |
| `parallel_strategy` | str | "single_thread", "multi_thread", "async". |
| `batch_size` | int | Typical batch size (0 = auto‑detect by the planner). |
| `is_pipeline_breaking` | bool | If true, forces a pipeline flush before execution. |
| `is_join` | bool | Convenience flag. |
| `is_scan` | bool | Convenience flag. |
| `is_stateless` | bool | Operator can be re‑executed without preserving state. |
| `target_queue_depth` | int | Desired parallel queue depth (used by future dispatcher). |
| `max_queue_depth` | int | Maximum queue depth. |
| `verbose_telemetry` | bool | Include in verbose telemetry. |
| `user_telemetry` | bool | Include in user telemetry. |

All flags are set as class attributes in the operator definition. The catalog stores only this class‑level metadata.

## 5. Registration Pattern

### 5.1 Class‑Level Registration
Operators inherit from `BasePlanNode` and set class attributes. Registration happens at class definition via `__init_subclass__`:

```python
class MyFilterNode(BasePlanNode):
    category = OperatorCategory.FILTER
    data_container = "arrow"
    parallel_strategy = "single_thread"
    batch_size = 10000
    is_pipeline_breaking = False
    is_stateless = True
    verbose_telemetry = True
    user_telemetry = True
    # ... other flags as needed
```

The base class’s `__init_subclass__` collects these attributes and registers the **class** in the catalog. No per‑instance registration occurs automatically.

### 5.2 Instance Instantiation
When the planner builds a physical plan, it instantiates operator classes with query‑specific parameters (e.g., filter expressions, join conditions). Each instance receives a unique `instance_id` (UUID) for telemetry, but the catalog is **not** updated with this instance (unless optional instance tracking is enabled for debugging). The instance parameters are stored locally in the plan node, not in the catalog.

```python
# Planner creates concrete instance
filter_node = MyFilterNode(
    expression=parse_expression("age > 18"),
    instance_id=uuid4(),
    # ... other runtime parameters
)
```

## 6. Catalog vs. Physical Plan Instances – Clarification

- **Catalog** holds **static metadata** about operator *classes*. It does not know about specific filter expressions, join keys, or other runtime parameters.
- **Physical Plan Nodes** are **concrete instances** built from these classes. They carry all information needed for execution (e.g., `expression` for a filter, `left_child` and `right_child` for a join).
- The **dispatcher** (future) will work with physical plan instances, using catalog metadata to determine parallelism settings, queue depths, etc.

This separation ensures that the catalog remains lightweight, thread‑safe, and does not accumulate per‑query state.

## 7. Telemetry System

### 7.1 Two‑Level Telemetry

- **Verbose**: Full details (per‑operator execution, timing, rows, bytes, internal events).
- **User**: Summary metrics only (execution time, rows/bytes in/out, batch size, category).

### 7.2 Telemetry Event Structure

```python
@dataclass
class TelemetryEvent:
    operator_name: str        # class name
    operator_id: str          # instance UUID
    event_type: str
    timestamp: float
    duration_ns: float
    rows_in: int
    rows_out: int
    bytes_in: int
    bytes_out: int
    tags: Dict[str, str]
```

### 7.3 Telemetry Sink Interface

```python
class TelemetrySink(ABC):
    @abstractmethod
    def emit(self, event: TelemetryEvent) -> None: ...
    @abstractmethod
    def flush(self) -> None: ...
```

Concrete sinks (e.g., `LoggingSink`, `PrometheusSink`) are registered globally or per query. The buffer periodically flushes events to the configured sink(s).

### 7.4 Configuration

Telemetry can be controlled via query properties:
- `telemetry_level` (NONE, USER, VERBOSE)
- `telemetry_sinks` (list of sink instances)

If no sinks are configured, telemetry events are dropped.

## 8. Mermaid Visualization

### 8.1 Generating from Logical Plan

The `visualization.py` module provides a function `to_mermaid(plan, ...)` that walks the logical plan and produces a Mermaid diagram. It does **not** rely on stored parent/child maps in the registry; instead, it traverses the plan structure.

### 8.2 Node Styling

| Category | Color | Style |
|----------|-------|-------|
| SCAN | Blue | `fill:#e1f5fe` |
| JOIN | Orange | `fill:#fff3e0` |
| AGGREGATE | Purple | `fill:#f3e5f5` |
| IO | Green | `fill:#e8f5e9` |
| FILTER | Gray | `fill:#f5f5f5` |
| (others) | Light gray | `fill:#fafafa` |

### 8.3 Support for DAGs

The function handles operators with multiple inputs (e.g., joins) and multiple outputs, correctly representing the graph structure.

## 9. Execution Flow

### 9.1 Query Execution with Catalog

```python
from opteryx.models import QueryProperties
from opteryx.operators.catalog import get_registry

registry = get_registry()

# 1. Parse query and build logical plan
logical_plan = planner.build_plan(sql)

# 2. Convert logical plan to physical plan, instantiating operator classes
physical_plan = physical_planner.create_physical_plan(logical_plan)
# Each node in physical_plan is a concrete operator instance with its own parameters

# 3. (Optional) Enable instance tracking for debugging (weak references)
registry.enable_instance_tracking()

# 4. Execute plan (telemetry events are sent to configured sinks)
result = executor.execute(physical_plan)

# 5. Clean up instance tracking if used
registry.disable_instance_tracking()
```

### 9.2 Telemetry Recording in Operators

Inside an operator’s `execute` method, it can record events:

```python
def execute(self):
    start = time.perf_counter_ns()
    rows_in, bytes_in = self.get_input_stats()
    result = self._do_work()
    rows_out, bytes_out = self.get_output_stats()
    duration = time.perf_counter_ns() - start
    telemetry.record(
        operator_name=self.__class__.__name__,
        operator_id=self.instance_id,
        event_type="execute",
        duration_ns=duration,
        rows_in=rows_in,
        rows_out=rows_out,
        bytes_in=bytes_in,
        bytes_out=bytes_out,
        tags={"category": self.category.value},
    )
```

### 9.3 Queue Depths and Parallelism (Future)

The catalog stores `target_queue_depth` and `max_queue_depth` per operator class. When the dispatcher is implemented, it will read these values from the registry and use them to size queues for parallel execution. The physical plan instances will be distributed across queues according to these settings.

## 10. Implementation Notes

### 10.1 Class‑Level Registration
- `BasePlanNode.__init_subclass__` registers the subclass once, using its class attributes.
- Registration uses a thread‑safe lock to prevent race conditions in the global registry.
- The registry stores only class metadata; instances are not stored unless explicitly tracked (and then only with weak references).

### 10.2 Instance Identity and Cleanup
- Each operator instance gets a unique UUID (instance_id) for telemetry.
- If instance tracking is enabled, the registry holds a weak reference to the instance (or a set of instance IDs) for debugging. The query must disable tracking or call `unregister_instance()` when finished to avoid memory leaks.

### 10.3 Thread Safety
- All registry methods that modify internal dictionaries use a reentrant lock.
- Telemetry sinks must be thread‑safe; the buffer uses a lock to protect its event queue.

### 10.4 Performance Considerations
- Telemetry event creation is kept lightweight; row/byte counts are already tracked by operators.
- The buffer is sized and flushes on a separate thread to avoid blocking execution.
- Mermaid generation is lazy and only called when requested.

## 11. Future Work

- [ ] Parallel execution with dispatcher reading queue depths from catalog.
- [ ] Distributed tracing integration (e.g., OpenTelemetry).
- [ ] Prometheus metrics export via a dedicated sink.
- [ ] Grafana dashboard integration.
- [ ] Operator profiling and optimization suggestions using telemetry data.

## 12. Files Created / Modified

**Created**:
1. `opteryx/operators/catalog.py`
2. `opteryx/operators/telemetry.py`
3. `opteryx/operators/visualization.py`
4. `opteryx/operators/__init__.py`

**Modified**:
1. `opteryx/operators/base_plan_node.py` – added `__init_subclass__`, instance UUID generation.
2. `opteryx/planner/logical_planner/logical_planner_builders.py` – updated to use catalog.

## 13. Testing

Test cases should verify:

- [x] Class-level registration occurs exactly once per operator class.
- [x] Category is explicitly set (no inference) and correctly stored.
- [x] Telemetry events are recorded at appropriate levels and delivered to configured sinks.
- [x] Mermaid diagram generation works for plans with multiple inputs/outputs.
- [x] Registry methods are thread-safe under concurrent access.
- [x] Queue depths are correctly propagated to future dispatcher.
- [x] Physical plan instantiation does not trigger unnecessary catalog updates.

## 14. Status

- **Design**: **Finalized** based on open question resolution.
- **Implementation**: In progress (Week 1–2).
- **Integration**: Pending (Week 3–4).
- **Testing**: Pending (Week 5).

## 15. Resolution Notes

All open questions from the LLM review have been resolved:

1. **Queue Depth**: Store in catalog with default 0; dispatcher team will derive from CPU/system config. V1: use depth of 5.
2. **Data Container**: Keep as strings ("arrow", "draken", "both"). Contract documented. Reduce to 1 in short-term.
3. **Pipeline Breaking**: Marks ORDER BY / GROUP BY operators that require materialization before moving to next stage.
4. **Telemetry Sinks**: Only query telemetry exists. No global telemetry. If no sinks configured, events are dropped.
5. **Instance Tracking**: Not needed after Q4 resolution. Rely on telemetry events with instance_id.
6. **Error Handling**: Synchronous telemetry fails the query. Async telemetry logs failure and continues.
7. **Parent/Child**: Catalog provides child lookup for dispatcher to build execution graph.
8. **Backpressure**: Telemetry off by default (drop policy). Turn on for analysis only.
9. **Batch Size**: Default = 2048. If <= 0, error out. Planner overrides based on data source.
10. **Thread Safety**: Multithreaded system. Take locks late, release early. Don't block unrelated actions.

---

**Author**: System Engineer
**Date**: 2024 (finalized)
**Review**: Resolved - No outstanding issues.
