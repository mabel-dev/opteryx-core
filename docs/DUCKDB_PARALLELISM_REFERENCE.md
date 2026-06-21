# DuckDB Parallel Execution — Reference Design Document

**Status:** Reference / external study
**Audience:** Opteryx execution-engine contributors (M4 central scheduler, GIL-off carrier work)
**Source basis:** Reconstructed directly from DuckDB `main` source, not from memory. Files read:
`src/parallel/executor.cpp`, `pipeline.cpp`, `pipeline_executor.cpp`, `meta_pipeline.cpp`, `event.cpp`,
`task_scheduler.cpp`; `src/storage/table/row_group_collection.cpp`;
`src/execution/operator/aggregate/physical_hash_aggregate.cpp`,
`src/execution/operator/join/physical_hash_join.cpp`,
`src/execution/radix_partitioned_hashtable.cpp`; and headers
`pipeline_executor.hpp`, `interrupt.hpp`, `task_scheduler.hpp`,
`common/enums/operator_result_type.hpp`.

Symbol and method names in `code font` are quoted from the source. Statements that generalize
beyond a directly-quoted line are marked *(inferred)*.

---

## Table of Contents

1. Design Philosophy
2. Core Vocabulary
3. Plan Compilation: Pipelines and Pipeline Breakers
4. MetaPipelines and Pipeline Dependencies
5. Event-Driven Scheduling (the DAG)
6. The Event Base Class Mechanics
7. The Task Scheduler and Thread Pool
8. Choosing a Pipeline's Degree of Parallelism
9. The Per-Task Push Loop (PipelineExecutor)
10. The Result-Type State Machine
11. Blocking, Interrupts, and Backpressure
12. Local vs Global State — the Lock-Free Substrate
13. Source Parallelism: Morsel Handout
14. Sink Parallelism Deep Dive: Hash Aggregation
15. Sink Parallelism Deep Dive: Hash Join
16. Memory-Awareness and Spilling
17. End-to-End Execution Trace
18. Design Invariants (the contract every operator obeys)
19. Implications for Opteryx

---

## 1. Design Philosophy

DuckDB implements **morsel-driven parallelism** (Leis et al., *"Morsel-Driven Parallelism: A
NUMA-Aware Query Evaluation Framework for the Many-Core Age"*, SIGMOD 2014). Four commitments fall
out of that choice and shape the entire engine:

1. **Parallelism is a property of a *pipeline*, not of an operator.** There is no per-operator thread
   pool, and no operator spawns its own threads. A pipeline is executed by *N identical tasks*; each
   task pulls small units of input ("morsels") from the pipeline's source and pushes each one through
   the *entire* operator chain to the sink. Parallelism is achieved by many threads racing through the
   *same* operator chain on *disjoint* morsels.

2. **Operators are lock-free on the hot path via state duplication.** Every operator exposes *global*
   state (shared, created once per query) and *thread-local* state (one per task). The hot path
   mutates only thread-local state. Threads coordinate at exactly two points: the short critical
   section that hands out the next morsel, and the merge of local into global state (`Combine`).

3. **Tasks are cooperative and re-entrant.** A task runs under an `ExecutionBudget`, processes a
   bounded amount of work, then yields control. Any operator phase can return `BLOCKED` to park a task
   (async I/O, memory wait) and have it resumed later. Execution is therefore a *state machine*, not a
   blocking call stack — a worker thread never spins or sleeps inside an operator.

4. **Parallelism degree is data-bounded.** The number of tasks is the minimum of what the data
   supports (how many morsels the source can produce), what every operator in the chain allows, and
   what the scheduler has threads for. Tiny inputs do not oversubscribe threads, so there is no
   fixed-floor heuristic to tune.

---

## 2. Core Vocabulary

| Abstraction | File | Role |
|---|---|---|
| `PhysicalOperator` | `src/execution/operator/...` | A node in the physical plan. Declares whether it acts as source / streaming-operator / sink, and whether each role is parallel-safe (`ParallelSource`, `ParallelOperator`, `ParallelSink`). |
| `Pipeline` | `parallel/pipeline.cpp` | A linear chain: one source → 0..N streaming operators → one sink. The unit of scheduling and the unit of parallelism. |
| `MetaPipeline` | `parallel/meta_pipeline.cpp` | A group of pipelines sharing a sink, plus the dependency structure among them (e.g. join build-before-probe). |
| `Event` | `parallel/event.cpp` | A node in the scheduling DAG. Fires when its dependencies complete; owns a batch of tasks. |
| `PipelineTask` | `parallel/pipeline.cpp` | One unit of work handed to a worker thread; one per thread for a parallel pipeline. |
| `PipelineExecutor` | `parallel/pipeline_executor.cpp` | The per-task push loop: fetch morsel → push through operators → sink. |
| `TaskScheduler` | `parallel/task_scheduler.cpp` | The worker thread pool and the producer/consumer task queues. |
| `Executor` | `parallel/executor.cpp` | Per-query orchestrator: builds MetaPipelines, lowers to events, collects errors and results, manages blocked tasks. |
| `InterruptState` | `parallel/interrupt.hpp` | The async wake-up token carried by a blockable task. |

---

## 3. Plan Compilation: Pipelines and Pipeline Breakers

### 3.1 Where pipelines break

A pipeline is a chain that can stream a single chunk end-to-end without a global barrier. A
**pipeline breaker** is any operator that must *fully consume* its input before it can produce any
output. These are precisely the **sinks**: hash-join *build*, hash *aggregate*, *sort* / order-by,
CTE / result *materialization*. Each breaker terminates one pipeline (it is that pipeline's sink) and
begins another (it becomes the *source* of a downstream pipeline). *(inferred from the
source/operator/sink role model and the join/aggregate operators read.)*

### 3.2 Worked decomposition

`SELECT k, COUNT(*) FROM t JOIN d ON t.id = d.id GROUP BY k` decomposes into:

```
Pipeline 1 (build):   Scan(d)  ───────────────────────────► HashJoin.Sink   (build side)
Pipeline 2 (probe):   Scan(t)  ─► HashJoin(operator) ──────► HashAggregate.Sink
Pipeline 3 (output):  HashAggregate(source) ─► Projection ─► Result
```

Ordering constraints:
- Pipeline 1 must complete before Pipeline 2 (the hash table must exist before probing).
- Pipeline 2 must complete before Pipeline 3 (all groups must be aggregated before they can be read).

These orderings are **not** locks. They are **event dependencies** (§5).

---

## 4. MetaPipelines and Pipeline Dependencies

`MetaPipeline::Build()` calls `op.BuildPipelines(*pipelines.back(), *this)`, delegating wiring to each
physical operator. A MetaPipeline holds a reference to a single **sink** and creates pipelines via
`CreatePipeline()`. Two mechanisms encode ordering:

- **`CreateChildMetaPipeline()`** — establishes a parent/child relationship in which (per the code)
  the *"child MetaPipeline must finish completely before this MetaPipeline can start."* This is how a
  hash join's build side (child) is forced to finish before the probe side (parent) begins.

- **Fine-grained pipeline dependencies** — the class tracks a `pipeline_dependencies` map:
  - `AddDependenciesFrom()` "collects pipelines created after a starting point and adds them as
    dependencies for a dependent pipeline."
  - `CreateChildPipeline()` "establishes dependencies where the child pipeline depends on all
    pipelines scheduled between itself and the current pipeline."
  - `AddRecursiveDependencies()` "conditionally adds dependencies across child meta-pipelines based on
    thread count estimation."

The output of the build phase is a DAG of pipelines with explicit ordering edges, which the executor
then lowers into an event DAG.

---

## 5. Event-Driven Scheduling (the DAG)

`src/parallel/executor.cpp`. The executor does **not** schedule pipelines by directly spawning
threads in dependency order. It lowers the pipeline DAG into an **event DAG** and lets event
completion drive everything.

### 5.1 The per-pipeline event chain

`ScheduleEventsInternal()` builds, for each pipeline, a chain of events whose dependency order is
quoted verbatim from the code:

```
initialize → event → prepare finish → finish → complete
```

| Event | Purpose |
|---|---|
| `PipelineInitializeEvent` | One-time global init of the pipeline — allocate global source/sink state. |
| `PipelineEvent` | The main parallel execution phase — schedules the `PipelineTask`s. |
| `PipelinePrepareFinishEvent` | Pre-finalize coordination. Exists so sibling sinks can let the `TemporaryMemoryManager` "make an informed decision" about memory before any one of them finalizes. |
| `PipelineFinishEvent` | Runs `Finalize()` — the single global, post-`Combine` wrap-up (e.g. build the join pointer table; partition the aggregate). |
| `PipelineCompleteEvent` | Marks the pipeline done and releases dependents. |

Inter-pipeline edges (§4) become inter-event edges, so "build before probe" reduces to "the probe
pipeline's initialize event depends on the build pipeline's complete event." *(inferred from the
dependency-lowering description.)*

### 5.2 Scheduling entry points

- `Initialize()` — builds the physical plan into MetaPipelines and schedules the initial events.
- `ScheduleEventsInternal()` — creates the event dependency chains and schedules every pipeline that
  has no outstanding dependencies.
- `VerifyScheduledEvents()` — in debug builds, runs a depth-first search over the scheduled events to
  detect cyclic dependencies (a malformed `BuildPipelines` would otherwise deadlock).

### 5.3 Per-query task lifecycle

- `ExecuteTask()` processes a task partially. On a blocking result it parks the task on
  `to_be_rescheduled_tasks`.
- `RescheduleTask()` restores a previously-blocked task once its wake-up fires.
- Errors raised inside a task are routed back to the executor (`PushError`-style propagation) and
  surface as the query result. *(inferred.)*

---

## 6. The Event Base Class Mechanics

`src/parallel/event.cpp`. An `Event` is simultaneously a node in a dependency graph *and* a container
for a batch of tasks. Two independent counters gate it.

### 6.1 Dependency counters (upstream gating)

- `AddDependency(Event &event)` increments `total_dependencies` on this event and adds this event to
  the dependency's `parents` (a weak-pointer list). This builds the DAG.
- When a dependency finishes, it calls `CompleteDependency()` on each parent, which increments
  `finished_dependencies`. The guard is:
  ```cpp
  if (current_finished == total_dependencies) { Schedule(); }
  ```
  i.e. an event only **schedules its tasks** once *all* its dependencies have finished.

### 6.2 Task counters (downstream gating)

- `SetTasks()` records `total_tasks` and submits them to the scheduler.
- As each task completes, `FinishTask()` increments `finished_tasks`. When
  `current_finished == current_tasks`, it calls `Finish()`.
- `Finish()` runs the event-specific `FinishEvent()` hook, then calls `CompleteDependency()` on every
  parent — cascading completion downstream.

### 6.3 Dynamic event insertion

`InsertEvent()` splices a *new* event into the chain at runtime: it transfers this event's `parents`
to the replacement, makes itself a dependency of the replacement, and registers the replacement with
the executor. This is how operators schedule *additional* parallel sub-phases discovered only at
finalize time — e.g. the hash aggregate's distinct-finalize event, or the hash join's
table-init → finalize event pair (§14, §15).

### 6.4 Why two counters

The double-gate (`dependencies` upstream, `tasks` downstream) is what lets the same primitive express
both "wait for the build pipeline" and "wait for all 8 build tasks" without special cases. An event
is *runnable* when dependencies hit zero, and *complete* when tasks hit total.

---

## 7. The Task Scheduler and Thread Pool

`src/parallel/task_scheduler.cpp` + `task_scheduler.hpp`.

### 7.1 Thread configuration

- `SetThreads(idx_t total_threads, idx_t external_threads)` launches `total_threads - external_threads`
  background worker threads. **External threads** (e.g. the client's own thread) also participate in
  execution rather than blocking idle.
- `SetAsyncThreads(idx_t n)` configures a separate pool of async/IO worker threads.
- `RelaunchThreads()` restarts the pools after a configuration change.
- `NumberOfThreads()` / `NumberOfAsyncThreads()` report the current sizes.

### 7.2 NUMA / CPU awareness

`GetEstimatedCPUId()` returns the CPU number the calling thread is currently running on (falling back
to the thread id when the OS does not expose it). This is the hook used to keep work and memory
NUMA-local in the morsel-driven design.

### 7.3 Queues and pools

- The queues are `array<unique_ptr<TaskSchedulerQueue>, TASK_SCHEDULER_TYPE_COUNT>` — one per
  `TaskSchedulerType` (a **regular** compute pool and an **async/IO** pool).
- Each queue is a producer/consumer structure with a blocking semaphore. The default poll timeout is
  `TASK_TIMEOUT_USECS = 5000` (5 ms) — a worker that finds nothing parks for at most 5 ms before
  re-checking, bounding wake-up latency without a busy spin.

### 7.4 Submission via producer tokens

- `CreateProducer()` returns a `ProducerToken` (holding a `producer_lock` and a `QueueProducerToken`
  per pool). Each pipeline event owns a producer token *(inferred)*, which keeps that event's tasks
  grouped within the queue.
- `ScheduleTask(ProducerToken &, shared_ptr<Task>)` and
  `ScheduleTasks(ProducerToken &, vector<shared_ptr<Task>> &)` enqueue work; pool-specific overloads
  take a `TaskSchedulerType`.

### 7.5 Worker loop and work stealing

- Each worker runs `ExecuteForever()`, looping on `TryDequeueAndProcessTask()`.
- `TryDequeueAndProcessTask()` runs a task and dispatches on its `TaskExecutionResult`:
  completed → drop; error → propagate; *incomplete* (budget exhausted, more work remains) → re-enqueue
  via `RescheduleTask()`; *blocked* → deschedule (the blocking operator owns the wake-up).
- **Work stealing:** regular-pool workers pull from *all* pools — "Regular thread pool picks up tasks
  from all pools." Async threads only drain their dedicated queue via `GetQueue(pool_type).Dequeue()`.
- **Signaling:** after enqueuing, `Signal()` / `SignalForTaskType()` wakes idle workers so latency
  stays low.

---

## 8. Choosing a Pipeline's Degree of Parallelism

`src/parallel/pipeline.cpp`. `Pipeline::Schedule()` calls `TryGetMaxThreads()`:

1. **Opt-in check.** Every operator in the chain must support its role in parallel —
   `ParallelSource()`, `ParallelOperator()`, `ParallelSink()`. If *any* operator declines, the entire
   pipeline runs single-threaded. (An order-sensitive operator forces serialization this way.)

2. **Per-operator hints — take the minimum:**
   ```cpp
   max_threads = MinValue<idx_t>(max_threads, op.op_state->MaxThreads(max_threads));
   ```
   The source's `MaxThreads` is usually dominant — effectively "how many morsels can I hand out
   concurrently."

3. **Scheduler cap.** Clamp to the active thread count from `TaskScheduler::GetScheduler()`.

4. **Launch.** If parallel, `LaunchScanTasks()` creates one task per thread:
   ```cpp
   for (idx_t i = 0; i < max_threads; i++) {
       tasks.push_back(make_uniq<PipelineTask>(*this, event));
   }
   ```
   Otherwise `ScheduleSequentialTask()` creates exactly one task.

**Consequence:** a pipeline whose source can only yield two morsels gets two tasks even on a 64-core
box — no oversubscription, and no recombination cost paid for trivially small inputs.

---

## 9. The Per-Task Push Loop (PipelineExecutor)

`src/parallel/pipeline_executor.cpp` + `pipeline_executor.hpp`. One `PipelineExecutor` drives one task.

### 9.1 State carried by the executor (from the header)

| Member | Meaning |
|---|---|
| `vector<unique_ptr<DataChunk>> intermediate_chunks` | Scratch buffers between operators. |
| `vector<unique_ptr<OperatorState>> intermediate_states` | Per-operator execution state for *this task*. |
| `DataChunk final_chunk` | "The final chunk used for moving data into the sink." |
| `stack<idx_t> in_process_operators` | Operators "not yet finished executing and have data remaining." When empty, the loop fetches a fresh morsel. This stack lets an operator emit *more* output than it received (e.g. a join producing many matches per probe row) without losing its place. |
| `unique_ptr<LocalSourceState> local_source_state` | Thread-local source state. |
| `unique_ptr<LocalSinkState> local_sink_state` | Thread-local sink state (present if the pipeline has a sink). |
| `InterruptState interrupt_state` | Lets a sink/source block the task. |
| `bool exhausted_source` | The source has no more morsels. |
| `bool started_flushing`, `bool done_flushing` | Govern draining intermediate operators' caches after the source is exhausted. |
| `ExecutionBudget` | Bounds work per invocation via `Next()` / `IsDepleted()`. |

### 9.2 The loop

`PipelineExecutor::Execute()` runs while the `ExecutionBudget` is not depleted:

1. **`FetchFromSource()`** — calls the source operator's `GetData()` to obtain the next morsel; wrapped
   in `StartOperator()` / `EndOperator()` profiling. Returns a `SourceResultType`.
2. **`ExecutePushInternal()`** — pushes the chunk through the streaming operators in order. Each
   operator's `Execute()` transforms its input chunk into its output chunk; output feeds the next
   operator. Operators returning `HAVE_MORE_OUTPUT` are pushed onto `in_process_operators`.
3. **`Sink()`** — the terminal chunk goes to the sink, returning a `SinkResultType`. If `BLOCKED`,
   execution interrupts and resumes later with the saved `remaining_sink_chunk`.

The public method returns a `PipelineExecuteResult`: `FINISHED`, `NOT_FINISHED` (budget exhausted —
requeue me), or `INTERRUPTED` (blocked — park me).

### 9.3 The flush phase

When the source is exhausted, streaming operators that *cache* internal state must be drained before
the pipeline reports done. `started_flushing` / `done_flushing`, together with
`OperatorFinalizeResultType` (§10), govern this final drain. *(inferred from the flush flags + the
finalize result enum.)*

---

## 10. The Result-Type State Machine

`src/include/duckdb/common/enums/operator_result_type.hpp`. The cooperative scheduler is driven
entirely by these return enums — this is the contract every operator implements.

**`OperatorResultType`** (streaming operator `Execute`):
- `NEED_MORE_INPUT` — done with this chunk; give me the next.
- `HAVE_MORE_OUTPUT` — call me again with the *same* input; I have more to emit. (Tracked via
  `in_process_operators`.)
- `FINISHED` — I am done for the whole pipeline; stop pulling from the source.
- `BLOCKED` — I cannot proceed (e.g. async I/O); I have armed the interrupt state.

**`OperatorFinalizeResultType`** (cache drain): `HAVE_MORE_OUTPUT` / `FINISHED`.

**`OperatorFinalResultType`**: `FINISHED` / `BLOCKED`.

**`SourceResultType`** (`GetData`): `HAVE_MORE_OUTPUT` / `FINISHED` / `BLOCKED`.

**`SinkResultType`** (`Sink`): `NEED_MORE_INPUT` / `FINISHED` (further input cannot change the result —
enables early-out, e.g. `LIMIT`) / `BLOCKED`.

**`SinkCombineResultType`** (`Combine`): `FINISHED` / `BLOCKED`.

**`SinkFinalizeType`** (`Finalize`): `READY` / `NO_OUTPUT_POSSIBLE` (the sink will never produce output
— downstream pipelines can be skipped entirely) / `BLOCKED`.

**`SinkNextBatchType`** (batched execution): `READY` / `BLOCKED`.

**`PipelineExecuteResult`**: `FINISHED` / `NOT_FINISHED` / `INTERRUPTED`.

The `BLOCKED` value appearing in *every* phase is the backbone of async + backpressure: any phase can
park the task, and the blocking operator is responsible for arming the `InterruptState` to wake it.

---

## 11. Blocking, Interrupts, and Backpressure

`src/include/duckdb/parallel/interrupt.hpp`. The `BLOCKED` return plus `InterruptState` form a
cooperative async system so a worker thread never spins or sleeps inside an operator.

### 11.1 The three interrupt modes (`InterruptMode`)

1. **`NO_INTERRUPTS`** — "No blocking mode is specified; an error is thrown if the operator blocks."
   For code paths where blocking is known to be impossible.
2. **`TASK`** (the preferred mode) — "A weak pointer to a task is provided. On the callback, this task
   will be signalled." The async subsystem holds a *weak* reference to the blocked `Task` and signals
   it when the operation completes.
3. **`BLOCKING`** — "The caller has blocked awaiting some synchronization primitive." Used by code
   without task support; backed by a condition variable.

### 11.2 Wake-up flow

- `InterruptState::Callback()` resumes a blocked task. In `TASK` mode it signals the stored task;
  in `BLOCKING` mode `InterruptDoneSignalState::Signal()` sets a `done` flag and notifies waiters via a
  condition variable; a waiter in `Await()` sleeps until `done` becomes true.
- `StateWithBlockableTasks` stores `blocked_tasks` in a vector; `UnblockTasks()` iterates them, calls
  `Callback()` on each, and clears the list. This is the typical operator pattern: "I'm full / waiting
  on a sibling — park all callers; when state changes, unblock them all at once."

### 11.3 Lifecycle of a blocked task

```
operator phase returns BLOCKED  (arms InterruptState)
        │
PipelineExecutor::Execute → INTERRUPTED
        │
worker deschedules → Executor::to_be_rescheduled_tasks
        │
... async op completes / sibling unblocks → InterruptState::Callback()
        │
TaskScheduler reschedules task → Signal() wakes a worker
        │
PipelineExecutor resumes with saved chunk (remaining_sink_chunk)
```

The `ExecutionBudget` provides the *cooperative* half even when nothing blocks: a long-running task
yields after bounded work so no single task monopolizes a worker and starves others.

---

## 12. Local vs Global State — the Lock-Free Substrate

This is the single most important pattern; it recurs in every parallel operator.

```
                 ┌─────────────── GlobalSinkState (one, shared) ───────────────┐
                 │  touched ONLY at: morsel handout · Combine() · Finalize()    │
                 └──────────────▲──────────────▲──────────────▲────────────────┘
                                │ Combine()    │ Combine()    │ Combine()
                                │              │              │
                 ┌──────────────┴──┐  ┌────────┴────────┐  ┌──┴──────────────┐
   thread 0  →   │ LocalSinkState  │  │ LocalSinkState  │  │ LocalSinkState  │  ← thread N
                 │ (private HT)    │  │ (private HT)    │  │ (private HT)    │
                 └─────────────────┘  └─────────────────┘  └─────────────────┘
                       ▲ Sink()              ▲ Sink()             ▲ Sink()
                       │                      │                    │
                  morsels of input      morsels of input     morsels of input
```

The lifecycle methods:

- `GetGlobalSinkState()` / `GetGlobalSourceState()` — called **once** per query.
- `GetLocalSinkState()` / `GetLocalSourceState()` — called **once per task** (per thread). All
  hot-path mutation lands here, lock-free.
- `Sink(chunk, local_state)` — ingest a chunk into thread-local state.
- `Combine(local_state → global_state)` — at the task's end, merge local into global. The *only*
  mutating contact with shared state; guarded by a lock or implemented as a lock-free hand-off.
- `Finalize(global_state)` — a single global wrap-up after all `Combine`s, run inside
  `PipelineFinishEvent`.

The hash-aggregate local-state header even documents the rule: *"everything that lives in this class
should be read-only at execution time."* Immutable shared data (e.g. precomputed `filter_indexes`) may
be shared freely; anything mutable is per-thread.

---

## 13. Source Parallelism: Morsel Handout

`src/storage/table/row_group_collection.cpp`. A parallel table scan distributes work at **row-group
granularity**.

- `NextParallelScan` / `GetNextRowGroup`, under `lock_guard<mutex> l(state.lock)`, assigns the next
  row group to whichever thread asks (`state.AssignRowGroup`). The lock protects only the *cursor* —
  "which row group is next" — never the scanning itself.
- Within an assigned row group, the thread iterates in `STANDARD_VECTOR_SIZE` (2048-row) vectors:
  `max_row = row_start + current_row_group.count`.
- A debug mode (`verify_parallelism`) shrinks morsels to a single vector
  (`vector_index * STANDARD_VECTOR_SIZE < current_row_group.count`) to maximize interleaving and
  surface races.
- `ParallelCollectionScanState` holds `current_row_group` and advances via `GetNextRowGroup`.

**Granularity summary:** one row group per handout; 2048 rows per push iteration. The per-morsel
shared-state cost is a single short critical section incrementing a cursor.

---

## 14. Sink Parallelism Deep Dive: Hash Aggregation

`physical_hash_aggregate.cpp` + `radix_partitioned_hashtable.cpp`. This is the canonical *partitioned*
parallel aggregation, and the design directly relevant to Opteryx's grouped-aggregate work.

### 14.1 Sink phase (per thread, lock-free)

`GetLocalSinkState()` returns a `HashAggregateLocalSinkState` holding `grouping_states`, each wrapping
a `GroupedAggregateHashTable`. `Sink()` routes the chunk through `RadixPartitionedHashTable::Sink`,
which hashes the group keys and **radix-partitions** rows by the high bits of the hash into a fixed
number of partitions (controlled by `radix_bits`). When a local HT reaches capacity, `ht.Abandon()`
snapshots it into partitioned data and starts a fresh table — so a thread never pays a global resize
and never blocks on shared state.

### 14.2 Combine phase (lock-free pointer hand-off)

`RadixPartitionedHashTable::Combine` merges thread-local partitioned data into global
`uncombined_data` with a *move*, not a rehash:
```cpp
if (gstate.uncombined_data) { gstate.uncombined_data->Combine(*lstate.abandoned_data); }
else                        { gstate.uncombined_data = std::move(lstate.abandoned_data); }
```
Because rows are already partitioned by hash bits, *all rows for a given group live in the same
partition index across every thread*. Merging is therefore concatenation of same-partition fragments,
not a global hash merge — this is the structural reason the merge is cheap even at high cardinality.

### 14.3 Finalize phase (partitions become independent work units)

`Finalize()` converts the combined data into `AggregatePartition` objects, each marked
`READY_TO_FINALIZE`:
```cpp
gstate.partitions.emplace_back(make_uniq<AggregatePartition>(std::move(partition)));
```

### 14.4 Parallel scan-out (source phase)

Because each partition is self-contained (no group spans two partitions), partitions can be *finalized
and read out in parallel with no locks*. `HashAggregateGlobalSourceState::MaxThreads()` sets
concurrency by how many partitions fit in memory:
```cpp
const auto partitions_fit = MaxValue<idx_t>(usable_memory / sink.max_partition_size, 1);
```
`GetData()` calls `gstate.AssignTask()` to hand each thread a different partition to finalize-and-scan.
The output phase is thus parallel by construction — not just the ingest.

### 14.5 Distinct aggregates (two-phase)

Distinct values are collected into separate `radix_tables` inside `DistinctAggregateData`. Then
`FinalizeDistinct()` schedules a `HashAggregateDistinctFinalizeEvent` (inserted via `InsertEvent`,
§6.3) whose `HashAggregateDistinctFinalizeTask` sinks the de-duplicated values back into the main
aggregate tables — itself a parallel sub-pipeline.

### 14.6 Why this beats naive intra-operator parallelism

A naive design splits one aggregation across threads and then performs a *serial* merge whose cost
rivals the aggregation itself at high cardinality. DuckDB avoids that with three properties:
(a) every thread builds a *private* HT (lock-free ingest); (b) the merge is a *partition-aligned
pointer hand-off* (O(partitions), not O(groups)); (c) the *read-out is also parallel* across
partitions. Together these eliminate the serial-merge Amdahl bottleneck.

---

## 15. Sink Parallelism Deep Dive: Hash Join

`physical_hash_join.cpp`.

### 15.1 Build side (Pipeline 1, parallel sink)

Each thread's `HashJoinLocalSinkState` owns its own `JoinHashTable` and builds independent per-condition
key executors:
```cpp
for (auto &cond : op.conditions) { join_key_executor.AddExpression(cond.GetRHS()); }
```
`Combine()` does **not** merge tables eagerly. It transfers ownership of each local HT into the global
state and keeps a reference for the finalize step:
```cpp
gstate.owned_local_hash_tables.push_back(std::move(lstate.hash_table));
gstate.local_hash_tables.push_back(*gstate.owned_local_hash_tables.back());
```

### 15.2 Finalize (parallel, two inserted events)

`HashJoinGlobalSinkState::ScheduleFinalize()` runs two sequential events:
1. `HashJoinTableInitEvent` — parallel allocation / `memset` of the global pointer table, sliced across
   threads (`entry_idx += entries_per_task`).
2. `HashJoinFinalizeEvent` — partition-aware finalize:
   `for (auto &chunk_range : chunk_ranges) sink.hash_table->Finalize(...)`, each partition independent.

Both are spliced in with `InsertEvent` (§6.3), so the build pipeline's "finish" actually fans back out
into parallel sub-tasks rather than running serially.

### 15.3 Probe side (Pipeline 2)

The join is a *streaming operator* in the probe pipeline. `ExecuteInternal()` extracts probe keys and
probes the shared, now-immutable hash table:
```cpp
state.probe_executor.Execute(input, state.lhs_join_keys);
sink.hash_table->Probe(state.scan_structure, ...);
```
It asserts `D_ASSERT(sink.finalized)` — proof the build finalize already ran. Probe parallelism is
inherited from the probe pipeline's *source* (the scan of the left input): each probe task gets its own
morsels and probes the read-only table with no locks.

### 15.4 External (spilling) probe

When `sink.external` is true, probe-side data was spilled. `HashJoinGlobalSourceState::AssignTask()`
hands each thread independent spilled chunks:
```cpp
if (sink.probe_spill->consumer->AssignChunk(lstate.probe_local_scan)) {
    lstate.local_stage = global_stage; return true;
}
```
`HashJoinLocalSourceState::ExternalProbe()` rescans and probes them
(`probe_spill->consumer->ScanChunk(...)` → `hash_table->Probe(...)`). Work stays morsel-shaped.

### 15.5 Build-before-probe ordering

This is *not* a runtime lock. The probe pipeline depends on the build pipeline's completion
(`CreateChildMetaPipeline`, §4), and the `OperatorSinkFinalizeInput` barrier guarantees all `Combine()`s
precede any probe. `sink.scanned_data = true` / `D_ASSERT(sink.finalized)` are the runtime assertions
that the ordering held.

---

## 16. Memory-Awareness and Spilling

Parallelism and memory management are coupled, not independent:

- `PipelinePrepareFinishEvent` exists so sibling sinks (e.g. several joins building concurrently) can
  coordinate via the `TemporaryMemoryManager` *before* any of them finalizes — "to make an informed
  decision" about how much memory each may claim.
- Under pressure, operators change their *parallel structure*, not just their buffer sizes. Hash
  aggregation calls `MaybeRepartition()`: if `total_size > gstate.GetThreadLimit()` it raises the
  radix-bit count via `config.SetRadixBitsToExternal()`, producing more, smaller partitions that can
  be processed (and spilled) one at a time — enabling external aggregation while keeping partitions
  independent.
- Hash join spills probe data and switches to the external-probe source path (§15.4).
- The read-out parallelism degree itself is a function of memory: `usable_memory / max_partition_size`
  (§14.4).

---

## 17. End-to-End Execution Trace

`SELECT k, COUNT(*) FROM t JOIN d ON t.id = d.id GROUP BY k` on an 8-thread server:

```
Executor::Initialize
  ├─ Build MetaPipelines  → 3 pipelines + dependency edges
  └─ ScheduleEventsInternal → event DAG ; VerifyScheduledEvents (debug)

EVENT P1.initialize → P1.event           [build d into hash table]
  Schedule 8 × PipelineTask
    each task: FetchFromSource(scan d morsel) → HashJoin.Sink(local JoinHashTable)
    each task end: Combine → move local HT into gstate.owned_local_hash_tables
  P1.finish: InsertEvent(HashJoinTableInitEvent[8-way memset])
                       → InsertEvent(HashJoinFinalizeEvent[8-way])
  P1.complete ───────────────────────────────────────► releases P2

EVENT P2.initialize → P2.event           [probe t, aggregate]
  Schedule 8 × PipelineTask
    each task: FetchFromSource(scan t morsel)
               → HashJoin.Execute (probe the read-only hash table)
               → HashAggregate.Sink (local radix-partitioned HT; Abandon() when full)
    each task end: Combine → partition-aligned pointer hand-off of partitioned data
  P2.finish: Finalize → build AggregatePartition[] (READY_TO_FINALIZE)
  P2.complete ───────────────────────────────────────► releases P3

EVENT P3.event                           [read out the aggregate]
  MaxThreads = usable_memory / max_partition_size
  Schedule min(8, #partitions) × PipelineTask
    each task: AssignTask → finalize + scan ONE partition → emit result chunks → Result
```

Total synchronization across the whole query: per-morsel cursor locks (short), three `Combine` merges
(pointer moves), and two `Finalize` barriers expressed as events. No operator holds a lock across
actual computation.

---

## 18. Design Invariants (the contract every operator obeys)

1. **Uniform role declaration.** An operator declares source / operator / sink roles and a
   `Parallel*()` flag per role. A single non-parallel role serializes the pipeline.
2. **Global state once, local state per task.** `GetGlobal*State` is called once; `GetLocal*State`
   once per thread. Hot-path mutation touches local state only.
3. **Two synchronization points only.** Morsel handout (short cursor lock) and `Combine` (merge).
   `Finalize` is a single-threaded-or-fanned-out global step gated by an event.
4. **Return a result type, never block the thread.** Every phase returns one of the result enums;
   `BLOCKED` + `InterruptState` replaces blocking calls.
5. **Bounded work per call.** Respect the `ExecutionBudget`; yield and let the task be requeued.
6. **Ordering via events, not locks.** Cross-pipeline ordering (build-before-probe,
   aggregate-before-read) is expressed as event dependencies, validated by `VerifyScheduledEvents`.
7. **A fast path must equal the uniform path.** *(inferred — a general engine invariant, stated here
   because it matches Opteryx's §11 contract: a shape/partition discriminant may change layout, never
   the answer or the row set.)*

---

## 19. Implications for Opteryx

Mapping DuckDB's model onto the decisions recorded in the Opteryx memory:

1. **Parallelize pipelines, not operators.** The reverted intra-operator grouped-agg experiments
   (round-robin 0.94×, hash-partition 0.62–0.72× on ClickBench) failed for exactly the reason this
   design predicts: a *serial* merge whose cost rivals the aggregation. The fix is not a better
   intra-op split — it is pipeline-level parallelism with (a) thread-local sinks, (b) a
   **partition-aligned** merge that is a pointer hand-off, and (c) **parallel read-out** of partitions.
   That is the central-scheduler direction.

2. **The local/global state contract is the prerequisite.** Each mergeable operator needs
   `GetGlobalSinkState` (once) / `GetLocalSinkState` (per task) / `Combine` / `Finalize`. The existing
   `merge()` / `merge_group_state` work is the embryo of `Combine`; the missing piece is **radix-aligned
   partitioning** so `Combine` is O(partitions), not O(groups), and so the read-out is parallel too.

3. **Morsel handout = one short cursor lock.** The native thread-safe concurrent pull (8-thread ==
   serial) already mirrors `NextParallelScan` under `state.lock`. Row-group handout with
   vector-sized push iterations is the right granularity.

4. **A cooperative `BLOCKED` state machine is what makes a GIL-off carrier pay off.** Tasks must
   *return control* (result enums + interrupt state) instead of blocking, so a central scheduler can
   reschedule them. The `OperatorResultType` / `SinkResultType` / `BLOCKED` protocol is worth copying
   closely as the operator return contract.

5. **Data-bounded parallelism replaces a magic floor.** `TryGetMaxThreads` = min over operator hints,
   clamped to scheduler threads, driven by the source's morsel count — this is the principled version
   of a row-floor gate, and naturally avoids spinning threads on tiny inputs without a tuned constant.

6. **Events express ordering; locks express nothing about ordering.** Build-before-probe and
   aggregate-before-read become event dependencies with a debug-time cycle check — a clean, auditable
   alternative to ad-hoc completion flags.
