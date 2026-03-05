# Opteryx Engine Principles

These principles guide architectural decisions and trade-offs throughout the Opteryx query engine.

## Core Architecture

**Opteryx is a high-performance query engine**

Performance is the primary design goal, balanced against the need to remain maintainable and correct.

**Python is the glue, not the motor**

Python serves as the interface layer for consuming systems and powers the planning phase. The execution engine itself is increasingly Cython/C code, optimized for speed and efficiency. Python's role is coordination and control, not computation.

**Planning prioritizes understandability; execution prioritizes performance**

The planning phase must produce explainable query plans that humans can reason about and debug. The execution phase must be as fast as possible, with deterministic behavior and minimal decision-making. These competing concerns are addressed at different stages of the pipeline.

## Integration & Interfaces

**Arrow is an interface, not an engine**

Apache Arrow is how we speak to the broader data ecosystem. We use it at the boundaries to ensure compatibility with other tools, but Arrow is not the substrate of the engine itself. Internal data structures are optimized for our specific access patterns, not generalized Arrow semantics.

**NumPy is ultimately unhelpful**

While NumPy is powerful, its translation costs and type inference complications outweigh the performance benefits it provides. No new NumPy usage should be introduced, and opportunities to remove existing NumPy dependencies should be taken.

## Performance Focus

**Databases live and die on their JOIN, GROUP BY, and WHERE performance**

These three operations are the performance bottlenecks for most workloads. Optimization efforts are concentrated here, as improvements at this level have outsized impact on overall throughput. All other operations are secondary to making these three as efficient as possible.

**IO is currently our performance bottleneck**

IO efficiency directly impacts throughput more than any other single factor. Optimization of read patterns, buffering strategies, and data prefetching should be prioritized over other optimizations.

## Reliability & Transparency

**Fail visibly rather than silently degrade**

Compensating for errors through fallbacks and broad exception handling masks problems and makes the system harder to reason about. When a dependency is missing or a scenario isn't accounted for, it's better to fail early and clearly than to silently degrade. This surfaces real problems that can be solved, rather than burying them in workarounds.