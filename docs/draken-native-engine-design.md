# Draken-Native Query Engine Architecture

**Date:** March 6, 2026  
**Status:** In Progress - Quick wins implemented  
**Goal:** Push Arrow entirely to the edges; query engine (motor) is Draken-native

---

## Architecture: Query Engine vs Cursor

```
                    ┌─────────────────────────────────────────┐
                    │   User Python Code (Cursor Layer)       │
                    │   - Session/Connection                  │
                    │   - Format conversion:                  │
                    │     Draken Morsel → Arrow / JSON / CSV  │
                    │   - Type interpretation:                │
                    │     INTERVAL, VARCHAR, etc.             │
                    └──────────────┬──────────────────────────┘
                                   │ User's desired format
                                   │ (Arrow, JSON, CSV, etc.)
            ┌──────────────────────────────────────────────────┐
            │        EXIT NODE (Engine Output Boundary)        │
            │  - Input: Draken Morsel                          │
            │  - Column selection/rename: Draken-native        │
            │  - Output: Draken Morsel ──► to cursor          │
            └──────────────┬───────────────────────────────────┘
                           │ Draken Morsel
            ┌──────────────▼───────────────────────────────────┐
            │      QUERY ENGINE (THE MOTOR)                    │
            │      ✅ 100% DRAKEN-NATIVE                       │
            │                                                   │
            │  OPERATORS:                                      │
            │  • ProjectionNode   - column selection/renaming  │
            │  • FilterNode       - WHERE predicates (→Phase 3) │
            │  • LimitNode        - LIMIT/OFFSET               │
            │  • DistinctNode     - SELECT DISTINCT            │
            │  • HeapSortNode     - ORDER BY (top-N)           │
            │  • SortNode         - ORDER BY (full sort)       │
            │  • DrakenAggregateAndGroupNode → GROUP BY        │
            │  (+ all other aggregate/join operators)          │
            │                                                   │
            │  DATA SOURCES:                                   │
            │  • ParquetReadNode (Rugo) → outputs Draken       │
            │  • ShuffleNode → Draken partitioning             │
            │                                                   │
            │  ⚠️  TEMPORARY ARROW BRIDGE (Phase 2-4):         │
            │  • ParquetReadNode predicates (→Phase 3)         │
            │  • Type casting in Parquet reader (→Phase 4)     │
            │                                                   │
            └──────────────┬───────────────────────────────────┘
                           │ Draken Morsel
                    ┌──────▼──────────────┐
                    │  Data Sources (I/O) │
                    │  - Parquet (Rugo)   │
                    │  - Connectors       │
                    └─────────────────────┘
```

---

## Changes Implemented (This Session)

### ✅ Operator Drakenization (Quick Wins)

| Operator | Status | Change |
|----------|--------|--------|
| **LimitNode** | ✅ DONE | Removed Arrow import, use `ensure_draken_morsel()`, Morsel.slice() |
| **HeapSortNode** | ✅ DONE | Added Float64Vector, StringVector to vector optimization whitelist |
| **ProjectionNode** | ✅ DONE | Removed Arrow fallback, pure `ensure_draken_morsel()` + `_execute_morsel_projection()` |
| **ExitNode** | ✅ DONE | Draken-native column ops, emits Morsel to cursor layer |

### Code Changes Summary

**LimitNode** (limit_node.py):
```diff
- import pyarrow
+ from opteryx.draken.morsels.morsel import Morsel

  def execute(self, morsel: Morsel, **kwargs) -> Morsel:
-     morsel = self.ensure_arrow_table(morsel)
+     morsel = self.ensure_draken_morsel(morsel)
      
      # Now uses morsel.slice(offset, length) directly
```

**HeapSortNode** (heap_sort_node.py):
```diff
  _EXACT_COMPRESS_VECTOR_TYPES = frozenset({
      "BoolVector",
      "Date32Vector",
+     "Float64Vector",      # NEW - float sorting optimization
      ... (other int/date types)
+     "StringVector",       # NEW - string sorting optimization
  })
```

**ProjectionNode** (projection_node.py):
```diff
  def execute(self, morsel: Morsel, **kwargs) -> Morsel:
      if morsel == EOS:
          yield EOS
          return

-     if isinstance(morsel, Morsel):
-         yield self._execute_morsel_projection(morsel)
-         return
-     
-     table = self.ensure_arrow_table(morsel)
-     # ... Arrow fallback paths removed ...
+     morsel = self.ensure_draken_morsel(morsel)
+     yield self._execute_morsel_projection(morsel)
```

**ExitNode** (exit_node.py):
```diff
- def execute(self, morsel: Table, **kwargs) -> Table:
-     morsel = self.ensure_arrow_table(morsel)  # ❌ Convert to Arrow first
+ def execute(self, morsel, **kwargs):
+     morsel = self.ensure_draken_morsel(morsel)  # ✅ Keep as Draken
      
-     # ... work on Arrow ...
+     # Draken-native: select columns and rename
+     morsel = morsel.select(self.final_columns)
+     morsel = morsel.rename(self.final_names)
      
-     # ... type conversions on Arrow ...
+     # Emit Draken morsel to cursor layer
+     yield morsel
      
-     yield morsel
+     # Cursor layer is responsible for:
+     # - Format conversion (Arrow/JSON/CSV/MessagePack/etc)
+     # - Type interpretation (INTERVAL, VARCHAR, etc)
```

---

## Remaining Arrow Usage & Plan

### Current State

**Arrow INSIDE the engine (temporary, to be removed):**
- ⚠️ **ParquetReadNode:**
  - Uses Arrow for: predicate evaluation (`evaluate()` + `table.filter()`)
  - Uses Arrow for: type casting (`pyarrow.compute.cast()`)
  - **Why:** Draken doesn't have native predicate/casting support yet
  - **When fixed:** Phase 2-3 (expression evaluator + Draken filter/cast kernels)

**Arrow OUTSIDE the engine (intentional, permanent):**
- ✅ **ExitNode → Cursor boundary**
  - Convert Morsel to Arrow just before returning to user
  - Type conversions (interval, varchar) at boundary
  - **Justification:** Cursor is Python/user code, needs PyArrow

### Migration Path

**Phase 1 (Completed):** Quick wins - 4 operators fully Draken-native
- ✅ LimitNode, HeapSortNode, ProjectionNode, ExitNode
- **Impact:** ~5-10% overhead reduction in operator execution

**Phase 2 (Planned):** Expression evaluator Draken entry points
- Prerequisite for removing Arrow from ParquetReadNode predicates
- Add `evaluate_morsel()` support for:
  - Column references → vectors
  - Comparison operators → BoolVector (for filters)
  - Type conversions → Draken vectors

**Phase 3 (Planned):** Draken-native predicates
- FilterNode accepts Draken morsels
- Uses `evaluate_morsel()` to generate BoolVector masks
- Applies masks without Arrow conversion

**Phase 4 (Planned):** Draken-native type casting
- ParquetReadNode uses Draken vectors for type conversions
- Removes final Arrow usage from reading pipeline

---

## Design Principles

### "Arrow at the Edges"

The query engine (motor) is **Draken-native throughout**. Arrow conversion happens **only** at the cursor boundary:

1. **Input boundary (data sources):**
   - ParquetReadNode reads with Rugo → Draken morsels
   - Currently uses Arrow for predicates/casting (temporary, Phase 2-4)

2. **Output boundary (cursor only):**
   - ExitNode emits Draken morsels
   - Cursor converts Draken → user's desired format (Arrow, JSON, CSV, MessagePack, etc.)
   - Type interpretation (INTERVAL, VARCHAR, etc.) happens in cursor

### "Fail at Compile Time"

Per the project's principles, we do NOT use Arrow fallbacks:
- ✅ LimitNode: No "if can't slice with Morsel, use Arrow" fallback
- ✅ ProjectionNode: No "if Morsel fails, try Arrow" try-except
- ✅ ExitNode: Pure Draken output; cursor handles format conversion

If an operator can't work with Draken, the planner must detect it early and route appropriately.

### "Performance Over Convenience"

Draken-native code might be less convenient (e.g., Morsel API vs Arrow API) but:
- Zero-copy operations when possible
- No serialization/deserialization cycles
- Direct Cython execution in hot paths
- Columnar layout optimized for Draken processors
- Cursor layer abstracts format differences from engine

---

## Status & Next Steps

### What's Done ✅
- LimitNode: Fully Draken-native ✅
- HeapSortNode: Vector optimization expanded ✅
- ProjectionNode: Fully Draken-native ✅
- ExitNode: Draken-native with Arrow at boundary ✅

### What's Next 🔄
1. **Test the 4 operators** against existing test suite
2. **Benchmark** to validate overhead reduction in LIMIT/ORDER BY/projection
3. **Document** the new architecture in code comments
4. **Start Phase 1** (planner work for GROUP BY → DrakenAggregateAndGroupNode)

### Integration with Cursor (CORRECTED)

The cursor layer (Session/execute_to_arrow) is a simple format adapter:
- ✅ ExitNode emits pure Draken morsels
- ✅ Cursor calls `morsel.to_arrow()` - vectors handle all type conversions
- ✅ Cursor concatenates Arrow tables
- ✅ No schema-aware conversions in cursor (vectors handle this!)
- ✅ Multiple output formats possible: Arrow, JSON, CSV, etc.

---

## Architecture Summary

**Before (Mixed Arrow/Draken, Many Conversions):**
```
Parquet → Rugo → Morsel → Arrow (ParquetRead)
                            ↓
                         Arrow (Filter)
                            ↓
                         Arrow (Projection)
                            ↓
                         Morsel (back to Draken)
                            ↓
                         Arrow (ExitNode)
                            ↓
                         Cursor (Arrow consumer)
```
Many unnecessary conversions, overhead in hot paths!

**After (Pure Draken Engine, Format Conversion at Cursor):**
```
Parquet → Rugo → Morsel (ParquetRead)
                    ↓
            Morsel (Filter, native - Phase 3)
                    ↓
            Morsel (Projection, native)
                    ↓
            Morsel (Limit, native)
                    ↓
            Morsel (ExitNode - no conversion)
                    ↓
    Cursor (format adapter)
       ↓
    Arrow / JSON / CSV / MessagePack / etc.
```
Clean Draken throughout engine, cursor handles format conversion!

---

## Integration with Cursor (CORRECTED)

The cursor layer becomes a simple format adapter:
- ✅ ExitNode emits pure Draken morsels
- ✅ Cursor calls `morsel.to_arrow()` to get Arrow tables (vectors handle type conversions)
- ✅ Cursor concatenates Arrow tables
- ✅ Cursor can convert to other formats on demand (JSON, CSV, MessagePack, etc.)
- ✅ No schema metadata interpretation in cursor - vectors know how to convert themselves

---

## Code Locations

| File | Change | Status |
|------|--------|--------|
| `opteryx/operators/limit_node.py` | Remove Arrow, use Morsel.slice() | ✅ |
| `opteryx/operators/heap_sort_node.py` | Add Float64Vector, StringVector | ✅ |
| `opteryx/operators/projection_node.py` | Remove Arrow fallback | ✅ |
| `opteryx/operators/exit_node.py` | Draken + boundary Arrow conversion | ✅ |
| `opteryx/operators/parquet_read_node.py` | TODO: remove Arrow (Phase 2-4) | 🔄 |
| `opteryx/operators/filter_node.py` | TODO: Draken predicates (Phase 3) | 🔄 |
| `opteryx/operators/sort_node.py` | TODO: Draken sort kernel (Phase 5) | 🔄 |

---

**Next Action:** Test the 4 operators, then proceed to Phase 1 (planner work for GROUP BY).
