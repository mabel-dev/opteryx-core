# Phase 9c Implementation Plan — C Kernel ABI Dispatch

## Blocking Issue (Fixed)
✅ **Validity Merging Bug** — Fixed in binary_op_arithmetic.cpp:
- Arithmetic kernels now merge validity bitmaps: `out_validity[i] = left_valid & right_valid`
- All 5 arithmetic ops (add, subtract, multiply, divide, modulo) propagate NULLs correctly
- Tests: 137/137 passing

## Implementation Scope

### 1. Executor Dispatch (evaluation.pyx)

**BC_INSTR_C_NATIVE Flag Dispatch Pattern:**
```cython
if slot.flags & BC_INSTR_C_NATIVE:
    # C path: kernel_fn is set by 9b
    dv_result_ptr = call_c_kernel(...)
else:
    # Python fallback: callable_ref (live until 9a-fn)
    legacy_result = (<object>slot.callable_ref)(...)
```

### 2. Function Pointer Typedefs (evaluation.pyx top-level)

Five signatures per Decision 3:
```cython
ctypedef struct VecResult:
    void* data
    uint8_t* validity
    const uint32_t* selection
    bint owns_selection
    uint32_t data_length
    uint32_t length
    DrakenType type
    uint8_t flags

ctypedef VecResult (*binop_fn_t)(void* ctx, const DrakenVector* left, const DrakenVector* right) noexcept nogil
ctypedef VecResult (*cast_fn_t)(void* ctx, const DrakenVector* v) noexcept nogil
ctypedef VecResult (*extr_fn_t)(void* ctx, const DrakenVector* v, const DrakenVector* key) noexcept nogil
ctypedef VecResult (*func_fn_t)(void* ctx, const DrakenVector* const* args, uint32_t nargs) noexcept nogil
ctypedef VecResult (*case_fn_t)(void* ctx, const Morsel* morsel) noexcept nogil
```

### 3. VecResult → DrakenVector Materialization

Convert kernel result into executor stack format:
- Allocate DrakenVector struct in dv_store[sp] (like combinators)
- Copy VecResult fields into DrakenVector
- Create Python Vector wrapper to take ownership of draken_malloc'd memory
- Store Vector in anchor[sp] (keeps memory alive)
- Store &dv_store[sp] in dv_stack[sp]

Helper function:
```cython
cdef DrakenVector* _vecresult_to_draken(
    VecResult result, 
    DrakenVector* storage,
    DrakenFrameArena* arena
) nogil:
    storage.data = result.data
    storage.validity = result.validity
    storage.selection = result.selection
    storage.data_length = result.data_length
    storage.length = result.length
    storage.type = result.type
    return storage
```

### 4. Error Handling

Check sentinel: `if kernel_result.data == NULL`
- Read thread-local error message via `draken_get_error_message()`
- Raise Python exception at GIL boundary

### 5. Five Opcode Branches

#### BC_BINARY_OP (line ~1718)
- Check `BC_INSTR_C_NATIVE` flag
- Cast kernel_fn to `binop_fn_t`
- Call: `result = (<binop_fn_t>slot.kernel_fn)(slot.ctx_ptr, dv_left_ptr, dv_right_ptr)`
- Fallback to Python path if flag not set

#### BC_FUNCTION (line ~1779)
- Check flag; skip for now (9a-fn will add C kernels later)
- Keep Python path live until then

#### BC_EXTRACTION (line ~1856)
- Check flag
- Cast kernel_fn to `extr_fn_t`
- Construct key DrakenVector from slot.literal_obj or convert scalar
- Call: `result = (<extr_fn_t>slot.kernel_fn)(slot.ctx_ptr, dv_left_ptr, dv_key_ptr)`

#### BC_CAST (line ~1898)
- Check flag
- Cast kernel_fn to `cast_fn_t`
- Call: `result = (<cast_fn_t>slot.kernel_fn)(slot.ctx_ptr, dv_input_ptr)`

#### BC_CASE (line ~1922)
- Check flag
- Cast kernel_fn to `case_fn_t`
- Special: needs execute_bytecode_c(bc, morsel) nogil entry point
- Call: `result = (<case_fn_t>slot.kernel_fn)(slot.ctx_ptr, morsel.ptr)`
- Defer nogil surface to 9d

### 6. Telemetry (for verification)

Add per-opcode counter: `c_native_kernel_calls`
- Increment in each C branch
- Assert non-zero after C-dispatch queries
- Guarantees C path actually executed, not fallback

### 7. Testing

Value-checked spot tests per opcode:
```python
# BC_FUNCTION
SELECT LENGTH(name), LOWER(name) FROM $planets LIMIT 3

# BC_EXTRACTION
SELECT missions[0] FROM testdata.astronauts LIMIT 3

# BC_CAST
SELECT CAST(id AS VARCHAR), CAST(id AS DOUBLE) FROM $planets LIMIT 3

# BC_BINARY_OP
SELECT id + 1, id * 2, name || '!' FROM $planets LIMIT 3
SELECT NULL + 1  # Must return NULL

# BC_CASE
SELECT CASE WHEN id < 5 THEN 'small' ELSE 'big' END FROM $planets LIMIT 4

# Chained
SELECT LENGTH(CAST(name AS VARCHAR)) FROM $planets LIMIT 3
```

## Definition of Done

- [ ] Validity bug fixed (✅ DONE)
- [ ] Function-pointer typedefs added
- [ ] 5 opcode branches dispatch on BC_INSTR_C_NATIVE
- [ ] VecResult → DrakenVector materialization working
- [ ] Error sentinel handling (data == NULL) raises
- [ ] Telemetry counter present and tested
- [ ] Value-checked spot tests pass (including NULL propagation)
- [ ] `make c` clean; `make q` 100/100
- [ ] `make clickbench` shows improvement (per-morsel PyObject_Call eliminated)
- [ ] No live `callable_ref` calls in CAST/BINARY_OP/EXTRACTION branches
- [ ] BC_FUNCTION Python fallback live (for 9a-fn)

## Notes

- Don't delete callable_ref branches (9f does that, after 9a-fn lands)
- nogil annotation deferred to 9e (9c branches are nogil-compatible but hold GIL)
- Morsel nogil surface deferred to 9d (BC_CASE re-entry needs it)
- ~120-150 LOC added to evaluation.pyx
- ~10-15 LOC helper function
