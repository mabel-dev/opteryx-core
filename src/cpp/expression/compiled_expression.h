// compiled_expression.h — flat C++ representation of an expression tree.
//
// Layering (CLAUDE.md):
//   - Python : planner / binder produce the source Node tree.
//   - C++    : this header defines the lowered representation, owned by a
//              CompiledExpressionArena and walked by the Cython orchestrator.
//   - C++    : kernels (Draken vector ops) are called by the orchestrator using
//              data fished out of these structs (PyObject* handles to value /
//              schema_column / kernel, resolved at lower-time).
//
// NodeType integer constants MUST match:
//   - opteryx/expression/__init__.py            (NodeType IntEnum, runtime truth)
//   - opteryx/expression/evaluator/evaluation.pyx (DEF constants)
// A Python-level fail-fast assertion in opteryx/expression/evaluator/__init__.py
// guards the first two; the values here are validated by the round-trip test.

#pragma once

#include <Python.h>
#include <cstddef>
#include <deque>
#include <vector>

namespace opteryx_expr {

enum NodeTypeCode : int {
    NT_UNKNOWN = 0,
    NT_AND = 17, NT_OR = 18, NT_XOR = 19, NT_NOT = 20, NT_DNF = 21, NT_CNF = 22,
    NT_CASE = 32, NT_WILDCARD = 33, NT_COMPARISON_OPERATOR = 34,
    NT_BINARY_OPERATOR = 35, NT_UNARY_OPERATOR = 36, NT_FUNCTION = 37,
    NT_IDENTIFIER = 38, NT_SUBQUERY = 39, NT_NESTED = 40, NT_AGGREGATOR = 41,
    NT_LITERAL = 42, NT_EXPRESSION_LIST = 43, NT_EVALUATED = 44, NT_CAST = 45,
    NT_EXTRACTION_OPERATOR = 46, NT_BETWEEN = 47,
};

// One node in the lowered tree. Trivially copyable layout (pointers + ints); the
// arena holds the storage and the Python reference counts on the embedded
// PyObject* fields. Do not INCREF/DECREF these from outside the arena.
struct CompiledExpression {
    int node_type;

    // Borrowed views into Python objects that live for the arena's lifetime.
    // Never NULL: missing fields are Py_None (no special-case in consumers).
    PyObject* value;
    PyObject* schema_column;

    // Reference to the source opteryx Node. Held for the arena's lifetime so
    // the Cython walker can hand it to existing Python-facing kernels without
    // rebuilding it. Wedge D2 will replace this with resolved fields and the
    // source_node ref can then be dropped.
    PyObject* source_node;

    // Child pointers into the same arena. NULL if absent. Children remain valid
    // for the arena's lifetime — std::deque does not invalidate pointers on
    // push_back, which is why the arena uses one.
    CompiledExpression* left;
    CompiledExpression* right;
    CompiledExpression* centre;

    // FUNCTION / EXPRESSION_LIST / DNF / CNF use this; empty for others.
    std::vector<CompiledExpression*> parameters;
};

// Owns the storage for a compiled expression tree and the Python refs it holds.
// One arena per compiled expression tree. Construct, call lower() once with the
// source Node, then walk via the returned root pointer.
class CompiledExpressionArena {
public:
    CompiledExpressionArena() = default;
    ~CompiledExpressionArena();

    CompiledExpressionArena(const CompiledExpressionArena&) = delete;
    CompiledExpressionArena& operator=(const CompiledExpressionArena&) = delete;

    // Lower a Python opteryx.compiled.structures.node.Node tree into this arena.
    // GIL must be held. Returns the root, which is owned by the arena.
    // On error returns NULL with a Python exception set.
    CompiledExpression* lower(PyObject* py_node);

    // Depth-first walk for round-trip tests. Returns a Python list of
    // (node_type, num_children) tuples. GIL must be held.
    PyObject* node_type_walk(CompiledExpression* root) const;

    std::size_t node_count() const { return nodes_.size(); }

private:
    std::deque<CompiledExpression> nodes_;
    std::vector<PyObject*> held_refs_;

    CompiledExpression* lower_one(PyObject* py_node);
    void hold(PyObject* obj);
};

}  // namespace opteryx_expr
