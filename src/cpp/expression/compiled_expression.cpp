// compiled_expression.cpp — lowers a Python Node tree into the arena.
//
// The lowering pass reads attributes from the source Node via the Python C API.
// It never dispatches on schema_column type or kernel resolution at this stage —
// it only restructures the tree into a C++ layout the Cython orchestrator can
// walk without going through Node.__getattr__'s dict fallback per access.
//
// PyObject* fields on each CompiledExpression are guaranteed non-NULL: missing
// attributes are normalised to Py_None so consumers never have to NULL-check.

#include "compiled_expression.h"

#include <cstddef>

namespace opteryx_expr {

CompiledExpressionArena::~CompiledExpressionArena() {
    // The arena may outlive the Python interpreter shutdown sequence in
    // pathological cases (interpreter teardown); guard with Py_IsInitialized.
    if (!Py_IsInitialized()) {
        return;
    }
    for (PyObject* obj : held_refs_) {
        Py_XDECREF(obj);
    }
}

void CompiledExpressionArena::hold(PyObject* obj) {
    Py_INCREF(obj);
    held_refs_.push_back(obj);
}

// Read an attribute by name. Returns a new reference. If missing or None,
// returns Py_None with refcount incremented. Never returns NULL except on
// genuine Python error (e.g., MemoryError).
static PyObject* get_attr_or_none(PyObject* obj, const char* name) {
    PyObject* val = PyObject_GetAttrString(obj, name);
    if (val == nullptr) {
        // Cython Node.__getattr__ returns None for missing keys, so we don't
        // expect AttributeError here, but defend against arbitrary Python
        // objects passed in.
        if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
            PyErr_Clear();
            Py_INCREF(Py_None);
            return Py_None;
        }
        return nullptr;
    }
    return val;
}

CompiledExpression* CompiledExpressionArena::lower(PyObject* py_node) {
    return lower_one(py_node);
}

CompiledExpression* CompiledExpressionArena::lower_one(PyObject* py_node) {
    if (py_node == nullptr || py_node == Py_None) {
        PyErr_SetString(PyExc_TypeError,
                        "compiled_expression.lower: source node is None");
        return nullptr;
    }

    // Allocate the slot up-front so children can be placed first and our
    // pointer to `self` remains stable (std::deque does not invalidate).
    nodes_.emplace_back();
    CompiledExpression* slot = &nodes_.back();
    slot->node_type = NT_UNKNOWN;
    slot->value = nullptr;
    slot->schema_column = nullptr;
    slot->source_node = nullptr;
    slot->left = nullptr;
    slot->right = nullptr;
    slot->centre = nullptr;

    // Hold the source Node for the arena's lifetime so consumers can hand it
    // back to Python-facing kernels without reconstructing it.
    Py_INCREF(py_node);
    slot->source_node = py_node;
    held_refs_.push_back(py_node);

    // node_type — Node.node_type is an IntEnum, int(...) gives us the value.
    PyObject* nt_obj = PyObject_GetAttrString(py_node, "node_type");
    if (nt_obj == nullptr) {
        return nullptr;
    }
    long nt = PyLong_AsLong(nt_obj);
    Py_DECREF(nt_obj);
    if (nt == -1 && PyErr_Occurred()) {
        return nullptr;
    }
    slot->node_type = static_cast<int>(nt);

    // value, schema_column — hold a ref for the arena's lifetime.
    PyObject* value = get_attr_or_none(py_node, "value");
    if (value == nullptr) return nullptr;
    slot->value = value;  // owns the ref returned by get_attr_or_none
    held_refs_.push_back(value);

    PyObject* sc = get_attr_or_none(py_node, "schema_column");
    if (sc == nullptr) return nullptr;
    slot->schema_column = sc;
    held_refs_.push_back(sc);

    // Children. For each, if present and not None, recurse. Pointer remains
    // stable because std::deque does not relocate elements.
    auto recurse_child = [&](const char* attr_name,
                             CompiledExpression** out) -> bool {
        PyObject* child = PyObject_GetAttrString(py_node, attr_name);
        if (child == nullptr) {
            if (PyErr_ExceptionMatches(PyExc_AttributeError)) {
                PyErr_Clear();
                return true;  // absent, leave NULL
            }
            return false;
        }
        if (child == Py_None) {
            Py_DECREF(child);
            return true;
        }
        CompiledExpression* sub = lower_one(child);
        Py_DECREF(child);
        if (sub == nullptr) return false;
        *out = sub;
        return true;
    };

    if (!recurse_child("left", &slot->left)) return nullptr;
    if (!recurse_child("right", &slot->right)) return nullptr;
    if (!recurse_child("centre", &slot->centre)) return nullptr;

    // parameters — list of Node, may be missing/None.
    PyObject* params = PyObject_GetAttrString(py_node, "parameters");
    if (params == nullptr) {
        if (!PyErr_ExceptionMatches(PyExc_AttributeError)) {
            return nullptr;
        }
        PyErr_Clear();
    } else if (params != Py_None) {
        if (!PyList_Check(params) && !PyTuple_Check(params)) {
            Py_DECREF(params);
            PyErr_SetString(PyExc_TypeError,
                            "compiled_expression.lower: Node.parameters must be list or tuple");
            return nullptr;
        }
        Py_ssize_t n = PySequence_Length(params);
        if (n < 0) {
            Py_DECREF(params);
            return nullptr;
        }
        slot->parameters.reserve(static_cast<std::size_t>(n));
        for (Py_ssize_t i = 0; i < n; ++i) {
            PyObject* item = PySequence_GetItem(params, i);
            if (item == nullptr) {
                Py_DECREF(params);
                return nullptr;
            }
            if (item == Py_None) {
                Py_DECREF(item);
                slot->parameters.push_back(nullptr);
                continue;
            }
            CompiledExpression* sub = lower_one(item);
            Py_DECREF(item);
            if (sub == nullptr) {
                Py_DECREF(params);
                return nullptr;
            }
            slot->parameters.push_back(sub);
        }
        Py_DECREF(params);
    } else {
        Py_DECREF(params);
    }

    return slot;
}

// Depth-first walk producing (node_type, num_children) tuples.
// num_children counts non-NULL children: left, right, centre, plus every
// non-NULL element of parameters.
static bool walk_recursive(const CompiledExpression* node, PyObject* out_list) {
    int num_children = 0;
    if (node->left) ++num_children;
    if (node->right) ++num_children;
    if (node->centre) ++num_children;
    for (auto* p : node->parameters) {
        if (p) ++num_children;
    }

    PyObject* entry = Py_BuildValue("(ii)", node->node_type, num_children);
    if (entry == nullptr) return false;
    int rc = PyList_Append(out_list, entry);
    Py_DECREF(entry);
    if (rc < 0) return false;

    if (node->left && !walk_recursive(node->left, out_list)) return false;
    if (node->right && !walk_recursive(node->right, out_list)) return false;
    if (node->centre && !walk_recursive(node->centre, out_list)) return false;
    for (auto* p : node->parameters) {
        if (p && !walk_recursive(p, out_list)) return false;
    }
    return true;
}

PyObject* CompiledExpressionArena::node_type_walk(CompiledExpression* root) const {
    PyObject* list = PyList_New(0);
    if (list == nullptr) return nullptr;
    if (root == nullptr) return list;
    if (!walk_recursive(root, list)) {
        Py_DECREF(list);
        return nullptr;
    }
    return list;
}

}  // namespace opteryx_expr
