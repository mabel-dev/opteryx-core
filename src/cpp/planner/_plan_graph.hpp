// The plan graph's Python bridge: the Python-free structure (plan_topology.hpp)
// plus the typed step object the planner built for each node.
//
// Native plan graph P2 (architect rulings 2026-09-27). A step is an OWNED
// reference: each graph holds its own, so replacing the step behind an id in one
// graph (the optimizer's copy-on-write working plan) leaves every other graph
// that holds the id untouched.
//
// Steps live in `steps_`, parallel to the topology's node positions: a node
// added is appended at the end and a node removed closes the gap, so every
// node-set mutation below pairs the topology's change with the same change here.
// Native readers that do not need the steps (the statistics refresh) take a
// `const PlanTopology*` and never see this header.
//
// Mutations require the GIL (they move references); reading ids and edges does
// not touch Python.

#pragma once

#include <Python.h>

#include <cstddef>
#include <vector>

#include "planner/plan_topology.hpp"

namespace opteryx { namespace planner {

class PlanGraph : public PlanTopology {
public:
    PlanGraph() = default;

    // A structural copy: same ids, same edges (stamps included), the SAME step
    // objects (one more reference each). Starts unmodified: epoch 0.
    PlanGraph(const PlanGraph& other) : PlanTopology(other), steps_(other.steps_) {
        for (PyObject* step : steps_) {
            Py_INCREF(step);
        }
    }

    PlanGraph& operator=(const PlanGraph&) = delete;

    ~PlanGraph() {
        for (PyObject* step : steps_) {
            Py_DECREF(step);
        }
    }

    PyObject* step(NodeId id) const { return steps_[position(id)]; }
    PyObject* step_at(size_t position) const { return steps_[position]; }

    void add(NodeId id, PyObject* step) {
        PlanTopology::add(id);
        Py_INCREF(step);
        steps_.push_back(step);
    }

    void replace(NodeId id, PyObject* step) {
        PyObject*& slot = steps_[position(id)];
        Py_INCREF(step);
        Py_DECREF(slot);
        slot = step;
        bump_epoch();
    }

    void remove(NodeId id, bool heal) {
        const size_t at = position(id);
        PyObject* step = steps_[at];
        PlanTopology::remove(id, heal);
        steps_.erase(steps_.begin() + static_cast<std::ptrdiff_t>(at));
        Py_DECREF(step);
    }

    void insert_before(NodeId id, PyObject* step, NodeId before) {
        PlanTopology::insert_before(id, before);
        Py_INCREF(step);
        steps_.push_back(step);
    }

    void insert_after(NodeId id, PyObject* step, NodeId after) {
        PlanTopology::insert_after(id, after);
        Py_INCREF(step);
        steps_.push_back(step);
    }

    // Take in every node, edge and step of `other`; the two must share no node.
    void absorb(const PlanGraph& other) {
        PlanTopology::absorb(other);
        for (PyObject* step : other.steps_) {
            Py_INCREF(step);
            steps_.push_back(step);
        }
    }

    // A copy with each node's id and step replaced: position i of the result is
    // position i of this graph under `ids[i]`, holding `steps[i]`. Edges and their
    // stamps carry over, remapped.
    PlanGraph* remapped(const std::vector<NodeId>& ids, const std::vector<PyObject*>& steps) const {
        PlanGraph* out = new PlanGraph();
        remap_into(*out, ids);
        out->steps_.reserve(steps.size());
        for (size_t i = 0; i < size(); ++i) {
            Py_INCREF(steps[i]);
            out->steps_.push_back(steps[i]);
        }
        return out;
    }

private:
    std::vector<PyObject*> steps_;  // owned; parallel to the topology's positions
};

}}  // namespace opteryx::planner
