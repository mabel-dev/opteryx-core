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
//
// Step kinds: each node also carries its step's kind (a LogicalPlanStepType value,
// in `kinds_`, parallel to `steps_`) and the graph keeps a count per kind, so "does
// the plan hold a step of kind K" is one read rather than a walk of the steps. A
// physical plan's steps have no kind and are added with kNoStepKind, which is
// never counted.

#pragma once

#include <Python.h>

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "planner/plan_topology.hpp"

namespace opteryx { namespace planner {

constexpr int32_t kNoStepKind = -1;
constexpr int32_t kStepKindSlots = 64;  // LogicalPlanStepType values are below this

class PlanGraph : public PlanTopology {
public:
    PlanGraph() = default;

    // A structural copy: same ids, same edges (stamps included), the SAME step
    // objects (one more reference each). Starts unmodified: epoch 0.
    PlanGraph(const PlanGraph& other)
        : PlanTopology(other), steps_(other.steps_), kinds_(other.kinds_) {
        for (int32_t k = 0; k < kStepKindSlots; ++k) {
            kind_counts_[k] = other.kind_counts_[k];
        }
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
    int32_t kind_at(size_t position) const { return kinds_[position]; }

    // How many steps of `kind` the plan holds.
    uint32_t kind_count(int32_t kind) const {
        check_kind(kind);
        return kind == kNoStepKind ? 0 : kind_counts_[kind];
    }

    void add(NodeId id, PyObject* step, int32_t kind) {
        check_kind(kind);
        PlanTopology::add(id);
        Py_INCREF(step);
        steps_.push_back(step);
        push_kind(kind);
    }

    void replace(NodeId id, PyObject* step, int32_t kind) {
        check_kind(kind);
        const size_t at = position(id);
        PyObject*& slot = steps_[at];
        Py_INCREF(step);
        Py_DECREF(slot);
        slot = step;
        uncount(kinds_[at]);
        kinds_[at] = kind;
        count(kind);
        bump_epoch();
    }

    void remove(NodeId id, bool heal) {
        const size_t at = position(id);
        PyObject* step = steps_[at];
        PlanTopology::remove(id, heal);
        steps_.erase(steps_.begin() + static_cast<std::ptrdiff_t>(at));
        uncount(kinds_[at]);
        kinds_.erase(kinds_.begin() + static_cast<std::ptrdiff_t>(at));
        Py_DECREF(step);
    }

    void insert_before(NodeId id, PyObject* step, int32_t kind, NodeId before) {
        check_kind(kind);
        PlanTopology::insert_before(id, before);
        Py_INCREF(step);
        steps_.push_back(step);
        push_kind(kind);
    }

    void insert_after(NodeId id, PyObject* step, int32_t kind, NodeId after) {
        check_kind(kind);
        PlanTopology::insert_after(id, after);
        Py_INCREF(step);
        steps_.push_back(step);
        push_kind(kind);
    }

    // Take in every node, edge and step of `other`; the two must share no node.
    void absorb(const PlanGraph& other) {
        PlanTopology::absorb(other);
        for (size_t i = 0; i < other.steps_.size(); ++i) {
            Py_INCREF(other.steps_[i]);
            steps_.push_back(other.steps_[i]);
            push_kind(other.kinds_[i]);
        }
    }

    // A copy with each node's id and step replaced: position i of the result is
    // position i of this graph under `ids[i]`, holding `steps[i]`. Edges and their
    // stamps carry over, remapped.
    PlanGraph* remapped(const std::vector<NodeId>& ids, const std::vector<PyObject*>& steps,
                        const std::vector<int32_t>& kinds) const {
        for (int32_t kind : kinds) {
            check_kind(kind);
        }
        PlanGraph* out = new PlanGraph();
        remap_into(*out, ids);
        out->steps_.reserve(steps.size());
        out->kinds_.reserve(kinds.size());
        for (size_t i = 0; i < size(); ++i) {
            Py_INCREF(steps[i]);
            out->steps_.push_back(steps[i]);
            out->push_kind(kinds[i]);
        }
        return out;
    }

private:
    static void check_kind(int32_t kind) {
        if (kind != kNoStepKind && (kind < 0 || kind >= kStepKindSlots)) {
            throw std::out_of_range("plan step kind outside the plan graph's kind slots");
        }
    }
    void count(int32_t kind) {
        if (kind != kNoStepKind) {
            ++kind_counts_[kind];
        }
    }
    void uncount(int32_t kind) {
        if (kind != kNoStepKind) {
            --kind_counts_[kind];
        }
    }
    void push_kind(int32_t kind) {
        kinds_.push_back(kind);
        count(kind);
    }

    std::vector<PyObject*> steps_;  // owned; parallel to the topology's positions
    std::vector<int32_t> kinds_;    // each step's kind; parallel to steps_
    uint32_t kind_counts_[kStepKindSlots] = {};
};

}}  // namespace opteryx::planner
