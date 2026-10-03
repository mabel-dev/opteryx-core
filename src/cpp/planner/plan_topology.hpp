// The plan graph's structure: node ids, edges and edge roles, native and
// Python-free.
//
// Native plan graph P2 (architect rulings 2026-09-27). Planning is Python; the
// structure it builds and rewrites lives here so native code (the statistics
// refresh, later the engine) can walk a plan without touching Python. The step
// object each node carries lives in the bridge (_plan_graph.hpp), which extends
// this class with a parallel array of owned steps - nothing here knows they exist.
//
//   - A node is a per-query NodeId, minted by the query's PlanContext and never
//     reused.
//   - Edges run from a producer to its consumer and carry a role: LEFT/RIGHT for
//     the two legs of a join or set operation, NONE otherwise.
//   - Out-edges keep insertion order. In-edges are held in the CANONICAL order
//     (ruling 2026-09-27): LEFT, then RIGHT, then unlabelled, creation order
//     within a role. A copy keeps each edge's creation stamp, so it keeps the
//     order.
//   - Every edge joins two nodes of the graph. Removing a node removes its edges;
//     `heal` also reconnects each producer to each consumer.
//   - Positions: a node is appended at the end when added and keeps its relative
//     position until removed (removal closes the gap). That is the whole contract
//     the bridge's parallel step array relies on.

#pragma once

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace opteryx { namespace planner {

using NodeId = uint32_t;

enum EdgeRole : uint8_t {
    EDGE_NONE = 0,
    EDGE_LEFT = 1,
    EDGE_RIGHT = 2,
};

inline int edge_role_rank(EdgeRole role) {
    return role == EDGE_LEFT ? 0 : (role == EDGE_RIGHT ? 1 : 2);
}

struct Edge {
    NodeId other;   // the consumer on an out-edge, the producer on an in-edge
    EdgeRole role;
    uint64_t stamp;  // creation order; shared by both halves of one edge
};

struct PlanNode {
    NodeId id;
    std::vector<Edge> out;
    std::vector<Edge> in;
};

class PlanTopology {
public:
    PlanTopology() = default;

    // A structural copy: same ids, same edges (stamps included). Starts
    // unmodified: epoch 0.
    PlanTopology(const PlanTopology& other)
        : nodes_(other.nodes_), index_(other.index_), epoch_(0), next_stamp_(other.next_stamp_) {}

    PlanTopology& operator=(const PlanTopology&) = delete;

    size_t size() const { return nodes_.size(); }
    uint64_t epoch() const { return epoch_; }

    size_t edge_count() const {
        size_t count = 0;
        for (const PlanNode& node : nodes_) {
            count += node.out.size();
        }
        return count;
    }

    bool contains(NodeId id) const {
        return id < index_.size() && index_[id] >= 0;
    }

    // Nodes in insertion order.
    const PlanNode& node_at(size_t position) const { return nodes_[position]; }
    const PlanNode& node(NodeId id) const { return nodes_[position(id)]; }

    // Add the edge source -> target, or set the role of the one already there.
    void add_edge(NodeId source, NodeId target, EdgeRole role) {
        PlanNode& producer = nodes_[position(source)];
        PlanNode& consumer = nodes_[position(target)];
        for (Edge& edge : producer.out) {
            if (edge.other == target) {
                if (edge.role != role) {
                    edge.role = role;
                    for (Edge& back : consumer.in) {
                        if (back.other == source) {
                            back.role = role;
                        }
                    }
                    sort_in(consumer);
                }
                ++epoch_;
                return;
            }
        }
        const uint64_t stamp = next_stamp_++;
        producer.out.push_back(Edge{target, role, stamp});
        insert_in(consumer, Edge{source, role, stamp});
        ++epoch_;
    }

    // Remove the edge source -> target carrying `role`; false when there is none.
    bool remove_edge(NodeId source, NodeId target, EdgeRole role) {
        PlanNode& producer = nodes_[position(source)];
        auto found = std::find_if(producer.out.begin(), producer.out.end(), [&](const Edge& edge) {
            return edge.other == target && edge.role == role;
        });
        if (found == producer.out.end()) {
            return false;
        }
        producer.out.erase(found);
        erase_in(nodes_[position(target)], source);
        ++epoch_;
        return true;
    }

    bool relationship(NodeId source, NodeId target, EdgeRole* role) const {
        for (const Edge& edge : nodes_[position(source)].out) {
            if (edge.other == target) {
                *role = edge.role;
                return true;
            }
        }
        return false;
    }

    // The (left, right) producers of a two-input node - a join or a set
    // operation - by edge role. Legs are labelled where they are made and nothing
    // reads a leg from edge order (architect ruling 2026-09-27): false unless the
    // node has exactly one LEFT and one RIGHT in-edge and nothing else.
    bool legs(NodeId id, NodeId* left, NodeId* right) const {
        const PlanNode& node = nodes_[position(id)];
        if (node.in.size() != 2 || node.in[0].role != EDGE_LEFT || node.in[1].role != EDGE_RIGHT) {
            return false;
        }
        *left = node.in[0].other;
        *right = node.in[1].other;
        return true;
    }

    // The order a visitor walks the plan from `root` toward the producers: each
    // node before its producers, producers in the canonical in-edge order, a node
    // reached by two consumers visited once per consumer (no visited set — this is
    // the walk the optimizer's strategies see, node by node). Each entry is
    // (node, the consumer it was reached from); the root's consumer is `root`
    // itself. Throws rather than loop forever when the plan has a cycle.
    std::vector<std::pair<NodeId, NodeId>> visit_order(NodeId root) const {
        struct Frame {
            size_t at;    // position of the node
            size_t next;  // the next in-edge to descend
        };
        std::vector<std::pair<NodeId, NodeId>> out;
        std::vector<Frame> path;
        std::vector<uint8_t> on_path(nodes_.size(), 0);
        out.reserve(nodes_.size());
        out.emplace_back(root, root);
        path.push_back(Frame{position(root), 0});
        on_path[path.back().at] = 1;
        while (!path.empty()) {
            Frame& frame = path.back();
            const PlanNode& node = nodes_[frame.at];
            if (frame.next == node.in.size()) {
                on_path[frame.at] = 0;
                path.pop_back();
                continue;
            }
            const size_t child = position(node.in[frame.next++].other);
            if (on_path[child]) {
                throw std::logic_error("plan graph: node " + std::to_string(nodes_[child].id) +
                                       " feeds itself; the plan has a cycle");
            }
            out.emplace_back(nodes_[child].id, node.id);
            on_path[child] = 1;
            path.push_back(Frame{child, 0});
        }
        return out;
    }

    // Nodes that consume something and feed nothing, ascending by id — the plan's
    // heads. A single-node graph's one node is its head.
    std::vector<NodeId> exit_points() const {
        std::vector<NodeId> out;
        if (nodes_.size() == 1) {
            out.push_back(nodes_[0].id);
            return out;
        }
        for (const PlanNode& node : nodes_) {
            if (node.out.empty() && !node.in.empty()) {
                out.push_back(node.id);
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    }

protected:
    // Node-set mutations. Protected: a node is never added or removed without
    // its step, so only the bridge (which keeps the parallel step array) calls
    // these. Each either throws before mutating anything or completes.

    // Append `id` at the end (its position is the old size()).
    void add(NodeId id) {
        if (contains(id)) {
            throw std::logic_error("plan graph: node " + std::to_string(id) + " is already present");
        }
        if (id >= index_.size()) {
            index_.resize(static_cast<size_t>(id) + 1, -1);
        }
        index_[id] = static_cast<int32_t>(nodes_.size());
        nodes_.push_back(PlanNode{id, {}, {}});
        ++epoch_;
    }

    // Remove a node and every edge touching it. With `heal`, each producer that
    // fed it is wired to each consumer it fed, the new edge taking the role of the
    // consumer-side edge (a role describes the edge's place at its CONSUMER).
    void remove(NodeId id, bool heal) {
        const size_t at = position(id);
        const std::vector<Edge> out = nodes_[at].out;
        const std::vector<Edge> in = nodes_[at].in;
        for (const Edge& edge : out) {
            erase_in(nodes_[position(edge.other)], id);
        }
        for (const Edge& edge : in) {
            erase_out(nodes_[position(edge.other)], id);
        }
        nodes_.erase(nodes_.begin() + static_cast<std::ptrdiff_t>(at));
        index_[id] = -1;
        for (size_t i = at; i < nodes_.size(); ++i) {
            index_[nodes_[i].id] = static_cast<int32_t>(i);
        }
        ++epoch_;
        if (heal) {
            for (const Edge& consumer : out) {
                for (const Edge& producer : in) {
                    add_edge(producer.other, consumer.other, consumer.role);
                }
            }
        }
    }

    // Put a new node between `before` and everything feeding it: every edge into
    // `before` now enters `id` (same role, same stamp), and `id` feeds `before`.
    void insert_before(NodeId id, NodeId before) {
        position(before);  // validate before mutating
        add(id);
        std::vector<Edge> moved;
        moved.swap(nodes_[position(before)].in);
        for (const Edge& edge : moved) {
            for (Edge& out : nodes_[position(edge.other)].out) {
                if (out.other == before) {
                    out.other = id;
                }
            }
        }
        nodes_[position(id)].in = std::move(moved);
        add_edge(id, before, EDGE_NONE);
    }

    // Put a new node between `after` and everything it feeds: `id` takes over
    // `after`'s out-edges (same role, same stamp), and `after` feeds `id`.
    void insert_after(NodeId id, NodeId after) {
        position(after);  // validate before mutating
        add(id);
        std::vector<Edge> moved;
        moved.swap(nodes_[position(after)].out);
        for (const Edge& edge : moved) {
            for (Edge& in : nodes_[position(edge.other)].in) {
                if (in.other == after) {
                    in.other = id;
                }
            }
        }
        nodes_[position(id)].out = std::move(moved);
        add_edge(after, id, EDGE_NONE);
    }

    // Take in every node and edge of `other`, appended in `other`'s node order;
    // the two must share no node.
    void absorb(const PlanTopology& other) {
        for (const PlanNode& node : other.nodes_) {
            if (contains(node.id)) {
                throw std::logic_error(
                    "plan graph: cannot merge plans that share node " + std::to_string(node.id));
            }
        }
        const uint64_t offset = next_stamp_;
        for (const PlanNode& node : other.nodes_) {
            add(node.id);
        }
        for (const PlanNode& node : other.nodes_) {
            PlanNode& mine = nodes_[position(node.id)];
            for (const Edge& edge : node.out) {
                mine.out.push_back(Edge{edge.other, edge.role, edge.stamp + offset});
            }
            for (const Edge& edge : node.in) {
                mine.in.push_back(Edge{edge.other, edge.role, edge.stamp + offset});
            }
        }
        next_stamp_ = offset + other.next_stamp_;
        ++epoch_;
    }

    // Fill the empty `out` with this structure under new ids: position i of `out`
    // is position i of this graph under `ids[i]`. Edges and their stamps carry
    // over, remapped. `out` starts unmodified: epoch 0.
    void remap_into(PlanTopology& out, const std::vector<NodeId>& ids) const {
        for (size_t i = 0; i < nodes_.size(); ++i) {
            out.add(ids[i]);
        }
        for (size_t i = 0; i < nodes_.size(); ++i) {
            PlanNode& target = out.nodes_[i];
            for (const Edge& edge : nodes_[i].out) {
                target.out.push_back(Edge{ids[position(edge.other)], edge.role, edge.stamp});
            }
            for (const Edge& edge : nodes_[i].in) {
                target.in.push_back(Edge{ids[position(edge.other)], edge.role, edge.stamp});
            }
        }
        out.next_stamp_ = next_stamp_;
        out.epoch_ = 0;
    }

    void bump_epoch() { ++epoch_; }

    size_t position(NodeId id) const {
        if (!contains(id)) {
            throw std::logic_error("plan graph: node " + std::to_string(id) + " is not in the plan");
        }
        return static_cast<size_t>(index_[id]);
    }

private:
    std::vector<PlanNode> nodes_;
    std::vector<int32_t> index_;  // NodeId -> position in nodes_, -1 when absent
    uint64_t epoch_ = 0;
    uint64_t next_stamp_ = 0;

    static bool in_order(const Edge& a, const Edge& b) {
        const int ra = edge_role_rank(a.role);
        const int rb = edge_role_rank(b.role);
        return ra != rb ? ra < rb : a.stamp < b.stamp;
    }

    static void insert_in(PlanNode& node, const Edge& edge) {
        auto at = std::upper_bound(node.in.begin(), node.in.end(), edge, in_order);
        node.in.insert(at, edge);
    }

    static void sort_in(PlanNode& node) {
        std::stable_sort(node.in.begin(), node.in.end(), in_order);
    }

    static void erase_in(PlanNode& node, NodeId source) {
        node.in.erase(
            std::remove_if(node.in.begin(), node.in.end(), [&](const Edge& e) { return e.other == source; }),
            node.in.end());
    }

    static void erase_out(PlanNode& node, NodeId target) {
        node.out.erase(
            std::remove_if(node.out.begin(), node.out.end(), [&](const Edge& e) { return e.other == target; }),
            node.out.end());
    }
};

}}  // namespace opteryx::planner
