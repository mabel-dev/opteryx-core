#pragma once
// rugo/src/avro/avro_schema.hpp — an Avro schema, parsed from its JSON text.
//
// Design: docs/AVRO_READER_DESIGN.md §4. The tree is immutable once parsed and owns
// every node; named types (record, enum, fixed) are registered by FULL name and a
// reference to one resolves to the same node. Pure C++ — no Python.
//
// Refused at parse time (§8), each with the schema path in the message:
//   recursive named types, unions other than two branches with one `null`,
//   `timestamp-nanos`, `local-timestamp-*`, `duration`, decimal precision > 38,
//   an invalid decimal (missing/zero precision, scale > precision, a fixed too
//   narrow for its precision). The spec says an invalid logical type is IGNORED;
//   we refuse it instead (fail fast, docs §18).
// Aliases are not consulted (D5 refuses alias-based resolution).

#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

namespace rugo::avro {

enum class Kind : uint8_t {
    Null, Boolean, Int, Long, Float, Double, Bytes, String,
    Record, Enum, Array, Map, Union, Fixed
};

enum class Logical : uint8_t {
    None,
    Date,             // int: days since epoch
    TimeMillis,       // int
    TimeMicros,       // long
    TimestampMillis,  // long, UTC instant
    TimestampMicros,  // long, UTC instant
    Uuid,             // string, or fixed(16)
    Decimal,          // bytes or fixed; precision/scale on the node
};

struct Node;

struct Field {
    std::string name;
    const Node* type = nullptr;
    int64_t     field_id = -1;  // Iceberg's `field-id` attribute; -1 = absent
    bool        has_default = false;
    std::string default_json;   // the `default` value, as JSON text (has_default only)
};

struct Node {
    Kind        kind = Kind::Null;
    Logical     logical = Logical::None;
    std::string full_name;              // record / enum / fixed only
    std::vector<Field>       fields;    // record
    std::vector<std::string> symbols;   // enum
    const Node* items  = nullptr;       // array
    const Node* values = nullptr;       // map
    std::vector<const Node*> branches;  // union (always 2, one of them null)
    uint32_t    fixed_size = 0;         // fixed
    uint8_t     precision = 0;          // decimal
    uint8_t     scale = 0;              // decimal

    // Union only: index of the `null` branch (0 or 1) and the value branch.
    uint8_t null_branch = 0;
    const Node* value_branch() const { return branches[null_branch == 0 ? 1 : 0]; }
};

class Schema {
public:
    // Parse `json` (an Avro schema). Throws std::runtime_error naming the schema
    // path on anything malformed or refused.
    static Schema parse(const char* json, size_t len);

    const Node* root() const { return root_; }

    Schema() = default;
    Schema(Schema&&) = default;
    Schema& operator=(Schema&&) = default;
    Schema(const Schema&) = delete;
    Schema& operator=(const Schema&) = delete;

private:
    friend struct SchemaParser;
    std::deque<Node> nodes_;   // deque: stable addresses as it grows
    std::unordered_map<std::string, const Node*> named_;
    const Node* root_ = nullptr;
};

const char* kind_name(Kind k);

}  // namespace rugo::avro
