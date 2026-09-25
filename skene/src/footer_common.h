#pragma once
// Internal: footer records whose bytes are the SAME in v2 and v3 — the schema
// directory (FORMAT.md §5.4) and the cluster spec (§5.5) — parsed once for both
// readers, plus the bounds-checked cursor every footer walk goes through.
// Like chunk_decode.h, this forks the day a version changes these bytes.

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "skene/format.h"
#include "skene/reader.h"
#include "skene/status.h"

namespace skene {
namespace footer {

// Every read is checked against the footer extent. The footer is attacker- and
// corruption-adjacent data whose own checksum has passed, but a checksum proves
// the bytes are the bytes that were written — not that the writer was sane. A
// length field can still say "a billion columns follow".
class Cursor {
  public:
    Cursor(const uint8_t* begin, size_t bytes) : p_(begin), end_(begin + bytes) {}

    bool take(void* dst, size_t n) {
        if (static_cast<size_t>(end_ - p_) < n) return false;
        std::memcpy(dst, p_, n);
        p_ += n;
        return true;
    }

    const uint8_t* raw(size_t n) {
        if (static_cast<size_t>(end_ - p_) < n) return nullptr;
        const uint8_t* result = p_;
        p_ += n;
        return result;
    }

    size_t remaining() const { return static_cast<size_t>(end_ - p_); }

  private:
    const uint8_t* p_;
    const uint8_t* end_;
};

struct ParsedSchema {
    SchemaEntryHead           head{};
    std::string               name;
    LogicalTypeDescriptor     logical{};
    std::vector<ParsedSchema> children;
};

// One schema entry and its children, depth first.
Status parse_schema(Cursor& cursor, ParsedSchema* out, int depth);

// Column nodes in `node`'s subtree, itself included.
uint32_t count_schema_nodes(const ParsedSchema& node);

// The cluster spec, validated structurally: reserved bytes zero, key ordinals
// inside the top-level schema, nulls_first consistent with draken's rule.
Status parse_cluster_spec(Cursor& cursor, uint32_t column_count,
                          std::vector<SortKey>* out);

void fill_schema(const ParsedSchema& parsed, ColumnSchema* out);

}  // namespace footer
}  // namespace skene
