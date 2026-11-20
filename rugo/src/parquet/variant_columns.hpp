#pragma once
// rugo/src/parquet/variant_columns.hpp — a VARIANT column as the writer's leaf columns
// (docs/parquet_variant_design.md §5.4, W1).
//
// The writer knows one ColumnInput per column chunk. A shredded VARIANT is a group of
// chunks — metadata, value, and a {value, typed_value} pair per shredded field — so it
// enters the writer as one ColumnInput per LEAF: an ordinary scalar column plus its
// path_in_schema, its own definition levels and the group elements written before it.
// Chunk index stays equal to ColumnInput index, so row-group slicing, placement, the
// footer and every per-chunk structure are unchanged.

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "_parquet_writer.hpp"
#include "variant_encode.hpp"

namespace rugo_pq_write {

// The shredding a column takes for the whole file, learnt from its first row group.
inline rugo::variant::ShredType infer_variant(const StrSlice *docs, const uint8_t *validity,
                                              size_t rows) {
  return rugo::variant::Shredder::infer(docs, validity, rows);
}

// One VARIANT column of one write. Owns every buffer its leaves' ColumnInputs point
// into, so it must outlive the write of the rows it last appended.
class VariantColumn {
 public:
  VariantColumn(const std::string &name, rugo::variant::ShredType type)
      : shredder_(name, std::move(type)) {}

  // Shred `rows` documents (`validity`: a bit per row, null = every row holds one) and
  // append the column's leaves to `out`, in schema order.
  void append(const StrSlice *docs, const uint8_t *validity, size_t rows, bool dictionary,
              std::vector<ColumnInput> &out) {
    using rugo::variant::ShredLeaf;
    shredder_.encode(docs, validity, rows);
    const std::vector<ShredLeaf> &leaves = shredder_.leaves();
    strs_.assign(leaves.size(), {});
    validity_.assign(leaves.size(), {});

    // Groups by depth: 0 the VARIANT itself, then typed_value groups (odd depth,
    // optional) and shredded fields' groups (even depth, required) alternate. Parity,
    // not the name, says which: a shredded field may itself be named "typed_value".
    std::vector<std::pair<size_t, size_t>> open;   // (leaf in `out`, group in its `groups`)
    const std::vector<std::string> *prev = nullptr;
    for (size_t k = 0; k < leaves.size(); ++k) {
      const ShredLeaf &l = leaves[k];
      const size_t depth = l.path.size() - 1;      // the leaf's parent group is depth - 1
      size_t common = 0;
      if (prev != nullptr)
        while (common < open.size() && common < depth && (*prev)[common] == l.path[common]) ++common;
      open.resize(common);

      ColumnInput c;
      c.path = l.path;
      for (size_t s = 0; s < l.path.size(); ++s) c.name += (s ? "." : "") + l.path[s];
      for (size_t d = common; d < depth; ++d) {
        SchemaGroup g;
        g.name = l.path[d];
        g.top = d == 0;
        g.variant = d == 0;
        g.required = d > 0 && d % 2 == 0;
        if (d > 0) bump(out, c, open.back());
        c.groups.push_back(std::move(g));
        open.emplace_back(out.size(), c.groups.size() - 1);
      }
      bump(out, c, open.back());
      prev = &l.path;

      c.leaf_required = l.kind == ShredLeaf::Metadata;
      c.max_def = l.max_def;
      c.levels = l.def.data();
      std::vector<uint8_t> &bits = validity_[k];
      bits.assign((rows + 7) / 8, 0);
      for (size_t r = 0; r < rows; ++r)
        if (l.def[r] == l.max_def) bits[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
      c.validity = bits.data();
      c.dict_enabled = dictionary && l.kind != ShredLeaf::Bool;

      switch (l.kind) {
      case ShredLeaf::Metadata:
      case ShredLeaf::Value:
      case ShredLeaf::String: {
        std::vector<StrSlice> &s = strs_[k];
        s.resize(rows);
        const uint8_t *base = reinterpret_cast<const uint8_t *>(l.bytes.data());
        for (size_t r = 0; r < rows; ++r)
          s[r] = StrSlice{base + l.offset[r], l.offset[r + 1] - l.offset[r]};
        c.type = PT_BYTE_ARRAY;
        c.is_utf8 = l.kind == ShredLeaf::String;
        c.strs = s.data();
        break;
      }
      case ShredLeaf::Bool:
        c.type = PT_BOOLEAN;
        c.boolean = l.boolean.data();
        break;
      case ShredLeaf::Int64:
        c.type = PT_INT64;
        c.i64 = l.i64.data();
        break;
      case ShredLeaf::Decimal:
        c.type = PT_FLBA;
        c.logical = LK_DECIMAL;
        c.dec_scale = l.scale;
        c.dec_precision = l.precision;
        c.dec_width = l.precision <= 18 ? 8 : 16;
        c.dec_raw = l.precision <= 18 ? reinterpret_cast<const uint8_t *>(l.i64.data())
                                      : reinterpret_cast<const uint8_t *>(l.i128.data());
        break;
      }
      out.push_back(std::move(c));
    }
  }

 private:
  // One more child for the group at `at` — in a leaf already in `out`, or in `c`, the
  // leaf being built (index out.size()).
  static void bump(std::vector<ColumnInput> &out, ColumnInput &c,
                   const std::pair<size_t, size_t> &at) {
    ColumnInput &owner = at.first == out.size() ? c : out[at.first];
    owner.groups[at.second].num_children++;
  }

  rugo::variant::Shredder shredder_;
  std::vector<std::vector<StrSlice>> strs_;
  std::vector<std::vector<uint8_t>> validity_;
};

}  // namespace rugo_pq_write
