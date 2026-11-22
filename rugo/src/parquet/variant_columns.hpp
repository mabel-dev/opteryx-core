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

#include <algorithm>
#include <cstdint>
#include <future>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "../_writer_pool.hpp"   // the row chunks encode as tasks on the shared writer pool
#include "_parquet_writer.hpp"
#include "variant_encode.hpp"

namespace rugo_pq_write {

// The shredding a column takes for the whole file, learnt from its first row group.
inline rugo::variant::ShredType infer_variant(const StrSlice *docs, const uint8_t *validity,
                                              size_t rows) {
  return rugo::variant::Shredder::infer(docs, validity, rows);
}

// Rows per encode task (docs/parquet_variant_parallel_encode_design.md §6 D-a). Only the
// balance of the tasks depends on it; the leaves are the same for any value.
constexpr size_t kVariantEncodeChunkRows = 4096;

// One VARIANT column of one write. Owns every buffer its leaves' ColumnInputs point
// into, so it must outlive the write of the rows it last appended.
class VariantColumn {
 public:
  VariantColumn(const std::string &name, rugo::variant::ShredType type)
      : name_(name), type_(std::move(type)) {}

  // Shred `rows` documents (`validity`: a bit per row, null = every row holds one) and
  // append the column's leaves to `out`, in schema order.
  void append(const StrSlice *docs, const uint8_t *validity, size_t rows, bool dictionary,
              std::vector<ColumnInput> &out) {
    using rugo::variant::ShredLeaf;
    encode(docs, validity, rows);
    const std::vector<ShredLeaf> &leaves = shredders_[0].leaves();
    strs_.assign(leaves.size(), {});
    validity_.assign(leaves.size(), {});
    def_.assign(leaves.size(), {});
    i64_.assign(leaves.size(), {});
    i128_.assign(leaves.size(), {});
    bool_.assign(leaves.size(), {});

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
      const uint8_t *def = joined(k, &ShredLeaf::def, def_[k]);
      c.levels = def;
      std::vector<uint8_t> &bits = validity_[k];
      bits.assign((rows + 7) / 8, 0);
      for (size_t r = 0; r < rows; ++r)
        if (def[r] == l.max_def) bits[r >> 3] |= static_cast<uint8_t>(1u << (r & 7));
      c.validity = bits.data();
      c.dict_enabled = dictionary && l.kind != ShredLeaf::Bool;

      switch (l.kind) {
      case ShredLeaf::Metadata:
      case ShredLeaf::Value:
      case ShredLeaf::String: {
        // Each row's slice points into its own chunk's bytes: the payload is never copied.
        std::vector<StrSlice> &s = strs_[k];
        s.resize(rows);
        size_t r = 0;
        for (const rugo::variant::Shredder &part : shredders_) {
          const ShredLeaf &pl = part.leaves()[k];
          const uint8_t *base = reinterpret_cast<const uint8_t *>(pl.bytes.data());
          for (size_t i = 0; i + 1 < pl.offset.size(); ++i, ++r)
            s[r] = StrSlice{base + pl.offset[i], pl.offset[i + 1] - pl.offset[i]};
        }
        c.type = PT_BYTE_ARRAY;
        c.is_utf8 = l.kind == ShredLeaf::String;
        c.strs = s.data();
        break;
      }
      case ShredLeaf::Bool:
        c.type = PT_BOOLEAN;
        c.boolean = joined(k, &ShredLeaf::boolean, bool_[k]);
        break;
      case ShredLeaf::Int64:
        c.type = PT_INT64;
        c.i64 = joined(k, &ShredLeaf::i64, i64_[k]);
        break;
      case ShredLeaf::Decimal:
        c.type = PT_FLBA;
        c.logical = LK_DECIMAL;
        c.dec_scale = l.scale;
        c.dec_precision = l.precision;
        c.dec_width = l.precision <= 18 ? 8 : 16;
        c.dec_raw = l.precision <= 18
                        ? reinterpret_cast<const uint8_t *>(joined(k, &ShredLeaf::i64, i64_[k]))
                        : reinterpret_cast<const uint8_t *>(joined(k, &ShredLeaf::i128, i128_[k]));
        break;
      }
      out.push_back(std::move(c));
    }
  }

 private:
  // Encode the rows in chunks of kVariantEncodeChunkRows, one Shredder each, as tasks on the
  // writer pool. One chunk (or a one-thread pool) encodes on this thread, as before.
  void encode(const StrSlice *docs, const uint8_t *validity, size_t rows) {
    const size_t want = std::max<size_t>(1, (rows + kVariantEncodeChunkRows - 1) / kVariantEncodeChunkRows);
    size_t chunks = std::min(want, rugo_writer::pool_width());
    const size_t per = std::max<size_t>(1, (rows + chunks - 1) / chunks);
    chunks = std::max<size_t>(1, (rows + per - 1) / per);   // every chunk holds a row
    shredders_.clear();
    shredders_.reserve(chunks);
    for (size_t c = 0; c < chunks; ++c) shredders_.emplace_back(name_, type_);
    auto run = [&](size_t c) {
      shredders_[c].encode(docs, validity, c * per, std::min(rows, (c + 1) * per));
    };
    if (chunks == 1) {
      run(0);
    } else {
      BS::thread_pool<> &pool = rugo_writer::pool();
      std::vector<std::future<void>> futs;
      futs.reserve(chunks);
      for (size_t c = 0; c < chunks; ++c) futs.push_back(pool.submit_task([&run, c]() { run(c); }));
      // Every task writes its own Shredder: all must finish before any is read or freed. Then
      // the lowest chunk's failure is raised, the one a serial encode would have met first.
      for (std::future<void> &f : futs) f.wait();
      for (std::future<void> &f : futs) f.get();
    }
    // The 4 GiB refusal is per row group, as it was with one Shredder.
    for (size_t k = 0; k < shredders_[0].leaves().size(); ++k) {
      size_t bytes = 0;
      for (const rugo::variant::Shredder &part : shredders_) bytes += part.leaves()[k].bytes.size();
      if (bytes > UINT32_MAX)
        throw std::runtime_error("VARIANT column '" + name_ +
                                 "': a row group holds more than 4 GiB in one of its columns");
    }
  }

  // Leaf `k`'s per-row `field` across the chunks, in row order: the one chunk's own array, or
  // the chunks' arrays joined into `dst`.
  template <typename T>
  const T *joined(size_t k, std::vector<T> rugo::variant::ShredLeaf::*field, std::vector<T> &dst) {
    if (shredders_.size() == 1) return (shredders_[0].leaves()[k].*field).data();
    dst.clear();
    for (const rugo::variant::Shredder &part : shredders_) {
      const std::vector<T> &v = part.leaves()[k].*field;
      dst.insert(dst.end(), v.begin(), v.end());
    }
    return dst.data();
  }

  // One more child for the group at `at` — in a leaf already in `out`, or in `c`, the
  // leaf being built (index out.size()).
  static void bump(std::vector<ColumnInput> &out, ColumnInput &c,
                   const std::pair<size_t, size_t> &at) {
    ColumnInput &owner = at.first == out.size() ? c : out[at.first];
    owner.groups[at.second].num_children++;
  }

  std::string name_;
  rugo::variant::ShredType type_;
  std::vector<rugo::variant::Shredder> shredders_;   // one per chunk of rows, in row order
  std::vector<std::vector<StrSlice>> strs_;
  std::vector<std::vector<uint8_t>> validity_;
  // Per leaf, the chunks' per-row arrays joined (unused when there is one chunk).
  std::vector<std::vector<uint8_t>> def_;
  std::vector<std::vector<int64_t>> i64_;
  std::vector<std::vector<__int128>> i128_;
  std::vector<std::vector<uint8_t>> bool_;
};

}  // namespace rugo_pq_write
