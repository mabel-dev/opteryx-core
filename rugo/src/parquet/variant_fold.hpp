#pragma once
// rugo/src/parquet/variant_fold.hpp — the standalone reader's VARIANT columns
// (docs/parquet_variant_design.md §5.4, W3).
//
// A Parquet VARIANT is a group of leaf chunks. The engine's scan folds them back into
// one JSON-text column (nested_json.hpp); this is the same fold for rugo's own
// read_parquet, so a VARIANT reads as its JSON text there too. Only VARIANT columns are
// folded: every other projected name decodes exactly as DecodeRowGroupColumns does.

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "decode.hpp"
#include "metadata.hpp"
#include "nested_json.hpp"

namespace rugo {

// assemble() gives every row an entry, as the engine's string build reads them; the
// standalone materializer (str_resolve) reads entries for valid rows only.
inline void keep_valid_entries(DecodedColumn &c) {
  if (c.valid_bits.empty()) return;
  size_t k = 0;
  for (size_t i = 0; i < static_cast<size_t>(c.num_rows); ++i) {
    if (!((c.valid_bits[i >> 3] >> (i & 7)) & 1)) continue;
    c.string_offsets[k] = c.string_offsets[i];
    c.string_lens[k] = c.string_lens[i];
    ++k;
  }
  c.string_offsets.resize_uninit(k);
  c.string_lens.resize_uninit(k);
}

// Row group `rg` of the projection `names`, where `is_variant[i]` marks a VARIANT field:
// one decoded column per name, in order (an absent plain column stays absent, as
// DecodeRowGroupColumns leaves it).
inline std::vector<DecodedColumn> DecodeRowGroupVariants(const uint8_t *data, size_t size,
                                                         const FileStats &fs,
                                                         const std::vector<std::string> &names,
                                                         const std::vector<uint8_t> &is_variant,
                                                         int rg) {
  std::vector<std::string> plain, variants;
  std::vector<size_t> plain_at, variant_at;
  for (size_t i = 0; i < names.size(); ++i) {
    if (is_variant[i]) { variants.push_back(names[i]); variant_at.push_back(i); }
    else { plain.push_back(names[i]); plain_at.push_back(i); }
  }
  const RowGroupStats &group = fs.row_groups[static_cast<size_t>(rg)];
  std::vector<DecodedColumn> out(names.size());
  std::vector<DecodedColumn> decoded = DecodeRowGroupColumns(data, size, plain, group, rg);
  for (size_t i = 0; i < plain.size(); ++i) out[plain_at[i]] = std::move(decoded[i]);
  if (variants.empty()) return out;

  std::vector<std::string> leaves;
  std::vector<std::vector<ColumnStats>> stats;
  std::shared_ptr<const NestedSpec> spec;
  std::string err;
  if (!resolve_projection(fs, {rg}, variants, leaves, stats, spec, err))
    throw std::runtime_error("row group " + std::to_string(rg) + ": " + err);
  if (!spec || spec->groups.size() != variants.size())
    throw std::runtime_error("row group " + std::to_string(rg) +
                             ": a VARIANT column was not found among the file's leaves");
  decoded = DecodeRowGroupColumns(data, size, leaves, group, rg);
  for (const NestedGroup &g : spec->groups) {
    std::vector<DecodedColumn> group_leaves(std::make_move_iterator(decoded.begin() + g.first),
                                            std::make_move_iterator(decoded.begin() + g.first + g.count));
    std::vector<DecodedColumn> folded;
    nested::assemble(g.plans, group_leaves, nullptr, folded, /*leaf_passthrough=*/true);
    for (size_t j = 0; j < folded.size(); ++j) {
      folded[j].logical_type = "varchar";   // JSON text
      keep_valid_entries(folded[j]);
      out[variant_at[spec->orig[g.slots[j]]]] = std::move(folded[j]);
    }
  }
  return out;
}

}  // namespace rugo
