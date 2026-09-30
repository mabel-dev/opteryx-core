// Native STRUCT / MAP parquet columns rendered as JSON text.
//
// A parquet STRUCT or MAP is stored as several leaf column chunks ("s.a",
// "s.b.c", "m.key_value.key", ...), each carrying repetition/definition levels.
// There is no decoder for the group as a whole, so the scan expands one projected
// group column into its leaves (resolve_projection), decodes each leaf with its
// levels, and assemble() walks the schema subtree in lockstep over those leaves
// (Dremel record assembly) writing ONE JSON document per row. The result is an
// ordinary byte_array DecodedColumn, so everything downstream — direct string
// build, pool serialisation, the engine — sees a plain NVARCHAR column and never
// learns the column was nested.
//
// Rendering rules (architect rulings):
//   * member order  = the file's schema order (cheapest: no sorting)
//   * NULL field    = JSON `null`; a NULL top-level group = SQL NULL; a NULL group
//                     nested inside another = `null`
//   * MAP           = a JSON object; the key must be a string — a non-string key
//                     type is refused when the plan is built (loud, names the column)
//   * NaN / +-Inf   = `null` (the one rule every JSON emitter in the engine shares)
//   * DECIMAL       = a bare JSON number (exact, it is text)
//   * DATE/TIMESTAMP/TIME = quoted ISO-8601, the shared fmt_*_quoted renderers
//   * BINARY        = quoted base64 (mabel bintob64)
//   * STRING        = JSON-escaped UTF-8 (shared json_string)
//   * nesting       = arbitrary: STRUCT/MAP/LIST in any combination
//
// Anything it cannot render — an unsupported leaf type, a MAP with a non-string key,
// a malformed LIST/MAP shape, a NULL map key — is an error, never a NULL.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "decode.hpp"
#include "metadata.hpp"

namespace rugo {
namespace nested {

struct Plan;   // immutable render plan for one top-level STRUCT / MAP column

// Build the plan for top-level field `name` of `fs`. nullptr + `err` when the
// column is not a STRUCT/MAP this renderer supports (and why).
std::shared_ptr<const Plan> build_plan(const FileStats& fs, const std::string& name,
                                       std::string& err);

// Number of leaf column chunks the plan consumes, in schema (DFS) order.
size_t leaf_count(const Plan& plan);

// Assemble one JSON document per row from the decoded leaves (in schema order,
// each decoded UNMASKED with its rep/def levels). `row_mask`, when non-null, is one
// byte per row-group row; only rows with a non-zero byte are emitted. `out` is
// overwritten with a positional byte_array column (one entry per emitted row, NULL
// rows carried in valid_bits). Throws std::runtime_error on any shape it cannot
// render or any inconsistency between the levels and the schema.
void assemble(const Plan& plan, const std::vector<DecodedColumn>& leaves,
              const uint8_t* row_mask, DecodedColumn& out);

}  // namespace nested

// One projected STRUCT/MAP column expanded to its leaf chunks.
struct NestedGroup {
    std::string name;    // the projected column's name
    size_t first = 0;    // index of its first leaf in the expanded column list
    size_t count = 0;    // number of leaves
    std::shared_ptr<const nested::Plan> plan;
};

// What the pipeline needs to fold leaves back into their group columns.
struct NestedSpec {
    std::vector<NestedGroup> groups;
    std::vector<uint32_t> orig;   // expanded index -> projected index
};

// Resolve a projection against the footer of one file for the given row groups.
//
// For every projected column: a chunk of exactly that name is a plain column (the
// stats are taken as they always were). Otherwise, if chunks named "<name>.*" exist
// it is a STRUCT/MAP group: its leaves replace it in `names`/`stats` and `nested`
// describes the fold. A column with neither is left out, so the caller's existing
// "missing projected column" handling (schema evolution) still fires.
//
// Returns false with `err` when a group cannot be rendered. When no projected
// column is a group, `nested` is null and `names` == `projected`.
bool resolve_projection(const FileStats& fs, const std::vector<int>& rg_idxs,
                        const std::vector<std::string>& projected,
                        std::vector<std::string>& names,
                        std::vector<std::vector<ColumnStats>>& stats,
                        std::shared_ptr<const NestedSpec>& nested, std::string& err);

// Footer-gate probe: can every leaf of group `name` be rendered? (No decode.)
bool group_renderable(const FileStats& fs, const std::string& name, std::string& err);

}  // namespace rugo
