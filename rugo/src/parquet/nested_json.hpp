// Native STRUCT / MAP parquet columns (and a LIST whose elements are STRUCT / MAP, however
// deeply listed) rendered as JSON text.
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
//   * nesting       = arbitrary: STRUCT/MAP/LIST in any combination. A top-level LIST is
//                     this renderer's business only when a STRUCT/MAP sits under it (one JSON
//                     array per row); a LIST of scalars, or of lists of scalars, is the ARRAY
//                     path, not this one (metadata.cpp ListHoldsGroups).
//
// Anything it cannot render — an unsupported leaf type, a MAP with a non-string key,
// a malformed LIST/MAP shape, a NULL map key — is an error, never a NULL.
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "../declared_shape.hpp"   // DeclaredNode: the shape a table declares for a nested column
#include "decode.hpp"
#include "metadata.hpp"

namespace rugo {
namespace nested {

struct Plan;   // immutable render plan for one top-level STRUCT / MAP column

// Build the plan for top-level field `name` of `fs`. nullptr + `err` when the
// column is not a STRUCT/MAP this renderer supports (and why).
//
// `declared`, when given (a catalog table's declared shape for this column, pre-order), is what the
// JSON is rendered AS: the table's member names in the table's order, each matched to the file's field
// of the same field id. A member the file lacks is JSON null; a field the file has and the table no
// longer does is not read. Iceberg names a nested field by id and renames it freely, so this is what
// makes a file written before a rename, an add or a drop render as the table says
// (docs/UUID_AND_NESTED_COLUMNS_DESIGN.md §3.3, option (c)). A file that cannot be matched (no field
// ids, a different kind of thing under an id) is refused with `err` naming the field.
std::shared_ptr<const Plan> build_plan(const FileStats& fs, const std::string& name,
                                       std::string& err,
                                       const std::vector<DeclaredNode>* declared = nullptr);

// Number of leaf column chunks the plan consumes, in plan order.
size_t leaf_count(const Plan& plan);
// Number of leaf chunks the file's column has, and for each leaf the plan reads its position among them.
size_t file_leaf_count(const Plan& plan);
const std::vector<int32_t>& used_leaf_index(const Plan& plan);

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
//
// `declared`, parallel to `projected` (or null), holds each column's declared shape; an empty entry is
// "none declared". See build_plan.
bool resolve_projection(const FileStats& fs, const std::vector<int>& rg_idxs,
                        const std::vector<std::string>& projected,
                        std::vector<std::string>& names,
                        std::vector<std::vector<ColumnStats>>& stats,
                        std::shared_ptr<const NestedSpec>& nested, std::string& err,
                        const std::vector<std::vector<DeclaredNode>>* declared = nullptr);

// Footer-gate probe: can every leaf of group `name` be rendered - and, with `declared`, matched to
// the table's shape? (No decode.)
bool group_renderable(const FileStats& fs, const std::string& name, std::string& err,
                      const std::vector<DeclaredNode>* declared = nullptr);

}  // namespace rugo
