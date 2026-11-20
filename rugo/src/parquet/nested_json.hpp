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

// A VARIANT path column. The scan is asked for `data->'commit'->>'collection'` by NAME, in the
// notation the JSONL pushdown uses: <base>(->'<key>')*(->|->>)'<key>', where '' inside a key is
// one quote. `->>` (only ever the last step) reads the value as TEXT - a string unquoted,
// anything else its JSON - and `->` as JSON. docs/parquet_variant_design.md §4.3.
struct VariantPath {
    std::string base;
    std::vector<std::string> keys;
    bool text = false;
};
// False when `name` is not in that notation.
bool parse_variant_path(const std::string& name, VariantPath& out);

namespace nested {

struct Plan;   // immutable render plan for one top-level STRUCT / MAP column

// Build the plan for top-level field `name` of `fs`. nullptr + `err` when the
// column is not a STRUCT/MAP this renderer supports (and why).
//
// A `name` no field has, that is a VariantPath over a VARIANT field, gets a PATH plan: it reads
// only the leaves that path needs from THIS file's shredding and yields what `->` / `->>` would
// return from the whole value (SQL NULL where the path leads nowhere or to a JSON null).
// A VariantPath over a field the file holds as a STRING - one JSON document per row, how a
// VARIANT was written before the Parquet VARIANT type - gets a TEXT plan: it reads that one
// chunk, and assemble() parses each row to answer the path (docs/parquet_variant_design.md P5).
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
// byte per row-group row; only rows with a non-zero byte are emitted. `outs` is
// overwritten with one positional byte_array column per plan (one entry per emitted
// row, NULL rows carried in valid_bits). Throws std::runtime_error on any shape it
// cannot render or any inconsistency between the levels and the schema.
//
// `plans` are the plans over ONE top-level field (a VARIANT beside paths into it, several
// paths) and `leaves` the leaves they read between them, in shared_leaf_index() order: the
// field is decoded once, however many columns are read out of it. TEXT plans share more -
// each emitted row is parsed ONCE to answer all of them.
void assemble(const std::vector<std::shared_ptr<const Plan>>& plans,
              const std::vector<DecodedColumn>& leaves,
              const uint8_t* row_mask, std::vector<DecodedColumn>& outs);

// The leaves `plans` read between them: each one's position among the file's leaves of the
// field, in first-use order (plan by plan, each in its own plan order).
std::vector<int32_t> shared_leaf_index(const std::vector<std::shared_ptr<const Plan>>& plans);

}  // namespace nested

// One projected STRUCT/MAP column expanded to its leaf chunks.
struct NestedGroup {
    size_t first = 0;    // index of its first leaf in the expanded column list
    size_t count = 0;    // number of leaves
    // The columns the leaves fold into: each one's projected name, plan and result slot. Every
    // projected column read out of one top-level field is here - a STRUCT/MAP/VARIANT itself,
    // the paths into a VARIANT or a JSON-text column - so the field is read once. The first is
    // at slot `first`; the further ones take slots past the expanded list.
    std::vector<std::string> names;
    std::vector<std::shared_ptr<const nested::Plan>> plans;
    std::vector<uint32_t> slots;
};

// What the pipeline needs to fold leaves back into their group columns.
struct NestedSpec {
    std::vector<NestedGroup> groups;
    // result slot -> projected index. The first slots are the expanded column list's, one each;
    // any after them are the further columns of the groups that yield several.
    std::vector<uint32_t> orig;
    std::vector<uint32_t> slot;   // projected index -> the result slot that holds the column
};

// Resolve a projection against the footer of one file for the given row groups.
//
// For every projected column: a chunk of exactly that name is a plain column (the
// stats are taken as they always were). Otherwise, if chunks named "<name>.*" exist
// it is a STRUCT/MAP group: its leaves replace it in `names`/`stats` and `nested`
// describes the fold. Otherwise, a VariantPath whose base the file holds is a group
// too - the leaves its path plan reads, or, when the file holds the base as JSON text,
// that one chunk. Every projected column over one top-level field shares ONE group, whose
// leaves are read once. A column with none of these is left out, so
// the caller's existing "missing projected column" handling (schema evolution) still fires.
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
