// test_patch.cpp — DROP and RENAME COLUMN over a real .skene file.
//
// Every file here is written by skene's own writer and read back by skene's own
// reader, which validates magic, versions, every checksum and the structural
// consistency of both footers before it interprets anything. So "the reader
// accepted it" is a strong statement, not a smoke test.
//
// The property that matters, and that only a byte comparison can catch: the
// columns that survive are COPIED, not re-encoded. A patcher that decoded and
// rewrote them would return identical VALUES and different BYTES, and would
// pass every value assertion below.

#include "skene/patch.h"

#include <cstring>
#include <string>
#include <vector>

#include "build_vectors.h"
#include "skene/reader.h"
#include "skene/writer.h"
#include "harness.h"

using namespace skene;

namespace {

// A four-column file: two plain ints, a string (arena + selection sections) and
// a constant. Enough shapes that a section-index slip shows up.
std::vector<uint8_t> make_file(size_t row_groups = 1) {
    WriteOptions options;
    options.writer_tag = "skene-test/patch";

    std::vector<uint8_t> out;
    FileWriter writer;
    CHECK(writer.begin(options, &out).is_ok());

    for (size_t g = 0; g < row_groups; ++g) {
        const int64_t base = static_cast<int64_t>(g) * 100;
        CxxMorsel morsel;
        morsel.names = {"id", "amount", "label", "flag"};
        morsel.columns.push_back(
            skene_test::dense_column<int64_t>({base + 1, base + 2, base + 3, base + 4},
                                              DRAKEN_INT64));
        morsel.columns.push_back(
            skene_test::dense_column<int64_t>({base + 10, base + 20, base + 30, base + 40},
                                              DRAKEN_INT64));
        morsel.columns.push_back(
            skene_test::string_column({"alpha", "beta", "alpha", "gamma"}));
        morsel.columns.push_back(
            skene_test::dense_column<int64_t>({7, 7, 7, 7}, DRAKEN_INT64));
        CHECK(writer.add_row_group(morsel).is_ok());
    }
    CHECK(writer.finish().is_ok());
    return out;
}

std::vector<std::string> column_names(const std::vector<uint8_t>& file) {
    FileMetadata meta;
    CHECK(read_metadata(file.data(), file.size(), &meta).is_ok());
    std::vector<std::string> names;
    for (const ColumnSchema& column : meta.columns) names.push_back(column.name);
    return names;
}

// Every row group's DATA + INDEX extent concatenated — the encoded sections,
// and NOT the row group footers.
//
// Spanning [head, file footer) instead would sweep the row group footers in
// too, and those legitimately change on a rename: it is their column directory
// that carries the name. Comparing that span would report a rename as touching
// data when it had not.
std::vector<uint8_t> data_region(const std::vector<uint8_t>& file) {
    FileMetadata meta;
    CHECK(read_metadata(file.data(), file.size(), &meta).is_ok());
    std::vector<uint8_t> region;
    for (const RowGroupSummary& rg : meta.row_groups)
        region.insert(region.end(), file.begin() + rg.byte_offset,
                      file.begin() + rg.byte_offset + rg.byte_bytes);
    return region;
}

// Read one row group back through the real reader, as int64 values.
std::vector<int64_t> int_values(const std::vector<uint8_t>& file, uint32_t row_group,
                                const std::string& name) {
    RowGroupMetadata rg;
    CHECK(read_row_group_metadata(file.data(), file.size(), row_group, &rg).is_ok());
    for (const ColumnMetadata& column : rg.columns) {
        if (column.name != name) continue;
        CxxMorsel morsel;
        CHECK(read_morsel(file.data(), file.size(), row_group, &morsel).is_ok());
        for (size_t i = 0; i < morsel.names.size(); ++i) {
            if (morsel.names[i] != name) continue;
            const DrakenVector& v = morsel.columns[i].view;
            std::vector<int64_t> values;
            for (uint32_t r = 0; r < v.length; ++r)
                values.push_back(static_cast<const int64_t*>(v.data)[v.selection[r]]);
            return values;
        }
    }
    CHECK(false);  // column not found
    return {};
}

}  // namespace

int main() {
    // ── RENAME ──────────────────────────────────────────────────────────────
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        Status s = patch_columns(source.data(), source.size(), {}, {{"amount", "total"}},
                                 &patched);
        CHECK(s.is_ok());

        const std::vector<std::string> names = column_names(patched);
        CHECK(names.size() == 4);
        CHECK(names[1] == "total");
        CHECK(names[0] == "id" && names[2] == "label" && names[3] == "flag");

        // THE property: a rename is a footer edit. Not one data byte moves.
        CHECK(data_region(patched) == data_region(source));

        CHECK(int_values(patched, 0, "total") == std::vector<int64_t>({10, 20, 30, 40}));
        CHECK(int_values(patched, 0, "id") == std::vector<int64_t>({1, 2, 3, 4}));
    }

    // A rename to a longer name still moves no data — names live only in
    // footers, so only footer bytes shift.
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {},
                            {{"id", "a_very_much_longer_column_name"}}, &patched).is_ok());
        CHECK(data_region(patched) == data_region(source));
        CHECK(column_names(patched)[0] == "a_very_much_longer_column_name");
        CHECK(patched.size() > source.size());
    }

    // ── DROP ────────────────────────────────────────────────────────────────
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {"flag"}, {}, &patched).is_ok());

        const std::vector<std::string> names = column_names(patched);
        CHECK(names == std::vector<std::string>({"id", "amount", "label"}));

        // Dropping the LAST column leaves every earlier section exactly where it
        // was, so the new data region is a byte-for-byte PREFIX of the old.
        const std::vector<uint8_t> before = data_region(source);
        const std::vector<uint8_t> after  = data_region(patched);
        CHECK(after.size() < before.size());
        CHECK(std::memcmp(before.data(), after.data(), after.size()) == 0);

        CHECK(int_values(patched, 0, "id") == std::vector<int64_t>({1, 2, 3, 4}));
        CHECK(int_values(patched, 0, "amount") == std::vector<int64_t>({10, 20, 30, 40}));
        CHECK(patched.size() < source.size());
    }

    // Dropping from the MIDDLE: the columns after it move, and must still read.
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {"amount"}, {}, &patched).is_ok());
        CHECK(column_names(patched) == std::vector<std::string>({"id", "label", "flag"}));
        CHECK(int_values(patched, 0, "id") == std::vector<int64_t>({1, 2, 3, 4}));
        CHECK(int_values(patched, 0, "flag") == std::vector<int64_t>({7, 7, 7, 7}));
    }

    // Dropping the string column exercises a multi-section column (arena and
    // selection alongside the data), where a section-count slip would misalign
    // every later column's directory entry.
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {"label"}, {}, &patched).is_ok());
        CHECK(column_names(patched) == std::vector<std::string>({"id", "amount", "flag"}));
        CHECK(int_values(patched, 0, "flag") == std::vector<int64_t>({7, 7, 7, 7}));
    }

    // Several at once, down to a single surviving column.
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {"amount", "label", "flag"}, {},
                            &patched).is_ok());
        CHECK(column_names(patched) == std::vector<std::string>({"id"}));
        CHECK(int_values(patched, 0, "id") == std::vector<int64_t>({1, 2, 3, 4}));
    }

    // ── the two composed ────────────────────────────────────────────────────
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {"label"},
                            {{"amount", "total"}}, &patched).is_ok());
        CHECK(column_names(patched) == std::vector<std::string>({"id", "total", "flag"}));
        CHECK(int_values(patched, 0, "total") == std::vector<int64_t>({10, 20, 30, 40}));
    }

    // Renaming onto the name of a column being dropped is legal — the check is
    // against what SURVIVES, not against what the file happened to contain.
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {"label"},
                            {{"amount", "label"}}, &patched).is_ok());
        CHECK(column_names(patched) == std::vector<std::string>({"id", "label", "flag"}));
        CHECK(int_values(patched, 0, "label") == std::vector<int64_t>({10, 20, 30, 40}));
    }

    // ── multiple row groups ─────────────────────────────────────────────────
    {
        const std::vector<uint8_t> source = make_file(/*row_groups=*/3);
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {"flag"},
                            {{"amount", "total"}}, &patched).is_ok());

        FileMetadata meta;
        CHECK(read_metadata(patched.data(), patched.size(), &meta).is_ok());
        CHECK(meta.row_groups.size() == 3);
        CHECK(meta.row_count == 12);

        // Every row group has to be patched, and its first_row chain kept — a
        // single-row-group implementation passes everything above and fails here.
        for (uint32_t g = 0; g < 3; ++g) {
            const int64_t base = static_cast<int64_t>(g) * 100;
            CHECK(int_values(patched, g, "id") ==
                  std::vector<int64_t>({base + 1, base + 2, base + 3, base + 4}));
            CHECK(int_values(patched, g, "total") ==
                  std::vector<int64_t>({base + 10, base + 20, base + 30, base + 40}));
        }
        CHECK(data_region(patched).size() < data_region(source).size());
    }

    // ── refusals ────────────────────────────────────────────────────────────
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;

        CHECK(!patch_columns(source.data(), source.size(), {"nope"}, {}, &patched).is_ok());
        CHECK(!patch_columns(source.data(), source.size(), {}, {{"nope", "x"}}, &patched).is_ok());
        // every column dropped
        CHECK(!patch_columns(source.data(), source.size(),
                             {"id", "amount", "label", "flag"}, {}, &patched).is_ok());
        // a rename that would collide with a surviving column
        CHECK(!patch_columns(source.data(), source.size(), {}, {{"amount", "id"}},
                             &patched).is_ok());
        // no changes at all
        CHECK(!patch_columns(source.data(), source.size(), {}, {}, &patched).is_ok());
        // not a skene file
        const std::vector<uint8_t> junk(64, 0);
        CHECK(!patch_columns(junk.data(), junk.size(), {"id"}, {}, &patched).is_ok());
    }

    // A corrupted source is refused rather than propagated into a file whose
    // own checksums would then be valid over damaged bytes.
    {
        std::vector<uint8_t> source = make_file();
        source[kFileHeadBytes + 3] ^= 0xFFu;  // flip a bit inside a data section
        std::vector<uint8_t> patched;
        const Status s = patch_columns(source.data(), source.size(), {"flag"}, {}, &patched);
        // The section checksums are copied verbatim, so the damage is caught on
        // READ of the patched file rather than during the patch itself.
        if (s.is_ok()) {
            CxxMorsel morsel;
            CHECK(!read_morsel(patched.data(), patched.size(), 0, &morsel).is_ok());
        }
    }

    return skene_test::summary("test_patch");
}
