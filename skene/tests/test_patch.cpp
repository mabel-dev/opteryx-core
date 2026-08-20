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

// A donor: a one-column, one-row file, exactly as the caller of patch_columns
// would build one — through skene's own writer.
template <typename T>
std::vector<uint8_t> donor(const std::string& name, T value, DrakenType type) {
    WriteOptions options;
    options.writer_tag = "skene-test/donor";
    CxxMorsel morsel;
    morsel.names = {name};
    morsel.columns.push_back(skene_test::dense_column<T>({value}, type));
    std::vector<uint8_t> out;
    CHECK(write_morsel(morsel, options, &out).is_ok());
    return out;
}

// A donor whose single row is NULL — the fill for an ADD with no default.
std::vector<uint8_t> null_donor(const std::string& name, DrakenType type) {
    WriteOptions options;
    options.writer_tag = "skene-test/donor";
    CxxMorsel morsel;
    morsel.names = {name};
    morsel.columns.push_back(skene_test::dense_column<int64_t>({0}, type, {false}));
    std::vector<uint8_t> out;
    CHECK(write_morsel(morsel, options, &out).is_ok());
    return out;
}

// Which rows of a column read back as NULL.
std::vector<bool> null_mask(const std::vector<uint8_t>& file, uint32_t row_group,
                            const std::string& name) {
    CxxMorsel morsel;
    CHECK(read_morsel(file.data(), file.size(), row_group, &morsel).is_ok());
    for (size_t i = 0; i < morsel.names.size(); ++i) {
        if (morsel.names[i] != name) continue;
        const DrakenVector& v = morsel.columns[i].view;
        std::vector<bool> nulls;
        for (uint32_t r = 0; r < v.length; ++r)
            nulls.push_back(v.validity != nullptr &&
                            ((v.validity[r >> 3] >> (r & 7)) & 1u) == 0u);
        return nulls;
    }
    CHECK(false);
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
        // v2 aligns section bodies to kSectionAlign, so the first DATA byte is
        // at 64, not right after the 16-byte head — a flip in the padding gap
        // would be dead bytes and prove nothing.
        source[kSectionAlign + 3] ^= 0xFFu;  // flip a bit inside a data section
        std::vector<uint8_t> patched;
        const Status s = patch_columns(source.data(), source.size(), {"flag"}, {}, &patched);
        // The section checksums are copied verbatim, so the damage is caught on
        // READ of the patched file rather than during the patch itself.
        if (s.is_ok()) {
            CxxMorsel morsel;
            CHECK(!read_morsel(patched.data(), patched.size(), 0, &morsel).is_ok());
        }
    }

    // ── ADD ─────────────────────────────────────────────────────────────────
    //
    // skene stores a constant column as ONE value with no selection section, so
    // a backfilled column costs the same handful of bytes whatever the row
    // count. These pin that, not just the values.
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {}, {},
                            {donor<int64_t>("added", 99, DRAKEN_INT64)}, &patched).is_ok());

        CHECK(column_names(patched) ==
              std::vector<std::string>({"id", "amount", "label", "flag", "added"}));
        CHECK(int_values(patched, 0, "added") == std::vector<int64_t>({99, 99, 99, 99}));
        // every existing column untouched
        CHECK(int_values(patched, 0, "id") == std::vector<int64_t>({1, 2, 3, 4}));
        CHECK(int_values(patched, 0, "amount") == std::vector<int64_t>({10, 20, 30, 40}));

        // The existing sections keep their bytes AND their order, so the old
        // data region is a prefix of the new one.
        const std::vector<uint8_t> before = data_region(source);
        const std::vector<uint8_t> after  = data_region(patched);
        CHECK(after.size() > before.size());
        CHECK(std::memcmp(before.data(), after.data(), before.size()) == 0);
    }

    // NULL fill: the donor's own row is null, and that is the only signal.
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {}, {},
                            {null_donor("note", DRAKEN_INT64)}, &patched).is_ok());
        CHECK(null_mask(patched, 0, "note") == std::vector<bool>({true, true, true, true}));
        CHECK(null_mask(patched, 0, "id") == std::vector<bool>({false, false, false, false}));
    }

    // A constant column costs a constant number of bytes: adding one to a file
    // with 4 rows and to a file with 4x the row groups must cost the same per
    // row group, because only `length` changes.
    {
        const std::vector<uint8_t> one = make_file(1);
        const std::vector<uint8_t> three = make_file(3);
        std::vector<uint8_t> p1, p3;
        CHECK(patch_columns(one.data(), one.size(), {}, {},
                            {donor<int64_t>("k", 5, DRAKEN_INT64)}, &p1).is_ok());
        CHECK(patch_columns(three.data(), three.size(), {}, {},
                            {donor<int64_t>("k", 5, DRAKEN_INT64)}, &p3).is_ok());
        const size_t grew_1 = data_region(p1).size() - data_region(one).size();
        const size_t grew_3 = data_region(p3).size() - data_region(three).size();
        // v2 alignment makes the growth per row group "one aligned section of
        // one int64", not a byte-exact 8: each added data section starts at a
        // kSectionAlign boundary, so up to kSectionAlign - 1 bytes of padding
        // join the 8 value bytes. What must stay true is the SHAPE of the cost:
        // per row group and independent of the row count.
        // No lower bound: a row group whose region already ended short of an
        // alignment boundary can absorb the 8 value bytes into what would have
        // been padding, so a per-row-group growth of less than another file's
        // is possible — the ceiling is the invariant.
        CHECK(grew_1 <= kSectionAlign + 8);
        CHECK(grew_3 <= 3 * (kSectionAlign + 8));
    }

    // Several at once, and composed with the other operations.
    {
        const std::vector<uint8_t> source = make_file(2);
        std::vector<uint8_t> patched;
        std::vector<DonorFile> add;
        add.push_back(donor<int64_t>("one", 1, DRAKEN_INT64));
        add.push_back(null_donor("two", DRAKEN_INT64));
        CHECK(patch_columns(source.data(), source.size(), {"label"},
                            {{"amount", "total"}}, add, &patched).is_ok());

        CHECK(column_names(patched) ==
              std::vector<std::string>({"id", "total", "flag", "one", "two"}));
        for (uint32_t g = 0; g < 2; ++g) {
            const int64_t base = static_cast<int64_t>(g) * 100;
            CHECK(int_values(patched, g, "one") == std::vector<int64_t>({1, 1, 1, 1}));
            CHECK(int_values(patched, g, "total") ==
                  std::vector<int64_t>({base + 10, base + 20, base + 30, base + 40}));
            CHECK(null_mask(patched, g, "two") ==
                  std::vector<bool>({true, true, true, true}));
        }
    }

    // A string constant carries slots and an arena, so it exercises the
    // multi-section donor path.
    {
        const std::vector<uint8_t> source = make_file();
        WriteOptions options;
        options.writer_tag = "skene-test/donor";
        CxxMorsel morsel;
        morsel.names = {"tag"};
        morsel.columns.push_back(skene_test::string_column({"backfilled"}));
        std::vector<uint8_t> d;
        CHECK(write_morsel(morsel, options, &d).is_ok());

        std::vector<uint8_t> patched;
        CHECK(patch_columns(source.data(), source.size(), {}, {}, {d}, &patched).is_ok());
        CHECK(column_names(patched).back() == "tag");

        CxxMorsel back;
        CHECK(read_morsel(patched.data(), patched.size(), 0, &back).is_ok());
        CHECK(back.num_rows() == 4);
    }

    // ── ADD refusals ────────────────────────────────────────────────────────
    {
        const std::vector<uint8_t> source = make_file();
        std::vector<uint8_t> patched;

        // a name already in use
        CHECK(!patch_columns(source.data(), source.size(), {}, {},
                             {donor<int64_t>("id", 1, DRAKEN_INT64)}, &patched).is_ok());
        // two added columns sharing a name
        std::vector<DonorFile> twice;
        twice.push_back(donor<int64_t>("dup", 1, DRAKEN_INT64));
        twice.push_back(donor<int64_t>("dup", 2, DRAKEN_INT64));
        CHECK(!patch_columns(source.data(), source.size(), {}, {}, twice, &patched).is_ok());
        // adding a name freed by a drop in the same call is fine
        CHECK(patch_columns(source.data(), source.size(), {"flag"}, {},
                            {donor<int64_t>("flag", 3, DRAKEN_INT64)}, &patched).is_ok());
        CHECK(int_values(patched, 0, "flag") == std::vector<int64_t>({3, 3, 3, 3}));
        // a donor that is not a skene file
        const std::vector<uint8_t> junk(64, 0);
        CHECK(!patch_columns(source.data(), source.size(), {}, {}, {junk}, &patched).is_ok());
        // a donor with more than one row
        CHECK(!patch_columns(source.data(), source.size(), {}, {}, {make_file()},
                             &patched).is_ok());
    }

    return skene_test::summary("test_patch");
}
