// Native timing/validation bridge for dev/bench_orc_fixed.py. The actual reader is
// the existing rugo_native build; no Python or FFI call occurs inside a sample.
#include "orc/orc_reader.hpp"

#include <bit>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <vector>

namespace {
using Clock = std::chrono::steady_clock;
using rugo::orc::OrcFile;

[[noreturn]] void fail(const char* message) {
    std::fprintf(stderr, "ORC fixed-width benchmark: %s\n", message);
    std::abort();
}

struct Case {
    OrcFile file;
    uint32_t rows;
    DrakenType type;
    Case(const uint8_t* bytes, size_t size, uint32_t n, DrakenType t)
        : file(bytes, size), rows(n), type(t) {}
};

int64_t value_at(const rugo::orc::Column& col, size_t row) {
    switch (col.type) {
        case DRAKEN_INT8: return static_cast<const int8_t*>(col.data)[row];
        case DRAKEN_INT16: return static_cast<const int16_t*>(col.data)[row];
        case DRAKEN_INT32: case DRAKEN_DATE32: return static_cast<const int32_t*>(col.data)[row];
        case DRAKEN_INT64: case DRAKEN_TIMESTAMP64: return static_cast<const int64_t*>(col.data)[row];
        case DRAKEN_FLOAT32:
            return std::bit_cast<int64_t>(static_cast<double>(static_cast<const float*>(col.data)[row]));
        case DRAKEN_FLOAT64: return std::bit_cast<int64_t>(static_cast<const double*>(col.data)[row]);
        default: fail("unexpected output type");
    }
}

void validate(Case& c, const int64_t* expected, const uint8_t* valid) {
    const auto batch = c.file.decode_stripe(0);
    if (batch.rows != c.rows || batch.columns.size() != 1) fail("unexpected batch shape");
    const auto& col = batch.columns[0];
    if (col.type != c.type || col.length != c.rows || col.data == nullptr) fail("unexpected column layout");
    for (uint32_t i = 0; i < c.rows; ++i) {
        const bool present = col.validity == nullptr || ((col.validity[i >> 3] >> (i & 7)) & 1);
        if (present != (valid[i] != 0) || (present && value_at(col, i) != expected[i])) {
            std::fprintf(stderr, "row %u: validity=%d expected=%d, value=%lld expected=%lld\n", i,
                         present, valid[i], static_cast<long long>(value_at(col, i)),
                         static_cast<long long>(expected[i]));
            fail("decoded output differs from fixture");
        }
    }
}
} // namespace

// Exceptions cannot cross the C ABI: terminate loudly with the original error.
extern "C" void* orc_fixed_create(const uint8_t* bytes, size_t size, uint32_t rows,
                                  unsigned kind, unsigned source_type, unsigned type,
                                  const int64_t* expected, const uint8_t* valid) {
    try {
        auto c = std::make_unique<Case>(bytes, size, rows, static_cast<DrakenType>(type));
        if (c->file.num_rows() != rows || c->file.stripe_count() != 1 ||
            c->file.compression() != rugo::orc::Compression::None ||
            c->file.file_columns().size() != 1 ||
            static_cast<unsigned>(c->file.file_columns()[0].kind) != kind ||
            static_cast<unsigned>(c->file.file_columns()[0].type) != source_type)
            fail("fixture metadata differs from requested single-stripe uncompressed type");
        rugo::orc::ReaderColumn rc;
        rc.name = c->file.file_columns()[0].name;
        rc.type = c->type;
        c->file.bind({rc}, {rc.name}, false);
        validate(*c, expected, valid);
        return c.release();
    } catch (const std::exception& e) {
        fail(e.what());
    }
}

extern "C" double orc_fixed_sample(void* handle, unsigned iterations, uint64_t* checksum) {
    try {
        auto& c = *static_cast<Case*>(handle);
        uint64_t sum = 0;
        const auto start = Clock::now();
        for (unsigned i = 0; i < iterations; ++i) {
            auto batch = c.file.decode_stripe(0);
            const auto& col = batch.columns[0];
            asm volatile("" : : "r"(col.data) : "memory");
            sum += static_cast<uint64_t>(value_at(col, i % c.rows));
        }
        const double ns = std::chrono::duration<double, std::nano>(Clock::now() - start).count();
        *checksum += sum;
        return ns / (static_cast<double>(iterations) * c.rows);
    } catch (const std::exception& e) {
        fail(e.what());
    }
}

extern "C" void orc_fixed_destroy(void* handle) { delete static_cast<Case*>(handle); }

// The PRESENT suite binds every column, including mixed fixed-width files.
namespace {
struct PresentCase {
    OrcFile file;
    uint32_t rows;
    std::vector<DrakenType> types;
    PresentCase(const uint8_t* bytes, size_t size, uint32_t n)
        : file(bytes, size), rows(n) {}
};
}

extern "C" void* orc_present_create(const uint8_t* bytes, size_t size, uint32_t rows,
                                    unsigned columns, const unsigned* kinds,
                                    const unsigned* types, const int64_t* expected,
                                    const uint8_t* valid) {
    try {
        auto c = std::make_unique<PresentCase>(bytes, size, rows);
        const auto& file_columns = c->file.file_columns();
        if (c->file.num_rows() != rows || c->file.stripe_count() != 1 ||
            c->file.compression() != rugo::orc::Compression::None ||
            file_columns.size() != columns)
            fail("PRESENT fixture metadata differs from requested single-stripe file");
        for (unsigned j = 0; j < columns; ++j) {
            if (static_cast<unsigned>(file_columns[j].kind) != kinds[j] ||
                static_cast<unsigned>(file_columns[j].type) != types[j])
                fail("PRESENT fixture column kind/type mismatch");
            c->types.push_back(static_cast<DrakenType>(types[j]));
        }
        c->file.bind_all();
        const auto batch = c->file.decode_stripe(0);
        if (batch.rows != rows || batch.columns.size() != columns) fail("PRESENT batch shape mismatch");
        for (unsigned j = 0; j < columns; ++j) {
            const auto& col = batch.columns[j];
            if (col.type != c->types[j] || col.length != rows || col.data == nullptr)
                fail("PRESENT column layout mismatch");
            for (uint32_t i = 0; i < rows; ++i) {
                const size_t at = static_cast<size_t>(j) * rows + i;
                const bool present = col.validity == nullptr || ((col.validity[i >> 3] >> (i & 7)) & 1);
                if (present != (valid[at] != 0) || (present && value_at(col, i) != expected[at])) {
                    std::fprintf(stderr, "column %u row %u: validity=%d expected=%d\n", j, i,
                                 present, valid[at]);
                    if (present) std::fprintf(stderr, "value=%lld expected=%lld\n",
                                              static_cast<long long>(value_at(col, i)),
                                              static_cast<long long>(expected[at]));
                    fail("PRESENT decoded output differs from fixture");
                }
            }
        }
        return c.release();
    } catch (const std::exception& e) {
        fail(e.what());
    }
}

extern "C" double orc_present_sample(void* handle, unsigned iterations, uint64_t* checksum) {
    try {
        auto& c = *static_cast<PresentCase*>(handle);
        uint64_t sum = 0;
        const auto start = Clock::now();
        for (unsigned i = 0; i < iterations; ++i) {
            auto batch = c.file.decode_stripe(0);
            for (const auto& col : batch.columns) {
                asm volatile("" : : "r"(col.data) : "memory");
                const uint32_t row = i % c.rows;
                if (col.validity == nullptr || ((col.validity[row >> 3] >> (row & 7)) & 1))
                    sum += static_cast<uint64_t>(value_at(col, row));
            }
        }
        const double ns = std::chrono::duration<double, std::nano>(Clock::now() - start).count();
        *checksum += sum;
        return ns / (static_cast<double>(iterations) * c.rows * c.types.size());
    } catch (const std::exception& e) {
        fail(e.what());
    }
}

extern "C" void orc_present_destroy(void* handle) { delete static_cast<PresentCase*>(handle); }
