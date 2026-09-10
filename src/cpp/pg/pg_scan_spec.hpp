#pragma once
// src/cpp/pg/pg_scan_spec.hpp — the plan-time description of one Postgres scan.
//
// Built by the Cython PostgresScanPlan (opteryx/operators/_operators.pyx) from
// what the planner decided, and BORROWED by NativePostgresScanSource for the
// driver's lifetime. Every field is a plain value fixed at plan time; nothing
// here is consulted by Python during execution.

#include <cstdint>
#include <string>
#include <vector>

#include "pg/pg_client.hpp"

namespace opteryx::pg {

struct PgScanSpec {
    PgConfig    config;
    std::string sql;                       // the scan statement, $n placeholders
    std::vector<std::string> params;       // text-format bind values, parallel to param_is_null
    std::vector<uint8_t>     param_is_null;

    // Per emitted column (parallel vectors, emit order):
    std::vector<std::string> out_identities;   // plan identities the morsel names carry
    std::vector<uint32_t>    expected_oids;    // what the binder saw; the stream must agree
    std::vector<int>         column_types;     // DrakenType each column is emitted as
    std::vector<int>         decimal_precision;// DECIMAL/DECIMAL128 only, else 0
    std::vector<int>         decimal_scale;    // DECIMAL/DECIMAL128 only, else 0

    uint32_t batch_rows = 65536;   // morsel cut
    int64_t  row_limit = -1;       // pushed LIMIT (also in the SQL); -1 = none
    bool     zero_columns = false; // COUNT(*)-shaped scan: emit zero-column morsels
                                   // carrying only a row count

    // Written by the Source (once, when the stream ends) and read from Python
    // after the driver finishes: rows the server sent. -1 = never ran. `mutable`
    // because the Source borrows the spec as const and this is its one write.
    mutable int64_t rows_read = -1;
};

}  // namespace opteryx::pg
