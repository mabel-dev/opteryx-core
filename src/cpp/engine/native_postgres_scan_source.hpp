#pragma once
// src/cpp/engine/native_postgres_scan_source.hpp — a native (zero-Python) scan
// Source that reads rows from a PostgreSQL server.
//
// Sibling of native_skene_scan_source.hpp. The rows arrive over ONE server
// session as a stream of binary DataRow messages (pg/pg_client.hpp), which a
// worker decodes straight into Draken columns (pg/pg_decode.hpp) and cuts into
// morsels of `batch_rows`. Nothing here touches Python: the statement, its bind
// parameters, the emitted column identities and types were all fixed at plan
// time in the PgScanSpec this Source borrows.
//
// Parallelism: none within one scan. A PostgreSQL result is a single ordered
// stream on a single session, so `get_morsel` is serialised on the global
// state's mutex — extra workers simply wait their turn and, once the stream is
// exhausted, are told FINISHED. Splitting a table across sessions (key-range or
// ctid-range partitioning) is a later, separately-agreed optimisation.
//
// Errors: the ONE error channel is ErrCtx. A server error (bad SQL, permission,
// a type the decoder refuses), a transport failure or a decode failure ends the
// scan with `err.code = 1` and a message naming the cause; the session is
// discarded rather than pooled because its protocol state is unknown.
//
// Row limit: a pushed LIMIT is ALSO in the SQL, so the server stops producing
// at the quota; the counter here is the engine-side guarantee that no more
// than `row_limit` rows are emitted even if the SQL were ever built without it.

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "operator.hpp"
#include "morsels/cxx_morsel.h"
#include "pg/pg_client.hpp"
#include "pg/pg_decode.hpp"
#include "pg/pg_scan_spec.hpp"

namespace opteryx::engine {

struct NativePostgresScanGlobal : GlobalSourceState {
    std::mutex mtx;
    bool started = false;
    bool finished = false;
    std::string err;                                   // ErrCtx::msg points here
    std::unique_ptr<pg::PgConnection> conn;
    std::vector<pg::PgColumnDecoder> decoders;
    int64_t rows_emitted = 0;
    int64_t rows_read = 0;
};

class NativePostgresScanSource : public Source {
public:
    explicit NativePostgresScanSource(const pg::PgScanSpec* spec) : spec_(spec) {}

    std::unique_ptr<GlobalSourceState> make_global() override {
        return std::make_unique<NativePostgresScanGlobal>();
    }
    std::unique_ptr<LocalSourceState> make_local(GlobalSourceState&) override {
        return std::make_unique<LocalSourceState>();
    }

    SourceResult get_morsel(GlobalSourceState& gs, LocalSourceState&, MorselPtr& out,
                            ErrCtx& err) override {
        auto& g = static_cast<NativePostgresScanGlobal&>(gs);
        std::lock_guard<std::mutex> lock(g.mtx);
        if (g.finished) {
            if (!g.err.empty()) { err.code = 1; err.msg = g.err.c_str(); }
            return SourceResult::FINISHED;
        }
        try {
            if (!g.started) start(g);
            return pull(g, out);
        } catch (const std::exception& e) {
            g.err = e.what();
            g.finished = true;
            // Unknown protocol state: never back into the pool.
            g.conn.reset();
            spec_->rows_read = g.rows_read;
            err.code = 1;
            err.msg = g.err.c_str();
            return SourceResult::FINISHED;
        }
    }

private:
    void start(NativePostgresScanGlobal& g) {
        g.conn = pg::PgPool::instance().acquire(spec_->config);
        std::vector<std::optional<std::string>> params;
        params.reserve(spec_->params.size());
        for (size_t i = 0; i < spec_->params.size(); i++) {
            if (spec_->param_is_null[i]) params.emplace_back(std::nullopt);
            else params.emplace_back(spec_->params[i]);
        }
        std::vector<pg::PgField> fields = g.conn->begin(spec_->sql, params);

        // The stream must be the relation the binder described: same column
        // count, same OIDs, in order. Drift (a column type altered between bind
        // and execute) fails here instead of decoding one type as another.
        const size_t expected = spec_->zero_columns ? 1u : spec_->expected_oids.size();
        if (fields.size() != expected)
            throw pg::PgError("postgres scan: server returned " + std::to_string(fields.size()) +
                              " columns, plan expected " + std::to_string(expected));
        if (!spec_->zero_columns) {
            g.decoders.resize(fields.size());
            for (size_t i = 0; i < fields.size(); i++) {
                if (fields[i].oid != spec_->expected_oids[i])
                    throw pg::PgError("postgres scan: column '" + fields[i].name + "' is " +
                                      pg::pg_oid_name(fields[i].oid) + " on the server but the plan bound it as " +
                                      pg::pg_oid_name(spec_->expected_oids[i]) + " (relation changed since binding?)");
                g.decoders[i].init(fields[i].name, fields[i].oid,
                                   static_cast<DrakenType>(spec_->column_types[i]),
                                   spec_->decimal_precision[i], spec_->decimal_scale[i],
                                   spec_->batch_rows);
            }
        }
        g.started = true;
    }

    SourceResult pull(NativePostgresScanGlobal& g, MorselPtr& out) {
        const uint8_t* payload = nullptr;
        size_t length = 0;
        uint32_t rows = 0;
        const uint32_t batch = spec_->batch_rows;
        bool exhausted = false;

        while (rows < batch) {
            if (spec_->row_limit >= 0 && g.rows_emitted + rows >= spec_->row_limit) break;
            if (!g.conn->next_row(&payload, &length)) { exhausted = true; break; }
            g.rows_read++;
            if (!spec_->zero_columns) decode_row(g, payload, length);
            rows++;
        }

        const bool limit_hit = spec_->row_limit >= 0 && g.rows_emitted + rows >= spec_->row_limit;
        if (exhausted || limit_hit) {
            if (!exhausted) g.conn->finish();   // drain the rest of the stream
            pg::PgPool::instance().release(spec_->config, std::move(g.conn));
            g.finished = true;
            spec_->rows_read = g.rows_read;
        }

        if (rows == 0) return SourceResult::FINISHED;

        auto morsel = std::make_shared<CxxMorsel>();
        if (spec_->zero_columns) {
            morsel->zero_col_rows = rows;
        } else {
            morsel->columns.resize(g.decoders.size());
            morsel->names.reserve(g.decoders.size());
            for (size_t i = 0; i < g.decoders.size(); i++) {
                g.decoders[i].finish(morsel->columns[i]);
                morsel->names.push_back(spec_->out_identities[i]);
            }
        }
        g.rows_emitted += rows;
        stats.rows_out.fetch_add(rows, std::memory_order_relaxed);
        stats.calls.fetch_add(1, std::memory_order_relaxed);
        out = std::move(morsel);
        return SourceResult::HAVE_MORE;
    }

    void decode_row(NativePostgresScanGlobal& g, const uint8_t* p, size_t n) {
        if (n < 2) throw pg::PgError("postgres protocol: truncated DataRow");
        const int16_t ncols = (int16_t)((p[0] << 8) | p[1]);
        if ((size_t)ncols != g.decoders.size())
            throw pg::PgError("postgres protocol: DataRow column count does not match RowDescription");
        size_t pos = 2;
        for (int16_t i = 0; i < ncols; i++) {
            if (pos + 4 > n) throw pg::PgError("postgres protocol: truncated DataRow cell");
            const int32_t len = (int32_t)(((uint32_t)p[pos] << 24) | ((uint32_t)p[pos + 1] << 16) |
                                          ((uint32_t)p[pos + 2] << 8) | (uint32_t)p[pos + 3]);
            pos += 4;
            if (len < 0) {
                g.decoders[(size_t)i].append_null();
                continue;
            }
            if (pos + (size_t)len > n) throw pg::PgError("postgres protocol: truncated DataRow cell");
            g.decoders[(size_t)i].append(p + pos, len);
            pos += (size_t)len;
        }
    }

    const pg::PgScanSpec* spec_;
};

}  // namespace opteryx::engine
