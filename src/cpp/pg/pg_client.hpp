#pragma once
// src/cpp/pg/pg_client.hpp — native PostgreSQL frontend/backend protocol (v3) client.
//
// Opteryx reads a PostgreSQL-bound workspace over the wire protocol directly:
// no libpq, no Python driver. This header is the ONE client, used by two callers
// in the same shared object (opteryx.operators._operators):
//
//   * plan time  — PgConnection::describe() gives the binder a relation's exact
//                  result columns (name, type OID, typmod) and query_text() answers
//                  the small metadata questions (does the table exist, how many rows
//                  does pg_class think it has). Called from Cython with the GIL held.
//   * execution  — NativePostgresScanSource (src/cpp/engine/) runs the scan
//                  statement with BINARY result columns and decodes the DataRow
//                  stream straight into Draken vectors on a worker thread, GIL
//                  released. See pg_decode.hpp for the per-type decoders.
//
// Protocol scope: TCP + TLS (OpenSSL; sslmode disable | require | verify-full),
// auth by cleartext, md5 and SCRAM-SHA-256 (no channel binding, no GSSAPI, no
// client certificates), the extended-query protocol (Parse/Bind/Describe/Execute/
// Sync) with text-format bind parameters. Anything outside that fails loud with a
// message that names what was asked for.
//
// Threading: a PgConnection is used by ONE thread at a time. PgPool hands
// connections out and takes them back under its own mutex; a connection that
// errored mid-stream is discarded rather than pooled (its protocol state is
// unknown).

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/buffers.h"  // DrakenType

namespace opteryx::pg {

struct PgConfig {
    std::string host;
    int         port = 5432;
    std::string dbname;
    std::string user;
    std::string password;
    std::string sslmode = "require";  // disable | require | verify-full
    int         timeout_s = 30;

    // Pool key: everything that selects a distinct server session. The password
    // is part of it so a rotated credential never reuses a session opened under
    // the old one.
    std::string key() const;
};

// One error class for the whole client. `sqlstate` is set when the server sent
// an ErrorResponse (five characters, e.g. "42P01"); empty for transport,
// protocol and auth failures raised on this side.
struct PgError : std::runtime_error {
    std::string sqlstate;
    PgError(const std::string& what, std::string state = "")
        : std::runtime_error(what), sqlstate(std::move(state)) {}
};

struct PgField {
    std::string name;
    uint32_t    oid = 0;
    int32_t     typmod = -1;
};

// The Draken physical type a PostgreSQL type OID decodes to. DRAKEN_NULL means
// "not supported" — the ONE table both the binder's schema and the execution
// decoder read, so the two can never disagree about a column's type.
DrakenType pg_oid_to_draken(uint32_t oid);

// Human-readable PostgreSQL type name for an OID we refuse (used in the refusal
// message only; unknown OIDs render as their number).
std::string pg_oid_name(uint32_t oid);

class Transport;

class PgConnection {
public:
    explicit PgConnection(const PgConfig& config);  // connects + authenticates
    ~PgConnection();
    PgConnection(const PgConnection&) = delete;
    PgConnection& operator=(const PgConnection&) = delete;

    // Parse + Describe(statement) + Sync. Returns the statement's result
    // columns without executing it. Throws PgError on a server error.
    std::vector<PgField> describe(const std::string& sql);

    // Run a statement with TEXT-format results and return every row. For small
    // metadata queries only (pg_class lookups, listings). nullopt = SQL NULL.
    std::vector<std::vector<std::optional<std::string>>> query_text(
        const std::string& sql, const std::vector<std::string>& params);

    // ---- streaming, BINARY-format results ----------------------------------
    // begin(): sends Parse/Bind/Describe(portal)/Execute/Sync and reads up to and
    // including the RowDescription. Returns the result columns (format = binary).
    // A NoData statement (no result columns) returns an empty vector.
    std::vector<PgField> begin(const std::string& sql,
                               const std::vector<std::optional<std::string>>& params);
    // next_row(): the next DataRow payload (int16 ncols, then per column int32 len
    // + bytes; len == -1 is NULL). The pointer is valid until the next call.
    // Returns false once CommandComplete + ReadyForQuery have been consumed.
    // Throws PgError on a server ErrorResponse (after draining to ReadyForQuery,
    // so the connection is reusable).
    bool next_row(const uint8_t** payload, size_t* length);
    // Abandon a stream early: drains remaining messages to ReadyForQuery. After
    // a server-side error or transport failure the connection is marked
    // unhealthy and must not be pooled.
    void finish();

    bool healthy() const { return healthy_; }
    const std::string& server_version() const { return server_version_; }
    const std::string& command_tag() const { return command_tag_; }

private:
    struct Msg;
    void startup(const PgConfig& config);
    void scram_sha256(const Msg& first, const PgConfig& config);
    void send_password(const std::string& pw);
    Msg  read_msg();
    void send_extended(const std::string& sql,
                       const std::vector<std::optional<std::string>>& params,
                       bool binary_results, bool describe_portal);
    static std::vector<PgField> parse_row_description(const Msg& m);
    [[noreturn]] void raise_server_error(const Msg& m);

    std::unique_ptr<Transport> t_;
    std::map<std::string, std::string> params_;
    std::string server_version_;
    std::string command_tag_;
    std::vector<uint8_t> row_buf_;    // last DataRow payload (next_row)
    bool streaming_ = false;          // between begin() and the final ReadyForQuery
    bool healthy_ = true;
};

class PgPool {
public:
    static PgPool& instance();
    // A pooled idle connection for `config`, or a freshly opened one.
    std::unique_ptr<PgConnection> acquire(const PgConfig& config);
    // Returns a connection to the pool; unhealthy connections are closed instead.
    void release(const PgConfig& config, std::unique_ptr<PgConnection> conn);

private:
    static constexpr size_t kMaxIdlePerKey = 4;
    std::mutex mtx_;
    std::unordered_map<std::string, std::vector<std::unique_ptr<PgConnection>>> idle_;
};

// Convenience for the plan-time callers: acquire, run, release.
std::vector<PgField> pg_describe(const PgConfig& config, const std::string& sql);
std::vector<std::vector<std::optional<std::string>>> pg_query_text(
    const PgConfig& config, const std::string& sql, const std::vector<std::string>& params);

}  // namespace opteryx::pg
