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

#include <chrono>
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
//
// `transport` is set only for a socket/TLS read or write that failed -- the
// connection died under us. It is what makes a retry decidable: a server error
// (the server is fine, the statement is not) and a protocol or auth failure
// (this client is wrong, or the credentials are) are reproducible and must NOT
// be retried, while a dead socket says nothing about the statement at all. See
// `pg_with_retry`.
struct PgError : std::runtime_error {
    std::string sqlstate;
    bool        transport = false;
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
    // + bytes; len == -1 is NULL). The pointer is valid until the next call: it
    // points INTO the transport's read buffer, which the next read may refill
    // over. Nothing is copied or allocated per row - a caller that needs a row
    // to outlive its next next_row() copies it.
    // Returns false once CommandComplete + ReadyForQuery have been consumed.
    // Throws PgError on a server ErrorResponse (after draining to ReadyForQuery,
    // so the connection is reusable).
    bool next_row(const uint8_t** payload, size_t* length);
    // Abandon a stream early: drains remaining messages to ReadyForQuery. After
    // a server-side error or transport failure the connection is marked
    // unhealthy and must not be pooled.
    void finish();

    bool healthy() const { return healthy_; }

    // Whether this connection's socket is still usable, as far as can be known
    // without writing to it. For an IDLE connection only -- it reads the socket
    // state, so it is meaningless mid-stream. False means definitely dead (the
    // peer closed or reset it), true means "nothing says otherwise", which is
    // not a guarantee: the server can close it in the gap between this check
    // and the next write. That residual race is what the retry covers.
    bool alive() const;

    // Set by PgPool::acquire: true when this connection came out of the idle
    // pool, false when it was just opened. Only the former is worth retrying --
    // a connection that fails on its first use is reporting a server that is
    // genuinely unreachable, and retrying only doubles the wait before saying so.
    bool pooled() const { return pooled_; }
    void set_pooled(bool v) { pooled_ = v; }

    // When this connection was last returned to the pool. Idle connections age
    // out (see PgPool::kMaxIdleSeconds) rather than being handed out at any age:
    // the longer one sits, the likelier the server, a pooler or a NAT has
    // dropped it, and an expired one costs a reconnect where a dead one costs a
    // failed query and a retry.
    std::chrono::steady_clock::time_point idle_since{};

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
    [[noreturn]] void raise_server_error(const uint8_t* payload, size_t len);

    std::unique_ptr<Transport> t_;
    std::map<std::string, std::string> params_;
    std::string server_version_;
    std::string command_tag_;
    bool streaming_ = false;          // between begin() and the final ReadyForQuery
    bool healthy_ = true;
    bool pooled_ = false;             // came from the idle pool, not freshly opened
};

class PgPool {
public:
    static PgPool& instance();
    // A pooled idle connection for `config`, or a freshly opened one. Pooled
    // connections are checked for liveness and age first; dead or expired ones
    // are dropped rather than handed out.
    std::unique_ptr<PgConnection> acquire(const PgConfig& config);
    // A newly opened connection, ignoring the pool entirely. The retry path
    // uses this: the whole point of the second attempt is not to be handed
    // another connection from the same possibly-stale bucket.
    std::unique_ptr<PgConnection> acquire_fresh(const PgConfig& config);
    // Returns a connection to the pool; unhealthy connections are closed instead.
    void release(const PgConfig& config, std::unique_ptr<PgConnection> conn);

private:
    static constexpr size_t kMaxIdlePerKey = 4;
    // How long a connection may sit idle before it is closed instead of reused.
    // Comfortably under the shortest idle timeout anything upstream is likely to
    // impose -- managed PostgreSQL, pgbouncer and cloud NATs reap at minutes,
    // not seconds -- so in the ordinary case the socket is gone because it aged
    // out here, not because the far end tore it down.
    static constexpr std::chrono::seconds kMaxIdleSeconds{60};
    std::mutex mtx_;
    std::unordered_map<std::string, std::vector<std::unique_ptr<PgConnection>>> idle_;
};

// Run `fn` against a pooled connection, retrying ONCE on a freshly opened one
// if the first attempt died of a transport error on a connection that came out
// of the pool.
//
// Safe for anything whose work has not yet been observed: a pooled connection
// that dies on the first write never delivered a row, never ran the statement
// server-side (the bytes did not arrive) and therefore has nothing to replay.
// It must NOT wrap work that has already emitted rows -- see the scan source,
// which retries inside its start() and never once a row has been decoded.
//
// `fn` takes the connection and returns its result; it must leave the
// connection fit to pool or throw.
template <typename Fn>
auto pg_with_retry(const PgConfig& config, Fn&& fn) -> decltype(fn(*(PgConnection*)nullptr)) {
    for (int attempt = 0;; attempt++) {
        std::unique_ptr<PgConnection> conn =
            attempt == 0 ? PgPool::instance().acquire(config)
                         : PgPool::instance().acquire_fresh(config);
        try {
            auto out = fn(*conn);
            PgPool::instance().release(config, std::move(conn));
            return out;
        } catch (const PgError& e) {
            const bool retryable = attempt == 0 && e.transport && conn->pooled();
            // Dead or unknown protocol state either way: never back to the pool.
            conn.reset();
            if (!retryable) throw;
        } catch (...) {
            conn.reset();
            throw;
        }
    }
}

// Convenience for the plan-time callers: acquire, run, release.
std::vector<PgField> pg_describe(const PgConfig& config, const std::string& sql);
std::vector<std::vector<std::optional<std::string>>> pg_query_text(
    const PgConfig& config, const std::string& sql, const std::vector<std::string>& params);

}  // namespace opteryx::pg
