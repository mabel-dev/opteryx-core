// DirectPath discovery (design C4, §10, ruling "implement RLS"):
//
//   metadata server → zone, access token
//   ADS stream to directpath-pa.googleapis.com (kept open, ACKed, resubscribed
//   after a reconnect):
//     LDS  xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.listener.v3.Listener/storage.googleapis.com
//          → ApiListener → HttpConnectionManager → INLINE RouteConfiguration
//          → first route matching the method: a cluster, or the RLS cluster
//            specifier plugin
//   RLS  RouteLookup to the plugin's lookup service, per bucket, cached for
//        max_age → target cluster (the GFE default target is an ERROR: the
//        caller asked for DirectPath)
//   CDS  that cluster (EDS type only) → EDS → endpoint ip:port list
//
// Everything outside this subset (RDS, scoped routes, non-prefix route
// matches, aggregate clusters, unknown plugins) is refused with an error that
// names it, never approximated.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "grpc_conn.hpp"

namespace gcs_grpc {

struct Endpoint {
    std::string ip;
    int port = 0;
};

// GET http://metadata.google.internal/computeMetadata/v1/<path> with
// Metadata-Flavor: Google. Throws on any failure or non-200.
std::string gce_metadata_get(const std::string& path, int timeout_ms);

// The VM's attached service account token, cached until a minute before expiry.
std::string gce_access_token();

// The RouteLookupConfig the listener's RLS plugin carries (rls_config.proto).
struct RlsConfig {
    struct KeyBuilder {
        std::vector<std::pair<std::string, std::string>> names;  // (service, method); method "" = any
        std::string host_key, service_key, method_key;           // ExtraKeys
        std::vector<std::pair<std::string, std::vector<std::string>>> headers;  // key → header names
        std::map<std::string, std::string> constant_keys;
    };
    std::vector<KeyBuilder> grpc_keybuilders;
    std::string lookup_service;  // e.g. "dns:///storage.googleapis.com"
    int64_t max_age_s = 300;
    std::string default_target;
};

// What the listener's route says for one gRPC method.
struct RouteDecision {
    std::string cluster;  // a fixed cluster, or ""
    bool use_rls = false;
    RlsConfig rls;
};

// Parsers, exposed for the offline tests. Each takes the serialized resource
// (the Any's value) and throws std::runtime_error on anything outside the subset.
RouteDecision parse_listener_route(const std::string& listener, const std::string& authority, const std::string& path);
std::string parse_cluster_eds_service(const std::string& cluster);
std::vector<Endpoint> parse_cluster_load_assignment(const std::string& cla);
std::map<std::string, std::string> rls_key_map(const RlsConfig& cfg, const std::string& authority,
                                               const std::string& service, const std::string& method,
                                               const Metadata& request_headers);
std::string rls_request(const std::map<std::string, std::string>& key_map);
std::vector<std::string> parse_rls_response(const std::string& response);

class DirectPathResolver {
public:
    // `ca_file` verifies the TLS connections to directpath-pa and the RLS service.
    explicit DirectPathResolver(std::string ca_file);
    ~DirectPathResolver();

    DirectPathResolver(const DirectPathResolver&) = delete;
    DirectPathResolver& operator=(const DirectPathResolver&) = delete;

    // The DirectPath endpoints serving ReadObject for `bucket`. Blocks until
    // discovery has answered or `timeout_ms` passes. Throws std::runtime_error.
    std::vector<Endpoint> endpoints(const std::string& bucket, int timeout_ms);

private:
    struct Rls;
    void ads_loop();
    void ads_connect_locked();
    void ads_send_locked(const std::string& type_url, const std::vector<std::string>& names, const std::string& version,
                         const std::string& nonce);
    void on_ads_response(const std::string& msg);
    std::string cluster_for(const std::string& bucket, const RouteDecision& route, int timeout_ms);

    const std::string ca_file_;
    std::string node_id_;
    std::string zone_;

    std::mutex mu_;
    std::condition_variable cv_;
    bool stop_ = false;
    std::string ads_error_;  // why the newest ADS attempt failed, for timeouts

    // ADS stream (guarded by mu_; the reader thread owns the waiting).
    std::shared_ptr<GrpcConnection> ads_conn_;
    std::unique_ptr<MessageQueue> ads_q_;
    uint64_t ads_call_ = 0;
    bool node_sent_ = false;

    // Resource state, keyed by resource name.
    bool have_listener_ = false;
    std::string listener_;  // serialized Listener
    std::map<std::string, std::string> cluster_eds_;  // cluster → EDS service name ("" until known)
    std::map<std::string, std::vector<Endpoint>> eds_;  // EDS service → endpoints
    std::map<std::string, bool> eds_known_;
    // Last accepted version per type, for ACKs and resubscription.
    std::map<std::string, std::string> versions_;

    std::unique_ptr<Rls> rls_;
    std::thread reader_;
};

}  // namespace gcs_grpc
