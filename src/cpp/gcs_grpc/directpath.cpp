#include "directpath.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <random>
#include <stdexcept>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include "pb_wire.hpp"
#include "transport.hpp"

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0  // Apple: SO_NOSIGPIPE is set by connect_tcp
#endif

namespace gcs_grpc {

namespace {

constexpr const char* kXdsServer = "directpath-pa.googleapis.com";
constexpr const char* kAdsPath = "/envoy.service.discovery.v3.AggregatedDiscoveryService/StreamAggregatedResources";
constexpr const char* kRlsPath = "/grpc.lookup.v1.RouteLookupService/RouteLookup";
constexpr const char* kStorageAuthority = "storage.googleapis.com";
constexpr const char* kStorageService = "google.storage.v2.Storage";
constexpr const char* kReadObjectMethod = "ReadObject";
constexpr const char* kListenerName =
    "xdstp://traffic-director-c2p.xds.googleapis.com/envoy.config.listener.v3.Listener/storage.googleapis.com";

constexpr const char* kTypeListener = "type.googleapis.com/envoy.config.listener.v3.Listener";
constexpr const char* kTypeCluster = "type.googleapis.com/envoy.config.cluster.v3.Cluster";
constexpr const char* kTypeEds = "type.googleapis.com/envoy.config.endpoint.v3.ClusterLoadAssignment";
constexpr const char* kTypeResource = "type.googleapis.com/envoy.service.discovery.v3.Resource";
constexpr const char* kTypeHcm =
    "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager";
constexpr const char* kTypeRls = "type.googleapis.com/grpc.lookup.v1.RouteLookupClusterSpecifier";

using pb::Field;
using pb::Reader;

Reader reader(const std::string& s) { return Reader(reinterpret_cast<const uint8_t*>(s.data()), s.size()); }
Reader reader(const Field& f) { return Reader(f.p, f.n); }

// google.protobuf.Any { type_url = 1; value = 2; }
void parse_any(const Field& f, std::string& type_url, std::string& value) {
    Reader r = reader(f);
    Field g;
    while (r.next(g)) {
        if (g.number == 1 && g.type == pb::kLen) type_url.assign(g.view());
        else if (g.number == 2 && g.type == pb::kLen) value.assign(g.view());
    }
}

bool domain_matches(const std::string& pattern, const std::string& host, int& score) {
    // gRPC's virtual host selection: exact > suffix wildcard > prefix wildcard > "*".
    if (pattern == host) { score = 4; return true; }
    if (pattern == "*") { score = 1; return true; }
    if (pattern.size() > 1 && pattern[0] == '*' && host.size() >= pattern.size() - 1 &&
        host.compare(host.size() - (pattern.size() - 1), pattern.size() - 1, pattern, 1) == 0) {
        score = 3;
        return true;
    }
    if (pattern.size() > 1 && pattern.back() == '*' && host.compare(0, pattern.size() - 1, pattern, 0, pattern.size() - 1) == 0) {
        score = 2;
        return true;
    }
    return false;
}

RlsConfig parse_rls_config(const Field& f) {
    RlsConfig cfg;
    Reader r = reader(f);
    Field g;
    while (r.next(g)) {
        if (g.type != pb::kLen) continue;
        if (g.number == 2) {  // GrpcKeyBuilder
            RlsConfig::KeyBuilder kb;
            Reader k = reader(g);
            Field h;
            while (k.next(h)) {
                if (h.type != pb::kLen) continue;
                if (h.number == 1) {  // Name { service = 1; method = 2; }
                    std::string service, method;
                    Reader n = reader(h);
                    Field i;
                    while (n.next(i)) {
                        if (i.number == 1 && i.type == pb::kLen) service.assign(i.view());
                        else if (i.number == 2 && i.type == pb::kLen) method.assign(i.view());
                    }
                    kb.names.emplace_back(service, method);
                } else if (h.number == 3) {  // ExtraKeys { host = 1; service = 2; method = 3; }
                    Reader n = reader(h);
                    Field i;
                    while (n.next(i)) {
                        if (i.type != pb::kLen) continue;
                        if (i.number == 1) kb.host_key.assign(i.view());
                        else if (i.number == 2) kb.service_key.assign(i.view());
                        else if (i.number == 3) kb.method_key.assign(i.view());
                    }
                } else if (h.number == 2) {  // NameMatcher { key = 1; repeated names = 2; }
                    std::string key;
                    std::vector<std::string> names;
                    Reader n = reader(h);
                    Field i;
                    while (n.next(i)) {
                        if (i.number == 1 && i.type == pb::kLen) key.assign(i.view());
                        else if (i.number == 2 && i.type == pb::kLen) names.emplace_back(i.view());
                    }
                    kb.headers.emplace_back(key, names);
                } else if (h.number == 4) {  // map<string, string> constant_keys
                    std::string key, value;
                    Reader n = reader(h);
                    Field i;
                    while (n.next(i)) {
                        if (i.number == 1 && i.type == pb::kLen) key.assign(i.view());
                        else if (i.number == 2 && i.type == pb::kLen) value.assign(i.view());
                    }
                    kb.constant_keys[key] = value;
                }
            }
            cfg.grpc_keybuilders.push_back(std::move(kb));
        } else if (g.number == 3) {
            cfg.lookup_service.assign(g.view());
        } else if (g.number == 5) {  // Duration { seconds = 1; }
            Reader d = reader(g);
            Field h;
            while (d.next(h))
                if (h.number == 1 && h.type == pb::kVarint) cfg.max_age_s = static_cast<int64_t>(h.u);
        } else if (g.number == 9) {
            cfg.default_target.assign(g.view());
        }
    }
    if (cfg.lookup_service.empty()) throw std::runtime_error("DirectPath: RLS config has no lookup service");
    return cfg;
}

// RouteMatch: only `prefix` (1) and `case_sensitive` (4) are evaluated; any
// other criterion would need matching this reader does not do.
bool route_matches(const Field& match, const std::string& path) {
    std::string prefix;
    bool have_prefix = false;
    bool case_sensitive = true;
    Reader r = reader(match);
    Field g;
    while (r.next(g)) {
        if (g.number == 1 && g.type == pb::kLen) {
            prefix.assign(g.view());
            have_prefix = true;
        } else if (g.number == 4 && g.type == pb::kLen) {  // BoolValue { value = 1; }
            Reader b = reader(g);
            Field h;
            case_sensitive = false;
            while (b.next(h))
                if (h.number == 1 && h.type == pb::kVarint) case_sensitive = h.u != 0;
        } else {
            throw std::runtime_error("DirectPath: route match field " + std::to_string(g.number) + " is not supported");
        }
    }
    if (!have_prefix) throw std::runtime_error("DirectPath: route match without a prefix is not supported");
    if (prefix.size() > path.size()) return false;
    if (case_sensitive) return path.compare(0, prefix.size(), prefix) == 0;
    for (size_t i = 0; i < prefix.size(); ++i)
        if (std::tolower(static_cast<unsigned char>(prefix[i])) != std::tolower(static_cast<unsigned char>(path[i])))
            return false;
    return true;
}

// Minimal blocking HTTP/1.0 GET for the metadata server.
std::string http_get(const std::string& host, int port, const std::string& target, const std::string& headers,
                     int timeout_ms) {
    const int fd = connect_tcp(host, port, timeout_ms);
    struct Closer {
        int fd;
        ~Closer() { ::close(fd); }
    } closer{fd};
    const std::string req = "GET " + target + " HTTP/1.0\r\nHost: " + host + "\r\n" + headers + "\r\n";
    size_t at = 0;
    while (at < req.size()) {
        const ssize_t w = ::send(fd, req.data() + at, req.size() - at, MSG_NOSIGNAL);
        if (w <= 0) throw std::runtime_error("metadata server: write failed: " + std::string(std::strerror(errno)));
        at += static_cast<size_t>(w);
    }
    std::string resp;
    char buf[8192];
    for (;;) {
        pollfd p{fd, POLLIN, 0};
        if (poll(&p, 1, timeout_ms) != 1) throw std::runtime_error("metadata server: timed out");
        const ssize_t r = ::recv(fd, buf, sizeof(buf), 0);
        if (r < 0) throw std::runtime_error("metadata server: read failed: " + std::string(std::strerror(errno)));
        if (r == 0) break;
        resp.append(buf, static_cast<size_t>(r));
    }
    const size_t sp = resp.find(' ');
    const size_t body = resp.find("\r\n\r\n");
    if (sp == std::string::npos || body == std::string::npos) throw std::runtime_error("metadata server: malformed response");
    const int status = std::atoi(resp.c_str() + sp + 1);
    if (status != 200)
        throw std::runtime_error("metadata server: HTTP " + std::to_string(status) + " for " + target);
    return resp.substr(body + 4);
}

std::string json_string_field(const std::string& json, const std::string& key) {
    const std::string k = "\"" + key + "\"";
    size_t at = json.find(k);
    if (at == std::string::npos) return "";
    at = json.find('"', json.find(':', at + k.size()) + 1);
    const size_t end = json.find('"', at + 1);
    if (at == std::string::npos || end == std::string::npos) return "";
    return json.substr(at + 1, end - at - 1);
}

int64_t json_int_field(const std::string& json, const std::string& key) {
    const std::string k = "\"" + key + "\"";
    const size_t at = json.find(k);
    if (at == std::string::npos) return 0;
    return std::atoll(json.c_str() + json.find(':', at + k.size()) + 1);
}

std::string url_encode(const std::string& s) {
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    for (unsigned char ch : s) {
        if (std::isalnum(ch) || ch == '-' || ch == '_' || ch == '.' || ch == '~') {
            out.push_back(static_cast<char>(ch));
        } else {
            out.push_back('%');
            out.push_back(hex[ch >> 4]);
            out.push_back(hex[ch & 15]);
        }
    }
    return out;
}

// "dns:///storage.googleapis.com" or "storage.googleapis.com[:port]" → host, port.
void parse_target(const std::string& target, std::string& host, int& port) {
    std::string t = target;
    if (t.rfind("dns:///", 0) == 0) t = t.substr(7);
    else if (t.find("://") != std::string::npos)
        throw std::runtime_error("DirectPath: unsupported RLS lookup service '" + target + "'");
    const size_t colon = t.rfind(':');
    host = colon == std::string::npos ? t : t.substr(0, colon);
    port = colon == std::string::npos ? 443 : std::atoi(t.c_str() + colon + 1);
}

}  // namespace

// ── Metadata server ─────────────────────────────────────────────────────────

std::string gce_metadata_get(const std::string& path, int timeout_ms) {
    return http_get("metadata.google.internal.", 80, "/computeMetadata/v1/" + path, "Metadata-Flavor: Google\r\n",
                    timeout_ms);
}

std::string gce_access_token() {
    static std::mutex mu;
    static std::string token;
    static std::chrono::steady_clock::time_point expires;
    std::lock_guard<std::mutex> g(mu);
    const auto now = std::chrono::steady_clock::now();
    if (token.empty() || now >= expires) {
        const std::string body = gce_metadata_get("instance/service-accounts/default/token", 5000);
        token = json_string_field(body, "access_token");
        if (token.empty()) throw std::runtime_error("metadata server: token response has no access_token");
        expires = now + std::chrono::seconds(std::max<int64_t>(json_int_field(body, "expires_in") - 60, 0));
    }
    return token;
}

// ── Resource parsers ────────────────────────────────────────────────────────

RouteDecision parse_listener_route(const std::string& listener, const std::string& authority, const std::string& path) {
    // Listener.api_listener (19) → ApiListener.api_listener (1): Any(HttpConnectionManager)
    std::string hcm;
    {
        Reader r = reader(listener);
        Field f;
        while (r.next(f)) {
            if (f.number != 19 || f.type != pb::kLen) continue;
            Reader a = reader(f);
            Field g;
            while (a.next(g)) {
                if (g.number != 1 || g.type != pb::kLen) continue;
                std::string type;
                parse_any(g, type, hcm);
                if (type != kTypeHcm) throw std::runtime_error("DirectPath: listener API type '" + type + "' is not supported");
            }
        }
    }
    if (hcm.empty()) throw std::runtime_error("DirectPath: listener has no HttpConnectionManager");

    // HttpConnectionManager.route_config (4); RDS (3) and scoped routes (31) are refused.
    std::string route_config;
    {
        Reader r = reader(hcm);
        Field f;
        while (r.next(f)) {
            if (f.number == 4 && f.type == pb::kLen) route_config.assign(f.view());
            else if (f.number == 3 || f.number == 31)
                throw std::runtime_error("DirectPath: RDS / scoped routes are not supported (inline route config only)");
        }
    }
    if (route_config.empty()) throw std::runtime_error("DirectPath: listener has no inline route configuration");

    // RouteConfiguration { virtual_hosts = 2; cluster_specifier_plugins = 12; }
    std::vector<std::string> vhosts;
    std::map<std::string, std::pair<std::string, std::string>> plugins;  // name → (type, value)
    {
        Reader r = reader(route_config);
        Field f;
        while (r.next(f)) {
            if (f.type != pb::kLen) continue;
            if (f.number == 2) vhosts.emplace_back(f.view());
            if (f.number == 12) {  // ClusterSpecifierPlugin { extension = 1: TypedExtensionConfig { name = 1; typed_config = 2; } }
                Reader p = reader(f);
                Field g;
                while (p.next(g)) {
                    if (g.number != 1 || g.type != pb::kLen) continue;
                    std::string name, type, value;
                    Reader e = reader(g);
                    Field h;
                    while (e.next(h)) {
                        if (h.number == 1 && h.type == pb::kLen) name.assign(h.view());
                        else if (h.number == 2 && h.type == pb::kLen) parse_any(h, type, value);
                    }
                    plugins[name] = {type, value};
                }
            }
        }
    }

    // Best virtual host for the authority.
    const std::string* best = nullptr;
    int best_score = 0;
    for (const std::string& vh : vhosts) {
        Reader r = reader(vh);
        Field f;
        while (r.next(f)) {
            int score = 0;
            if (f.number == 2 && f.type == pb::kLen && domain_matches(std::string(f.view()), authority, score) &&
                score > best_score) {
                best = &vh;
                best_score = score;
            }
        }
    }
    if (!best) throw std::runtime_error("DirectPath: no virtual host matches " + authority);

    // First route (VirtualHost.routes = 3) whose match accepts the path.
    Reader r = reader(*best);
    Field f;
    while (r.next(f)) {
        if (f.number != 3 || f.type != pb::kLen) continue;
        bool matched = false;
        std::string action;
        bool has_action = false;
        Reader route = reader(f);
        Field g;
        while (route.next(g)) {
            if (g.number == 1 && g.type == pb::kLen) matched = route_matches(g, path);
            else if (g.number == 2 && g.type == pb::kLen) {
                action.assign(g.view());
                has_action = true;
            } else if (g.number == 3 || g.number == 7 || g.number == 18) {
                // Evaluated only if this route matches; recorded as "no route action".
                has_action = false;
                action.clear();
            }
        }
        if (!matched) continue;
        if (!has_action) throw std::runtime_error("DirectPath: matching route is not a forwarding route");
        // RouteAction { cluster = 1; cluster_header = 2; weighted_clusters = 3; cluster_specifier_plugin = 37; }
        RouteDecision d;
        Reader a = reader(action);
        Field h;
        while (a.next(h)) {
            if (h.number == 1 && h.type == pb::kLen) d.cluster.assign(h.view());
            else if (h.number == 2 || h.number == 3)
                throw std::runtime_error("DirectPath: cluster_header / weighted_clusters routes are not supported");
            else if (h.number == 37 && h.type == pb::kLen) {
                const std::string name(h.view());
                auto it = plugins.find(name);
                if (it == plugins.end()) throw std::runtime_error("DirectPath: route names unknown plugin '" + name + "'");
                if (it->second.first != kTypeRls)
                    throw std::runtime_error("DirectPath: cluster specifier plugin type '" + it->second.first +
                                             "' is not supported");
                Reader spec = reader(it->second.second);
                Field i;
                bool have = false;
                while (spec.next(i))
                    if (i.number == 1 && i.type == pb::kLen) {
                        d.rls = parse_rls_config(i);
                        have = true;
                    }
                if (!have) throw std::runtime_error("DirectPath: RLS plugin has no route lookup config");
                d.use_rls = true;
            }
        }
        if (d.cluster.empty() && !d.use_rls) throw std::runtime_error("DirectPath: matching route names no cluster");
        return d;
    }
    throw std::runtime_error("DirectPath: no route matches " + path);
}

std::string parse_cluster_eds_service(const std::string& cluster) {
    // Cluster { name = 1; type = 2; eds_cluster_config = 3 { service_name = 2; }; cluster_type = 38; }
    std::string name, service;
    uint64_t type = 0;  // STATIC when absent
    bool custom = false;
    Reader r = reader(cluster);
    Field f;
    while (r.next(f)) {
        if (f.number == 1 && f.type == pb::kLen) name.assign(f.view());
        else if (f.number == 2 && f.type == pb::kVarint) type = f.u;
        else if (f.number == 38) custom = true;
        else if (f.number == 3 && f.type == pb::kLen) {
            Reader e = reader(f);
            Field g;
            while (e.next(g))
                if (g.number == 2 && g.type == pb::kLen) service.assign(g.view());
        }
    }
    if (custom || type != 3)
        throw std::runtime_error("DirectPath: cluster '" + name + "' is not an EDS cluster (only EDS is supported)");
    return service.empty() ? name : service;
}

std::vector<Endpoint> parse_cluster_load_assignment(const std::string& cla) {
    // ClusterLoadAssignment.endpoints (2): LocalityLbEndpoints { lb_endpoints = 2; priority = 5; }
    // LbEndpoint { endpoint = 1; health_status = 2; } → Endpoint.address (1) → Address.socket_address (1)
    // → SocketAddress { address = 2; port_value = 3; }. Only the best (lowest) priority is used.
    std::map<uint64_t, std::vector<Endpoint>> by_priority;
    Reader r = reader(cla);
    Field f;
    while (r.next(f)) {
        if (f.number != 2 || f.type != pb::kLen) continue;
        uint64_t priority = 0;
        std::vector<Endpoint> eps;
        Reader l = reader(f);
        Field g;
        while (l.next(g)) {
            if (g.number == 5 && g.type == pb::kVarint) priority = g.u;
            if (g.number != 2 || g.type != pb::kLen) continue;
            uint64_t health = 0;
            Endpoint ep;
            Reader lb = reader(g);
            Field h;
            while (lb.next(h)) {
                if (h.number == 2 && h.type == pb::kVarint) health = h.u;
                if (h.number != 1 || h.type != pb::kLen) continue;
                Reader e = reader(h);
                Field i;
                while (e.next(i)) {
                    if (i.number != 1 || i.type != pb::kLen) continue;
                    Reader addr = reader(i);
                    Field j;
                    while (addr.next(j)) {
                        if (j.number != 1 || j.type != pb::kLen) continue;
                        Reader sa = reader(j);
                        Field k;
                        while (sa.next(k)) {
                            if (k.number == 2 && k.type == pb::kLen) ep.ip.assign(k.view());
                            else if (k.number == 3 && k.type == pb::kVarint) ep.port = static_cast<int>(k.u);
                        }
                    }
                }
            }
            // UNKNOWN (0), HEALTHY (1) and DEGRADED (5) are usable, as in gRPC.
            if ((health == 0 || health == 1 || health == 5) && !ep.ip.empty() && ep.port > 0) eps.push_back(ep);
        }
        auto& slot = by_priority[priority];
        slot.insert(slot.end(), eps.begin(), eps.end());
    }
    for (auto& kv : by_priority)
        if (!kv.second.empty()) return kv.second;
    return {};
}

std::map<std::string, std::string> rls_key_map(const RlsConfig& cfg, const std::string& authority,
                                               const std::string& service, const std::string& method,
                                               const Metadata& request_headers) {
    const RlsConfig::KeyBuilder* kb = nullptr;
    for (const auto& b : cfg.grpc_keybuilders)
        for (const auto& n : b.names) {
            if (n.first != service) continue;
            if (n.second == method) { kb = &b; break; }       // exact method wins
            if (n.second.empty() && !kb) kb = &b;              // service-wide otherwise
        }
    std::map<std::string, std::string> keys;
    if (!kb) return keys;
    for (const auto& h : kb->headers)
        for (const std::string& name : h.second) {
            auto it = std::find_if(request_headers.begin(), request_headers.end(),
                                   [&](const auto& kv) { return kv.first == name; });
            if (it != request_headers.end()) {
                keys[h.first] = it->second;
                break;
            }
        }
    if (!kb->host_key.empty()) keys[kb->host_key] = authority;
    if (!kb->service_key.empty()) keys[kb->service_key] = service;
    if (!kb->method_key.empty()) keys[kb->method_key] = method;
    for (const auto& kv : kb->constant_keys) keys[kv.first] = kv.second;
    return keys;
}

std::string rls_request(const std::map<std::string, std::string>& key_map) {
    // RouteLookupRequest { target_type = 3; key_map = 4; reason = 5 (REASON_MISS = 1); }
    std::string req;
    pb::put_bytes(req, 3, "grpc");
    for (const auto& kv : key_map) {
        std::string entry;
        pb::put_bytes(entry, 1, kv.first);
        pb::put_bytes(entry, 2, kv.second);
        pb::put_bytes(req, 4, entry);
    }
    pb::put_uint(req, 5, 1);
    return req;
}

std::vector<std::string> parse_rls_response(const std::string& response) {
    std::vector<std::string> targets;  // RouteLookupResponse { targets = 3; }
    Reader r = reader(response);
    Field f;
    while (r.next(f))
        if (f.number == 3 && f.type == pb::kLen) targets.emplace_back(f.view());
    return targets;
}

// ── Resolver ────────────────────────────────────────────────────────────────

struct DirectPathResolver::Rls {
    std::mutex mu;
    std::shared_ptr<GrpcConnection> conn;
    std::string host;
    int port = 443;
    struct Entry {
        std::string cluster;
        std::chrono::steady_clock::time_point expires;
    };
    std::map<std::map<std::string, std::string>, Entry> cache;
};

DirectPathResolver::DirectPathResolver(std::string ca_file) : ca_file_(std::move(ca_file)), rls_(new Rls) {
    // Node identity as the c2p resolver builds it (design §10).
    std::random_device rd;
    std::mt19937_64 mt(rd());
    node_id_ = "C2P-" + std::to_string(std::uniform_int_distribution<uint64_t>(1, UINT64_MAX)(mt));
    const std::string zone_path = gce_metadata_get("instance/zone", 5000);  // projects/<n>/zones/<zone>
    zone_ = zone_path.substr(zone_path.rfind('/') + 1);
    {
        std::lock_guard<std::mutex> g(mu_);
        ads_connect_locked();
    }
    reader_ = std::thread([this] { ads_loop(); });
}

DirectPathResolver::~DirectPathResolver() {
    {
        std::lock_guard<std::mutex> g(mu_);
        stop_ = true;
        if (ads_conn_ && ads_q_) ads_conn_->cancel(ads_call_);
    }
    cv_.notify_all();
    if (reader_.joinable()) reader_.join();
    if (ads_q_) ads_q_->wait_closed();
}

void DirectPathResolver::ads_send_locked(const std::string& type_url, const std::vector<std::string>& names,
                                         const std::string& version, const std::string& nonce) {
    // DiscoveryRequest { version_info = 1; node = 2; resource_names = 3; type_url = 4; response_nonce = 5; }
    std::string req;
    if (!version.empty()) pb::put_bytes(req, 1, version);
    if (!node_sent_) {
        // Node { id = 1; locality = 4 { zone = 2 }; user_agent_name = 6; client_features = 10; }
        std::string locality;
        pb::put_bytes(locality, 2, zone_);
        std::string node;
        pb::put_bytes(node, 1, node_id_);
        pb::put_bytes(node, 4, locality);
        pb::put_bytes(node, 6, "opteryx");
        pb::put_bytes(node, 10, "envoy.lb.does_not_support_overprovisioning");
        pb::put_bytes(node, 10, "xds.config.resource-in-sotw");
        pb::put_bytes(req, 2, node);
        node_sent_ = true;
    }
    for (const std::string& n : names) pb::put_bytes(req, 3, n);
    pb::put_bytes(req, 4, type_url);
    if (!nonce.empty()) pb::put_bytes(req, 5, nonce);
    std::string body;
    GrpcConnection::frame_message(body, req);
    ads_conn_->send(ads_call_, std::move(body), false);
}

void DirectPathResolver::ads_connect_locked() {
    ads_conn_ = std::make_shared<GrpcConnection>(std::make_unique<TlsTransport>(kXdsServer, 443, ca_file_), kXdsServer);
    ads_q_ = std::make_unique<MessageQueue>();
    Metadata md;
    md.emplace_back("authorization", "Bearer " + gce_access_token());
    ads_call_ = ads_conn_->start_call(kAdsPath, std::move(md), "", ads_q_.get(), /*half_close=*/false);
    node_sent_ = false;
    // Subscribe to everything already wanted (all of it again after a reconnect).
    ads_send_locked(kTypeListener, {kListenerName}, versions_[kTypeListener], "");
    std::vector<std::string> clusters, services;
    for (const auto& kv : cluster_eds_) clusters.push_back(kv.first);
    for (const auto& kv : eds_known_) services.push_back(kv.first);
    if (!clusters.empty()) ads_send_locked(kTypeCluster, clusters, versions_[kTypeCluster], "");
    if (!services.empty()) ads_send_locked(kTypeEds, services, versions_[kTypeEds], "");
}

void DirectPathResolver::on_ads_response(const std::string& msg) {
    // DiscoveryResponse { version_info = 1; resources = 2 (Any); type_url = 4; nonce = 5; }
    std::string version, type_url, nonce;
    std::vector<std::pair<std::string, std::string>> resources;  // (type, value)
    Reader r = reader(msg);
    Field f;
    while (r.next(f)) {
        if (f.type != pb::kLen) continue;
        if (f.number == 1) version.assign(f.view());
        else if (f.number == 4) type_url.assign(f.view());
        else if (f.number == 5) nonce.assign(f.view());
        else if (f.number == 2) {
            std::string type, value;
            parse_any(f, type, value);
            if (type == kTypeResource) {  // Resource { resource = 2 (Any) } — "resource-in-sotw"
                Reader w = reader(value);
                Field g;
                std::string inner_type, inner;
                while (w.next(g))
                    if (g.number == 2 && g.type == pb::kLen) parse_any(g, inner_type, inner);
                type = inner_type;
                value = inner;
            }
            resources.emplace_back(type, value);
        }
    }

    std::lock_guard<std::mutex> g(mu_);
    try {
        for (const auto& res : resources) {
            if (res.first != type_url) throw std::runtime_error("resource type " + res.first + " in a " + type_url + " response");
            if (type_url == kTypeListener) {
                listener_ = res.second;
                have_listener_ = true;
            } else if (type_url == kTypeCluster) {
                // Cluster.name (1) says which subscription this answers.
                Reader c = reader(res.second);
                Field h;
                std::string name;
                while (c.next(h))
                    if (h.number == 1 && h.type == pb::kLen) name.assign(h.view());
                if (!cluster_eds_.count(name)) continue;
                const std::string service = parse_cluster_eds_service(res.second);
                cluster_eds_[name] = service;
                if (!eds_known_.count(service)) {
                    eds_known_[service] = false;
                    std::vector<std::string> services;
                    for (const auto& kv : eds_known_) services.push_back(kv.first);
                    ads_send_locked(kTypeEds, services, versions_[kTypeEds], "");
                }
            } else if (type_url == kTypeEds) {
                Reader c = reader(res.second);
                Field h;
                std::string name;
                while (c.next(h))
                    if (h.number == 1 && h.type == pb::kLen) name.assign(h.view());
                if (!eds_known_.count(name)) continue;
                eds_[name] = parse_cluster_load_assignment(res.second);
                eds_known_[name] = true;
            }
        }
        versions_[type_url] = version;
        ads_error_.clear();
    } catch (const std::exception& e) {
        // A NACK keeps the last good version; the error is what a waiting caller sees.
        ads_error_ = std::string("DirectPath: rejected ") + type_url + ": " + e.what();
    }
    std::vector<std::string> names;
    if (type_url == kTypeListener) names = {kListenerName};
    else if (type_url == kTypeCluster) for (const auto& kv : cluster_eds_) names.push_back(kv.first);
    else if (type_url == kTypeEds) for (const auto& kv : eds_known_) names.push_back(kv.first);
    ads_send_locked(type_url, names, versions_[type_url], nonce);
    cv_.notify_all();
}

void DirectPathResolver::ads_loop() {
    int backoff_ms = 500;
    for (;;) {
        MessageQueue* q;
        {
            std::lock_guard<std::mutex> g(mu_);
            if (stop_) return;
            q = ads_q_.get();
        }
        std::string msg;
        if (q->next(msg, std::chrono::milliseconds(1000))) {
            try {
                on_ads_response(msg);
            } catch (const std::exception& e) {
                std::lock_guard<std::mutex> g(mu_);
                ads_error_ = std::string("DirectPath: bad ADS response: ") + e.what();
            }
            backoff_ms = 500;
            continue;
        }
        if (q->timed_out()) continue;
        // The stream ended: record why, then reconnect and resubscribe.
        std::unique_lock<std::mutex> lk(mu_);
        if (stop_) return;
        ads_error_ = "DirectPath: ADS stream ended: status " + std::to_string(q->status()) + " " + q->message();
        cv_.notify_all();
        cv_.wait_for(lk, std::chrono::milliseconds(backoff_ms), [this] { return stop_; });
        if (stop_) return;
        backoff_ms = std::min(backoff_ms * 2, 30000);
        try {
            std::unique_ptr<MessageQueue> old = std::move(ads_q_);
            ads_connect_locked();
        } catch (const std::exception& e) {
            ads_error_ = std::string("DirectPath: ADS reconnect failed: ") + e.what();
            // Keep a closed queue in place so the next pass waits and retries.
            ads_q_ = std::make_unique<MessageQueue>();
            ads_q_->on_close(kUnavailable, e.what());
        }
    }
}

std::string DirectPathResolver::cluster_for(const std::string& bucket, const RouteDecision& route, int timeout_ms) {
    if (!route.use_rls) return route.cluster;
    Metadata headers;
    headers.emplace_back("x-goog-request-params", "bucket=" + url_encode("projects/_/buckets/" + bucket));
    const auto keys = rls_key_map(route.rls, kStorageAuthority, kStorageService, kReadObjectMethod, headers);

    std::lock_guard<std::mutex> g(rls_->mu);
    const auto now = std::chrono::steady_clock::now();
    auto hit = rls_->cache.find(keys);
    if (hit != rls_->cache.end() && now < hit->second.expires) return hit->second.cluster;

    std::string host;
    int port = 443;
    parse_target(route.rls.lookup_service, host, port);
    if (!rls_->conn || !rls_->conn->alive() || rls_->host != host || rls_->port != port) {
        rls_->conn = std::make_shared<GrpcConnection>(std::make_unique<TlsTransport>(host, port, ca_file_), host);
        rls_->host = host;
        rls_->port = port;
    }
    MessageQueue q;
    Metadata md;
    md.emplace_back("authorization", "Bearer " + gce_access_token());
    md.emplace_back("grpc-timeout", std::to_string(timeout_ms) + "m");
    std::string body;
    GrpcConnection::frame_message(body, rls_request(keys));
    rls_->conn->start_call(kRlsPath, std::move(md), std::move(body), &q);
    std::string resp;
    const bool got = q.next(resp, std::chrono::milliseconds(timeout_ms));
    q.wait_closed();
    if (!got || q.status() != kOk)
        throw std::runtime_error("DirectPath: route lookup for bucket " + bucket + " failed: status " +
                                 std::to_string(q.status()) + " " + q.message());
    const std::vector<std::string> targets = parse_rls_response(resp);
    if (targets.empty()) throw std::runtime_error("DirectPath: route lookup for bucket " + bucket + " returned no target");
    const std::string& target = targets.front();
    if (target == route.rls.default_target)
        throw std::runtime_error("DirectPath is not available for bucket " + bucket +
                                 ": route lookup sent it to the front end (" + target + ")");
    rls_->cache[keys] = Rls::Entry{target, now + std::chrono::seconds(route.rls.max_age_s)};
    return target;
}

std::vector<Endpoint> DirectPathResolver::endpoints(const std::string& bucket, int timeout_ms) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    auto timeout_error = [&](const char* waiting_for) {
        std::string why = ads_error_.empty() ? std::string("no answer") : ads_error_;
        return std::runtime_error(std::string("DirectPath discovery timed out waiting for ") + waiting_for + " (" +
                                  why + ")");
    };

    RouteDecision route;
    {
        std::unique_lock<std::mutex> lk(mu_);
        if (!cv_.wait_until(lk, deadline, [this] { return have_listener_; })) throw timeout_error("the listener");
        route = parse_listener_route(listener_, kStorageAuthority,
                                     std::string("/") + kStorageService + "/" + kReadObjectMethod);
    }
    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
    const std::string cluster = cluster_for(bucket, route, static_cast<int>(std::max<int64_t>(left.count(), 1)));

    std::unique_lock<std::mutex> lk(mu_);
    if (!cluster_eds_.count(cluster)) {
        cluster_eds_[cluster] = "";
        std::vector<std::string> clusters;
        for (const auto& kv : cluster_eds_) clusters.push_back(kv.first);
        ads_send_locked(kTypeCluster, clusters, versions_[kTypeCluster], "");
    }
    if (!cv_.wait_until(lk, deadline, [&] {
            const std::string& svc = cluster_eds_[cluster];
            return !svc.empty() && eds_known_[svc];
        }))
        throw timeout_error(("cluster " + cluster).c_str());
    const std::vector<Endpoint>& eps = eds_[cluster_eds_[cluster]];
    if (eps.empty()) throw std::runtime_error("DirectPath: cluster " + cluster + " has no healthy endpoints");
    return eps;
}

}  // namespace gcs_grpc
