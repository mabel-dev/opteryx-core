#include "directpath_fetch.hpp"

#include <chrono>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>

#include "gcs_grpc_client.hpp"

namespace gcs_grpc {

namespace {

std::mutex g_mu;
std::unique_ptr<GcsGrpcClient> g_client;
int g_connections = 0;

GcsGrpcClient& process_client(int connections) {
    std::lock_guard<std::mutex> g(g_mu);
    if (!g_client) {
        GcsGrpcClient::Options o;
        o.direct_path = true;
        o.connections = connections;
        // The wheel's vendored OpenSSL has no usable default verify paths on the host.
        o.ca_file = HttpClient::find_ca_bundle();
        o.on_received = &http_link::note_received;
        g_client = std::make_unique<GcsGrpcClient>(o);
        g_connections = connections;
    } else if (connections != g_connections) {
        throw std::logic_error("GCS_GRPC_CONNECTIONS changed from " + std::to_string(g_connections) + " to " +
                               std::to_string(connections) +
                               " after the DirectPath client was created; it is per process");
    }
    return *g_client;
}

}  // namespace

std::vector<std::vector<uint8_t>> directpath_get_many(const std::string& gs_path,
                                                      const std::vector<std::pair<int64_t, int64_t>>& ranges,
                                                      const std::string& authorization, int connections,
                                                      const HttpTuning& tuning) {
    const size_t slash = gs_path.find('/', 5);
    if (gs_path.compare(0, 5, "gs://") != 0 || slash == std::string::npos || slash == 5 || slash + 1 == gs_path.size())
        throw std::invalid_argument("DirectPath: not a gs://bucket/object path: " + gs_path);
    if (authorization.empty())
        throw HttpError("DirectPath (GCS_TRANSPORT=grpc) needs a bearer credential and this scan has none: " + gs_path,
                        false, 0);
    const std::string bucket = gs_path.substr(5, slash - 5);
    const std::string object = gs_path.substr(slash + 1);

    std::vector<ByteRange> br;
    br.reserve(ranges.size());
    long long bytes = 0;
    for (const auto& r : ranges) {
        br.push_back(ByteRange{r.first, r.second});
        bytes += r.second;
    }
    GcsGrpcClient& client = process_client(connections);
    // shortcut: a retry re-reads every range of the batch, not only the failed
    // ones (read_many is all-or-nothing); upgrade if retries show up in the ladder.
    for (int attempt = 0;; ++attempt) {
        try {
            // Admitted before the deadline starts, as get_many() does.
            http_link::Admission admission(bytes, tuning);
            return client.read_many(bucket, object, br, authorization,
                                    http_link::timeout_ms(static_cast<long>(bytes), tuning));
        } catch (const GcsGrpcError& e) {
            if (!e.retryable) throw HttpError(std::string("DirectPath read failed: ") + e.what(), false, 0);
            if (attempt >= tuning.max_retries)
                throw HttpError("DirectPath read: exhausted " + std::to_string(tuning.max_retries) + " retries (" +
                                    e.what() + ")",
                                true, 0);
        }
        http_link::count_retries(br.size());
        std::this_thread::sleep_for(std::chrono::milliseconds(http_link::backoff_ms(attempt)));
    }
}

}  // namespace gcs_grpc
