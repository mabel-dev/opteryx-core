// The engine's DirectPath entry point (design §3, GCS_TRANSPORT=grpc): byte
// ranges of one gs:// object through the process-wide GcsGrpcClient (R3), with
// HttpClient::get_many()'s contract — one buffer per range in order, HttpError
// (retryable or not) on failure, the scan's retry budget and byte-gate admission.
//
// Defined ONLY in pool_reader.so, so the process has exactly one client: the
// other extensions that compile io_pipeline.hpp resolve this symbol at load from
// pool_reader, which is loaded RTLD_GLOBAL (opteryx/connectors/parquet_io).
#pragma once

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "http_client.hpp"

namespace gcs_grpc {

std::vector<std::vector<uint8_t>> directpath_get_many(const std::string& gs_path,
                                                      const std::vector<std::pair<int64_t, int64_t>>& ranges,
                                                      const std::string& authorization, int connections,
                                                      const HttpTuning& tuning);

}  // namespace gcs_grpc
