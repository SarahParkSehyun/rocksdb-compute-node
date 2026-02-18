// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).
//
// eRPC-based BloomCheck client library
// Thread-local Rpc design for RocksDB integration

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

#include "tools/bloomcheck.pb.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

struct ErpcBloomCheckClientImpl;

class ErpcBloomCheckClient {
 public:
  static constexpr uint8_t kReqTypeNoop = 1;
  static constexpr uint8_t kReqTypeCheck = 2;

  ErpcBloomCheckClient(const std::string& local_uri, const std::string& server_uri,
                       int numa_node, uint8_t rpc_id, uint8_t server_rpc_id);

  ErpcBloomCheckClient(const ErpcBloomCheckClient&) = delete;
  ErpcBloomCheckClient& operator=(const ErpcBloomCheckClient&) = delete;
  ErpcBloomCheckClient(ErpcBloomCheckClient&&) = delete;
  ErpcBloomCheckClient& operator=(ErpcBloomCheckClient&&) = delete;

  ~ErpcBloomCheckClient();

  // Initializes process-scoped context (Nexus only).
  bool Init();

  // Synchronous Noop RPC call through per-thread Rpc context.
  bool Noop(uint64_t* server_ts_ns = nullptr, uint64_t* rtt_ns = nullptr,
            size_t* slot_idx = nullptr);

  // Synchronous Check RPC (BatchRequest -> BatchResponse).
  bool CheckBatch(const bloomcheck::BatchRequest& request,
                  bloomcheck::BatchResponse* response,
                  uint64_t* rtt_ns = nullptr, size_t* slot_idx = nullptr);

  // Optional explicit shutdown for standalone tools.
  void Shutdown();

  bool IsConnected() const;
  size_t PoolSize() const;

 private:
  std::unique_ptr<ErpcBloomCheckClientImpl> impl_;
};

}  // namespace ROCKSDB_NAMESPACE


