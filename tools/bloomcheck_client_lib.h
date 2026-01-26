// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#pragma once

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <string>
#include <thread>
#include <vector>
#include "bloomcheck.grpc.pb.h"

namespace ROCKSDB_NAMESPACE {

// BloomCheckServiceClient provides a C++ client interface for the BloomCheck gRPC service
class BloomCheckServiceClient {
 public:
  explicit BloomCheckServiceClient(const std::string& server_address)
      : channel_count_(1) {
    // Read channel count from environment variable
    const char* env = std::getenv("BLOOMCHECK_CHANNELS");
    if (env != nullptr) {
      int count = std::atoi(env);
      if (count > 0) {
        channel_count_ = static_cast<size_t>(count);
      }
    }

    // Create channel pool
    channels_.reserve(channel_count_);
    stubs_.reserve(channel_count_);
    
    // Set channel arguments to use local subchannel pool (prevent connection sharing)
    grpc::ChannelArguments channel_args;
    channel_args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    
    for (size_t i = 0; i < channel_count_; ++i) {
      auto channel = grpc::CreateCustomChannel(
          server_address, grpc::InsecureChannelCredentials(), channel_args);
      channels_.push_back(channel);
      stubs_.push_back(bloomcheck::BloomCheck::NewStub(channel));
    }
    
    // Debug: Log channel pool size
    fprintf(stderr, "[BloomCheckClient] Initialized: channels_.size()=%zu, stubs_.size()=%zu, channel_count_=%zu\n",
            channels_.size(), stubs_.size(), channel_count_);
  }

  // Get channel index for current thread
  size_t GetChannelIndex() const {
    if (channel_count_ == 1) {
      return 0;
    }
    // Use thread id hash to select channel
    std::hash<std::thread::id> hasher;
    uint64_t tid_hash = static_cast<uint64_t>(hasher(std::this_thread::get_id()));
    return static_cast<size_t>(tid_hash % channel_count_);
  }

  // Get channel count (for logging)
  size_t GetChannelCount() const { return channel_count_; }

  // Check if a key may exist in the given SST file (by file_number)
  // Returns true if key may exist (or on error for safety), false if definitely not
  bool CheckKey(const std::string& user_key, uint64_t sst_file_number) {
    bloomcheck::BatchRequest request;
    bloomcheck::KeyRequest* key_req = request.add_reqs();
    key_req->set_user_key(user_key);
    key_req->add_sst_file_numbers(sst_file_number);

    bloomcheck::BatchResponse response;
    size_t idx = GetChannelIndex();
    grpc::ClientContext context;
    grpc::Status status = stubs_[idx]->Check(&context, request, &response);

    // On error, return true (safe default to prevent false negatives)
    if (!status.ok()) {
      return true;
    }

    if (response.resps_size() > 0) {
      const auto& key_resp = response.resps(0);
      // If any SST file_number is in may_match, return true
      return key_resp.may_match_sst_file_numbers_size() > 0;
    }

    return true;  // Safe default
  }

  // Check multiple keys against multiple SST files
  bool CheckBatch(const bloomcheck::BatchRequest& request,
                  bloomcheck::BatchResponse* response, size_t* channel_idx = nullptr) {
    size_t idx = GetChannelIndex();
    if (channel_idx != nullptr) {
      *channel_idx = idx;
    }
    // Debug: Log selected channel index (sampled)
    static std::atomic<uint64_t> call_count(0);
    uint64_t count = call_count.fetch_add(1, std::memory_order_relaxed);
    if (count % 1000 == 0) {  // Log every 1000th call
      fprintf(stderr, "[BloomCheckClient] CheckBatch: selected idx=%zu (channels_.size()=%zu, stubs_.size()=%zu, call_count=%llu)\n",
              idx, channels_.size(), stubs_.size(), (unsigned long long)count);
    }
    grpc::ClientContext context;
    grpc::Status status = stubs_[idx]->Check(&context, request, response);
    return status.ok();
  }

  // Noop RPC for latency measurement (sync unary)
  bool Noop(size_t* channel_idx = nullptr) {
    size_t idx = GetChannelIndex();
    if (channel_idx != nullptr) {
      *channel_idx = idx;
    }
    // Debug: Log selected channel index (sampled)
    static std::atomic<uint64_t> call_count(0);
    uint64_t count = call_count.fetch_add(1, std::memory_order_relaxed);
    if (count % 1000 == 0) {  // Log every 1000th call
      fprintf(stderr, "[BloomCheckClient] Noop: selected idx=%zu (channels_.size()=%zu, stubs_.size()=%zu, call_count=%llu)\n",
              idx, channels_.size(), stubs_.size(), (unsigned long long)count);
    }
    bloomcheck::NoopRequest request;
    bloomcheck::NoopResponse response;
    grpc::ClientContext context;
    grpc::Status status = stubs_[idx]->Noop(&context, request, &response);
    return status.ok();
  }

  // Warm-up: Send one RPC through each channel to establish connections
  void WarmupChannels() {
    fprintf(stderr, "[BloomCheckClient] Warmup: Starting warm-up for %zu channels\n", channel_count_);
    for (size_t i = 0; i < channel_count_; ++i) {
      bloomcheck::NoopRequest request;
      bloomcheck::NoopResponse response;
      grpc::ClientContext context;
      grpc::Status status = stubs_[i]->Noop(&context, request, &response);
      fprintf(stderr, "[BloomCheckClient] Warmup: channel[%zu] RPC %s\n",
              i, status.ok() ? "OK" : "FAILED");
    }
    fprintf(stderr, "[BloomCheckClient] Warmup: Completed warm-up for %zu channels\n", channel_count_);
  }

 private:
  size_t channel_count_;
  std::vector<std::shared_ptr<grpc::Channel>> channels_;
  std::vector<std::unique_ptr<bloomcheck::BloomCheck::Stub>> stubs_;
};

}  // namespace ROCKSDB_NAMESPACE

