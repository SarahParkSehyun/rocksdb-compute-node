// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <string>
#include <thread>
#include <vector>
#include "bloomcheck.grpc.pb.h"

namespace ROCKSDB_NAMESPACE {

// Simple semaphore implementation for inflight RPC limiting
class Semaphore {
 public:
  explicit Semaphore(size_t max_count) : max_count_(max_count), current_count_(max_count) {}

  void Acquire() {
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return current_count_ > 0; });
    --current_count_;
  }

  void Release() {
    std::lock_guard<std::mutex> lock(mutex_);
    ++current_count_;
    cv_.notify_one();
  }

  size_t GetMaxCount() const { return max_count_; }
  size_t GetCurrentCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return current_count_;
  }

 private:
  size_t max_count_;
  size_t current_count_;
  mutable std::mutex mutex_;  // mutable to allow locking in const methods
  std::condition_variable cv_;
};

// RAII wrapper for semaphore acquire/release
class SemaphoreGuard {
 public:
  explicit SemaphoreGuard(Semaphore* semaphore) : semaphore_(semaphore) {
    if (semaphore_) {
      semaphore_->Acquire();
    }
  }

  ~SemaphoreGuard() {
    if (semaphore_) {
      semaphore_->Release();
    }
  }

  // Non-copyable
  SemaphoreGuard(const SemaphoreGuard&) = delete;
  SemaphoreGuard& operator=(const SemaphoreGuard&) = delete;

 private:
  Semaphore* semaphore_;
};

// Per-RPC state object for async BloomCheck calls
class AsyncBloomCheckCall {
 public:
  AsyncBloomCheckCall() : ok_(false), completed_(false) {}

  // Synchronization
  void Wait() {
    // No spin-wait: directly use condition variable
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return completed_.load(std::memory_order_acquire); });
  }

  void Signal() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      completed_ = true;
    }
    cv_.notify_one();
  }

  // RPC state
  grpc::ClientContext context_;
  bloomcheck::BatchRequest request_;
  bloomcheck::BatchResponse response_;
  grpc::Status status_;
  size_t channel_idx_ = 0;
  bool ok_;

 private:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> completed_;  // Atomic for lock-free spin-wait check
};

// BloomCheckServiceClient provides a C++ client interface for the BloomCheck gRPC service
class BloomCheckServiceClient {
 public:
  explicit BloomCheckServiceClient(const std::string& server_address)
      : channel_count_(32), shutdown_(false) {  // Default: 32 channels for 32 physical cores
    // Read channel count from environment variable (default: 32)
    const char* env = std::getenv("BLOOMCHECK_CHANNELS");
    if (env != nullptr) {
      int count = std::atoi(env);
      if (count > 0) {
        channel_count_ = static_cast<size_t>(count);
      }
    }

    // Read CQ count from environment variable (default: 1 per channel, ensuring 1:1 mapping)
    size_t cq_count = channel_count_;
    const char* cq_env = std::getenv("BLOOMCHECK_CQS");
    if (cq_env != nullptr) {
      int count = std::atoi(cq_env);
      if (count > 0) {
        cq_count = static_cast<size_t>(count);
      }
    }
    
    // Ensure CQ count matches channel count for 1:1 mapping
    if (cq_count != channel_count_) {
      fprintf(stderr, "[BloomCheckClient] Warning: CQ count (%zu) != channel count (%zu), adjusting CQ count to match\n", cq_count, channel_count_);
      cq_count = channel_count_;
    }

    // Read MAX_INFLIGHT from environment variable (default: unlimited, 0 means unlimited)
    size_t max_inflight = 0;  // 0 means unlimited
    const char* inflight_env = std::getenv("BLOOMCHECK_MAX_INFLIGHT");
    if (inflight_env != nullptr) {
      int count = std::atoi(inflight_env);
      if (count > 0) {
        max_inflight = static_cast<size_t>(count);
      }
    }
    
    // Create semaphore if inflight limiting is enabled
    if (max_inflight > 0) {
      inflight_semaphore_ = std::make_unique<Semaphore>(max_inflight);
    }

    // Create channel pool with unique identifiers to prevent connection sharing
    channels_.reserve(channel_count_);
    stubs_.reserve(channel_count_);
    
    for (size_t i = 0; i < channel_count_; ++i) {
      // Create unique channel arguments for each channel to ensure independent TCP connections
      grpc::ChannelArguments channel_args;
      channel_args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
      // Set unique channel identifier to prevent connection pooling
      std::string channel_id = "bloomcheck_channel_" + std::to_string(i);
      channel_args.SetString(GRPC_ARG_CHANNEL_ID, channel_id);
      
      // Minimize gRPC executor threads to reduce background task interference with poller cores
      // Use minimal executor pool size to avoid background tasks on poller-dedicated cores (0-3)
      channel_args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, -1);
      channel_args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, -1);
      // Disable resolver background threads that might interfere
      channel_args.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
      
      auto channel = grpc::CreateCustomChannel(
          server_address, grpc::InsecureChannelCredentials(), channel_args);
      channels_.push_back(channel);
      stubs_.push_back(bloomcheck::BloomCheck::NewStub(channel));
    }

    // Create CompletionQueues
    cqs_.reserve(cq_count);
    for (size_t i = 0; i < cq_count; ++i) {
      cqs_.emplace_back(std::make_unique<grpc::CompletionQueue>());
    }

    // Start CQ poller threads with CPU affinity (1:1 mapping to physical cores 0-31)
    // Poller threads are pinned to cores 0-(channel_count-1) to ensure complete isolation
    // RocksDB worker threads use system default scheduling (no CPU affinity restriction)
    const int poller_cpu_start = 0;
    const int poller_cpu_end = static_cast<int>(channel_count_) - 1;
    
    cq_poller_threads_.reserve(cq_count);
    for (size_t i = 0; i < cq_count; ++i) {
      cq_poller_threads_.emplace_back(
          std::thread(&BloomCheckServiceClient::CQPollerThread, this, i));
      
      // Pin poller thread to specific physical core (0 to channel_count-1)
      // For 4 channels: cores 0, 1, 2, 3
      // For 32 channels: cores 0-31
      int cpu_id = poller_cpu_start + static_cast<int>(i);
      cpu_set_t cpuset;
      CPU_ZERO(&cpuset);
      CPU_SET(cpu_id, &cpuset);
      
      pthread_t thread_handle = cq_poller_threads_.back().native_handle();
      int ret = pthread_setaffinity_np(thread_handle, sizeof(cpu_set_t), &cpuset);
      if (ret == 0) {
        // Set high priority for poller threads to minimize interference from RocksDB workers
        struct sched_param param;
        param.sched_priority = sched_get_priority_max(SCHED_FIFO) - 1;  // High priority, but not max
        pthread_setschedparam(thread_handle, SCHED_FIFO, &param);
        fprintf(stderr, "[BloomCheckClient] CQ poller thread %zu pinned to CPU %d (poller range: %d-%d) with high priority\n", 
                i, cpu_id, poller_cpu_start, poller_cpu_end);
      } else {
        fprintf(stderr, "[BloomCheckClient] Warning: Failed to pin CQ poller thread %zu to CPU %d (errno=%d)\n", i, cpu_id, ret);
      }
    }
    
    fprintf(stderr, "[BloomCheckClient] Poller isolation: %d poller threads pinned to CPUs %d-%d, "
            "RocksDB workers use system default scheduling (all CPUs 0-31)\n",
            static_cast<int>(cq_count), poller_cpu_start, poller_cpu_end);
    
    // Log initialization info
    fprintf(stderr, "[BloomCheckClient] Initialized: stubs_.size()=%zu, cqs_.size()=%zu, poller_thread_count=%zu\n",
            stubs_.size(), cqs_.size(), cq_poller_threads_.size());
  }

  ~BloomCheckServiceClient() {
    // Shutdown all CQs
    {
      std::lock_guard<std::mutex> lock(shutdown_mutex_);
      shutdown_ = true;
    }
    for (auto& cq : cqs_) {
      cq->Shutdown();
    }
    // Wait for poller threads to finish
    for (auto& thread : cq_poller_threads_) {
      if (thread.joinable()) {
        thread.join();
      }
    }
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

  // Check multiple keys against multiple SST files (synchronous - for backward compatibility)
  bool CheckBatch(const bloomcheck::BatchRequest& request,
                  bloomcheck::BatchResponse* response, size_t* channel_idx = nullptr) {
    size_t idx = GetChannelIndex();
    if (channel_idx != nullptr) {
      *channel_idx = idx;
    }
    grpc::ClientContext context;
    grpc::Status status = stubs_[idx]->Check(&context, request, response);
    return status.ok();
  }

  // Async version: Check multiple keys against multiple SST files
  // Blocks the calling thread until RPC completes, but does NOT participate in pollset_work
  // Respects MAX_INFLIGHT limit if set via BLOOMCHECK_MAX_INFLIGHT environment variable
  bool AsyncCheckBatch(const bloomcheck::BatchRequest& request,
                       bloomcheck::BatchResponse* response, size_t* channel_idx = nullptr) {
    // Acquire semaphore slot if inflight limiting is enabled
    // This will block if MAX_INFLIGHT limit is reached
    SemaphoreGuard guard(inflight_semaphore_.get());

    // Select channel and CQ
    size_t stub_idx = GetChannelIndex();
    size_t cq_idx = stub_idx % cqs_.size();
    
    if (channel_idx != nullptr) {
      *channel_idx = stub_idx;
    }

    // Create per-RPC state object (must outlive the RPC)
    auto call = std::make_shared<AsyncBloomCheckCall>();
    call->request_ = request;
    call->channel_idx_ = stub_idx;

    // Start async RPC
    std::unique_ptr<grpc::ClientAsyncResponseReader<bloomcheck::BatchResponse>> rpc(
        stubs_[stub_idx]->AsyncCheck(&call->context_, call->request_, cqs_[cq_idx].get()));

    // Finish is an async operation - tag must remain valid until CQ poller processes it
    void* tag = call.get();
    rpc->Finish(&call->response_, &call->status_, tag);

    // Keep rpc alive until Wait() completes (rpc destructor is safe after Finish)
    // Block on condition_variable (NOT on gRPC polling)
    call->Wait();

    // Copy results
    // Note: SemaphoreGuard destructor will release the semaphore slot here
    *response = call->response_;
    return call->status_.ok();
  }

  // Noop RPC for latency measurement (sync unary)
  bool Noop(size_t* channel_idx = nullptr) {
    size_t idx = GetChannelIndex();
    if (channel_idx != nullptr) {
      *channel_idx = idx;
    }
    // Debug: Log selected channel index (sampled)
    // static std::atomic<uint64_t> call_count(0);
    // uint64_t count = call_count.fetch_add(1, std::memory_order_relaxed);
    // if (count % 1000 == 0) {  // Log every 1000th call
    //   fprintf(stderr, "[BloomCheckClient] Noop: selected idx=%zu (channels_.size()=%zu, stubs_.size()=%zu, call_count=%llu)\n",
    //           idx, channels_.size(), stubs_.size(), (unsigned long long)count);
    // }
    bloomcheck::NoopRequest request;
    bloomcheck::NoopResponse response;
    grpc::ClientContext context;
    grpc::Status status = stubs_[idx]->Noop(&context, request, &response);
    return status.ok();
  }

  // Warm-up: Send one RPC through each channel to establish connections
  void WarmupChannels() {
    // fprintf(stderr, "[BloomCheckClient] Warmup: Starting warm-up for %zu channels\n", channel_count_);
    for (size_t i = 0; i < channel_count_; ++i) {
      bloomcheck::NoopRequest request;
      bloomcheck::NoopResponse response;
      grpc::ClientContext context;
      grpc::Status status = stubs_[i]->Noop(&context, request, &response);
      // fprintf(stderr, "[BloomCheckClient] Warmup: channel[%zu] RPC %s\n",
      //         i, status.ok() ? "OK" : "FAILED");
    }
    // fprintf(stderr, "[BloomCheckClient] Warmup: Completed warm-up for %zu channels\n", channel_count_);
  }

 private:
  // CompletionQueue poller thread (runs cq->Next() to handle async RPC completions)
  void CQPollerThread(size_t cq_idx) {
    // Set thread name for debugging/monitoring
    pthread_setname_np(pthread_self(), "bloom_poller");
    
    void* tag;
    bool ok;
    while (true) {
      // Check shutdown flag
      {
        std::lock_guard<std::mutex> lock(shutdown_mutex_);
        if (shutdown_) {
          break;
        }
      }

      // Block on CQ->Next() (this is the ONLY place that calls polling)
      bool got_event = cqs_[cq_idx]->Next(&tag, &ok);
      if (!got_event) {
        // SHUTDOWN
        break;
      }

      // Handle completed RPC
      AsyncBloomCheckCall* call = static_cast<AsyncBloomCheckCall*>(tag);
      call->ok_ = ok;
      call->Signal();  // Wake up waiting Get() thread
    }
  }

  size_t channel_count_;
  std::vector<std::shared_ptr<grpc::Channel>> channels_;
  std::vector<std::unique_ptr<bloomcheck::BloomCheck::Stub>> stubs_;
  std::vector<std::unique_ptr<grpc::CompletionQueue>> cqs_;
  std::vector<std::thread> cq_poller_threads_;
  std::mutex shutdown_mutex_;
  bool shutdown_;
  std::unique_ptr<Semaphore> inflight_semaphore_;  // For limiting concurrent inflight RPCs
};

}  // namespace ROCKSDB_NAMESPACE

