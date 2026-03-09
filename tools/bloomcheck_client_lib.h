// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <grpcpp/grpcpp.h>
#include <memory>
#include <mutex>
#include <pthread.h>
#include <sched.h>
#include <string>
#include <thread>
#include <unordered_map>
#include <unistd.h>  // for sysconf, getpid
#include <sys/time.h>  // for gettimeofday
#include <sys/syscall.h>  // for SYS_gettid
#include <execinfo.h>  // for backtrace
#include <cxxabi.h>  // for __cxa_demangle
#include <vector>
#include "bloomcheck.grpc.pb.h"

namespace ROCKSDB_NAMESPACE {

inline uint64_t BloomNowMicros() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

// Simple semaphore implementation for inflight RPC limiting
class Semaphore {
 public:
  explicit Semaphore(size_t max_count) 
      : max_count_(max_count), current_count_(max_count),
        wait_cnt_(0), wait_us_total_(0), max_wait_us_(0) {}

  void Acquire() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    // Check if we need to wait (current_count_ == 0)
    if (current_count_ == 0) {
      // Measure wait time
      struct timeval start_tv, end_tv;
      gettimeofday(&start_tv, nullptr);
      
      // Wait for available slot
      cv_.wait(lock, [this] { return current_count_ > 0; });
      
      // Calculate wait time
      gettimeofday(&end_tv, nullptr);
      uint64_t wait_us = (end_tv.tv_sec - start_tv.tv_sec) * 1000000ULL + 
                         (end_tv.tv_usec - start_tv.tv_usec);
      
      // Update statistics
      wait_cnt_++;
      wait_us_total_ += wait_us;
      if (wait_us > max_wait_us_) {
        max_wait_us_ = wait_us;
      }
    }
    
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

  // Get statistics (thread-safe)
  void GetStats(uint64_t& wait_cnt, uint64_t& wait_us_total, uint64_t& max_wait_us) const {
    std::lock_guard<std::mutex> lock(mutex_);
    wait_cnt = wait_cnt_;
    wait_us_total = wait_us_total_;
    max_wait_us = max_wait_us_;
  }

 private:
  size_t max_count_;
  size_t current_count_;
  mutable std::mutex mutex_;  // mutable to allow locking in const methods
  std::condition_variable cv_;
  
  // Statistics for monitoring inflight limiting effectiveness
  uint64_t wait_cnt_;         // Number of times Acquire() had to wait
  uint64_t wait_us_total_;    // Total wait time in microseconds
  uint64_t max_wait_us_;      // Maximum single wait time in microseconds
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

// Base class for async RPC calls (allows CQ poller to handle different RPC types)
class CqTagBase {
 public:
  virtual ~CqTagBase() = default;
  virtual void Proceed(bool ok) = 0;
};

class AsyncRPCCallBase : public CqTagBase {
 public:
  AsyncRPCCallBase() : ok_(false), completed_(false) {}

  // Synchronization
  void Wait() {
    // Direct condition variable wait (no spin-wait)
    std::unique_lock<std::mutex> lock(mutex_);
    cv_.wait(lock, [this] { return completed_.load(std::memory_order_acquire); });
  }

  void Signal() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      signal_us_.store(BloomNowMicros(), std::memory_order_release);
      completed_ = true;
    }
    cv_.notify_one();
  }

  bool ok_;
  grpc::Status status_;
  size_t channel_idx_ = 0;
  std::atomic<uint64_t> signal_us_{0};
  void Proceed(bool ok) override {
    ok_ = ok;
    Signal();
  }

 protected:
  std::mutex mutex_;
  std::condition_variable cv_;
  std::atomic<bool> completed_;  // Atomic for lock-free spin-wait check
};

// Per-RPC state object for async BloomCheck calls
class AsyncBloomCheckCall : public AsyncRPCCallBase {
 public:
  AsyncBloomCheckCall() : AsyncRPCCallBase() {}

  // RPC state
  grpc::ClientContext context_;
  bloomcheck::BatchRequest request_;
  bloomcheck::BatchResponse response_;
};

// Per-RPC state object for async Noop calls
class AsyncNoopCall : public AsyncRPCCallBase {
 public:
  AsyncNoopCall() : AsyncRPCCallBase() {}

  // RPC state
  grpc::ClientContext context_;
  bloomcheck::NoopRequest request_;
  bloomcheck::NoopResponse response_;
};

// BloomCheckServiceClient provides a C++ client interface for the BloomCheck gRPC service
class BloomCheckServiceClient {
 public:
  struct RpcTimingBreakdown {
    uint64_t acquire_wait_us = 0;
    uint64_t submit_prep_us = 0;
    uint64_t wait_completion_us = 0;
    uint64_t wake_up_us = 0;
    uint64_t response_parse_us = 0;
  };

  explicit BloomCheckServiceClient(const std::string& server_address)
      : channel_count_(32),
        shutdown_(false),
        stream_stats_enabled_(false),
        stats_thread_started_(false) {  // Default: 32 channels for 32 physical cores
    
    // Log constructor invocation to track duplicate instances
    // IMPORTANT: This log MUST be at the very beginning of constructor body
    pid_t ctor_pid = getpid();
    void* ctor_this_ptr = static_cast<void*>(this);
    
    // Print stack trace to identify caller
    void* callstack[20];
    int frames = backtrace(callstack, 20);
    char** symbols = backtrace_symbols(callstack, frames);
    fprintf(stderr, "[BloomCheckClientCtor] ENTRY pid=%d this=%p channels=%zu server_address=%s\n",
            ctor_pid, ctor_this_ptr, channel_count_, server_address.c_str());
    fprintf(stderr, "[BloomCheckClientCtor] Stack trace (%d frames):\n", frames);
    for (int i = 1; i < frames && i < 8; ++i) {  // Skip frame 0 (this constructor), show top 7 frames
      if (symbols[i]) {
        fprintf(stderr, "  [%d] %s\n", i, symbols[i]);
      }
    }
    free(symbols);
    fflush(stderr);  // Force immediate output
    
    // Set gRPC polling strategy environment variable for pollset isolation
    // Use epoll1 (simple epoll) or poll instead of epollex (multi-queue shared model)
    // This helps ensure each channel uses independent pollset
    const char* poll_strategy = std::getenv("GRPC_POLL_STRATEGY");
    if (poll_strategy == nullptr) {
      // Default to epoll1 for better isolation (if not already set)
      setenv("GRPC_POLL_STRATEGY", "epoll1", 0);  // 0 = don't overwrite if exists
      // fprintf(stderr, "[BloomCheckClient] Set GRPC_POLL_STRATEGY=epoll1 for pollset isolation\n");
    } else {
      // fprintf(stderr, "[BloomCheckClient] Using existing GRPC_POLL_STRATEGY=%s\n", poll_strategy);
    }
    
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
      // fprintf(stderr, "[BloomCheckClient] Warning: CQ count (%zu) != channel count (%zu), adjusting CQ count to match\n", cq_count, channel_count_);
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
    // NOTE: This is a GLOBAL semaphore shared across all channels
    // All channels compete for the same inflight limit (not per-channel)
    // This is appropriate for "total concurrent RPC limit" goal
    if (max_inflight > 0) {
      inflight_semaphore_ = std::make_unique<Semaphore>(max_inflight);
      // fprintf(stderr, "[BloomCheckClient] Inflight limiting enabled: max_inflight=%zu (GLOBAL semaphore, shared across all %zu channels)\n", 
      //         max_inflight, channel_count_);
    } else {
      // fprintf(stderr, "[BloomCheckClient] Inflight limiting disabled (unlimited)\n");
    }

    use_streaming_ = false;
    streams_per_channel_ = 4;
    stream_max_pending_ = 4096;
    const char* streaming_env = std::getenv("BLOOMCHECK_USE_STREAMING");
    if (streaming_env != nullptr && std::atoi(streaming_env) == 1) {
      use_streaming_ = true;
    }
    const char* streams_env = std::getenv("BLOOMCHECK_STREAMS_PER_CHANNEL");
    if (streams_env != nullptr) {
      int stream_count = std::atoi(streams_env);
      if (stream_count > 0) {
        streams_per_channel_ = static_cast<size_t>(stream_count);
      }
    }
    const char* stream_pending_env = std::getenv("BLOOMCHECK_STREAM_MAX_PENDING");
    if (stream_pending_env != nullptr) {
      int pending_count = std::atoi(stream_pending_env);
      if (pending_count > 0) {
        stream_max_pending_ = static_cast<size_t>(pending_count);
      }
    }

    // Create channel pool with unique identifiers to prevent connection sharing
    channels_.reserve(channel_count_);
    stubs_.reserve(channel_count_);
    
    for (size_t i = 0; i < channel_count_; ++i) {
      // Create unique channel arguments for each channel to ensure independent TCP connections
      // and complete pollset isolation to eliminate epoll contention
      grpc::ChannelArguments channel_args;
      
      // CRITICAL: Force each channel to use independent subchannel pool
      // This prevents connection sharing and helps pollset separation
      channel_args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
      
      // Set unique channel identifier to prevent connection pooling
      // Each channel gets a unique ID to ensure OS-level socket/epoll separation
      std::string channel_id = "bloomcheck_channel_" + std::to_string(i);
      channel_args.SetString(GRPC_ARG_CHANNEL_ID, channel_id);
      
      // Optimization target: latency (minimizes batching and encourages immediate processing)
      // This helps ensure each channel's pollset is processed independently
      // Note: GRPC_ARG_OPTIMIZATION_TARGET may not be available in all gRPC versions
      // If compilation fails, comment out the following line
      // channel_args.SetInt(GRPC_ARG_OPTIMIZATION_TARGET, 0);  // 0 = latency optimization
      
      // Minimize gRPC executor threads to reduce background task interference with poller cores
      // Use minimal executor pool size to avoid background tasks on poller-dedicated cores
      channel_args.SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, -1);
      channel_args.SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, -1);
      // Disable Nagle's algorithm on client sockets to reduce small-RPC latency.
      channel_args.SetInt("grpc.tcp_nodelay", 1);
      // Increase HTTP/2 flow-control windows for higher throughput under concurrency.
      channel_args.SetInt("grpc.http2.initial_stream_window_size", 1 << 20);       // 1MB
      channel_args.SetInt("grpc.http2.initial_connection_window_size", 16 << 20);  // 16MB
      // Set HTTP/2 concurrent streams limit.
      // This maps to SETTINGS_MAX_CONCURRENT_STREAMS.
      channel_args.SetInt("grpc.max_concurrent_streams", 1000);
      
      // Disable resolver background threads that might interfere
      channel_args.SetInt(GRPC_ARG_ENABLE_RETRIES, 0);
      
      // Minimize HTTP/2 pings to reduce shared pollset activity
      channel_args.SetInt(GRPC_ARG_HTTP2_MAX_PINGS_WITHOUT_DATA, 0);
      channel_args.SetInt(GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS, 0);
      
      // Keepalive settings to minimize background polling activity
      channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);  // 30 seconds
      channel_args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 5000);
      channel_args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 0);
      
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

    // Start CQ poller threads without CPU affinity pinning.

    // Optional poller RT priority override (default: 10)
    int poller_fifo_prio = 10;
    int fifo_min_prio = sched_get_priority_min(SCHED_FIFO);
    int fifo_max_prio = sched_get_priority_max(SCHED_FIFO);
    const char* poller_prio_env = std::getenv("BLOOMCHECK_POLLER_FIFO_PRIO");
    if (poller_prio_env != nullptr) {
      int requested = std::atoi(poller_prio_env);
      if (requested >= fifo_min_prio && requested <= fifo_max_prio) {
        poller_fifo_prio = requested;
      }
    }

    cq_poller_threads_.reserve(cq_count);
    for (size_t i = 0; i < cq_count; ++i) {
      cq_poller_threads_.emplace_back(
          std::thread(&BloomCheckServiceClient::CQPollerThread, this, i));
      pthread_t thread_handle = cq_poller_threads_.back().native_handle();
      struct sched_param param;
      param.sched_priority = poller_fifo_prio;
      (void)pthread_setschedparam(thread_handle, SCHED_FIFO, &param);
    }

    // Log poller summary
    // fprintf(stderr, "[BloomCheckClient] Poller threads started without CPU pinning, count=%d, fifo_prio=%d\n",
    //         static_cast<int>(cq_count), poller_fifo_prio);
    
    // Log initialization info
    // fprintf(stderr, "[BloomCheckClient] Initialized: stubs_.size()=%zu, cqs_.size()=%zu, poller_thread_count=%zu\n",
    //         stubs_.size(), cqs_.size(), cq_poller_threads_.size());

    if (use_streaming_) {
      stream_sessions_.resize(channel_count_);
      for (size_t ch = 0; ch < channel_count_; ++ch) {
        auto& per_channel = stream_sessions_[ch];
        per_channel.reserve(streams_per_channel_);
        for (size_t s = 0; s < streams_per_channel_; ++s) {
          size_t cq_idx = ch % cqs_.size();
          per_channel.emplace_back(std::make_unique<StreamSession>(
              this, ch, s, cq_idx, stream_max_pending_));
        }
      }
      // fprintf(stderr,
      //         "[BloomCheckClient] Streaming enabled: channels=%zu streams_per_channel=%zu stream_max_pending=%zu\n",
      //         channel_count_, streams_per_channel_, stream_max_pending_);
    } else {
      // fprintf(stderr, "[BloomCheckClient] Streaming disabled: using unary RPC path\n");
    }
    
    // Initialize inflight statistics
    inflight_now_.store(0, std::memory_order_relaxed);
    inflight_max_.store(0, std::memory_order_relaxed);
    inflight_sum_.store(0, std::memory_order_relaxed);
    inflight_samples_.store(0, std::memory_order_relaxed);
    rpc_started_total_.store(0, std::memory_order_relaxed);
    rpc_completed_total_.store(0, std::memory_order_relaxed);
    stream_enq_ok_total_.store(0, std::memory_order_relaxed);
    stream_enq_fail_total_.store(0, std::memory_order_relaxed);
    stream_write_done_total_.store(0, std::memory_order_relaxed);
    stream_read_done_total_.store(0, std::memory_order_relaxed);
    stream_unmatched_resp_total_.store(0, std::memory_order_relaxed);
    
    // Initialize per-channel inflight arrays
    // Note: std::atomic is not copyable, so we use unique_ptr
    inflight_per_channel_.clear();
    inflight_per_channel_max_.clear();
    for (size_t i = 0; i < channel_count_; ++i) {
      inflight_per_channel_.emplace_back(std::make_unique<std::atomic<int64_t>>(0));
      inflight_per_channel_max_.emplace_back(std::make_unique<std::atomic<int64_t>>(0));
    }
    
    // Start stats thread if enabled
    const char* stats_env = std::getenv("BLOOMCHECK_CLIENT_STATS");
    stream_stats_enabled_ = (stats_env != nullptr && std::atoi(stats_env) != 0);
    if (stream_stats_enabled_) {
      // Use atomic flag to prevent duplicate stats thread starts
      bool expected = false;
      if (!stats_thread_started_.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        // fprintf(stderr, "[BloomCheckClient] WARNING: Stats thread already started! Not starting duplicate. [this=%p]\n",
        //         static_cast<void*>(this));
      } else {
        // Check if stats thread is already running (guard against duplicate starts)
        if (stats_thread_.joinable()) {
          // fprintf(stderr, "[BloomCheckClient] WARNING: Stats thread already running! Not starting duplicate. [this=%p]\n",
          //         static_cast<void*>(this));
          stats_thread_started_.store(false, std::memory_order_release);  // Reset flag
        } else {
          // pid_t pid = getpid();
          // pid_t tid = syscall(SYS_gettid);
          // void* this_ptr = static_cast<void*>(this);
          stats_thread_ = std::thread(&BloomCheckServiceClient::StatsThread, this);
          // fprintf(stderr, "[BloomCheckClient] Stats thread enabled (BLOOMCHECK_CLIENT_STATS=1) [pid=%d][tid=%d][this=%p]\n",
          //         pid, tid, this_ptr);
        }
      }
    }
  }

  ~BloomCheckServiceClient() {
    // Shutdown stats thread
    {
      std::lock_guard<std::mutex> lock(shutdown_mutex_);
      shutdown_ = true;
    }
    if (stats_thread_.joinable()) {
      stats_thread_.join();
    }
    
    // Print final inflight statistics before shutdown
    // int64_t final_inflight = inflight_now_.load(std::memory_order_relaxed);
    // int64_t final_max = inflight_max_.load(std::memory_order_relaxed);
    // uint64_t total_started = rpc_started_total_.load(std::memory_order_relaxed);
    // uint64_t total_completed = rpc_completed_total_.load(std::memory_order_relaxed);
    
    // fprintf(stderr, "[BloomCheckClient] Final statistics: inflight_now=%lld inflight_max=%lld "
    //         "rpc_started_total=%llu rpc_completed_total=%llu\n",
    //         (long long)final_inflight, (long long)final_max,
    //         (unsigned long long)total_started, (unsigned long long)total_completed);
    
    // Print per-channel max inflight
    // fprintf(stderr, "[BloomCheckClient] Per-channel max inflight: [");
    // for (size_t i = 0; i < std::min(channel_count_, static_cast<size_t>(10)); ++i) {
    //   int64_t ch_max = inflight_per_channel_max_[i]->load(std::memory_order_relaxed);
    //   if (i > 0) fprintf(stderr, ",");
    //   fprintf(stderr, "%lld", (long long)ch_max);
    // }
    // if (channel_count_ > 10) {
    //   fprintf(stderr, ",...(%zu total)", channel_count_);
    // }
    // fprintf(stderr, "]\n");
    
    // Interpretation hint if inflight_max is low relative to typical thread counts
    // if (final_max < 10) {
    //   fprintf(stderr, "[BloomCheckClient] NOTE: inflight_max=%lld is low. "
    //           "If RocksDB uses many worker threads (e.g., threads=128/256), "
    //           "this suggests RPCs may be completing too quickly or there's a bottleneck preventing concurrent RPCs.\n",
    //           (long long)final_max);
    // }
    
    // if (inflight_semaphore_) {
    //   uint64_t wait_cnt = 0, wait_us_total = 0, max_wait_us = 0;
    //   inflight_semaphore_->GetStats(wait_cnt, wait_us_total, max_wait_us);
    //   fprintf(stderr, "[BloomCheckClient] Inflight semaphore statistics: wait_cnt=%llu, wait_us_total=%llu, max_wait_us=%llu\n",
    //           (unsigned long long)wait_cnt, (unsigned long long)wait_us_total, (unsigned long long)max_wait_us);
    //   if (wait_cnt == 0) {
    //     fprintf(stderr, "[BloomCheckClient] WARNING: wait_cnt=0 means inflight limiting may not be effective\n");
    //   }
    // }

    if (use_streaming_) {
      for (auto& per_channel : stream_sessions_) {
        for (auto& session : per_channel) {
          if (session) {
            session->Shutdown();
          }
        }
      }
    }
    
    // Shutdown all CQs
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

  // SplitMix64 hash mixing function for independent stream index selection
  static inline uint64_t SplitMix64(uint64_t x) {
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    x *= 0x94d049bb133111ebULL;
    x ^= x >> 31;
    return x;
  }

  uint64_t GetThreadHash() const {
    std::hash<std::thread::id> hasher;
    return static_cast<uint64_t>(hasher(std::this_thread::get_id()));
  }

  // Get channel index for current thread
  size_t GetChannelIndex() const {
    if (channel_count_ == 1) {
      return 0;
    }
    // Use thread id hash to select channel
    uint64_t tid_hash = GetThreadHash();
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
                       bloomcheck::BatchResponse* response, size_t* channel_idx = nullptr,
                       RpcTimingBreakdown* timing = nullptr) {
    const uint64_t t_acquire_begin = BloomNowMicros();
    // Acquire semaphore slot if inflight limiting is enabled
    // This will block if MAX_INFLIGHT limit is reached
    SemaphoreGuard guard(inflight_semaphore_.get());
    const uint64_t t_after_acquire = BloomNowMicros();

    if (use_streaming_ && request.reqs_size() == 1) {
      return AsyncCheckBatchStreaming(request, response, channel_idx, timing,
                                      t_acquire_begin, t_after_acquire);
    }

    return AsyncCheckBatchUnary(request, response, channel_idx, timing,
                                t_acquire_begin, t_after_acquire);
  }

 private:
  bool AsyncCheckBatchUnary(const bloomcheck::BatchRequest& request,
                            bloomcheck::BatchResponse* response,
                            size_t* channel_idx, RpcTimingBreakdown* timing,
                            uint64_t t_acquire_begin,
                            uint64_t t_after_acquire) {

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
    const uint64_t t_after_finish = BloomNowMicros();

    // Increment inflight counters AFTER rpc->Finish() (RPC is now outstanding)
    // Use RAII guard to ensure decrement even on exception/early return
    std::atomic<int64_t>* total_inflight = nullptr;
    std::atomic<int64_t>* channel_inflight = nullptr;
    std::atomic<int64_t>* total_max = nullptr;
    std::atomic<int64_t>* channel_max = nullptr;
    if (stream_stats_enabled_) {
      total_inflight = &inflight_now_;
      channel_inflight = inflight_per_channel_[stub_idx].get();
      total_max = &inflight_max_;
      channel_max = inflight_per_channel_max_[stub_idx].get();
    }
    InflightGuard inflight_guard(total_inflight, channel_inflight, total_max,
                                 channel_max);
    
    // Increment started counter
    if (stream_stats_enabled_) {
      rpc_started_total_.fetch_add(1, std::memory_order_relaxed);
    }

    // Keep rpc alive until Wait() completes (rpc destructor is safe after Finish)
    // Block on condition_variable (NOT on gRPC polling)
    const uint64_t t_wait_begin = BloomNowMicros();
    call->Wait();
    const uint64_t t_wait_end = BloomNowMicros();

    // RPC completed - inflight_guard destructor will decrement counters
    // Increment completed counter
    if (stream_stats_enabled_) {
      rpc_completed_total_.fetch_add(1, std::memory_order_relaxed);
    }

    // Copy results
    // Note: SemaphoreGuard destructor will release the semaphore slot here
    *response = call->response_;
    const uint64_t t_parse_end = BloomNowMicros();
    if (timing != nullptr) {
      timing->acquire_wait_us = t_after_acquire - t_acquire_begin;
      timing->submit_prep_us = t_after_finish - t_after_acquire;
      const uint64_t signal_us = call->signal_us_.load(std::memory_order_acquire);
      if (signal_us > 0) {
        if (signal_us > t_wait_begin) {
          timing->wait_completion_us = signal_us - t_wait_begin;
          if (t_wait_end > signal_us) {
            timing->wake_up_us = t_wait_end - signal_us;
          }
        } else {
          // Completion already arrived before or at Wait() entry.
          timing->wait_completion_us = 0;
          if (t_wait_end > t_wait_begin) {
            timing->wake_up_us = t_wait_end - t_wait_begin;
          }
        }
      } else {
        // Fallback: unknown signal timestamp, treat entire wait as completion wait.
        timing->wait_completion_us = t_wait_end - t_wait_begin;
        timing->wake_up_us = 0;
      }
      timing->response_parse_us = t_parse_end - t_wait_end;
    }
    return call->status_.ok();
  }

  bool AsyncCheckBatchStreaming(const bloomcheck::BatchRequest& request,
                                bloomcheck::BatchResponse* response,
                                size_t* channel_idx, RpcTimingBreakdown* timing,
                                uint64_t t_acquire_begin,
                                uint64_t t_after_acquire) {
    if (stream_sessions_.empty()) {
      return AsyncCheckBatchUnary(request, response, channel_idx, timing,
                                  t_acquire_begin, t_after_acquire);
    }

    // Use independent hashing for channel and stream selection to avoid correlation
    const uint64_t h = GetThreadHash();
    const size_t stub_idx = static_cast<size_t>(h % channel_count_);
    
    // Mix hash with salt and apply SplitMix64 for independent stream index
    const uint64_t h2 = SplitMix64(h ^ 0x9e3779b97f4a7c15ULL);
    const size_t stream_idx = static_cast<size_t>(h2 % streams_per_channel_);
    if (channel_idx != nullptr) {
      *channel_idx = stub_idx;
    }

    auto& per_channel = stream_sessions_[stub_idx];
    if (stream_idx >= per_channel.size() || per_channel[stream_idx] == nullptr) {
      return AsyncCheckBatchUnary(request, response, channel_idx, timing,
                                  t_acquire_begin, t_after_acquire);
    }

    const auto& key_req = request.reqs(0);
    const uint64_t request_id =
        next_stream_request_id_.fetch_add(1, std::memory_order_relaxed) + 1;
    auto pending = MakePendingCall();
    pending->req.set_request_id(request_id);
    pending->req.set_user_key(key_req.user_key());
    for (int i = 0; i < key_req.sst_file_numbers_size(); ++i) {
      pending->req.add_sst_file_numbers(key_req.sst_file_numbers(i));
    }
    bool enqueued = per_channel[stream_idx]->EnqueueRequest(pending);
    const uint64_t t_after_finish = BloomNowMicros();
    if (!enqueued) {
      return false;
    }

    std::atomic<int64_t>* total_inflight = nullptr;
    std::atomic<int64_t>* channel_inflight = nullptr;
    std::atomic<int64_t>* total_max = nullptr;
    std::atomic<int64_t>* channel_max = nullptr;
    if (stream_stats_enabled_) {
      total_inflight = &inflight_now_;
      channel_inflight = inflight_per_channel_[stub_idx].get();
      total_max = &inflight_max_;
      channel_max = inflight_per_channel_max_[stub_idx].get();
    }
    InflightGuard inflight_guard(total_inflight, channel_inflight, total_max,
                                 channel_max);
    if (stream_stats_enabled_) {
      rpc_started_total_.fetch_add(1, std::memory_order_relaxed);
    }

    const uint64_t t_wait_begin = BloomNowMicros();
    bool ok = pending->Wait();
    const uint64_t t_wait_end = BloomNowMicros();
    if (stream_stats_enabled_) {
      rpc_completed_total_.fetch_add(1, std::memory_order_relaxed);
    }

    if (ok) {
      response->Clear();
      auto* out = response->add_resps();
      for (int i = 0; i < pending->resp.may_match_sst_file_numbers_size(); ++i) {
        out->add_may_match_sst_file_numbers(
            pending->resp.may_match_sst_file_numbers(i));
      }
      for (int i = 0; i < pending->resp.path_not_found_sst_file_numbers_size();
           ++i) {
        out->add_path_not_found_sst_file_numbers(
            pending->resp.path_not_found_sst_file_numbers(i));
      }
    }
    const uint64_t t_parse_end = BloomNowMicros();

    if (timing != nullptr) {
      timing->acquire_wait_us = t_after_acquire - t_acquire_begin;
      timing->submit_prep_us = t_after_finish - t_after_acquire;
      const uint64_t signal_us =
          pending->signal_us.load(std::memory_order_acquire);
      if (signal_us > 0) {
        if (signal_us > t_wait_begin) {
          timing->wait_completion_us = signal_us - t_wait_begin;
          timing->wake_up_us = (t_wait_end > signal_us) ? (t_wait_end - signal_us) : 0;
        } else {
          timing->wait_completion_us = 0;
          timing->wake_up_us =
              (t_wait_end > t_wait_begin) ? (t_wait_end - t_wait_begin) : 0;
        }
      } else {
        timing->wait_completion_us = t_wait_end - t_wait_begin;
        timing->wake_up_us = 0;
      }
      timing->response_parse_us = t_parse_end - t_wait_end;
    }
    return ok;
  }

 public:

  // Noop RPC for latency measurement (sync unary) - deprecated, use AsyncNoop instead
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

  // Async version: Noop RPC for latency measurement
  // Blocks the calling thread until RPC completes, but does NOT participate in pollset_work
  // Respects MAX_INFLIGHT limit if set via BLOOMCHECK_MAX_INFLIGHT environment variable
  bool AsyncNoop(size_t* channel_idx = nullptr, RpcTimingBreakdown* timing = nullptr) {
    const uint64_t t_acquire_begin = BloomNowMicros();
    // Acquire semaphore slot if inflight limiting is enabled
    // This will block if MAX_INFLIGHT limit is reached
    SemaphoreGuard guard(inflight_semaphore_.get());
    const uint64_t t_after_acquire = BloomNowMicros();

    // Select channel and CQ
    size_t stub_idx = GetChannelIndex();
    size_t cq_idx = stub_idx % cqs_.size();
    
    if (channel_idx != nullptr) {
      *channel_idx = stub_idx;
    }

    // Create per-RPC state object (must outlive the RPC)
    auto call = std::make_shared<AsyncNoopCall>();
    call->channel_idx_ = stub_idx;

    // Start async RPC
    std::unique_ptr<grpc::ClientAsyncResponseReader<bloomcheck::NoopResponse>> rpc(
        stubs_[stub_idx]->AsyncNoop(&call->context_, call->request_, cqs_[cq_idx].get()));

    // Finish is an async operation - tag must remain valid until CQ poller processes it
    void* tag = call.get();
    rpc->Finish(&call->response_, &call->status_, tag);
    const uint64_t t_after_finish = BloomNowMicros();

    // Keep rpc alive until Wait() completes (rpc destructor is safe after Finish)
    // Block on condition_variable (NOT on gRPC polling)
    const uint64_t t_wait_begin = BloomNowMicros();
    call->Wait();
    const uint64_t t_wait_end = BloomNowMicros();

    // Note: SemaphoreGuard destructor will release the semaphore slot here
    const uint64_t t_parse_end = BloomNowMicros();
    if (timing != nullptr) {
      timing->acquire_wait_us = t_after_acquire - t_acquire_begin;
      timing->submit_prep_us = t_after_finish - t_after_acquire;
      const uint64_t signal_us = call->signal_us_.load(std::memory_order_acquire);
      if (signal_us > 0) {
        if (signal_us > t_wait_begin) {
          timing->wait_completion_us = signal_us - t_wait_begin;
          if (t_wait_end > signal_us) {
            timing->wake_up_us = t_wait_end - signal_us;
          }
        } else {
          timing->wait_completion_us = 0;
          if (t_wait_end > t_wait_begin) {
            timing->wake_up_us = t_wait_end - t_wait_begin;
          }
        }
      } else {
        timing->wait_completion_us = t_wait_end - t_wait_begin;
        timing->wake_up_us = 0;
      }
      timing->response_parse_us = t_parse_end - t_wait_end;
    }
    return call->status_.ok();
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
  struct PendingStreamCall {
    std::mutex mu;
    std::condition_variable cv;
    bool done = false;
    bool ok = false;
    bloomcheck::StreamRequest req;
    bloomcheck::StreamResponse resp;
    std::atomic<uint64_t> signal_us{0};

    void Reset() {
      req.Clear();
      resp.Clear();
      done = false;
      ok = false;
      signal_us.store(0, std::memory_order_relaxed);
    }

    void CompleteSuccess(const bloomcheck::StreamResponse& response) {
      {
        std::lock_guard<std::mutex> lock(mu);
        resp = response;
        ok = true;
        done = true;
        signal_us.store(BloomNowMicros(), std::memory_order_release);
      }
      cv.notify_one();
    }

    void CompleteFailure() {
      {
        std::lock_guard<std::mutex> lock(mu);
        ok = false;
        done = true;
        signal_us.store(BloomNowMicros(), std::memory_order_release);
      }
      cv.notify_one();
    }

    bool Wait() {
      std::unique_lock<std::mutex> lock(mu);
      cv.wait(lock, [this] { return done; });
      return ok;
    }
  };

  struct PendingPool {
    std::vector<PendingStreamCall*> free_list;

    ~PendingPool() {
      for (auto* p : free_list) {
        delete p;
      }
    }
  };

  static constexpr size_t kPendingPoolMaxPerThread = 256;

  static PendingPool& GetPendingPool() {
    static thread_local PendingPool pool;
    return pool;
  }

  static PendingStreamCall* AcquirePending() {
    auto& free_list = GetPendingPool().free_list;
    if (free_list.empty()) {
      return new PendingStreamCall();
    }
    PendingStreamCall* call = free_list.back();
    free_list.pop_back();
    return call;
  }

  static void ReleasePending(PendingStreamCall* call) {
    if (call == nullptr) {
      return;
    }
    call->Reset();
    auto& free_list = GetPendingPool().free_list;
    if (free_list.size() >= kPendingPoolMaxPerThread) {
      delete call;
      return;
    }
    free_list.push_back(call);
  }

  static std::shared_ptr<PendingStreamCall> MakePendingCall() {
    return std::shared_ptr<PendingStreamCall>(AcquirePending(),
                                              &BloomCheckServiceClient::ReleasePending);
  }

  bool IsShutdownRequested() {
    std::lock_guard<std::mutex> lock(shutdown_mutex_);
    return shutdown_;
  }

  class StreamSession {
   public:
    StreamSession(BloomCheckServiceClient* owner, size_t channel_idx,
                  size_t stream_idx, size_t cq_idx, size_t max_pending)
        : owner_(owner),
          channel_idx_(channel_idx),
          stream_idx_(stream_idx),
          cq_idx_(cq_idx),
          max_pending_(max_pending),
          start_tag_(this, TagType::kStart),
          read_tag_(this, TagType::kRead),
          write_tag_(this, TagType::kWrite) {}

    bool EnqueueRequest(const std::shared_ptr<PendingStreamCall>& call) {
      std::lock_guard<std::mutex> lock(mu_);
      if (state_ == State::kClosed) {
        if (owner_->stream_stats_enabled_) {
          owner_->stream_enq_fail_total_.fetch_add(1, std::memory_order_relaxed);
        }
        return false;
      }
      if (pending_.size() >= max_pending_) {
        if (owner_->stream_stats_enabled_) {
          owner_->stream_enq_fail_total_.fetch_add(1, std::memory_order_relaxed);
        }
        return false;
      }
      pending_[call->req.request_id()] = call;
      write_queue_.push_back(call);
      if (owner_->stream_stats_enabled_) {
        owner_->stream_enq_ok_total_.fetch_add(1, std::memory_order_relaxed);
      }
      if (state_ == State::kInit || state_ == State::kBroken) {
        StartLocked();
      }
      if (state_ == State::kReady && !write_in_flight_) {
        StartNextWriteLocked();
      }
      return true;
    }

    void Shutdown() {
      std::vector<std::shared_ptr<PendingStreamCall>> to_fail;
      {
        std::lock_guard<std::mutex> lock(mu_);
        state_ = State::kClosed;
        CollectAndClearPendingLocked(&to_fail);
        write_queue_.clear();
        write_in_flight_ = false;
        write_call_.reset();
        read_in_flight_ = false;
        if (ctx_) {
          ctx_->TryCancel();
        }
        stream_.reset();
        ctx_.reset();
      }
      for (const auto& call : to_fail) {
        call->CompleteFailure();
      }
    }

    void SnapshotDepths(size_t* pending_size, size_t* writeq_size) {
      std::lock_guard<std::mutex> lock(mu_);
      *pending_size = pending_.size();
      *writeq_size = write_queue_.size();
    }

   private:
    enum class State { kInit, kStarting, kReady, kBroken, kClosed };
    enum class TagType { kStart, kRead, kWrite };

    struct StreamTag : public CqTagBase {
      StreamSession* session;
      TagType type;
      StreamTag(StreamSession* s, TagType t) : session(s), type(t) {}
      void Proceed(bool ok) override { session->OnTag(type, ok); }
    };

    void OnTag(TagType type, bool ok) {
      if (type == TagType::kStart) {
        OnStartComplete(ok);
      } else if (type == TagType::kRead) {
        OnReadComplete(ok);
      } else {
        OnWriteComplete(ok);
      }
    }

    void StartLocked() {
      if (state_ == State::kClosed || state_ == State::kStarting) {
        return;
      }
      ctx_ = std::make_unique<grpc::ClientContext>();
      stream_ = owner_->stubs_[channel_idx_]->AsyncCheckBatchStream(
          ctx_.get(), owner_->cqs_[cq_idx_].get(), &start_tag_);
      state_ = State::kStarting;
    }

    void StartNextWriteLocked() {
      if (state_ != State::kReady || write_in_flight_ || write_queue_.empty() ||
          !stream_) {
        return;
      }
      write_call_ = std::move(write_queue_.front());
      write_queue_.pop_front();
      write_in_flight_ = true;
      stream_->Write(write_call_->req, &write_tag_);
    }

    void CollectAndClearPendingLocked(
        std::vector<std::shared_ptr<PendingStreamCall>>* to_fail) {
      to_fail->reserve(to_fail->size() + pending_.size());
      for (auto& it : pending_) {
        to_fail->push_back(it.second);
      }
      pending_.clear();
    }

    void BreakAndReconnectLocked(
        std::vector<std::shared_ptr<PendingStreamCall>>* to_fail) {
      if (state_ == State::kClosed) {
        return;
      }
      state_ = State::kBroken;
      CollectAndClearPendingLocked(to_fail);
      write_queue_.clear();
      write_in_flight_ = false;
      write_call_.reset();
      read_in_flight_ = false;
      if (ctx_) {
        ctx_->TryCancel();
      }
      stream_.reset();
      ctx_.reset();
      if (!owner_->IsShutdownRequested()) {
        StartLocked();
      }
    }

    void OnStartComplete(bool ok) {
      std::vector<std::shared_ptr<PendingStreamCall>> to_fail;
      {
        std::lock_guard<std::mutex> lock(mu_);
        if (!ok || !stream_) {
          BreakAndReconnectLocked(&to_fail);
        } else {
          state_ = State::kReady;
          if (!read_in_flight_) {
            read_in_flight_ = true;
            stream_->Read(&read_resp_, &read_tag_);
          }
          StartNextWriteLocked();
        }
      }
      for (const auto& call : to_fail) {
        call->CompleteFailure();
      }
    }

    void OnWriteComplete(bool ok) {
      std::vector<std::shared_ptr<PendingStreamCall>> to_fail;
      {
        std::lock_guard<std::mutex> lock(mu_);
        write_in_flight_ = false;
        write_call_.reset();
        if (!ok) {
          BreakAndReconnectLocked(&to_fail);
        } else {
          if (owner_->stream_stats_enabled_) {
            owner_->stream_write_done_total_.fetch_add(1, std::memory_order_relaxed);
          }
          StartNextWriteLocked();
        }
      }
      for (const auto& call : to_fail) {
        call->CompleteFailure();
      }
    }

    void OnReadComplete(bool ok) {
      std::shared_ptr<PendingStreamCall> done_call;
      bloomcheck::StreamResponse resp_copy;
      bool unmatched_resp = false;
      std::vector<std::shared_ptr<PendingStreamCall>> to_fail;
      {
        std::lock_guard<std::mutex> lock(mu_);
        read_in_flight_ = false;
        if (!ok) {
          BreakAndReconnectLocked(&to_fail);
        } else {
          if (owner_->stream_stats_enabled_) {
            owner_->stream_read_done_total_.fetch_add(1, std::memory_order_relaxed);
          }
          resp_copy = read_resp_;
          auto it = pending_.find(resp_copy.request_id());
          if (it != pending_.end()) {
            done_call = it->second;
            pending_.erase(it);
          } else {
            unmatched_resp = true;
          }
          if (state_ == State::kReady && stream_) {
            read_in_flight_ = true;
            stream_->Read(&read_resp_, &read_tag_);
          }
        }
      }

      if (unmatched_resp && owner_->stream_stats_enabled_) {
        owner_->stream_unmatched_resp_total_.fetch_add(1, std::memory_order_relaxed);
      }

      if (done_call) {
        done_call->CompleteSuccess(resp_copy);
      }
      for (const auto& call : to_fail) {
        call->CompleteFailure();
      }
    }

    BloomCheckServiceClient* owner_;
    const size_t channel_idx_;
    const size_t stream_idx_;
    const size_t cq_idx_;
    const size_t max_pending_;

    std::mutex mu_;
    State state_ = State::kInit;
    std::unique_ptr<grpc::ClientContext> ctx_;
    std::unique_ptr<grpc::ClientAsyncReaderWriter<bloomcheck::StreamRequest,
                                                  bloomcheck::StreamResponse>>
        stream_;
    std::deque<std::shared_ptr<PendingStreamCall>> write_queue_;
    std::unordered_map<uint64_t, std::shared_ptr<PendingStreamCall>> pending_;
    std::shared_ptr<PendingStreamCall> write_call_;
    bloomcheck::StreamResponse read_resp_;
    bool write_in_flight_ = false;
    bool read_in_flight_ = false;

    StreamTag start_tag_;
    StreamTag read_tag_;
    StreamTag write_tag_;
  };

  // RAII guard for inflight counting (ensures decrement even on exception/early return)
  class InflightGuard {
   public:
    InflightGuard(std::atomic<int64_t>* total_inflight,
                  std::atomic<int64_t>* channel_inflight,
                  std::atomic<int64_t>* total_max,
                  std::atomic<int64_t>* channel_max)
        : total_inflight_(total_inflight),
          channel_inflight_(channel_inflight),
          total_max_(total_max),
          channel_max_(channel_max),
          active_(false) {
      if (total_inflight_ && channel_inflight_) {
        // Increment counters
        int64_t new_total = total_inflight_->fetch_add(1, std::memory_order_relaxed) + 1;
        int64_t new_channel = channel_inflight_->fetch_add(1, std::memory_order_relaxed) + 1;
        active_ = true;
        
        // Update high-watermarks
        if (total_max_) {
          int64_t current_max = total_max_->load(std::memory_order_relaxed);
          while (new_total > current_max &&
                 !total_max_->compare_exchange_weak(current_max, new_total,
                                                    std::memory_order_relaxed,
                                                    std::memory_order_relaxed)) {
            // Retry loop
          }
        }
        
        if (channel_max_) {
          int64_t current_max = channel_max_->load(std::memory_order_relaxed);
          while (new_channel > current_max &&
                 !channel_max_->compare_exchange_weak(current_max, new_channel,
                                                      std::memory_order_relaxed,
                                                      std::memory_order_relaxed)) {
            // Retry loop
          }
        }
      }
    }
    
    ~InflightGuard() {
      if (active_ && total_inflight_ && channel_inflight_) {
        total_inflight_->fetch_sub(1, std::memory_order_relaxed);
        channel_inflight_->fetch_sub(1, std::memory_order_relaxed);
      }
    }
    
    // Non-copyable
    InflightGuard(const InflightGuard&) = delete;
    InflightGuard& operator=(const InflightGuard&) = delete;
    
   private:
    std::atomic<int64_t>* total_inflight_;
    std::atomic<int64_t>* channel_inflight_;
    std::atomic<int64_t>* total_max_;
    std::atomic<int64_t>* channel_max_;
    bool active_;
  };
  
  // Stats thread function: 1s마다 출력, read_done은 10초 구간 누적/Δ도 함께 출력
  void StatsThread() {
    pthread_setname_np(pthread_self(), "bloom_stats");
    
    uint64_t local_sample_count = 0;
    const int samples_per_second = 100;
    const auto sample_interval = std::chrono::milliseconds(10);
    
    // 10초 구간 read_done Δ 계산용: 10초 전 시점의 read_done 누적값
    uint64_t read_done_snapshot_10s_ago[10] = {0};
    int read_done_ring_idx = 0;
    int read_done_ring_filled = 0;  // 10초 채워질 때까지 0..9
    const int kRingSize = 10;
    
    while (true) {
      {
        std::lock_guard<std::mutex> lock(shutdown_mutex_);
        if (shutdown_) {
          break;
        }
      }
      
      const uint64_t baseline_enq_ok =
          stream_enq_ok_total_.load(std::memory_order_relaxed);
      const uint64_t baseline_enq_fail =
          stream_enq_fail_total_.load(std::memory_order_relaxed);
      const uint64_t baseline_write_done =
          stream_write_done_total_.load(std::memory_order_relaxed);
      const uint64_t baseline_read_done =
          stream_read_done_total_.load(std::memory_order_relaxed);
      const uint64_t baseline_unmatched =
          stream_unmatched_resp_total_.load(std::memory_order_relaxed);
      
      local_sample_count = 0;
      uint64_t local_pending_sum = 0;
      uint64_t local_writeq_sum = 0;
      size_t local_pending_max = 0;
      size_t local_writeq_max = 0;
      
      for (int i = 0; i < samples_per_second; ++i) {
        {
          std::lock_guard<std::mutex> lock(shutdown_mutex_);
          if (shutdown_) {
            return;
          }
        }
        local_sample_count++;
        if (use_streaming_) {
          size_t sample_pending_total = 0;
          size_t sample_writeq_total = 0;
          for (auto& per_channel : stream_sessions_) {
            for (auto& session : per_channel) {
              if (!session) continue;
              size_t pending_size = 0;
              size_t writeq_size = 0;
              session->SnapshotDepths(&pending_size, &writeq_size);
              sample_pending_total += pending_size;
              sample_writeq_total += writeq_size;
            }
          }
          local_pending_sum += sample_pending_total;
          local_writeq_sum += sample_writeq_total;
          if (sample_pending_total > local_pending_max) local_pending_max = sample_pending_total;
          if (sample_writeq_total > local_writeq_max) local_writeq_max = sample_writeq_total;
        }
        std::this_thread::sleep_for(sample_interval);
      }
      
      const uint64_t current_enq_ok =
          stream_enq_ok_total_.load(std::memory_order_relaxed);
      const uint64_t current_enq_fail =
          stream_enq_fail_total_.load(std::memory_order_relaxed);
      const uint64_t current_write_done =
          stream_write_done_total_.load(std::memory_order_relaxed);
      const uint64_t current_read_done =
          stream_read_done_total_.load(std::memory_order_relaxed);
      const uint64_t current_unmatched =
          stream_unmatched_resp_total_.load(std::memory_order_relaxed);

      const uint64_t enq_ok_per_sec = current_enq_ok - baseline_enq_ok;
      const uint64_t enq_fail_per_sec = current_enq_fail - baseline_enq_fail;
      const uint64_t write_done_per_sec = current_write_done - baseline_write_done;
      const uint64_t read_done_per_sec = current_read_done - baseline_read_done;
      const uint64_t unmatched_per_sec = current_unmatched - baseline_unmatched;

      uint64_t read_done_delta_10s = 0;
      if (read_done_ring_filled >= kRingSize) {
        read_done_delta_10s = current_read_done - read_done_snapshot_10s_ago[read_done_ring_idx];
      }
      read_done_snapshot_10s_ago[read_done_ring_idx] = current_read_done;
      read_done_ring_idx = (read_done_ring_idx + 1) % kRingSize;
      if (read_done_ring_filled < kRingSize) {
        read_done_ring_filled++;
      }

      double pending_avg = 0.0;
      double writeq_avg = 0.0;
      if (local_sample_count > 0) {
        pending_avg = static_cast<double>(local_pending_sum) / static_cast<double>(local_sample_count);
        writeq_avg = static_cast<double>(local_writeq_sum) / static_cast<double>(local_sample_count);
      }

      fprintf(stderr,
              "[StreamStats] enq=%llu/s fail=%llu/s pending_avg=%.1f pending_max=%zu "
              "writeq_avg=%.1f writeq_max=%zu write_done=%llu/s read_done=%llu/s "
              "read_done_total=%llu read_done_Δ=%llu/10s unmatched=%llu/s\n",
              (unsigned long long)enq_ok_per_sec,
              (unsigned long long)enq_fail_per_sec, pending_avg,
              local_pending_max, writeq_avg, local_writeq_max,
              (unsigned long long)write_done_per_sec,
              (unsigned long long)read_done_per_sec,
              (unsigned long long)current_read_done,
              (unsigned long long)read_done_delta_10s,
              (unsigned long long)unmatched_per_sec);
    }
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

      CqTagBase* cq_tag = static_cast<CqTagBase*>(tag);
      cq_tag->Proceed(ok);
    }
  }

  size_t channel_count_;
  std::vector<std::shared_ptr<grpc::Channel>> channels_;
  std::vector<std::unique_ptr<bloomcheck::BloomCheck::Stub>> stubs_;
  std::vector<std::unique_ptr<grpc::CompletionQueue>> cqs_;
  std::vector<std::thread> cq_poller_threads_;
  std::mutex shutdown_mutex_;
  bool shutdown_;
  bool stream_stats_enabled_;  // True when BLOOMCHECK_CLIENT_STATS is non-zero
  std::unique_ptr<Semaphore> inflight_semaphore_;  // For limiting concurrent inflight RPCs
  bool use_streaming_ = false;
  size_t streams_per_channel_ = 4;
  size_t stream_max_pending_ = 4096;
  std::atomic<uint64_t> next_stream_request_id_{0};
  std::vector<std::vector<std::unique_ptr<StreamSession>>> stream_sessions_;
  
  // Inflight RPC statistics
  std::atomic<int64_t> inflight_now_;              // Current outstanding RPC count
  std::atomic<int64_t> inflight_max_;              // High-watermark of inflight
  std::atomic<int64_t> inflight_sum_;              // Sum of inflight samples (for average)
  std::atomic<uint64_t> inflight_samples_;         // Number of inflight samples
  std::vector<std::unique_ptr<std::atomic<int64_t>>> inflight_per_channel_;        // Per-channel outstanding RPC count
  std::vector<std::unique_ptr<std::atomic<int64_t>>> inflight_per_channel_max_;    // Per-channel high-watermark
  
  // RPC throughput statistics
  std::atomic<uint64_t> rpc_started_total_;         // Total RPCs started
  std::atomic<uint64_t> rpc_completed_total_;       // Total RPCs completed
  std::atomic<uint64_t> stream_enq_ok_total_;       // Total successful stream enqueue
  std::atomic<uint64_t> stream_enq_fail_total_;     // Total failed stream enqueue
  std::atomic<uint64_t> stream_write_done_total_;   // Total successful stream write completion
  std::atomic<uint64_t> stream_read_done_total_;    // Total successful stream read completion
  std::atomic<uint64_t> stream_unmatched_resp_total_;  // Total unmatched stream responses
  
  // Stats thread
  std::thread stats_thread_;                        // Statistics reporting thread
  std::atomic<bool> stats_thread_started_;          // Flag to prevent duplicate stats thread starts
  
  // Disable copy and move constructors/operators to prevent duplicate instances
  BloomCheckServiceClient(const BloomCheckServiceClient&) = delete;
  BloomCheckServiceClient& operator=(const BloomCheckServiceClient&) = delete;
  BloomCheckServiceClient(BloomCheckServiceClient&&) = delete;
  BloomCheckServiceClient& operator=(BloomCheckServiceClient&&) = delete;
};

}  // namespace ROCKSDB_NAMESPACE
