// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/completion_queue.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iostream>
#include "bloomcheck.grpc.pb.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {

// BloomHint: 캐시된 BloomCheck 결과
struct BloomHint {
  std::unordered_set<uint64_t> may_match_files;
  std::chrono::steady_clock::time_point ts;
  
  BloomHint() : ts(std::chrono::steady_clock::now()) {}
  
  bool IsExpired(std::chrono::milliseconds ttl) const {
    auto now = std::chrono::steady_clock::now();
    return (now - ts) > ttl;
  }
};

// 비동기 BloomCheck RPC 요청 컨텍스트
struct AsyncCheckContext {
  std::string user_key;
  std::unique_ptr<grpc::ClientContext> context;
  std::unique_ptr<bloomcheck::BatchRequest> request;
  std::unique_ptr<bloomcheck::BatchResponse> response;
  std::unique_ptr<grpc::ClientAsyncResponseReader<bloomcheck::BatchResponse>> reader;
  grpc::Status status;
  std::chrono::steady_clock::time_point start_time;
  std::atomic<bool> valid{true};  // 유효성 플래그
  
  AsyncCheckContext() : start_time(std::chrono::steady_clock::now()) {}
};

// 비동기 BloomCheck 서비스 클라이언트
class BloomCheckAsyncClient {
 public:
  explicit BloomCheckAsyncClient(const std::string& server_address,
                                 size_t max_inflight = 32,
                                 std::chrono::milliseconds hint_ttl = std::chrono::milliseconds(5),
                                 Logger* logger = nullptr)
      : channel_(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())),
        stub_(bloomcheck::BloomCheck::NewStub(channel_)),
        shutdown_(false),
        max_inflight_(max_inflight),
        hint_ttl_(hint_ttl),
        logger_(logger) {
    // CompletionQueue 처리 스레드 시작
    std::cerr << "[BloomCheck] Starting ProcessCompletionQueue thread..." << std::endl;
    cq_thread_ = std::thread(&BloomCheckAsyncClient::ProcessCompletionQueue, this);
    std::cerr << "[BloomCheck] ProcessCompletionQueue thread started successfully" << std::endl;
  }
  
  ~BloomCheckAsyncClient() {
    shutdown_.store(true, std::memory_order_relaxed);
    cq_.Shutdown();
    if (cq_thread_.joinable()) {
      cq_thread_.join();
    }
  }
  
  // 비동기로 BloomCheck RPC 발사 (non-blocking)
  // 결과는 캐시에 저장되며, 이후 GetHint()로 조회 가능
  bool FireAsyncCheck(const std::string& user_key,
                      const std::unordered_set<uint64_t>& candidate_file_numbers) {
    if (shutdown_.load(std::memory_order_relaxed)) {
      return false;
    }
    
    // Inflight 제한 체크 및 drop
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      // max_inflight 초과 시 가장 오래된 요청들을 drop
      // TryCancel()을 호출하지 않음 - CompletionQueue에서 자연스럽게 처리되도록
      while (inflight_requests_.size() >= max_inflight_) {
        if (!inflight_requests_.empty()) {
          auto oldest = inflight_requests_.begin();
          // 유효성 플래그 설정 후 drop
          oldest->second->valid.store(false, std::memory_order_release);
          inflight_requests_.erase(oldest);
          stats_.rpc_dropped.fetch_add(1, std::memory_order_relaxed);
        } else {
          break;
        }
      }
      
      // 여전히 max_inflight에 도달했다면 새 요청 거부
      if (inflight_requests_.size() >= max_inflight_) {
        stats_.rpc_dropped.fetch_add(1, std::memory_order_relaxed);
        return false;
      }
    }
    
    // 요청 생성
    auto ctx = std::make_unique<AsyncCheckContext>();
    ctx->user_key = user_key;
    ctx->request = std::make_unique<bloomcheck::BatchRequest>();
    ctx->response = std::make_unique<bloomcheck::BatchResponse>();
    ctx->context = std::make_unique<grpc::ClientContext>();
    
    bloomcheck::KeyRequest* key_req = ctx->request->add_reqs();
    key_req->set_user_key(user_key);
    for (uint64_t file_number : candidate_file_numbers) {
      key_req->add_sst_file_numbers(file_number);
    }
    
    // 비동기 RPC 발사
    AsyncCheckContext* ctx_ptr = ctx.get();
    
    // 디버깅: RPC 발사 전 로그 (처음 10개)
    static std::atomic<uint64_t> fire_counter{0};
    uint64_t fire_id = fire_counter.fetch_add(1, std::memory_order_relaxed);
    if (fire_id < 10) {
      std::cerr << "[BloomCheck] FireAsyncCheck #" << fire_id 
                << ": preparing async RPC, ctx_ptr=" << ctx_ptr << std::endl;
      std::cerr.flush();
    }
    
    ctx->reader = stub_->PrepareAsyncCheck(ctx->context.get(), *ctx->request, &cq_);
    
    if (fire_id < 10) {
      std::cerr << "[BloomCheck] FireAsyncCheck #" << fire_id 
                << ": PrepareAsyncCheck done, reader=" << ctx->reader.get() << std::endl;
      std::cerr.flush();
    }
    
    ctx->reader->StartCall();
    
    if (fire_id < 10) {
      std::cerr << "[BloomCheck] FireAsyncCheck #" << fire_id 
                << ": StartCall() called" << std::endl;
      std::cerr.flush();
    }
    
    ctx->reader->Finish(ctx->response.get(), &ctx->status, ctx_ptr);
    
    if (fire_id < 10) {
      std::cerr << "[BloomCheck] FireAsyncCheck #" << fire_id 
                << ": Finish() called with tag=" << ctx_ptr << std::endl;
      std::cerr.flush();
    }
    
    // 디버깅: RPC 발사 로그 (thread-safe) - 디버깅 중이므로 잠시 비활성화
    // static std::atomic<uint64_t> rpc_counter{0};
    // uint64_t rpc_id = rpc_counter.fetch_add(1, std::memory_order_relaxed);
    // if (rpc_id % 1000 == 0 && logger_) {
    //   std::string user_key_preview = user_key.substr(0, 16);
    //   ROCKS_LOG_INFO(logger_,
    //                  "[BloomCheck] FireAsyncCheck #%lu: user_key=%s..., "
    //                  "candidate_files=%zu",
    //                  rpc_id, user_key_preview.c_str(),
    //                  candidate_file_numbers.size());
    // }
    
    // Inflight에 추가
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      inflight_requests_[ctx_ptr] = std::move(ctx);
      stats_.inflight_rpc_count.store(inflight_requests_.size(), std::memory_order_relaxed);
    }
    
    return true;
  }
  
  // 캐시에서 BloomHint 조회 (non-blocking)
  // 없으면 nullptr 반환 (baseline 동작)
  const BloomHint* GetHint(const std::string& user_key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = hint_cache_.find(user_key);
    if (it == hint_cache_.end()) {
      stats_.cache_misses++;
      return nullptr;
    }
    
    if (it->second.IsExpired(hint_ttl_)) {
      hint_cache_.erase(it);
      stats_.cache_misses++;
      return nullptr;
    }
    
    stats_.cache_hits++;
    return &it->second;
  }
  
  // 스킵된 파일 수 기록 (통계용)
  void RecordSkippedFiles(uint64_t count) {
    stats_.sst_files_skipped.fetch_add(count, std::memory_order_relaxed);
  }
  
  // 통계 조회
  struct Stats {
    uint64_t inflight_rpc_count;
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t rpc_dropped;
    uint64_t rpc_timeout;
    uint64_t rpc_success;
    uint64_t sst_files_skipped;
  };
  
  Stats GetStats() const {
    Stats s;
    s.inflight_rpc_count = stats_.inflight_rpc_count.load(std::memory_order_relaxed);
    s.cache_hits = stats_.cache_hits.load(std::memory_order_relaxed);
    s.cache_misses = stats_.cache_misses.load(std::memory_order_relaxed);
    s.rpc_dropped = stats_.rpc_dropped.load(std::memory_order_relaxed);
    s.rpc_timeout = stats_.rpc_timeout.load(std::memory_order_relaxed);
    s.rpc_success = stats_.rpc_success.load(std::memory_order_relaxed);
    s.sst_files_skipped = stats_.sst_files_skipped.load(std::memory_order_relaxed);
    return s;
  }
  
 private:
  void ProcessCompletionQueue() {
    // 스레드 시작 로그를 stderr로도 출력 (더 눈에 띄게)
    std::cerr << "[BloomCheck] ProcessCompletionQueue thread started, thread_id=" << std::this_thread::get_id() << std::endl;
    std::cerr.flush();
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread started");
    }
    
    void* tag;
    bool ok;
    uint64_t total_responses = 0;
    uint64_t loop_count = 0;
    
    std::cerr << "[BloomCheck] ProcessCompletionQueue entering main loop..." << std::endl;
    std::cerr.flush();
    
    // 첫 번째 cq_.Next() 호출 전에 강제로 로그 출력
    std::cerr << "[BloomCheck] About to call first cq_.Next()..." << std::endl;
    std::cerr.flush();
    
    while (true) {
      loop_count++;
      
      // 디버깅: cq_.Next() 호출 전 로그 (처음 100번은 모두 출력)
      if (loop_count <= 100) {
        std::cerr << "[BloomCheck] ProcessCompletionQueue calling cq_.Next() #" << loop_count << std::endl;
        std::cerr.flush();
      } else if (loop_count % 10000 == 0) {
        std::cerr << "[BloomCheck] ProcessCompletionQueue calling cq_.Next() #" << loop_count << std::endl;
        std::cerr.flush();
      }
      
      bool got_event = cq_.Next(&tag, &ok);
      
      // cq_.Next() 반환 후 즉시 로그 출력
      if (loop_count <= 100) {
        std::cerr << "[BloomCheck] ProcessCompletionQueue cq_.Next() #" << loop_count 
                  << " returned: got_event=" << (got_event ? 1 : 0) << std::endl;
        std::cerr.flush();
      }
      
      if (!got_event) {
        std::cerr << "[BloomCheck] ProcessCompletionQueue cq_.Next() returned false, breaking loop" << std::endl;
        break;
      }
      
      total_responses++;
      
      // 디버깅: 모든 응답에 대해 로그 (처음 100개, 이후 1000번마다)
      // stderr로도 출력하여 더 눈에 띄게
      if (total_responses <= 100 || total_responses % 1000 == 0) {
        std::cerr << "[BloomCheck] ProcessCompletionQueue received response #" << total_responses 
                  << ": ok=" << (ok ? 1 : 0) << ", tag=" << tag << std::endl;
        std::cerr.flush();
        if (logger_) {
          ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue received response #%lu: ok=%d", total_responses, ok ? 1 : 0);
        }
      }
      
      if (shutdown_.load(std::memory_order_relaxed)) {
        std::cerr << "[BloomCheck] ProcessCompletionQueue shutting down, total_responses=" << total_responses << std::endl;
        if (logger_) {
          ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue shutting down, total_responses=%lu", total_responses);
        }
        break;
      }
      
      AsyncCheckContext* ctx = static_cast<AsyncCheckContext*>(tag);
      
      // Inflight에서 먼저 찾기 (ctx 포인터 접근 전에)
      // ctx 포인터가 이미 삭제된 메모리를 가리킬 수 있으므로,
      // 먼저 inflight_requests_에서 찾아서 안전하게 접근
      std::unique_ptr<AsyncCheckContext> ctx_ptr;
      {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        auto it = inflight_requests_.find(ctx);
        if (it != inflight_requests_.end()) {
          // 찾았으면 소유권 이전
          ctx_ptr = std::move(it->second);
          inflight_requests_.erase(it);
          stats_.inflight_rpc_count.store(inflight_requests_.size(), std::memory_order_relaxed);
        } else {
          // 이미 drop된 요청의 응답 - 무시
          // 디버깅: drop된 응답 로그 (처음 100개, 이후 1000번마다)
          static std::atomic<uint64_t> dropped_resp_counter{0};
          uint64_t dropped_id = dropped_resp_counter.fetch_add(1, std::memory_order_relaxed);
          if (dropped_id <= 100 || dropped_id % 1000 == 0) {
            if (logger_) {
              ROCKS_LOG_INFO(logger_,
                             "[BloomCheck] Dropped response #%lu (already removed from inflight)",
                             dropped_id);
            }
          }
          continue;
        }
      }
      
      // ctx_ptr가 nullptr이면 무시 (방어적 체크)
      if (!ctx_ptr) {
        continue;
      }
      
      // 이제 안전하게 valid 플래그 체크 가능
      if (!ctx_ptr->valid.load(std::memory_order_acquire)) {
        // drop된 요청 - 무시
        continue;
      }
      
      // 타임아웃 체크 (예: 100ms)
      auto elapsed = std::chrono::steady_clock::now() - ctx_ptr->start_time;
      auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
      
      static std::atomic<uint64_t> response_counter{0};
      uint64_t resp_id = response_counter.fetch_add(1, std::memory_order_relaxed);
      
      // 디버깅: 응답 수신 로그 (처음 100개, 이후 1000번마다) - thread-safe하게
      if (resp_id <= 100 || resp_id % 1000 == 0) {
        std::string user_key_preview = ctx_ptr->user_key.substr(0, 16);
        std::cerr << "[BloomCheck] ProcessCompletionQueue #" << resp_id 
                  << ": ok=" << (ok ? 1 : 0)
                  << ", status.ok()=" << (ctx_ptr->status.ok() ? 1 : 0)
                  << ", status.code()=" << ctx_ptr->status.error_code()
                  << ", elapsed_ms=" << elapsed_ms
                  << ", resps_size()=" << ctx_ptr->response->resps_size()
                  << ", user_key=" << user_key_preview << "..." << std::endl;
        if (logger_) {
          ROCKS_LOG_INFO(logger_,
                         "[BloomCheck] ProcessCompletionQueue #%lu: ok=%d, "
                         "status.ok()=%d, status.code()=%d, elapsed_ms=%ld, "
                         "resps_size()=%d, user_key=%s...",
                         resp_id, ok ? 1 : 0,
                         ctx_ptr->status.ok() ? 1 : 0,
                         ctx_ptr->status.error_code(),
                         elapsed_ms,
                         ctx_ptr->response->resps_size(),
                         user_key_preview.c_str());
        }
      }
      
      if (elapsed > std::chrono::milliseconds(100)) {
        if (resp_id % 100 == 0 && logger_) {
          ROCKS_LOG_INFO(logger_,
                         "[BloomCheck] RPC timeout: elapsed_ms=%ld",
                         elapsed_ms);
        }
        stats_.rpc_timeout.fetch_add(1, std::memory_order_relaxed);
        continue;
      }
      
      // 응답 처리
      if (ok && ctx_ptr->status.ok() && 
          ctx_ptr->response->resps_size() > 0) {
        const auto& key_resp = ctx_ptr->response->resps(0);
        
        // BloomHint 생성 및 캐시 저장
        BloomHint hint;
        hint.ts = std::chrono::steady_clock::now();
        for (uint64_t file_number : key_resp.may_match_sst_file_numbers()) {
          hint.may_match_files.insert(file_number);
        }
        
        // 디버깅: 성공 로그 (100번마다)
        if (resp_id % 100 == 0 && logger_) {
          ROCKS_LOG_INFO(logger_,
                         "[BloomCheck] RPC success: may_match_files=%zu",
                         hint.may_match_files.size());
        }
        
        {
          std::lock_guard<std::mutex> lock(cache_mutex_);
          hint_cache_[ctx_ptr->user_key] = std::move(hint);
          
          // 캐시 크기 제한 (예: 10000개)
          if (hint_cache_.size() > 10000) {
            // 가장 오래된 항목 제거
            auto oldest = hint_cache_.begin();
            for (auto it = hint_cache_.begin(); it != hint_cache_.end(); ++it) {
              if (it->second.ts < oldest->second.ts) {
                oldest = it;
              }
            }
            hint_cache_.erase(oldest);
          }
        }
        
        stats_.rpc_success.fetch_add(1, std::memory_order_relaxed);
      } else {
        // 실패 원인: ok=false 또는 status.ok()=false
        if (resp_id % 100 == 0 && logger_) {
          ROCKS_LOG_INFO(logger_,
                         "[BloomCheck] RPC failed: ok=%d, status.ok()=%d, "
                         "status.code()=%d, status.message()=%s, resps_size()=%d",
                         ok ? 1 : 0,
                         ctx_ptr->status.ok() ? 1 : 0,
                         ctx_ptr->status.error_code(),
                         ctx_ptr->status.error_message().c_str(),
                         ctx_ptr->response->resps_size());
        }
        stats_.rpc_timeout.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }
  
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<bloomcheck::BloomCheck::Stub> stub_;
  grpc::CompletionQueue cq_;
  std::thread cq_thread_;
  std::atomic<bool> shutdown_;
  
  size_t max_inflight_;
  std::chrono::milliseconds hint_ttl_;
  Logger* logger_;
  
  // Inflight 요청 관리
  std::mutex inflight_mutex_;
  std::unordered_map<AsyncCheckContext*, std::unique_ptr<AsyncCheckContext>> inflight_requests_;
  
  // BloomHint 캐시
  std::mutex cache_mutex_;
  std::unordered_map<std::string, BloomHint> hint_cache_;
  
  struct StatsInternal {
    std::atomic<uint64_t> inflight_rpc_count{0};
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> rpc_dropped{0};
    std::atomic<uint64_t> rpc_timeout{0};
    std::atomic<uint64_t> rpc_success{0};
    std::atomic<uint64_t> sst_files_skipped{0};
  };
  
  mutable StatsInternal stats_;
};

}  // namespace ROCKSDB_NAMESPACE

