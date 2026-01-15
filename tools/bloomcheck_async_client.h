// Copyright (c) 2024-present, RocksDB Contributors
// This source code is licensed under both the GPLv2 and Apache 2.0 License

#pragma once

#include <grpcpp/grpcpp.h>
#include <grpcpp/completion_queue.h>
#include <grpc/impl/codegen/gpr_types.h>
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
#include <sstream>
#include <iomanip>
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

// AsyncCall: CQ tag로 직접 사용되는 비동기 RPC 호출 구조체
// 메모리 해제는 CQ thread에서만 수행
struct AsyncCall {
  std::string user_key;
  bloomcheck::BatchRequest request;
  bloomcheck::BatchResponse response;
  grpc::ClientContext ctx;
  grpc::Status status;
  std::unique_ptr<grpc::ClientAsyncResponseReader<bloomcheck::BatchResponse>> rpc;
  
  std::chrono::steady_clock::time_point start;
  std::atomic<bool> cancelled{false};
  uint64_t call_id;  // 디버깅용 call ID
  
  AsyncCall() : start(std::chrono::steady_clock::now()), call_id(0) {}
  
  // 복사/이동 금지 (포인터로만 관리)
  AsyncCall(const AsyncCall&) = delete;
  AsyncCall& operator=(const AsyncCall&) = delete;
  AsyncCall(AsyncCall&&) = delete;
  AsyncCall& operator=(AsyncCall&&) = delete;
};

// 비동기 BloomCheck 서비스 클라이언트
class BloomCheckAsyncClient {
 public:
  explicit BloomCheckAsyncClient(const std::string& server_address,
                                 size_t max_inflight = 128,
                                 std::chrono::milliseconds hint_ttl = std::chrono::milliseconds(100),
                                 Logger* logger = nullptr)
      : channel_(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())),
        stub_(bloomcheck::BloomCheck::NewStub(channel_)),
        shutdown_(false),
        max_inflight_(max_inflight),
        hint_ttl_(hint_ttl),
        logger_(logger) {
    // 채널이 READY 상태가 될 때까지 대기
    std::chrono::system_clock::time_point deadline = 
        std::chrono::system_clock::now() + std::chrono::seconds(10);
    grpc_connectivity_state state = channel_->GetState(true);
    
    if (state == GRPC_CHANNEL_CONNECTING || state == GRPC_CHANNEL_IDLE) {
      if (logger_) {
        ROCKS_LOG_INFO(logger_, "[BloomCheck] Channel connecting (state=%d), waiting for READY...", state);
      }
      if (!channel_->WaitForStateChange(state, deadline)) {
        if (logger_) {
          ROCKS_LOG_WARN(logger_, "[BloomCheck] Channel failed to become READY within 10s");
        }
      } else {
        state = channel_->GetState(false);
        if (state == GRPC_CHANNEL_READY) {
          if (logger_) {
            ROCKS_LOG_INFO(logger_, "[BloomCheck] Channel is now READY");
          }
        }
      }
    } else if (state == GRPC_CHANNEL_READY) {
      if (logger_) {
        ROCKS_LOG_INFO(logger_, "[BloomCheck] Channel is already READY");
      }
    }
    
    // CompletionQueue 처리 스레드 시작
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] Starting ProcessCompletionQueue thread...");
    }
    cq_thread_ = std::thread(&BloomCheckAsyncClient::ProcessCompletionQueue, this);
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread started");
    }
    
    // 스레드가 시작될 시간 제공
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  
  ~BloomCheckAsyncClient() {
    // 1. shutdown 플래그 설정
    shutdown_.store(true, std::memory_order_relaxed);
    
    // 2. 모든 inflight call에 대해 TryCancel() 호출 (graceful shutdown)
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      for (auto& pair : inflight_requests_) {
        AsyncCall* call = pair.first;
        if (call && !call->cancelled.load(std::memory_order_relaxed)) {
          call->cancelled.store(true, std::memory_order_relaxed);
          call->ctx.TryCancel();
        }
      }
    }
    
    // 3. CompletionQueue 종료
    cq_.Shutdown();
    
    // 4. CQ thread 종료 대기
    if (cq_thread_.joinable()) {
      cq_thread_.join();
    }
    
    // 5. 남은 inflight call 정리 (CQ thread가 이미 종료되었으므로 안전)
    // unique_ptr이 자동으로 delete하므로 clear()만 호출
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      inflight_requests_.clear();  // unique_ptr이 알아서 delete
    }
  }
  
  // user_key의 hex 문자열 변환 (로깅용)
  static std::string KeyToHex(const std::string& key) {
    std::ostringstream oss;
    size_t preview_len = std::min(size_t(16), key.size());
    for (size_t i = 0; i < preview_len; ++i) {
      oss << std::hex << std::setw(2) << std::setfill('0') 
          << (static_cast<unsigned char>(key[i]) & 0xff);
    }
    if (key.size() > preview_len) {
      oss << "...";
    }
    return oss.str();
  }
  
  // 비동기로 BloomCheck RPC 발사 (non-blocking)
  // 조건부 발사는 호출 측에서 처리
  bool FireAsyncCheck(const std::string& user_key,
                      const std::unordered_set<uint64_t>& candidate_file_numbers) {
    static std::atomic<uint64_t> fire_counter{0};
    uint64_t fire_id = fire_counter.fetch_add(1, std::memory_order_relaxed);
    
    if (shutdown_.load(std::memory_order_relaxed)) {
      return false;
    }
    
    // 1. AsyncCall 생성 (new로 할당)
    AsyncCall* call = new AsyncCall();
    call->user_key = user_key;
    call->call_id = fire_id;  // 디버깅용 call ID 설정
    
    bloomcheck::KeyRequest* key_req = call->request.add_reqs();
    key_req->set_user_key(user_key);
    for (uint64_t file_number : candidate_file_numbers) {
      key_req->add_sst_file_numbers(file_number);
    }
    
    // 2. inflight에 먼저 등록 (CRITICAL: Finish 전에 등록)
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      
      // max_inflight 체크 - 포화 시 그냥 drop (cancel 없음)
      if (inflight_requests_.size() >= max_inflight_) {
        delete call;  // 아직 inflight에 등록하지 않았으므로 여기서 삭제 가능
        stats_.rpc_dropped.fetch_add(1, std::memory_order_relaxed);
        if (fire_id < 10 || fire_id % 10000 == 0) {
          if (logger_) {
            ROCKS_LOG_INFO(logger_, "[BloomCheck] FireAsyncCheck #%lu: dropped (max_inflight=%zu reached)", 
                          fire_id, max_inflight_);
          }
        }
        return false;
      }
      
      // inflight에 등록
      inflight_requests_[call] = std::unique_ptr<AsyncCall>(call);
      stats_.inflight_rpc_count.store(inflight_requests_.size(), std::memory_order_relaxed);
      stats_.rpc_started.fetch_add(1, std::memory_order_relaxed);
    }
    
    // 3. PrepareAsync (CQ 사용)
    call->rpc = stub_->PrepareAsyncCheck(&call->ctx, call->request, &cq_);
    
    if (!call->rpc) {
      if (fire_id < 10) {
        if (logger_) {
          ROCKS_LOG_ERROR(logger_, "[BloomCheck] FireAsyncCheck #%lu: PrepareAsyncCheck returned null!", fire_id);
        }
      }
      // PrepareAsync 실패 시 inflight에서 제거 후 삭제
      {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        inflight_requests_.erase(call);
        stats_.inflight_rpc_count.store(inflight_requests_.size(), std::memory_order_relaxed);
      }
      delete call;
      return false;
    }
    
    // 4. StartCall()
    call->rpc->StartCall();
    
    // 5. Finish(tag=call) - call 포인터가 tag가 됨
    call->rpc->Finish(&call->response, &call->status, call);
    
    return true;
  }
  
  // 캐시에서 BloomHint 조회 (non-blocking)
  const BloomHint* GetHint(const std::string& user_key) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = hint_cache_.find(user_key);
    if (it == hint_cache_.end()) {
      stats_.cache_misses.fetch_add(1, std::memory_order_relaxed);
      return nullptr;
    }
    
    if (it->second.IsExpired(hint_ttl_)) {
      hint_cache_.erase(it);
      stats_.cache_misses.fetch_add(1, std::memory_order_relaxed);
      return nullptr;
    }
    
    stats_.cache_hits.fetch_add(1, std::memory_order_relaxed);
    
    // 캐시 hit 시 may_match_files.size() 샘플 로깅 (초기 100회 또는 1000회마다)
    static std::atomic<uint64_t> cache_hit_counter{0};
    uint64_t hit_id = cache_hit_counter.fetch_add(1, std::memory_order_relaxed);
    if (hit_id < 100 || hit_id % 1000 == 0) {
      size_t cached_size = it->second.may_match_files.size();
      std::cerr << "[BloomCheck] GetHint hit #" << hit_id 
                << ": may_match_files=" << cached_size << std::endl;
    }
    
    return &it->second;
  }
  
  // 스킵된 파일 수 기록
  void RecordSkippedFiles(uint64_t count) {
    stats_.sst_files_skipped.fetch_add(count, std::memory_order_relaxed);
  }
  
  // 통계 조회
  struct Stats {
    uint64_t inflight_rpc_count;
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t rpc_started;
    uint64_t rpc_completed;
    uint64_t rpc_ok;
    uint64_t rpc_failed;
    uint64_t rpc_cancelled;
    uint64_t rpc_dropped;
    uint64_t rpc_timed_out;
    uint64_t sst_files_skipped;
  };
  
  Stats GetStats() const {
    Stats s;
    s.inflight_rpc_count = stats_.inflight_rpc_count.load(std::memory_order_relaxed);
    s.cache_hits = stats_.cache_hits.load(std::memory_order_relaxed);
    s.cache_misses = stats_.cache_misses.load(std::memory_order_relaxed);
    s.rpc_started = stats_.rpc_started.load(std::memory_order_relaxed);
    s.rpc_completed = stats_.rpc_completed.load(std::memory_order_relaxed);
    s.rpc_ok = stats_.rpc_ok.load(std::memory_order_relaxed);
    s.rpc_failed = stats_.rpc_failed.load(std::memory_order_relaxed);
    s.rpc_cancelled = stats_.rpc_cancelled.load(std::memory_order_relaxed);
    s.rpc_dropped = stats_.rpc_dropped.load(std::memory_order_relaxed);
    s.rpc_timed_out = stats_.rpc_timed_out.load(std::memory_order_relaxed);
    s.sst_files_skipped = stats_.sst_files_skipped.load(std::memory_order_relaxed);
    return s;
  }
  
 private:
  void ProcessCompletionQueue() {
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread started");
    }
    
    void* tag = nullptr;
    bool ok = false;
    uint64_t total_responses = 0;
    
    while (true) {
      // AsyncNext 올바른 사용법: enum 반환값 처리
      // GPR_TIMESPAN을 사용하여 duration을 나타냄
      gpr_timespec deadline =
          gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                       gpr_time_from_millis(1000, GPR_TIMESPAN));
      
      auto next_status = cq_.AsyncNext(&tag, &ok, deadline);
      
      // TIMEOUT: continue
      if (next_status == grpc::CompletionQueue::NextStatus::TIMEOUT) {
        continue;
      }
      
      // SHUTDOWN: break
      if (next_status == grpc::CompletionQueue::NextStatus::SHUTDOWN) {
        if (logger_) {
          ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue: CQ shutdown");
        }
        break;
      }
      
      // GOT_EVENT: tag와 ok 사용
      if (next_status != grpc::CompletionQueue::NextStatus::GOT_EVENT) {
        continue;
      }
      
      total_responses++;
      stats_.rpc_completed.fetch_add(1, std::memory_order_relaxed);
      
      // tag가 nullptr인 경우 무시
      if (tag == nullptr) {
        if (total_responses <= 100 || total_responses % 1000 == 0) {
          if (logger_) {
            ROCKS_LOG_WARN(logger_, "[BloomCheck] ProcessCompletionQueue received null tag! ok=%d, response #%lu", 
                          ok ? 1 : 0, total_responses);
          }
        }
        continue;
      }
      
      // tag를 AsyncCall*로 캐스팅
      AsyncCall* call = static_cast<AsyncCall*>(tag);
      
      // inflight에서 찾기
      std::unique_ptr<AsyncCall> call_owner;
      {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        auto it = inflight_requests_.find(call);
        if (it != inflight_requests_.end()) {
          call_owner = std::move(it->second);
          inflight_requests_.erase(it);
          stats_.inflight_rpc_count.store(inflight_requests_.size(), std::memory_order_relaxed);
        } else {
          // 이미 제거된 call (drop된 경우 등) - 무시
          // CRITICAL: delete 하지 않음! unique_ptr이 이미 삭제했을 수 있음
          if (total_responses <= 100 || total_responses % 1000 == 0) {
            if (logger_) {
              ROCKS_LOG_WARN(logger_, "[BloomCheck] Dropped response (not in inflight), tag=%p", call);
            }
          }
          continue;  // NO delete - 더블 free 방지
        }
      }
      
      // call_owner가 유효한지 확인
      if (!call_owner || call_owner.get() != call) {
        // 메모리 안전성 체크 실패 - 로그만 남기고 무시
        // CRITICAL: delete 하지 않음! unique_ptr이 이미 삭제했을 수 있음
        if (total_responses <= 100 || total_responses % 1000 == 0) {
          if (logger_) {
            ROCKS_LOG_WARN(logger_, "[BloomCheck] Invalid call_owner: call=%p, owner=%p", 
                          call, call_owner ? call_owner.get() : nullptr);
          }
        }
        continue;  // NO delete - 더블 free 방지
      }
      
      // cancelled 플래그 체크
      bool was_cancelled = call->cancelled.load(std::memory_order_acquire);
      
      // 경과 시간 계산 (현재 사용하지 않음 - 주석처리된 로그에서만 사용)
      // auto elapsed = std::chrono::steady_clock::now() - call->start;
      // auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
      
      // 로그 출력 (처음 100개, 이후 1000번마다) - 주석처리
      // if (total_responses <= 100 || total_responses % 1000 == 0) {
      //   std::string user_key_hex = KeyToHex(call->user_key);
      //   if (logger_) {
      //     ROCKS_LOG_INFO(logger_,
      //                    "[BloomCheck] ProcessCompletionQueue #%lu: ok=%d, "
      //                    "status.ok()=%d, status.code()=%d, status.message()=%s, "
      //                    "cancelled=%d, elapsed_ms=%ld, user_key_hex=%s",
      //                    total_responses, ok ? 1 : 0,
      //                    call->status.ok() ? 1 : 0,
      //                    call->status.error_code(),
      //                    call->status.error_message().c_str(),
      //                    was_cancelled ? 1 : 0,
      //                    elapsed_ms,
      //                    user_key_hex.c_str());
      //   }
      // }
      
      // cancelled된 call은 캐시에 저장하지 않음 (shutdown 시에만 발생)
      if (was_cancelled) {
        stats_.rpc_cancelled.fetch_add(1, std::memory_order_relaxed);
        // call_owner가 소멸되면서 자동으로 delete됨
        continue;
      }
      
      // ok=false 또는 status.ok()=false 처리
      if (!ok || !call->status.ok()) {
        stats_.rpc_failed.fetch_add(1, std::memory_order_relaxed);
        
        // 로그 출력 - 주석처리
        // if (total_responses <= 100 || total_responses % 1000 == 0) {
        //   if (logger_) {
        //     ROCKS_LOG_INFO(logger_,
        //                    "[BloomCheck] RPC failed: ok=%d, status.ok()=%d, "
        //                    "status.code()=%d, status.message()=%s, elapsed_ms=%ld",
        //                    ok ? 1 : 0,
        //                    call->status.ok() ? 1 : 0,
        //                    call->status.error_code(),
        //                    call->status.error_message().c_str(),
        //                    elapsed_ms);
        //   }
        // }
        
        // call_owner가 소멸되면서 자동으로 delete됨
        continue;
      }
      
      // 성공: 응답 처리 및 캐시 저장
      // CRITICAL: call->response는 Finish() 후에 채워짐
      // 로그를 한 덩어리 문자열로 만들어서 원자적으로 출력
      {
        std::ostringstream oss;
        oss << "[BloomCheck] call_id=" << call->call_id
            << " resps_size=" << call->response.resps_size();
        
        size_t key0_size = 0;
        if (call->response.resps_size() > 0) {
          key0_size = call->response.resps(0).may_match_sst_file_numbers_size();
        }
        oss << " key0_size=" << key0_size
            << " resp=" << call->response.DebugString();
        
        // ROCKS_LOG_INFO는 thread-safe하므로 사용
        if (logger_) {
          ROCKS_LOG_INFO(logger_, "%s", oss.str().c_str());
        } else {
          // logger가 없으면 std::cerr 사용 (mutex로 보호)
          static std::mutex log_mutex;
          std::lock_guard<std::mutex> lock(log_mutex);
          std::cerr << oss.str() << std::endl;
        }
      }
      
      if (call->response.resps_size() > 0) {
        const auto& key_resp = call->response.resps(0);
        
        // BloomHint 생성 및 캐시 저장
        BloomHint hint;
        hint.ts = std::chrono::steady_clock::now();
        
        for (uint64_t file_number : key_resp.may_match_sst_file_numbers()) {
          hint.may_match_files.insert(file_number);
        }
        
        // CRITICAL: std::move 이전에 size 저장 및 로그 출력
        size_t may_match_count = hint.may_match_files.size();
        
        // may_match_files=0일 때 요청/응답 덤프 출력 (move 이전)
        if (may_match_count == 0) {
          std::ostringstream oss;
          oss << "[BloomCheck] RPC success: may_match_files=0 call_id=" << call->call_id;
          if (logger_) {
            ROCKS_LOG_INFO(logger_, "%s", oss.str().c_str());
          } else {
            static std::mutex log_mutex;
            std::lock_guard<std::mutex> lock(log_mutex);
            std::cerr << oss.str() << std::endl;
          }
        }
        
        {
          std::lock_guard<std::mutex> lock(cache_mutex_);
          hint_cache_[call->user_key] = std::move(hint);  // move 이후 hint는 사용하지 않음
          
          // 캐시 크기 제한 (10000개)
          if (hint_cache_.size() > 10000) {
            // 가장 오래된 항목 제거 (O(n) 스캔)
            auto oldest = hint_cache_.begin();
            for (auto it = hint_cache_.begin(); it != hint_cache_.end(); ++it) {
              if (it->second.ts < oldest->second.ts) {
                oldest = it;
              }
            }
            hint_cache_.erase(oldest);
          }
        }
        
        stats_.rpc_ok.fetch_add(1, std::memory_order_relaxed);
        
        // move 이후에는 hint를 절대 참조하지 않음
      } else {
        // 응답이 비어있음
        stats_.rpc_failed.fetch_add(1, std::memory_order_relaxed);
      }
      
      // call_owner가 소멸되면서 자동으로 delete됨
    }
    
    // 남은 inflight call 정리
    // unique_ptr이 자동으로 delete하므로 clear()만 호출
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      inflight_requests_.clear();  // unique_ptr이 알아서 delete
    }
    
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread exiting, total_responses=%lu", 
                    total_responses);
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
  
  // Inflight 요청 관리 (AsyncCall* -> unique_ptr<AsyncCall>)
  std::mutex inflight_mutex_;
  std::unordered_map<AsyncCall*, std::unique_ptr<AsyncCall>> inflight_requests_;
  
  // BloomHint 캐시
  std::mutex cache_mutex_;
  std::unordered_map<std::string, BloomHint> hint_cache_;
  
  struct StatsInternal {
    std::atomic<uint64_t> inflight_rpc_count{0};
    std::atomic<uint64_t> cache_hits{0};
    std::atomic<uint64_t> cache_misses{0};
    std::atomic<uint64_t> rpc_started{0};
    std::atomic<uint64_t> rpc_completed{0};
    std::atomic<uint64_t> rpc_ok{0};
    std::atomic<uint64_t> rpc_failed{0};
    std::atomic<uint64_t> rpc_cancelled{0};
    std::atomic<uint64_t> rpc_dropped{0};
    std::atomic<uint64_t> rpc_timed_out{0};
    std::atomic<uint64_t> sst_files_skipped{0};
  };
  
  mutable StatsInternal stats_;
};

}  // namespace ROCKSDB_NAMESPACE
