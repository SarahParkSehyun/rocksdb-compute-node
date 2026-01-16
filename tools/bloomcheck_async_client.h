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

// FileHint: file_number별 캐시된 BloomCheck 결과
// positive: 최근에 may_match=true였던 시각
// negative: 최근에 may_match=false였던 시각
struct FileHint {
  std::chrono::steady_clock::time_point positive_ts;  // may_match=true였던 시각
  std::chrono::steady_clock::time_point negative_ts;  // may_match=false였던 시각
  
  FileHint() {
    // 초기값: 매우 오래된 시각 (캐시 미스로 처리)
    positive_ts = std::chrono::steady_clock::time_point::min();
    negative_ts = std::chrono::steady_clock::time_point::min();
  }
  
  void UpdatePositive() {
    positive_ts = std::chrono::steady_clock::now();
  }
  
  void UpdateNegative() {
    negative_ts = std::chrono::steady_clock::now();
  }
  
  // negative가 최근인지 확인 (예: 5ms 이내)
  bool IsRecentNegative(std::chrono::milliseconds threshold) const {
    if (negative_ts == std::chrono::steady_clock::time_point::min()) {
      return false;
    }
    auto now = std::chrono::steady_clock::now();
    return (now - negative_ts) < threshold;
  }
  
  // positive가 최근인지 확인
  bool IsRecentPositive(std::chrono::milliseconds threshold) const {
    if (positive_ts == std::chrono::steady_clock::time_point::min()) {
      return false;
    }
    auto now = std::chrono::steady_clock::now();
    return (now - positive_ts) < threshold;
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
    
    // CompletionQueue 처리 스레드 시작 (여러 개)
    // N = min(8, hardware_concurrency()/4)로 설정
    size_t num_cq_threads = std::min(static_cast<size_t>(8), 
                                      std::max(static_cast<size_t>(1), 
                                               static_cast<size_t>(std::thread::hardware_concurrency()) / 4));
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] Starting %zu ProcessCompletionQueue threads...", num_cq_threads);
    }
    cq_threads_.reserve(num_cq_threads);
    for (size_t i = 0; i < num_cq_threads; ++i) {
      cq_threads_.emplace_back(&BloomCheckAsyncClient::ProcessCompletionQueue, this);
    }
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] %zu ProcessCompletionQueue threads started", num_cq_threads);
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
    
    // 4. CQ threads 종료 대기
    for (auto& thread : cq_threads_) {
      if (thread.joinable()) {
        thread.join();
      }
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
  
  // file_number별 힌트 조회 (non-blocking)
  // negative가 최근이면 true 반환 (스킵 권장)
  // positive가 최근이면 false 반환 (처리 권장)
  bool ShouldSkipFile(uint64_t file_number) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    
    auto it = file_hint_cache_.find(file_number);
    if (it == file_hint_cache_.end()) {
      stats_.cache_misses.fetch_add(1, std::memory_order_relaxed);
      return false;  // 힌트 없음 → 처리
    }
    
    // negative가 최근(예: 5ms)이면 스킵 권장
    if (it->second.IsRecentNegative(std::chrono::milliseconds(5))) {
      stats_.cache_hits.fetch_add(1, std::memory_order_relaxed);
      return true;  // 스킵
    }
    
    stats_.cache_hits.fetch_add(1, std::memory_order_relaxed);
    return false;  // 처리
  }
  
  // 레거시 호환성: GetHint는 더 이상 사용하지 않음 (제거됨)
  // file_number별 힌트는 ShouldSkipFile()로 조회
  
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
    static std::atomic<uint64_t> thread_id_counter{0};
    uint64_t thread_id = thread_id_counter.fetch_add(1);
    
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread #%lu started", thread_id);
    }
    
    void* tag = nullptr;
    bool ok = false;
    uint64_t total_responses = 0;
    
    while (true) {
      // 첫 번째 AsyncNext: 긴 deadline(1초)으로 이벤트 확보
      gpr_timespec long_deadline =
          gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                       gpr_time_from_millis(1000, GPR_TIMESPAN));
      
      auto next_status = cq_.AsyncNext(&tag, &ok, long_deadline);
      
      // TIMEOUT: continue (큐가 비어있음)
      if (next_status == grpc::CompletionQueue::NextStatus::TIMEOUT) {
        continue;
      }
      
      // SHUTDOWN: break
      if (next_status == grpc::CompletionQueue::NextStatus::SHUTDOWN) {
        if (logger_) {
          ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread #%lu: CQ shutdown", thread_id);
        }
        break;
      }
      
      // GOT_EVENT: 첫 이벤트 처리
      if (next_status != grpc::CompletionQueue::NextStatus::GOT_EVENT) {
        continue;
      }
      
      // 첫 이벤트 처리
      ProcessEvent(tag, ok, &total_responses);
      
      // 이벤트가 있는 동안 연속으로 drain (공격적 처리)
      // 짧은 deadline(0ms)으로 최대한 많은 이벤트를 빠르게 처리
      const size_t max_drain_batch = 100;  // 한 번에 최대 100개까지 drain
      for (size_t drain_count = 0; drain_count < max_drain_batch; ++drain_count) {
        gpr_timespec short_deadline =
            gpr_time_add(gpr_now(GPR_CLOCK_MONOTONIC),
                         gpr_time_from_millis(0, GPR_TIMESPAN));  // 즉시 반환
        
        auto drain_status = cq_.AsyncNext(&tag, &ok, short_deadline);
        
        if (drain_status == grpc::CompletionQueue::NextStatus::TIMEOUT) {
          // 큐가 비어있으면 drain 종료
          break;
        }
        
        if (drain_status == grpc::CompletionQueue::NextStatus::SHUTDOWN) {
          if (logger_) {
            ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread #%lu: CQ shutdown during drain", thread_id);
          }
          return;  // while 루프 종료
        }
        
        if (drain_status == grpc::CompletionQueue::NextStatus::GOT_EVENT) {
          ProcessEvent(tag, ok, &total_responses);
        }
      }
    }
    
    // 남은 inflight call 정리
    // unique_ptr이 자동으로 delete하므로 clear()만 호출
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      // 다른 스레드가 이미 정리했을 수 있으므로 여기서는 로그만
    }
    
    if (logger_) {
      ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread #%lu exiting, total_responses=%lu", 
                    thread_id, total_responses);
    }
  }
  
  // 이벤트 처리 헬퍼 함수 (락 보호 내부 로직)
  void ProcessEvent(void* tag, bool ok, uint64_t* total_responses) {
    (*total_responses)++;
    stats_.rpc_completed.fetch_add(1, std::memory_order_relaxed);
    
    // tag가 nullptr인 경우 무시
    if (tag == nullptr) {
        if (*total_responses <= 100 || (*total_responses) % 1000 == 0) {
          if (logger_) {
            ROCKS_LOG_WARN(logger_, "[BloomCheck] ProcessCompletionQueue received null tag! ok=%d, response #%lu", 
                          ok ? 1 : 0, *total_responses);
          }
        }
        return;
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
          if (*total_responses <= 100 || (*total_responses) % 1000 == 0) {
            if (logger_) {
              ROCKS_LOG_WARN(logger_, "[BloomCheck] Dropped response (not in inflight), tag=%p", call);
            }
          }
          return;  // NO delete - 더블 free 방지
        }
      }
      
      // call_owner가 유효한지 확인
      if (!call_owner || call_owner.get() != call) {
        // 메모리 안전성 체크 실패 - 로그만 남기고 무시
        // CRITICAL: delete 하지 않음! unique_ptr이 이미 삭제했을 수 있음
        if (*total_responses <= 100 || (*total_responses) % 1000 == 0) {
          if (logger_) {
            ROCKS_LOG_WARN(logger_, "[BloomCheck] Invalid call_owner: call=%p, owner=%p", 
                          call, call_owner ? call_owner.get() : nullptr);
          }
        }
        return;  // NO delete - 더블 free 방지
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
        return;
      }
      
      // ok=false 또는 status.ok()=false 처리
      if (!ok || !call->status.ok()) {
        stats_.rpc_failed.fetch_add(1, std::memory_order_relaxed);
        
        // 로그 출력 - 주석처리
        // if (*total_responses <= 100 || (*total_responses) % 1000 == 0) {
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
        return;
      }
      
      // 성공: 응답 처리 및 캐시 저장
      // CRITICAL: call->response는 Finish() 후에 채워짐
      // 로그를 한 덩어리 문자열로 만들어서 원자적으로 출력 - 주석처리
      // {
      //   std::ostringstream oss;
      //   oss << "[BloomCheck] call_id=" << call->call_id
      //       << " resps_size=" << call->response.resps_size();
      //   
      //   size_t key0_size = 0;
      //   if (call->response.resps_size() > 0) {
      //     key0_size = call->response.resps(0).may_match_sst_file_numbers_size();
      //   }
      //   oss << " key0_size=" << key0_size
      //       << " resp=" << call->response.DebugString();
      //   
      //   // ROCKS_LOG_INFO는 thread-safe하므로 사용
      //   if (logger_) {
      //     ROCKS_LOG_INFO(logger_, "%s", oss.str().c_str());
      //   } else {
      //     // logger가 없으면 std::cerr 사용 (mutex로 보호)
      //     static std::mutex log_mutex;
      //     std::lock_guard<std::mutex> lock(log_mutex);
      //     std::cerr << oss.str() << std::endl;
      //   }
      // }
      
      if (call->response.resps_size() > 0) {
        const auto& key_resp = call->response.resps(0);
        
        // file_number별 힌트 캐시 업데이트
        std::unordered_set<uint64_t> may_match_files;
        for (uint64_t file_number : key_resp.may_match_sst_file_numbers()) {
          may_match_files.insert(file_number);
        }
        
        // 요청에 포함된 모든 candidate 파일 번호 수집
        std::unordered_set<uint64_t> candidate_files;
        for (const auto& req : call->request.reqs()) {
          for (uint64_t file_number : req.sst_file_numbers()) {
            candidate_files.insert(file_number);
          }
        }
        
        {
          std::lock_guard<std::mutex> lock(cache_mutex_);
          
          // 각 candidate 파일에 대해 positive/negative 업데이트
          for (uint64_t file_number : candidate_files) {
            auto it = file_hint_cache_.find(file_number);
            if (it == file_hint_cache_.end()) {
              // 새 파일 힌트 생성
              FileHint hint;
              if (may_match_files.find(file_number) != may_match_files.end()) {
                hint.UpdatePositive();
              } else {
                hint.UpdateNegative();
              }
              file_hint_cache_[file_number] = hint;
            } else {
              // 기존 힌트 업데이트
              if (may_match_files.find(file_number) != may_match_files.end()) {
                it->second.UpdatePositive();
              } else {
                it->second.UpdateNegative();
              }
            }
          }
          
          // 캐시 크기 제한 (10000개)
          if (file_hint_cache_.size() > 10000) {
            // 가장 오래된 negative 힌트부터 제거
            uint64_t oldest_file = 0;
            auto oldest_time = std::chrono::steady_clock::time_point::max();
            for (const auto& pair : file_hint_cache_) {
              auto time = std::min(pair.second.positive_ts, pair.second.negative_ts);
              if (time < oldest_time) {
                oldest_time = time;
                oldest_file = pair.first;
              }
            }
            if (oldest_file != 0) {
              file_hint_cache_.erase(oldest_file);
            }
          }
          
          // 디버깅: 캐시 업데이트 로깅
          static std::atomic<uint64_t> cache_update_log_counter{0};
          uint64_t cache_update_log_id = cache_update_log_counter.fetch_add(1, std::memory_order_relaxed);
          if (cache_update_log_id < 100 || cache_update_log_id % 1000 == 0) {
            size_t cache_size = file_hint_cache_.size();
            size_t positive_count = 0;
            size_t negative_count = 0;
            for (const auto& pair : file_hint_cache_) {
              if (pair.second.IsRecentPositive(std::chrono::milliseconds(100))) {
                positive_count++;
              }
              if (pair.second.IsRecentNegative(std::chrono::milliseconds(100))) {
                negative_count++;
              }
            }
            if (logger_) {
              ROCKS_LOG_INFO(logger_,
                             "[BloomCheck] CQ cache_update: call_id=%lu "
                             "candidate_files=%zu may_match_files=%zu "
                             "cache_size=%zu positive_recent=%zu negative_recent=%zu",
                             call->call_id, candidate_files.size(), may_match_files.size(),
                             cache_size, positive_count, negative_count);
            }
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
  
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<bloomcheck::BloomCheck::Stub> stub_;
  grpc::CompletionQueue cq_;
  std::vector<std::thread> cq_threads_;  // 여러 CQ polling threads
  std::atomic<bool> shutdown_;
  
  size_t max_inflight_;
  std::chrono::milliseconds hint_ttl_;
  Logger* logger_;
  
  // Inflight 요청 관리 (AsyncCall* -> unique_ptr<AsyncCall>)
  std::mutex inflight_mutex_;
  std::unordered_map<AsyncCall*, std::unique_ptr<AsyncCall>> inflight_requests_;
  
  // FileHint 캐시 (file_number -> FileHint)
  std::mutex cache_mutex_;
  std::unordered_map<uint64_t, FileHint> file_hint_cache_;
  
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
