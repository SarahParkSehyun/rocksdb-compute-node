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

// PerCallState: Get 요청 단위로 BloomCheck 결과를 추적하는 구조
struct PerCallState {
  std::unordered_set<uint64_t> candidate_file_numbers;  // 원래 요청한 파일 번호들
  std::unordered_set<uint64_t> may_match_file_numbers;  // 서버가 반환한 may_match 파일 번호들
  bool complete;  // 완전한 응답인지 여부
  bool ready;  // 결과가 준비되었는지 여부
  
  PerCallState() : complete(false), ready(false) {}
  
  // 결과 설정
  void SetResult(const std::unordered_set<uint64_t>& candidates,
                 const std::unordered_set<uint64_t>& may_match,
                 bool is_complete) {
    candidate_file_numbers = candidates;
    may_match_file_numbers = may_match;
    complete = is_complete;
    ready = true;
  }
  
  // 파일이 may_match인지 확인 (ready && complete일 때만 신뢰 가능)
  bool IsMayMatch(uint64_t file_number) const {
    if (!ready || !complete) {
      return true;  // 신뢰할 수 없으면 안전하게 true 반환
    }
    return may_match_file_numbers.find(file_number) != may_match_file_numbers.end();
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
                                 Logger* logger = nullptr)
      : channel_(grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials())),
        stub_(bloomcheck::BloomCheck::NewStub(channel_)),
        shutdown_(false),
        max_inflight_(max_inflight),
        logger_(logger),
        overload_cooldown_us_(200),  // 기본값 200us
        next_fire_allowed_ts_(0),
        tokens_(max_inflight / 2),  // 초기 토큰: max_inflight의 절반
        last_refill_time_(0),
        last_completed_count_(0) {
    // 채널이 READY 상태가 될 때까지 대기
    std::chrono::system_clock::time_point deadline = 
        std::chrono::system_clock::now() + std::chrono::seconds(10);
    grpc_connectivity_state state = channel_->GetState(true);
    
    // 로그 주석처리
    // if (state == GRPC_CHANNEL_CONNECTING || state == GRPC_CHANNEL_IDLE) {
    //   if (logger_) {
    //     ROCKS_LOG_INFO(logger_, "[BloomCheck] Channel connecting (state=%d), waiting for READY...", state);
    //   }
      if (!channel_->WaitForStateChange(state, deadline)) {
        // if (logger_) {
        //   ROCKS_LOG_WARN(logger_, "[BloomCheck] Channel failed to become READY within 10s");
        // }
      } else {
        state = channel_->GetState(false);
        // if (state == GRPC_CHANNEL_READY) {
        //   if (logger_) {
        //     ROCKS_LOG_INFO(logger_, "[BloomCheck] Channel is now READY");
        //   }
        // }
      }
    // } else if (state == GRPC_CHANNEL_READY) {
    //   if (logger_) {
    //     ROCKS_LOG_INFO(logger_, "[BloomCheck] Channel is already READY");
    //   }
    // }
    
    // CompletionQueue 처리 스레드 시작 (여러 개)
    // N = min(8, hardware_concurrency()/4)로 설정
    size_t num_cq_threads = std::min(static_cast<size_t>(8), 
                                      std::max(static_cast<size_t>(1), 
                                               static_cast<size_t>(std::thread::hardware_concurrency()) / 4));
    // 로그 주석처리
    // if (logger_) {
    //   ROCKS_LOG_INFO(logger_, "[BloomCheck] Starting %zu ProcessCompletionQueue threads...", num_cq_threads);
    // }
    cq_threads_.reserve(num_cq_threads);
    for (size_t i = 0; i < num_cq_threads; ++i) {
      cq_threads_.emplace_back(&BloomCheckAsyncClient::ProcessCompletionQueue, this);
    }
    // if (logger_) {
    //   ROCKS_LOG_INFO(logger_, "[BloomCheck] %zu ProcessCompletionQueue threads started", num_cq_threads);
    // }
    
    // Token refill thread 시작
    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    last_refill_time_.store(now_ns, std::memory_order_relaxed);
    last_completed_count_.store(0, std::memory_order_relaxed);
    token_refill_thread_ = std::thread(&BloomCheckAsyncClient::TokenRefillLoop, this);
    
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
    
    // 4. Token refill thread 종료 대기
    if (token_refill_thread_.joinable()) {
      token_refill_thread_.join();
    }
    
    // 5. CQ threads 종료 대기
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
  // 반환값: call_id (0 = 실패/스킵, 0이 아니면 성공한 call_id)
  // 조건부 발사는 호출 측에서 처리
  uint64_t FireAsyncCheck(const std::string& user_key,
                          const std::unordered_set<uint64_t>& candidate_file_numbers) {
    static std::atomic<uint64_t> fire_counter{0};
    uint64_t fire_id = fire_counter.fetch_add(1, std::memory_order_relaxed);
    
    if (shutdown_.load(std::memory_order_relaxed)) {
      return 0;  // 0 = 실패
    }
    
    // 1. Overload cooldown 체크
    auto now_time = std::chrono::steady_clock::now();
    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        now_time.time_since_epoch()).count();
    uint64_t next_allowed = next_fire_allowed_ts_.load(std::memory_order_acquire);
    if (static_cast<int64_t>(now_ns) < static_cast<int64_t>(next_allowed)) {
      stats_.skipped_due_to_overload_cooldown.fetch_add(1, std::memory_order_relaxed);
      return 0;  // 0 = skip (not counted as dropped)
    }
    
    // 2. Token bucket 체크
    uint64_t tokens = tokens_.load(std::memory_order_acquire);
    if (tokens == 0) {
      stats_.skipped_due_to_tokens.fetch_add(1, std::memory_order_relaxed);
      return 0;  // 0 = skip (not counted as dropped)
    }
    
    // 토큰 소비 시도 (compare-and-swap)
    uint64_t expected = tokens;
    while (!tokens_.compare_exchange_weak(expected, tokens - 1, 
                                          std::memory_order_acq_rel,
                                          std::memory_order_acquire)) {
      if (expected == 0) {
        stats_.skipped_due_to_tokens.fetch_add(1, std::memory_order_relaxed);
        return 0;  // 0 = skip (not counted as dropped)
      }
      tokens = expected;
      expected = tokens;
    }
    
    // 3. Per-call state 생성 (call_id로 추적)
    uint64_t call_id = fire_id + 1;  // 0이 아닌 값으로 사용 (fire_id는 0부터 시작)
    
    // 4. AsyncCall 생성 (new로 할당)
    AsyncCall* call = new AsyncCall();
    call->user_key = user_key;
    call->call_id = call_id;  // call ID 설정
    
    bloomcheck::KeyRequest* key_req = call->request.add_reqs();
    key_req->set_user_key(user_key);
    for (uint64_t file_number : candidate_file_numbers) {
      key_req->add_sst_file_numbers(file_number);
    }
    
    // 5. Per-call state 초기 등록 (결과 도착 전까지 ready=false)
    {
      std::lock_guard<std::mutex> lock(per_call_state_mutex_);
      PerCallState& state = per_call_states_[call_id];
      state.candidate_file_numbers = candidate_file_numbers;
      state.ready = false;
      state.complete = false;
    }
    
    // 6. inflight에 먼저 등록 (CRITICAL: Finish 전에 등록)
    {
      std::lock_guard<std::mutex> lock(inflight_mutex_);
      
      // max_inflight 체크 - 포화 시 그냥 drop (cancel 없음)
      if (inflight_requests_.size() >= max_inflight_) {
        delete call;  // 아직 inflight에 등록하지 않았으므로 여기서 삭제 가능
        
        // Per-call state 정리
        {
          std::lock_guard<std::mutex> lock2(per_call_state_mutex_);
          per_call_states_.erase(call_id);
        }
        
        stats_.drop_due_to_inflight_full.fetch_add(1, std::memory_order_relaxed);
        stats_.rpc_dropped.fetch_add(1, std::memory_order_relaxed);
        
        // Overload cooldown 설정 (현재 시간 + cooldown)
        auto cooldown_ns = std::chrono::microseconds(overload_cooldown_us_).count() * 1000;
        uint64_t new_next_allowed = now_ns + cooldown_ns;
        next_fire_allowed_ts_.store(new_next_allowed, std::memory_order_release);
        
        // 로그 주석처리
        // if (fire_id < 10 || fire_id % 10000 == 0) {
        //   if (logger_) {
        //     ROCKS_LOG_INFO(logger_, "[BloomCheck] FireAsyncCheck #%lu: dropped (max_inflight=%zu reached)", 
        //                   fire_id, max_inflight_);
        //   }
        // }
        return 0;  // 0 = 실패
      }
      
      // inflight에 등록
      inflight_requests_[call] = std::unique_ptr<AsyncCall>(call);
      stats_.inflight_rpc_count.store(inflight_requests_.size(), std::memory_order_relaxed);
      stats_.rpc_started.fetch_add(1, std::memory_order_relaxed);
    }
    
    // 7. PrepareAsync (CQ 사용)
    call->rpc = stub_->PrepareAsyncCheck(&call->ctx, call->request, &cq_);
    
    if (!call->rpc) {
      // 로그 주석처리
      // if (fire_id < 10) {
      //   if (logger_) {
      //     ROCKS_LOG_ERROR(logger_, "[BloomCheck] FireAsyncCheck #%lu: PrepareAsyncCheck returned null!", fire_id);
      //   }
      // }
      // PrepareAsync 실패 시 inflight에서 제거 후 삭제
      {
        std::lock_guard<std::mutex> lock(inflight_mutex_);
        inflight_requests_.erase(call);
        stats_.inflight_rpc_count.store(inflight_requests_.size(), std::memory_order_relaxed);
      }
      
      // Per-call state 정리
      {
        std::lock_guard<std::mutex> lock2(per_call_state_mutex_);
        per_call_states_.erase(call_id);
      }
      
      stats_.drop_due_to_other.fetch_add(1, std::memory_order_relaxed);
      stats_.rpc_dropped.fetch_add(1, std::memory_order_relaxed);
      delete call;
      return 0;  // 0 = 실패
    }
    
    // 8. StartCall()
    call->rpc->StartCall();
    
    // 9. Finish(tag=call) - call 포인터가 tag가 됨
    call->rpc->Finish(&call->response, &call->status, call);
    
    return call_id;  // call_id 반환
  }
  
  // Get 요청에서 BloomCheck 결과를 Poll (non-blocking)
  // 반환값: true = 결과 도착 (ready && complete), false = 아직 없음 또는 incomplete
  bool TryPollResult(uint64_t call_id, PerCallState** out_state) {
    if (call_id == 0) {
      return false;  // 유효하지 않은 call_id
    }
    
    std::lock_guard<std::mutex> lock(per_call_state_mutex_);
    auto it = per_call_states_.find(call_id);
    if (it == per_call_states_.end()) {
      return false;  // 상태 없음 (이미 정리되었거나 존재하지 않음)
    }
    
    PerCallState& state = it->second;
    if (state.ready && state.complete) {
      *out_state = &state;
      return true;  // 완전한 결과 도착
    }
    
    return false;  // 아직 없음 또는 incomplete
  }
  
  // Get 요청 완료 시 per-call state 정리 (call_id로)
  void ReleaseCallState(uint64_t call_id) {
    if (call_id == 0) {
      return;
    }
    
    std::lock_guard<std::mutex> lock(per_call_state_mutex_);
    per_call_states_.erase(call_id);
  }
  
  // 통계 조회
  struct Stats {
    uint64_t inflight_rpc_count;
    uint64_t rpc_started;
    uint64_t rpc_completed;
    uint64_t rpc_ok;
    uint64_t rpc_failed;
    uint64_t rpc_cancelled;
    uint64_t rpc_dropped;
    uint64_t rpc_timed_out;
    uint64_t sst_files_skipped;
    // Drop reason breakdown
    uint64_t drop_due_to_inflight_full;
    uint64_t drop_due_to_cooldown;
    uint64_t drop_due_to_min_candidates;
    uint64_t drop_due_to_other;
    // Skip counters (not counted as drops)
    uint64_t skipped_due_to_overload_cooldown;
    uint64_t skipped_due_to_tokens;
  };
  
  Stats GetStats() const {
    Stats s;
    s.inflight_rpc_count = stats_.inflight_rpc_count.load(std::memory_order_relaxed);
    s.rpc_started = stats_.rpc_started.load(std::memory_order_relaxed);
    s.rpc_completed = stats_.rpc_completed.load(std::memory_order_relaxed);
    s.rpc_ok = stats_.rpc_ok.load(std::memory_order_relaxed);
    s.rpc_failed = stats_.rpc_failed.load(std::memory_order_relaxed);
    s.rpc_cancelled = stats_.rpc_cancelled.load(std::memory_order_relaxed);
    s.rpc_dropped = stats_.rpc_dropped.load(std::memory_order_relaxed);
    s.rpc_timed_out = stats_.rpc_timed_out.load(std::memory_order_relaxed);
    s.sst_files_skipped = stats_.sst_files_skipped.load(std::memory_order_relaxed);
    s.drop_due_to_inflight_full = stats_.drop_due_to_inflight_full.load(std::memory_order_relaxed);
    s.drop_due_to_cooldown = stats_.drop_due_to_cooldown.load(std::memory_order_relaxed);
    s.drop_due_to_min_candidates = stats_.drop_due_to_min_candidates.load(std::memory_order_relaxed);
    s.drop_due_to_other = stats_.drop_due_to_other.load(std::memory_order_relaxed);
    s.skipped_due_to_overload_cooldown = stats_.skipped_due_to_overload_cooldown.load(std::memory_order_relaxed);
    s.skipped_due_to_tokens = stats_.skipped_due_to_tokens.load(std::memory_order_relaxed);
    return s;
  }
  
  // version_set.cc에서 rate limiting drop을 기록하기 위한 메서드
  void RecordDropDueToCooldown() {
    stats_.drop_due_to_cooldown.fetch_add(1, std::memory_order_relaxed);
    stats_.rpc_dropped.fetch_add(1, std::memory_order_relaxed);
  }
  
  void RecordDropDueToMinCandidates() {
    stats_.drop_due_to_min_candidates.fetch_add(1, std::memory_order_relaxed);
    stats_.rpc_dropped.fetch_add(1, std::memory_order_relaxed);
  }
  
 private:
  void ProcessCompletionQueue() {
    // 로그 주석처리로 thread_id 미사용
    // static std::atomic<uint64_t> thread_id_counter{0};
    // uint64_t thread_id = thread_id_counter.fetch_add(1);
    
    // 로그 주석처리
    // if (logger_) {
    //   ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread #%lu started", thread_id);
    // }
    
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
        // 로그 주석처리
        // if (logger_) {
        //   ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread #%lu: CQ shutdown", thread_id);
        // }
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
          // 로그 주석처리
          // if (logger_) {
          //   ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread #%lu: CQ shutdown during drain", thread_id);
          // }
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
    
    // 로그 주석처리
    // if (logger_) {
    //   ROCKS_LOG_INFO(logger_, "[BloomCheck] ProcessCompletionQueue thread #%lu exiting, total_responses=%lu", 
    //                 thread_id, total_responses);
    // }
  }
  
  // 이벤트 처리 헬퍼 함수 (락 보호 내부 로직)
  void ProcessEvent(void* tag, bool ok, uint64_t* total_responses) {
    (*total_responses)++;
    stats_.rpc_completed.fetch_add(1, std::memory_order_relaxed);
    
    // tag가 nullptr인 경우 무시
    if (tag == nullptr) {
        // 로그 주석처리
        // if (*total_responses <= 100 || (*total_responses) % 1000 == 0) {
        //   if (logger_) {
        //     ROCKS_LOG_WARN(logger_, "[BloomCheck] ProcessCompletionQueue received null tag! ok=%d, response #%lu", 
        //                   ok ? 1 : 0, *total_responses);
        //   }
        // }
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
          // 로그 주석처리
          // if (*total_responses <= 100 || (*total_responses) % 1000 == 0) {
          //   if (logger_) {
          //     ROCKS_LOG_WARN(logger_, "[BloomCheck] Dropped response (not in inflight), tag=%p", call);
          //   }
          // }
          return;  // NO delete - 더블 free 방지
        }
      }
      
      // call_owner가 유효한지 확인
      if (!call_owner || call_owner.get() != call) {
        // 메모리 안전성 체크 실패 - 로그만 남기고 무시
        // CRITICAL: delete 하지 않음! unique_ptr이 이미 삭제했을 수 있음
        // 로그 주석처리
        // if (*total_responses <= 100 || (*total_responses) % 1000 == 0) {
        //   if (logger_) {
        //     ROCKS_LOG_WARN(logger_, "[BloomCheck] Invalid call_owner: call=%p, owner=%p", 
        //                   call, call_owner ? call_owner.get() : nullptr);
        //   }
        // }
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
        
        // 응답에서 완전성 정보 추출
        bool complete = key_resp.complete();  // 서버가 모든 파일을 검사했는지
        // checked_count와 total_candidates는 나중에 로그/통계에 사용 가능
        // uint64_t checked_count = key_resp.checked_count();
        // uint64_t total_candidates = key_resp.total_candidates();
        
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
        
        // Per-call state 업데이트 (이번 Get 요청에 즉시 적용 가능)
        {
          std::lock_guard<std::mutex> lock(per_call_state_mutex_);
          auto it = per_call_states_.find(call->call_id);
          if (it != per_call_states_.end()) {
            it->second.SetResult(candidate_files, may_match_files, complete);
          }
          // call_id가 없으면 무시 (이미 정리되었거나 잘못된 call_id)
        }
        
        
        stats_.rpc_ok.fetch_add(1, std::memory_order_relaxed);
        
        // move 이후에는 hint를 절대 참조하지 않음
      } else {
        // 응답이 비어있음
        stats_.rpc_failed.fetch_add(1, std::memory_order_relaxed);
      }
      
      // call_owner가 소멸되면서 자동으로 delete됨
    }
  
  // Token refill loop (10-50ms 주기로 토큰 보충)
  void TokenRefillLoop() {
    const uint64_t refill_interval_ms = 20;  // 20ms마다 refill
    
    while (!shutdown_.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(refill_interval_ms));
      
      if (shutdown_.load(std::memory_order_relaxed)) {
        break;
      }
      
      auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count();
      last_refill_time_.load(std::memory_order_acquire);  // unused but kept for consistency
      uint64_t last_completed = last_completed_count_.load(std::memory_order_acquire);
      
      uint64_t current_completed = stats_.rpc_completed.load(std::memory_order_relaxed);
      uint64_t completed_delta = (current_completed > last_completed) ? 
                                 (current_completed - last_completed) : 0;
      
      // completed_delta * 1.1로 토큰 보충 (10% 여유)
      uint64_t tokens_to_add = static_cast<uint64_t>(completed_delta * 1.1);
      
      if (tokens_to_add > 0) {
        uint64_t current_tokens = tokens_.load(std::memory_order_acquire);
        uint64_t new_tokens = std::min(current_tokens + tokens_to_add, 
                                       static_cast<uint64_t>(max_inflight_));
        
        // CAS로 토큰 업데이트
        uint64_t expected = current_tokens;
        while (!tokens_.compare_exchange_weak(expected, new_tokens,
                                             std::memory_order_acq_rel,
                                             std::memory_order_acquire)) {
          new_tokens = std::min(expected + tokens_to_add, 
                               static_cast<uint64_t>(max_inflight_));
        }
      }
      
      last_refill_time_.store(now_ns, std::memory_order_release);
      last_completed_count_.store(current_completed, std::memory_order_release);
    }
  }
  
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<bloomcheck::BloomCheck::Stub> stub_;
  grpc::CompletionQueue cq_;
  std::vector<std::thread> cq_threads_;  // 여러 CQ polling threads
  std::thread token_refill_thread_;  // Token refill thread
  std::atomic<bool> shutdown_;
  
  size_t max_inflight_;
  std::chrono::milliseconds hint_ttl_;
  Logger* logger_;
  
  // Overload cooldown (declaration order must match initialization order)
  uint64_t overload_cooldown_us_;  // cooldown duration in microseconds
  std::atomic<uint64_t> next_fire_allowed_ts_;  // nanoseconds since epoch
  
  // Token bucket
  std::atomic<uint64_t> tokens_;  // current tokens
  std::atomic<uint64_t> last_refill_time_;  // nanoseconds since epoch
  std::atomic<uint64_t> last_completed_count_;  // last completed count at refill time
  
  // Inflight 요청 관리 (AsyncCall* -> unique_ptr<AsyncCall>)
  std::mutex inflight_mutex_;
  std::unordered_map<AsyncCall*, std::unique_ptr<AsyncCall>> inflight_requests_;
  
  
  // Per-call state 관리 (call_id -> PerCallState)
  std::mutex per_call_state_mutex_;
  std::unordered_map<uint64_t, PerCallState> per_call_states_;
  
  struct StatsInternal {
    std::atomic<uint64_t> inflight_rpc_count{0};
    std::atomic<uint64_t> rpc_started{0};
    std::atomic<uint64_t> rpc_completed{0};
    std::atomic<uint64_t> rpc_ok{0};
    std::atomic<uint64_t> rpc_failed{0};
    std::atomic<uint64_t> rpc_cancelled{0};
    std::atomic<uint64_t> rpc_dropped{0};
    std::atomic<uint64_t> rpc_timed_out{0};
    std::atomic<uint64_t> sst_files_skipped{0};
    // Drop reason breakdown
    std::atomic<uint64_t> drop_due_to_inflight_full{0};
    std::atomic<uint64_t> drop_due_to_cooldown{0};
    std::atomic<uint64_t> drop_due_to_min_candidates{0};
    std::atomic<uint64_t> drop_due_to_other{0};
    // Skip counters (not counted as drops)
    std::atomic<uint64_t> skipped_due_to_overload_cooldown{0};
    std::atomic<uint64_t> skipped_due_to_tokens{0};
  };
  
  mutable StatsInternal stats_;
};

}  // namespace ROCKSDB_NAMESPACE
