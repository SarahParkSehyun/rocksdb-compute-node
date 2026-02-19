// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "util/bloomcheck_erpc_client.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <time.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <unistd.h>

// eRPC includes
#if __has_include("erpc/transport_impl/infiniband/ib_transport.h")
#include "erpc/transport_impl/infiniband/ib_transport.h"
#include "erpc/nexus.h"
#include "erpc/rpc.h"
#elif __has_include("transport_impl/infiniband/ib_transport.h")
#include "transport_impl/infiniband/ib_transport.h"
#include "nexus.h"
#include "rpc.h"
#else
#error "Cannot find eRPC headers. Check ERPC_ROOT include path."
#endif

namespace ROCKSDB_NAMESPACE {
namespace {

uint64_t NowNs() {
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    return 0;
  }
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL +
         static_cast<uint64_t>(ts.tv_nsec);
}

int ParseEnvMs(const char* name, int default_value) {
  const char* val = std::getenv(name);
  if (val == nullptr || val[0] == '\0') {
    return default_value;
  }
  char* end = nullptr;
  long parsed = std::strtol(val, &end, 10);
  if (end == val || parsed <= 0 || parsed > 600000) {
    return default_value;
  }
  return static_cast<int>(parsed);
}

int ParseEnvInt(const char* name, int default_value, int min_v, int max_v) {
  const char* val = std::getenv(name);
  if (val == nullptr || val[0] == '\0') {
    return default_value;
  }
  char* end = nullptr;
  long parsed = std::strtol(val, &end, 10);
  if (end == val || parsed < min_v || parsed > max_v) {
    return default_value;
  }
  return static_cast<int>(parsed);
}

bool ReadFileToString(const std::string& path, std::string* out) {
  if (out == nullptr) {
    return false;
  }
  FILE* fp = std::fopen(path.c_str(), "r");
  if (fp == nullptr) {
    return false;
  }
  char buf[256];
  if (std::fgets(buf, sizeof(buf), fp) == nullptr) {
    std::fclose(fp);
    return false;
  }
  std::fclose(fp);
  *out = buf;
  while (!out->empty() && ((*out)[out->size() - 1] == '\n' ||
                           (*out)[out->size() - 1] == '\r' ||
                           (*out)[out->size() - 1] == ' ' ||
                           (*out)[out->size() - 1] == '\t')) {
    out->pop_back();
  }
  return !out->empty();
}

std::vector<int> ParseCpuList(const std::string& cpu_list) {
  std::vector<int> cpus;
  size_t start = 0;
  while (start < cpu_list.size()) {
    size_t comma = cpu_list.find(',', start);
    std::string token = cpu_list.substr(
        start, comma == std::string::npos ? std::string::npos : comma - start);
    if (!token.empty()) {
      size_t dash = token.find('-');
      if (dash == std::string::npos) {
        int cpu = std::atoi(token.c_str());
        if (cpu >= 0) {
          cpus.push_back(cpu);
        }
      } else {
        int lo = std::atoi(token.substr(0, dash).c_str());
        int hi = std::atoi(token.substr(dash + 1).c_str());
        if (lo >= 0 && hi >= lo) {
          for (int c = lo; c <= hi; ++c) {
            cpus.push_back(c);
          }
        }
      }
    }
    if (comma == std::string::npos) {
      break;
    }
    start = comma + 1;
  }
  return cpus;
}

std::vector<int> GetNumaNodeCpus(int numa_node) {
  std::string cpulist;
  const std::string path =
      "/sys/devices/system/node/node" + std::to_string(numa_node) + "/cpulist";
  if (!ReadFileToString(path, &cpulist)) {
    return {};
  }
  return ParseCpuList(cpulist);
}

bool PinCurrentThreadToCpu(int cpu) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  return pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset) == 0;
}

std::vector<int> GetCurrentThreadAllowedCpus() {
  std::vector<int> cpus;
  cpu_set_t mask;
  CPU_ZERO(&mask);
  if (sched_getaffinity(0, sizeof(mask), &mask) != 0) {
    return cpus;
  }
  for (int c = 0; c < CPU_SETSIZE; ++c) {
    if (CPU_ISSET(c, &mask)) {
      cpus.push_back(c);
    }
  }
  return cpus;
}

std::vector<int> BuildDefaultOddWorkerCpuOrder() {
  // Default preference keeps workers on odd cores and starts at 17 descending:
  // 17, 15, 13, 11, ...
  static const int kPreferred[] = {17, 15, 13, 11, 9, 7, 5, 3, 1,
                                   31, 29, 27, 25, 23, 21, 19};
  return std::vector<int>(kPreferred, kPreferred + sizeof(kPreferred) / sizeof(int));
}

void SetCurrentThreadName(size_t index) {
  char name[16];
  std::snprintf(name, sizeof(name), "erpc_w%02zu", index);
  pthread_setname_np(pthread_self(), name);
}

void LowerCurrentThreadPriority() {
#ifdef SCHED_BATCH
  struct sched_param sp;
  std::memset(&sp, 0, sizeof(sp));
  if (pthread_setschedparam(pthread_self(), SCHED_BATCH, &sp) == 0) {
    return;
  }
#endif
  // Fallback: lower worker priority with nice(+10).
  const pid_t tid = static_cast<pid_t>(syscall(SYS_gettid));
  (void)setpriority(PRIO_PROCESS, static_cast<id_t>(tid), 10);
}

class Semaphore {
 public:
  explicit Semaphore(int permits) : permits_(permits), available_(permits) {}

  bool AcquireFor(std::chrono::milliseconds timeout) {
    if (permits_ <= 0) {
      return true;
    }
    std::unique_lock<std::mutex> lock(mu_);
    bool ok = cv_.wait_for(lock, timeout, [&] { return available_ > 0; });
    if (!ok) {
      return false;
    }
    --available_;
    return true;
  }

  void Release() {
    if (permits_ <= 0) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (available_ < permits_) {
        ++available_;
      }
    }
    cv_.notify_one();
  }

 private:
  int permits_ = 0;
  int available_ = 0;
  std::mutex mu_;
  std::condition_variable cv_;
};

class SemaphoreGuard {
 public:
  explicit SemaphoreGuard(Semaphore* sem) : sem_(sem) {}
  ~SemaphoreGuard() {
    if (held_ && sem_ != nullptr) {
      sem_->Release();
    }
  }
  bool AcquireFor(std::chrono::milliseconds timeout) {
    if (sem_ == nullptr) {
      held_ = true;
      return true;
    }
    held_ = sem_->AcquireFor(timeout);
    return held_;
  }

 private:
  Semaphore* sem_ = nullptr;
  bool held_ = false;
};

struct SmConnectState {
  std::atomic<int> session_num{-1};
  std::atomic<int> state{0};  // 0: pending, 1: connected, 2: failed
  std::atomic<int> err_type{0};
};

void sm_handler(int session_num, erpc::SmEventType event, erpc::SmErrType err_type,
                void* context) {
  auto* st = static_cast<SmConnectState*>(context);
  if (st == nullptr) {
    return;
  }
  if (st->session_num.load(std::memory_order_acquire) != session_num) {
    return;
  }
  if (event == erpc::SmEventType::kConnected) {
    st->state.store(1, std::memory_order_release);
    st->err_type.store(static_cast<int>(erpc::SmErrType::kNoError),
                       std::memory_order_release);
  } else if (event == erpc::SmEventType::kConnectFailed) {
    st->state.store(2, std::memory_order_release);
    st->err_type.store(static_cast<int>(err_type), std::memory_order_release);
  }
}

struct ProcessContext;
class Worker;
struct ThreadContext;

enum class TaskKind : uint8_t {
  kNoop = 1,
  kCheck = 2,
};

struct InflightCall {
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
  uint64_t send_ns = 0;
  uint64_t rtt_ns = 0;
  uint64_t server_ts_ns = 0;
  bool parse_ok = false;
  bloomcheck::BatchResponse check_response;
  std::atomic<bool> done{false};
};

struct RpcCallResult {
  bool ok = false;
  uint64_t server_ts_ns = 0;
  uint64_t rtt_ns = 0;
  bloomcheck::BatchResponse check_response;
};

struct CallTask {
  TaskKind kind = TaskKind::kNoop;
  std::string req_payload;
  size_t resp_capacity = 256;
  uint64_t enqueue_ns = 0;
  std::promise<RpcCallResult> promise;
};

struct ProcessContext {
  uint64_t client_id = 0;
  std::unique_ptr<erpc::Nexus> nexus;
  std::string local_uri;
  std::string server_uri;
  int numa_node = 0;
  uint8_t server_rpc_id = 0;
  uint8_t base_rpc_id = 0;
  std::atomic<uint16_t> next_rpc_id{0};
  std::atomic<uint64_t> next_worker_rr{0};
  int connect_timeout_ms = 2000;
  int rpc_timeout_ms = 100;
  int pool_size = 0;
  bool pool_pin_to_cores = false;
  std::vector<int> worker_cpus;
  int max_inflight = 0;
  bool debug_queue_delay = false;
  int worker_max_outstanding = 32;
  std::unique_ptr<Semaphore> inflight_sem;
  std::vector<std::unique_ptr<Worker>> workers;
  std::atomic<bool> pool_ready{false};
  bool initialized = false;
};

struct ThreadContext {
  explicit ThreadContext(std::shared_ptr<ProcessContext> process_ctx)
      : pc(std::move(process_ctx)) {}

  std::shared_ptr<ProcessContext> pc;
  uint8_t rpc_id = 0;
  std::unique_ptr<erpc::Rpc<erpc::CTransport>> rpc;
  int session_num = -1;
  bool connected = false;
  SmConnectState sm_state;
  std::vector<InflightCall*> timed_out_calls;

  ~ThreadContext() { ReapTimedOutCalls(); }

  void ReapTimedOutCalls() {
    for (size_t i = 0; i < timed_out_calls.size();) {
      InflightCall* call = timed_out_calls[i];
      if (call->done.load(std::memory_order_acquire)) {
        rpc->free_msg_buffer(call->req_msgbuf);
        rpc->free_msg_buffer(call->resp_msgbuf);
        delete call;
        timed_out_calls[i] = timed_out_calls.back();
        timed_out_calls.pop_back();
      } else {
        ++i;
      }
    }
  }

  bool InitThread() {
    if (connected) {
      return true;
    }

    const uint16_t raw_id = pc->next_rpc_id.fetch_add(1, std::memory_order_relaxed);
    const uint16_t rpc_id_range = static_cast<uint16_t>(erpc::kMaxRpcId) + 1;
    const uint16_t assigned =
        static_cast<uint16_t>((static_cast<uint16_t>(pc->base_rpc_id) + raw_id) %
                              rpc_id_range);
    rpc_id = static_cast<uint8_t>(assigned);

    sm_state.session_num.store(-1, std::memory_order_release);
    sm_state.state.store(0, std::memory_order_release);
    sm_state.err_type.store(0, std::memory_order_release);
    rpc = std::make_unique<erpc::Rpc<erpc::CTransport>>(pc->nexus.get(), &sm_state,
                                                         rpc_id, sm_handler);
    if (!rpc) {
      fprintf(stderr, "[ErpcBloomCheckClient] Failed to create per-thread Rpc\n");
      return false;
    }

    session_num = rpc->create_session(pc->server_uri, pc->server_rpc_id);
    if (session_num < 0) {
      fprintf(stderr, "[ErpcBloomCheckClient] Failed to create session (rpc_id=%u)\n",
              static_cast<unsigned>(rpc_id));
      return false;
    }

    sm_state.session_num.store(session_num, std::memory_order_release);
    sm_state.state.store(0, std::memory_order_release);
    sm_state.err_type.store(0, std::memory_order_release);
    const uint64_t deadline_ns =
        NowNs() + static_cast<uint64_t>(pc->connect_timeout_ms) * 1000000ULL;
    while (true) {
      rpc->run_event_loop_once();
      const int state = sm_state.state.load(std::memory_order_acquire);
      if (state == 1) {
        break;
      }
      if (state == 2) {
        fprintf(stderr,
                "[ErpcBloomCheckClient] Session connect failed (%d ms, rpc_id=%u, err=%d)\n",
                pc->connect_timeout_ms, static_cast<unsigned>(rpc_id),
                sm_state.err_type.load(std::memory_order_acquire));
        return false;
      }
      if (NowNs() > deadline_ns) {
        fprintf(stderr,
                "[ErpcBloomCheckClient] Session connect timeout (%d ms, rpc_id=%u)\n",
                pc->connect_timeout_ms, static_cast<unsigned>(rpc_id));
        return false;
      }
    }

    connected = true;
    return true;
  }
};

thread_local std::unordered_map<uint64_t, std::unique_ptr<ThreadContext>>
    tls_thread_ctx_map;
std::atomic<uint64_t> g_client_id_seq{1};

void cont_func_common(void* context, void* tag) {
  (void)context;
  auto* call = static_cast<InflightCall*>(tag);

  const uint64_t recv_ns = NowNs();
  call->rtt_ns = (recv_ns > call->send_ns) ? (recv_ns - call->send_ns) : 0;
  call->done.store(true, std::memory_order_release);
}

class Worker {
 public:
  Worker(std::shared_ptr<ProcessContext> pc, size_t index, uint8_t rpc_id)
      : pc_(pc), index_(index), rpc_id_(rpc_id) {}

  ~Worker() { Stop(); }

  void Start() {
    if (started_) {
      return;
    }
    started_ = true;
    th_ = std::thread(&Worker::ThreadMain, this);
  }

  void Stop() {
    if (!started_) {
      return;
    }
    {
      std::lock_guard<std::mutex> lock(mu_);
      shutdown_ = true;
    }
    cv_.notify_all();
    if (th_.joinable()) {
      th_.join();
    }
    started_ = false;
  }

  bool EnqueueTask(std::unique_ptr<CallTask> task, std::future<RpcCallResult>* out_future) {
    if (out_future == nullptr || !started_ || task == nullptr) {
      return false;
    }
    *out_future = task->promise.get_future();
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (shutdown_) {
        return false;
      }
      queue_.push(std::move(task));
    }
    cv_.notify_one();
    return true;
  }

  bool IsConnected() const { return connected_.load(std::memory_order_acquire); }

 private:
  struct PendingRpc {
    std::unique_ptr<CallTask> task;
    InflightCall* call = nullptr;
    uint64_t deadline_ns = 0;
    uint64_t queue_delay_us = 0;
    uint8_t req_type = 0;
  };

  void ReapTimedOutCalls() {
    if (!rpc_) {
      return;
    }
    for (size_t i = 0; i < timed_out_calls_.size();) {
      InflightCall* call = timed_out_calls_[i];
      if (call->done.load(std::memory_order_acquire)) {
        rpc_->free_msg_buffer(call->req_msgbuf);
        rpc_->free_msg_buffer(call->resp_msgbuf);
        delete call;
        timed_out_calls_[i] = timed_out_calls_.back();
        timed_out_calls_.pop_back();
      } else {
        ++i;
      }
    }
  }

  void ConfigureWorkerThread(const ProcessContext& pc) {
    SetCurrentThreadName(index_);
    LowerCurrentThreadPriority();

    if (!pc.pool_pin_to_cores) {
      return;
    }

    std::vector<int> cpus;
    if (!pc.worker_cpus.empty()) {
      cpus = pc.worker_cpus;
    } else {
      // Build default odd-core policy and keep only currently allowed CPUs.
      const std::vector<int> preferred = BuildDefaultOddWorkerCpuOrder();
      const std::vector<int> allowed = GetCurrentThreadAllowedCpus();
      for (int cpu : preferred) {
        if (std::find(allowed.begin(), allowed.end(), cpu) != allowed.end()) {
          cpus.push_back(cpu);
        }
      }
      if (cpus.empty()) {
        // Fallback to odd CPUs on target NUMA node in descending order.
        std::vector<int> node_cpus = GetNumaNodeCpus(pc.numa_node);
        for (auto it = node_cpus.rbegin(); it != node_cpus.rend(); ++it) {
          if ((*it & 1) == 1) {
            cpus.push_back(*it);
          }
        }
      }
    }

    if (cpus.empty()) {
      fprintf(stderr,
              "[ErpcBloomCheckClient] Worker %zu pin skipped: no eligible CPU set\n",
              index_);
      return;
    }
    int cpu = cpus[index_ % cpus.size()];
    if (PinCurrentThreadToCpu(cpu)) {
      fprintf(stderr,
              "[ErpcBloomCheckClient] Worker %zu pinned to CPU %d (odd-core policy)\n",
              index_, cpu);
    }
  }

  bool InitRpcAndSession(const ProcessContext& pc) {
    sm_state_.session_num.store(-1, std::memory_order_release);
    sm_state_.state.store(0, std::memory_order_release);
    sm_state_.err_type.store(0, std::memory_order_release);
    rpc_ = std::make_unique<erpc::Rpc<erpc::CTransport>>(pc.nexus.get(), &sm_state_,
                                                          rpc_id_, sm_handler);
    if (!rpc_) {
      fprintf(stderr, "[ErpcBloomCheckClient] Worker %zu failed to create Rpc\n", index_);
      return false;
    }

    session_num_ = rpc_->create_session(pc.server_uri, pc.server_rpc_id);
    if (session_num_ < 0) {
      fprintf(stderr,
              "[ErpcBloomCheckClient] Worker %zu failed to create session (rpc_id=%u)\n",
              index_, static_cast<unsigned>(rpc_id_));
      return false;
    }

    sm_state_.session_num.store(session_num_, std::memory_order_release);
    sm_state_.state.store(0, std::memory_order_release);
    sm_state_.err_type.store(0, std::memory_order_release);

    const uint64_t deadline_ns =
        NowNs() + static_cast<uint64_t>(pc.connect_timeout_ms) * 1000000ULL;
    while (true) {
      rpc_->run_event_loop_once();
      const int state = sm_state_.state.load(std::memory_order_acquire);
      if (state == 1) {
        break;
      }
      if (state == 2) {
        fprintf(stderr,
                "[ErpcBloomCheckClient] Worker %zu connect failed (%d ms, rpc_id=%u, err=%d)\n",
                index_, pc.connect_timeout_ms, static_cast<unsigned>(rpc_id_),
                sm_state_.err_type.load(std::memory_order_acquire));
        return false;
      }
      if (NowNs() > deadline_ns) {
        fprintf(stderr,
                "[ErpcBloomCheckClient] Worker %zu connect timeout (%d ms, rpc_id=%u)\n",
                index_, pc.connect_timeout_ms, static_cast<unsigned>(rpc_id_));
        return false;
      }
    }

    connected_.store(true, std::memory_order_release);
    return true;
  }

  bool EnqueuePendingTask(const ProcessContext& pc, std::unique_ptr<CallTask> task) {
    if (!task) {
      return false;
    }
    const uint64_t worker_start_ns = NowNs();
    const uint64_t queue_delay_us =
        worker_start_ns > task->enqueue_ns
            ? (worker_start_ns - task->enqueue_ns) / 1000ULL
            : 0ULL;
    if (pc.debug_queue_delay && queue_delay_us > 0) {
      fprintf(stderr,
              "[ErpcBloomCheckClient][DBG] worker=%zu type=%u queue_delay_us=%llu\n",
              index_, static_cast<unsigned>(task->kind),
              static_cast<unsigned long long>(queue_delay_us));
    }

    auto* call = new InflightCall();
    call->req_msgbuf = rpc_->alloc_msg_buffer_or_die(task->req_payload.empty()
                                                          ? 1
                                                          : task->req_payload.size());
    call->resp_msgbuf = rpc_->alloc_msg_buffer_or_die(task->resp_capacity);

    if (!task->req_payload.empty()) {
      rpc_->resize_msg_buffer(&call->req_msgbuf, task->req_payload.size());
      memcpy(call->req_msgbuf.buf_, task->req_payload.data(), task->req_payload.size());
    } else {
      rpc_->resize_msg_buffer(&call->req_msgbuf, 0);
    }

    call->send_ns = NowNs();
    const uint64_t deadline_ns =
        call->send_ns + static_cast<uint64_t>(pc.rpc_timeout_ms) * 1000000ULL;
    const uint8_t req_type =
        task->kind == TaskKind::kNoop ? ErpcBloomCheckClient::kReqTypeNoop
                                      : ErpcBloomCheckClient::kReqTypeCheck;
    rpc_->enqueue_request(session_num_, req_type, &call->req_msgbuf, &call->resp_msgbuf,
                          cont_func_common, call);

    PendingRpc pending;
    pending.task = std::move(task);
    pending.call = call;
    pending.deadline_ns = deadline_ns;
    pending.queue_delay_us = queue_delay_us;
    pending.req_type = req_type;
    pending_calls_.emplace_back(std::move(pending));
    return true;
  }

  RpcCallResult BuildResultFromCall(TaskKind kind, InflightCall* call) {
    RpcCallResult result;
    if (call == nullptr) {
      return result;
    }
    result.rtt_ns = call->rtt_ns;

    if (kind == TaskKind::kNoop) {
      if (call->resp_msgbuf.get_data_size() > 0) {
        bloomcheck::NoopResponse resp;
        call->parse_ok = resp.ParseFromArray(
            call->resp_msgbuf.buf_, static_cast<int>(call->resp_msgbuf.get_data_size()));
        if (call->parse_ok && resp.has_server_ts_ns()) {
          result.server_ts_ns = resp.server_ts_ns();
        }
      }
      result.ok = call->parse_ok;
    } else {
      if (call->resp_msgbuf.get_data_size() > 0) {
        bloomcheck::BatchResponse resp;
        call->parse_ok = resp.ParseFromArray(
            call->resp_msgbuf.buf_, static_cast<int>(call->resp_msgbuf.get_data_size()));
        if (call->parse_ok) {
          result.check_response = std::move(resp);
        }
      }
      result.ok = call->parse_ok;
    }
    return result;
  }

  void FinalizePendingAt(size_t idx, bool timed_out, const ProcessContext& pc) {
    PendingRpc pending = std::move(pending_calls_[idx]);
    pending_calls_[idx] = std::move(pending_calls_.back());
    pending_calls_.pop_back();

    if (!pending.task) {
      return;
    }

    RpcCallResult result;
    if (timed_out) {
      timed_out_calls_.push_back(pending.call);
      fprintf(stderr,
              "[ErpcBloomCheckClient] Worker %zu rpc timeout (%d ms, rpc_id=%u, type=%u, queue_delay_us=%llu)\n",
              index_, pc.rpc_timeout_ms, static_cast<unsigned>(rpc_id_),
              static_cast<unsigned>(pending.req_type),
              static_cast<unsigned long long>(pending.queue_delay_us));
      pending.task->promise.set_value(std::move(result));
      return;
    }

    result = BuildResultFromCall(pending.task->kind, pending.call);
    rpc_->free_msg_buffer(pending.call->req_msgbuf);
    rpc_->free_msg_buffer(pending.call->resp_msgbuf);
    delete pending.call;
    pending.task->promise.set_value(std::move(result));
  }

  void PollAndHarvest(const ProcessContext& pc) {
    if (!rpc_) {
      return;
    }
    rpc_->run_event_loop_once();
    const uint64_t now_ns = NowNs();
    for (size_t i = 0; i < pending_calls_.size();) {
      const bool done = pending_calls_[i].call->done.load(std::memory_order_acquire);
      const bool timed_out = !done && (shutdown_ || now_ns > pending_calls_[i].deadline_ns);
      if (!done && !timed_out) {
        ++i;
        continue;
      }
      FinalizePendingAt(i, timed_out, pc);
    }
    ReapTimedOutCalls();
  }

  bool PopOneTask(std::unique_ptr<CallTask>* out_task) {
    if (out_task == nullptr) {
      return false;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (queue_.empty()) {
      return false;
    }
    *out_task = std::move(queue_.front());
    queue_.pop();
    return true;
  }

  void ThreadMain() {
    auto pc = pc_.lock();
    if (!pc) {
      return;
    }

    ConfigureWorkerThread(*pc);

    bool init_ok = InitRpcAndSession(*pc);
    if (!init_ok) {
      disabled_ = true;
      connected_.store(false, std::memory_order_release);
    }

    const size_t max_outstanding =
        static_cast<size_t>(pc->worker_max_outstanding > 0
                                ? pc->worker_max_outstanding
                                : 1);

    while (true) {
      while (!disabled_ && rpc_ && pending_calls_.size() < max_outstanding) {
        std::unique_ptr<CallTask> task;
        if (!PopOneTask(&task)) {
          break;
        }
        if (!EnqueuePendingTask(*pc, std::move(task))) {
          break;
        }
      }

      if (disabled_ || !rpc_) {
        std::unique_ptr<CallTask> task;
        if (PopOneTask(&task) && task) {
          task->promise.set_value(RpcCallResult{});
          continue;
        }
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait_for(lock, std::chrono::milliseconds(1),
                     [&] { return shutdown_ || !queue_.empty(); });
        if (shutdown_ && queue_.empty()) {
          break;
        }
        continue;
      }

      if (!pending_calls_.empty()) {
        PollAndHarvest(*pc);
        continue;
      }

      std::unique_lock<std::mutex> lock(mu_);
      cv_.wait_for(lock, std::chrono::milliseconds(1),
                   [&] { return shutdown_ || !queue_.empty(); });
      if (shutdown_ && queue_.empty() && pending_calls_.empty()) {
        break;
      }
    }

    if (rpc_) {
      for (int i = 0; i < 8; ++i) {
        rpc_->run_event_loop_once();
      }
      while (!pending_calls_.empty()) {
        FinalizePendingAt(0, true, *pc);
      }
      ReapTimedOutCalls();
    }

    while (!timed_out_calls_.empty()) {
      InflightCall* call = timed_out_calls_.back();
      timed_out_calls_.pop_back();
      if (rpc_) {
        rpc_->free_msg_buffer(call->req_msgbuf);
        rpc_->free_msg_buffer(call->resp_msgbuf);
      }
      delete call;
    }
  }

  std::weak_ptr<ProcessContext> pc_;
  size_t index_ = 0;
  uint8_t rpc_id_ = 0;
  SmConnectState sm_state_;

  std::thread th_;
  std::unique_ptr<erpc::Rpc<erpc::CTransport>> rpc_;
  int session_num_ = -1;

  std::mutex mu_;
  std::condition_variable cv_;
  std::queue<std::unique_ptr<CallTask>> queue_;
  bool shutdown_ = false;
  bool started_ = false;
  bool disabled_ = false;
  std::atomic<bool> connected_{false};
  std::vector<InflightCall*> timed_out_calls_;
  std::vector<PendingRpc> pending_calls_;
};

}  // namespace

struct ErpcBloomCheckClientImpl {
  explicit ErpcBloomCheckClientImpl(const std::string& local_uri,
                                    const std::string& server_uri, int numa_node,
                                    uint8_t base_rpc_id, uint8_t server_rpc_id) {
    pc = std::make_shared<ProcessContext>();
    pc->client_id = g_client_id_seq.fetch_add(1, std::memory_order_relaxed);
    pc->local_uri = local_uri;
    pc->server_uri = server_uri;
    pc->numa_node = numa_node;
    pc->base_rpc_id = base_rpc_id;
    pc->server_rpc_id = server_rpc_id;
    pc->connect_timeout_ms = ParseEnvMs("BLOOMCHECK_ERPC_CONNECT_TIMEOUT_MS", 2000);
    pc->rpc_timeout_ms = ParseEnvMs("BLOOMCHECK_ERPC_RPC_TIMEOUT_MS", 100);
    pc->pool_size = ParseEnvInt("BLOOMCHECK_ERPC_POOL_SIZE", 0, 0, 1024);
    pc->pool_pin_to_cores =
        ParseEnvInt("BLOOMCHECK_ERPC_POOL_PIN_TO_CORES", 1, 0, 1) == 1;
    const char* worker_cpus_env = std::getenv("BLOOMCHECK_ERPC_WORKER_CPUS");
    if (worker_cpus_env != nullptr && worker_cpus_env[0] != '\0') {
      pc->worker_cpus = ParseCpuList(worker_cpus_env);
    } else if (pc->pool_size == 16) {
      // Default fixed mapping for 16-worker pool: odd CPUs 1..31
      pc->worker_cpus = {1, 3, 5, 7, 9, 11, 13, 15,
                         17, 19, 21, 23, 25, 27, 29, 31};
    }
    pc->max_inflight =
        ParseEnvInt("BLOOMCHECK_ERPC_MAX_INFLIGHT", 0, 0, 1000000);
    pc->debug_queue_delay =
        ParseEnvInt("BLOOMCHECK_ERPC_DEBUG_QUEUE_DELAY", 0, 0, 1) == 1;
    pc->worker_max_outstanding = ParseEnvInt(
        "BLOOMCHECK_ERPC_WORKER_MAX_OUTSTANDING", 32, 1, 4096);
    if (pc->max_inflight > 0) {
      pc->inflight_sem = std::make_unique<Semaphore>(pc->max_inflight);
    }
  }

  bool StartWorkerPoolLocked() {
    if (pc->pool_size <= 0) {
      pc->pool_ready.store(false, std::memory_order_release);
      return true;
    }
    if (!pc->workers.empty()) {
      pc->pool_ready.store(true, std::memory_order_release);
      return true;
    }

    const uint16_t rpc_id_range = static_cast<uint16_t>(erpc::kMaxRpcId) + 1;
    for (int i = 0; i < pc->pool_size; ++i) {
      uint8_t worker_rpc_id = static_cast<uint8_t>(
          (static_cast<uint16_t>(pc->base_rpc_id) + static_cast<uint16_t>(i)) %
          rpc_id_range);
      auto worker = std::make_unique<Worker>(pc, static_cast<size_t>(i), worker_rpc_id);
      worker->Start();
      pc->workers.emplace_back(std::move(worker));
    }

    pc->pool_ready.store(true, std::memory_order_release);
    fprintf(stderr,
            "[ErpcBloomCheckClient] WorkerPool enabled: size=%d pin_to_cores=%d node=%d\n",
            pc->pool_size, pc->pool_pin_to_cores ? 1 : 0, pc->numa_node);
    return true;
  }

  void StopWorkerPoolLocked() {
    for (auto& worker : pc->workers) {
      if (worker) {
        worker->Stop();
      }
    }
    pc->workers.clear();
    pc->pool_ready.store(false, std::memory_order_release);
  }

  bool Init() {
    std::lock_guard<std::mutex> lock(init_mu);
    if (pc->initialized) {
      return true;
    }

    try {
      pc->nexus = std::make_unique<erpc::Nexus>(pc->local_uri, pc->numa_node, 0);
      pc->initialized = true;
      fprintf(stderr,
              "[ErpcBloomCheckClient] ProcessContext initialized: local=%s server=%s "
              "base_rpc_id=%u server_rpc_id=%u connect_timeout_ms=%d rpc_timeout_ms=%d "
              "pool_size=%d pool_pin_to_cores=%d max_inflight=%d debug_queue_delay=%d "
              "worker_max_outstanding=%d\n",
              pc->local_uri.c_str(), pc->server_uri.c_str(),
              static_cast<unsigned>(pc->base_rpc_id),
              static_cast<unsigned>(pc->server_rpc_id), pc->connect_timeout_ms,
              pc->rpc_timeout_ms, pc->pool_size, pc->pool_pin_to_cores ? 1 : 0,
              pc->max_inflight, pc->debug_queue_delay ? 1 : 0,
              pc->worker_max_outstanding);
      return StartWorkerPoolLocked();
    } catch (const std::exception& e) {
      fprintf(stderr, "[ErpcBloomCheckClient] Nexus init failed: %s\n", e.what());
      return false;
    }
  }

  void Shutdown() {
    std::lock_guard<std::mutex> lock(init_mu);
    if (!pc->initialized) {
      return;
    }
    StopWorkerPoolLocked();
    pc->initialized = false;
    pc->nexus.reset();
  }

  ThreadContext* GetOrCreateThreadContext() {
    auto it = tls_thread_ctx_map.find(pc->client_id);
    if (it != tls_thread_ctx_map.end()) {
      return it->second.get();
    }

    auto tc = std::make_unique<ThreadContext>(pc);
    ThreadContext* tc_ptr = tc.get();
    tls_thread_ctx_map.emplace(pc->client_id, std::move(tc));
    return tc_ptr;
  }

  bool RunThreadLocalTask(TaskKind kind, const std::string& req_payload,
                          size_t resp_capacity, RpcCallResult* out_result) {
    if (out_result == nullptr) {
      return false;
    }
    ThreadContext* tc = GetOrCreateThreadContext();
    if (tc == nullptr || !tc->InitThread()) {
      return false;
    }

    tc->ReapTimedOutCalls();

    auto* call = new InflightCall();
    call->req_msgbuf = tc->rpc->alloc_msg_buffer_or_die(req_payload.empty() ? 1
                                                                              : req_payload.size());
    call->resp_msgbuf = tc->rpc->alloc_msg_buffer_or_die(resp_capacity);

    if (!req_payload.empty()) {
      tc->rpc->resize_msg_buffer(&call->req_msgbuf, req_payload.size());
      memcpy(call->req_msgbuf.buf_, req_payload.data(), req_payload.size());
    } else {
      tc->rpc->resize_msg_buffer(&call->req_msgbuf, 0);
    }

    call->send_ns = NowNs();
    const uint8_t req_type =
        kind == TaskKind::kNoop ? ErpcBloomCheckClient::kReqTypeNoop
                                : ErpcBloomCheckClient::kReqTypeCheck;
    tc->rpc->enqueue_request(tc->session_num, req_type, &call->req_msgbuf,
                             &call->resp_msgbuf, cont_func_common, call);

    const uint64_t deadline_ns =
        call->send_ns + static_cast<uint64_t>(pc->rpc_timeout_ms) * 1000000ULL;
    bool timed_out = false;
    while (!call->done.load(std::memory_order_acquire)) {
      tc->rpc->run_event_loop_once();
      if (NowNs() > deadline_ns) {
        timed_out = true;
        break;
      }
    }

    if (timed_out) {
      tc->timed_out_calls.push_back(call);
      fprintf(stderr,
              "[ErpcBloomCheckClient] rpc timeout (%d ms, rpc_id=%u, type=%u)\n",
              pc->rpc_timeout_ms, static_cast<unsigned>(tc->rpc_id),
              static_cast<unsigned>(req_type));
      return false;
    }

    out_result->rtt_ns = call->rtt_ns;
    if (kind == TaskKind::kNoop) {
      bloomcheck::NoopResponse resp;
      call->parse_ok = resp.ParseFromArray(
          call->resp_msgbuf.buf_, static_cast<int>(call->resp_msgbuf.get_data_size()));
      if (call->parse_ok && resp.has_server_ts_ns()) {
        out_result->server_ts_ns = resp.server_ts_ns();
      }
      out_result->ok = call->parse_ok;
    } else {
      bloomcheck::BatchResponse resp;
      call->parse_ok = resp.ParseFromArray(
          call->resp_msgbuf.buf_, static_cast<int>(call->resp_msgbuf.get_data_size()));
      if (call->parse_ok) {
        out_result->check_response = std::move(resp);
      }
      out_result->ok = call->parse_ok;
    }

    tc->rpc->free_msg_buffer(call->req_msgbuf);
    tc->rpc->free_msg_buffer(call->resp_msgbuf);
    delete call;
    return out_result->ok;
  }

  bool RunPoolTask(TaskKind kind, const std::string& req_payload, size_t resp_capacity,
                   RpcCallResult* out_result, size_t* slot_idx) {
    if (out_result == nullptr) {
      return false;
    }
    if (!pc->pool_ready.load(std::memory_order_acquire) || pc->workers.empty()) {
      return false;
    }

    const size_t worker_idx = static_cast<size_t>(
        pc->next_worker_rr.fetch_add(1, std::memory_order_relaxed) %
        static_cast<uint64_t>(pc->workers.size()));
    if (slot_idx != nullptr) {
      *slot_idx = worker_idx;
    }
    Worker* worker = pc->workers[worker_idx].get();
    if (worker == nullptr) {
      return false;
    }

    auto task = std::make_unique<CallTask>();
    task->kind = kind;
    task->req_payload = req_payload;
    task->resp_capacity = resp_capacity;
    task->enqueue_ns = NowNs();

    std::future<RpcCallResult> fut;
    if (!worker->EnqueueTask(std::move(task), &fut)) {
      return false;
    }

    auto status = fut.wait_for(std::chrono::milliseconds(pc->rpc_timeout_ms));
    if (status != std::future_status::ready) {
      return false;
    }

    *out_result = fut.get();
    return out_result->ok;
  }

  bool Invoke(TaskKind kind, const std::string& req_payload, size_t resp_capacity,
              RpcCallResult* out_result, size_t* slot_idx) {
    if (!Init()) {
      return false;
    }

    SemaphoreGuard inflight_guard(pc->inflight_sem.get());
    if (!inflight_guard.AcquireFor(std::chrono::milliseconds(pc->rpc_timeout_ms))) {
      return false;
    }

    if (pc->pool_size > 0) {
      return RunPoolTask(kind, req_payload, resp_capacity, out_result, slot_idx);
    }
    return RunThreadLocalTask(kind, req_payload, resp_capacity, out_result);
  }

  size_t PoolSize() const {
    return pc->pool_size > 0 ? static_cast<size_t>(pc->pool_size) : 0;
  }

  bool IsConnected() const {
    if (!pc->initialized) {
      return false;
    }

    if (pc->pool_size > 0) {
      for (const auto& worker : pc->workers) {
        if (worker && worker->IsConnected()) {
          return true;
        }
      }
      return false;
    }

    auto it = tls_thread_ctx_map.find(pc->client_id);
    return (it != tls_thread_ctx_map.end()) && it->second && it->second->connected;
  }

  std::shared_ptr<ProcessContext> pc;
  std::mutex init_mu;
};

ErpcBloomCheckClient::ErpcBloomCheckClient(const std::string& local_uri,
                                           const std::string& server_uri, int numa_node,
                                           uint8_t rpc_id, uint8_t server_rpc_id)
    : impl_(std::make_unique<ErpcBloomCheckClientImpl>(local_uri, server_uri, numa_node,
                                                        rpc_id, server_rpc_id)) {}

ErpcBloomCheckClient::~ErpcBloomCheckClient() {
  if (impl_ != nullptr) {
    impl_->Shutdown();
    if (impl_->pc != nullptr) {
      tls_thread_ctx_map.erase(impl_->pc->client_id);
    }
  }
}

bool ErpcBloomCheckClient::Init() { return impl_->Init(); }

void ErpcBloomCheckClient::Shutdown() {
  if (impl_ != nullptr) {
    impl_->Shutdown();
  }
}

bool ErpcBloomCheckClient::IsConnected() const { return impl_->IsConnected(); }

size_t ErpcBloomCheckClient::PoolSize() const {
  return impl_ != nullptr ? impl_->PoolSize() : 0;
}

bool ErpcBloomCheckClient::Noop(uint64_t* server_ts_ns, uint64_t* rtt_ns,
                                size_t* slot_idx) {
  RpcCallResult result;
  bloomcheck::NoopRequest req;
  std::string payload;
  req.SerializeToString(&payload);

  bool ok = impl_->Invoke(TaskKind::kNoop, payload, 256, &result, slot_idx);
  if (server_ts_ns != nullptr) {
    *server_ts_ns = result.server_ts_ns;
  }
  if (rtt_ns != nullptr) {
    *rtt_ns = result.rtt_ns;
  }
  return ok;
}

bool ErpcBloomCheckClient::CheckBatch(const bloomcheck::BatchRequest& request,
                                      bloomcheck::BatchResponse* response,
                                      uint64_t* rtt_ns, size_t* slot_idx) {
  if (response == nullptr) {
    return false;
  }
  RpcCallResult result;
  std::string payload;
  if (!request.SerializeToString(&payload)) {
    return false;
  }

  bool ok = impl_->Invoke(TaskKind::kCheck, payload, 65536, &result, slot_idx);
  if (rtt_ns != nullptr) {
    *rtt_ns = result.rtt_ns;
  }
  if (ok) {
    *response = std::move(result.check_response);
  }
  return ok;
}

}  // namespace ROCKSDB_NAMESPACE
