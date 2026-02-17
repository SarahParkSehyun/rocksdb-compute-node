// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#include "util/bloomcheck_erpc_client.h"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <time.h>
#include <unordered_map>
#include <utility>
#include <vector>

#include <pthread.h>
#include <sched.h>

#include "tools/bloomcheck.pb.h"

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

void sm_handler(int, erpc::SmEventType, erpc::SmErrType, void*) {
  // Client-side Noop path does not need session event hooks.
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

struct ProcessContext;
class Worker;
struct ThreadContext;

struct InflightNoopCall {
  erpc::MsgBuffer req_msgbuf;
  erpc::MsgBuffer resp_msgbuf;
  uint64_t send_ns = 0;
  uint64_t rtt_ns = 0;
  uint64_t server_ts_ns = 0;
  bool parse_ok = false;
  std::atomic<bool> done{false};
};

struct NoopResult {
  bool ok = false;
  uint64_t server_ts_ns = 0;
  uint64_t rtt_ns = 0;
};

struct NoopTask {
  uint64_t deadline_ns = 0;
  std::promise<NoopResult> promise;
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
  int connect_timeout_ms = 2000;
  int rpc_timeout_ms = 100;
  int pool_size = 0;
  bool pool_pin_to_cores = false;
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
  std::vector<InflightNoopCall*> timed_out_calls;

  ~ThreadContext() { ReapTimedOutCalls(); }

  void ReapTimedOutCalls() {
    for (size_t i = 0; i < timed_out_calls.size();) {
      InflightNoopCall* call = timed_out_calls[i];
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

    rpc = std::make_unique<erpc::Rpc<erpc::CTransport>>(pc->nexus.get(), this, rpc_id,
                                                         sm_handler);
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

    const uint64_t deadline_ns =
        NowNs() + static_cast<uint64_t>(pc->connect_timeout_ms) * 1000000ULL;
    while (!rpc->is_connected(session_num)) {
      rpc->run_event_loop_once();
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

void noop_cont_func(void* context, void* tag) {
  (void)context;
  auto* call = static_cast<InflightNoopCall*>(tag);

  const uint64_t recv_ns = NowNs();
  call->rtt_ns = (recv_ns > call->send_ns) ? (recv_ns - call->send_ns) : 0;

  if (call->resp_msgbuf.get_data_size() > 0) {
    bloomcheck::NoopResponse resp;
    call->parse_ok = resp.ParseFromArray(
        call->resp_msgbuf.buf_, static_cast<int>(call->resp_msgbuf.get_data_size()));
    if (call->parse_ok && resp.has_server_ts_ns()) {
      call->server_ts_ns = resp.server_ts_ns();
    }
  } else {
    call->parse_ok = false;
  }

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

  bool EnqueueNoop(uint64_t deadline_ns, std::future<NoopResult>* out_future) {
    if (out_future == nullptr || !started_) {
      return false;
    }

    auto task = std::make_unique<NoopTask>();
    task->deadline_ns = deadline_ns;
    *out_future = task->promise.get_future();

    {
      std::lock_guard<std::mutex> lock(mu_);
      if (shutdown_) {
        return false;
      }
      queue_.push_back(std::move(task));
    }
    cv_.notify_one();
    return true;
  }

  bool IsConnected() const { return connected_.load(std::memory_order_acquire); }

 private:
  void ReapTimedOutCalls() {
    if (!rpc_) {
      return;
    }
    for (size_t i = 0; i < timed_out_calls_.size();) {
      InflightNoopCall* call = timed_out_calls_[i];
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

  void FailTask(std::unique_ptr<NoopTask>* task) {
    if (task == nullptr || task->get() == nullptr) {
      return;
    }
    NoopResult result;
    (*task)->promise.set_value(result);
    task->reset();
  }

  void TryPinToNumaCore(const ProcessContext& pc) {
    if (!pc.pool_pin_to_cores) {
      return;
    }
    std::vector<int> cpus = GetNumaNodeCpus(pc.numa_node);
    if (cpus.empty()) {
      fprintf(stderr,
              "[ErpcBloomCheckClient] Worker %zu pin skipped: node%d cpulist unavailable\n",
              index_, pc.numa_node);
      return;
    }
    int cpu = cpus[index_ % cpus.size()];
    if (PinCurrentThreadToCpu(cpu)) {
      fprintf(stderr,
              "[ErpcBloomCheckClient] Worker %zu pinned to CPU %d (node%d)\n", index_,
              cpu, pc.numa_node);
    } else {
      fprintf(stderr,
              "[ErpcBloomCheckClient] Worker %zu pin failed for CPU %d (node%d)\n",
              index_, cpu, pc.numa_node);
    }
  }

  bool InitRpcAndSession(const ProcessContext& pc) {
    rpc_ = std::make_unique<erpc::Rpc<erpc::CTransport>>(pc.nexus.get(), this, rpc_id_,
                                                          sm_handler);
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

    const uint64_t deadline_ns =
        NowNs() + static_cast<uint64_t>(pc.connect_timeout_ms) * 1000000ULL;
    while (!rpc_->is_connected(session_num_)) {
      rpc_->run_event_loop_once();
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

  NoopResult ExecuteNoop(const ProcessContext& pc, uint64_t deadline_ns) {
    NoopResult result;
    ReapTimedOutCalls();

    bloomcheck::NoopRequest req;
    std::string payload;
    req.SerializeToString(&payload);

    auto* call = new InflightNoopCall();
    call->req_msgbuf = rpc_->alloc_msg_buffer_or_die(256);
    call->resp_msgbuf = rpc_->alloc_msg_buffer_or_die(256);

    if (!payload.empty()) {
      rpc_->resize_msg_buffer(&call->req_msgbuf, payload.size());
      memcpy(call->req_msgbuf.buf_, payload.data(), payload.size());
    } else {
      rpc_->resize_msg_buffer(&call->req_msgbuf, 0);
    }

    call->send_ns = NowNs();
    rpc_->enqueue_request(session_num_, ErpcBloomCheckClient::kReqTypeNoop,
                          &call->req_msgbuf, &call->resp_msgbuf, noop_cont_func, call);

    bool timed_out = false;
    while (!call->done.load(std::memory_order_acquire)) {
      rpc_->run_event_loop_once();
      if (NowNs() > deadline_ns) {
        timed_out = true;
        break;
      }
      if (shutdown_) {
        timed_out = true;
        break;
      }
    }

    if (timed_out) {
      timed_out_calls_.push_back(call);
      fprintf(stderr,
              "[ErpcBloomCheckClient] Worker %zu Noop timeout (%d ms, rpc_id=%u)\n",
              index_, pc.rpc_timeout_ms, static_cast<unsigned>(rpc_id_));
      return result;
    }

    result.ok = call->parse_ok;
    result.server_ts_ns = call->server_ts_ns;
    result.rtt_ns = call->rtt_ns;

    rpc_->free_msg_buffer(call->req_msgbuf);
    rpc_->free_msg_buffer(call->resp_msgbuf);
    delete call;
    return result;
  }

  void ThreadMain() {
    auto pc = pc_.lock();
    if (!pc) {
      return;
    }

    TryPinToNumaCore(*pc);

    bool init_ok = InitRpcAndSession(*pc);
    if (!init_ok) {
      disabled_ = true;
      connected_.store(false, std::memory_order_release);
    }

    while (true) {
      std::unique_ptr<NoopTask> task;
      {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait_for(lock, std::chrono::milliseconds(1),
                     [&] { return shutdown_ || !queue_.empty(); });
        if (shutdown_ && queue_.empty()) {
          break;
        }
        if (!queue_.empty()) {
          task = std::move(queue_.front());
          queue_.pop_front();
        }
      }

      if (!task) {
        if (rpc_) {
          rpc_->run_event_loop_once();
          ReapTimedOutCalls();
        }
        continue;
      }

      if (disabled_ || !rpc_) {
        FailTask(&task);
        continue;
      }

      NoopResult result = ExecuteNoop(*pc, task->deadline_ns);
      task->promise.set_value(result);
    }

    if (rpc_) {
      for (int i = 0; i < 8; ++i) {
        rpc_->run_event_loop_once();
      }
      ReapTimedOutCalls();
    }

    while (!timed_out_calls_.empty()) {
      InflightNoopCall* call = timed_out_calls_.back();
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

  std::thread th_;
  std::unique_ptr<erpc::Rpc<erpc::CTransport>> rpc_;
  int session_num_ = -1;

  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<std::unique_ptr<NoopTask>> queue_;
  bool shutdown_ = false;
  bool started_ = false;
  bool disabled_ = false;
  std::atomic<bool> connected_{false};
  std::vector<InflightNoopCall*> timed_out_calls_;
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
        ParseEnvInt("BLOOMCHECK_ERPC_POOL_PIN_TO_CORES", 0, 0, 1) == 1;
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
              "pool_size=%d pool_pin_to_cores=%d\n",
              pc->local_uri.c_str(), pc->server_uri.c_str(),
              static_cast<unsigned>(pc->base_rpc_id),
              static_cast<unsigned>(pc->server_rpc_id), pc->connect_timeout_ms,
              pc->rpc_timeout_ms, pc->pool_size, pc->pool_pin_to_cores ? 1 : 0);
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

  bool NoopViaThreadLocal(uint64_t* server_ts_ns, uint64_t* rtt_ns) {
    ThreadContext* tc = GetOrCreateThreadContext();
    if (tc == nullptr || !tc->InitThread()) {
      return false;
    }

    tc->ReapTimedOutCalls();

    bloomcheck::NoopRequest req;
    std::string payload;
    req.SerializeToString(&payload);

    auto* call = new InflightNoopCall();
    call->req_msgbuf = tc->rpc->alloc_msg_buffer_or_die(256);
    call->resp_msgbuf = tc->rpc->alloc_msg_buffer_or_die(256);

    if (!payload.empty()) {
      tc->rpc->resize_msg_buffer(&call->req_msgbuf, payload.size());
      memcpy(call->req_msgbuf.buf_, payload.data(), payload.size());
    } else {
      tc->rpc->resize_msg_buffer(&call->req_msgbuf, 0);
    }

    call->send_ns = NowNs();
    tc->rpc->enqueue_request(tc->session_num, ErpcBloomCheckClient::kReqTypeNoop,
                             &call->req_msgbuf, &call->resp_msgbuf, noop_cont_func,
                             call);

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
      fprintf(stderr, "[ErpcBloomCheckClient] Noop timeout (%d ms, rpc_id=%u)\n",
              pc->rpc_timeout_ms, static_cast<unsigned>(tc->rpc_id));
      return false;
    }

    if (server_ts_ns != nullptr) {
      *server_ts_ns = call->server_ts_ns;
    }
    if (rtt_ns != nullptr) {
      *rtt_ns = call->rtt_ns;
    }

    tc->rpc->free_msg_buffer(call->req_msgbuf);
    tc->rpc->free_msg_buffer(call->resp_msgbuf);
    const bool ok = call->parse_ok;
    delete call;
    return ok;
  }

  bool NoopViaWorkerPool(uint64_t* server_ts_ns, uint64_t* rtt_ns) {
    if (!pc->pool_ready.load(std::memory_order_acquire) || pc->workers.empty()) {
      return false;
    }

    const size_t worker_idx =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % pc->workers.size();
    Worker* worker = pc->workers[worker_idx].get();
    if (worker == nullptr) {
      return false;
    }

    std::future<NoopResult> fut;
    const uint64_t deadline_ns =
        NowNs() + static_cast<uint64_t>(pc->rpc_timeout_ms) * 1000000ULL;
    if (!worker->EnqueueNoop(deadline_ns, &fut)) {
      return false;
    }

    auto status = fut.wait_for(std::chrono::milliseconds(pc->rpc_timeout_ms));
    if (status != std::future_status::ready) {
      return false;
    }

    NoopResult result = fut.get();
    if (server_ts_ns != nullptr) {
      *server_ts_ns = result.server_ts_ns;
    }
    if (rtt_ns != nullptr) {
      *rtt_ns = result.rtt_ns;
    }
    return result.ok;
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

bool ErpcBloomCheckClient::Noop(uint64_t* server_ts_ns, uint64_t* rtt_ns) {
  if (!impl_->Init()) {
    return false;
  }

  if (impl_->pc->pool_size > 0) {
    return impl_->NoopViaWorkerPool(server_ts_ns, rtt_ns);
  }
  return impl_->NoopViaThreadLocal(server_ts_ns, rtt_ns);
}

}  // namespace ROCKSDB_NAMESPACE
